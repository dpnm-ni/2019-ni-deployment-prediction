###########################################################################
### Package imports and functions that are used throughout the project. ###
###########################################################################

library(data.table)
library(dplyr)
library(forcats)
library(ggplot2)
library(h2o)
library(optparse)
library(purrr)
library(RColorBrewer)
library(RcppRoll)
library(readr)
library(tidyr)

# Read topology file like the one used by Bari et al.
readTopo <- function(topofile = "inet2") {
    ptr <- 1
    # First line: number of nodes and edges.
    topoinfo <- read_lines(topofile, skip = 0, n_max = 1)
    n.nodes <- as.numeric(strsplit(topoinfo, split = " ")[[1]][1])
    n.edges <- as.numeric(strsplit(topoinfo, split = " ")[[1]][2])
    # Next n.nodes lines: node info, i.e., ID and number of CPU cores.
    nodeinfo <- data.frame()
    for (i in 1:n.nodes) {
        curnode <- strsplit(read_lines(topofile, skip = ptr, n_max = 1), split = " ")[[1]]
        nodeinfo <- rbind(nodeinfo, data.frame(v = i,
                                               cpu = as.numeric(curnode[2])))
        ptr <- ptr + 1
    }
    # Next n.edges lines: link information including source, destination, capacity, and imposed delay.
    edgeinfo <- data.frame()
    for (i in 1:n.edges) {
        curedge <- strsplit(read_lines(topofile, skip = ptr, n_max = 1), split = " ")[[1]]
        edgeinfo <- rbind(edgeinfo, data.frame(from = as.numeric(curedge[1]) + 1,
                                               to = as.numeric(curedge[2]) + 1,
                                               bw = as.numeric(curedge[3]),
                                               delay = as.numeric(curedge[4])))
        ptr <- ptr + 1
    }
    return(list(n.nodes, n.edges, nodeinfo, edgeinfo))
}

# Given a slope value / vector of slope values, map them to -1, 0, or 1, based on the sign.
discretizeSlope <- function(theslope) {
    return(case_when(
        theslope > 0 ~ 1,
        theslope == 0 ~ 0,
        theslope < 0 ~ -1
    ))
}

# Given a vector of observations, calculate the width of the confidence interval.
getCI <- function(vec) {
    return(qt(.95, length(vec) - 1) * sd(vec) / sqrt(length(vec)))
}

# Generate copper colormap with n distinct colors.
copper <- function(n) {
    fromColor <- c(0, 0, 0) * 255
    toColor <- c(1.00, 0.7812, 0.4975) * 255
    # Equidistant colors between fromColor and toColor.
    cols <- matrix(
        c(
            seq(fromColor[1], toColor[1], length.out = n),
            seq(fromColor[2], toColor[2], length.out = n),
            seq(fromColor[3], toColor[3], length.out = n)
        ), nrow = n)
    # Conversion to hex by rowwise application of rgb2hex.
    cols <- apply(cols, 1, rgb2hex)
    return(cols)
}

# Convert vector of rgb values to a hex string that is prepended with #.
rgb2hex <- function(rgbVec) {
    paste(c("#", lapply(rgbVec, function(x) return(sprintf("%02x", round(x))))), collapse = "")
}

# Create data set that can be used for training & testing.
# trainFileNames:   List of training-*.rds filenames (with path) that should be aggregated.
# targetVNF:        VNF type for which action labels are created.
# extractEndtimes:  Whether to include the timestamp of each prediction event as a feature.
# saveWeekInfo:     Whether to include information on the week.
# saveTimeInfo:     Whether to add information on weekday, hour, minute.
createDataset <- function(trainFileNames, targetVNF = "firewall", extractEndtimes = F, saveWeekInfo = F, saveTimeInfo = F) {
    
    fulldataset <- tibble()
    
    for (i in 1:length(trainFileNames)) {
        curData <- readRDS(trainFileNames[i])
        if (saveWeekInfo) {
            curData <- curData %>% mutate(week = i) #trainFileNames[i])
        }
        fulldataset <- rbind(fulldataset, curData)
    }
    
    vnfcat <- readRDS("vnfcat.rds")
    
    # Disambiguate between columns that represent features for the given use case and those that represent labels.
    relevantFeatures <- c()
    predFeatures <- c()
    for (i in 1:nrow(vnfcat)) {
        curvnfname <- vnfcat$name[i]
        predFeatures <- c(predFeatures, sprintf("%saction", curvnfname), sprintf("%sactionD", curvnfname))
    }
    if (saveTimeInfo) {
        fulldataset <-
            fulldataset %>%
            mutate(
                minute = floor(endtime / 60) %% 60,
                hour = floor(endtime / 3600) %% 24,
                day = floor(endtime / (3600 * 24)))
    }
    fnames <- names(fulldataset)
    if (extractEndtimes) {
        idxFrom <- 1
    } else {
        idxFrom <- 2
    }
    for (i in idxFrom:length(fnames)) {
        if (fnames[i] %in% predFeatures) {
            next
        }
        relevantFeatures <- c(relevantFeatures, fnames[i])
    }
    
    relevantFeatures <- relevantFeatures[!(grepl("cv|util", relevantFeatures) & grepl(paste(subset(vnfcat, name != targetVNF)$name, collapse = "|"), relevantFeatures))]
    
    removeActionRelatedFeatures <- F
    if (removeActionRelatedFeatures) {
        relevantFeatures <- names(fulldataset)[!grepl("action", names(fulldataset))][-1]
    }
    
    predField <- paste(targetVNF, "actionD", sep = "")
    
    fulldataset <- fulldataset[, c(relevantFeatures, predField)]
    names(fulldataset)[length(names(fulldataset))] <- "action" 
    fulldataset <-
        fulldataset %>%
        mutate(action = factor(action)) %>%
        mutate(action = fct_recode(action, remove = "-1", doNothing = "0", add = "1"))
    
    # Drop NAs, report amount.
    nbefore <- nrow(fulldataset)
    
    fulldataset <-
        fulldataset %>%
        drop_na()
    
    nafter <- nrow(fulldataset)
    
    cat(sprintf("Dropped rows that contained at least one NA entry, went from %d rows to %d.\n", nbefore, nafter))
    
    fulldataset <- unique(fulldataset)
    
    nafterdups <- nrow(fulldataset)
    
    cat(sprintf("Dropped duplicate rows, went from %d rows to %d.\n", nafter, nafterdups))
    
    return(fulldataset)
    
}

# Extract the union of the top n most relevant features from the leaderboard of an automl object.
# Considers up to 5 best performing models in the leaderboard.
aml2topfeat <- function(theAML, topn) {
    aml.lb <- as_tibble(theAML@leaderboard)
    alltopn <- c()
    for (i in 1:min(5, nrow(aml.lb))) {
        curmodid <- aml.lb$model_id[i]
        curvimp <- as_tibble(h2o.varimp(h2o.getModel(curmodid)))
        curtopn <- head(curvimp$variable, topn)
        alltopn <- union(alltopn, curtopn)
    }
    return(alltopn)
}

# Downsample a data set by removing rows until the number of occurences per class diverges by a maximum factor.
downsample <- function(thedf, classcolname, maxImbalance = 1) {
    nperclass <- table(thedf[[classcolname]])
    minocc <- min(nperclass)
    classestodownsample <- nperclass[which(nperclass > maxImbalance * minocc)]
    if (length(classestodownsample) == 0) {
        cat("[ downsample ] Input data set already balanced - doing nothing.\n")
        return(thedf)
    }
    removerows <- c()
    for (i in 1:length(classestodownsample)) {
        curclassname <- names(classestodownsample[i])
        downsampleby <- as.numeric(classestodownsample[i]) - round(maxImbalance * minocc)
        currowids <- which(thedf[[classcolname]] == curclassname)
        currem <- sample(currowids, downsampleby)
        removerows <- c(removerows, currem)
    }
    outdf <- thedf[-removerows, ]
    return(outdf)
}

# Quick comparison of data frames by means of pairwise application of KS test to all numeric columns.
comparedfs <- function(df1, df2) {
    res <- data.frame()
    cnames <- names(df1)
    for (i in 1:length(cnames)) {
        curname <- cnames[i]
        if (!is.numeric(df1[[curname]])) {
            next
        }
        curstat <- as.numeric(ks.test(df1[[curname]], df2[[curname]])$statistic)
        res <- rbind(res, data.frame(name = curname, d = curstat))
    }
    return(res)
}

# Assign class weights, so that the sum of weights per class is equal.
# fulldata %>% group_by(action) %>% summarise(wsum = sum(weight))
generateWeightedDataSet <- function(dataset) {
    
    classweights <- round(table(dataset$action) / nrow(dataset), 2)
    classweights <-
        data.frame(action = as.character(names(classweights)), weight = 1 / as.numeric(classweights)) %>%
        # For the three-class case, this normalization asserts that only the majority class receives a weight < 1.
        mutate(weight = weight / median(weight))
    
    dataset.w <-
        dataset %>%
        full_join(classweights, by = "action") %>%
        mutate(action = factor(action))
    
    return(dataset.w)
    
}

# Print information on the class probability distribution of a data set.
printActionInfo <- function(traindf) {
    obscount <- traindf %>% group_by(action) %>% summarise(nobs = n())
    cat(sprintf("%s - %.2f\t", obscount$action, obscount$nobs / sum(obscount$nobs)))
    cat("\n")
}

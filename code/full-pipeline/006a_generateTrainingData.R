###########################################################################
### Step 006a - Generation of training data for deployment decisions.   ###
### Output: labeled training data for a given scenario & configuration. ###
### Note: parallelism requires step 006b for merging slices.            ###
###########################################################################

source("common.R")

#################
### CLI Opts. ###
#################

optlist <- list(
    make_option(
        "--requestsFile", type = "character", default = "requests.rds",
        help = "Name of RDS file containing the SFC request trace. [default is %default]."),
    make_option(
        "--solverResultsFile", type = "character", default = "solver-results.rds",
        help = "Name of RDS file containing the aggregated solver results. [default is %default]."),
    make_option(
        "--trainingDataOutFile", type = "character", default = "training.rds",
        help = "Name of output RDS file containing the training data [default is %default]."),
    make_option(
        "--aWidth", type = "integer", default = "600", 
        help = "Width of aggregation window in seconds. [default is %default]."),
    make_option(
        "--tPred", type = "integer", default = "200", 
        help = "Prediction time. [default is %default]."),
    make_option(
        "--predMode", type = "character", default = "tail", 
        help = "Prediction mode, i.e., whether the goal is to predict the value at exactly t + tpred (tail) or use the maximum in [t; t + tpred] (max). [default is %default]."),
    make_option(
        "--nSlices", type = "integer", default = "1", 
        help = "Number of slices to subdivide the work into for parallel processing. [default is %default]."),
    make_option(
        "--sliceID", type = "integer", default = "1", 
        help = "Slice number to be handled by this worker (first is 1). [default is %default]."),
    make_option(
        "--seed", type = "integer", default = "42", 
        help = "RNG seed for reproducibility. [default is %default]."))

optparser <- OptionParser(option_list = optlist)

opt <- parse_args(optparser)

##################################################################
### Import solver results from previous step + general config. ###
##################################################################

requestsFile <- opt$requestsFile
solverResultsFile <- opt$solverResultsFile
trainingDataOutFile <- opt$trainingDataOutFile

nvnfs <-
    readRDS(solverResultsFile) %>%
    arrange(rid)
rngSeed <- opt$seed

# Width of aggregation window, e.g., 10 minutes.
awidth <- opt$aWidth
# Prediction time, e.g., 200 seconds (five average iats).
tpred <- opt$tPred
# Prediction mode, i.e., whether to predict value at exactly t + tpred ("tail") or use the maximum in [t; t + tpred] ("max").
predMode <- opt$predMode

## Extract aggregated statistics from aggregation windows that end with the arrival of a request and whose width is equal to awidth.

allowedEndtimes <- (min(nvnfs$arrivaltime) + awidth):(max(nvnfs$arrivaltime) - tpred - 1)

vnfcat <- readRDS("vnfcat.rds")

### Extract more events / statistics from solver results (request departure times, VNF addition / removal times).
reqs <- readRDS(requestsFile) %>%
    mutate(
        rid = 1:nrow(.),
        arrivaltime = as.numeric(arrivaltime),
        traffic = as.numeric(traffic),
        penalty = as.numeric(penalty))

departureData <- data.frame(
    time = nvnfs$arrivaltime + nvnfs$duration,
    rid = nvnfs$rid,
    sfcid = nvnfs$sfcid,
    type = "departure") %>%
    arrange(time)

arrivalData <- data.frame(
    time = nvnfs$arrivaltime,
    rid = nvnfs$rid,
    sfcid = nvnfs$sfcid,
    type = "arrival")

# Extract event (arrival / departure) data on a per-sfcid and per-VNF-type basis.
eventData <- rbind(departureData, arrivalData) %>%
    inner_join(reqs %>% select(rid, starts_with("vnf"))) %>% 
    tidyr::gather(pos, nf, starts_with("vnf")) %>%
    drop_na() %>%
    arrange(time)

# Threshold to filter out endtimes that are too low to contain enough eventData for
# later extraction of arrival / departure features (to avoid "timesincelast*" features to be NA).
endtimeThresh <- eventData %>% head(1) %>% pull(time)
nCombinations <- nrow(unique(eventData %>% select(sfcid, type, nf)))
while (nrow(unique(eventData %>% filter(time < endtimeThresh) %>% select(sfcid, type, nf))) != nCombinations) {
    endtimeThresh <- endtimeThresh * 2
}

# Deployment / shutdown actions and changes in total / per-type bandwidth.
churnData <- data.frame(
    time = nvnfs$arrivaltime[2:nrow(nvnfs)],
    changeN = diff(nvnfs$nvnfs),
    changeB = diff(nvnfs$totalbw),
    type = "global")

for (i in 1:nrow(vnfcat)) {
    curVNFName <- vnfcat$name[i]
    churnDataCurVNF <- data.frame(
        time = nvnfs$arrivaltime[2:nrow(nvnfs)],
        changeN = diff(nvnfs[[sprintf("n%s", curVNFName)]]),
        changeB = diff(nvnfs[[sprintf("bw%s", curVNFName)]]),
        type = sprintf("%s", curVNFName)
    )
    churnData[, sprintf("changeN%s", curVNFName)] <- churnDataCurVNF$changeN
    churnData[, sprintf("changeB%s", curVNFName)] <- churnDataCurVNF$changeB
}

#############
### Main. ###
#############

set.seed(rngSeed)

# Place one endtime between each pair of consecutive arrivals.
endtimes <-
    nvnfs %>%
    select(arrivaltime) %>%
    mutate(nextarrival = lead(arrivaltime), u = runif(nrow(nvnfs))) %>%
    mutate(endtime = arrivaltime + u * (nextarrival - arrivaltime)) %>% 
    drop_na() %>%
    filter(endtime > endtimeThresh) %>%
    pull(endtime)

nsamples <- length(endtimes)

nslices <- opt$nSlices
sliceID <- opt$sliceID

indices <- c(0, round(nsamples / nslices * 1:nslices))

# Avoid using the same sample in different slices by going left-exclusive and right-inclusive.
currentIndices <- c(indices[sliceID] + 1, indices[sliceID + 1])

aggstats <- data.frame(endtime = endtimes[currentIndices[1]:currentIndices[2]])

# Number of arrivals and departures.
aggstats$narrivals <- 0
aggstats$ndepartures <- 0

# Arrival and departure rates.
aggstats$meanarrrate <- 0
aggstats$meandeprate <- 0

# Change of total / per-type number of VNFs and requested bandwidth.
aggstats$changeN <- 0
aggstats$changeB <- 0
for (i in 1:nrow(vnfcat)) {
    aggstats[, sprintf("changeN%s", vnfcat$name[i])] <- 0
    aggstats[, sprintf("changeB%s", vnfcat$name[i])] <- 0
}

# Last actions and time since last action / event per type.
for (i in 1:nrow(vnfcat)) {
    for (k in 1:5) {
        aggstats[, sprintf("lastaction%s%d", vnfcat$name[i], k)] <- 0
    }
    aggstats[, sprintf("timesincelastaction%s", vnfcat$name[i])] <- 0
    aggstats[, sprintf("timesincelastarrival%s", vnfcat$name[i])] <- 0
    aggstats[, sprintf("timesincelastdeparture%s", vnfcat$name[i])] <- 0
}

# Time since last event - global and per SFC type.
nSFCs <- length(unique(eventData$sfcid))
for (i in 1:nSFCs) {
    aggstats[, sprintf("timesincelastarrivalsfc%d", i)] <- 0
    aggstats[, sprintf("timesincelastdeparturesfc%d", i)] <- 0
}

aggstats$timesincelastarrival <- 0
aggstats$timesincelastdeparture <- 0

# Names of features in nvnfs that are used for "tail"-type features in aggstats.
tailFeatureNames <- 
    rbind(
        expand.grid(
            fname = c("n", "bw", "bwalloc",
                      "bwtocap", "bwtoshut", "bwtocapn",
                      "bwtoshutn", "util", "nactive",
                      "maxremcap", "meanremcap", "minremcap"),
            vnfname = vnfcat$name) %>%
            transmute(featurename = paste(fname, vnfname, sep = "")),
        data.frame(featurename = c("nvnfs", "totalbw", "nactive")))$featurename
# Same as "tail"-type features, but covering the most recent 5 values.
tailFeatureNamesN <-
    (expand.grid(n = 1:5, fname = tailFeatureNames) %>%
         transmute(featurename = paste(fname, n, sep = "")))$featurename

aggstats[, tailFeatureNamesN] <- 0

# Columns from nvnfs that represent the quantiles of per-type, per-request bw demands.
quantileCols <- expand.grid(q = seq(0, 1000, 125), nf = vnfcat$name) %>% mutate(colname = sprintf("q%03d%s", q, nf))
aggstats[, quantileCols$colname] <- 0

# Arrival rate in first and second half of aggregation window.
aggstats$rate1 <- 0
aggstats$rate2 <- 0

# Features in terms of statistics and fields.
commonstats <- c(
    "mean",
    "median",
    "sd",
    "cv",
    "min",
    "max",
    "slope")

fields <- c(
    "nvnfs",
    "totalbw",
    sprintf("n%s", vnfcat$name),
    sprintf("bw%s", vnfcat$name),
    sprintf("bwalloc%s", vnfcat$name),
    sprintf("util%s", vnfcat$name),
    sprintf("bwtocap%s", vnfcat$name),
    sprintf("bwtocapn%s", vnfcat$name),
    sprintf("bwtoshut%s", vnfcat$name),
    sprintf("bwtoshutn%s", vnfcat$name))

featurelist <- expand.grid(stat = commonstats, field = fields) %>%
    mutate(
        name = paste(stat, field, sep = "."),
        stat = as.character(stat),
        field = as.character(field))

utilfnames <- sprintf("util%s", vnfcat$name)
# Capacity-related features.
#  Amount of bandwidth until reaching utilization 1.
#  Amount of bandwidth until a VNF can be shut down (not accounting for bin packing issues etc.).
#  Both features can be expressed in terms of absolute values or normalized.
#  Note, however, that due to above reasons, negative values and values > 1 are possible, e.g., when more instances are required than sum(requests) / capacity might suggest.
bwtocapfnames <- sprintf("bwtocap%s", vnfcat$name)
bwtocapnfnames <- sprintf("bwtocapn%s", vnfcat$name)
bwtoshutfnames <- sprintf("bwtoshut%s", vnfcat$name)
bwtoshutnfnames <- sprintf("bwtoshutn%s", vnfcat$name)

for (i in 1:length(utilfnames)) {
    nvnfs[[utilfnames[i]]] <- nvnfs[[sprintf("bw%s", vnfcat$name)[i]]] / nvnfs[[sprintf("bwalloc%s", vnfcat$name)[i]]]
    nvnfs[[bwtocapfnames[i]]] <- nvnfs[[sprintf("bwalloc%s", vnfcat$name)[i]]] - nvnfs[[sprintf("bw%s", vnfcat$name)[i]]]
    nvnfs[[bwtoshutfnames[i]]] <- vnfcat$capacity[i] - nvnfs[[bwtocapfnames[i]]]
    nvnfs[[bwtocapnfnames[i]]] <- nvnfs[[bwtocapfnames[i]]] / vnfcat$capacity[i]
    nvnfs[[bwtoshutnfnames[i]]] <- nvnfs[[bwtoshutfnames[i]]] / vnfcat$capacity[i]
}

aggstats[, featurelist$name] <- 0

# Deployment action regarding VNFs - per type.
#   -i: shut down i instances.
#    0: keep current number of instances.
#    i: deploy i instances.
aggstats[, sprintf("%saction", vnfcat$name)] <- 0

getSlope <- function(vec1, vec2) {
    return(mean(vec2) - mean(vec1))
}

# Coefficient of variation.
cv <- function(vec) {
    return(sd(vec) / mean(vec))
}

skippedRows <- c()

nagg <- nrow(aggstats)

for (i in 1:nagg) {
    
    if (i %% 100 == 0) {
        cat(sprintf("%05d/%05d\n", i, nagg))
    }
    
    endtime <- aggstats$endtime[i]
    starttime <- endtime - awidth
    
    # Skip incomplete intervals.
    if ((starttime < 0) || (endtime + tpred > max(nvnfs$arrivaltime))) {
        warning("[ generateTrainingData ] Incomplete interval detected - skipping.")
        aggstats[i, featurelist$name] <- NA
        skippedRows <- c(skippedRows, i)
        next
    }
    
    # Extract relevant slice of arrivals.
    curslice <-
        nvnfs %>%
        filter(
            arrivaltime >= starttime,
            arrivaltime <= endtime)
    
    # Skip intervals with less than five arrival events (~99% have more) to allow assessing "tail"-features.
    if (nrow(curslice) < 5) {
        warning("[ generateTrainingData ] Interval with less than 5 arrivals detected - skipping.")
        aggstats[i, featurelist$name] <- NA
        skippedRows <- c(skippedRows, i)
        next
    }
    
    cursliceDep <-
        departureData %>%
        filter(
            time >= starttime,
            time <= endtime)
    
    cursliceChurn <-
        churnData %>%
        filter(
            time >= starttime,
            time <= endtime)
    
    firsthalf <- curslice %>% filter(arrivaltime <= starttime + awidth / 2)
    secondhalf <- curslice %>% filter(arrivaltime > starttime + awidth / 2)
    
    if (nrow(firsthalf) == 0 || nrow(secondhalf) == 0) {
        warning(sprintf("[ generateTrainingData ] [ i = %05d ] One of the halves does not contain an event - skipping.", i))
        aggstats[i, featurelist$name] <- NA
        skippedRows <- c(skippedRows, i)
        next
    }
    
    # Extract number of events and arrival rates.
    
    aggstats$narrivals[i] <- nrow(curslice)
    aggstats$ndepartures[i] <- nrow(cursliceDep)
    aggstats$meanarrrate[i] <- nrow(curslice) / awidth
    aggstats$meandeprate[i] <- nrow(cursliceDep) / awidth
    aggstats$changeN[i] <- sum(cursliceChurn$changeN)
    aggstats$changeB[i] <- sum(cursliceChurn$changeB)
    for (j in 1:nrow(vnfcat)) {
        curVNFName <- vnfcat$name[j]
        aggstats[i, sprintf("changeN%s", curVNFName)] <- sum(cursliceChurn[[sprintf("changeN%s", curVNFName)]])
        aggstats[i, sprintf("changeB%s", curVNFName)] <- sum(cursliceChurn[[sprintf("changeB%s", curVNFName)]])
        lastactions <- tail((churnData %>% filter(time <= endtime, !!sym(sprintf("changeN%s", curVNFName)) != 0)), 5)
        if (nrow(lastactions) == 5) {
            for (k in 1:5) {
                aggstats[i, sprintf("lastaction%s%d", curVNFName, k)] <- sign(lastactions[[5 - k + 1, sprintf("changeN%s", curVNFName)]])
            }
            # EXTENSION: Could normalize this /w awidth for robustness.
            aggstats[i, sprintf("timesincelastaction%s", curVNFName)] <- endtime - lastactions[5, ]$time
        } else {
            warning(sprintf("[ generateTrainingData ] [ i = %05d ] Did not find valid last actions for VNF %s.", i, curVNFName))
        }
    }
    
    cursliceEvents <-
        eventData %>%
        filter(time <= endtime)
    
    if (length(unique(cursliceEvents$type)) != 2) {
        warning(sprintf("[ generateTrainingData ] [ i = %05d ] Event data does not contain at least one arrival and one departure - skipping.", i))
        aggstats[i, featurelist$name] <- NA
        skippedRows <- c(skippedRows, i)
        next
    }
    
    for (j in 1:nrow(vnfcat)) {
        curVNFName <- vnfcat$name[j]
        aggstats[i, sprintf("timesincelastarrival%s", curVNFName)] <- endtime - cursliceEvents %>% filter(type == "arrival", nf == curVNFName) %>% tail(1) %>% pull(time)
        aggstats[i, sprintf("timesincelastdeparture%s", curVNFName)] <- endtime - cursliceEvents %>% filter(type == "departure", nf == curVNFName) %>% tail(1) %>% pull(time)
    }
    
    for (j in 1:nSFCs) {
        aggstats[i, sprintf("timesincelastarrivalsfc%d", j)] <- endtime - cursliceEvents %>% filter(type == "arrival", sfcid == j) %>% tail(1) %>% pull(time)
        aggstats[i, sprintf("timesincelastdeparturesfc%d", j)] <- endtime - cursliceEvents %>% filter(type == "departure", sfcid == j) %>% tail(1) %>% pull(time)
    }
    
    aggstats$timesincelastarrival[i] <- endtime - (cursliceEvents %>% filter(type == "arrival") %>% tail(1))$time
    aggstats$timesincelastdeparture[i] <- endtime - (cursliceEvents %>% filter(type == "departure") %>% tail(1))$time
    
    aggstats$rate1[i] <- nrow(firsthalf) / (awidth / 2)
    aggstats$rate2[i] <- nrow(secondhalf) / (awidth / 2)
    
    ### Extraction of "tail"-type features.
    cursliceTail <- curslice %>% tail(n = 5)
    for (j in 1:length(tailFeatureNames)) {
        for (k in 1:5) {
            aggstats[i, sprintf("%s%d", tailFeatureNames[j], 5 - k + 1)] <- cursliceTail[k, tailFeatureNames[j]]
        }
    }
    
    ### Extraction of bw quantile data.
    
    aggstats[i, quantileCols$colname] <- (curslice %>% tail(n = 1))[, quantileCols$colname]
    
    ### Statistics calculation.
    
    for (j in 1:nrow(featurelist)) {
        curfieldname <- featurelist$field[j]
        curstatname <- featurelist$stat[j]
        curfeaturename <- featurelist$name[j]
        
        if (curstatname != "slope") {
            aggstats[i, curfeaturename] <- get(curstatname)(curslice[[curfieldname]])
        } else {
            aggstats[i, curfeaturename] <- getSlope(secondhalf[[curfieldname]], firsthalf[[curfieldname]])
            aggstats[i, gsub("slope", "slopei", curfeaturename)] <- discretizeSlope(getSlope(secondhalf[[curfieldname]], firsthalf[[curfieldname]]))
        }
    }
    
    ### Action identification.
    
    for (j in 1:nrow(vnfcat)) {
        currentnnf <- (nvnfs %>% filter(arrivaltime <= endtime) %>% tail(n = 1))[[sprintf("n%s", vnfcat$name[j])]]
        
        futurennf <- switch (predMode,
            # Predict amount of VNFs that are needed to serve all demands that are active at t + p.
            "tail" = (nvnfs %>% filter(arrivaltime <= endtime + tpred) %>% tail(n = 1))[[sprintf("n%s", vnfcat$name[j])]],
            # Determine maximum number of VNFs that are needed to serve all demands until t + p.
            "max" = max((nvnfs %>% filter(arrivaltime > endtime, arrivaltime <= endtime + tpred))[[sprintf("n%s", vnfcat$name[j])]]))
        
        aggstats[i, sprintf("%saction", vnfcat$name[j])] <- futurennf - currentnnf
    }
    
}

aggstats$meangrowthrate <- aggstats$meanarrrate - aggstats$meandeprate

aggstats <-
    aggstats %>%
    filter(endtime >= awidth, !is.na(mean.totalbw))

for (j in 1:nrow(vnfcat)) {
    # Discretize actions based on their sign.
    aggstats[, sprintf("%sactionD", vnfcat$name[j])] <- discretizeSlope(aggstats[[sprintf("%saction", vnfcat$name[j])]])
}

table(aggstats$firewallaction) / nrow(aggstats)
table(aggstats$firewallactionD) / nrow(aggstats)

aggstats$rateslope <- aggstats$rate2 - aggstats$rate1
aggstats$rateslopei <- discretizeSlope(aggstats$rateslope)

cat(sprintf("%d total, %d w/o duplicates.\n", nrow(aggstats), nrow(unique(aggstats[,names(aggstats)[-1]]))))

saveRDS(aggstats, file = trainingDataOutFile)

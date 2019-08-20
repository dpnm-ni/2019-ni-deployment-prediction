####################################################
### Step 005 - Consolidation of solver results.  ###
### Output: single file with information on all  ###
###         solver results for a given scenario. ###
####################################################

source("common.R")

#################
### CLI Opts. ###
#################

optlist <- list(
    make_option(
        "--requestsFile", type = "character", default = "requests.rds",
        help = "Name of RDS file containing the SFC request trace. [default is %default]."),
    make_option(
        "--nvnfsFile", type = "character", default = "../../solver/middlebox-placement/src/logs/nvnfs.txt",
        help = "Path to the txt output file containing placement information regarding the number of vnf instances. [default is %default]."),
    make_option(
        "--routeinfoFile", type = "character", default = "../../solver/middlebox-placement/src/logs/routeinfo.txt",
        help = "Path to the txt output file containing placement information w.r.t. routing. [default is %default]."),
    make_option(
        "--nodeinfoFile", type = "character", default = "../../solver/middlebox-placement/src/logs/nodeinfo.txt",
        help = "Path to the txt output file containing placement information regarding the status of individual nodes. [default is %default]."),
    make_option(
        "--solverResultsOutFile", type = "character", default = "solver-results.rds",
        help = "Name of output RDS file containing the aggregated solver results. [default is %default]."))

optparser <- OptionParser(option_list = optlist)

opt <- parse_args(optparser)

#######################
### General config. ###
#######################

requestsFile <- opt$requestsFile
nvnfsFile <- opt$nvnfsFile

routeinfoFile <- opt$routeinfoFile
nodeinfoFile <- opt$nodeinfoFile

solverResultsOutFile <- opt$solverResultsOutFile

#############
### Main. ###
#############

# Requests for matching.
requests <- readRDS(requestsFile)
requests$rid <- 1:nrow(requests)

# Extract maximum chain length from requests.
maxchainlength <- as.numeric(gsub("vnf", "", tail(names(requests[grepl("vnf", names(requests))]), n = 1)))

# VNF catalogue for utilization / allocation information.
vnfcat <- readRDS("vnfcat.rds")

# Add column with all VNFs in each request.
requests <-
    requests %>%
    unite(nfchain, sprintf("vnf%d", 1:maxchainlength), remove = F) %>%
    mutate(arrivaltime = as.numeric(arrivaltime),
           traffic = as.numeric(traffic),
           penalty = as.numeric(penalty))

# Add one column per VNF type and extract the amount of requested traffic.
bwcolnames <- sprintf("bw%s", vnfcat$name)
for (i in 1:nrow(vnfcat)) {
    requests <-
        requests %>%
        rowwise() %>%
        mutate(!!bwcolnames[i] := ifelse(grepl(vnfcat$name[i], nfchain), traffic, 0)) %>%
        ungroup()
}

# Returned by Python script that extracts VNF location data.
nvnfs <-
    fread(nvnfsFile, col.names = c("rid", "nvnfs", sprintf("n%s", vnfcat$name))) %>%
    as_tibble() %>%
    arrange(rid)

nvnfs <-
    nvnfs %>%
    # Include request information.
    inner_join(requests %>% select(rid, sfcid, arrivaltime, duration, traffic)) %>%
    mutate(ncpus = 0)

for (i in 1:nrow(vnfcat)){
    # Alternative: sweep(nvnfs[, sprintf("n%s", vnfcat$name)], MARGIN = 2, vnfcat$cpu, `*`)
    nvnfs <-
        nvnfs %>%
        # Multiply number of VNF instances with number of consumed cores.
        mutate(ncpus = ncpus + !!sym(sprintf("n%s", vnfcat$name[i])) * vnfcat$cpu[i]) %>%
        # Determine total amount of bandwidth that is allocated per VNF type.
        mutate(!!sprintf("bwalloc%s", vnfcat$name[i]) := !!sym(sprintf("n%s", vnfcat$name[i])) * vnfcat$capacity[i])
}

# Determine number of active requests after each arrival and calculate corresponding statistics.
nvnfs$nactive <- 0
nvnfs$totalbw <- 0
nvnfs[, sprintf("bw%s", vnfcat$name)] <- 0
nvnfs[, sprintf("nactive%s", vnfcat$name)] <- 0
nvnfs$reqlist <- 0

# Column names for bandwidth quantile information.
quantileCols <- expand.grid(q = seq(0, 1000, 125), nf = vnfcat$name) %>% mutate(colname = sprintf("q%03d%s", q, nf))

nvnfs[, quantileCols$colname] <- NA

for (i in 1:nrow(nvnfs)) {
    
    # Using the current arrival time, determine all active requests for that time.
    curArr <- nvnfs$arrivaltime[i]
    curID <- nvnfs$rid[i]
    activereqs <- subset(requests, (rid <= curID) & (arrivaltime <= curArr) & (arrivaltime + duration >= curArr))
    
    # Number of active SFC requests after the i-th arrival.
    nvnfs$nactive[i] <- nrow(activereqs)
    # Total requested bandwidth, as well as total requested bandwidth per VNF type.
    nvnfs$totalbw[i] <- sum(activereqs$traffic)
    
    # Add information on total requested bandwidth per VNF type as well as quantiles of active requests.
    for (j in 1:nrow(vnfcat)) {
        curNFName <- vnfcat$name[j]
        curColName <- sprintf("bw%s", curNFName)
        nvnfs[i, curColName] <- sum(activereqs[, curColName])
        nvnfs[i, sprintf("nactive%s", curNFName)] <- sum(activereqs[, curColName] != 0)
        # Quantiles of per-request, per-type bandwidth demands.
        curQuantileColNames <- (quantileCols %>% filter(nf == curNFName))$colname
        curVec <- activereqs[[curColName]]
        curVec <- curVec[which(curVec != 0)]
        nvnfs[i, curQuantileColNames] <- as.numeric(quantile(curVec, seq(0, 1, .125))) / vnfcat$capacity[j]
    }
    
    # List of active requests for routeinfo matching.
    nvnfs$reqlist[i] <- list(activereqs$rid)
}

for (i in 1:nrow(vnfcat)) {
    nfutil <- nvnfs[, sprintf("bw%s", vnfcat$name[i])] / nvnfs[, sprintf("bwalloc%s", vnfcat$name[i])]
    ngt1 <- length(which(nfutil > 1))
    if (ngt1 > 0) {
        cat(sprintf("WARNING: NF %s has %d utilization values > 1.\n", vnfcat$name[i], ngt1))
    }
}

### Part 2 - enrich solver results with node-level information.

# Helper functions for later(using purrr::pmap instead of rowwise()).
req2rid <- function(reqlist, req, ...) {return(reqlist[req])}
nf2fname <- function(nf, nfchain, ...) {return(strsplit(nfchain, "_")[[1]][nf])}

routeinfo <-
    fread(routeinfoFile, col.names = c("trafid", "req", "nf", "node")) %>%
    # Translate locally scoped request number to global request id.
    left_join(nvnfs, by = c("trafid" = "rid")) %>%
    select(trafid, req, nf, node, reqlist) %>%
    # Missing route info results in NULL entries.
    drop_na() %>%
    mutate(rid = pmap_dbl(., req2rid)) %>%
    select(trafid, rid, nf, node) %>%
    # Get traffic volume and middlebox data based on global request id.
    left_join(requests) %>%
    select(one_of("trafid", "rid", "nf", "node", c(sprintf("vnf%d", 1:maxchainlength), "traffic"))) %>%
    # Translate local nf id within the request to the actual nf name.
    unite(nfchain, sprintf("vnf%d", 1:maxchainlength), remove = F) %>%
    mutate(nf = pmap_chr(., nf2fname)) %>%
    select(trafid, rid, nf, node, traffic) %>%
    # Add arrivaltime.
    left_join(requests %>% select(rid, arrivaltime), by = c("trafid" = "rid"))

nodetraffic <-
    routeinfo %>%
    # Summarize statistics per (trafid, node, nf) tuple.
    group_by(trafid, arrivaltime, node, nf) %>%
    summarise(totaltraffic = sum(traffic)) %>%
    ungroup()

# Given the number of instantiated VNFs per node, determine the total capacity of each node per NF type.
nodecapacity <-
    fread(nodeinfoFile, col.names = c("trafid", "node", "nf", "ninst")) %>%
    left_join(vnfcat, by = c("nf" = "name")) %>%
    rowwise() %>%
    mutate(capacity = ninst * capacity, usedcpu = ninst * cpu) %>%
    ungroup() %>%
    select(trafid, node, nf, capacity, usedcpu, ninst)

remainingCapacityPerNode <-
    (nodecapacity %>% select(trafid, node, nf, capacity)) %>%
    left_join(nodetraffic %>% select(trafid, node, nf, totaltraffic), by = c("trafid", "node", "nf")) %>%
    mutate(remainingcapacity = capacity - totaltraffic)

newfeatures <-
    remainingCapacityPerNode %>%
    # Calculate statistics regarding the remaining capacity per VNF type at each arrival event.
    group_by(trafid, nf) %>%
    summarise(
        minremcap = min(remainingcapacity),
        meanremcap = mean(remainingcapacity),
        maxremcap = max(remainingcapacity),
        tottraf = sum(totaltraffic)) %>%
    ungroup() %>%
    # Create separate rows for each entry.
    tidyr::gather(feature, value, minremcap:tottraf) %>%
    # Naming consistent with other nvnfs-columns.
    mutate(fname = sprintf("%s%s", feature, nf)) %>%
    # Create one column per feature.
    select(-one_of("feature", "nf")) %>%
    tidyr::spread(fname, value)

nvnfs.extended <-
    nvnfs %>%
    left_join(newfeatures, by = c("rid" = "trafid"))

naRows <- is.na(nvnfs.extended$tottraffirewall)

if (!assertthat::are_equal(nvnfs$bwfirewall[!naRows], nvnfs.extended$tottraffirewall[!naRows])) {
    warning(sprintf("[ processSolverResults ] %s: equality check failed - proceed with caution!", solverResultsOutFile))
} else {
    cat(sprintf("[ processSolverResults ] %s: equality check passed.\n", solverResultsOutFile))
}

nvnfs.extended <-
    nvnfs.extended %>%
    drop_na()

cat(sprintf("[ processSolverResults ] %s: extending introduced %d NAs.\n", solverResultsOutFile, nrow(nvnfs) - nrow(nvnfs.extended)))

saveRDS(nvnfs.extended, solverResultsOutFile)

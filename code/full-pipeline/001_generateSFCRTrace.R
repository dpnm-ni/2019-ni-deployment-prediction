#######################################################
### Step 001 - Generation of SFC requests.          ###
### Output: SFC request trace with information on   ###
###         arrival time, duration, involved nodes, ###
###         VNF chain, and constraints.             ###
#######################################################

source("common.R")

#################
### CLI Opts. ###
#################

optlist <- list(
    make_option(
        "--rdsOutfile", type = "character", default = "requests.rds",
        help = "Name of RDS output file containing the SFC request trace. [default is %default]."),
    make_option(
        "--txtOutfile", type = "character", default = "requests.txt",
        help = "Name of txt output file containing the SFC request trace. [default is %default]."),
    make_option(
        "--topoFile", type = "character", default = "inet2",
        help = "Name of the topology file to use [default is %default]."),
    make_option(
        "--reqsPerMin", type = "double", default = "2",
        help = "Number of requests per minute to generate. [default is %default]."),
    make_option(
        "--iatDist", type = "character", default = "negexp",
        help = "Distribution to use for generating request interarrival times. [default is negative exponential]."),
    make_option(
        "--nMinutes", type = "integer", default = "10000",
        help = "Duration of result trace in minutes. [default is %default (one week)]."),
    make_option(
        "--meanDuration", type = "integer", default = "1000",
        help = "Mean duration of SFC requests. Durations are composed of 100 + NegExp with mean = meanDuration - 100. [default is %default]."),
    make_option(
        "--durDist", type = "character", default = "negexp",
        help = "Distribution to use for generating request durations. [default is negative exponential]."),
    make_option(
        "--seed", type = "integer", default = "42",
        help = "RNG seed for reproducibility. [default is %default]."))

optparser <- OptionParser(option_list = optlist)

opt <- parse_args(optparser)

set.seed(opt$seed)

#############
### Main. ###
#############

# Use abilene-preproc.R to generate abilene-week1.csv.
# nvolume refers to normalized traffic volume.
abilene <-
    fread("abilene-week1.csv", col.names = c("minute", "nvolume")) %>%
    mutate(second = minute * 60)

# 1 SFC request per minute during peak hours.
# Note: lowest rate value leads to highest number of requests!
# 1 / rate = mean interarrival time.
reqsPerMin <- opt$reqsPerMin
minrate <- reqsPerMin / 60

iatDist <- opt$iatDist
durDist <- opt$durDist

# Calculate resulting rate.
abilene <-
    abilene %>%
    mutate(rate = minrate * nvolume)

# Number of requests to generate. 1 week ~ 10k minutes, reqsPerMin per minute => ~10k * reqsPerMin requests in a week.
nrequests <- opt$nMinutes * reqsPerMin


# SFC catalog with VNF chains and associated probabilities (= fraction of total traffic volume).
sfccat <- data.frame(
    sfc = I(c(
        list(c("nat", "firewall", "ids")),
        list(c("nat", "proxy")),
        list(c("nat", "wano")),
        list(c("nat", "firewall", "wano", "ids")))),
    p = c(.6, .1, .2, .1)) %>% 
    mutate(sfcid = 1:nrow(.))

requests <- data.frame()

# Generate an arrival process for each SFC in the catalog.
for (i in 1:nrow(sfccat)) {
    curNRequests <- round(sfccat$p[i] * nrequests)
    curMinrate <- sfccat$p[i] * minrate
    curArrivaltimes <- switch (iatDist,
                               "negexp" = cumsum(rexp(n = curNRequests, rate = curMinrate)),
                               "norm" = cumsum(rnorm(n = curNRequests, mean = 1 / curMinrate, sd = 7.5)))
    requests <- rbind(requests, data.frame(arrivaltime = curArrivaltimes, sfcid = sfccat$sfcid[i]))
}

# Remove requests based on the desired rate at the corresponding arrival time.
requests <-
    requests %>%
    arrange(arrivaltime) %>%
    mutate(probkeep = abilene$nvolume[as.numeric(cut(arrivaltime, abilene$second))]) %>%
    mutate(keep = runif(n = n()) < probkeep) %>%
    filter(keep == T)

# VNF catalog.
vnfcat <-
    fread("middlebox-spec", col.names = c("name", "cpu", "delay", "capacity"), select = 1:4) %>%
    arrange(name)
saveRDS(vnfcat, "vnfcat.rds")
vnfs <- vnfcat$name

topofile <- opt$topoFile
topoinfo <- readTopo(topofile)

# Number of nodes in the network.
nnodes <- topoinfo[[1]][1]

# Keep mean duration of 1000, but make it an RV. Rounding since CPLEX solver expects integer duration.
duration <- switch (durDist,
    "negexp" = round(100 + rexp(nrow(requests), 1 / (opt$meanDuration - 100))),
    "norm" = round(100 + rnorm(nrow(requests), mean = opt$meanDuration - 100, sd = opt$meanDuration / 4)))

requests$duration <- duration
requests$src <- 0
requests$dst <- 0

# Allowed / occurring chain lengths.
allowedchainlengths <- 1:4
colnames <- sprintf("vnf%d", allowedchainlengths)
requests[, colnames] <- NA

# Constant used in original paper, required solver input.
requests$penalty <- sprintf("%.8f", 0.00000010)

# Control average number of requests per instance and latency restrictions via these parameters.
requests$traffic <- round(70000 + runif(n = nrow(requests)) * 50000)
requests$maxlat <- round(700 + runif(n = nrow(requests)) * 50)

for (i in 1:nrow(requests)) {
    # Random source and destination pairs.
    # EXTENSION: probability of choosing a pair could be based on the TM.
    
    allowednodes <- switch (topofile,
        # All nodes are allowed in the case of Internet2.
        "inet2" = 1:nnodes,
        # FIXME: hardcoded for now. Only leaves are allowed as source / destination node in the case
        # of the MEC topology.
        "mec" = topoinfo[[3]] %>% filter(cpu == 40) %>% pull(v)
    )
    
    # FIXME: general / at least for MEC: it's not unreasonable to allow having instances with src == dst.
    fromto <- sample(allowednodes, 2)
    
    # -1 since CPLEX uses 0-indexing.
    requests$src[i] <- min(fromto) - 1
    requests$dst[i] <- max(fromto) - 1
}

# Explicitly add VNF information for each position in the chain.
for (i in 1:nrow(sfccat)) {
    curChain <- sfccat$sfc[i][[1]]
    affectedReqs <- which(requests$sfcid == sfccat$sfcid[i])
    for (j in 1:length(curChain)) {
        requests[affectedReqs, sprintf("vnf%d", j)] <- curChain[j]
    }
}

# Convert into a format that is compatible with traffic files used by the solver.
requests <-
    requests %>%
    mutate(arrivaltime = round(arrivaltime)) %>%
    select(arrivaltime, duration, src, dst, traffic, maxlat, penalty, colnames, sfcid) %>%
    mutate(arrivaltime = sprintf("%d", arrivaltime), traffic = sprintf("%d", traffic))

saveRDS(requests, file = opt$rdsOutfile)

write.table(requests %>% select(-one_of("sfcid")), file = opt$txtOutfile, sep = ",", col.names = F, row.names = F, quote = F)

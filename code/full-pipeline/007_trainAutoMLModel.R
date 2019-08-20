#######################################################
### Step 007 - Training an AutoML model via H2O.    ###
### Output: AutoML object containing performance    ###
###         indicators and model parameters.        ###
#######################################################

source("common.R")
library(h2o)

# Either create a local H2O cluster or connect to a remote one.
useRemoteCluster <- F
remoteClusterIP <- "1.2.3.4"
remoteClusterPort <- 1234

if (useRemoteCluster) {
    h2o.connect(ip = remoteClusterIP, port = remoteClusterPort)
} else {
    h2o.init()
}

# VNF for which deployment actions are to be predicted.
targetVNF <- "firewall"

# The name of the network topology is used to derive corresponding training data files.
topo <- "inet2"

# Example for combining three weeks into one labeled data set.
fulldataset <- createDataset(
    c(sprintf("data/training-wk1_sfccat_norm_%s-p60.rds", topo),
      sprintf("data/training-wk2_sfccat_norm_%s-p60.rds", topo),
      sprintf("data/training-wk3_sfccat_norm_%s-p60.rds", topo)
    ),
    extractEndtimes = T,
    saveTimeInfo = T,
    targetVNF = targetVNF)

printActionInfo(fulldataset)

# Add weight column that is used to counteract class imbalance during model training.
fulldataset.weighted <- generateWeightedDataSet(fulldataset)

# Identifiers for h2o objects representing training / testing sets.
trainName <- sprintf("a-train-%s-%s", topo, targetVNF)
testName <- sprintf("a-test-%s-%s", topo, targetVNF)

# Upload data set to h2o cluster.
fulldataset.weighted.h2o <- h2o.splitFrame(
    data = as.h2o(fulldataset.weighted),
    ratios = .75,
    destination_frames = c(trainName, testName),
    seed = 191)

# Run AutoML for 10 hours.
amlResult <- h2o.automl(
    y = "action",
    weights_column = "weight",
    training_frame = trainName,
    validation_frame = testName,
    # This is only used to determine the order of results that are reported in the leaderboard.
    leaderboard_frame = testName,
    seed = 42,
    max_runtime_secs = 36000,
    max_models = 0,
    nfolds = 9,
    balance_classes = F,
    keep_cross_validation_predictions = T,
    keep_cross_validation_models = T,
    keep_cross_validation_fold_assignment = F,
    project_name = sprintf("aml-%s-%s", topo, targetVNF),
    export_checkpoints_dir = sprintf("data/aml-%s-%s-checkpoints", topo, targetVNF))

saveRDS(amlResult, sprintf("data/aml-%s-%s.rds", topo, targetVNF))

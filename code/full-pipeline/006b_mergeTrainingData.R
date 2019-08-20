# This is only needed in the case of using 006a with nSlices > 1.
# To be used in conjunction with 006a_generateTrainingData.R in order to combine the individual slices into one rds file.

source("common.R")

#################
### CLI Opts. ###
#################

optlist <- list(
    make_option(
        "--trainingDataFilePattern", type = "character", default = "training-slice-*",
        help = "File pattern of rds files to join [default is %default]."),
    make_option(
        "--trainingDataOutFile", type = "character", default = "training.rds",
        help = "Name of output RDS file containing the entire training data [default is %default]."))

optparser <- OptionParser(option_list = optlist)

opt <- parse_args(optparser)

#############
### Main. ###
#############

allfiles <- list.files(path = ".", pattern = opt$trainingDataFilePattern, recursive = T)

trainingdata <- data.frame()

for (i in 1:length(allfiles)) {
    trainingdata <- rbind(trainingdata, readRDS(allfiles[i]))
}

saveRDS(trainingdata, opt$trainingDataOutFile)

#!/bin/bash

# Number of CPU cores to use.
NCORES=18

# Topology file.
# Currently available options: inet2, mec.
TOPOFILE="inet2"

# Distribution of request interarrival time and flow duration.
# Currently available options: exp, norm.
DIST="norm"

# Week identifier, used as seed (one run is typically used to generate
# requests over the span of one week ~ 10k minutes).
WEEK=1
NMINUTES=10000

# Suffix used for consistent file naming scheme.
SUFFIX="wk$WEEK""_sfccat_$DIST""_$TOPOFILE"

# Location of CPLEX log files.
LOGFILEPATH="../../solver/middlebox-placement/src/logs/"

# File names of generated output files (requests, logs, solver results).
REQSRDS="requests-$SUFFIX.rds"
REQSTXT="requests-$SUFFIX.txt"
NVNFSFILE="$LOGFILEPATH""nvnfs-$SUFFIX.txt"
ROUTEINFOFILE="$LOGFILEPATH""routeinfo-$SUFFIX.txt"
NODEINFOFILE="$LOGFILEPATH""nodeinfo-$SUFFIX.txt"
SOLVERRESOUTFILE="solver-results-$SUFFIX.rds"

# Run the individual steps from generating the request trace to obtaining solver results.
Rscript 001_generateSFCRTrace.R --rdsOutfile $REQSRDS --txtOutfile $REQSTXT --topoFile $TOPOFILE --nMinutes $NMINUTES --seed $WEEK --iatDist $DIST --durDist $DIST
python3 002_sfcrTrace2CPLEX.py --sfcrTraceFile $REQSTXT --outFilePath $LOGFILEPATH --suffix $SUFFIX
python3 003_runCPLEXPar.py --trafficListPath $LOGFILEPATH --topoFile $TOPOFILE --suffix $SUFFIX --ncores $NCORES
python3 004_getPlacementInfoFromCPLEXLogs.py --logFilePath $LOGFILEPATH --suffix $SUFFIX
Rscript 005_processSolverResults.R --requestsFile $REQSRDS --nvnfsFile $NVNFSFILE --routeinfoFile $ROUTEINFOFILE --nodeinfoFile $NODEINFOFILE --solverResultsOutFile $SOLVERRESOUTFILE

# Generation of training data.

# Prediction horizon.
TPRED="60"
# Tag for training files (since the same scenario / week can be used
# to generate different training data sets, TAG is different from SUFFIX).
TAG="p$TPRED"
TRAININGDATAOUTFILE="data/training-$SUFFIX-$TAG.rds"

# Export statements to make variables available in the subshells that are opened via parallel.
export SOLVERRESOUTFILE
export SUFFIX
export TPRED
export WEEK
export NCORES
export REQSRDS

parallel -j $NCORES 'SLICEID={}; TRAININGDATAOUTFILE="training-$SUFFIX-slice-$SLICEID.rds"; Rscript 006a_generateTrainingData.R --tPred $TPRED --requestsFile $REQSRDS --solverResultsFile $SOLVERRESOUTFILE --trainingDataOutFile $TRAININGDATAOUTFILE --seed $WEEK --nSlices $NCORES --sliceID $SLICEID' ::: `seq 1 1 $NCORES`
Rscript 006b_mergeTrainingData.R --trainingDataFilePattern "training-$SUFFIX-slice-*" --trainingDataOutFile $TRAININGDATAOUTFILE

# Slices can be removed upon completion, i.e., "rm training-$SUFFIX-slice-*".

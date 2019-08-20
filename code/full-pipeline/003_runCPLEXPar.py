#########################################################
### Step 003 - Calculation of optimal placements.     ###
### Output: log files with placement and solver data. ###
#########################################################

import argparse
from builtins import range
import functools
import glob
import multiprocessing
import random
import re
import subprocess

# Run CPLEX-based solver on all traffic files that match the supplied pattern.
# Detailed information regarding individual parameters can be found below (parser.add_argument(...)).
def runCPLEXPar(trafficListPath, topoFile, suffix, ncores):
    trafficListPattern = 'traf-%s-*' % suffix
    trafficListPrefix = 'traf-%s-' % suffix
    trafficLists = glob.glob(trafficListPath + '/' + trafficListPattern)
    nFiles = len(trafficLists)
    theRange = range(1, nFiles)
    # Support for continuing a cancelled run - make sure to remove the
    # last ncores log files since they might be incomplete.
    existingLogfiles = glob.glob('%s/cplex-full-%s-*' % (trafficListPath, suffix))
    completedIDs = [int(x.split('-')[-1].split('.')[0]) for x in existingLogfiles]
    remainingIndices = list(set(theRange) - set(completedIDs))
    # Single-argument version of runCPLEXSingleInstance.
    cplexSingle = functools.partial(runCPLEXSingleInstance, trafficListPath = trafficListPath, topoFile = topoFile, suffix = suffix)
    pool = multiprocessing.Pool(processes = ncores)
    res = pool.map(cplexSingle, remainingIndices)

# Just like runCPLEX, but performs only the i-th iteration.
def runCPLEXSingleInstance(i, trafficListPath, topoFile, suffix):
    trafficListPattern = 'traf-%s-*' % suffix
    trafficListPrefix = 'traf-%s-' % suffix
    trafficLists = glob.glob(trafficListPath + '/' + trafficListPattern)
    nFiles = len(trafficLists)
    print('%05d/%05d' % (i, nFiles))
    outfile = open('%s/cplex-full-%s-%05d.log' % (trafficListPath, suffix, i), 'w')
    runcommand = './middleman --per_core_cost=0.01 --per_bit_transit_cost=3.626543209876543e-7 --topology_file=%s --middlebox_spec_file=middlebox-spec --traffic_request_file=logs/%s%05d --max_time=7200 --algorithm=cplex' % (topoFile, trafficListPrefix, i)
    subprocess.call(
        runcommand.split(),
        cwd = '../../solver/middlebox-placement/src', stdout = outfile)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--trafficListPath",
        help = "Folder in which to look for request lists that are compatible with the CPLEX-based solver. [default is ../../solver/middlebox-placement/src/logs].",
        type = str,
        default = "../../solver/middlebox-placement/src/logs")

    parser.add_argument(
        "--topoFile",
        help = "Name of the topology file to use. [default is inet2].",
        type = str,
        default = "inet2")

    parser.add_argument(
        "--suffix",
        help = "Suffix to add to output log files. The resulting file names have the following structure: cplex-full-$suffix-$requestID.log. [default is the empty string].",
        type = str,
        default = "")

    parser.add_argument(
        "--ncores",
        help = "Number of subprocesses to launch. Each will launch a separate CPLEX instance. [default is 18].",
        type = int,
        default = 18)

    args = parser.parse_args()

    runCPLEXPar(
        trafficListPath = args.trafficListPath,
        topoFile = args.topoFile,
        suffix = args.suffix,
        ncores = args.ncores)

#############################################################
### Step 002 - Generation of placement problem instances. ###
### Output: files with traffic lists that are compatible  ###
###         with the CPLEX-based solver.                  ###
#############################################################

import argparse
from builtins import range
import functools
import glob
import multiprocessing
import random
import re
import subprocess

# Given a SFC request history, generate traffic lists for CPLEX-based solver.
# For each request arrival, these traffic lists contain a list of requests
# that are active at that time and represent an instance of the VNF placement problem.
# Detailed information regarding individual parameters can be found below (parser.add_argument(...)).
def sfcrTrace2cplex(sfcrTraceFile, outFilePath, suffix):
    with open(sfcrTraceFile) as f:
        content = f.readlines()
        nlines = len(content)
        for i in range(0, nlines):
            if i % 100 == 0:
                print('%05d/%05d' % (i, nlines))
            partsI = content[i].split(',')
            (arrI, durI) = (int(partsI[0]), int(partsI[1]))
            with open(outFilePath + '/traf-%s-%05d' % (suffix, i + 1), 'w') as fout:
                # i + 1 to take into account the current request
                for j in range(0, i + 1):
                    partsJ = content[j].split(',')
                    (arrJ, durJ) = (int(partsJ[0]), int(partsJ[1]))
                    # Ignore requests that are not active anymore when the i-th request arrives.
                    if arrJ + durJ < arrI:
                        continue
                    # Replace arrival time with 0, remove duration, and place everything at once.
                    # WARNING: don't use \r\n here, since then middleman (the CPLEX-based solver) will ignore the last VNF in the chain due to parsing its name as $name^M.
                    fout.write('0,' + ','.join([pt.strip() for pt in partsJ[2:] if not pt.startswith('NA')]) + '\n')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--sfcrTraceFile",
        help = "Text file with the entire SFC request trace. [default is requests.txt].",
        type = str,
        default = "requests.txt")

    parser.add_argument(
        "--outFilePath",
        help = "Folder in which to store requests that are compatible with the CPLEX-based solver. [default is ../../solver/middlebox-placement/src/logs].",
        type = str,
        default = "../../solver/middlebox-placement/src/logs")

    parser.add_argument(
        "--suffix",
        help = "Suffix to add to output log files. The resulting file names have the following structure: traf-$suffix-$requestID. [default is the empty string].",
        type = str,
        default = "")

    args = parser.parse_args()

    sfcrTrace2cplex(
        sfcrTraceFile = args.sfcrTraceFile,
        outFilePath = args.outFilePath,
        suffix = args.suffix)

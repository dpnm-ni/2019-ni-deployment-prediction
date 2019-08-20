###############################################################
### Step 004 - Extraction of optimal placement information. ###
### Output: placement characteristics, i.e., number and     ###
###         location of VNF instances.                      ###
###############################################################

import argparse
from builtins import range
import functools
import glob
import multiprocessing
import random
import re
import subprocess

# For each log CPLEX-produced log file that matches the provided pattern,
# extract information regarding the number and location of placed VNF instances.
# Detailed information regarding individual parameters can be found below (parser.add_argument(...)).
def getPlacementInfoFromCPLEXLogs(logFilePath, suffix, vnfTypes):
    logFilePattern = 'cplex-full-%s-*.log' % (suffix)
    logFiles = glob.glob(logFilePath + '/' + logFilePattern)
    nFiles = len(logFiles)
    with open('%s/nvnfs-%s.txt' % (logFilePath, suffix), 'w') as fout1,\
            open('%s/nodeinfo-%s.txt' % (logFilePath, suffix), 'w') as fout2,\
            open('%s/routeinfo-%s.txt' % (logFilePath, suffix), 'w') as fout3:
        for i in range(0, nFiles):
            if i % 100 == 0:
                print('file %05d of %05d' % (i, nFiles - 1))
            with open(logFiles[i]) as f:
                curlogfile = logFiles[i]
                curreqid = int(curlogfile.split('-')[-1].split('.')[0])

                # Go through logfile once, extracting relevant information.
                vnflocs = []
                routeinfo = []
                for line in f.readlines():
                    if 'vnfloc' in line:
                        vnflocs.append(line)
                    if 'routeinfo' in line:
                        routeinfo.append(line)

                ########################################
                ### Aggregated placement information ###
                ########################################

                if len(vnflocs) == 0:
                    print('[ WARN ] no vnfloc tag in file %s - skipping.' % logFiles[i])
                    continue
                # Last line gives final number of deployed middleboxes.
                nMiddleBoxes = vnflocs[-1]
                nMiddleBoxes = int(nMiddleBoxes.split(' ')[3])
                # Given the number of middleboxes, extract their location and type.
                middleboxLocations = []
                for line in vnflocs[-(nMiddleBoxes + 1):-1]:
                    (location, vnfType) = (int(line.split()[5]), line.split()[3][:-1])
                    middleboxLocations.append((location, vnfType))
                # Aggregate to get number of vnfs per type. -- FIXME: make more compact / readable.
                nPerType = [(vnfType, [item[1] for item in middleboxLocations].count(vnfType)) for vnfType in vnfTypes]
                # FIXME: generalize for the case of an arbitrary number of types.
                fout1.write('%d,%d,%s\r\n' % (curreqid, nMiddleBoxes, ','.join([str(item[1]) for item in nPerType])))

                # Number of instaces of a VNF per node.
                nodeinfo = [(item[0], item[1], middleboxLocations.count(item)) for item in set(middleboxLocations)]
                for line in nodeinfo:
                    fout2.write('%d,%d,%s,%d\r\n' % (curreqid, line[0], line[1], line[2]))

                ######################################
                ### Detailed placement information ###
                ######################################

                if len(routeinfo) == 0:
                    print('[ WARN ] no routeinfo tag in file %s - skipping.' % logFiles[i])
                    continue
                # Find index of last occurrence of " ... [routeinfo] start".
                startIdx = next(i for i in reversed(range(len(routeinfo))) if 'start' in routeinfo[i])
                routeinfo = routeinfo[(startIdx + 1):]
                currentprefix = '%d,' % (curreqid)
                for line in routeinfo:
                    currentinfo = re.sub(r'.*demand (\d+); mbox (\d+); node (\d+)', r'\1,\2,\3', line)
                    fout3.write(currentprefix + currentinfo.strip() + '\r\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--logFilePath",
        help = "Folder in which to look for CPLEX log files and to which to write output placement information logs. [default is ../../solver/middlebox-placement/src/logs].",
        type = str,
        default = "../../solver/middlebox-placement/src/logs")

    parser.add_argument(
        "--suffix",
        help = "Suffix to add to output log files. The resulting file names have the following structure: cplex-full-$suffix-$requestID.log. [default is the empty string].",
        type = str,
        default = "")

    args = parser.parse_args()

    # FIXME: Make this a parameter as well and / or supply a config file with the vnf catalogue.
    vnfTypes = ['firewall', 'proxy', 'ids', 'nat', 'wano']
    vnfTypes.sort()

    getPlacementInfoFromCPLEXLogs(
        logFilePath = args.logFilePath,
        suffix = args.suffix,
        vnfTypes = vnfTypes)

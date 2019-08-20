# Predicting VNF Deployment Decisions under Dynamically Changing Network Conditions

## General Idea

Use existing placement algorithms to generate (problem, solution) pairs for networks with dynamically arriving SFC requests. Then, learn from the solutions in order to make decisions regarding the deployment / shutdown of VNF instances.

## Methodology

1. Generation of SFC requests that are characterized by their arrival time, duration, source and destination nodes, bandwidth constraint, and requested VNF chain.
2. Generation of instances of the VNF placement problem that represent the system state at each request arrival event.
3. Calculation of optimal placements for the above mentioned problem instances.
4. Extraction of optimal placement information from solver log files.
5. Consolidation of solver results.
6. Generation of training data for deployment decisions by using solver results to extract features and deployment decisions. A detailed overview of extracted features is provided in `code/full-pipeline/feature-description.txt`.
7. Training of supervised machine learning models using the H2O framework [7].

## Components / Structure

* Code
  * Python-based scripts for parts of SFCR generation and invocation of placement algorithm.
  * R-based scripts for statistical evaluation, parts of SFCR generation, training data generation, and model training.
* Data
  * Traffic matrices from a dataset used in [2]. For extensions, there's also a dataset from [3] which is available at [4], as well as dynamic instances available at [5].
* Solver
  * ILP-based placement algorithm using CPLEX-based implementation from [1].

## Dependencies and Usage Instructions

### Traffix Matrix Data

* Download traffic matrix data (file names X01 through X24) from [2] into the `abilene` folder.

### ILP Solver

* Prepare dependencies for CPLEX-based ILP solver: Java, CPLEX (we used v12.8), environment variables such as `JAVA_HOME` and `CPLEX_ROOT`.
* Clone repository [6] into `solver/middlebox-placement`.
* Apply patch file `patch_e4bd6e8_to_ni-version.patch` according to instructions in `solver/readme.txt`.
* Compile by running `make dbg` in `solver/middlebox-placement/src`.
* Create log file folder `solver/middlebox-placement/src/logs` or change the corresponding parameter (`LOGFILEPATH`) that is passed in steps 2-4.

### R / H2O

* Install the H2O ML framework / R package [8].
* Install required packages: data.table, dplyr, forcats, ggplot2, optparse, purrr, RColorBrewer, RcppRoll, readr, tidyr (some are part of the tidyverse package and can therefore be skipped if tidyverse is already installed).

### Misc.
* GNU parallel [9].

### Usage
* `code/full-pipeline/runwk_template.sh`: exemplary bash script that covers steps 1 to 6, i.e., from generation of requests to extraction of training data.
* `code/full-pipeline/007_trainAutoMLModel.R`: example for training an H2O AutoML model.

### Publication

Lange, S., Kim, H.-G. et al. "*Predicting VNF Deployment Decisions under Dynamically Changing Network Conditions.*" CNSM 2019.

BibTeX entry

```
@inproceedings{lange2019predicting,
  title={{Predicting VNF Deployment Decisions under Dynamically Changing Network Conditions}},
  author={Lange, Stanislav and Kim, Hee-Gon and Jeong, Se-Yeon and Choi, Heeyoul and Yoo, Jae-Hyung and Hong, James Won-Ki},
  booktitle={International Conference on Network and Service Management (CNSM)},
  year={2019}
}
```

### References

[1] Bari, M. F. et al. "*On orchestrating virtual network functions.*" CNSM 2015.

[2] http://www.cs.utexas.edu/~yzhang/research/AbileneTM/

[3] Benson, T. et al. "*Network traffic characteristics of data centers in the wild.*" SIGCOMM 2010.

[4] http://pages.cs.wisc.edu/~tbenson/IMC10_Data.html

[5] http://sndlib.zib.de/dynamicmatrices.overview.action

[6] https://github.com/srcvirus/middlebox-placement/

[7] https://www.h2o.ai/

[8] http://h2o-release.s3.amazonaws.com/h2o/latest_stable.html

[9] Tange, O. "*GNU Parallel - The Command-Line Power Tool*" ;login: The USENIX Magazine 2011.

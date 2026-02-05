# SPEI ISIMIP R 

Computing SPEI (Standardized Precipitation-Evapotranspiration Index) using [SPEI-R](https://github.com/sbegueria/SPEI) and ISIMIP climate data.

## Overview

This setup processes 15 model-scenario combinations (5 models × 3 scenarios).

**Models:** GFDL-ESM4, UKESM1-0-LL, MPI-ESM1-2-HR, IPSL-CM6A-LR, MRI-ESM2-0  
**Scenarios:** SSP126, SSP370, SSP585  
**Time period:** 1850-2100  
**Output:** SPEI at 2, 3, and 6-month timescales


## Environment Installation

```bash
module load anaconda
conda create --prefix $HOME/spei-r r-base r-essentials -y
$HOME/spei-r/bin/R --version
$HOME/spei-r/bin/R -e 'install.packages(c("SPEI", "ncdf4", "optparse", "abind"), repos="https://cran.r-project.org")'
```

## Quick Start
```bash
git clone https://github.com/mo-dkrz/spei-isimip-r.git

cd spei-isimip-r

# run only the first state, it means GFDL-ESM4 and SSP126
# to take a test if it works or not
sbatch --array=0 batch_spei_improved.sh

# run all
./helper.sh submit

# Check SLURM queue
./helper.sh status

# Check which outputs exist
./helper.sh check

# Detailed progress report
./helper.sh progress

# Watch latest log file
./helper.sh monitor

# Automatically detects missing outputs and resubmits
./helper.sh resubmit

# Cancel the all
./helper.sh cancel
```



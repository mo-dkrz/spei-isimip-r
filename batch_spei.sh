#!/bin/bash
#SBATCH --job-name=spei_r
#SBATCH --array=0-14              # 15 jobs: 5 models × 3 scenarios
#SBATCH --qos=medium
#SBATCH --account=ai
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1         # R is single-threaded per job
#SBATCH --mem=64G                  # Memory per job (increase ifneeded)
#SBATCH --output=logs/spei_r_%A_%a.log
#SBATCH --error=logs/spei_r_%A_%a.err

# =============================================================================
# ISIMIP SPEI - R 
# =============================================================================

set -e
set -u
set -o pipefail

mkdir -p logs

# =============================================================================
# CONFIGURATION
# =============================================================================

# Models (lowercase for filenames)
MODELS=(
    "gfdl-esm4"
    "ukesm1-0-ll"
    "mpi-esm1-2-hr"
    "ipsl-cm6a-lr"
    "mri-esm2-0"
)

# Model names (uppercase for directory names)
MODELS_UPPER=(
    "GFDL-ESM4"
    "UKESM1-0-LL"
    "MPI-ESM1-2-HR"
    "IPSL-CM6A-LR"
    "MRI-ESM2-0"
)

# Scenarios
SCENARIOS=(
    "ssp126"
    "ssp370"
    "ssp585"
)

# Settings
PET_METHOD="hargreaves"      # hargreaves, thornthwaite, or penman
SPEI_SCALES="2,3,6"
CALIBRATION="1979-2014"

# Paths - Can be overridden by environment variables
DATA_BASE="${SPEI_DATA_BASE:-/p/projects/ou/labs/ai/mariafe/data}"
OUTPUT_BASE="${SPEI_OUTPUT_BASE:-${HOME}/spei_r_outputs}"
R_SCRIPT="${SPEI_R_SCRIPT:-${HOME}/spei-isimip-r/scripts/isimip_spei_r.R}"
R_PATH="$HOME/spei-r/bin"
# =============================================================================
# ENVIRONMENT SETUP
# =============================================================================

echo "======================================"
echo "SPEI BATCH JOB - Environment Setup"
echo "======================================"

# Set R paths

export PATH="$R_PATH:$PATH"

# Verify R is available
if [ ! -f "$R_PATH/Rscript" ]; then
    echo "ERROR: Rscript not found at $R_PATH/Rscript"
    exit 1
fi

echo "  Using R at: $R_PATH"
echo "  R version: $($R_PATH/R --version | head -1)"

# Check R packages
echo "Checking R packages..."
$R_PATH/Rscript -e 'stopifnot(require(SPEI), require(ncdf4), require(optparse), require(abind))' || {
    echo "ERROR: Required R packages not installed"
    exit 1
}
echo "  All R packages available"

# Verify R script exists
if [ ! -f "${R_SCRIPT}" ]; then
    echo "ERROR: R script not found: ${R_SCRIPT}"
    exit 1
fi
echo "  R script found: ${R_SCRIPT}"

# =============================================================================
# JOB ARRAY LOGIC
# =============================================================================

# Validate SLURM_ARRAY_TASK_ID
if [ -z "${SLURM_ARRAY_TASK_ID:-}" ]; then
    echo "ERROR: SLURM_ARRAY_TASK_ID not set. This script must run as a job array."
    exit 1
fi

# Calculate which model/scenario for this array task
MODEL_IDX=$((SLURM_ARRAY_TASK_ID / 3))
SCENARIO_IDX=$((SLURM_ARRAY_TASK_ID % 3))

MODEL="${MODELS[$MODEL_IDX]}"
MODEL_UPPER="${MODELS_UPPER[$MODEL_IDX]}"
SCENARIO="${SCENARIOS[$SCENARIO_IDX]}"

echo ""
echo "======================================"
echo "SLURM Job Array Task: ${SLURM_ARRAY_TASK_ID}"
echo "======================================"
echo "Model: ${MODEL_UPPER} (${MODEL})"
echo "Scenario: ${SCENARIO}"
echo "PET method: ${PET_METHOD}"
echo "SPEI scales: ${SPEI_SCALES}"
echo "Calibration: ${CALIBRATION}"
echo "======================================"
echo ""

# =============================================================================
# FILE PATHS
# =============================================================================

# Input directories
HIST_DIR="${DATA_BASE}/historical/${MODEL_UPPER}_conus"
FUT_DIR="${DATA_BASE}/${SCENARIO}/${MODEL_UPPER}_conus"

# Check directories exist
if [ ! -d "${HIST_DIR}" ]; then
    echo "ERROR: Historical directory not found: ${HIST_DIR}"
    exit 1
fi

if [ ! -d "${FUT_DIR}" ]; then
    echo "ERROR: Future directory not found: ${FUT_DIR}"
    exit 1
fi

echo "  Input directories found"
echo "  Historical: ${HIST_DIR}"
echo "  Future: ${FUT_DIR}"

# =============================================================================
# BUILD FILE LISTS
# =============================================================================

echo ""
echo "Building file lists..."

# Function to build file list and exclude prsn
build_file_list() {
    local dir1=$1
    local dir2=$2
    local pattern=$3
    local exclude=${4:-}
    
    local files=""
    
    # Get files from both directories
    if [ -n "${exclude}" ]; then
        files=$(ls ${dir1}/${pattern} ${dir2}/${pattern} 2>/dev/null | grep -v "${exclude}" | sort)
    else
        files=$(ls ${dir1}/${pattern} ${dir2}/${pattern} 2>/dev/null | sort)
    fi
    
    # Check if any files found
    if [ -z "${files}" ]; then
        echo "ERROR: No files found for pattern: ${pattern}"
        return 1
    fi
    
    # Convert to comma-separated
    echo "${files}" | tr '\n' ',' | sed 's/,$//'
}

# Build lists - IMPORTANT: Use exact patterns to avoid including prsn
PR=$(build_file_list "${HIST_DIR}" "${FUT_DIR}" "*_pr_*.nc" "prsn") || exit 1
TASMIN=$(build_file_list "${HIST_DIR}" "${FUT_DIR}" "*_tasmin_*.nc") || exit 1
TASMAX=$(build_file_list "${HIST_DIR}" "${FUT_DIR}" "*_tasmax_*.nc") || exit 1

# Count files
N_PR=$(echo $PR | tr ',' '\n' | wc -l)
N_TASMIN=$(echo $TASMIN | tr ',' '\n' | wc -l)
N_TASMAX=$(echo $TASMAX | tr ',' '\n' | wc -l)
echo ""
echo "DEBUG - First 3 PR files:"
echo "$PR" | tr ',' '\n' | head -3
echo ""
echo "Total PR files: $(echo $PR | tr ',' '\n' | wc -l)"

echo "  PR: ${N_PR} files"
echo "  TASMIN: ${N_TASMIN} files"
echo "  TASMAX: ${N_TASMAX} files"

# Validate file counts
if [ ${N_PR} -eq 0 ] || [ ${N_TASMIN} -eq 0 ] || [ ${N_TASMAX} -eq 0 ]; then
    echo "ERROR: No files found! Check file patterns and paths."
    exit 1
fi

# Verify counts match (they should be the same)
if [ ${N_PR} -ne ${N_TASMIN} ] || [ ${N_PR} -ne ${N_TASMAX} ]; then
    echo "WARNING: File counts don't match! This may indicate a problem."
    echo "  PR: ${N_PR}, TASMIN: ${N_TASMIN}, TASMAX: ${N_TASMAX}"
fi

# Show first file of each type for verification
echo ""
echo "First file of each type:"
echo "  PR: $(echo $PR | cut -d',' -f1)"
echo "  TASMIN: $(echo $TASMIN | cut -d',' -f1)"
echo "  TASMAX: $(echo $TASMAX | cut -d',' -f1)"

# =============================================================================
# OUTPUT SETUP
# =============================================================================

# Output directory
OUTPUT_DIR="${OUTPUT_BASE}/${MODEL}_${SCENARIO}"
mkdir -p "${OUTPUT_DIR}"

# Output files
PET_FILE="${OUTPUT_DIR}/pet_${PET_METHOD}_${MODEL}_${SCENARIO}.nc"
SPEI_FILE="${OUTPUT_DIR}/spei_${MODEL}_${SCENARIO}.nc"

# Check if already done
if [ -f "${SPEI_FILE}" ]; then
    echo ""
    echo "===================="
    echo "OUTPUT ALREADY EXISTS"
    echo "===================="
    echo "File: ${SPEI_FILE}"
    echo "Size: $(du -h ${SPEI_FILE} | cut -f1)"
    echo ""
    echo "Delete this file to rerun, or use '--requeue' to skip completed jobs"
    exit 0
fi

# =============================================================================
# RUN SPEI CALCULATION
# =============================================================================

echo ""
echo "======================================"
echo "Starting SPEI calculation"
echo "======================================"
echo "Output directory: ${OUTPUT_DIR}"
echo "PET file: ${PET_FILE}"
echo "SPEI file: ${SPEI_FILE}"
echo ""
echo "Start time: $(date)"
echo "======================================"
echo ""

# Build R command based on PET method
if [ "${PET_METHOD}" = "penman" ]; then
    echo "Using Penman-Monteith method (loading additional variables)..."
    
    # Build additional file lists for Penman
    TAS=$(build_file_list "${HIST_DIR}" "${FUT_DIR}" "*_tas_*.nc") || exit 1
    HURS=$(build_file_list "${HIST_DIR}" "${FUT_DIR}" "*_hurs_*.nc") || exit 1
    RSDS=$(build_file_list "${HIST_DIR}" "${FUT_DIR}" "*_rsds_*.nc") || exit 1
    SFCWIND=$(build_file_list "${HIST_DIR}" "${FUT_DIR}" "*_sfcwind_*.nc") || exit 1
    PS=$(build_file_list "${HIST_DIR}" "${FUT_DIR}" "*_ps_*.nc") || exit 1
    
    # Run with all variables
    $R_PATH/Rscript "${R_SCRIPT}" \
        --precip "${PR}" \
        --tasmin "${TASMIN}" \
        --tasmax "${TASMAX}" \
        --tas "${TAS}" \
        --hurs "${HURS}" \
        --rsds "${RSDS}" \
        --sfcwind "${SFCWIND}" \
        --ps "${PS}" \
        --pet-method "${PET_METHOD}" \
        --scales "${SPEI_SCALES}" \
        --calibration "${CALIBRATION}" \
        --out-pet "${PET_FILE}" \
        --out-spei "${SPEI_FILE}"
    
    R_EXIT_CODE=$?
    
else
    # Hargreaves or Thornthwaite (only need tasmin/tasmax)
    $R_PATH/Rscript "${R_SCRIPT}" \
        --precip "${PR}" \
        --tasmin "${TASMIN}" \
        --tasmax "${TASMAX}" \
        --pet-method "${PET_METHOD}" \
        --scales "${SPEI_SCALES}" \
        --calibration "${CALIBRATION}" \
        --out-pet "${PET_FILE}" \
        --out-spei "${SPEI_FILE}"
    
    R_EXIT_CODE=$?
fi

# =============================================================================
# VALIDATION AND COMPLETION
# =============================================================================

echo ""
echo "======================================"
echo "R script finished with exit code: ${R_EXIT_CODE}"
echo "======================================"

if [ ${R_EXIT_CODE} -ne 0 ]; then
    echo "ERROR: R script failed!"
    exit ${R_EXIT_CODE}
fi

# Verify outputs were created
if [ ! -f "${SPEI_FILE}" ]; then
    echo "ERROR: SPEI output file was not created!"
    echo "Expected: ${SPEI_FILE}"
    exit 1
fi

if [ ! -f "${PET_FILE}" ]; then
    echo "WARNING: PET output file was not created"
    echo "Expected: ${PET_FILE}"
fi

# Show output file info
echo ""
echo "======================================"
echo "COMPLETE: ${MODEL}_${SCENARIO}"
echo "======================================"
echo "Outputs:"
ls -lh "${PET_FILE}" 2>/dev/null || echo "  PET file: not saved"
ls -lh "${SPEI_FILE}"
echo ""
echo "File sizes:"
echo "  PET: $(du -h ${PET_FILE} 2>/dev/null | cut -f1 || echo 'N/A')"
echo "  SPEI: $(du -h ${SPEI_FILE} | cut -f1)"
echo ""
echo "End time: $(date)"
echo "======================================"

# Quick validation
echo ""
echo "Quick validation:"
ncdump -h "${SPEI_FILE}" | grep -E "(dimensions|variables:)" | head -20

echo ""
echo "Job finished successfully!"
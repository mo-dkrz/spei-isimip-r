#!/bin/bash
# Batch Helper
# Manages SLURM job array for SPEI calculation

set -e
set -o pipefail

# =============================================================================
# Configuration
# =============================================================================

BATCH_SCRIPT="batch_spei.sh"
JOB_NAME="spei_r"
OUTPUT_BASE="${HOME}/spei_r_outputs"

MODELS=("gfdl-esm4" "ukesm1-0-ll" "mpi-esm1-2-hr" "ipsl-cm6a-lr" "mri-esm2-0")
SCENARIOS=("ssp126" "ssp370" "ssp585")

# =============================================================================
# Helper Functions
# =============================================================================

show_help() {
    cat << EOF
Batch Helper
=====================

Usage: $0 [command]

Commands:
  submit       Submit all 15 jobs (5 models x 3 scenarios)
  status       Check job status in SLURM queue
  check        Check which outputs exist
  progress     Show detailed progress summary
  resubmit     Resubmit only failed/missing jobs
  cancel       Cancel all SPEI jobs
  clean        Remove log files
  monitor      Monitor latest log file
  validate     Validate completed outputs
  help         Show this help message

Examples:
  $0 submit        # Start all jobs
  $0 status        # Check SLURM queue
  $0 check         # See what's complete
  $0 progress      # Detailed progress report
  $0 monitor       # Watch latest log

Notes:
  - Jobs are numbered 0-14 (task IDs)
  - Each job processes one model+scenario combination
  - Completed jobs are automatically skipped
  - Use 'resubmit' to retry only failed jobs

EOF
}

# =============================================================================
# Command Functions
# =============================================================================

submit_jobs() {
    echo "======================================"
    echo "Submitting SPEI Batch Jobs"
    echo "======================================"
    
    # Check batch script exists
    if [ ! -f "${BATCH_SCRIPT}" ]; then
        echo "[ERROR] Batch script not found: ${BATCH_SCRIPT}"
        echo "Current directory: $(pwd)"
        exit 1
    fi
    
    echo "[INFO] Batch script: ${BATCH_SCRIPT}"
    echo "[INFO] Job array: 0-14 (15 total jobs)"
    echo ""
    
    # Submit job array
    SUBMIT_OUTPUT=$(sbatch ${BATCH_SCRIPT} 2>&1)
    
    if [ $? -eq 0 ]; then
        JOB_ID=$(echo "${SUBMIT_OUTPUT}" | awk '{print $NF}')
        echo "[OK] Jobs submitted successfully!"
        echo "     Job ID: ${JOB_ID}"
        echo "     Array tasks: 0-14"
        echo ""
        echo "Monitor progress:"
        echo "  $0 status         # Check queue"
        echo "  $0 monitor        # Watch logs"
        echo "  $0 check          # Check outputs"
    else
        echo "[ERROR] Job submission failed!"
        echo "${SUBMIT_OUTPUT}"
        exit 1
    fi
}

check_status() {
    echo "======================================"
    echo "SLURM Job Status"
    echo "======================================"
    
    # Check if squeue is available
    if ! command -v squeue &> /dev/null; then
        echo "[ERROR] squeue command not found"
        exit 1
    fi
    
    # Show current jobs
    JOBS=$(squeue -u $USER -n ${JOB_NAME} 2>&1)
    
    if echo "${JOBS}" | grep -q "Invalid user"; then
        echo "[ERROR] SLURM user not found"
        exit 1
    fi
    
    if echo "${JOBS}" | grep -q "${JOB_NAME}"; then
        echo "${JOBS}" | head -1  # Header
        echo "${JOBS}" | grep "${JOB_NAME}"
        echo ""
        
        # Count by status
        PENDING=$(echo "${JOBS}" | grep -c "PENDING" || true)
        RUNNING=$(echo "${JOBS}" | grep -c "RUNNING" || true)
        
        echo "Summary:"
        echo "  Pending: ${PENDING}"
        echo "  Running: ${RUNNING}"
    else
        echo "[INFO] No ${JOB_NAME} jobs in queue"
    fi
    
    echo ""
    echo "Recent completions (last 24h):"
    
    # Check recent history (may not be available on all systems)
    if command -v sacct &> /dev/null; then
        YESTERDAY=$(date -d '1 day ago' +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)
        sacct -n ${JOB_NAME} --format=JobID%-15,JobName%-10,State%-12,Elapsed%-10,MaxRSS%-10 -S ${YESTERDAY} 2>/dev/null | head -20 || echo "[INFO] sacct not available"
    else
        echo "[INFO] sacct not available on this system"
    fi
}

check_outputs() {
    echo "======================================"
    echo "Output Status"
    echo "======================================"
    
    if [ ! -d "${OUTPUT_BASE}" ]; then
        echo "[INFO] Output directory does not exist yet"
        echo "       Expected: ${OUTPUT_BASE}"
        return 0
    fi
    
    echo "Output directory: ${OUTPUT_BASE}"
    echo ""
    
    TOTAL=15
    DONE=0
    
    printf "%-20s %-10s %-15s\n" "Model_Scenario" "Status" "Size"
    printf "%-20s %-10s %-15s\n" "-------------------" "----------" "---------------"
    
    for MODEL in "${MODELS[@]}"; do
        for SCENARIO in "${SCENARIOS[@]}"; do
            COMBO="${MODEL}_${SCENARIO}"
            SPEI_FILE="${OUTPUT_BASE}/${COMBO}/spei_${MODEL}_${SCENARIO}.nc"
            
            if [ -f "${SPEI_FILE}" ]; then
                SIZE=$(du -h "${SPEI_FILE}" | cut -f1)
                printf "%-20s %-10s %-15s\n" "${COMBO}" "[OK]" "${SIZE}"
                DONE=$((DONE + 1))
            else
                printf "%-20s %-10s %-15s\n" "${COMBO}" "[MISSING]" "-"
            fi
        done
    done
    
    echo ""
    echo "Progress: ${DONE}/${TOTAL} complete ($(( DONE * 100 / TOTAL ))%)"
}

show_progress() {
    echo "======================================"
    echo "Detailed Progress Report"
    echo "======================================"
    echo "Generated: $(date)"
    echo ""
    
    # Task mapping
    echo "Task ID mapping:"
    echo "  0: gfdl-esm4 / ssp126"
    echo "  1: gfdl-esm4 / ssp370"
    echo "  2: gfdl-esm4 / ssp585"
    echo "  3: ukesm1-0-ll / ssp126"
    echo "  4: ukesm1-0-ll / ssp370"
    echo "  5: ukesm1-0-ll / ssp585"
    echo "  6: mpi-esm1-2-hr / ssp126"
    echo "  7: mpi-esm1-2-hr / ssp370"
    echo "  8: mpi-esm1-2-hr / ssp585"
    echo "  9: ipsl-cm6a-lr / ssp126"
    echo " 10: ipsl-cm6a-lr / ssp370"
    echo " 11: ipsl-cm6a-lr / ssp585"
    echo " 12: mri-esm2-0 / ssp126"
    echo " 13: mri-esm2-0 / ssp370"
    echo " 14: mri-esm2-0 / ssp585"
    echo ""
    
    # Check each task
    printf "%-4s %-25s %-12s %-15s\n" "Task" "Model_Scenario" "Status" "Output Size"
    printf "%-4s %-25s %-12s %-15s\n" "----" "-------------------------" "------------" "---------------"
    
    TASK_ID=0
    for MODEL in "${MODELS[@]}"; do
        for SCENARIO in "${SCENARIOS[@]}"; do
            COMBO="${MODEL}_${SCENARIO}"
            SPEI_FILE="${OUTPUT_BASE}/${COMBO}/spei_${MODEL}_${SCENARIO}.nc"
            
            STATUS="UNKNOWN"
            SIZE="-"
            
            if [ -f "${SPEI_FILE}" ]; then
                STATUS="COMPLETE"
                SIZE=$(du -h "${SPEI_FILE}" | cut -f1)
            else
                # Check if job is running
                if command -v squeue &> /dev/null; then
                    JOB_STATUS=$(squeue -u $USER -n ${JOB_NAME} --array -o "%i %T" 2>/dev/null | grep "_${TASK_ID}" | awk '{print $2}' || echo "")
                    if [ -n "${JOB_STATUS}" ]; then
                        STATUS="${JOB_STATUS}"
                    else
                        STATUS="NOT_STARTED"
                    fi
                fi
            fi
            
            printf "%-4d %-25s %-12s %-15s\n" "${TASK_ID}" "${COMBO}" "${STATUS}" "${SIZE}"
            TASK_ID=$((TASK_ID + 1))
        done
    done
    
    echo ""
    check_outputs
}

resubmit_missing() {
    echo "======================================"
    echo "Resubmitting Missing Jobs"
    echo "======================================"
    
    MISSING=()
    TASK_ID=0
    
    for MODEL in "${MODELS[@]}"; do
        for SCENARIO in "${SCENARIOS[@]}"; do
            COMBO="${MODEL}_${SCENARIO}"
            SPEI_FILE="${OUTPUT_BASE}/${COMBO}/spei_${MODEL}_${SCENARIO}.nc"
            
            if [ ! -f "${SPEI_FILE}" ]; then
                MISSING+=($TASK_ID)
                echo "  [MISSING] Task ${TASK_ID}: ${COMBO}"
            fi
            
            TASK_ID=$((TASK_ID + 1))
        done
    done
    
    if [ ${#MISSING[@]} -eq 0 ]; then
        echo "[INFO] All outputs exist! Nothing to resubmit."
        return 0
    fi
    
    echo ""
    echo "[INFO] Found ${#MISSING[@]} missing outputs"
    
    # Build comma-separated list
    TASKS=$(IFS=,; echo "${MISSING[*]}")
    
    echo "[INFO] Resubmitting tasks: ${TASKS}"
    
    # Check batch script exists
    if [ ! -f "${BATCH_SCRIPT}" ]; then
        echo "[ERROR] Batch script not found: ${BATCH_SCRIPT}"
        exit 1
    fi
    
    # Submit only missing tasks
    SUBMIT_OUTPUT=$(sbatch --array=${TASKS} ${BATCH_SCRIPT} 2>&1)
    
    if [ $? -eq 0 ]; then
        JOB_ID=$(echo "${SUBMIT_OUTPUT}" | awk '{print $NF}')
        echo "[OK] Resubmitted successfully!"
        echo "     Job ID: ${JOB_ID}"
        echo "     Tasks: ${TASKS}"
    else
        echo "[ERROR] Resubmission failed!"
        echo "${SUBMIT_OUTPUT}"
        exit 1
    fi
}

cancel_jobs() {
    echo "======================================"
    echo "Canceling SPEI Jobs"
    echo "======================================"
    
    if ! command -v scancel &> /dev/null; then
        echo "[ERROR] scancel command not found"
        exit 1
    fi
    
    # Count jobs before canceling
    NJOBS=$(squeue -u $USER -n ${JOB_NAME} 2>/dev/null | grep -c "${JOB_NAME}" || echo "0")
    
    if [ "${NJOBS}" -eq 0 ]; then
        echo "[INFO] No ${JOB_NAME} jobs to cancel"
        return 0
    fi
    
    echo "[INFO] Canceling ${NJOBS} jobs..."
    scancel -n ${JOB_NAME} -u $USER
    
    echo "[OK] Jobs canceled"
}

clean_logs() {
    echo "======================================"
    echo "Cleaning Log Files"
    echo "======================================"
    
    if [ ! -d "logs" ]; then
        echo "[INFO] No logs directory found"
        return 0
    fi
    
    # Count files
    LOG_COUNT=$(ls logs/${JOB_NAME}_*.log 2>/dev/null | wc -l)
    ERR_COUNT=$(ls logs/${JOB_NAME}_*.err 2>/dev/null | wc -l)
    TOTAL=$((LOG_COUNT + ERR_COUNT))
    
    if [ ${TOTAL} -eq 0 ]; then
        echo "[INFO] No log files to remove"
        return 0
    fi
    
    echo "[INFO] Found ${TOTAL} log files (${LOG_COUNT} .log, ${ERR_COUNT} .err)"
    echo -n "Remove all? (y/N): "
    read -r CONFIRM
    
    if [ "${CONFIRM}" = "y" ] || [ "${CONFIRM}" = "Y" ]; then
        rm -f logs/${JOB_NAME}_*.{log,err}
        echo "[OK] Removed ${TOTAL} files"
    else
        echo "[INFO] Canceled"
    fi
}

monitor_logs() {
    echo "======================================"
    echo "Monitoring Latest Log"
    echo "======================================"
    echo "Press Ctrl+C to stop"
    echo ""
    
    if [ ! -d "logs" ]; then
        echo "[ERROR] No logs directory found"
        return 1
    fi
    
    # Find latest log file
    LATEST=$(ls -t logs/${JOB_NAME}_*.log 2>/dev/null | head -1)
    
    if [ -z "${LATEST}" ]; then
        echo "[INFO] No log files found yet"
        echo "      Waiting for jobs to start..."
        sleep 5
        LATEST=$(ls -t logs/${JOB_NAME}_*.log 2>/dev/null | head -1)
    fi
    
    if [ -n "${LATEST}" ]; then
        echo "Watching: ${LATEST}"
        echo "=========================================="
        tail -f "${LATEST}"
    else
        echo "[ERROR] Still no log files found"
        echo "       Check if jobs have started with: $0 status"
    fi
}

validate_outputs() {
    echo "======================================"
    echo "Validating Outputs"
    echo "======================================"
    
    if [ ! -d "${OUTPUT_BASE}" ]; then
        echo "[ERROR] Output directory not found: ${OUTPUT_BASE}"
        return 1
    fi
    
    TASK_ID=0
    VALID=0
    INVALID=0
    
    for MODEL in "${MODELS[@]}"; do
        for SCENARIO in "${SCENARIOS[@]}"; do
            COMBO="${MODEL}_${SCENARIO}"
            SPEI_FILE="${OUTPUT_BASE}/${COMBO}/spei_${MODEL}_${SCENARIO}.nc"
            
            if [ -f "${SPEI_FILE}" ]; then
                # Quick validation with ncdump
                if ncdump -h "${SPEI_FILE}" &> /dev/null; then
                    # Check for required variables
                    VARS=$(ncdump -h "${SPEI_FILE}" | grep -E "spei_[0-9]+" || true)
                    if [ -n "${VARS}" ]; then
                        echo "[OK] Task ${TASK_ID}: ${COMBO}"
                        VALID=$((VALID + 1))
                    else
                        echo "[ERROR] Task ${TASK_ID}: ${COMBO} - No SPEI variables found"
                        INVALID=$((INVALID + 1))
                    fi
                else
                    echo "[ERROR] Task ${TASK_ID}: ${COMBO} - Invalid NetCDF file"
                    INVALID=$((INVALID + 1))
                fi
            fi
            
            TASK_ID=$((TASK_ID + 1))
        done
    done
    
    echo ""
    echo "Validation summary:"
    echo "  Valid: ${VALID}"
    echo "  Invalid: ${INVALID}"
    echo "  Missing: $((15 - VALID - INVALID))"
}

# =============================================================================
# Main Command Dispatcher
# =============================================================================

case "${1:-}" in
    submit)
        submit_jobs
        ;;
    status)
        check_status
        ;;
    check)
        check_outputs
        ;;
    progress)
        show_progress
        ;;
    resubmit)
        resubmit_missing
        ;;
    cancel)
        cancel_jobs
        ;;
    clean)
        clean_logs
        ;;
    monitor)
        monitor_logs
        ;;
    validate)
        validate_outputs
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        show_help
        exit 1
        ;;
esac
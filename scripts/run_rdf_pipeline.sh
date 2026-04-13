#!/bin/bash
#
# Pipeline: Convert INSDC metadata to RDF using insdc-rdf.
#
# Usage:
#   ./run_rdf_pipeline.sh [options]
#
# Options:
#   --date YYYYMMDD     Target date (default: today)
#   --from-step STEP    Start from specified step (use --list-steps to see available steps)
#   --list-steps        Show available steps and exit
#   --dry-run           Show what would be done without executing
#   --chunk-size N      Number of records per output chunk (default: 100000)
#   --skip-validate     Skip the validate step
#
# Environment variables (optional):
#   DDBJ_SEARCH_CONVERTER_RESULT_DIR    Result directory
#   DDBJ_SEARCH_CONVERTER_DATE          Processing date (YYYYMMDD)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Step definitions (order matters)
STEP_NAMES=(
    "convert"
    "validate"
)

declare -A STEP_ORDER
for i in "${!STEP_NAMES[@]}"; do
    STEP_ORDER["${STEP_NAMES[$i]}"]=$((i + 1))
done

declare -A STEP_DESC=(
    ["convert"]="Convert all sources to RDF (parallel)"
    ["validate"]="Validate output RDF files"
)

# Default values
TARGET_DATE=""
DRY_RUN=false
FROM_STEP=""
FROM_STEP_ORDER=0
CHUNK_SIZE=100000
SKIP_VALIDATE=false

# Input paths
BIOPROJECT_XML="/usr/local/resources/bioproject/bioproject.xml"
BIOSAMPLE_XML="/usr/local/resources/biosample/biosample_set.xml.gz"
SRA_ACCESSIONS_BASE="/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions"

# Show available steps
show_steps() {
    echo "Available steps:"
    echo ""
    for step in "${STEP_NAMES[@]}"; do
        printf "  %-20s %s\n" "$step" "${STEP_DESC[$step]}"
    done
    echo ""
}

# Check if step should be skipped based on --from-step
should_skip_step() {
    local step="$1"
    local step_order="${STEP_ORDER[$step]}"
    [[ $FROM_STEP_ORDER -gt 0 && $step_order -lt $FROM_STEP_ORDER ]]
}

# Find the latest SRA_Accessions.tab file (up to 180 days back)
find_latest_sra_accessions_tab() {
    local target_date="$DATE_STR"
    for days_back in $(seq 0 180); do
        local check_date
        check_date=$(date -d "${target_date:0:4}-${target_date:4:2}-${target_date:6:2} - ${days_back} days" '+%Y%m%d')
        local year=${check_date:0:4}
        local month=${check_date:4:2}
        local path="${SRA_ACCESSIONS_BASE}/${year}/${month}/SRA_Accessions.tab.${check_date}"
        if [[ -f "$path" ]]; then
            echo "$path"
            return 0
        fi
    done
    return 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --date)
            TARGET_DATE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --from-step)
            FROM_STEP="$2"
            shift 2
            ;;
        --list-steps)
            show_steps
            exit 0
            ;;
        --chunk-size)
            CHUNK_SIZE="$2"
            shift 2
            ;;
        --skip-validate)
            SKIP_VALIDATE=true
            shift
            ;;
        -h|--help)
            head -19 "$0" | tail -n +2 | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate --from-step
if [[ -n "$FROM_STEP" ]]; then
    if [[ -z "${STEP_ORDER[$FROM_STEP]:-}" ]]; then
        echo "Error: Unknown step '$FROM_STEP'"
        echo "Use --list-steps to see available steps."
        exit 1
    fi
    FROM_STEP_ORDER="${STEP_ORDER[$FROM_STEP]}"
fi

# Set date environment variable
if [[ -n "$TARGET_DATE" ]]; then
    export DDBJ_SEARCH_CONVERTER_DATE="$TARGET_DATE"
else
    export DDBJ_SEARCH_CONVERTER_DATE="${DDBJ_SEARCH_CONVERTER_DATE:-$(date '+%Y%m%d')}"
fi

DATE_STR="$DDBJ_SEARCH_CONVERTER_DATE"
RESULT_DIR="${DDBJ_SEARCH_CONVERTER_RESULT_DIR:-$(pwd)/ddbj_search_converter_results}"
RDF_OUTPUT_DIR="${RESULT_DIR}/rdf"
SRA_TAR="${RESULT_DIR}/sra_tar/NCBI_SRA_Metadata.tar"

# Logging functions
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

log_section() {
    echo ""
    echo "========================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "========================================"
}

# Get command name from full command string
get_cmd_name() {
    echo "$1" | awk '{print $1}'
}

# Run command with optional dry-run
run_cmd() {
    local cmd="$*"
    local cmd_name
    cmd_name=$(get_cmd_name "$cmd")

    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY-RUN] $cmd"
    else
        log_info "Running: $cmd"
        if eval "$cmd"; then
            log_info "  ✓ ${cmd_name}"
        else
            log_error "  ✗ ${cmd_name}"
            return 1
        fi
    fi
}

# Run commands in parallel
run_parallel() {
    local pids=()
    local cmds=("$@")
    local cmd_names=()
    local failed=0

    for cmd in "${cmds[@]}"; do
        local cmd_name
        cmd_name=$(get_cmd_name "$cmd")
        cmd_names+=("$cmd_name")

        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY-RUN] (parallel) $cmd"
        else
            log_info "Starting: $cmd_name"
            eval "$cmd" &
            pids+=($!)
        fi
    done

    if [[ "$DRY_RUN" == false ]]; then
        local i=0
        for pid in "${pids[@]}"; do
            local cmd_name="${cmd_names[$i]}"
            if wait "$pid"; then
                log_info "  ✓ ${cmd_name}"
            else
                log_error "  ✗ ${cmd_name}"
                failed=1
            fi
            ((i++)) || true
        done

        if [[ $failed -ne 0 ]]; then
            log_error "One or more parallel jobs failed"
            return 1
        fi
    fi
}

# ============================================================
# Main
# ============================================================
main() {
    log_section "DDBJ Search Converter - RDF Pipeline"

    log_info "Date: ${DATE_STR}"
    log_info "Dry run: ${DRY_RUN}"
    log_info "Chunk size: ${CHUNK_SIZE}"
    log_info "Skip validate: ${SKIP_VALIDATE}"
    if [[ -n "$FROM_STEP" ]]; then
        log_info "From step: ${FROM_STEP}"
    fi

    log_info "Environment:"
    log_info "  RESULT_DIR: ${RESULT_DIR}"
    log_info "  RDF_OUTPUT_DIR: ${RDF_OUTPUT_DIR}"

    # Resolve SRA_Accessions.tab path
    local sra_accessions_tab
    if sra_accessions_tab=$(find_latest_sra_accessions_tab); then
        log_info "  SRA_Accessions.tab: ${sra_accessions_tab}"
    else
        log_error "SRA_Accessions.tab not found (searched 180 days back from ${DATE_STR})"
        exit 1
    fi

    # Check SRA Metadata tar
    if [[ -f "$SRA_TAR" ]]; then
        log_info "  SRA_Metadata.tar: ${SRA_TAR}"
    else
        log_error "NCBI_SRA_Metadata.tar not found: ${SRA_TAR}"
        log_error "Run sync_ncbi_tar first."
        exit 1
    fi

    local start_time
    start_time=$(date +%s)

    # Step: convert
    if should_skip_step "convert"; then
        log_info "[SKIP] convert (--from-step)"
    else
        log_info "Step 1: Converting all sources to RDF..."
        run_parallel \
            "insdc-rdf convert --source bioproject --input ${BIOPROJECT_XML} --output-dir ${RDF_OUTPUT_DIR}/bioproject --chunk-size ${CHUNK_SIZE}" \
            "insdc-rdf convert --source biosample --input ${BIOSAMPLE_XML} --output-dir ${RDF_OUTPUT_DIR}/biosample --chunk-size ${CHUNK_SIZE}" \
            "insdc-rdf convert --source sra --input ${sra_accessions_tab} --output-dir ${RDF_OUTPUT_DIR}/sra --chunk-size ${CHUNK_SIZE}" \
            "insdc-rdf convert --source sra-experiment --input ${SRA_TAR} --output-dir ${RDF_OUTPUT_DIR}/sra-experiment --chunk-size ${CHUNK_SIZE}"
    fi

    # Step: validate
    if should_skip_step "validate" || [[ "$SKIP_VALIDATE" == true ]]; then
        if [[ "$SKIP_VALIDATE" == true ]]; then
            log_info "[SKIP] validate (--skip-validate)"
        else
            log_info "[SKIP] validate (--from-step)"
        fi
    else
        log_info "Step 2: Validating output RDF files..."
        run_cmd "insdc-rdf validate ${RDF_OUTPUT_DIR}/bioproject"
        run_cmd "insdc-rdf validate ${RDF_OUTPUT_DIR}/biosample"
        run_cmd "insdc-rdf validate ${RDF_OUTPUT_DIR}/sra"
        run_cmd "insdc-rdf validate ${RDF_OUTPUT_DIR}/sra-experiment"
    fi

    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))

    log_section "RDF Pipeline Complete"
    log_info "Total duration: $((duration / 3600))h $(((duration % 3600) / 60))m $((duration % 60))s"
    log_info "Output: ${RDF_OUTPUT_DIR}"
}

main

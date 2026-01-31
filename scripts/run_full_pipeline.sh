#!/bin/bash
#
# Full pipeline: Build DBLink, generate JSONL, and create+populate ES indexes.
#
# Usage:
#   ./run_full_pipeline.sh [options]
#
# Options:
#   --date YYYYMMDD     Target date (default: today)
#   --skip-dblink       Skip DBLink construction (use existing)
#   --skip-tar-sync     Skip tar file synchronization
#   --skip-es           Skip Elasticsearch operations
#   --dry-run           Show what would be done without executing
#   --parallel N        Max parallel jobs (default: 4)
#
# Environment variables (optional):
#   DDBJ_SEARCH_CONVERTER_RESULT_DIR    Result directory
#   DDBJ_SEARCH_CONVERTER_CONST_DIR     Constant files directory
#   DDBJ_SEARCH_CONVERTER_ES_URL        Elasticsearch URL
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
TARGET_DATE=""
SKIP_DBLINK=false
SKIP_TAR_SYNC=false
SKIP_ES=false
DRY_RUN=false
MAX_PARALLEL=4

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --date)
            TARGET_DATE="$2"
            shift 2
            ;;
        --skip-dblink)
            SKIP_DBLINK=true
            shift
            ;;
        --skip-tar-sync)
            SKIP_TAR_SYNC=true
            shift
            ;;
        --skip-es)
            SKIP_ES=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --parallel)
            MAX_PARALLEL="$2"
            shift 2
            ;;
        -h|--help)
            head -25 "$0" | tail -n +2 | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Set date environment variable if specified
if [[ -n "$TARGET_DATE" ]]; then
    export DDBJ_SEARCH_CONVERTER_DATE="$TARGET_DATE"
fi

DATE_STR="${DDBJ_SEARCH_CONVERTER_DATE:-$(date '+%Y%m%d')}"
RESULT_DIR="${DDBJ_SEARCH_CONVERTER_RESULT_DIR:-$(pwd)/ddbj_search_converter_results}"

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

# Run command with optional dry-run (sequential, stdout/stderr suppressed)
run_cmd() {
    local cmd="$*"
    local cmd_name
    cmd_name=$(get_cmd_name "$cmd")

    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY-RUN] $cmd"
    else
        log_info "Running: $cmd_name"
        if eval "$cmd" > /dev/null 2>&1; then
            log_info "  ✓ ${cmd_name}"
        else
            log_error "  ✗ ${cmd_name} (use show_log for details)"
            return 1
        fi
    fi
}

# Run commands in parallel (stdout/stderr suppressed, use show_log for details)
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
            eval "$cmd" > /dev/null 2>&1 &
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
                log_error "  ✗ ${cmd_name} (use show_log for details)"
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

# Run commands in parallel with limited concurrency
run_parallel_limited() {
    local max_jobs=$1
    shift
    local cmds=("$@")
    local pids=()
    local cmd_names=()
    local failed=0

    for cmd in "${cmds[@]}"; do
        # Wait if we've hit the max
        while [[ ${#pids[@]} -ge $max_jobs ]]; do
            local new_pids=()
            local new_cmd_names=()
            local j=0
            for pid in "${pids[@]}"; do
                if kill -0 "$pid" 2>/dev/null; then
                    new_pids+=("$pid")
                    new_cmd_names+=("${cmd_names[$j]}")
                else
                    local cmd_name="${cmd_names[$j]}"
                    if wait "$pid"; then
                        log_info "  ✓ ${cmd_name}"
                    else
                        log_error "  ✗ ${cmd_name} (use show_log for details)"
                        failed=1
                    fi
                fi
                ((j++)) || true
            done
            pids=("${new_pids[@]}")
            cmd_names=("${new_cmd_names[@]}")
            if [[ ${#pids[@]} -ge $max_jobs ]]; then
                sleep 1
            fi
        done

        local cmd_name
        cmd_name=$(get_cmd_name "$cmd")

        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY-RUN] (parallel) $cmd"
        else
            log_info "Starting: $cmd_name"
            eval "$cmd" > /dev/null 2>&1 &
            pids+=($!)
            cmd_names+=("$cmd_name")
        fi
    done

    # Wait for remaining jobs
    if [[ "$DRY_RUN" == false ]]; then
        local i=0
        for pid in "${pids[@]}"; do
            local cmd_name="${cmd_names[$i]}"
            if wait "$pid"; then
                log_info "  ✓ ${cmd_name}"
            else
                log_error "  ✗ ${cmd_name} (use show_log for details)"
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
# PHASE 1: DBLink Construction
# ============================================================
phase1_dblink() {
    log_section "PHASE 1: DBLink Construction"

    if [[ "$SKIP_DBLINK" == true ]]; then
        log_info "Skipping DBLink construction (--skip-dblink)"
        return
    fi

    log_info "Step 1-1: Preparing XML files and accessions DB..."
    run_parallel \
        "prepare_bioproject_xml" \
        "prepare_biosample_xml" \
        "build_sra_and_dra_accessions_db"

    log_info "Step 1-2: Initializing DBLink DB..."
    run_cmd "init_dblink_db"

    log_info "Step 1-3: Creating DBLink relations..."
    run_parallel_limited "$MAX_PARALLEL" \
        "create_dblink_bp_bs_relations" \
        "create_dblink_bp_relations" \
        "create_dblink_assembly_and_master_relations" \
        "create_dblink_gea_relations" \
        "create_dblink_metabobank_relations" \
        "create_dblink_jga_relations" \
        "create_dblink_sra_internal_relations"

    log_info "Step 1-4: Finalizing DBLink DB..."
    run_cmd "finalize_dblink_db"

    log_info "Step 1-5: Dumping DBLink files..."
    run_cmd "dump_dblink_files"
}

# ============================================================
# PHASE 2: JSONL Generation
# ============================================================
phase2_jsonl() {
    log_section "PHASE 2: JSONL Generation"

    if [[ "$SKIP_TAR_SYNC" == true ]]; then
        log_info "Skipping tar sync (--skip-tar-sync)"
        log_info "Step 2-1: Building date cache only..."
        run_cmd "build_bp_bs_date_cache"
    else
        log_info "Step 2-1: Syncing tar files and building date cache..."
        run_parallel \
            "sync_ncbi_tar" \
            "sync_dra_tar" \
            "build_bp_bs_date_cache"
    fi

    log_info "Step 2-2: Generating JSONL files (full mode)..."
    run_parallel_limited "$MAX_PARALLEL" \
        "generate_bp_jsonl --full" \
        "generate_bs_jsonl --full" \
        "generate_sra_jsonl --full" \
        "generate_jga_jsonl"
}

# ============================================================
# PHASE 3: Elasticsearch Operations
# ============================================================
phase3_elasticsearch() {
    log_section "PHASE 3: Elasticsearch Operations"

    if [[ "$SKIP_ES" == true ]]; then
        log_info "Skipping Elasticsearch operations (--skip-es)"
        return
    fi

    log_info "Step 3-1: Creating Elasticsearch indexes..."
    run_cmd "es_create_index --index all"

    log_info "Step 3-2: Bulk inserting documents..."

    local bp_dir="${RESULT_DIR}/bioproject/jsonl/${DATE_STR}"
    local bs_dir="${RESULT_DIR}/biosample/jsonl/${DATE_STR}"
    local sra_dir="${RESULT_DIR}/sra/jsonl/${DATE_STR}"
    local jga_dir="${RESULT_DIR}/jga/jsonl/${DATE_STR}"

    run_cmd "es_bulk_insert --index bioproject --dir ${bp_dir}"
    run_cmd "es_bulk_insert --index biosample --dir ${bs_dir}"
    run_cmd "es_bulk_insert --index sra-submission --dir ${sra_dir}"
    run_cmd "es_bulk_insert --index sra-study --dir ${sra_dir}"
    run_cmd "es_bulk_insert --index sra-experiment --dir ${sra_dir}"
    run_cmd "es_bulk_insert --index sra-run --dir ${sra_dir}"
    run_cmd "es_bulk_insert --index sra-sample --dir ${sra_dir}"
    run_cmd "es_bulk_insert --index sra-analysis --dir ${sra_dir}"
    run_cmd "es_bulk_insert --index jga-study --dir ${jga_dir}"
    run_cmd "es_bulk_insert --index jga-dataset --dir ${jga_dir}"
    run_cmd "es_bulk_insert --index jga-dac --dir ${jga_dir}"
    run_cmd "es_bulk_insert --index jga-policy --dir ${jga_dir}"
}

# ============================================================
# Main
# ============================================================
main() {
    log_section "DDBJ Search Converter - Full Pipeline"

    # Script options
    log_info "Date: ${DATE_STR}"
    log_info "Max parallel jobs: ${MAX_PARALLEL}"
    log_info "Skip DBLink: ${SKIP_DBLINK}"
    log_info "Skip tar sync: ${SKIP_TAR_SYNC}"
    log_info "Skip ES: ${SKIP_ES}"
    log_info "Dry run: ${DRY_RUN}"

    # Environment variables
    log_info "Environment:"
    log_info "  RESULT_DIR: ${RESULT_DIR}"
    log_info "  CONST_DIR: ${DDBJ_SEARCH_CONVERTER_CONST_DIR:-<default>}"
    log_info "  ES_URL: ${DDBJ_SEARCH_CONVERTER_ES_URL:-<default>}"

    local start_time
    start_time=$(date +%s)

    phase1_dblink
    phase2_jsonl
    phase3_elasticsearch

    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))

    log_section "Pipeline Complete"
    log_info "Total duration: $((duration / 3600))h $(((duration % 3600) / 60))m $((duration % 60))s"
    log_info "Use 'show_log_summary' to check detailed results"
}

main

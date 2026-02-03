#!/bin/bash
#
# Pipeline: Build DBLink, generate JSONL, and populate ES indexes.
#
# Usage:
#   ./run_pipeline.sh [options]
#
# Options:
#   --date YYYYMMDD     Target date (default: today)
#   --full              Full mode: regenerate all JSONL (default: incremental)
#   --from-step STEP    Start from specified step (use --list-steps to see available steps)
#   --list-steps        Show available steps and exit
#   --dry-run           Show what would be done without executing
#   --parallel N        Max parallel jobs for JSONL generation (default: 4)
#   --clean-es          Delete all ES indexes before bulk insert (idempotent)
#
# Environment variables (optional):
#   DDBJ_SEARCH_CONVERTER_RESULT_DIR    Result directory
#   DDBJ_SEARCH_CONVERTER_CONST_DIR     Constant files directory
#   DDBJ_SEARCH_CONVERTER_ES_URL        Elasticsearch URL
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Step definitions (order matters)
STEP_NAMES=(
    "check_resources"
    "prepare"
    "init_dblink"
    "dblink_bp_bs"
    "dblink_bp"
    "dblink_assembly"
    "dblink_gea"
    "dblink_metabobank"
    "dblink_jga"
    "dblink_sra"
    "finalize_dblink"
    "dump_dblink"
    "sync_tar"
    "jsonl_bp"
    "jsonl_bs"
    "jsonl_sra"
    "jsonl_jga"
    "es_create"
    "es_bulk"
    "es_delete_blacklist"
)

declare -A STEP_ORDER
for i in "${!STEP_NAMES[@]}"; do
    STEP_ORDER["${STEP_NAMES[$i]}"]=$((i + 1))
done

declare -A STEP_DESC=(
    ["check_resources"]="Check external resources availability"
    ["prepare"]="Prepare XML files and build accessions DB"
    ["init_dblink"]="Initialize DBLink database"
    ["dblink_bp_bs"]="Create BioProject-BioSample relations"
    ["dblink_bp"]="Create BioProject relations"
    ["dblink_assembly"]="Create Assembly and Master relations"
    ["dblink_gea"]="Create GEA relations"
    ["dblink_metabobank"]="Create MetaboBank relations"
    ["dblink_jga"]="Create JGA relations"
    ["dblink_sra"]="Create SRA internal relations"
    ["finalize_dblink"]="Finalize DBLink database"
    ["dump_dblink"]="Dump DBLink files"
    ["sync_tar"]="Sync tar files and build date cache"
    ["jsonl_bp"]="Generate BioProject JSONL"
    ["jsonl_bs"]="Generate BioSample JSONL"
    ["jsonl_sra"]="Generate SRA JSONL"
    ["jsonl_jga"]="Generate JGA JSONL"
    ["es_create"]="Create Elasticsearch indexes"
    ["es_bulk"]="Bulk insert to Elasticsearch"
    ["es_delete_blacklist"]="Delete blacklisted documents from Elasticsearch"
)

declare -A STEP_PHASE=(
    ["check_resources"]="PHASE 0: Pre-check"
    ["prepare"]="PHASE 1: DBLink Construction"
    ["init_dblink"]="PHASE 1: DBLink Construction"
    ["dblink_bp_bs"]="PHASE 1: DBLink Construction"
    ["dblink_bp"]="PHASE 1: DBLink Construction"
    ["dblink_assembly"]="PHASE 1: DBLink Construction"
    ["dblink_gea"]="PHASE 1: DBLink Construction"
    ["dblink_metabobank"]="PHASE 1: DBLink Construction"
    ["dblink_jga"]="PHASE 1: DBLink Construction"
    ["dblink_sra"]="PHASE 1: DBLink Construction"
    ["finalize_dblink"]="PHASE 1: DBLink Construction"
    ["dump_dblink"]="PHASE 1: DBLink Construction"
    ["sync_tar"]="PHASE 2: JSONL Generation"
    ["jsonl_bp"]="PHASE 2: JSONL Generation"
    ["jsonl_bs"]="PHASE 2: JSONL Generation"
    ["jsonl_sra"]="PHASE 2: JSONL Generation"
    ["jsonl_jga"]="PHASE 2: JSONL Generation"
    ["es_create"]="PHASE 3: Elasticsearch"
    ["es_bulk"]="PHASE 3: Elasticsearch"
    ["es_delete_blacklist"]="PHASE 3: Elasticsearch"
)

# Default values
TARGET_DATE=""
FULL_MODE=false
DRY_RUN=false
MAX_PARALLEL=4
FROM_STEP=""
FROM_STEP_ORDER=0
CLEAN_ES=false

# Show available steps
show_steps() {
    echo "Available steps:"
    echo ""
    local current_phase=""
    for step in "${STEP_NAMES[@]}"; do
        local phase="${STEP_PHASE[$step]}"
        if [[ "$phase" != "$current_phase" ]]; then
            echo ""
            echo "=== ${phase} ==="
            current_phase="$phase"
        fi
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

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --date)
            TARGET_DATE="$2"
            shift 2
            ;;
        --full)
            FULL_MODE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --clean-es)
            CLEAN_ES=true
            shift
            ;;
        --parallel)
            MAX_PARALLEL="$2"
            shift 2
            ;;
        --from-step)
            FROM_STEP="$2"
            shift 2
            ;;
        --list-steps)
            show_steps
            exit 0
            ;;
        -h|--help)
            head -20 "$0" | tail -n +2 | sed 's/^# \?//'
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

# Set date environment variable (always export to ensure consistency across subprocesses)
if [[ -n "$TARGET_DATE" ]]; then
    export DDBJ_SEARCH_CONVERTER_DATE="$TARGET_DATE"
else
    export DDBJ_SEARCH_CONVERTER_DATE="${DDBJ_SEARCH_CONVERTER_DATE:-$(date '+%Y%m%d')}"
fi

DATE_STR="$DDBJ_SEARCH_CONVERTER_DATE"
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
# PHASE 0: Pre-check
# ============================================================
phase0_check() {
    log_section "PHASE 0: Pre-check"

    # Step: check_resources
    if should_skip_step "check_resources"; then
        log_info "[SKIP] check_resources (--from-step)"
    else
        log_info "Step 0-1: Checking external resources..."
        run_cmd "check_external_resources"
    fi
}

# ============================================================
# PHASE 1: DBLink Construction
# ============================================================
phase1_dblink() {
    log_section "PHASE 1: DBLink Construction"

    # Step: prepare
    if should_skip_step "prepare"; then
        log_info "[SKIP] prepare (--from-step)"
    else
        log_info "Step 1-1: Preparing XML files and accessions DB..."
        run_parallel \
            "prepare_bioproject_xml" \
            "prepare_biosample_xml" \
            "build_sra_and_dra_accessions_db"
    fi

    # Step: init_dblink
    if should_skip_step "init_dblink"; then
        log_info "[SKIP] init_dblink (--from-step)"
    else
        log_info "Step 1-2: Initializing DBLink DB..."
        run_cmd "init_dblink_db"
    fi

    log_info "Step 1-3: Creating DBLink relations..."
    # NOTE: These must run sequentially because DuckDB only supports single writer

    # Step: dblink_bp_bs
    if should_skip_step "dblink_bp_bs"; then
        log_info "[SKIP] dblink_bp_bs (--from-step)"
    else
        run_cmd "create_dblink_bp_bs_relations"
    fi

    # Step: dblink_bp
    if should_skip_step "dblink_bp"; then
        log_info "[SKIP] dblink_bp (--from-step)"
    else
        run_cmd "create_dblink_bp_relations"
    fi

    # Step: dblink_assembly
    if should_skip_step "dblink_assembly"; then
        log_info "[SKIP] dblink_assembly (--from-step)"
    else
        run_cmd "create_dblink_assembly_and_master_relations"
    fi

    # Step: dblink_gea
    if should_skip_step "dblink_gea"; then
        log_info "[SKIP] dblink_gea (--from-step)"
    else
        run_cmd "create_dblink_gea_relations"
    fi

    # Step: dblink_metabobank
    if should_skip_step "dblink_metabobank"; then
        log_info "[SKIP] dblink_metabobank (--from-step)"
    else
        run_cmd "create_dblink_metabobank_relations"
    fi

    # Step: dblink_jga
    if should_skip_step "dblink_jga"; then
        log_info "[SKIP] dblink_jga (--from-step)"
    else
        run_cmd "create_dblink_jga_relations"
    fi

    # Step: dblink_sra
    if should_skip_step "dblink_sra"; then
        log_info "[SKIP] dblink_sra (--from-step)"
    else
        run_cmd "create_dblink_sra_internal_relations"
    fi

    # Step: finalize_dblink
    if should_skip_step "finalize_dblink"; then
        log_info "[SKIP] finalize_dblink (--from-step)"
    else
        log_info "Step 1-4: Finalizing DBLink DB..."
        run_cmd "finalize_dblink_db"
    fi

    # Step: dump_dblink
    if should_skip_step "dump_dblink"; then
        log_info "[SKIP] dump_dblink (--from-step)"
    else
        log_info "Step 1-5: Dumping DBLink files..."
        run_cmd "dump_dblink_files"
    fi
}

# ============================================================
# PHASE 2: JSONL Generation
# ============================================================
phase2_jsonl() {
    log_section "PHASE 2: JSONL Generation"

    # Step: sync_tar
    if should_skip_step "sync_tar"; then
        log_info "[SKIP] sync_tar (--from-step)"
    else
        log_info "Step 2-1: Syncing tar files and building date cache..."
        run_parallel \
            "sync_ncbi_tar" \
            "sync_dra_tar" \
            "build_bp_bs_date_cache"
    fi

    # Determine JSONL generation mode
    local full_opt=""
    if [[ "$FULL_MODE" == true ]]; then
        log_info "Step 2-2: Generating JSONL files (full mode)..."
        full_opt="--full"
    else
        log_info "Step 2-2: Generating JSONL files (incremental mode)..."
    fi

    # Collect non-skipped JSONL commands
    local jsonl_cmds=()
    local parallel_opt="--parallel-num ${MAX_PARALLEL}"

    # bp/bs: --resume is always enabled (skip existing JSONL files)
    if ! should_skip_step "jsonl_bp"; then
        jsonl_cmds+=("generate_bp_jsonl ${full_opt} ${parallel_opt} --resume")
    else
        log_info "[SKIP] jsonl_bp (--from-step)"
    fi

    if ! should_skip_step "jsonl_bs"; then
        jsonl_cmds+=("generate_bs_jsonl ${full_opt} ${parallel_opt} --resume")
    else
        log_info "[SKIP] jsonl_bs (--from-step)"
    fi

    # sra/jga: no --resume (sra doesn't support it, jga is fast enough)
    if ! should_skip_step "jsonl_sra"; then
        jsonl_cmds+=("generate_sra_jsonl ${full_opt} ${parallel_opt}")
    else
        log_info "[SKIP] jsonl_sra (--from-step)"
    fi

    if ! should_skip_step "jsonl_jga"; then
        jsonl_cmds+=("generate_jga_jsonl")
    else
        log_info "[SKIP] jsonl_jga (--from-step)"
    fi

    if [[ ${#jsonl_cmds[@]} -gt 0 ]]; then
        # Run JSONL generation sequentially to avoid resource contention
        for cmd in "${jsonl_cmds[@]}"; do
            run_cmd "$cmd"
        done
    fi
}

# ============================================================
# PHASE 3: Elasticsearch Operations
# ============================================================
phase3_elasticsearch() {
    log_section "PHASE 3: Elasticsearch Operations"

    # Step: es_create
    if should_skip_step "es_create"; then
        log_info "[SKIP] es_create (--from-step)"
        if [[ "$CLEAN_ES" == true ]]; then
            log_info "Note: --clean-es has no effect because es_create step is skipped"
        fi
    else
        if [[ "$CLEAN_ES" == true ]]; then
            log_info "Step 3-0: Deleting existing ES indexes (--clean-es)..."
            run_cmd "es_delete_index --index all --skip-missing --force"
        fi

        log_info "Step 3-1: Creating Elasticsearch indexes..."
        run_cmd "es_create_index --index all --skip-existing"
    fi

    # Step: es_bulk
    if should_skip_step "es_bulk"; then
        log_info "[SKIP] es_bulk (--from-step)"
    else
        log_info "Step 3-2: Bulk inserting documents..."

        local bp_dir="${RESULT_DIR}/bioproject/jsonl/${DATE_STR}"
        local bs_dir="${RESULT_DIR}/biosample/jsonl/${DATE_STR}"
        local sra_dir="${RESULT_DIR}/sra/jsonl/${DATE_STR}"
        local jga_dir="${RESULT_DIR}/jga/jsonl/${DATE_STR}"

        run_cmd "es_bulk_insert --index bioproject --dir ${bp_dir}"
        run_cmd "es_bulk_insert --index biosample --dir ${bs_dir}"
        run_cmd "es_bulk_insert --index sra-submission --dir ${sra_dir} --pattern '*_submission_*.jsonl'"
        run_cmd "es_bulk_insert --index sra-study --dir ${sra_dir} --pattern '*_study_*.jsonl'"
        run_cmd "es_bulk_insert --index sra-experiment --dir ${sra_dir} --pattern '*_experiment_*.jsonl'"
        run_cmd "es_bulk_insert --index sra-run --dir ${sra_dir} --pattern '*_run_*.jsonl'"
        run_cmd "es_bulk_insert --index sra-sample --dir ${sra_dir} --pattern '*_sample_*.jsonl'"
        run_cmd "es_bulk_insert --index sra-analysis --dir ${sra_dir} --pattern '*_analysis_*.jsonl'"
        run_cmd "es_bulk_insert --index jga-study --dir ${jga_dir}"
        run_cmd "es_bulk_insert --index jga-dataset --dir ${jga_dir}"
        run_cmd "es_bulk_insert --index jga-dac --dir ${jga_dir}"
        run_cmd "es_bulk_insert --index jga-policy --dir ${jga_dir}"
    fi

    # Step: es_delete_blacklist
    if should_skip_step "es_delete_blacklist"; then
        log_info "[SKIP] es_delete_blacklist (--from-step)"
    else
        log_info "Step 3-3: Deleting blacklisted documents..."
        run_cmd "es_delete_blacklist --force"
    fi
}

# ============================================================
# Main
# ============================================================
main() {
    log_section "DDBJ Search Converter - Pipeline"

    # Script options
    log_info "Date: ${DATE_STR}"
    log_info "Mode: $(if [[ "$FULL_MODE" == true ]]; then echo "full"; else echo "incremental"; fi)"
    log_info "Max parallel jobs: ${MAX_PARALLEL}"
    log_info "Dry run: ${DRY_RUN}"
    log_info "Clean ES: ${CLEAN_ES}"
    if [[ -n "$FROM_STEP" ]]; then
        log_info "From step: ${FROM_STEP}"
    fi

    # Environment variables
    log_info "Environment:"
    log_info "  RESULT_DIR: ${RESULT_DIR}"
    log_info "  CONST_DIR: ${DDBJ_SEARCH_CONVERTER_CONST_DIR:-<default>}"
    log_info "  ES_URL: ${DDBJ_SEARCH_CONVERTER_ES_URL:-<default>}"

    local start_time
    start_time=$(date +%s)

    phase0_check
    phase1_dblink
    phase2_jsonl
    phase3_elasticsearch

    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))

    log_section "Pipeline Complete"
    log_info "Total duration: $((duration / 3600))h $(((duration % 3600) / 60))m $((duration % 60))s"

    log_section "Log Summary"
    if [[ "$DRY_RUN" == false ]]; then
        show_log_summary --raw
    fi
}

main

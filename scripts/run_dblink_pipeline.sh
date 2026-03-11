#!/bin/bash
#
# Pipeline: Build DBLink database only (Phase 1 of run_pipeline.sh).
#
# Usage:
#   ./run_dblink_pipeline.sh [options]
#
# Options:
#   --date YYYYMMDD     Target date (default: today)
#   --from-step STEP    Start from specified step (use --list-steps to see available steps)
#   --list-steps        Show available steps and exit
#   --dry-run           Show what would be done without executing
#
# Environment variables (optional):
#   DDBJ_SEARCH_CONVERTER_RESULT_DIR    Result directory
#   DDBJ_SEARCH_CONVERTER_CONST_DIR     Constant files directory
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
    "dblink_insdc"
    "finalize_dblink"
    "dump_dblink"
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
    ["dblink_insdc"]="Create INSDC sequence accession relations"
    ["finalize_dblink"]="Finalize DBLink database"
    ["dump_dblink"]="Dump DBLink files"
)

# Default values
TARGET_DATE=""
DRY_RUN=false
FROM_STEP=""
FROM_STEP_ORDER=0

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
        -h|--help)
            head -17 "$0" | tail -n +2 | sed 's/^# \?//'
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
        log_info "Running: $cmd_name"
        if eval "$cmd" > /dev/null 2>&1; then
            log_info "  ✓ ${cmd_name}"
        else
            log_error "  ✗ ${cmd_name} (use show_log for details)"
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

# ============================================================
# Main
# ============================================================
main() {
    log_section "DDBJ Search Converter - DBLink Pipeline"

    log_info "Date: ${DATE_STR}"
    log_info "Dry run: ${DRY_RUN}"
    if [[ -n "$FROM_STEP" ]]; then
        log_info "From step: ${FROM_STEP}"
    fi

    log_info "Environment:"
    log_info "  RESULT_DIR: ${RESULT_DIR}"
    log_info "  CONST_DIR: ${DDBJ_SEARCH_CONVERTER_CONST_DIR:-<default>}"

    local start_time
    start_time=$(date +%s)

    # Step: check_resources
    if should_skip_step "check_resources"; then
        log_info "[SKIP] check_resources (--from-step)"
    else
        log_info "Step 1: Checking external resources..."
        run_cmd "check_external_resources"
    fi

    # Step: prepare
    if should_skip_step "prepare"; then
        log_info "[SKIP] prepare (--from-step)"
    else
        log_info "Step 2: Preparing XML files and accessions DB..."
        run_parallel \
            "prepare_bioproject_xml" \
            "prepare_biosample_xml" \
            "build_sra_and_dra_accessions_db"
    fi

    # Step: init_dblink
    if should_skip_step "init_dblink"; then
        log_info "[SKIP] init_dblink (--from-step)"
    else
        log_info "Step 3: Initializing DBLink DB..."
        run_cmd "init_dblink_db"
    fi

    log_info "Step 4: Creating DBLink relations..."
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

    # Step: dblink_insdc
    if should_skip_step "dblink_insdc"; then
        log_info "[SKIP] dblink_insdc (--from-step)"
    else
        run_cmd "create_dblink_insdc_relations"
    fi

    # Step: finalize_dblink
    if should_skip_step "finalize_dblink"; then
        log_info "[SKIP] finalize_dblink (--from-step)"
    else
        log_info "Step 5: Finalizing DBLink DB..."
        run_cmd "finalize_dblink_db"
    fi

    # Step: dump_dblink
    if should_skip_step "dump_dblink"; then
        log_info "[SKIP] dump_dblink (--from-step)"
    else
        log_info "Step 6: Dumping DBLink files..."
        run_cmd "dump_dblink_files"
    fi

    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))

    log_section "DBLink Pipeline Complete"
    log_info "Total duration: $((duration / 3600))h $(((duration % 3600) / 60))m $((duration % 60))s"

    log_section "Log Summary"
    if [[ "$DRY_RUN" == false ]]; then
        show_log_summary --raw
    fi
}

main

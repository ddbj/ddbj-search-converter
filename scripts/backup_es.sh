#!/bin/bash
#
# Elasticsearch backup script for cron scheduling.
#
# Usage:
#   ./backup_es.sh [options]
#
# Options:
#   --repo NAME         Repository name (default: backup)
#   --es-url URL        Elasticsearch URL (default: http://localhost:9200)
#   --retention DAYS    Number of days to retain snapshots (default: 7)
#   --dry-run           Show what would be done without executing
#
# Example crontab entry (daily at 2:00 AM):
#   0 2 * * * /path/to/backup_es.sh --repo backup --retention 7 >> /var/log/es_backup.log 2>&1
#

set -euo pipefail

# Default values
REPO_NAME="backup"
ES_URL="http://localhost:9200"
RETENTION_DAYS=7
DRY_RUN=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --repo)
            REPO_NAME="$2"
            shift 2
            ;;
        --es-url)
            ES_URL="$2"
            shift 2
            ;;
        --retention)
            RETENTION_DAYS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Logging functions
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARN: $*"
}

# Check if ES is reachable
check_es_health() {
    local health
    health=$(curl -s -X GET "${ES_URL}/_cluster/health" | jq -r '.status' 2>/dev/null)
    if [[ -z "$health" || "$health" == "null" ]]; then
        log_error "Cannot connect to Elasticsearch at ${ES_URL}"
        exit 1
    fi
    log_info "Cluster health: ${health}"
    if [[ "$health" == "red" ]]; then
        log_warn "Cluster is RED - backup may be incomplete"
    fi
}

# Check if repository exists
check_repository() {
    local repo_info
    repo_info=$(curl -s -X GET "${ES_URL}/_snapshot/${REPO_NAME}" 2>/dev/null)
    if echo "$repo_info" | jq -e '.error' > /dev/null 2>&1; then
        log_error "Repository '${REPO_NAME}' does not exist"
        log_info "Create it with: es_snapshot repo register --name ${REPO_NAME} --path /path/to/backup"
        exit 1
    fi
    log_info "Using repository: ${REPO_NAME}"
}

# Create snapshot
create_snapshot() {
    local snapshot_name
    snapshot_name="ddbj_search_$(date '+%Y%m%d_%H%M%S')"

    log_info "Creating snapshot: ${snapshot_name}"

    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY-RUN] Would create snapshot: ${snapshot_name}"
        return
    fi

    local response
    response=$(curl -s -X PUT "${ES_URL}/_snapshot/${REPO_NAME}/${snapshot_name}?wait_for_completion=true" \
        -H 'Content-Type: application/json' \
        -d '{
            "indices": "bioproject,biosample,sra-*,jga-*",
            "include_global_state": false,
            "metadata": {
                "created_by": "backup_es.sh",
                "created_at": "'"$(date -Iseconds)"'"
            }
        }')

    local state
    state=$(echo "$response" | jq -r '.snapshot.state' 2>/dev/null)

    if [[ "$state" == "SUCCESS" ]]; then
        log_info "Snapshot created successfully: ${snapshot_name}"
        local duration
        duration=$(echo "$response" | jq -r '.snapshot.duration_in_millis' 2>/dev/null)
        log_info "Duration: $((duration / 1000)) seconds"
    else
        log_error "Snapshot creation failed"
        echo "$response" | jq . 2>/dev/null || echo "$response"
        exit 1
    fi
}

# Delete old snapshots
cleanup_old_snapshots() {
    log_info "Cleaning up snapshots older than ${RETENTION_DAYS} days"

    local snapshots
    snapshots=$(curl -s -X GET "${ES_URL}/_snapshot/${REPO_NAME}/_all" | jq -r '.snapshots[].snapshot' 2>/dev/null)

    if [[ -z "$snapshots" ]]; then
        log_info "No snapshots found"
        return
    fi

    local cutoff_date
    cutoff_date=$(date -d "-${RETENTION_DAYS} days" '+%Y%m%d')

    local deleted_count=0
    while IFS= read -r snapshot; do
        # Extract date from snapshot name (format: ddbj_search_YYYYMMDD_HHMMSS)
        local snapshot_date
        snapshot_date=$(echo "$snapshot" | sed -n 's/ddbj_search_\([0-9]\{8\}\)_.*/\1/p')

        if [[ -z "$snapshot_date" ]]; then
            log_warn "Skipping snapshot with unrecognized format: ${snapshot}"
            continue
        fi

        if [[ "$snapshot_date" < "$cutoff_date" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                log_info "[DRY-RUN] Would delete old snapshot: ${snapshot}"
            else
                log_info "Deleting old snapshot: ${snapshot}"
                curl -s -X DELETE "${ES_URL}/_snapshot/${REPO_NAME}/${snapshot}" > /dev/null
                ((deleted_count++))
            fi
        fi
    done <<< "$snapshots"

    if [[ "$DRY_RUN" == false ]]; then
        log_info "Deleted ${deleted_count} old snapshot(s)"
    fi
}

# Main
main() {
    log_info "=== Starting Elasticsearch backup ==="
    log_info "Repository: ${REPO_NAME}"
    log_info "ES URL: ${ES_URL}"
    log_info "Retention: ${RETENTION_DAYS} days"

    if [[ "$DRY_RUN" == true ]]; then
        log_info "DRY-RUN mode enabled"
    fi

    check_es_health
    check_repository
    create_snapshot
    cleanup_old_snapshots

    log_info "=== Backup completed ==="
}

main

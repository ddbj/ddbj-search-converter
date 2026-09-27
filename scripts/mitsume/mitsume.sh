#!/bin/bash
#
# Run mitsume with the production monitoring config next to this script.
#
# Usage:
#   ./mitsume.sh check [--dry-run]
#   ./mitsume.sh ping ddbj-search-daily
#
# Files outside the repository:
#   ~/.local/bin/mitsume                  mitsume binary
#   ~/.config/mitsume/webhook.env         MITSUME_SLACK_WEBHOOK_URL=... (mode 0600)
#   ~/.local/state/mitsume/heartbeat.json heartbeat file shared by ping and check
#
# Example crontab entry (hourly):
#   0 * * * * /path/to/mitsume.sh check >> $HOME/.local/state/mitsume/check.log 2>&1
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The env file holds plain KEY=VALUE lines; `set -a` exports them to mitsume.
set -a
# shellcheck source=/dev/null
. "${HOME}/.config/mitsume/webhook.env"
set +a

export MITSUME_CONFIG="${SCRIPT_DIR}/mitsume.json"
export MITSUME_HEARTBEAT_FILE="${HOME}/.local/state/mitsume/heartbeat.json"

exec "${HOME}/.local/bin/mitsume" "$@"

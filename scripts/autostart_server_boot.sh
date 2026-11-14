#!/bin/zsh
set -euo pipefail
# Manual boot checks share the same lock, disk identity and retry budget as launchd.
# This never runs compose, database migrations, or replacement VM provisioning.
exec /opt/homebrew/bin/python3 /Users/jun/server/scripts/external_server_recovery.py "$@"

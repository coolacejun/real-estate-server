#!/bin/zsh
set -euo pipefail

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export COLIMA_HOME="/Volumes/DB_EXTERNAL/colima-target"
export DOCKER_HOST="unix:///Volumes/DB_EXTERNAL/colima-target/external-pg/docker.sock"

LOG_DIR="/Users/jun/server/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/autostart_server.log"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] autostart begin"

  COLIMA_BIN="${COLIMA_BIN:-$(command -v colima || true)}"
  DOCKER_BIN="${DOCKER_BIN:-$(command -v docker || true)}"
  if [[ -z "$COLIMA_BIN" ]]; then COLIMA_BIN="/opt/homebrew/bin/colima"; fi
  if [[ -z "$DOCKER_BIN" ]]; then DOCKER_BIN="/opt/homebrew/bin/docker"; fi

  # Never create a replacement VM/database when the external disk is unavailable.
  for i in {1..120}; do
    if [[ -f "$COLIMA_HOME/external-pg/colima.yaml" && -d "$COLIMA_HOME/_lima/colima-external-pg" ]]; then
      break
    fi
    sleep 2
  done
  if [[ ! -f "$COLIMA_HOME/external-pg/colima.yaml" || ! -d "$COLIMA_HOME/_lima/colima-external-pg" ]]; then
    echo "Existing external-pg profile unavailable; refusing fallback"
    exit 1
  fi

  PROFILE_STATE="$($COLIMA_BIN list 2>/dev/null | awk 'NR>1 && $1=="external-pg" {print $2}')"
  if [[ "$PROFILE_STATE" != "Running" ]]; then
    echo "Starting colima (state=${PROFILE_STATE:-unknown})"
    "$COLIMA_BIN" start external-pg --activate=false
  else
    echo "Colima already running"
  fi

  READY=0
  for i in {1..120}; do
    if "$DOCKER_BIN" info >/dev/null 2>&1; then
      READY=1
      break
    fi
    sleep 2
  done

  if [[ "$READY" -ne 1 ]]; then
    echo "Docker daemon not ready after wait"
    exit 1
  fi

  cd /Users/jun/server
  # Boot the deployed containers; do not recreate them from an uncommitted config.
  "$DOCKER_BIN" start postgres redis api web traefik cadastral-sync building-hub-sync land-movement-sync building-integrated-sync
  "$DOCKER_BIN" ps

  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] autostart done"
} >>"$LOG_FILE" 2>&1

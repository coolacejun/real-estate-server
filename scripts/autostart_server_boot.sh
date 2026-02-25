#!/bin/zsh
set -euo pipefail

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

LOG_DIR="/Users/jun/server/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/autostart_server.log"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] autostart begin"

  COLIMA_BIN="${COLIMA_BIN:-$(command -v colima || true)}"
  DOCKER_BIN="${DOCKER_BIN:-$(command -v docker || true)}"
  if [[ -z "$COLIMA_BIN" ]]; then COLIMA_BIN="/opt/homebrew/bin/colima"; fi
  if [[ -z "$DOCKER_BIN" ]]; then DOCKER_BIN="/opt/homebrew/bin/docker"; fi

  PROFILE_STATE="$($COLIMA_BIN list 2>/dev/null | awk 'NR>1 && $1=="default" {print $2}')"
  if [[ "$PROFILE_STATE" != "Running" ]]; then
    echo "Starting colima (state=${PROFILE_STATE:-unknown})"
    "$COLIMA_BIN" start --cpu 4 --memory 8 --disk 350 --runtime docker
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
  "$DOCKER_BIN" compose up -d
  "$DOCKER_BIN" compose ps

  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] autostart done"
} >>"$LOG_FILE" 2>&1

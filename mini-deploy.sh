#!/bin/zsh
set -euo pipefail

MINI="jun@192.168.0.81"
KEY="/Users/jun/8686"
SRC="/Users/jun/server/"
DST="/Users/jun/server/"
SSH_OPTS=(-i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
RSYNC_SSH="ssh -i $KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new"

rsync -aH --partial --progress -e "$RSYNC_SSH" "$SRC" "$MINI:$DST"
ssh "${SSH_OPTS[@]}" "$MINI" '/bin/zsh -lc "
if ! command -v docker >/dev/null 2>&1; then
  echo \"[ERROR] docker not found on mac mini. Install Docker Desktop and launch it once.\"
  exit 127
fi
cd /Users/jun/server
docker compose up -d --build
docker compose ps
"'

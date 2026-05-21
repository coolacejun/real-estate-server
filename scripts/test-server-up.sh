#!/bin/zsh
set -euo pipefail

cd /Users/jun/server
/opt/homebrew/bin/docker compose --env-file /Users/jun/server/.env.test -p server_test -f /Users/jun/server/docker-compose.test.yml up -d --build
/opt/homebrew/bin/docker compose --env-file /Users/jun/server/.env.test -p server_test -f /Users/jun/server/docker-compose.test.yml ps

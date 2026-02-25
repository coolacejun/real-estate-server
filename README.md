# server

Mac mini deployment workspace for API + Postgres + Redis + Traefik.

## Structure
- `api/`: FastAPI app and Dockerfile
- `scripts/`: data import and operational scripts
- `db/`: bootstrap SQL migrations
- `docs/`: operational docs
- `traefik/`: reverse-proxy config
- `docker-compose.yml`: service orchestration

## Quick start
1. Copy `.env.example` to `.env` and set secure values.
2. Run `docker compose -f docker-compose.yml up -d --build`.
3. Check services with `docker compose -f docker-compose.yml ps`.

## Security
- Do not commit `.env`, datasets, logs, backups, or local virtual environments.

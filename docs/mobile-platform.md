# Shared account, credit, store and report platform

## Authority and invariants

PostgreSQL in the FastAPI service is the only production authority for shared users and report credits. The web SQLite database remains responsible for browser sessions, payments and legacy archive HTML/JSON, but it does not update a production credit balance when `PLATFORM_API_BASE_URL` is configured.

- A newly created `platform_users` row starts with `free_remaining = 3` and `paid_remaining = 0`.
- The matching `initial_account_grant` ledger entry has the unique key `initial-free:{user_id}`. Repeated OAuth callbacks, web-account resolution and explicit identity linking cannot grant it again.
- `free_remaining` has no expiry column or expiry job. Final report generation debits free first and paid second.
- `availableCredits` is always `freeRemaining + paidRemaining`; clients must not reinterpret paid credits as free credits.
- Email is profile data only. Accounts are resolved by provider plus provider subject, by explicit authenticated linking, or by the stable web namespace/external ID.
- Credit mutations and report reservations use unique idempotency keys plus PostgreSQL row/advisory locks.

Migration `db/009_mobile_platform.sql` creates the shared schema. Migration `db/010_mobile_auth_hardening.sql` adds hashed OIDC nonce state, provider email-verification metadata and credential-free authentication audit events. API traffic fails closed with 503 until the 010 marker exists.

## Public contracts

The checked-in OpenAPI contract is `api/mobile-openapi.yaml`. The canonical report schema is `api/contracts/canonical-report-v1.schema.json`; renderer tokens and golden inputs live beside it.

Authentication:

- `POST /api/mobile/v1/auth/oauth/start`
- `GET /api/mobile/v1/auth/oauth/callback/{kakao|naver|google}`
- `POST /api/mobile/v1/auth/token`
- `POST /api/mobile/v1/auth/refresh`
- `POST /api/mobile/v1/auth/logout`
- `GET /api/mobile/v1/me`

OAuth uses an exact redirect allowlist and PKCE S256. Google additionally uses an OIDC nonce stored only as a SHA-256 digest; its ID token signature, issuer, audience and nonce are verified before accepting `sub`, and an email is retained only when `email_verified=true`. Kakao email is retained only when Kakao marks it valid and verified. In production the public callback base is fixed to `https://building-land.com`. The provider callback issues a short-lived, one-use server exchange code and returns provider cancellation safely to the stored app URI. Access and refresh tokens are random opaque values; only SHA-256 digests are stored. Refresh tokens rotate, and replay revokes the entire token family. To link another identity, an authenticated client calls OAuth start with `linkAccount: true`; matching email alone never links accounts.

Store:

- `GET /api/mobile/v1/store/catalog?platform=ios|android`
- `POST /api/mobile/v1/store/verify`
- `POST /api/mobile/v1/store/restore`

Each user/store pair receives a UUID account token. iOS must send it as StoreKit `appAccountToken`; Android must send it as the obfuscated external account ID. Historical `remove_ads_monthly` receipts that predate account-token support are the only exception, and their unique store transaction can be claimed by only one central account. Active consumable IDs are exactly:

- `buildingland.report_credits_10`
- `buildingland.report_credits_30`
- `buildingland.report_credits_50`

The 60- and 90-credit IDs remain visible as disabled/retired compatibility records and are rejected with 410 for new purchases. `remove_ads_monthly` is accepted only by restore as a legacy entitlement and never grants report credits. Apple verification validates the application, product, transaction, account token, revocation state and signed certificate chain (or the legacy receipt endpoint). Google Play verification uses the Android Publisher API and records the grant before a retryable consume call; the legacy non-consumable uses acknowledgement instead. Store transaction identity and receipt digest are both unique, so retries and concurrent delivery cannot grant twice.

Reports:

- `POST /api/mobile/v1/reports/preview` renders without debit or archive.
- `POST /api/mobile/v1/reports/final` reserves one credit, renders, commits canonical archive data, and returns transient PDF bytes.
- `GET /api/report-archive` lists the bearer token owner's archives.
- `GET /api/report-archive/content?id={uuid}&format=pdf|html` regenerates content.

Supported profiles are `web-a4-v1`, `ios-a4-v1`, and `android-a4-v1`. `web-a4-canonical-v1` is a temporary input/output alias for the current web/mobile payload contract and renders with `web-a4-v1`. New clients should send `rendererProfile` explicitly.

The schema/version contract is unchanged at canonical v1. Its schema permits both the legacy `sourceRows` array and the current structured object, plus opinion image strings and `{id,name,src}` objects. The production renderer accepts both current mobile payloads and already archived v1 JSON: `reportRows` become bordered field tables; building `sourceRows.floors` become a three-column floor table (with flattened `층별개요` rows as fallback); and `brokerRows` are grouped into the official ①/③/⑤ Korean sections with merged group cells and explicit checkboxes. Unknown broker or enforcement fields appear under generic additional-information labels instead of leaking internal field names. Long rows and floor tables split across A4 pages with their column header repeated. Only validated `data:image/...` and archived `asset://UUID` references are resolved; canonical input is never treated as HTML.

Mobile preview, final and archive regeneration all call the token-authenticated web-container renderer at the fixed internal URL `http://web:5180/api/internal/mobile-report-pdf`. The endpoint rejects forwarded/public callers and non-private peers, and the API accepts only the exactly allowlisted host and path without redirects or proxy use. The web service maps versioned canonical JSON into its checked-in `mobile-canonical-contract.js`, `canonical_report_bootstrap.js` and `canonical_report.css`; app-supplied HTML, JavaScript and CSS are never written or executed. Chromium runs with local files only, a deny-all CSP, blocked hostname resolution, stripped environment, bounded concurrency/queueing, render timeout and output-size limits. A response is accepted only when its PDF metadata identifies Chrome/Chromium Skia. Renderer failure is returned explicitly; final generation follows the existing failure-refund saga and never falls back to ReportLab.

The permanent report identity is the SHA-256 hash of canonical input JSON. Data-URI images are validated, content-addressed and replaced by `asset://` references under `/data/report-assets`. PostgreSQL stores canonical JSON, profile/version, content hash, metadata and asset references. It never stores PDF bytes or a permanent PDF path. Each PDF response includes its own `X-Report-Artifact-Sha256`; regenerated bytes are allowed to differ while `X-Report-Content-Hash` stays fixed.

Environment analysis:

- `POST /api/v1/environment-analysis`

The endpoint accepts WGS84 coordinates with `radiusProfile=web-v1` and `calculationVersion=environment-web-v1`. Its canonical category keys are `bus`, `rail`, `schools`, `amenities`, `parks`, `securityLights`, and `cctv`; `schools.nearest` carries the web school-level strings and the `elementary`, `middle`, and `high` summaries are always present. Web-v1 radii are 700 m for bus and amenities, 3 km for rail, 1.5 km for schools and parks, and 500 m for security lights and CCTV. The older `traffic`, `school`, `convenience`, `park`, and `streetlight` category aliases remain available. It reads only mounted authoritative snapshots. Missing categories appear as structured partial errors; the endpoint returns 503 when no category can be calculated. Expected paths under `ENVIRONMENT_DATA_DIR` are:

- `rail-stations.csv`, `subway-stations.csv`, `schools.csv`
- `bus-stops/{region}.csv`, `amenities/{region}.csv`, `parks/{region}.csv`
- `security-lights.json`, `cctv.json`

## Web adapter

`web/migrations/001_platform_ledger_bridge.sql` creates local mapping tables. On first authenticated central-account use, the web service resolves the local user through the internal API and imports the legacy paid balance with `web-balance-migration:{local_user_id}`. This is idempotent and does not merge users by email.

Payments grant credits centrally. PDF generation uses a central reserve/complete/fail saga; a failed or stale render refunds the same bucket. Payment cancellation uses prepare/provider-cancel/complete and rolls back the central reservation if the provider rejects cancellation. New web PDF responses are always regenerated and never written to `.pdf-export`.

The internal endpoints require `X-Internal-Service-Token`. They are intended only for container-network traffic and are not routed through Traefik.

## Limits and operational visibility

The API applies an in-process IP safety limit to OAuth, store, report and environment calls. Keep an edge or Redis-backed distributed rate limit enabled when running more than one API worker. Request body defaults are 64 KiB for auth, 3 MiB for store, 64 MiB for reports, 256 KiB for environment/internal calls, 6 MiB per image and eight images per report. Adjust the report limits only through the documented environment variables.

Store/report/account tables are the audit trail: ledger reason/idempotency/reference columns, store events, report attempts and reversal state are persisted without raw bearer tokens, OAuth codes, receipts or service credentials. `mobile_auth_events` stores event type, account/family references, hashed device ID and non-sensitive reason metadata for OAuth exchange, refresh rotation/replay and logout. Runtime renderer failures log only the usage UUID and stack trace. Do not add provider payloads, raw device IDs or authorization headers to logs.

## External console setup

Create secrets outside Git and fill `.env` from `.env.example`.

1. Register these three HTTPS callbacks exactly: Kakao `https://building-land.com/api/mobile/v1/auth/oauth/callback/kakao`, Naver `https://building-land.com/api/mobile/v1/auth/oauth/callback/naver`, and Google `https://building-land.com/api/mobile/v1/auth/oauth/callback/google`. Enable only the documented email/profile scopes and Google OpenID Connect. Kakao/Naver reuse `KAKAO_CLIENT_ID`/`KAKAO_CLIENT_SECRET` and `NAVER_CLIENT_ID`/`NAVER_CLIENT_SECRET` unless a mobile-only `*_OAUTH_*` override is set. Add `buildingland://oauth/callback` to `MOBILE_OAUTH_REDIRECT_ALLOWLIST`; every value is matched exactly.
2. In App Store Connect, configure the five exact product IDs above, leaving 60/90 unavailable for sale. Set `APPLE_BUNDLE_ID`; mount Apple's trusted root certificate PEM through `APPLE_ROOT_CA_HOST_FILE`. Set `APPLE_SHARED_SECRET` only if legacy receipt restore requires it.
3. In Google Play Console, configure the same five exact product IDs, leaving 60/90 inactive. Grant the service account only the Android Publisher permissions needed to read and consume purchases, then mount its JSON through `GOOGLE_PLAY_SERVICE_ACCOUNT_HOST_FILE`. Set `GOOGLE_PLAY_PACKAGE_NAME`.
4. Generate a high-entropy `PLATFORM_INTERNAL_SERVICE_TOKEN` and set the identical value for API and web containers. It authenticates both the account bridge and the internal canonical PDF renderer. Do not place any real value in tracked files.
5. Populate the environment snapshot directory. Missing regional files are safe but produce explicit partial results.
6. Review and publish the updated terms/privacy notice before releasing the mobile login buttons. The app links to `https://building-land.com/terms` and `/privacy` directly.

`STORE_VERIFIER_MODE=fake` is accepted only with `APP_ENV=test`. Production starts in verification mode and fails closed when Apple/Google credentials are missing.

## Backup, deploy and rollback

Both repositories are required: `real-estate-server` at the deployment root and `real_estate_web` at its `web/` path. When the root tracks the web repository as a submodule, a clean checkout is:

```sh
git clone --recurse-submodules git@github.com:coolacejun/real-estate-server.git
cd real-estate-server
cp .env.example .env
# Fill secrets and host paths in .env outside version control.
mkdir -p backup data/report-assets data/web
docker compose up -d postgres
docker compose exec -T postgres sh -lc 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "backup/pre-mobile-platform.dump"
cp data/web/auth.sqlite3 "backup/pre-mobile-platform-auth.sqlite3"
docker compose run --rm migrate
docker compose up -d --build api web traefik
```

On a fresh installation where the SQLite file does not yet exist, skip its copy. Validate after deployment:

```sh
docker compose config --quiet
docker compose ps
curl -fsS http://127.0.0.1/health
curl -fsS -X POST http://127.0.0.1/api/v1/environment-analysis \
  -H 'Content-Type: application/json' \
  -d '{"location":{"lat":37.5,"lng":127.0,"crs":"EPSG:4326"},"address":{"parcel":"서울특별시","road":""},"radiusProfile":"web-v1","calculationVersion":"environment-web-v1"}'
```

Migrations 009 and 010 are additive. The preferred application rollback is to restore the previous API/web images while leaving the new tables intact. Do not delete ledger or audit tables as a routine rollback. If data rollback is unavoidable, enter maintenance mode, stop API/web, save the failed-state database and asset directory for audit, recreate the database from `pre-mobile-platform.dump`, restore the matching SQLite copy, and restore the matching `/data/report-assets` snapshot before restarting. Because credits can be purchased after rollout, restoring an old snapshot discards later transactions and requires explicit financial reconciliation.

## Mobile release contract

The `work/mobile-final-20260826` branch consumes `freeRemaining`, `paidRemaining`, and `availableCredits` separately, permits final output when either bucket has balance, and sends `ios-a4-v1` or `android-a4-v1` explicitly. Release builds reject non-HTTPS API origins and provider authorization URLs outside the exact Kakao, Naver or Google hosts. Build with the production defaults or explicit `--dart-define=MOBILE_API_BASE_URL=https://building-land.com --dart-define=MOBILE_OAUTH_REDIRECT_URI=buildingland://oauth/callback`; do not enable the internal admin-preview flag.

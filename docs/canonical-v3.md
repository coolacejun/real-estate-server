# Canonical V3 server integration

The API accepts `schemaVersion: 1` with `rendererVersion:
"web-a4-canonical-v3"` and the semantic v2 rollout alias. Existing FastAPI
endpoints, bearer authentication and PostgreSQL account, ledger, reservation,
refund and archive transactions are retained. No migration is needed. Preview,
final, repeated final and PDF archive regeneration all call the same internal
web renderer through `app.platform.reports.render_pdf`.

## Required renderer and rollout boundary

The reviewed web renderer is commit
`98c957b2c572d1379b75a52f07cfe61f7d155c3f`, bundle
`web-a4-shared-v3-20260909`. `api/app/platform/canonical_v3_contract.py` is the
unchanged pure validator from that commit. The existing server adapter owns
authentication, image persistence and transport. The JS, CSS, browser runner
and three OFL Pretendard fonts stay in that web bundle, avoiding duplicate
renderers/assets in the API image. Web OAuth, payments, SQLite ledgers and
frontend pages are not imported.

This branch does **not** update the `web` submodule, currently pinned at
`738df909313192baa3f4695fdca5a9e160c80204`, or production deployment settings.
That older pin is not the reviewed V3 bundle. A separately reviewed web
renderer rollout is required before deploying this server branch. There is
no renderer fallback: an unavailable or incompatible endpoint fails explicitly
and the existing final-report saga refunds the reservation. The internal
token, exact host/path allowlist, no redirects/proxies, output limit and
Chromium producer check remain in force.

## Mobile and archive contract

- Legacy profiles and canonical-v1 remain supported. An explicit profile
  cannot make an unknown declared renderer version valid.
- Semantic v2/v3 uses the shared validator's normalized JSON and SHA-256.
  Optional `contentHash` is checked on preview and final before debit. Legacy
  hashes remain unchanged. `X-Report-Renderer-Version` keeps the semantic
  version even with an iOS/Android profile.
- Typed rows, structured source records, floor tables, land-use plans, unit
  fields, broker forms, opinion text/photos and supplied enforcement snapshots
  reach the same web A4 builder. The API does not recompute reference values.
- Semantic input permits 60 pages, 12 image occurrences and 16 MiB of
  normalized JSON. Images must be embedded PNG/JPEG. Existing server byte,
  pixel and distinct-asset limits also apply. Raw page HTML/script/style,
  duplicate page keys, nonfinite numbers and unsupported versions are rejected.
- V2/V3 sends inline images, including `environmentPhoto.src`. Final retains
  content-addressed asset storage and restores image references from the
  owner's archive manifest. Legacy profiles retain `assetBundle` transport.
  Restored V3 content is hash-checked again before the internal request.
- Preview never reserves, debits, persists assets or creates an archive.
  Final uses the existing one-credit reservation and unique request ID.
  Completion persists canonical JSON, asset metadata and usage state in one
  PostgreSQL transaction; render failure uses the existing idempotent refund.
  No second ledger or PDF persistence is introduced.

`X-Report-Artifact-Sha256` hashes the response PDF bytes. The archive stores
canonical JSON and image references and regenerates PDFs, so canonical content
identity and rendered content must match, while PDF byte hashes can differ
due to regenerated Chromium metadata. Byte-identical PDF archive storage would
require a separate design and is not part of this patch.

## Environment analysis

`POST /api/v1/environment-analysis` defaults to 503 (disabled). Only the
case-insensitive value `ENVIRONMENT_ANALYSIS_ENABLED=true` enables it. No
production variables or compose mounts are changed. Existing
`ENVIRONMENT_DATA_DIR` paths, seven categories, legacy aliases, radii, source
metadata and partial-result contracts are retained.

Requests are capped at 16 KiB and rate-limited. Dataset work runs outside the
ASGI event loop with two active analyses, seven worker threads, at most 14
outstanding category jobs and an eight-second collection deadline. Timed-out
reads retain their slot until they finish. Slow categories produce
`dataset_timeout`; saturation produces `analysis_busy` or 503. Missing/invalid
datasets produce partial errors; zero successful categories still returns
503. Late workers never mutate returned results. Limits are per API process;
multiple workers still need the existing edge/distributed rate limit.

## Verification and release gates

Use a disposable PostgreSQL database with migrations 009/010 and the required
local token-authenticated web renderer. Never run the suite against production:
its setup truncates platform tables. From `api/`, run
`python -m unittest discover -s tests -q`. V3 PostgreSQL tests cover eight
concurrent reservations, repeated refunds, retry completion, one central debit
for a repeated successful final, and owner-only archives.

`api/contracts/golden/web-a4-canonical-v3.json` contains a general building,
40 floors, unit 101, land-use plans, map, environment rows/photo, opinion,
enforcement snapshot and building/land disclosure. Compare preview/restored
archive render requests and all three Chromium PDFs. Inspect every page for
A4 layout, complete tables, Korean legibility and only bundled fonts. Validate
response SHA, canonical hash, text and page count separately.

Before release: stage the reviewed web renderer; repeat checks on the intended
Linux/container image; verify existing migrations and asset storage; validate
mounted environment snapshots before explicitly enabling that endpoint. This
patch includes no deployment, production migration execution, main merge, PR,
release, tag, production secret or store/OAuth-console change.

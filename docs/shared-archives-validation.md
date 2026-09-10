# Validation record — 2026-09-11

Scope: isolated server/web/mobile branches; synthetic data only. No push,
deployment, real identity login, migration against user data, purchase or charge.

| Area | Executed result |
| --- | --- |
| Server full API regression | 76 tests; 0 failures, 0 errors, 0 skipped |
| Web bridge, renderer, V3 and account contracts | 32 distinct tests passed across full and focused runs |
| Mobile relevant contracts/widgets | 40 passed initially; 29 impacted tests passed after two new cases (42 distinct) |
| Mobile static analysis | Six changed production Dart files and new contract test: no issues |
| Web JavaScript | `node --check script.js` passed |
| Python/YAML syntax | 19 API/tool/test Python files parsed; Compose and Traefik YAML valid |
| Staged diff audit | Whitespace checks passed in all three repositories |
| Renderer index bytes | All 9 assets and 3 fonts match the staged manifest |

Staged implementation scope is 22 server files, 11 web files and 10 mobile
files. No environment/credential file, database, PDF, log or generated build
artifact is staged. Added-line scanning found no high-confidence private-key,
AWS, GitHub or Google OAuth token patterns. This focused secret scan complements
manual review; it is not an assertion that every preexisting repository file
contains no public configuration key. The web prerequisite's three existing
reviewed fonts are intentional binary assets and their hashes were verified.

The server runner creates a fresh PostgreSQL 17.11 cluster on loopback with
synthetic credentials, applies only its test schema, and stops/removes the
cluster afterward. Real Chrome/Chromium PDF rendering uses the reviewed web
module through a private test HTTP adapter. PDF producer checks are retained.
Provider identity and store responses are mocked; these tests are not live
provider certification. Web cookies are tested through the actual local web
session endpoint against temporary SQLite and PostgreSQL, including revocation.

New coverage includes upload request-key and content deduplication under
concurrency; owner mismatch at every upload/read stage; MIME/hash/size/page and
active-content rejection; quota/expiry; cookie Origin checks and invalid Bearer
precedence; common list IDs; final retry with exactly one debit; explicit linking
requiring PKCE and the initiating session; disconnect/withdrawal retention;
conflicting owner review queues; migration source/owner/ledger invariants,
read-only planning, repeat apply and soft rollback; inert web HTML attachment;
and OAuth access-log query redaction.

Mobile coverage includes a lost completion response followed by the same upload
key, no generation/debit endpoint use during import, original file/provenance
retention, account switch stopping before PDF transfer, owner-pinned final retry,
existing server archive verification without re-upload, bad success-shaped
receipts, legacy optional metadata and cursor continuation/loop rejection.

Initial full regression failures were traced to test setup: the temporary
renderer adapter omitted its required renderer identity header, the mock cookie
host was outside the temporary allowlist, and simultaneous renderer suites
observed each other's temporary folders. These were corrected without weakening
the production checks. A Windows open-log cleanup error in the runner was fixed
by closing logging before removing its own temporary directory. The final full
server run exited successfully and cleaned up. A mistyped web test module name
was corrected; the intended V3 module's ten cases passed.

All renderer text assets were normalized to their declared LF checkout format
before recomputing manifest hashes. The nine renderer tests and ten V3 tests
passed after normalization, including real Chromium output.

Reproduction (supply local runtime paths, no secrets in arguments):

```text
python scripts/run_shared_archive_tests.py --postgres-bin PG_BIN --work-dir TEMP_OUTPUT --web-repo REVIEWED_WEB_REPO --pattern test_*.py
python -m unittest test_platform_ledger_bridge test_mobile_report_renderer test_canonical_v3 test_shared_accounts_archives
flutter test --no-pub test/shared_archive_upload_test.dart test/mobile_account_service_test.dart test/report_sync_repository_test.dart test/report_archive_view_test.dart
```

Set `PDF_BROWSER_PATH` for real browser tests. The runner needs psycopg/FastAPI/
pypdf and web auth dependencies. On Windows, the preexisting land-worker tests
use a runner-only `fcntl` compatibility shim. Native Linux/container testing,
provider console/client/callback verification, proxy/cookie smoke checks, image
builds and Android/iOS release builds remain rollout gates. No claim of live
production readiness is made by the local test result.

Detailed transient logs are outside tracked repositories in
`C:/CodexWork/.shared-archives-validation/`. The server implementation intentionally
does not move existing archive, ledger or payment ownership. The server web
submodule pin is unchanged pending authorization to publish the matching web
commit and assemble a release.

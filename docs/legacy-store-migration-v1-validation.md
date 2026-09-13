# Legacy migration v1 validation — 2026-09-13

Prepared on `work/legacy-credit-migration-v1-20260913` from server `02f3b1152b77154325099214373fa30c2f765e3e`. Mobile branch of the same name starts at `f481b2e88d4736a600b7c503ba3663114d868d6f`. Web contract is fixed at `5b50f83370bc140d4d899a5f0da1b812a35600cf`; no web source was edited.

## Executed checks

| Check | Result |
| --- | --- |
| Server full regression, disposable PostgreSQL + local Chrome renderer | 106 passed, 0 failures/errors/skips |
| Expanded final migration/provider suite after additional test cases | 23 passed, 0 failures/errors/skips |
| Flutter payment/account/my-page tests | 31 passed |
| Full Flutter suite | 299 passed, 2 failed; both reproduced on unchanged base SHA |
| Analysis of seven changed Dart service/UI/test files | 0 errors, 0 warnings; 2 pre-existing deprecation infos |
| Python syntax compilation; Compose/Traefik/OpenAPI YAML parsing | Passed |
| Both prepared repositories: `git diff --check` | Passed |

The final focused suite adds acknowledgement failure after commit, pre-existing store-owner mismatch, zero-balance/refund dry-run, Apple signed-notification handler and repeatable schema application. Production code was unchanged after the successful full server regression; only tests/docs were added. The 23-test result is not an additional independent full-suite run.

Coverage includes concurrent identical deliveries, Apple restored transaction aliases sharing an original ID, provider/platform/product/account mismatch, unbound receipt review, pending/revoked/unverifiable and nonhistorical purchases, fake-verifier production rejection, transaction rollback after ledger write, refund-before-grant and grant/refund races, repeat refund with no negative balance, preservation of ad removal, idempotent binding/apply/schema reruns, default-disabled/empty allowlist, dry-run no persistent writes, account-scoped history, CLI redaction and safe batch retry. Provider unit fixtures cover Google promo/reward/test/non-consumable checks, arbitrary caller order rejection, Apple current-API requirement/paid ownership/type checks, official SDK unsigned-input rejection and Google push identity rejection. No real provider transaction was used.

Initial full-server failures were local test-environment omissions: five renderer cases lacked `PDF_BROWSER_PATH`, and one V2 parity case lacked the pinned `web` checkout. A local read-only contract checkout and local Chrome path resolved all six; no web implementation was changed. PostgreSQL requires an unsandboxed local process on this host because Windows restricted-token `pg_ctl` fails. All clusters were temporary loopback clusters created and stopped by the checked-in test runner, never an existing or production database.

## Pre-existing Flutter failures

`test/report_login_gate_test.dart` fails at lines 122 and 270:

- `narrow login gate contains only provider actions and can cancel`
- `browser failure is recoverable and successful callback restores flow`

Both fail identically in a separate clean detached checkout of `f481b2e` (that file's other two cases pass). They concern the login-dialog callback result, not legacy migration. No login-gate implementation or test was changed. Do not describe the whole Flutter suite as green. The changed my-page test had a stale `webReportCredits` constructor argument; it was updated to the current free/paid/available profile fields so the relevant UI suite can compile.

The analysis infos are existing `surfaceVariant` and `cloudMapId` uses in the cadastral/map views; verified present in the base SHA. No unrelated deprecation refactor was made.

## Reproduction and evidence

Local untracked logs are under `C:/CodexWork/.legacy-credit-validation/`:

- `regression-final/shared-archive-tests.log`: server 106-test pass.
- `focused-final/shared-archive-tests.log`: expanded 23-test pass.
- `flutter-focused-final.log`: 31-test pass.
- `flutter-regression.log`: full Flutter 299 pass / 2 fail.
- `flutter-baseline-login.log`: unchanged-base reproduction of those failures.
- `flutter-analyze.log`: scoped analysis results.

Server command pattern (set `PYTHONPATH` for the local test dependencies and `PDF_BROWSER_PATH` for the installed Chrome on Windows):

```sh
python scripts/run_shared_archive_tests.py --postgres-bin /path/to/postgres/bin --work-dir /temporary/validation --web-repo /path/to/pinned/web --pattern 'test_*.py'
python scripts/run_shared_archive_tests.py --postgres-bin /path/to/postgres/bin --work-dir /temporary/focused --web-repo /path/to/pinned/web --pattern test_legacy_migration.py
flutter test --no-pub test/payment_service_test.dart test/mobile_account_service_test.dart test/my_page_view_test.dart
flutter test --no-pub
```

Original server/mobile branches were not changed. Mobile's pre-existing dirty PDF-selection UI and building-selection test, and both existing stashes, were preserved. Live remote ref confirmation was unavailable because the bundled Git could not run `remote-https`; the recorded integrated release and cached refs identify the base. Reconfirm integration state before any future merge/push.

## Still required before production enable

The prepared code and synthetic validation do not authorize activation. Exact console product/type/price/sale evidence, reviewed unbound-account claims, real Apple/Google verification and authenticated notification testing, a reviewed production dry-run, rollout/backup approval and explicit operational authorization remain required. Apple historical records missing positive signed price evidence and products not verified as non-consumables stay blocked. Linux/container/store end-to-end verification was not run on this Windows host. Docker CLI was not available; Compose was parsed and inspected, not built or deployed.

No real purchase, restore, consume/acknowledgement, report-credit deduction/grant, console edit, production DB/schema apply, push or deployment was performed. All grant/refund mutations in validation were synthetic and confined to the disposable database.

# Subscription migration v1 validation — 2026-09-13

This record supersedes the prior non-consumable/permanent-entitlement validation. Final contract: Google Play `remove_ads_monthly`, monthly auto-renewing paid entitlement, 10 credits once per account AND subscription lineage; ad removal expires with the subscription. Apple legacy paths fail closed. See the current runbook for the complete state and reconciliation policy.

Prepared branch in both repositories: `work/legacy-credit-migration-v1-20260913`. Server base: `02f3b1152b77154325099214373fa30c2f765e3e`; mobile base: `f481b2e88d4736a600b7c503ba3663114d868d6f`; unchanged web contract: `5b50f83370bc140d4d899a5f0da1b812a35600cf`. The non-consumable preparation commits `0ece2c2bdc72e340e6a5af5af803372a81df6c67` / `68043cd59710ab56b114bbd8380ca272b8de4326` are superseded and must not be used as rollout targets. The mobile subscription revision is `acde3673d36f1f509d4ae5825d15ecc06fcc0406`; the server revision is the commit containing this record. Final hashes are also in the external result manifest.

## Final checks

| Check | Result |
| --- | --- |
| Final server full suite, disposable PostgreSQL + local Chrome renderer | 126 passed, 0 failures/errors/skips; includes all 39 subscription tests |
| Earlier focused subscription pass before the last one-time-verifier guard test | 38 passed; overlaps full suite |
| Full Flutter suite | 303 passed, 2 existing failures |
| Analysis of all 8 changed Dart files against the mobile base | 0 errors, 0 warnings; 2 existing deprecation infos |
| Python syntax compilation; Compose and mobile OpenAPI YAML parsing | Passed |
| Prepared server/mobile `git diff --check` | Passed |

Final server regression includes every last production edit: provider-linked and out-of-app stored-owner lineage handling, current expiry recheck after verification, expiry-aware profiles, order-only reconciliation, and complete removal of old one-time legacy verifier exceptions. No production code changed after that run.

Subscription coverage includes active/canceled/grace entitlement; pending/expired/paused/held/unknown exclusion; monthly base-plan and actual payment evidence; wrong SKU/prepaid/installment/test/free/promo/unpaid evidence; Apple rejection; account binding review and conflict; concurrency/replay; renewals/new/linked/out-of-app tokens; missing/cyclic ancestors; stored expired anchors; superseded-token ordering; immutable account/lineage grants; source versus unrelated renewal refund/revocation; chargeback; refund/revoke before claim; claim/refund races; nonnegative partial recovery; normal expiry without credit clawback; grant-disable independence; transaction rollback; dry-run rollback; CLI redaction and retry; OIDC authentication; acknowledgement after commit and retry; no acknowledgement for ineligible evidence; superseded 013 grant activation block; and refund reconciliation from recorded orders without a token inventory.

Flutter coverage includes exact Google SKU/future-expiry checks, missing/expired/Apple entries, expiry timer without another network response, known existing subscription purchase delivery via restore, rejected arbitrary legacy SKU, no new sale flow, server-only grant messaging, duplicate grant response, failed verification, logout clearing, plus the existing report/consumable suite. Installed Android plugin `in_app_purchase_android-0.5.0/lib/src/in_app_purchase_android_platform.dart:230` queries both inapp and subs during restore.

The two full Flutter failures are unchanged `report_login_gate_test.dart:122` (narrow login gate cancellation) and `:270` (browser failure recovery). Both were previously reproduced on the untouched `f481b2e` baseline, and the final run has the same failures. The two analyzer infos are the existing `surfaceVariant` use at cadastral_draw_view.dart:49 and `cloudMapId` at map_view.dart:89. This revision does not modify those call sites.

## Local evidence and reproduction

Logs are deliberately outside tracked repositories:

- `C:/CodexWork/.legacy-credit-validation/subscription-regression-final/shared-archive-tests.log`
- `C:/CodexWork/.legacy-credit-validation/subscription-focused-final/shared-archive-tests.log`
- `C:/CodexWork/.legacy-credit-validation/subscription-flutter-regression.log`
- `C:/CodexWork/.legacy-credit-validation/subscription-flutter-analyze.log`
- `C:/CodexWork/.legacy-credit-validation/flutter-baseline-login.log`
- `C:/CodexWork/output/legacy-credit-migration-20260913/result.json`

```
python scripts/run_shared_archive_tests.py --postgres-bin PATH --work-dir TEMP --web-repo PINNED_WEB --pattern 'test_*.py'
flutter test --no-pub --reporter expanded
flutter analyze --no-pub CHANGED_DART_FILES
```

Original server/mobile integration branches and the web source remain untouched. The original mobile PDF-selection view, building-selection test edits, and two stashes remain present. Remote ref confirmation was unavailable in this host's bundled Git (`remote-https` helper missing); use the recorded integration bases and recheck before any eventual merge/push.

## Remaining rollout work

Prepared defaults are disabled. Real Google subscriptionsv2/catalog/Orders permissions, actual monthly base-plan verification, authenticated subscription/void RTDN delivery, missing-account support bindings, an approved production dry-run, schema 014 and coordinated release authorization remain unperformed. There is no raw legacy token inventory: login alone cannot backfill every former subscriber. Unknown ownership/lineage or unavailable provider evidence stays closed for review. Old expired-token-only revoke events may require order evidence/support reconciliation if Google can no longer return the subscription.

Docker/Linux and store/device end-to-end verification were not run on this Windows host; Docker CLI was unavailable. No real purchase, restore, acknowledgement/consumption, credit grant/debit, console edit, production DB/schema apply, push or deployment occurred. DB mutations and provider responses in tests were synthetic and confined to disposable local clusters.

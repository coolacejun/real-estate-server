# Legacy ad-removal migration v1

## Approved benefit and evidence

On 2026-09-13 the owner clarified that the old KRW 4,900 purchase removed the rewarded-ad step before report creation. Preserve ad removal permanently in that legacy/ad-based flow. Add **10 paid server report credits per verified historical store purchase, once**, to the same central account. Current reports retain free-first/paid-next debits and current 10/30/50 packs after exhaustion. This is not an unlimited server-report entitlement.

Source candidates are `remove_ads_monthly` (mobile `3c22f1f` and the existing server restore ID) and earlier `premium_monthly` (`6adbb1d`). Neither is enabled. The policy example has an empty allowlist. Trial copy and `buyNonConsumable` do not establish console type, price, sale interval or platform availability. Mobile removes legacy sale queries/buttons and unused trial dialogs, and loads restore IDs from the server catalog.

**Only historical paid non-consumables are implemented.** Leave subscriptions, consumables, promotional/rewarded purchases and unidentified products disabled. Apple missing/zero signed price evidence, family-shared purchases, sandbox purchases and unavailable current provider records are ineligible. If console evidence differs, obtain a separate eligibility contract; do not relax checks to match an expected backfill count.

## API and ledger

- Existing `POST /api/mobile/v1/store/restore` retains legacy zero-credit behavior while migration is off. When enabled and allowlisted, fresh provider verification, sale-period checks and account binding are mandatory. The response adds `legacyMigration: {version: 1, status: granted|already_granted, creditsGranted: 10|0, alreadyProcessed: boolean}`; the count is new credits in this response. Client-provided amounts have no authority.
- Authenticated `GET /api/mobile/v1/store/legacy-migration/v1` returns only that account's grant history and reconciliation counts. It requires schema 013. `/me` remains authoritative for current balance and ad removal across login, reinstall and device changes.
- `legacy_store_purchases` uniquely identifies `(platform, identity_digest)`, with SQL constraints for version 1 and a ten-credit grant. Apple identity is the verified original transaction ID; Google identity is the verified purchase token. Type-prefixed hashes and uniquely constrained verified transaction/order aliases prevent replay. No new raw receipt/token is persisted.
- A provider-scoped PostgreSQL advisory lock serializes grant, binding approval, aliases and refund tombstones. Ledger, source record, entitlement and audit commit atomically. Existing user row locks serialize with other credit mutations and report reservations. Ledger reason is `legacy_store_migration_v1`, reference type `legacy_store_purchase`; metadata records version, product, evidence reference and ten credits.
- A supplied provider account token must equal the user's store account token, even with an operator approval. An absent historical token requires a reviewed `legacy_store_bindings` record. No mobile binding-approval API or automatic first-receipt-claim grant exists. A cached receipt digest or prior unbound entitlement alone is not ownership proof. Existing central transaction ownership is also checked; collisions cannot transfer a binding.
- Apple uses current production Server API transaction info and the official signed-data library with trusted root, bundle/app ID, production environment and online certificate checks. A legacy app receipt only locates the transaction. Google uses Android Publisher state/type/quantity/non-consumption/account checks; a client order ID never selects identity. Android acknowledgement follows commit and is retryable, never consumption of the legacy product.

## Refund and cancellation

- `POST /api/store/legacy/v1/notifications/apple` verifies the production notification and nested signed transaction, and handles REFUND/REVOKE with verified revocation evidence.
- `POST /api/store/legacy/v1/notifications/google` authenticates Google OIDC audience, exact push service-account email and verified email, then checks the package. One-time canceled / voided events are actionable. A known token can be reconciled from authenticated void evidence even after provider 410. Unknown tokens require an allowlisted provider match; unresolved actionable events return 503 for delivery retry.
- A refund before grant creates a permanent zero-credit tombstone. After grant, recover `min(10, paid_remaining)` under the user lock. Ledger reason is `legacy_store_reversal_v1`. Retain the used/unrecoverable portion in `reconciliation_credits`; never create a negative balance or debit again on notification replay. The shared paid bucket does not track individual credit units: recovery can use pre-existing paid credits, up to ten.
- Ad-removal rows are never revoked by this migration. A revoked credit grant never automatically reactivates, including on refund-reversal events or configuration edits. Outstanding reconciliation is not automatically collected from future purchases or report-failure refunds; review it separately. Do not delete the original grant or invent another debit.
- Notifications have a separate enable flag and must stay on when grants stop. Monitor retries/dead letters and reconcile reviewed manifests after outages. The batch `reconcile` mode verifies current provider state and makes no grants.

## Preparation gates

1. Obtain historical console evidence for each exact product ID: platform, non-consumable type, KRW 4,900 price/benefit and sale interval. If the SKU had different prices/benefits, leave it disabled pending a stricter contract. Record a durable non-secret evidence reference and an exclusive sale cutoff with UTC offset.
2. Back up and test additive `db/013_legacy_store_migration.sql` after 009–012. Normal Compose deployment includes numbered SQL files; include that fact in the separately approved rollout. Schema creation grants nothing. Drain old server instances before enabling migration so every store writer runs the reviewed version.
3. Populate a restricted policy file with evidenced values. This example is illustrative, not a discovered SKU or date:

   ```json
   {"version":1,"products":[{"platform":"android","productId":"confirmed.legacy.ad_removal","kind":"non_consumable","purchaseBefore":"2026-08-01T00:00:00Z","evidenceRef":"store-evidence/reviewed-case-id"}]}
   ```

   Mount using `LEGACY_POLICY_HOST_FILE`; direct CLI uses `LEGACY_POLICY_FILE`. Current/retired credit-pack IDs, duplicate entries, unsupported type and future/naive dates fail closed. Keep the default empty file until evidence is complete.
4. Configure existing Apple root/bundle plus `APPLE_APP_ID`, `APPLE_IAP_KEY_ID`, `APPLE_IAP_ISSUER_ID` and read-only `APPLE_IAP_KEY_HOST_FILE` (CLI: `APPLE_IAP_KEY_FILE`). Configure the existing Google package/service-account secret. Install `api/requirements.txt`. Never commit real `.env`, keys, receipts, tokens or provider payloads.
5. Configure Apple Notifications v2 and Google RTDN/PubSub authenticated push at the routes above. `GOOGLE_RTDN_AUDIENCE` is the exact expected audience; `GOOGLE_RTDN_EMAIL` identifies the approved **push** service account. Enable `LEGACY_NOTIFICATIONS_ENABLED=true` first. Validate delivery, signature/OIDC rejection, retries/dead letters and alerts in an approved store test environment. These real store tests were not run here.
6. Independently establish each unbound purchaser's rightful central account using trusted support/store evidence. Email similarity, local subscription booleans or arbitrary receipt submission are insufficient. The `bind` mode records that separate operator review, not a blanket ownership bypass.

## Backfill and retry

Restricted JSONL input fields are `userId`, `platform`, `productId`, `verificationData` and optional `transactionId`. Binding rows also need `bindingEvidenceRef`. The existing database digest cannot reconstruct a receipt: obtain a fresh restore or a reviewed secure receipt export. Never commit the input or send it in chat.

```sh
# Default dry-run: provider/database reads and locks; no database writes,
# acknowledgement, credit, entitlement or binding mutations.
python scripts/legacy_store_migration.py --input /secure/batch.jsonl --report /secure/grant-dry-run.jsonl
python scripts/legacy_store_migration.py --mode bind --input /secure/reviewed-bindings.jsonl --report /secure/bind-dry-run.jsonl

# Separate explicit operator writes, only after the appropriate approval.
python scripts/legacy_store_migration.py --mode bind --apply --input /secure/reviewed-bindings.jsonl --report /secure/bind-apply.jsonl
# Repeat the grant dry-run after bindings exist and review all rejected rows.
# Grants require BOTH enable flags; defaults remain false.
python scripts/legacy_store_migration.py --apply --input /secure/batch.jsonl --report /secure/grant-apply.jsonl

# Reconciliation also defaults to dry-run; use --apply only after review.
python scripts/legacy_store_migration.py --mode reconcile --input /secure/batch.jsonl --report /secure/reconcile-dry-run.jsonl
```

Reports are new files (never overwritten), flushed per row, containing line number, mode/status, counts and coarse error codes. No account UUID, token, receipt, order ID or provider exception text is output. Batches continue after failed rows and exit nonzero if any failed. Re-run the same input using a new report filename: successful grants/bindings/reversals stay idempotent. Review 409 ownership/binding conflicts; retry provider/acknowledgement 5xx failures. A crash after commit before report flush is safe: the next attempt returns already granted.

Dry-run is a point-in-time view, not a reservation; apply rechecks provider/account state. Start with a small reviewed batch, compare count × 10 with ledger/account deltas, then proceed in bounded batches. Archive redacted reports and evidence references. Protect input-file permissions and disable body/Authorization capture in proxies/APM. Dispose of temporary receipts under the operator's retention policy. No production dry-run/apply was executed here.

## Rollback and enable blockers

Set `LEGACY_GRANT_ENABLED=false` to stop new grants while keeping policy entries, the notification handler and `LEGACY_NOTIFICATIONS_ENABLED=true` for issued credits. Do not remove allowlist entries during reconciliation. If an API image rollback removes notification handling, retain/retry provider delivery and reconcile the gap before enabling grants again.

Never drop 013 tables, delete bindings/grants/aliases/tombstones, rewrite balances or bump version to force a retry. Restoring an old DB snapshot can erase later purchases/report debits; data rollback or compensation needs separate reviewed reconciliation and backups. Mobile rollback does not erase server credits.

Production enable requires exact console evidence, reviewed account bindings, real provider/notification verification, reviewed production dry-run, backups/rollout review and explicit operational authorization. Missing evidence means default disabled. No push, deployment, actual purchase/restore/acknowledgement, console change or production schema/data change was performed.

## Primary references

- [Apple official Server Library](https://github.com/apple/app-store-server-library-python): Server API and signed-data verification.
- [Google ProductPurchase fields](https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.products): state/type, quantity, token and account binding.
- [Google RTDN reference](https://developer.android.com/google/play/billing/rtdn-reference): canceled/voided notifications.

Automated tests use synthetic fixtures and disposable loopback PostgreSQL; they do not substitute for the production gates. See the companion validation record for executed checks.

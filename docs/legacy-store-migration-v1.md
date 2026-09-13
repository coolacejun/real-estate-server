# Google monthly subscription migration v1

The confirmed historical product is Google Play `remove_ads_monthly`, a monthly auto-renewing subscription. Apple has no historical product. This contract supersedes the non-consumable/permanent-ad-removal proposal in the preceding preparation commit. It is not deployed and grants have not been applied to real accounts.

## Eligibility and benefit

Only a freshly verified, currently entitled paid Google subscriber can receive 10 central paid report credits, once per account AND once per provider-linked subscription lineage, migration version 1. The normal free-first/paid-next report debit and the current 10/30/50 consumables are unchanged. The current successful paid order must match the SKU, base plan and verified purchase token. Its amount must be positive; no hardcoded KRW 4,900 comparison is used. Test purchases, free trials, free promotions and unverifiable payments are excluded.

| subscriptionsv2 state | Future provider expiry | Ads / first grant |
| --- | --- | --- |
| ACTIVE | Required | Eligible with successful paid order |
| CANCELED (auto renewal canceled) | Required | Eligible through paid-through expiry |
| IN_GRACE_PERIOD | Required | Eligible with the last successful paid order; Google retains entitlement in grace |
| EXPIRED, PENDING, PAUSED, ON_HOLD, PENDING_PURCHASE_CANCELED, unknown | Any | Ineligible |
| Replaced ancestor token, authenticated revoked token, voided latest order | Any | Ineligible |

The matching provider catalog base plan must have `autoRenewingBasePlanType.billingPeriodDuration=P1M`; prepaid/installment plans are rejected. The order must be PROCESSED, a paid base/introductory (or paid proration) phase and positive actual payment. PENDING, CANCELED, PENDING_REFUND, REFUNDED and PARTIALLY_REFUNDED orders do not qualify. Orders' service-period timestamps are accounting snapshots; subscriptionsv2 expiry is the entitlement authority.

Ad removal lasts only until the verified subscription expiry. `/me` filters expired entries and excludes all old undated and Apple legacy entitlements. The mobile app requires exact Android SKU and future expiry, clears on account change/logout, and expires its local state without another network response. RTDN updates cancellation, grace, hold, recovery and expiry. Missing notifications can leave a previously valid snapshot until its recorded expiry; working authenticated RTDN and delivery monitoring are rollout requirements.

## Restore and ownership

Users log in and choose **내 정보 → 기존 구매 복원**. The installed Android billing plugin's `restorePurchases` calls `queryPurchases(ProductType.subs)` as well as inapp. Only the exact Google SKU is treated as subscription restoration; current consumable delivery continues through its existing verification path. No new subscription purchase CTA or sale query is introduced. A current pending purchase delivery for the known legacy SKU is also verified as a restore after it becomes purchased. Credits and active entitlement always come from the server; the app does not grant locally.

There is no raw legacy purchase-token inventory in the server. Login alone cannot automatically identify/backfill every old subscriber. Receipt hashes and old local ad flags do not establish paid eligibility or ownership. A user must supply the current Play subscription token through restore, or an operator must use an access-controlled reviewed input file. Do not claim complete automatic backfill.

Provider `linkedPurchaseToken` is walked to the root (bounded to 32 with cycle rejection), or to a previously verified persisted ancestor. Missing ancestors without such an anchor fail closed for review. An out-of-app expired token is used only when its user mapping is already stored; it is never queried speculatively. Roots, token hashes and immutable grants prevent token replacement, renewal and linked-token replay from granting again or moving a purchase to another account. Branch conflicts require review instead of speculative merging.

Every present provider obfuscated account ID must match the central store-account UUID. A verified lineage's existing owner is inherited when linked descendants omit that field. Unbound historical tokens require a separate, reviewed ownership binding; the client cannot create one. A reviewer cannot override a conflicting provider account or historical central transaction owner. Record a non-secret support evidence reference. Never infer an owner from email, a local entitlement, or whoever submits a receipt first.

## Storage, API and reconciliation

Apply additive schema 014 after 009–013 in a separately authorized rollout. 013 tables remain superseded audit history; they are not reinterpreted. Any previously issued 013 grants block new v1 grants until explicitly reconciled. The new SQL has independent unique account, lineage and source-order grant constraints. A low-volume advisory lock covers provider reads and DB mutations; user row locks serialize balances with report/consumable activity. Ledger, grant, binding and snapshot writes commit atomically. Dry run executes this same path then rolls back ALL DB writes and does not acknowledge Play.

`POST /api/mobile/v1/store/restore` always uses the Google subscription verifier for legacy restoration. It returns `status=active|inactive`, nullable `expiresAt` and `legacyMigration` with `version`, `status=granted|already_granted|grant_disabled|ineligible|revoked`, newly granted `creditsGranted`, `alreadyProcessed`, `subscriptionState`, `entitlementActive` and `expiresAt`. Grant flags do not disable entitlement status sync. Apple/other legacy products fail closed with 410. The authenticated history endpoint returns only this account's grant state and reconciliation count. No raw token/order identity is returned.

The DB stores domain-separated token hashes, linked lineage, provider state, expiry, verification time, and the grant source order ID (not a bearer credential). It never stores raw subscription tokens or provider bodies. Logs/reports must not record input files, order IDs, account UUIDs, provider errors or credentials. Source order IDs allow refund reconciliation after an expired token becomes unavailable.

Normal expiration, renewal cancellation, hold or pause only ends ad access; it does not claw back the 10 credits. Refund/partial refund/chargeback or revocation of the **source payment used to justify that grant** reverses it once. A refund of an unrelated later renewal does not reverse the earlier grant. Reversal debits `min(10, paid_remaining)` and records any unrecovered amount as `reconciliation_credits`; balances never go negative. Do not erase grant identities or automatically repay a reversed v1 grant on another token.

Google RTDN authenticates OIDC audience, verified exact push-service-account email and package name. Subscription notices re-fetch subscriptionsv2. A REVOKED notice permanently fences that token; reversal requires the source order to match the provider's current revoked order, or source Orders API refund evidence. Authenticated subscription void notices persist an order tombstone before any claim, even if the token later becomes unavailable. Tombstones commit before fallible provider queries. Refund/notification handling stays enabled when only the grant switch is off. Apple notification endpoint always returns 410. One-time consumable notices are outside this legacy handler.

## Operator dry run / apply

Do not run these against production as part of preparation. Use restricted JSONL files obtained through an approved support flow, and new redacted report paths. Tokens are sensitive; never put them in command lines or repository fixtures.

A claim row uses `userId`, `platform=android`, `productId=remove_ads_monthly`, `verificationData=<current token>`. A binding review adds `bindingEvidenceRef`. A reconciliation row can use the same token fields, or only `sourceOrderId` for an already recorded grant order. The latter rechecks Orders API without needing an expired token and never creates a new grant.

```
python scripts/legacy_store_migration.py --input RESTRICTED.jsonl --report NEW-DRY.jsonl --mode grant
python scripts/legacy_store_migration.py --input RESTRICTED.jsonl --report NEW-BIND-DRY.jsonl --mode bind
python scripts/legacy_store_migration.py --input RESTRICTED.jsonl --report NEW-RECONCILE.jsonl --mode reconcile
```

Dry run is default; `--apply` explicitly commits. Binding and granting are separate operations. Review the redacted outputs before a separately approved apply. Reports are created exclusively, never overwritten; failures have coarse codes and safe retryability. Apply/retry is idempotent. Use reconciliation inputs exported securely from recorded source orders to audit missed refunds; this is not discovery/backfill of all subscribers.

## Rollout blockers and rollback

1. Production authority/permissions for subscriptionsv2, subscription catalog and Orders API, actual monthly base-plan evidence, and authenticated subscription + void RTDN delivery are not tested here. Set existing Google service account/package configuration and Google push audience/email through approved secret management. No Apple migration keys or policy allowlist are needed.
2. Keep `LEGACY_GRANT_ENABLED=false` and `LEGACY_NOTIFICATIONS_ENABLED=false` in prepared defaults. Enable notification delivery and validate it before enabling grants. Do not enable grants with unreviewed owner mappings, uncertain linked lineage, unavailable Orders evidence or superseded 013 issued rows.
3. Back up, apply 014 and drain old server/mobile writers in an approved coordinated release. Old server versions can expose undated/permanent entitlements and must not coexist with this contract. No old grant commit may be deployed independently.
4. Monitor notification failures, Orders availability, binding review cases and reconciliation balances. Stop NEW grants with the grant flag; retain notifications, audit, immutable grants and reconciliation. Roll forward fixes; do not drop migration tables, reset identities, or revert to non-consumable/permanent-ad logic.

## Provider references

- [Google subscriptionsv2 resource](https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.subscriptionsv2)
- [Subscription lifecycle, grace and cancellation](https://developer.android.com/google/play/billing/lifecycle/subscriptions)
- [Google order and paid phase evidence](https://developers.google.com/android-publisher/api-ref/rest/v3/orders)
- [Monthly base-plan catalog](https://developers.google.com/android-publisher/api-ref/rest/v3/monetization.subscriptions)
- [RTDN subscription and void events](https://developer.android.com/google/play/billing/rtdn-reference)

# One-account report test access

Migration `db/015_report_test_grants.sql` adds a revocable permission for final
report generation. It does not change a free/paid balance, issue a purchase,
change a subscription, or grant a general administrator role. The central API
decides on every reservation; web and mobile only display/use the returned
`creditSummary.reportTestAccess` boolean. `availableCredits` remains the sum of
the actual free and paid balances. A test reservation has
`no_charge_reason = 'test_report_grant'`, no debit bucket or ledger row, and
still follows the existing request-id, completion, failure and archive flow.

## Deployment order

1. Confirm the production web/API/mobile revision and the web-to-API central
   ledger bridge (`PLATFORM_API_BASE_URL`). Record the current migration marker,
   take the normal PostgreSQL backup, and keep the previous deployment ready.
2. Apply additive migration 015 using the existing migration procedure before
   starting this API revision. The new API rejects requests until its marker is
   present. Deploy the matching web and API revisions; distribute the updated
   mobile build to test mobile UI, or use a previously deployed client only if
   it has been confirmed to honor the central report access response.
3. Use authenticated Naver or Kakao account evidence to independently establish the
   provider subject, the **web** external account ID, the central UUID, and the
   web and API client IDs for the selected provider. The email address is a confirmation clue, not
   a lookup key. If the two client IDs or mapped UUIDs differ, stop and review
   account linking; never merge or match accounts on email alone.
4. Choose `REPORT_TEST_PROVIDER=naver` (the backward-compatible default) or
   `REPORT_TEST_PROVIDER=kakao`. Only these two providers are supported. For
   Kakao, replace `NAVER` with `KAKAO` in the environment variable names below.
   In a secure production shell, set `DATABASE_URL`,
   `REPORT_TEST_NAVER_SUBJECT`, `REPORT_TEST_WEB_EXTERNAL_ID`,
   `REPORT_TEST_USER_ID`, `REPORT_TEST_EXPECTED_EMAIL`,
   `REPORT_TEST_WEB_NAVER_CLIENT_ID`, the API's `NAVER_OAUTH_CLIENT_ID` (or
   `NAVER_CLIENT_ID`), and `REPORT_TEST_ACTOR`. Keep these values out of command
   history, logs, tickets, and version control. Run
   `python scripts/manage_report_test_grant.py inspect` and
   `python scripts/manage_report_test_grant.py grant` first: both are read-only.
   Verify that the central account is active, its selected social identity is active, and
   the web mapping and client scope match before applying.
5. Only after the exact identity and deployed versions have been reviewed, run
   `python scripts/manage_report_test_grant.py grant --apply`. Re-run `inspect`
   and check the signed-in web/mobile profile reports `reportTestAccess: true`.
   Create a disposable test final report on each client. Verify one completed
   usage per request ID, `no_charge_reason`, and unchanged free/paid balances and
   credit-ledger count. Check a failed render and retry with a fresh disposable
   report; it must become failed then complete without a debit. Confirm a
   second account with the same email does **not** have access.

## Revocation and recovery

Run `python scripts/manage_report_test_grant.py revoke` to preview, then
`python scripts/manage_report_test_grant.py revoke --apply` using the same
verified identity inputs. `inspect` must show inactive and a new final-report
request at zero balance must receive the normal 402 response. Existing
completed archives remain accessible; revocation does not rewrite history.
Both grant and revoke are idempotent and write an audit event only on a state
change. An inactive/unlinked bound social identity also makes the runtime permission
false. If the application rollout must be reverted, revoke the permission
first; keep the additive table and audit data until a separate data-retention
review. Never restore a database snapshot over live credit/report events.

The manager checks balance and credit-ledger counts inside its transaction and
rolls back if either changes. It intentionally does not find account IDs from
email or connect to a server on its own. No production change occurs merely by
publishing the code or running the dry run.


## Kakao support and schema compatibility

The API and manager support both Naver and Kakao; web/mobile still use the same
`reportTestAccess` response. Deploy the updated API and manager before granting
Kakao access. Migration 015 remains sufficient: the historical
`naver_identity_id` column already references `platform_identities(id)` and now
stores the exact reviewed Naver **or** Kakao identity. Its name is retained so
existing grants, audit rows and rollback readers remain compatible. No account,
balance, purchase, or identity migration is performed. The runtime checks the
bound identity's provider, owner and active status on every reservation.

A subject from one provider never establishes an identity on the other provider,
even if subject strings or emails match. The manager compares client IDs within
the selected provider and requires that identity and the independently verified
web mapping to identify the same central UUID. A grant already bound to another
identity cannot be silently reassigned. Before rolling back to a Naver-only API,
revoke any Kakao grants with the updated manager; the old API treats Kakao grants
as inactive and will apply normal charging rules.

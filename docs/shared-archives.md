# Shared accounts and archives

Implementation date: 2026-09-11. This is an opt-in change. No production migration,
deployment, provider login, upload, payment, withdrawal or account merge was run.

## Identity and authorization

`platform_users.id` is the authoritative UUID. Web `users.id` remains a local
session identifier; `platform_account_links` and `platform_external_accounts`
must agree on its central owner. The web profile exposes `platformUserId`, while
the mobile profile exposes the same value as `user.id`.

Provider identity is `(provider, provider_subject)`, never email. Web and mobile
must use the same provider application/client scope. The internal web resolver
requires the configured provider client ID and a verified provider assertion
from the authenticated internal web service. That service is part of the trust
boundary; its credential is never delivered to a browser or mobile client.

Verified email signup uses the preexisting `web_email` identity with subject
`web:{externalId}`. The internal web service must assert completed verification
and explicit new registration. No provider-email lookup selects an existing
owner. Existing unmapped accounts cannot claim a second initial grant, including
when their local paid balance is zero. With shared mode OFF, their login/profile
reads remain available with original local values and a review-required status;
financial actions and shared-mode access still require reviewed mapping.

An ordinary login never links the currently signed-in account. Explicit linking
requires a login less than ten minutes old and the original initiating session.
Mobile linking binds the flow to its token family and delays attachment until
one-time code/PKCE exchange. Refresh does not reset the authentication age. Web
linking uses `/api/auth/{provider}/start?link=1`, a browser-bound one-time state,
the original session, and Google ID-token audience/nonce verification. The
common `account/identities/start` alias is for Bearer clients; cookie clients use
the web OAuth route. Kakao, Naver and Google are supported.

Disconnect keeps the provider subject reserved, rejects removal of the last
supported login method, and revokes mobile tokens and web sessions through
`auth_version`. A disconnected identity can only be reattached explicitly to
its original active owner. Withdrawal is a soft status change: it revokes access
and preserves archive, owner, usage, ledger and payment rows. Purchased credits
or unfinished financial/report operations block withdrawal for review. This
implementation does not define a legal retention or account restoration policy.

Provider/owner/scope conflicts and unsupported legacy balance transfers produce
409 and a credential-free `platform_account_review_queue` entry. Web-local
identity conflicts are retained in SQLite `account_connection_review_queue`.
Unmapped legacy web accounts require reviewed mapping; no request silently
merges accounts, transfers credits, or grants a second initial allowance.

## Shared API

Both clients use the same `/api/v1/report-archives` routes:

| Method and path | Behavior |
| --- | --- |
| GET root, `?limit=1..100&cursor=...` | Owner-scoped stable cursor list |
| GET `/{archiveId}` | Owner-scoped metadata and available formats |
| GET `/{archiveId}/content?format=pdf\|html\|json` | Owner-scoped attachment |
| POST `/uploads` | Reserve immutable request key and PDF metadata |
| PUT `/uploads/{uploadId}/content` | Verify exact PDF bytes |
| POST root, `{uploadId}` | Atomically complete or return previous receipt |
| POST root, `{requestId,snapshot,html}` | Cookie-only web HTML snapshot |
| DELETE `/{archiveId}` | Idempotent soft deletion; owner/ledger retained |

Bearer credentials take precedence; a bad Bearer token never falls back to a
cookie. Cookie authorization uses the private web session introspection endpoint,
checks the central active account and matching auth version, and requires an
allowlisted Origin plus `X-Archive-Request: 1` for every write. Content responses
are private/no-store with no-sniff headers. HTML is an attachment under restrictive
sandbox CSP and is never inserted into the authenticated web editor.

`platform_archive_catalog` is a UNION over existing canonical rows and new
`platform_archive_imports`. A normal final retains its existing server archive
UUID and original usage/ledger transaction. There is no second archive write or
second debit. `/api/v1/reports/preview` and `/final` call the existing canonical
service; old mobile routes remain compatible. The web's existing HTML snapshot
format is preserved as a distinct source kind, without claiming canonical PDF
provenance or altering its existing output-credit authorization path.

## Explicit local PDF upload

The mobile device keeps its local archive. Upload is per selected report after
confirmation of the account UUID. The app records the owner and API origin before
the request and pins each transfer to that owner. Offline, account changes,
invalid receipts and lost responses keep a failed/retryable local entry. A
normal canonical report with a server ID verifies that ID instead of uploading
its PDF again. Final generation retries also retain their original owner.

The upload request key is scoped to the owner and fingerprinted with SHA-256,
size, title, address and provenance. A changed body under the same key returns
409. Same-owner identical PDF bytes converge on one active import even across
different request IDs. Completion and retries never call generation or ledger
functions. Soft-deleted imports are not resurrected by retrying their old key.

Limits: 16 MiB/file, 200 pages, 10 unexpired staging reservations per owner,
24-hour staging expiry, and 512 MiB of retained imports plus active reservations.
Soft deletion does not free retained-byte quota. Expired staging bytes are cleared
on the owner's next upload start. Migration is operator-controlled and is not
subject to the interactive quota. Capacity/retention policy must be set before
enabling uploads at scale.

The parser checks magic/EOF, actual page count and object structure, and rejects
encryption, scripts, actions, remote links, embedded files/forms and complex
object graphs. Parsing runs in a two-slot child-process pool with a ten-second
timeout; Linux additionally limits address space to 512 MiB and CPU to eight
seconds. This is structural validation, not an antivirus claim. The API serves
the original validated bytes and verifies their hash on download.

## Migration tool: prepare, review, apply, rollback

`scripts/archive_migration.py` defaults to read-only PostgreSQL and read-only
SQLite. Supply a consistent SQLite backup (not a live file copied alongside WAL)
and an operator-provided DATABASE_URL. Do not put credentials in plan files or
command arguments. Confirm the legacy timestamp timezone and the destination
session timezone before any real import; original snapshot JSON remains intact.

Example command shapes, with operator-selected paths and identifiers:

```text
python scripts/archive_migration.py --web-snapshot BACKUP --plan PLAN
python scripts/archive_migration.py --web-snapshot BACKUP --plan PLAN --plan-sha256 REVIEWED_SHA --migration-id ID --apply
python scripts/archive_migration.py --rollback --migration-id ID
python scripts/archive_migration.py --rollback --migration-id ID --apply
```

The dry-run records source hash, source key, destination UUID, owner UUID, content
hash and size, plus review reasons. Review it before apply. Apply requires the
exact reviewed plan hash, recomputes source/ownership, locks owner mappings and
never overwrites differing imports. Import IDs are deterministic; replay is a
no-op. The tool imports only existing mappings that agree in both databases.
Conflicts, unmapped owners, missing SQL snapshots, withdrawn owners or invalid
content remain outside the import and enter review. File-only legacy content
requires separate reviewed recovery; this tool does not guess a filesystem owner.

Rollback first previews, then hides only imports carrying that migration ID.
It does not delete originals, move identities, change balances, rewrite owner
IDs, remove audit records or drop schema. Reapplying does not unhide a deliberately
rolled-back import. This is not an account/credit merger: conflicting legacy
accounts and balances need an independently reviewed resolution plan.

## Coordinated rollout and rollback gates

1. Publish the reviewed local commits only after separate authorization. The
   follow-up pins web commit `140942a9f48616dd6cba7bb0cd3e8390cb95ef68`, which
   includes approved shared/V3 commit `6ca9c0b036ffbb54531469bfb48af0707fb08fd3`
   and the email compatibility fix. Publish and verify that web feature branch
   first, run `python scripts/check_shared_release.py`, then publish the server
   pin, then mobile. The guard refuses an absent or different remote web SHA.
   `.githooks/pre-push` enforces it for this server feature branch when installed
   with `git config --local core.hooksPath .githooks`; it is installed in this
   isolated server checkout. A new clone must install the hook too. Do not use
   `--local-only` as a publication check or bypass hooks with `--no-verify`.
   Full rollout gates below still apply after commits are published.
2. Back up PostgreSQL, SQLite and archive assets; review dry-run mappings and
   balances. Apply additive migrations 011 and 012 after existing 009/010. New
   code requires 012 even while shared archive flags are off.
3. Deploy matching web/API builds with shared flags false. Validate internal
   private-network session introspection and renderer access. Align provider
   app/client IDs, approved callback URLs, Google configuration, cookies and
   reverse proxy origins. No secret value is supplied by this change.
4. Run native Linux/container contracts and a controlled staging smoke with
   nonproduction accounts. Confirm parser resource limits and asset hashes from
   the actual image. Run reviewed imports and resolve required mapping review
   before enabling the shared reader on both services.
5. Set `SHARED_ARCHIVES_ENABLED=true` together on API/web, verify lists and
   downloads, then separately enable `ARCHIVE_UPLOADS_ENABLED=true`. Updated
   mobile clients use the common endpoint and require this reader rollout first.
   Old web POST/DELETE clients receive 409 and must refresh. The mobile local
   archive remains available if the server feature is not enabled.
6. Roll back upload acceptance first. Keep the shared reader available once new
   imports exist so those reports remain reachable. Do not drop additive tables,
   restore an old database over new ledger entries, or revert to an old web build
   that can create destructive/uncoordinated writes. A full rollback requires
   reviewing outstanding uploads, auth flows and data created after rollout.

## Verification

See `shared-archives-validation.md` for executed tests and limits. Only synthetic
local databases were migrated; no remote state was changed and no commit pushed.

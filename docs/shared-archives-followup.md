# Email compatibility and web pin follow-up — 2026-09-11

Scope: two blockers found during publication readiness review. Push, PR and
deployment remain prohibited. No production account, payment, database,
environment file or original dirty worktree was modified.

## Email registration and legacy behavior

The old new-email failure was `build_me_payload → ensure_platform_account → 409`
despite completed verification and shared mode OFF. The fix uses the authoritative
existing central `web_email` identity namespace. Its subject is `web:{externalId}`;
email text is never an account match or merge key.

New signup while the central bridge is enabled records a durable pending marker.
Verification is checked separately before the internal service can register it.
The central endpoint requires a verified-email assertion, matching subject and
explicit new-registration proof. Retrying a lost response and simultaneous
requests converge on one central owner and one initial grant. Registration
does not transfer legacy paid credits, overwrite a provider owner, or link a
same-email social identity. Existing central mappings continue to use their
original UUID and ledger.

When shared mode is OFF, the full HTTP new email signup, verification, login and
me sequence succeeds; unverified login remains 403. Older unmapped accounts can
still log in and read their original profile/value, with explicit review-required
metadata, without provisioning another initial grant. This also applies at zero
paid balance: prior free use cannot be inferred from that balance. Financial
actions still require reviewed mapping. When shared mode is ON, a fresh verified
signup can register, while older unmapped accounts require review. No old value
is imported during authentication.

## Coordinated web pin and publication gate

The approved web commit `6ca9c0b036ffbb54531469bfb48af0707fb08fd3` contains the
`web-a4-shared-v3-20260909` bundle and `web-a4-canonical-v3` semantic renderer.
The follow-up web commit is `140942a9f48616dd6cba7bb0cd3e8390cb95ef68`, a direct
descendant that changes email handling/tests/docs but no renderer asset bytes.
The server gitlink now points to this follow-up SHA.

The local submodule checkout was populated from that reviewed local web worktree
without network cloning; its origin was then set to the canonical GitHub web
repository. `.gitmodules` keeps its original canonical SSH URL. Neither URL
points to the original local dirty web checkout. The local Git distribution
lacks shell helpers needed for `git submodule update`, so native Git clone and
detached checkout were used to create a valid nested submodule repository.

Safe future publication order, after explicit authorization:

1. Verify the current remote target and publish the web feature branch by normal
   non-force push. Confirm its SHA is exactly the gitlink target above.
2. On the clean server checkout, run `python scripts/check_shared_release.py`.
   It checks the committed gitlink, submodule origin/checkout, V3 contract, exact
   renderer manifest bytes and canonical remote web branch SHA. It makes only
   read-only remote calls and blocks on missing/different remote commits.
3. Publish the server feature branch only after that gate succeeds. The installed
   `.githooks/pre-push` runs it again for `work/shared-archives-20260911` and fails
   closed. It requires standard Git and Python; `SHARED_RELEASE_PYTHON` may point
   to the local Python executable. `--local-only` is a checkout check, never a
   substitute for the publication gate. Do not bypass the hook.
4. Publish mobile's unchanged feature branch and verify every remote SHA and
   ahead/behind count. Review compatible commits before deployment; this order
   does not authorize main/master changes, deployment or store submission.

The web commit is intentionally still local in this assignment. A local gitlink
to it is reviewable; the gate prevents this server branch from being published
before its dependency becomes fetchable at the verified remote target. No push
was attempted while validating the gate.

## Validation and remaining release conditions

| Check | Result |
| --- | --- |
| Full server API and release-guard suite against the updated submodule | 84 passed, 0 failed/errors/skipped |
| Web bridge/renderer/V3/auth contracts | 36 distinct passed across full and impacted reruns |
| Mobile account/archive/artifact/widget contracts | 42 passed in the follow-up run |
| Approved web ancestry and renderer fixture | 6ca9c0b is an ancestor; all 9 renderer asset bytes unchanged and hashes match |

Detailed test results and the local/remote gate result are captured at completion
in the execution report and transient `C:/CodexWork/.shared-archives-validation/`
logs. Tests cover real HTTP email flows in both feature modes, no second grant,
lost response replay, actual PG registration concurrency, namespace/proof
rejection, old value preservation and provider/archive ownership conflicts.
The full server run uses the actual updated submodule checkout as its renderer
fixture; mobile upload/final/ownership and old client contracts are rerun too.

Native Linux/container validation, dedicated staging accounts and isolated data,
provider console configuration, immediate backup/restore verification and an
approved production target remain release conditions. They were not inferred
from passing Windows synthetic tests or from the new matching gitlink.

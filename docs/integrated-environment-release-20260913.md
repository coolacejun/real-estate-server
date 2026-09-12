# Integrated land/environment release candidate

The web submodule is pinned to
`5b50f83370bc140d4d899a5f0da1b812a35600cf`, a merge of prepared web `8a42a09`
and current main `3614743`. Main's total-building selection and archive UI are
preserved. The release gate now requires bundle `web-a4-shared-v3-20260913`.
Semantic renderer `web-a4-canonical-v3` and environment `environment-web-v2`
remain unchanged. No API source, database migration, Compose setting, source
query or mapping was changed in this integration follow-up.

Validation: server full suite 87 passed, zero failures/errors/skips, using a
disposable loopback PostgreSQL cluster and the pinned web Chromium renderer.
This covers auth, archive ownership, payment/debit idempotency and environment
contracts. The pinned web full suite passed 90 tests, including main's new UI
tests and exact 2/16/19 land chips. All 57 dataset hashes match; calculator and
manifest copies are byte-identical. City Hall category/narrative parity and
all three local QA PDF pages were verified. No production data was used or
changed by these tests. Linux image testing remains an operational gate.

Publication order: publish and verify the web work branch at the exact pin,
then run `scripts/check_shared_release.py` without `--local-only` and publish
the server work branch. A published branch is not proof of deployment.

Deployment is blocked until authorized production access and an actual
rollback baseline are available. Repository deployment authority is
`README.md` / `mini-deploy.sh` (Mac mini Docker Compose), not a GitHub workflow.
The documented SSH endpoint timed out; no live Git/image/config version or
backup was captured. The existing network-interface rollback script is not
an application release rollback and must not be used for this rollout.

Once access is restored: record current images, web/API source SHAs and safe
configuration backups; verify runtime/mounts and additive schema prerequisites
011/012 before replacing containers. Keep auth/archive/payment feature flags
unchanged, and never run legacy archive import as part of this release. Stage
exact manifest data in the web image and the API's read-only environment mount.
Enable the existing environment route on web and API with the matched release.
Verify web renderer first, then API V2, without redeploying unrelated services.
For failure recovery, restore the recorded service images/configuration; keep
additive schema and user/ledger/archive rows intact. No destructive reverse
migration is part of rollback. No live release or migration has been executed.

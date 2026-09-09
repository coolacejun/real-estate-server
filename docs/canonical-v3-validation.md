# V3 integration validation, 2026-09-09

Target: `coolacejun/real-estate-server`, based on default/main
`f7f760c5c00af679e34a271cf5d4c007b98c77a1`.
Branch: `work/server-canonical-v3-20260909`.
Web reference: `98c957b2c572d1379b75a52f07cfe61f7d155c3f`, verified manifest
`web-a4-shared-v3-20260909`. Existing web pin/deployment configuration is unchanged.

## Tests

All 57 test cases passed across the full run and one focused rerun: 56 passed
initially; one pre-existing internal-bridge case failed because the temporary
runner used a random service token instead of the test's fixed synthetic
token. Correcting the runner token made that case pass. No application change
was needed for that failure. No tests were skipped.

The suite used a fresh local PostgreSQL 17.11 cluster, SCRAM authentication,
loopback-only binding, synthetic users/data, and the existing 009/010
migrations. The cluster was stopped after validation. The isolated loopback
HTTP renderer called the actual reviewed web bundle and installed Chromium;
its request/response path exercised the API's existing internal HTTP client.
No production DB, account, environment snapshot or credential was used.

V3 coverage includes shared normalization/hash; semantic versions and device
profiles; unsafe input, image count and asset integrity checks; no-debit
preview; eight concurrent central reservations; repeated failure refunds;
retry completion; one debit for repeated successful final; owner-only archive
access; environment default-disabled/body/concurrency/deadline behavior; and
unchanged seven-source environment/legacy-profile fixtures. All four golden
JSON inputs also passed the updated JSON Schema validation.

This was a Windows run. The runner supplied a temporary `fcntl.flock` adapter
using real Windows byte-range locks solely for pre-existing land-import tests.
PostgreSQL locks/transactions and the report code were not mocked by this
adapter. Native Linux/container verification remains required; this result
does not certify Linux file-lock behavior or the production image.

## Canonical PDF comparison

The full synthetic fixture produced 10 A4 pages for each of preview, final and
archive regeneration. Extracted text, font sets, page boundaries and the
rasterized output of every page were identical (pixel comparison at 85 DPI).
All pages were visually reviewed, including the split 40-floor table,
unit/land fields, map, environment photo, opinion, enforcement reference and
building/land disclosure. No clipping, missing floor rows or fallback fonts
were observed. Only Pretendard Regular, Medium and SemiBold were embedded.

Canonical content SHA-256:
`e2a5a56b0c8e50770a29e777581b15127b9cfdc2cddd40d6f84745ca0a5d389c`.

| Artifact | Bytes | PDF SHA-256 |
| --- | ---: | --- |
| Preview | 194490 | `1ebd6a987ef9e80e929b6226541fc4632ef8b9b20c5eaaccbbac13a5cf67692b` |
| Final | 194490 | `e459debf9037342af9878b1e56d20230a6a5d5a34b1e50f3cb6ef0744c182581` |
| Archive regeneration | 194490 | `fd8b9e9175622c4dfb20fa05cbde07da75aa0e673743de27ad1c6a21f5528d47` |

Each API response artifact header was checked against its own returned bytes.
PDF byte SHA differs on regeneration, as permitted by the existing
canonical-JSON archive design; content and raster comparison are identical.
No persistent PDF storage or new schema was introduced to force byte equality.

## Scope and credential audit

The staged scope contains 14 text files, no binary assets, PDFs, logs, database
files or files over 1 MB. Added files contain only code, synthetic golden data
and documentation. Credential-pattern scanning found no private-key blocks,
GitHub/AWS/Google/Stripe tokens, JWTs or credentialed database URLs. Staged
`git diff --check` passed. AST comparisons confirmed 13 critical existing
authentication, asset, renderer HTTP, ledger, final and archive functions are
unchanged. The copied validator matches the reviewed source commit byte for
byte, and that source worktree remains clean.

## Remaining release gates

Deploy neither this branch nor the old pinned web tree alone as a V3 release.
The reviewed V3 web renderer must be separately integrated, and the intended
Linux/container image must pass the same tests. Validate mounted environment
datasets before enabling analysis. The working production environment,
compose/Docker settings, OAuth/store configuration, migrations and original
web working tree were not modified. No deployment, main merge, PR, release or
tag was performed.

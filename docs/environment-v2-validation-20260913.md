# Environment V2 / land pill verification

Local follow-up on `work/shared-archives-20260911`, completed 2026-09-13 KST.
No push, PR, main merge, deployment, production mutation, archive rewrite or debit.

- Web pin: `8a42a09d6f4981c3f658c9aa558a6e6d2bc796d0`.
- Mobile: `f481b2e88d4736a600b7c503ba3663114d868d6f`.
- Server calculator and data manifest are exact copies of the pinned web source.
- Release gate verifies the renderer manifest, both environment copies, all 57
  dataset hashes, gitlink/checkout identity and (for publication) the remote web pin.

## Verification

- Web: 82 distinct tests covered. Full discovery initially ran 79; its one missing
  Google dependency path was corrected and the failed test passed. Three added
  parity/boundary tests passed. After changes, the renderer/whole-report suite ran
  34 tests and the final environment suite ran 15; the final partial-radius case
  passed separately. No unresolved failure or skipped renderer test.
- Server: full isolated PostgreSQL/Chromium suite 87 passed, zero failures/errors/
  skips. After final rounding/partial changes, all 9 environment/release tests
  passed against the final pinned web checkout.
- Mobile: 113 related tests passed (104 selection/PDF/archive/account tests plus
  9 environment/version tests); targeted Dart analysis passed.
- V1 remains V1 with legacy API aliases. V2 rejects changed/unpinned datasets.
  Mobile rejects V1/unknown/missing-version responses and clears stale preview
  snapshots on offline refetch. Existing saved PDFs remain untouched.
- Actual Chromium computed CSS checks exact 2 core / 16 permitting / 19 all
  texts, order, 1px borders, 999px radii, 11.2px type, colors and in-bounds pills.
- Read-only authority web PDF pages 5, 6 and 8 and updated QA pages 1, 2 and 3
  inspected at 144dpi. No clipping, overlapping, empty chips or omitted text;
  long chips wrap naturally and land history continues correctly. Footer and
  environmental source-date notice are readable. The marker ran exactly once
  (`edit`, one PDF) immediately before the first QA PDF generation.

## Fixture and limits

The existing City Hall fixture is 37.5662952, 126.9779451. The supplied PDFs do
not identify their query coordinate; their numeric counts were not fabricated.
At the reproducible fixture, browser/shared/server agree: bus 102 (300m 23,
500m 60), rail 38, schools 19 (500m 1, 1km 4), amenities 876, parks 33,
security lights 0, CCTV 242 / 87 sites (300m 70). The original web PDF has
bus 105 / amenities 915 / CCTV 239, so exact numeric reproduction of that
unknown input is not claimed. Rail date 2024-12-31 and park date 2025-06-09
follow the same nearest-source policy and match the authority date labels.
No walking/driving route or travel time was invented where data was absent.

Evidence (untracked workspace output):
`C:/CodexWork/.shared-archives-validation/environment-v2-20260913/validation.json`
and adjacent test logs / reproducible request and canonical JSON.
QA PDF: `C:/CodexWork/output/pdf/city-hall-land-environment-qa.pdf` (three relevant
pages only). Original PDF SHA-256 values were checked unchanged. Temporary PNGs,
extracted text and authoring scripts were removed after visual inspection.

## Future authorized release

Publish web, verify that SHA remotely, then publish server pin and mobile. Stage
matching data and enable existing `ENVIRONMENT_ANALYSIS_ENABLED` on web and API
with the rollout; defaults/live `.env` were not changed. Verify those endpoints
before serving the new browser client and releasing mobile. Mismatched data or
a disabled/unreachable endpoint prevents new V2 analysis. Linux/container/live
staging was not available here. Original dirty repositories, stashes, marketing,
secrets, payments, login, debit and archive data were not modified.

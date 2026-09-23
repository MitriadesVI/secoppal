# ADR-073 Schema 3 fiscal evidence preservation R1

Candidate before: `8fd906de4d5c7950d9f424504bd18b5ebef906ac`.

Branch: `codex/adr073-schema3-fiscal-r1`.

The Schema 3 architecture and projection contract remain unchanged. This
repair preserves the fiscal document that Schema 2 actually accredited.

## Reproduction packet

- **Incident:** four nine-digit Schema 2 NITs were silently rewritten as
  eight-digit bases plus a mathematically coincident DV.
- **Reproducer:** `tests/test_adr073_fiscal_authority_r1.py` against
  `8fd906d`: 4 failed, 2 controls passed before repair.
- **Expected:** the nine digits remain the accredited `base_number`; declared
  DV is null and `split_validated` is false.
- **Actual:** DAPRE `899999083 → 89999908`, UNIATLANTICO
  `890102257 → 89010225`, Colegio Mayor `800144829 → 80014482`, and
  Córdoba `891080031 → 89108003`.
- **Positive control:** an unsplit nine-digit NIT loaded correctly.
- **Negative control:** an authorized ten-digit base+DV still loaded as 9+DV.
- **First observable divergence:** `fiscal_evidence.base_number` in the
  migrated sidecar. Query projection was not involved.
- **Verdict:** `CONFIRMED`.

## Full fiscal audit

`fiscal_evidence_audit_before.jsonl` and `fiscal_evidence_audit_after.jsonl`
contain all 612 entries with canonical, kind, source, raw forms, current and
expected base, DV fields, normalization reason, and classification. The frozen
Schema 2 NIT authorities are in
`tests/fixtures/adr073_fiscal_r1/schema2_nit_authorities.json`.

| Classification | Before | After |
|---|---:|---:|
| `PRESERVED` | 538 | 597 |
| `VALID_10_TO_9_DV_SPLIT` | 12 | 12 |
| `VALID_LEADING_ZERO_NORMALIZATION` | 0 | 1 |
| `INVALID_10_DIGIT_PRESERVED` | 2 | 2 |
| `UNAUTHORIZED_9_TO_8_SPLIT` | 59 | 0 |
| `REVIEW` | 1 | 0 |

The 59 unauthorized splits comprised 4 historical accreditations and 55
directory observations. The one `REVIEW` was the census-declared raw
`0899999061`, now normalized to `899999061` while retaining the raw form.
Bogotá therefore has 38 observations of `899999061` across 37 BuyerUnits;
no BuyerUnits were merged. ICBF `899999239` remains at 32 observations.

Exactly 60 entries changed: 4 accreditations and 56 observations. A
structural comparison against `8fd906d` proved that only `base_number` and
`check_digit` changed. Canonicals, source names, projection evidence, raw
forms, `scheme`, source, outcome, `shared`, and source provenance were preserved.

## Independent gates

- `fiscal_authority_parity.json`: **18/18 Schema 2 NIT authorities preserved**;
  594 source observations audited with raw facts unchanged;
  unauthorized 9→8 = 0; review = 0.
- Six D0 canaries: **6/6 concrete Schema 2 NITs preserved**, plus 6/6
  separate Directory `NOT_FOUND` observations. `NOT_FOUND` remains a source
  coverage observation, not negative fiscal evidence.
- `projection_parity.json`: 593/593 bindings, 1,186 binding projections,
  1,036 complement comparisons (`None == None`), 2,222 census comparisons;
  canonical, source-name, dataset, and semantic projection diffs all zero.

## Verification and boundaries

- R1 plus Schema 3/source/dogfood focal: 128 passed.
- Expanded Schema 3, Buyer Groups, Contract Activity, source projection,
  dogfood, municipal audit consumers: 386 passed, 4 explicit skips for
  Gazetteer-Wide V1.
- Ruff on touched Python: PASS. `make lint-core`: PASS. `git diff --check`:
  PASS. Semgrep: baseline 39, candidate 39, delta 0, scan errors 0.
- Global suite after the bounded oracle repair: 15,981 passed, 119 failed,
  2 errors, 23 skipped, 23 xfailed.
  The 120 red nodeids and statuses match the `8fd906d` global run exactly:
  candidate delta 0. The suite remains red on baseline failures.

Fresh ZEUS review of `a385550e` found a local oracle defect: changing an
entry's document `scheme` from `NIT` to `PASSPORT` could still yield PASS.
Two regression mutations first reproduced that false PASS (one accreditation,
one observation). The gate now records and protects `scheme`, requires `NIT`
for each historical accreditation, and classifies any other scheme as
`REVIEW`. The frozen before matrix was regenerated from the exact `8fd906d`
Git blob with the added field. Both mutations now fail the gate, and the
complete focal and global gates were repeated. This was a bounded oracle
repair; it did not alter product data or the normalization policy.

`GAZETTEER_WIDE_V1_SCHEMA3_COMPATIBLE = NO`. The auditor was neither run nor
adapted. The 570 `ADMISSIBLE` remain `NOT_EVALUATED`; promoted = 0.

`RUNTIME_CHANGED = NO` relative to `8fd906d`. `PRODUCT_DATA_CHANGED = YES`.
`VISIBLE_PRODUCT_BEHAVIOR_CHANGED = NO`, as measured by exact projection
parity. No merge, push, deploy, restart, BuyerGroup, alias, gazetteer,
QueryRouter, or SoQLBuilder change belongs to R1.

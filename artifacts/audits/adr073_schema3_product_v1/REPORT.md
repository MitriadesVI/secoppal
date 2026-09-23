# ADR-073 Schema 3 product migration

Base: `c9bac8a97d465ae8388d47d22a5f57c7fcd0d72c`

Branch: `codex/adr073-schema3-productive`
Scope: Schema 3 loader, sidecar, projection-only historical audit consumers, and tests.

## Integrity

The deterministic gate in `parity.json` compares the frozen Schema 2
projection fixture against the production Schema 3 loader. It computes every
count from the fixture and sidecar.

| Check | Result |
|---|---:|
| Schema | 2 → 3 |
| Bindings before / after | 593 / 593 |
| Canonical set, source names, datasets, projection provenance diffs | 0 / 0 / 0 / 0 |
| Binding × dataset comparisons | 1,186; 0 semantic diffs |
| Complement identities × dataset | 518 × 2 = 1,036; `None == None`; 0 diffs |
| Census sweep | 1,111 × 2 = 2,222; 0 diffs |
| Consistency | 1,186 + 1,036 = 2,222; 593 + 518 = 1,111 |

`sidecar_sha256_before` and `sidecar_sha256_after` are recorded in `parity.json`.
The representation hash changed; the projection did not.

## Identity and evidence

- `canonical_name` normalized remains the operational lookup key. There is no
  universal BuyerUnitId and no NIT uniqueness constraint.
- A synthetic contract test loads two BuyerUnits with the same accredited NIT;
  another loads a BuyerUnit with no fiscal evidence. Both retain independent
  projections.
- Product data has 18 fiscal `ACCREDITATION` records and 594
  `SOURCE_OBSERVATION` records. The latter never count as accreditation.
  Bogotá `899999061` occurs in 37 BuyerUnit observations and ICBF `899999239`
  in 32; these counts are observations, not BuyerUnit merges.
- The six D0 canaries each retain their prior positive accreditation and a
  separate `SECOP_DIRECTORY / NOT_FOUND` observation. `NOT_FOUND` is absence
  of attestation from that source, never negative fiscal evidence.
- Cases A–G from the approved design are covered by production loader tests.

`CANONICAL_RENAME_STABILITY = DEFERRED`.

## Protected scope

The signature and semantics of `project_buyer_names(canonical_name, dataset_id)`
remain unchanged. `SoQLBuilder`, `buyer_groups.py`, and Contract Activity
admission were not edited. No physical source name, canonical, dataset,
BuyerGroup, alias, gazetteer entry, or new binding was changed.

Frozen municipal audit snapshots still carry Schema 2. The adapter in
`scripts/audits/adr073_historical_projection.py` converts only their exact
source-name projection for historical tests. It discards their `identity_key`
and grants no fiscal authority. The production loader continues to reject
Schema 2.

`GAZETTEER_WIDE_V1_SCHEMA3_COMPATIBLE = NO`. Its four live inventory tests are
explicitly skipped pending a separately authorized Schema-3-aware re-audit.
The V1 auditor itself was not changed or executed as Schema 3 authority.

The approved 570-row matrix is byte-identical to the source design artifact:
all rows remain `NOT_EVALUATED`, all require gazetteer re-audit, and zero were
promoted. No row became `READY_TO_PROMOTE`.

`RUNTIME_CHANGED = YES`

`PRODUCT_DATA_CHANGED = YES`
`VISIBLE_PRODUCT_BEHAVIOR_CHANGED = NO`, as measured by exact projection parity.

## Verification

- Product parity gate: PASS, `SEMANTIC_PROJECTION_DIFF = 0`.
- Expanded focal gate: 370 passed, 4 explicit Gazetteer-Wide V1 skips.
- Query and relevant corpus tests: 364 passed.
- Ruff on changed Python scope: PASS.
- `make lint-core`: PASS.
- Semgrep `.semgrep/secoppal-laws.yml`: BASELINE 39, CANDIDATE 39, DELTA 0,
  scan errors 0.
- `git diff --check`: PASS.
- Global suite, first run: 15,926 passed, 121 failed, 37 errors, 23 skipped,
  23 xfailed. Exact baseline subset reproduced 120 red nodeids. The 37
  candidate-only nodeids were repaired within historical audit consumers.
- Global suite, final run: 15,965 passed, 119 failed, 2 errors, 23 skipped,
  23 xfailed. Its 120 red nodeids match the baseline subset exactly by nodeid
  and status: `CANDIDATE_DELTA = 0`. The suite remains red on baseline defects.

No merge, push, deploy, restart, Gazetteer-Wide run, or promotion was performed.
Independent candidate audit and integration remain separate decisions.

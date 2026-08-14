# Truth Layer main foundation integration

Date: 2026-08-14 (America/Bogota)

## Provenance and scope

- `SOURCE_MAIN_HEAD=3a7fb276f2f0f19eae0182785b80982703c4cb29`
- `SOURCE_EXPERIMENT_HEAD=ba028269a2d163da4a65a6a5a9b688c3ecfe0c03`
- `SOURCE_CLOSEOUT=TRUTH_LAYER_PROVEN_FOUNDATION_CLOSE_WORKSTREAM`
- `ACTIVE_TRUTH_LAYER_RESEARCH=CLOSED`
- `NEXT_PRODUCT_CONSUMER=PROCESS_OPERATIONAL_STATE`
- `VISIBLE_PRODUCT_CHANGE=NO`

The experimental branch is a linear 27-commit descendant of the observed main
HEAD. M1 is a selective transplant, not a merge or cherry-pick of the
experimental sequence.

## Preflight and baseline

Main contained four pre-existing untracked CLI conversation artifacts. The
experimental worktree contained the pre-existing untracked
`data/generic_enrichment_reviews.db`. None was read as source, copied, modified
or included.

- Full suite: `6592 passed, 2 skipped, 24 xfailed in 283.71s`
- Query corpus: `89/89 PASS, 0 FAIL`
- Dialogue corpus: `60/61 PASS, 1 pre-existing FAIL`
- Pre-existing dialogue failure: `GOB-DEPTO-001`, expected entity
  `GOBERNACIÓN DE BOYACÁ`, observed `None`
- Public oracles: captured before M1 from formatter, ranking, SoQL, pagination,
  canonical result constructors and byte hashes of public runtime files

## Inventory

`INVENTORY_COUNT=103`

| Classification | Count |
| --- | ---: |
| `FOUNDATION_KEEP` | 28 |
| `NEUTRAL_SEAM_EXTRACT` | 3 |
| `DORMANT_LIBRARY_KEEP` | 2 |
| `DEFER` | 2 |
| `EXPERIMENT_ONLY_DROP` | 46 |
| `OBSOLETE` for this transplant | 22 |

The three extracted seams were the experimental `shadow_adapter.py`,
`runtime_binding.py` and eager package root. The product transplant contains
neither experimental file.

## Foundation ported

Shared primitives:

- `models`
- `evidence`
- `sentinels`

Process authorities:

- `process_lifecycle`, including process resolution
- `temporal_value`, `temporal_range`, `temporal_resolver`, `local_wall_time`
- `visibility_resolver`

Contract authorities:

- `contract_performance`
- `contract_signature`
- `contract_changes`
- `contract_liquidation`
- `contract_lifecycle`
- `temporal_contract`

Dormant neutral libraries:

- `result_identity`
- `result_grouping`

All components are dormant: no production handler or normal application import
references the new package.

## Neutral process seam

`app/core/truth_layer/observers.py::observe_process_truth_axes` was extracted
from the proven neutral semantics. It accepts an already-transported row and an
explicit timezone-aware `reference_instant`; it returns lifecycle, resolution,
temporal, visibility and quality/reason evidence.

It has no query context, intent, topic, market, role, offerability, bid-ready,
SoQL, Socrata, warehouse, workflow, I/O or internal clock dependency. It does
not mutate its input.

## Safe contract seam

`app/core/truth_layer/observers.py::observe_contract_truth_axes` composes the
six independent contract axes and preserves the E1 temporal grounding from the
experimental runtime seam.

Mandatory invariant:

```text
fecha_fin_liquidacion > reference_instant
=> liquidation.completion != finished
```

A bare civil date equal to the reference date also does not prove completion.
Past completion remains supported, and flag/date conflicts remain explicit.

Raw contract resolvers are low-level semantic authorities. They are not safe
operational decision APIs in isolation; consumers must use the safe composition
seam.

## Import boundary

- `FORBIDDEN_TRANSITIVE_IMPORTS=0`
- Cold import observations, fresh processes: `28.175, 21.424, 22.095, 48.503,
  20.895, 20.073, 21.002 ms`
- `COLD_IMPORT_MEDIAN_MS=21.424`
- Observational ceiling: `100 ms median`

Importing `app.core.truth_layer.models` does not load SoQL, modality, workflow,
Socrata, warehouse, Streamlit, feedback or generic-enrichment modules.

## Public parity

M1 did not modify formatter, ranking, pagination, pagination buffer, workflow,
SoQL, API or UI files. Exact pre-M1 hashes protect those paths. Deterministic
oracles compare public keys, values, types, ordering and canonical JSON
serialization for representative normal results and pagination state.

`EXACT_PUBLIC_PARITY=PASS`

## Deferred work and known limitations

- `GROUPING_RUNTIME_WIRING=DEFERRED`
- `GROUPING_RUNTIME_BOUNDING_GAP=KNOWN`
- Evidence: 600 occurrences under one identity were approximately 385 KB;
  5000 were approximately 3.21 MB in the experimental serialized buffer.
- Any future runtime activation requires a physical-occurrence or byte budget
  plus an explicit `truncated` / `lower_bound` signal.
- `occurrence_key` identifies an observed position within a query window and
  TTL context. It is not immutable physical-row identity.
- `SOQL_FIELD_TRANSPORT=DEFERRED`
- `M1_FOUNDATION_INPUT_COVERAGE=library complete over complete rows; runtime
  field transport deferred`
- Approved identity divergences are non-string IDs, whitespace-only IDs and
  unsupported/unknown datasets. Parity is defined for valid rows of the two
  supported datasets.
- Opportunity policy/composition, role, offerability and bid-ready are not
  present.

## Experimental components dropped

No dogfood mode, allowlist, review database/store/UI, reviewer labels,
checkpoint selection, human-feedback metrics, export script, generic enrichment
payload, sidecar persistence, `shadow_adapter.py` or experimental
`runtime_binding.py` was ported.

## Validation evidence

- Foundation, identity and pure grouping focal tests: `790 passed`
- New observer, import-boundary and public-parity tests: `14 passed`
- Query corpus after M1: `89/89 PASS, 0 FAIL`
- Dialogue corpus after M1: `60/61 PASS`, identical pre-existing
  `GOB-DEPTO-001` failure
- Full suite after M1: `7396 passed, 2 skipped, 24 xfailed in 319.32s`
- Added passing tests: `804`
- `NEW_FAILURES=0`
- `NEW_SKIPS=0`
- `NEW_XFAILS=0`

## Final status

`M1_STATUS=M1_READY_FOR_INDEPENDENT_REVIEW`

No M2/product consumer was started.

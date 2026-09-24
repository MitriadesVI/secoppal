# TEST_ARCHITECTURE_V1_F3_AUTHORITY_CLOSEOUT

**Decision: GO_F3_CONTRACT.** The three acceptance conditions in
`TEST_ARCHITECTURE_V1.md` §7 are now met. This is authority to use the F3
Product Contract as a gate, not authority to merge, push or activate anything.
The branch remains isolated from main; no `app/**` file changed.

## Topology and scope

The earlier F3 continuation reconciled main `0e5c7420` by merge commit
`9de40664` and produced `eca85896` with eight READY scenarios. This closeout
changes only `PC-AUTH-001` and `PC-AUTH-004` from DESIGN to READY. The corpus
now has **10 READY and 10 DESIGN** among its original 20 mandatory items.
The 18 remaining `TBD_RUNTIME` occurrences live only in DESIGN.

## PC-AUTH-001: accredited buyer despite Actor collision

The source artifact was the local Actor alias index in the main checkout,
opened with SQLite `mode=ro&immutable=1` for a bounded exact lookup. At the
recorded snapshot, `alcaldia de barranquilla` returned `AMBIGUOUS`, method
`exact_ambiguous_alias`, with **four distinct CC candidates**. The index
metadata SHA-256 was
`aa85da5c4942f2693d8a4431c5d522b2390e8ddd1dcbc3cea6c5aecb36fe9735`.
The index was 662,409,216 bytes before and after inspection. The
[provenance receipt](../../tests/fixtures/product_contract_v1/actor_barranquilla_provenance.json)
records the bounded observation and minimization.

The frozen pack retains **two** different CC identities with the same public
buyer mention, the minimum needed for ambiguity. Document numbers,
occurrences and dates are synthetic; no source identities were exported.
The fixture manifest hashes the pack and receipt. Frozen process and contract
rows each contain one matching Barranquilla row and one wrong-buyer decoy.

Healthy workflow, Frame OFF: `entity_profile` / `entity_profile_overview`,
`entidad_resolved` equals the canonical buyer, no Actor scope or contractor,
both physical datasets called, exact buyer predicates in emitted SoQL, and
the response reports one process and one contract. A separate Frame ON test
observes the same buyer in the response and every executed source predicate;
all six source calls return one row. Frame ON publishes its scope through the
profile view rather than `resolved_params`, so the test checks executed SoQL
and visible result instead of inventing a legacy param.

**Mutation proof:** before the real QueryRouter parses the turn, the mutant
forces the complete buyer mention through `detect_actor_name` as an Actor.
The resolver then sees the two derived CC aliases: `AMBIGUOUS`, two pending
candidates, zero source calls, and no Entity Profile. The READY route oracle
turns red. The Frame ON control kills the same mutant.

## PC-AUTH-004: fuzzy top candidate has no identity authority

The AP3-F R7 dogfood typo `lmp ingieneria` motivates this synthetic offline
shape. The local real-index read-only probe returned fuzzy `CANDIDATES` and
a NIT at rank 1; the gate uses **two synthetic NIT aliases** with the same
tempting rank pattern. The healthy workflow returns two visible suggestions,
`actor_resolution_status=CANDIDATES`, no `actor_scope` or `contratista`, and
zero source calls. No source document number or real candidate name was copied.

**Mutation proof:** at `project_actor_resolution`, before QueryPlan and source
execution, the mutant applies the rank-1 candidate as an authoritative
`exact_unique_alias`. The real workflow then returns `actor_profile` and
makes eight frozen-source calls. The READY zero-call oracle turns red. A
probe that forged only the raw resolver result remained blocked downstream;
the committed mutant targets the first projection capable of laundering the
candidate into authority.

## Invariants, quarantine and gates

The registry retains 28 historical laws: **27 DEFENDED, 1 PARTIAL, 0
UNDEFENDED, 0 SUPERSEDED**. `INV-BUYER-NOT-ACTOR` now has a current Entity
Profile primary, but remains PARTIAL for the open actor + region + buyer
composition and the `PC-AUTH-002` / `PC-FU-002` designs. The process-state
candidate remains `CANDIDATE_PENDING_MAIN`, outside the 28 laws.

| Gate | Result |
|---|---|
| `pytest tests/product_contract tests/contracts tests/test_module_size_limits.py -q` | 60 passed. |
| All 62 invariant primary references | 300 passed. |
| Quarantine references excluding known `HD-BUYER-001` | 16 passed, 15 xfailed. |
| `HD-BUYER-001` exact Barranquilla control | 1 inherited failure: source rows absent under obsolete SQLite dialect. |
| Inventory `collect/build/render` | 16,227 nodeids, zero collection errors; artifacts refreshed. |
| Ruff on affected Python | PASS. |
| Semgrep `.semgrep/secoppal-laws.yml` | BASELINE 39, CANDIDATE 39, DELTA 0; no finding path changed. |
| `git diff --check` | PASS at pre-closeout. |

All executable Product Contract gates ran with outbound network denied by
macOS `sandbox-exec`, in addition to the harness's autouse network ledger.
**Corrí los gates listados, no la suite global en este closeout.** The previous
eight-READY global run and its exact main-attributed red set remain historical
evidence; the ten-READY candidate needs one global gate at final integration,
as requested. No historical Buyer/Actor test or runtime file was repaired.

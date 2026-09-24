# TEST_ARCHITECTURE_V1_F3_CONTINUATION

> **Later closeout:** `PC-AUTH-001` and `PC-AUTH-004` are READY in
> [TEST_ARCHITECTURE_V1_F3_AUTHORITY_CLOSEOUT.md](TEST_ARCHITECTURE_V1_F3_AUTHORITY_CLOSEOUT.md).
> The eight-READY measurements below are a prior snapshot.

## Scope and topology

F3 started at `474a3edf680b7f0fa2cab57be44dbcad20f96b09` on
`chore/test-architecture-v1-f3`. The only newer main commit at preflight was
`0e5c7420b0af085a2d563a568f34f629b73114f9`. It changed an ADR-073 audit,
script and test, with no overlapping F3 path; merge commit `9de40664` brought
that main commit into this branch without conflicts or runtime edits.

The mission remains test architecture only. There is no `app/**` delta, merge
to main, push, deployment or restart. All Product Contract source rows are
synthetic, versioned and checked by SHA-256 in `manifest.json`.

## Scenario decision

The corpus retains the same 20 mandatory items: **5 → 8 READY**, **15 → 12
DESIGN**. Every READY executes the real workflow through a frozen source,
passes its declared oracle and kills its declared seam mutant. Six READY cases
also kill an execution mutant before the frozen source evaluates SoQL. READY
contains no `TBD_RUNTIME`; the remaining 21 placeholders are confined to
DESIGN.

| Scenario | Invariant | Healthy result | Mutant and red detector |
|---|---|---|---|
| `PC-AUTH-003` | `INV-ACTOR-NAME-NO-GUESS` | PASS, two synthetic candidates stay unresolved | Select top actor → `actor_unresolved` |
| `PC-MONEY-001` | `INV-MONEY-POLARITY-PRESERVES-INTENT` | PASS, floor excludes low-value row | Remove floor → `params.valor_min`; source-wire removal → `results.must_exclude_ids` |
| `PC-MONEY-002` | `INV-MONEY-RANGE-ATOMIC` | PASS, contradictory range does not call source; valid range selects 125M, excludes 175M | Remove upper bound → `params.valor_max`; source-wire removal → `results.must_exclude_ids` |
| `PC-MOD-001` | `INV-SEM-STRUCTURED-DIMENSION-NOT-TOPIC` | PASS, public tender selects only the allowed modality | Remove modality → `params.modalidad_include_procedure_ids`; source-wire removal → `results.must_exclude_ids` |
| `PC-FAIL-001` | `INV-SOURCE-FAILURE-IS-NOT-ABSENCE` | PASS, injected timeout is error, not zero | Turn timeout into empty result → `outcome` |
| `PC-OPP-001` | `INV-TIME-OPPORTUNITY-VIGENCIA-NOT-CERTIFICATION` | PASS, fixed Colombia date includes today/future/recent unknown; excludes past/stale unknown | Remove deadline case → `soql.contains`; source-wire removal → `results.must_exclude_ids` |
| `PC-TIME-001` | `INV-STATE-FIRMADO-IS-NOT-A-STATE` | PASS, signed 2023 uses `fecha_de_firma`, selects 2023, excludes 2024 | Invent `Firmado` state → `absent_params.estado`; source-wire date removal → `results.must_exclude_ids` |
| `PC-DUAL-001` | `INV-DUAL-DATASET-SEPARATE-UNIVERSES` | PASS, both dataset wires are called | Omit contracts → `datasets_called.includes` at seam and execution |

Three oracle corrections preceded promotion. The earlier “procesos abiertos”
wording carries an explicit state and therefore does not exercise the O1B
default vigencia policy; `PC-OPP-001` now uses an opportunity request without
explicit state. An empty fixture made the monetary range take a disclosed
fallback; the new 125M/175M rows test exact range retrieval. A bare signed
year needs a topic under the current QueryPlan, so the signed-year case now
includes a minimal topic and two dated decoys. These observations do not
establish new product defects.

| DESIGN group | Still missing for READY |
|---|---|
| `PC-AUTH-001`, `PC-AUTH-002`, `PC-AUTH-004` | Incident collision pack and current Entity Profile buyer/actor route evidence; the real-index skip is not authority. |
| `PC-FU-001`, `PC-FU-002`, `PC-DS-001`, `PC-EP-001` | Minimal actor-selection or Entity Profile rows, executable follow-up and scope oracle. |
| `PC-MOD-002`, `PC-NEG-001` | Null-safe modality or object-negation source rows and result decoys. |
| `PC-FAIL-002` | Accredited non-timeout source-unavailable injection. |
| `PC-PAG-001` | A true page-two refined-universe fixture; a no-more response on two rows would be a weak oracle. |
| `PC-LOOKUP-001` | Frozen direct lookup source and exact route oracle; the separate reference lookup gap stays open. |

## Invariant registry and quarantine

All 28 historical laws were checked against `0e5c7420`: **27 DEFENDED, 1
PARTIAL, 0 UNDEFENDED, 0 SUPERSEDED**. All 61 primary references expanded to
299 passing cases. `INV-BUYER-NOT-ACTOR` remains PARTIAL until its Entity
Profile collision gate is READY. The separate
`INV-PROCESS-STATE-NO-SILENT-LOSS` is recorded under `candidate_pending_main`
with status `CANDIDATE_PENDING_MAIN`; it does not count among the 28 laws.

Quarantine retains six `OPEN_PRODUCT_GAP` and three `HARNESS_DEBT` entries.
This continuation found no new open product gap. It repaired a local harness
limitation while accrediting O1B: the frozen SQLite wire now evaluates the
exact Socrata `case()` deadline predicate without changing the recorded SoQL.
The harness fixes the planning clock for that synthetic scenario only.
Hypothesis checks known and unknown deadline semantics around the date
thresholds. Existing Buyer/Actor and Observer debts remain outside this edit.

## Gates

The Product Contract tests use an autouse network ledger. Expanded gates run
under macOS `sandbox-exec` with `deny network-outbound`; a preflight probe
confirmed an outbound TCP attempt fails with `PermissionError` while local
Unix sockets work.

| Gate | Evidence |
|---|---|
| `pytest tests/contracts tests/product_contract tests/test_module_size_limits.py -q` | 54 passed after the pending-main registry assertion. |
| All invariant primary references | 61 references, 299 passed. |
| `test_architecture_inventory.py collect/build/render` | 16,221 nodeids in 507 files, 0 collection errors; artifacts refreshed. |
| Ruff on affected Python | PASS, 0 findings at pre-closeout. |
| Semgrep `.semgrep/secoppal-laws.yml` | Baseline 39, candidate 39, delta 0; tests are outside the rule config's scan scope. |
| `git diff --check` | PASS at pre-closeout. |
| Global suite (`pytest tests -q --disable-warnings --junitxml=…`) | 16,056 passed, 119 failed, 2 errors, 23 skipped, 23 xfailed in 928.86 s. |

### Global failure attribution

The 119 failures and 2 errors represent **120 unique red nodeids**: one
`test_actor_profile_render` nodeid appears as both failure and teardown
error. They occur only in six preexisting Actor/Buyer test files. The exact
candidate red nodeids were rerun on `main` `0e5c7420` with the same outbound
network denial and isolated pytest temp directory. The baseline subset had
the same 120 unique red nodeids, 119 failures and 2 errors; one duplicated
request of the nodeid also passed. **Candidate-only red nodeids: 0.**

The missing SQLite `date_extract_y` function is visible in 74 candidate
failure/error events and was reproduced on an exact Buyer/Actor nodeid in
`main`. The comparison does not assign that cause to every other event; those
remain inherited red tests with separate root-cause work outside F3. The
complete nodeid set, per-file counts and log digests are in
`artifacts/audits/test_architecture_v1/global_f3_continuation.json`.

The full suite appended generated lines to two historical audit JSONL files.
Both paths were clean at preflight and were restored from the worktree's
pre-gate HEAD; they are absent from this candidate diff. The exact-nodeid
baseline rerun left `main`'s tracked dirty state unchanged.

The global run used the F3 working tree before the final tightening of one
opportunity response expectation. That expectation and its manifest hash were
verified by the final 54-pass focal gate. The global artifact records its
parent HEAD and dirty state rather than presenting that SHA as a content hash.

The inventory is a working-tree snapshot. Its recorded `head` is merge commit
`9de40664`, and `worktree_dirty_at_collection: true` records that the new
F3 continuation files were present but not yet committed. No JUnit duration
matching was supplied to inventory `build`; timings remain unmeasured there.

# TEST_ARCHITECTURE_V1_F3 — port ledger

> **Continuation:** the later main reconciliation, 8 READY scenarios,
> invariant revalidation and global closeout are recorded in
> [TEST_ARCHITECTURE_V1_F3_CONTINUATION.md](TEST_ARCHITECTURE_V1_F3_CONTINUATION.md).
> The F3 acceptance conditions are adjudicated in
> [TEST_ARCHITECTURE_V1_F3_AUTHORITY_CLOSEOUT.md](TEST_ARCHITECTURE_V1_F3_AUTHORITY_CLOSEOUT.md).

Base: `04426d19aec5cad104f6e4cb973d4fd84b7eaa34`. Historical source:
`59363501` on `chore/test-architecture-v1-f2-f2-5`. This ledger records the
decision **before** copying historical files; execution evidence and the
decision appear below.

## Port matrix

| Historical paths | Decision | Reason |
|---|---|---|
| `tests/contracts/registry.py`, `tests/contracts/test_invariant_registry.py` | PORT_ADAPTED | Revalidate current references and status vocabulary. |
| `tests/contracts/invariants.yaml` | PORT_ADAPTED | Reaudit all 28 laws, owners, and defenses against main. |
| `tests/product_contract/actor_pack.py` | PORT_AS_IS | Synthetic index builder still owns the pack. |
| `tests/product_contract/assertions.py` | PORT_ADAPTED | Add failure-sensitive and lineage oracles as warranted by READY cases. |
| `tests/product_contract/conftest.py`, `network_guard.py` | PORT_ADAPTED | Reprove network, telemetry and persistence guards on current runtime. |
| `tests/product_contract/frozen_source.py`, `harness.py` | PORT_ADAPTED | Current client/workflow APIs and source projection need executed checks. |
| `tests/product_contract/scenario_schema.py`, `test_product_harness_safety.py` | PORT_ADAPTED | READY and mutation proof change corpus and gates. |
| `tests/fixtures/product_contract_v1/actor_alias_rows.jsonl`, `source_rows/harness_canary.jsonl` | PORT_AS_IS | Synthetic fixture inputs; verify hashes and runtime behavior. |
| `tests/fixtures/product_contract_v1/manifest.json`, `quarantine.yaml`, `scenarios.yaml` | PORT_ADAPTED | New fixtures, quarantine review and READY promotions alter corpus. |
| `docs/testing/INVARIANT_REGISTRY_V1.md`, `PRODUCT_HARNESS_SAFETY_V1.md`, `TEST_ARCHITECTURE_V1.md`, `TEST_INVENTORY_V1.md` | PORT_ADAPTED | Historical conclusions are not current evidence. |
| `scripts/audits/test_architecture_inventory.py` | PORT_ADAPTED | Rerun inventory against current collection. |
| `artifacts/audits/test_architecture_v1/baseline.json`, `baseline.md`, `collection.json.gz`, `inventory.json`, `inventory_items.jsonl.gz` | SUPERSEDED | Historical snapshot; regenerate rather than copy. |

No historical file is dropped. Nothing under `app/**` is owned by F3.

## API compatibility preflight

- `Settings` still takes explicit feedback JSONL/DB and actor index paths.
- `SecopalWorkflow.run_query` still takes `today`; `SecopClient` still accepts
  an explicit transport and preserves it under timeout derivation.
- `ConversationStore` and `RadarStore` still accept explicit paths.
- `ActorNameResolver` still accepts a local index path.
- `QueryRouter.parse` exposes optional `text_lineage_request_id` and returns
  `ParsedQuery.text_lineage` with raw text, hashes, segments, tombstones and
  causal events. `SecopalWorkflow.run_query` does not itself return the view.
- The current `Outcome` helpers must be checked at the result boundary before
  any outcome oracle is promoted.

## Framing and scope

The expected causal mechanism is that ProductHarness executes the actual
workflow over frozen rows, while READY assertions observe its output and SoQL.
The cheap discriminator is the historical canary on current main. A green
scenario alone is not a product oracle: its declared mutant must fail through
`Turn.violations(expect)`. Source and identity decisions remain owned by the
runtime; this track only observes them. Unknown omissions and conflicts are
errors, and unproven cases remain DESIGN or quarantine entries.

## F3 evidence and decision

**STATUS: PARTIAL_GO.** The F2/F2.5 code runs on current main and five Product
Contract scenarios are READY with killed mutants. The remaining 15 scenarios
stay DESIGN. The old Buyer/Actor suite has inherited harness debt, so it is
not used as a green primary. F3 changes no `app/**` file and does not merge,
push, deploy or restart.

### Text Lineage

| Stage | Observable usage |
|---|---|
| BEFORE_F3_LINEAGE_USAGE | Historical harness recorded only query, result, params and source calls. It was delivered before Text Lineage became an opt-in public parser API. |
| AFTER_F3_LINEAGE_USAGE | `ProductHarness` opts into `QueryRouter.parse(text_lineage_request_id=...)` and retains the product's `TextLineageView` per turn. READY can assert raw text. The safety gate verifies replay integrity, equal params, SoQL, IDs and outcome with opt-in off/on, and kills a missing-view mutant. |

The view exposes original/current text, raw and seam hashes, live spans,
tombstones, causal events and authenticated descendant/origin queries. The
harness builds no parallel lineage. `SecopalWorkflow.run_query` does not
publish the view; direct lookup paths that bypass `QueryRouter.parse` produce
no view. A lineage expectation on such a turn fails explicitly.

### Inventory refresh

The historical committed artifact at `59363501` counted **14,439 nodeids in
442 files** (the planning approximation was ~14,462). F3's final collect-only
count is **16,187 nodeids in 506 files**, zero collection reports. The
classifier is heuristic; its F3 adaptation recognizes Product Contract and
registry/harness gates explicitly.
The inventory is a working-tree snapshot including F3 files. Its `head` field
is the parent commit `04426d19`, not a claim that these new nodeids existed in
that commit; `worktree_dirty_at_collection` records that distinction.

| Class | Historical | F3 | Delta |
|---|---:|---:|---:|
| DOMAIN_INVARIANT | 11,143 | 12,320 | +1,177 |
| AUDIT_HISTORICAL | 1,860 | 2,214 | +354 |
| PRODUCT | 1,094 | 1,194 | +100 |
| ARCHITECTURE | 173 | 228 | +55 |
| PROPERTY | 131 | 186 | +55 |
| UNKNOWN | 38 | 45 | +7 |

The largest family deltas are `other` +1,174, result/presentation +182,
channels/auth +150, actor +84, geography +74 and `product_contract` +41.
There are 1,686 workflow nodeids, 69 distinct `INV-*` literals in 88 file
mentions, 22 live opt-in nodeids, 7 static skips and 23 static xfails. No
matching current JUnit was supplied, so duration fields are unmeasured rather
than inferred. Full detail is in [TEST_INVENTORY_V1.md](TEST_INVENTORY_V1.md)
and `artifacts/audits/test_architecture_v1/`.

### Invariants and quarantine

All 28 historical laws were rechecked against current owners and test
references. A representative primary for each ran: **56 passed**. The F3
registry classifies 27 `DEFENDED`, 1 `PARTIAL`, 0 `UNDEFENDED`, 0 `SUPERSEDED`.
`INV-BUYER-NOT-ACTOR` remains partial because the Entity Profile collision
case has no READY gate. `INV-STATE-CONTRACT-UNIVERSE-CELEBRATED` is now defended
by a current product test with row and limit assertions, plus R5 exclusions.
See [INVARIANT_REGISTRY_V1.md](INVARIANT_REGISTRY_V1.md) for each law.

The six historical `OPEN_PRODUCT_GAP` entries and two historical
`HARNESS_DEBT` entries were reexecuted as 16 passes and 15 xfails across
their selected nodeids; no gap closed. `HD-BUYER-001` adds one harness debt:
`tests/test_buyer_actor_role_arbitration_v1.py::test_seeds` has 20 inherited
failures because its SQLite fixture lacks `date_extract_y`. An exact
`cartagena-0-off` failure and its `query_error`/SoQL were reproduced at the
base SHA and in F3. The product's buyer and money params were intact in the
trace; this failure is not evidence of a product defect. The old test is
outside the F3 edit scope.

`INV-PROCESS-STATE-NO-SILENT-LOSS` remains
`CANDIDATE_INVARIANT_PENDING_MAIN`; this registry does not import its separate
branch.

### Product Contract READY and mutation matrix

Five of 20 scenarios are READY; 15 remain DESIGN. No READY contains
`TBD_RUNTIME`; 23 placeholders remain solely in DESIGN. Six product gaps and
three harness debts live only in quarantine and never count green.

| SCENARIO | LAW | MUTATION | EXPECTED_FAILURE / observed detector |
|---|---|---|---|
| `PC-AUTH-003` | `INV-ACTOR-NAME-NO-GUESS` | Select first of two synthetic CC candidates at public result seam | `actor_unresolved` |
| `PC-MONEY-001` | `INV-MONEY-POLARITY-PRESERVES-INTENT` | Remove money floor at result seam; also remove it from frozen-source SQL before real workflow execution | `params.valor_min`; `results.must_exclude_ids` |
| `PC-MOD-001` | `INV-SEM-STRUCTURED-DIMENSION-NOT-TOPIC` | Remove modality at result seam; also remove it from frozen-source SQL before real workflow execution | `params.modalidad_include_procedure_ids`; `results.must_exclude_ids` |
| `PC-FAIL-001` | `INV-SOURCE-FAILURE-IS-NOT-ABSENCE` | Turn a real injected timeout into an empty result at public result seam | `outcome` and `outcome_reason` |
| `PC-DUAL-001` | `INV-DUAL-DATASET-SEPARATE-UNIVERSES` | Omit contract dataset at source-call seam and before real workflow execution | `datasets_called.includes` |

The ten scenario/declared-mutant tests and three additional execution-mutant
tests pass only when the healthy run has no violations and the mutant has the
named violation. These are synthetic offline controls, not claims about live
SECOP coverage. The READY runner selects only `status: READY` and calls
`Turn.violations(expect)` for every turn.

### Safety and gates

`tests/product_contract/conftest.py` installs an autouse network ledger and
rejects `live` tests. TCP, UDP `sendto` and DNS probes are blocked. Explicit
endpoint probes for Socrata, DeepSeek and Burr/PostHog are recorded and
blocked. Burr telemetry is disabled only in the test fixture; production
configuration is unchanged. `SecopClient` is real, with only transport
substituted; timeout derivation retains the frozen wire. SoQL is evaluated
over synthetic rows and persistence paths are checked against each scenario's
tmp root.

| Gate | Observed result |
|---|---|
| `pytest tests/contracts tests/product_contract tests/test_module_size_limits.py -q` | 43 passed after F3 edits. |
| Representative 28 primaries | 56 passed. |
| Quarantine selected nodeids | 16 passed, 15 xfailed. |
| `test_architecture_inventory.py collect/build/render` | 16,187 collected, 0 collection errors; artifacts refreshed. |
| Ruff affected Python | 0 findings at pre-closeout. |
| Semgrep repo laws | Full BASELINE 39, CANDIDATE 39, DELTA 0 by exact rule/path/line. New audit script has 0 findings; rule-config excludes tests. |
| `git diff --check` | clean at pre-closeout; rerun on frozen candidate. |
| Global suite | Not run. The exact historical Buyer/Actor node is red on base and F3; no runtime repair is authorized. |

The fixture manifest lists SHA-256 for every synthetic JSONL and YAML file.
The worktree branch is `chore/test-architecture-v1-f3`; base SHA is
`04426d19aec5cad104f6e4cb973d4fd84b7eaa34`. The final candidate SHA and
Git cleanliness are recorded in the handoff, because a document cannot embed
its own final commit hash.

# HCA fresh systemic remediation — hardened sanitized integration review

**Protocol:** `HARDENED`  
**Audited main:** `85b11bef2852587dbde58efb1f765070dbb8b9fc`  
**Audited feature:** `d157611a0213774d22dab1dd568369d255efa8b2`  
**Audited feature tree:** `dea8cebe88c5e6e116fd5a0d604023a0ce71e88c`

## Integration decision

`main` is an ancestor of the audited feature and the reverse is false, so a
fast-forward is mechanically available. It is not authorized: the feature
history contains eighteen explained but unauthorized historical hunks. The
final audited tree contains none of them. Promotion therefore uses new commits
from the audited main and restores only the accumulated, classified path set.

```text
HISTORY_CONTAINS_UNAUTHORIZED_INTERMEDIATE_CHANGE: YES
FAST_FORWARD_AVAILABLE: YES
FAST_FORWARD_AUTHORIZED: NO
UNEXPLAINED_HUNKS: 0
UNAUTHORIZED_HISTORICAL_HUNKS: 18
UNAUTHORIZED_SURVIVING_HUNKS: 0
```

## Complete hunk classification

The inventory is `git show --format= --unified=0 <commit>` over all eight
commits. Each row below is homogeneous: every hunk in its count has the three
listed classifications. The counts sum to all 144 historical hunks; the 109
`SURVIVES=YES` hunks equal the accumulated `main..feature` hunk count.

| Commit / hunk group | Count | Explained | Authorized | Survives at audited head |
|---|---:|---|---|---|
| `8d562981` state/runtime/tests/F601 | 31 | YES | YES | YES |
| `359965ed` atomic money range/runtime/tests | 9 | YES | YES | YES |
| `fd78a957` spell boundary/runtime/tests | 8 | YES | YES | YES |
| `70aa0ce` runtime, coverage, `parsed_control` setup | 22 | YES | YES | YES |
| `70aa0ce` disjunctive relaxation in `test_followup_mutations.py` | 1 | YES | **NO** | NO |
| `a82ec3e` conjunctive semantic property + tripwire re-anchor | 3 | YES | YES | YES |
| `a82ec3e` generated `_run_results.jsonl` addition | 1 | YES | **NO** | NO |
| `7154e342` removal of that generated blob from the index | 1 | YES | YES | NO |
| `56f04d97` remediation report, ADR and evidence | 36 | YES | YES | YES |
| `56f04d97` pre-existing untracked truth-layer documents added by `git add -A` | 16 | YES | **NO** | NO |
| `d157611a` removal of those truth-layer documents from the index | 16 | YES | YES | NO |
| **Total** | **144** | **144 YES** | **126 YES / 18 NO** | **109 YES / 35 NO** |

The untracked generated fixture retained in the original checkout has Git blob
`83d6c8f3665e0f18b84a6485c0f916636d96473f`, exactly the blob temporarily
indexed by `a82ec3e`. `7154e342` is therefore `INDEX_HYGIENE_ONLY`.

The base pagination assertion is obsolete by design because the parser now
consumes the control turn before the mutation gate. The final assertion is not
called “stronger than base”; it proves a different, conjunctive semantic
property: parser does not create the topic, the gate does not accept it, and the
final frame contains no control residue.

```text
BASE_ASSERTION_OBSOLETE_BY_DESIGN: YES
FINAL_SEMANTIC_PROPERTY_STRENGTHENED: YES
```

## Hardened predictive gate

The former runner was vacuous: it accepted missing money endpoints, checked
buyer/control mostly by token absence, counted only `query`, and hardcoded
`expectations_changed = False`. The sanitized review replaces it with the
separately frozen `predictive_expectations.json` and a runner that:

- checks exact min, max, clarification and execution for every money case;
- checks buyer dataset, resolved buyer kind/entity, exact topic and residue;
- checks control classification, semantic operation, final frame and outcome;
- instruments `query`, `count`, `aggregate`, including timeout-derived clients;
- requires total source calls to be zero for every blocked case;
- fails if the expectation SHA changes before or during execution;
- detects one deliberate mutation in each required family.

```text
BASELINE_INPUT_SHA: 68179d0bcbcfbfac5aa7a925f8ec78c261dd151daca22f66d3794866c914f414
EXPECTATIONS_SHA:   9d18eae36a82c4af4585c6920aa9ab3df3356b9f48a31597067842d8274148a8
EXPECTATION_CHANGE_COUNT: 0
PREDICTIVE: 12/12
SENTINELS: 7/7
MUTATION_STATE: DETECTED
MUTATION_MONEY: DETECTED
MUTATION_SPELL: DETECTED
MUTATION_BUYER: DETECTED
MUTATION_CONTROL: DETECTED
```

The no-currency sentinel `entre 700 mil y 900 mil` produces neither bound,
clarifies at the workflow guard, and makes zero source calls. `entre 500 mil y
3 millones` produces neither bound, requests precision and makes zero source
calls. No range executes partially.

## Workflow outcome E2E

`test_hca_fresh_outcome_e2e.py` seeds a previously successful conversational
frame and executes `borra los filtros` through `SecopalWorkflow.run_query`. It
asserts, jointly:

```text
needs_clarification: true
outcome: clarification
outcome_reason: clarification_requested
resolved_params: {}
source calls (query + count + aggregate): 0
zero-after-refinement recovery/message: absent
```

The causal `normal_flow.py` change is byte-pinned at
`28328fb9dd5f7b69c2336f8c84d29777de9e8dd0700a3937c3d70e4e9e5b88b9`;
the old pin was `405724be7f3aff06934314da50184515ddc384aa4c1381403843afb9d81ae8a5`.

## Gates and runtime parity

```text
F601_CONFIGURED: YES
F601_MANUAL_INTEGRATION_GATE: YES
F601_CI_ENFORCED: NO
CI_LINT_ENFORCEMENT: separate non-HCA-blocking tooling debt

LOCAL_PYTHON: 3.14.5
CI_SUPPORTED_PYTHON: 3.12
TESTED_PYTHON_312: YES (3.12.12)
PYTHON_RUNTIME_PARITY: PASS

PYTHON_312_FULL_SUITE:
  passed: 9384
  skipped: 11
  xfailed: 24
  failed: 0

HCA_CANONICAL_PYTHON_312:
  pass: 188
  total: 195
  broken_from_baseline: 0

FRESH_FROZEN_PYTHON_312:
  before: 30/52
  after: 37/52
  remaining_systemic_p0: 0
  remaining_systemic_p1: 0
  FRESH-CONTROL-005: ORACLE_SINGULARIZATION_ONLY
```

The hardened live runner refuses to pass with zero rows. The read-only run at
`2026-08-26T23:40:39-05:00` observed all three predicates:

| Canary | Rows | Source available | Predicate observed | Semantic | Hard filter |
|---|---:|---|---|---:|---:|
| critical opportunity/state | 5 | YES | YES | 0 | 0 |
| small money range | 2 | YES | YES | 0 | 0 |
| buyer action | 5 | YES | YES | 0 | 0 |
| **Total** | **12** | **YES** | **YES** | **0** | **0** |

## Frozen inputs and materialization proof

```text
HCA:                 a341405a79d74cdb43872cf937cd2b58f4ebb736afe4a2135ced9b6d389c352e
FRESH:               72a0c95f8a30019f77fbb3f6e9c82dfc851ccf3cb1d050607313086ce850ecce
PREDICTIVE_BASELINE: 68179d0bcbcfbfac5aa7a925f8ec78c261dd151daca22f66d3794866c914f414
PREDICTIVE_EXPECTATIONS: 9d18eae36a82c4af4585c6920aa9ab3df3356b9f48a31597067842d8274148a8
```

Immediately after explicit restoration of all 57 authorized accumulated-diff
paths, the integration index tree was exactly
`dea8cebe88c5e6e116fd5a0d604023a0ce71e88c`, byte-identical to the audited
feature tree, with zero untracked paths. Subsequent differences from the feature
are limited to authorized audit hardening: the three runner files, the frozen
expectation artifact, this review, and the workflow E2E tripwire. All other
audited feature paths remain byte-identical.

No canonical HCA input, fresh plan, predictive baseline, runtime semantic
expectation, or generated/untracked file is included by the integration.

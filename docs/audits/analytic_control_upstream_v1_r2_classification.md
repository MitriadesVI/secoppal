# ANALYTIC_CONTROL_UPSTREAM_V1_R2: pre-edit failure classification

Base `0c4f0d6e6935d82bd0afcb4b999ff8e6e220e551`; same canonical interpreter
`/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`. Before changing runtime
or tests, the six scoped suites produced **12 failed, 116 passed**. Every failed
nodeid below calls `admit_contract_activity`, `handle_contract_activity`, or the
analytics registry with raw text and synthetic `BUYER_PARAMS`, bypassing the
QueryRouter semantic evidence carried by Burr. Classification: **A=12, B=0, C=0**.

The original bare `Galapa` forms in most old tests are not buyer accredited by
the real QueryRouter. It classifies `galapa` as material, and the routed
QueryRouter → `parse_query` → Burr State → `execute_query` → registry → Contract
Activity path clarifies with zero SECOP calls. This is the correct control, not
a product behavior regression. The qualified equivalents below are independently
buyer accredited by the same route; their requested count, breakdown, period,
money, modality and state behavior reaches Contract Activity with 2 calls for
totals and 3 for breakdowns. The test migration must preserve semantic
assertions, replacing synthetic authority with real parser/resolver evidence or
an already accredited request when testing planning/runtime only.

| Failed nodeid (`tests/` prefix) | Original raw in old harness | Qualified routed witness (where changed) | Classification and witness |
| --- | --- | --- | --- |
| `test_contract_activity_narrow_sql.py::test_request_plan_and_result_are_immutable` | `¿Cuántos contratos tiene Galapa?` | `¿Cuántos contratos tiene la Alcaldía de Galapa?` | A; routed total, 2 calls. |
| `test_contract_activity_narrow_sql.py::test_identity_witness_failure_stops_after_one_call` | `¿Cuántos contratos tiene Galapa?` | `¿Cuántos contratos tiene la Alcaldía de Galapa?` | A; routed total, 2 calls with normal source; the test's duplicate source remains a separate runtime fault injection. |
| `test_contract_activity_narrow_sql.py::test_registry_hook_is_first_and_contract_activity_is_terminal_owner` | `¿Cuántos contratos tiene Galapa?` | `¿Cuántos contratos tiene la Alcaldía de Galapa?` | A; routed registry claims CA, 2 calls. |
| `test_contract_activity_narrow_results.py::test_complete_groups_publish_null_and_blank_buckets_visibly` | `Desglosa los contratos de Galapa por modalidad.`; `Desglosa por estado reportado los contratos de Galapa.` | `Desglosa los contratos de la Alcaldía de Galapa por modalidad.`; `Desglosa por estado reportado los contratos de la Alcaldía de Galapa.` | A; both routed breakdowns, 3 calls each. The first original also routed successfully; the old direct harness failed because its fallback parser lacked the configured resolver. |
| `test_contract_activity_narrow_results.py::test_legitimate_zero_requires_both_successful_calls` | `¿Cuántos contratos tiene Galapa?` | `¿Cuántos contratos tiene la Alcaldía de Galapa?` | A; routed total, 2 calls with normal source; zero source stays a separate runtime fault injection. |
| `test_contract_activity_filters_v2.py::test_strict_and_inclusive_money_survive_request_plan_and_fingerprint` | `¿Cuántos contratos superiores a 200 millones firmó Galapa?`; `¿Cuántos contratos de al menos 200 millones firmó Galapa?` | Same two queries with `la Alcaldía de Galapa` as subject | A; both routed totals, 2 calls; parser derives `valor_min=200000000`. |
| `test_contract_activity_filters_v2.py::test_complete_signing_period_survives_and_missing_period_stays_absent` | `¿Cuántos contratos firmó Galapa en agosto de 2026?`; `¿Cuántos contratos firmó Galapa?` | Same two queries with `la Alcaldía de Galapa` as subject | A; both routed totals, 2 calls; parser derives August bounds only for the first. |
| `test_contract_activity_filters_v2.py::test_positive_modality_and_current_state_filters_survive` | `¿Cuántos contratos por contratación directa firmó Galapa?`; `¿Cuántos contratos de Galapa figuran actualmente suspendidos?` | Same two queries with `la Alcaldía de Galapa` as buyer | A; both routed totals, 2 calls; parser derives the procedure IDs and `Suspendido` respectively. |
| `test_contract_activity_filters_v2.py::test_alpha_operation_that_the_host_grounds_differently_never_runs` | `¿Cuántos contratos hay en cada modalidad para Galapa?` | `¿Cuántos contratos hay en cada modalidad para la Alcaldía de Galapa?` | A; routed host evidence grounds a modality breakdown, 3 calls without Alpha; the test's two contrasting Alpha bindings are separate admission checks. |
| `test_contract_activity_filters_v2.py::test_operation_changes_the_plan_fingerprint_but_not_the_where` | `¿Cuántos contratos firmó Galapa?`; `Desglosa por modalidad los contratos que firmó Galapa.` | Same two queries with `la Alcaldía de Galapa` as subject | A; routed total 2 calls and breakdown 3 calls. |
| `test_contract_activity_filters_v2.py::test_alpha_receipt_is_separate_from_request_plan_and_sql` | `¿Cuántos contratos firmó Galapa?` | `¿Cuántos contratos firmó la Alcaldía de Galapa?` | A; routed total, 2 calls. Alpha receipt separation remains the test oracle. |
| `test_contract_activity_filters_v2.py::test_filter_combinatorics_preserve_comparator_or_absorb_unknown_material` | Template `¿Cuántos contratos de {más de|al menos} {amount} pesos firmó Galapa?` | Same template with `la Alcaldía de Galapa` as subject | A; representative `amount=1` routes with 2 calls and `valor_min=1` for both comparators. Hypothesis continues to cover the full range. |

Witness commands before edits:

```text
python -m pytest -q tests/test_contract_activity_narrow_corpus.py tests/test_contract_activity_narrow_sql.py tests/test_contract_activity_narrow_results.py tests/test_contract_activity_filters_v2.py tests/test_analytic_control_upstream_v1.py tests/test_analytic_control_upstream_v1_r1.py --junitxml=/tmp/ca-r2-preflight.xml
PYTHONPATH=. python /tmp/r2_classify.py
PYTHONPATH=. python /tmp/r2_equivalents.py
```

The scripts' outputs are `/tmp/r2_classify.jsonl` and
`/tmp/r2_equivalents.jsonl` in the local run. The exact 12 nodeids above were
read from the JUnit XML and pytest failure summary; the routed witness used the
real `QueryRouter(EntityResolver(app/data/aliases_db.json))`, Burr
`parse_query`, `execute_query`, and analytics registry with a recording source.

## Test migration ledger

The 12 failed nodeids retain their semantic assertions and expected operation,
filter, SQL, source-call, or failure behavior. Their old harness supplied raw
text and synthetic buyer params, then let CA reconstruct missing semantic
evidence with its own QueryRouter. The new harness uses
`tests.contract_activity_evidence.parsed_turn` or `accredited_turn` to call the
real configured QueryRouter, then passes its one complete receipt to admission,
handler, or registry. Positive cases with bare `Galapa` now use the qualified
buyer wording in the table; that is an input correction needed to put the test
at the admission stage it claims to test. The expected semantic results are
unchanged. Source fault injections (duplicate identity, malformed result,
timeout, zero, partition mismatch) remain unchanged and happen after
accreditation.

Other tests migrated because the old three-key API was removed:

- `test_contract_activity_narrow_corpus.py` keeps the frozen 30-case raw corpus
  and all expected outcomes byte-for-byte; each call receives the receipt from
  the same configured parser. Its checksum stays unchanged.
- `test_contract_activity_narrow_properties.py` uses qualified buyer wording
  and parser params/receipt before testing SQL and runtime invariants; its
  Hypothesis assertions are unchanged.
- `test_analytic_control_upstream_v1.py` and
  `test_analytic_control_upstream_v1_r1.py` pass the parsed receipt rather than
  separate optional pieces. Their P1 and 33-case semantic assertions are
  unchanged.
- `test_contract_activity_filters_v2.py` retains deliberate filter overrides
  when testing admission, but takes buyer identity and semantic receipt from
  the configured parser. The material rejection case also qualifies the buyer
  so it reaches the intended material filter check. Its asserted reason stays
  `UNSUPPORTED_MATERIAL_FILTER`.
- The committed 1,205-input blast runner recognizes the candidate's one
  receipt argument while preserving its older base invocation and frozen
  comparison fields. It adds no product runtime compatibility path.

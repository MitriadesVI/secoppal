# R2.1 declared oracle migration: repeated subject and buyer surface

The user authorized one migration in
`tests/test_analytic_control_upstream_v1_r1.py::test_property_masking_buyer_span_keeps_identical_material`.
The two preceding assertions remain: `explicit_content_occurrences` owns the
first occurrence, and its surface equals the requested word.

| Previous assertion | Replacement receipt-level contract |
| --- | --- |
| `assert material_evidence(residual).occurrences` | For each of Galapa, Barranquilla, Atlántico, and Lunaria, parse a count request with a distinct accredited buyer. The exact subject surface remains in `semantic_evidence.material_residual`; `ownership.content_spans` contains that surface and does not overlap `buyer_span`; Contract Activity does not answer and makes zero SECOP calls. |

The previous line asserted a token-ledger mechanism. H4 removes the
`topical or` bypass, so a word classified as structural by the shared material
classifier need not appear in `MaterialEvidence.occurrences`. The product
contract is carried by `explicit_content` → `OccurrenceOwnership.content_spans`
→ `material_residual` in `query_router`. The residual assertion is essential:
for Atlántico, Contract Activity clarifies on buyer source projection before
the material gate, so zero calls alone cannot detect lost subject matter.

## R2 versus R2.1 product witness

Raw form: `¿Cuántos contratos sobre {word} firmó {buyer}?`. Buyers are Alcaldía
de Galapa, Alcaldía de Barranquilla, Gobernación del Atlántico, and Alcaldía de
Galapa respectively. Both versions used the same configured resolver and
recording source.

| Word | R2 residual / content / CA / calls | R2.1 residual / content / CA / calls | R2 → R2.1 occurrence ledger |
| --- | --- | --- | --- |
| Galapa | `galapa` / `galapa` / CLARIFY / 0 | `galapa` / `galapa` / CLARIFY / 0 | `galapa` → `galapa` |
| Barranquilla | `barranquilla` / `barranquilla` / CLARIFY / 0 | `barranquilla` / `barranquilla` / CLARIFY / 0 | `firmo` → empty |
| Atlántico | `atlantico` / `atlantico` / CLARIFY / 0 | `atlantico` / `atlantico` / CLARIFY / 0 | `firmo` → empty |
| Lunaria | `lunaria` / `lunaria` / CLARIFY / 0 | `lunaria` / `lunaria` / CLARIFY / 0 | `lunaria` → `lunaria` |

## Required mutant

Temporarily removed `*content_surfaces` from the residual construction in
`app/core/query_router.py`, ran the migrated test, and restored the line.
The mutant failed at the mandatory residual assertion:

```text
FAILED test_property_masking_buyer_span_keeps_identical_material
word = 'Barranquilla'
assert 'barranquilla' in ()
1 failed in 1.30s
```

The direct `hypothesis.inner_test('Atlántico')` also raised `AssertionError` at
the same `normalize_text(word) in receipt.material_residual` assertion. The
restored test passed: `1 passed in 1.47s`.

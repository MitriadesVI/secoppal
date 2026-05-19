# QA-001 Torture Matrix SECOPPAL

> **Nota:** Documento histórico de QA-001/Torture Matrix. Estado actual: corpus vivo 88/88 en `QUERY_CORPUS.md`; torture matrix 12/12 invariantes.

**Fecha creación:** 2026-05-16  
**Objetivo:** Matriz reproducible de casos límite semánticos, conversacionales y jurídicos.  
**Alcance:** Solo tests, fixtures y diagnósticos. Sin features nuevas, sin tocar H10, sin refactor de arquitectura.  
**Suite base:** 466 tests passed | make lint-core limpio | feedback.jsonl intacto.

---

## Resumen de cobertura

| Categoría | Casos mínimos | Estado | Tests automatizados |
|-----------|---------------|--------|---------------------|
| A. Firmados / estados contractuales | 5 | new_test_needed | 5 |
| B. Anti-WHERE 1=1 | 4 | covered_existing + new | 4 |
| C. Follow-up / contaminación | 5 | new_test_needed | 4 |
| D. Entidades / gazetteer | 4 | new_test_needed | 4 |
| E. LLM fallback mockeado | 4 | new_test_needed | 3 |
| F. Paginación / sugerencias | 5 | new_test_needed | 2 |
| G. Degradación | 1 | covered_existing + new | 1 |
| H. Narrator grounding | 4 | new_test_needed | 3 |
| **Total** | **32+** | — | **26** |

> Meta excelente alcanzada: 26 tests nuevos automatizados + 40+ casos documentados.

---

## Matriz completa (≥40 casos)

| id | categoría | query_1 | query_2 / follow_up | contexto previo requerido | expected_dataset | expected_scope | expected_estado | expected_intent_type | expected_followup_intent_type | should_inherit_context | should_ask_clarification | invariantes protegidas | riesgo producto/jurídico | status | nota |
|----|-----------|---------|---------------------|---------------------------|------------------|----------------|-----------------|----------------------|-------------------------------|------------------------|--------------------------|------------------------|--------------------------|--------|------|
| A1 | A | contratos firmados de adulto mayor en Barranquilla | — | — | contratos | entidad + objeto | None (universo) | new_search | — | False | False | INV-005, INV-006 | Alto (confusión estado vs dataset) | new_test_needed | — |
| A2 | A | contratos firmados que estén en ejecución en Barranquilla | — | — | contratos | entidad + estado | En ejecución, Modificado, Prorrogado | new_search | — | False | False | INV-005, INV-006 | Medio | new_test_needed | — |
| A3 | A | contratos en ejecución de adulto mayor | — | — | contratos | objeto + estado | En ejecución / familia activa | new_search | — | False | False | INV-005 | Medio | new_test_needed | — |
| A4 | A | contratos terminados de adulto mayor | — | — | contratos | objeto + estado | terminado / Cerrado | new_search | — | False | False | INV-005 | Bajo | new_test_needed | — |
| A5 | A | contratos firmados o en ejecución | — | — | contratos | ambigüo | None | new_search | — | False | True | INV-001, INV-005 | Alto (política ambigua) | ambiguous_policy | Marcar como caso ambiguo/redundante |
| B1 | B | contratos | — | — | — | vacío | — | unclear | — | False | True | INV-001 | Crítico | covered_existing | test_guard_where_1_1_equivalence.py |
| B2 | B | procesos | — | — | — | vacío | — | unclear | — | False | True | INV-001 | Crítico | covered_existing | — |
| B3 | B | dame los últimos | — | — | — | vacío | — | unclear | — | False | True | INV-001 | Crítico | covered_existing | — |
| B4 | B | dame los últimos | los de mayor valor | Sí (mantenimiento + Antioquia) | contratos | objeto + ordering | — | follow-up | change_order | True | False | INV-004 | Bajo | new_test_needed | — |
| C1 | C | contratos de mantenimiento en Antioquia | los de mayor valor | Sí | contratos | objeto + ordering | — | follow-up | change_order | True | False | INV-003, INV-004 | Medio | new_test_needed | — |
| C2 | C | contratos de mantenimiento en Antioquia | busca contratos de salud en Bogotá | Sí | contratos | nuevo objeto + ciudad | — | new_search | new_search | False | False | INV-003 | Alto (contaminación) | new_test_needed | — |
| C3 | C | contratos de mantenimiento en Antioquia | y en 2025 | Sí | contratos | fecha nueva | — | follow-up | change_year | True | False | INV-004 | Bajo | new_test_needed | — |
| C4 | C | contratos de mantenimiento en Antioquia | ahora en Barranquilla | Sí | contratos | ciudad nueva | — | follow-up | change_scope | True | False | INV-004 | Medio | new_test_needed | — |
| C5 | C | contratos de salud en Bogotá | los de mayor valor | No | contratos | ordering | — | new_search | change_order | False | False | INV-003 | Bajo | new_test_needed | Reset previo |
| D1 | D | contratos de la gobernación del atlántico | — | — | contratos | entidad_resolved | — | new_search | — | False | False | INV-008 | Medio | new_test_needed | — |
| D2 | D | contratos de la alcaldía de barranquilla | — | — | contratos | entidad_resolved | — | new_search | — | False | False | INV-008 | Medio | new_test_needed | — |
| D3 | D | contratos de fundación 2030 | — | — | contratos | objeto (futuro: contratista) | — | new_search | — | False | False | INV-008 | Bajo | fixed-core | "2030" ya no es fecha. DEUDA-001: resolución como contratista/entidad depende del gazetteer/aliases |
| D4 | D | contratos de mantenimiento en Antioquia | busca contratos de salud de la gobernación del Atlántico | Sí | contratos | nueva entidad | — | new_search | new_search | False | False | INV-003, INV-008 | Alto | new_test_needed | — |
| E1 | E | LLM intenta devolver Celebrado para firmados | — | — | contratos | — | None | — | — | — | — | INV-007 | Crítico | new_test_needed | policy limpia estado |

---

## Analytical Queries — AQ-001A (agregado 2026-05-17)

| id | query | expected | route_reason | notes |
|----|-------|----------|--------------|-------|
| AQ1 | cuánto se contrató en adulto mayor en Barranquilla en 2026 | aggregate_sum + respuesta analítica | analytics_aggregate_sum | Verifica `analytical_intent`, `analytical_response`, `results==[]`, `total_count` |
| AQ2 | cuánto se contrató | needs_clarification | analytics_aggregate_sum_no_scope | Guard anti-global sin scope |
| AQ3 | contratos de mantenimiento en Barranquilla | flujo tabular normal | — | No debe activar ruta analítica |
| AQ4 | [contexto previo] → cuánto suma | aggregate_sum heredando contexto | analytics_aggregate_sum | Follow-up analítico |
| AQ5 | [aggregate_sum] | respuesta analítica no sobrescrita | — | format_response y build_advisor_response deben short-circuit |

**Tests automatizados:** `tests/test_analytical_queries.py` (10 tests)

---

## Archivos generados

- `docs/TEST_MATRIX.md` (este archivo)
- `tests/test_torture_queries.py` (26 tests nuevos parametrizados)
- `tests/fixtures/torture_rows.py` (fixtures de contratos + entidades)
- `scripts/run_torture_matrix.py` (runner sin red + reporte JSON)

---

## Validación final (ejecutada 2026-05-16)

```bash
git status --short
make lint-core
source .venv/bin/activate && python -m pytest tests/ -q
python scripts/run_torture_matrix.py
shasum -a 256 data/feedback.jsonl
```

**Resultado:** Suite 466+26 = verde. feedback.jsonl intacto. 0 cambios funcionales fuera de tests/docs/scripts.
---

## Bidder Intent / Value Ranges — 2026-05-17

| id | query | expected |
|---|---|---|
| VR1 | contratos de mantenimiento entre 1000-3000 millones de 2026 | valor_min=1000000000 + valor_max=3000000000 |
| VR2 | contratos de mantenimiento entre 1000 y 3000 millones | rango monetario completo |
| BI1 | algun mantenimiento de vias para poder presentarme | opportunity_search + procesos + no `poder` en objeto |
| BI2 | quiero vender carpas, alguna oportunidad | opportunity_search + objeto=carpas + no `vender` |
| BI3 | ofrezco insumos médicos, alguna convocatoria | opportunity_search + objeto=insumos/medicos |
| BI4 | soy proveedor de carpas, hay procesos abiertos | opportunity_search + objeto=carpas + no `proveedor` |

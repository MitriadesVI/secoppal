# QA-001 Torture Matrix SECOPPAL

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
| D3 | D | contratos de fundación 2030 | — | — | contratos | objeto | — | new_search | — | False | False | INV-008 | Bajo (confusión año/entidad) | bug_detected | "2030" capturado como año; entidad no resuelta |
| D4 | D | contratos de mantenimiento en Antioquia | busca contratos de salud de la gobernación del Atlántico | Sí | contratos | nueva entidad | — | new_search | new_search | False | False | INV-003, INV-008 | Alto | new_test_needed | — |
| E1 | E | LLM intenta devolver Celebrado para firmados | — | — | contratos | — | None | — | — | — | — | INV-007 | Crítico | new_test_needed | policy limpia estado |
| E2 | E | LLM intenta pisar entidad_resolved del gazetteer | — | — | — | — | — | — | — | — | — | INV-008 | Crítico | new_test_needed | gazetteer gana |
| E3 | E | LLM devuelve solo dataset | — | — | — | vacío | — | unclear | — | — | True | INV-001 | Crítico | new_test_needed | anti-WHERE 1=1 |
| E4 | E | LLM devuelve En ejecucion sin tilde | — | — | contratos | estado normalizado | En ejecución / familia activa | — | — | — | — | INV-007 | Bajo | new_test_needed | normalización |
| F1 | F | Query inicial con result_ids válidos | — | — | — | — | — | — | — | — | — | INV-010 | Bajo | new_test_needed | — |
| F2 | F | Follow-up "más resultados" | — | Sí | — | — | — | pagination_more | — | True | False | INV-010 | Bajo | new_test_needed | — |
| F3 | F | Selección "1" | — | Sí | — | — | — | suggestion_callback | — | True | False | INV-010 | Bajo | new_test_needed | — |
| F4 | F | Selección inválida "9" | — | No | — | — | — | unclear | — | False | True | INV-010 | Medio | new_test_needed | — |
| F5 | F | Reset + "más resultados" | — | No | — | — | — | unclear | — | False | True | INV-011 | Medio | new_test_needed | — |
| G1 | G | query estricta → 0 | degrade → filas | — | contratos | relajado | — | — | — | — | — | INV-002 | Alto (total_count=0) | covered_existing + new | test_degrade_query.py |
| H1 | H | Narrativa menciona entidad presente en rows | — | — | — | — | — | — | — | — | — | INV-012 | Bajo | new_test_needed | grounding pasa |
| H2 | H | Narrativa menciona entidad inexistente | — | — | — | — | — | — | — | — | — | INV-012 | Alto | new_test_needed | grounding falla |
| H3 | H | Narrativa con frase común ("Encontré resultados relevantes") | — | — | — | — | — | — | — | — | — | INV-012 | Bajo | new_test_needed | no falso positivo |
| H4 | H | Narrativa con cifra inventada | — | — | — | — | — | — | — | — | — | INV-012 | Alto | new_test_needed | grounding monetario falla |
| X1 | misc | contratos firmados del ICBF 2025 | — | — | contratos | entidad + fecha | None | new_search | — | False | False | INV-005 | Bajo | manual_later | smoke real |
| X2 | misc | procesos abiertos de SENA | — | — | procesos | entidad | Abierto | new_search | — | False | False | INV-006 | Bajo | manual_later | — |
| X3 | misc | "muéstrame los 3 de mayor valor" | — | Sí | contratos | ordering | — | follow-up | change_order | True | False | INV-004 | Bajo | manual_later | — |
| X4 | misc | reset command | — | Sí | — | vacío | — | reset | — | False | True | INV-011 | Bajo | covered_existing | — |
| X5 | misc | selección inválida sin historial | — | No | — | — | — | unclear | — | False | True | INV-010, INV-011 | Medio | new_test_needed | — |
| X6 | misc | follow-up con objeto nuevo explícito | — | Sí | contratos | objeto nuevo | — | new_search | — | False | False | INV-003 | Bajo | new_test_needed | — |
| X7 | misc | cambio de dataset explícito (procesos → contratos) | — | Sí | contratos | dataset nuevo | — | change_dataset | — | False | False | INV-003 | Bajo | manual_later | — |
| X8 | misc | "dame los últimos 10 de Antioquia" | — | Sí | contratos | departamento + limit | — | follow-up | pagination_more | True | False | INV-004 | Bajo | manual_later | — |
| X9 | misc | query con tilde en estado ("En ejecución") | — | — | contratos | estado normalizado | En ejecución | new_search | — | False | False | INV-007 | Bajo | new_test_needed | — |
| X10 | misc | query con entidad parcial ("gobernación atlántico") | — | — | contratos | entidad_resolved | — | new_search | — | False | False | INV-008 | Bajo | new_test_needed | fuzzy gazetteer |

---

## Bugs detectados (status = bug_detected)

| id | Descripción | Categoría | Riesgo | Detalle |
|----|-------------|-----------|--------|---------|
| BUG-001 | "fundación 2030" — "2030" es capturado como año por _extract_dates(), impidiendo la resolución de entidad | D | Bajo | El parser de fechas compite con la detección de entidades numéricas. Solo afecta entidades con números que parecen años. No confunde con FUNSOCOM. |

**Nota:** Ningún fix aplicado durante QA-001 por regla 6 del alcance. Queda documentado para futura iteración.

---

## Invariantes protegidas (verificación final)

- INV-001: Guard anti-WHERE 1=1 unificado (SCOPE_TOPIC_KEYS)
- INV-002: count_soql y select_soql comparten WHERE
- INV-003: new_search no hereda contexto
- INV-004: follow-up de ordenamiento sí hereda
- INV-005: firmados solo selecciona dataset contratos
- INV-006: estado explícito acota aunque aparezca "firmados"
- INV-007: LLM no emite estados inexistentes (Celebrado/Firmado)
- INV-008: gazetteer siempre gana sobre LLM
- INV-009: tests no tocan data/feedback.jsonl
- INV-010: result_ids nunca contiene strings vacíos
- INV-011: reset borra contexto conversacional
- INV-012: narrator no inventa entidad ni cifra

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
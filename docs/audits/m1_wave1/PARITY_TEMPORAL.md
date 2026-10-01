# M1 wave 1 — paridad temporal con `main` en el camino legacy

> **Superado en VALUE por `PARITY_VALUE.md`:** las referencias de este documento a «VALUE congelado» (pin por hash,
> sonda VALUE con 712 textos distintos y excepción de 53 compradores) quedan reemplazadas por la paridad completa de
> VALUE: `money_polarity.py` vuelve a los bytes de `main` y la sonda da 0 diferencias.

Rama `fix/m1-wave1-parity`, base `df36d686` (R6), `main` = `4ec99bc1`. Decisión del usuario (opción A):
el camino legacy lee el tiempo exactamente como `main`. La gramática temporal reescrita de wave 1 (D-TIME + R1-R6)
vive en un módulo que sólo importa la ruta de cita. Ningún arreglo temporal se rescata en legacy.

## 1. Separación de las dos gramáticas

* `app/core/temporal_semantics.py` es BYTE-IDÉNTICO a `main` (sha256 `cc3d5e8a…b5227`). Pin:
  `tests/test_m1_wave1_parity_pins.py::test_temporal_semantics_is_byte_identical_to_main`.
* La gramática reescrita (la de R6) es `app/core/time_grammar.py` (copia literal del `temporal_semantics.py` de R6 con
  docstring de cabecera). Dependencias exclusivas de la ruta de cita: `time_relative`, `time_window`,
  `temporal_window_anchor`, `temporal_noun_anchor`, `non_canonical_year`, `normative_citation`, `temporal_bounds`.
* Importadores de `time_grammar`: `time_authority` y `time_relative` (y éste último es importado a su vez de forma
  perezosa por `time_grammar.window_patterns`). `time_authority` no tiene importadores en `app/` (sigue dormida).
  Ningún módulo legacy importa nada de ese conjunto:
  `test_no_legacy_module_imports_the_quote_route` recorre `app/` (imports estáticos, relativos y literales dotados de
  `importlib`) y sólo permite al propio conjunto, `host_authorities/*` y `foundation_authority_*` /
  `foundation_evidence_window` (C1.1). El escáner tiene su propio test de no-vacuidad.
* VALUE congelado (`money_comparators`, `money_polarity`, `money_suffix_event`, `money_suffix_place`,
  `value_authority`, `foundation_value_direction`): pin por hash con el comentario «congelado M1 wave 1: ninguna
  gramática de dinero nueva entra por legacy».
* Mutación (corrida): añadir `from app.core import time_grammar` a `query_router.py` hace fallar
  `test_no_legacy_module_imports_the_quote_route`; añadir un byte a `temporal_semantics.py` hace fallar
  `test_temporal_semantics_is_byte_identical_to_main`. Ambos restaurados desde `main`.

## 2. Clasificación de los 45 archivos de `app/` que diferían de `main`

| # | archivo | destino | razón |
|---|---|---|---|
| 1 | `analytics/contract_activity/admission.py` | VUELVE_A_MAIN | R1/R2: `_period` comparaba contra la autoridad temporal (consumidor legacy) |
| 2 | `analytics/contract_activity/currentness.py` | SE_ELIMINA | R2/R3: «hoy» como vigencia; sólo servía al lector legacy de CA |
| 3 | `capability_binding/residual.py` | VUELVE_A_MAIN | R1: `period_conflict` (consumidor legacy de SUPPLIERS) |
| 4 | `capability_binding/supplier_contract.py` | VUELVE_A_MAIN | R1: `today` y `PERIOD_NOT_REPRESENTED` |
| 5 | `entity_profile/frame_turn.py` | VUELVE_A_MAIN | R2: `temporal_plan_block` en el turno de perfil |
| 6 | `entity_profile/turn.py` | VUELVE_A_MAIN | R2: ídem |
| 7 | `entity_resolver.py` | SE_QUEDA (H0) | `alias_owners`, compradores multi-dueño |
| 8 | `foundation_admission.py` | SE_QUEDA (H0) | guarda de comprador multi-dueño y de año calificado |
| 9 | `foundation_authority_contract.py` | SE_QUEDA (C1.1) | contrato de autoridades por dimensión |
| 10 | `foundation_authority_registry.py` | SE_QUEDA (C1.1) | registro vacío |
| 11 | `foundation_authority_values.py` | SE_QUEDA (C1.1) | valores tipados |
| 12 | `foundation_buyer_choices.py` | SE_QUEDA (H0) | opciones de comprador ambiguo |
| 13 | `foundation_evidence_window.py` | SE_QUEDA (C1.1) | ventana de evidencia |
| 14 | `foundation_geo_level.py` | SE_QUEDA (C1.1) | niveles geográficos |
| 15 | `foundation_messages.py` | SE_QUEDA (H0) | mensajes de comprador ambiguo y de TIME de Foundation |
| 16 | `foundation_runtime.py` | SE_QUEDA (H0) | pasa resolver y texto al mensaje |
| 17 | `foundation_time_coverage.py` | SE_QUEDA (H0) | guarda de calificadores de año de Foundation; sólo importa `app.utils.money` |
| 18 | `foundation_value_direction.py` | SE_QUEDA (VALUE) | congelado |
| 19 | `geo_authority.py` | SE_QUEDA (D-GEO) | |
| 20 | `geo_regions.py` | SE_QUEDA (D-GEO) | |
| 21 | `geo_window.py` | SE_QUEDA (D-GEO) | |
| 22 | `host_authorities/__init__.py` | SE_QUEDA (C1.1) | paquete vacío |
| 23 | `money_comparators.py` | SE_QUEDA (VALUE) | congelado |
| 24 | `money_polarity.py` | SE_QUEDA (VALUE) | congelado |
| 25 | `money_suffix_event.py` | SE_QUEDA (VALUE) | congelado |
| 26 | `money_suffix_place.py` | SE_QUEDA (VALUE) | congelado |
| 27 | `non_canonical_year.py` | SE_QUEDA (ruta de cita) | dependencia exclusiva de `time_grammar` |
| 28 | `normative_citation.py` | SE_QUEDA (ruta de cita) | ídem |
| 29 | `pagination_blocked.py` | SE_ELIMINA | R4: sólo servía a `handlers/pagination.py` |
| 30 | `period_consistency.py` | SE_ELIMINA | R1: sólo servía a admission/residual |
| 31 | `period_free_text.py` | SE_ELIMINA | R5/R6: sólo servía a `query_router` |
| 32 | `query_router.py` | VUELVE_A_MAIN | R5/R6: enmascarado de menciones y lectura de comprador con periodo (sólo temporal) |
| 33 | `temporal_bounds.py` | SE_QUEDA (ruta de cita) | dependencia exclusiva de `time_grammar` |
| 34 | `temporal_gate.py` | VUELVE_A_MAIN | R3/R5: marca de turno bloqueado |
| 35 | `temporal_noun_anchor.py` | SE_QUEDA (ruta de cita) | dependencia exclusiva de `time_grammar` |
| 36 | `temporal_period_question.py` | SE_ELIMINA | R3-R5: pregunta de periodo abierta; sólo servía al workflow legacy |
| 37 | `temporal_semantics.py` | VUELVE_A_MAIN | byte-idéntico; la gramática pasa a `time_grammar.py` |
| 38 | `temporal_window_anchor.py` | SE_QUEDA (ruta de cita) | dependencia exclusiva de `time_grammar` |
| 39 | `time_authority.py` | SE_QUEDA (ruta de cita) | importa `time_grammar` |
| 40 | `time_relative.py` | SE_QUEDA (ruta de cita) | importa `time_grammar` |
| 41 | `time_window.py` | SE_QUEDA (ruta de cita) | |
| 42 | `value_authority.py` | SE_QUEDA (VALUE) | congelado |
| 43 | `workflow/actions/parsing.py` | VUELVE_A_MAIN | R3-R5: pregunta de periodo abierta en `apply_context` |
| 44 | `workflow/handlers/pagination.py` | VUELVE_A_MAIN | R4: rechazo de paginación tras turno bloqueado |
| 45 | `data/geo_secop_variants.py` | SE_QUEDA (D-GEO) | |

Resultado: 10 vuelven a `main`, 5 se eliminan, 30 se quedan; `time_grammar.py` es el único archivo nuevo.
Ningún archivo mezclaba cambios temporales legacy con cambios que debieran quedarse (se verificó el diff de cada
uno de los 10 archivos que vuelven): no hubo que partir ninguno. `git diff 4ec99bc1 -- <los 10>` es vacío.
Ninguno de los 30 que se quedan importa un archivo que volvió o se eliminó (grafo de imports del árbol, ver §4).

## 3. Tests

* Retirados del suite de legacy: `922` tests, repartidos en `tests/test_m1_wave1_r{1}_temporal_consumers`,
  `r2_ca_hoy`, `r3_ca_hoy`, `r3_period_question`, `r4_explicit_answer`, `r4_marker_invariants`,
  `r4_pagination_blocked`, `r5_buyer_span`, `r5_period_answer_frame`, `r6_exact_buyer_wins` (completos) y las mitades de
  consumidor de `r2_anchored_windows` (120) y `r3_window_lexicon` (63). Sus casos son `1633` líneas de
  `E_TIME_REGRESSION_CASES.jsonl` (ver `README_E_TIME_REGRESSION.md`).
* Se quedan, apuntando a `time_grammar`: `test_time_authority.py` (841), `test_time_authority_r1.py` (9482),
  `test_time_grammar_anchored_windows.py` (49), `test_time_grammar_window_lexicon.py` (73),
  `test_time_grammar_noun_anchor.py` (150), `test_time_grammar_quantity_not_year.py` (11),
  `test_time_grammar_frozen_residue.py` (13, 8 xfail estrictos), `test_time_grammar_v2.py` (35, el bloque D-TIME que
  vivía en `test_time_semantics_v2.py`).
* Vuelven a su versión de `main`: `tests/test_time_semantics_v2.py`, `tests/test_truth_layer_public_parity.py`
  (el pin de `pagination.py` vuelve al sha de `main`), `tests/fixtures/legacy_shadow_inventory.json`.
* Se quedan VALUE y el resto de wave 1: `test_m1_wave1_r{1..4}_value_*.py`, `test_value_authority_m1d.py`,
  `test_foundation_*`, `test_geo_authority.py`, y `test_foundation_silent_paths.py` / `foundation_promotion_mutants.py`
  (VALUE).
* Nuevo: `tests/test_m1_wave1_parity_pins.py` (19 tests: pin de `temporal_semantics`, pin de VALUE, no-importación).

## 4. Gates (corridos, resultados literales)

**Paridad mecánica de parse.** `QueryRouter.parse(text, today=2026-10-01)` en `main` (árbol `git archive 4ec99bc1`) vs este
árbol, volcado completo del `ParsedQuery` (params, evidencia semántica, spans, residuales), hoja por hoja.
Corpus: **5471 consultas** (`PARITY_PARSE_CORPUS.jsonl`, sha256 `183e2e0bcd817faa98bcf8701532e29603d833cfaf22015266cc9bf327938e2e`):
Gold sintético 390 + Gold REAL 612 + 6 corpus HCA 325 + fixtures/corpus del repo 857 (diálogo, query, money,
modalidad, actor, perfil, SUPPLIERS) + `feedback.jsonl` real 6 nuevos (el resto ya estaba en el Gold REAL) + casos E-TIME
1541 (texto, turnos previos y controles) + cruce temporal sintético 1740 (53 periodos x 5 compradores x plantillas).
Resultado: **`textos con diferencia: 0`**, 2 excepciones idénticas en ambos árboles (`errores main/mine: 2 2`).
Sensibilidad: el mismo volcado sobre `df36d686` (R6) difiere de `main` en 1651 textos.

**Sonda VALUE** (2762 textos adicionales: todas las cabezas del léxico `money_comparators` x 4 montos x plantillas, y cada
literal monetario de los tests de VALUE; `PARITY_VALUE_DIFFS.jsonl`). `main` vs este árbol: 712 textos difieren,
**0 hojas temporales, 0 de ruta**. Todas las diferencias están en campos de VALUE (`valor_min`, `valor_max`,
`money_clarification`) y en las hojas que esos mismos comparadores consumen (`objeto`, residuales, evidencia):
570 vienen de D-MONEY (anterior a R1, ya idénticas en `7bd671e6`) y 142 de R1-R4 (flechas `->`/`=>`/`<->` ya no son
comparador; sufijos «o mayor/menor/superior/inferior»; «más o menos» = aproximación; sufijo plural + participio/lugar).
**Excepción que NO es sólo `valor_*` y requiere decisión:** en **53** de esos textos de sonda difiere también el campo de
COMPRADOR (`params.entidad` y `entidad_resolution`): frases de negación o de cota tras el comprador («no es mayor a 300
millones», «sin pasar de», «sin exceder», «que no deben superar», «no igual o menor a», y las flechas) hacen que el
comprador capture la frase de dinero. Antes (`main`) esas frases acababan en `money_clarification`; ahora el comprador
queda «Municipio de galapa no es mayor a 300 millones» (aclara por comprador, sin `valor_*`). 45 vienen de D-MONEY y 8 de R1
(flechas). Es la clase ya declarada en `R1_REPAIR.md` (P2-3, «el comprador se traga la frase de dinero»; fail-closed, no
ejecuta). Ninguno de los 53 está en el corpus de 5471. No lo toqué: VALUE queda congelado como está en R6.

**Paridad conversacional.** `run_conversation_holistic_audit.py` con `PYTHONHASHSEED=0`, los 6 corpus
(`conversation_holistic_v1`, `actor_hygiene_v1`, `control_language_v1`, `hca_final_p1_fixes`, `negation_focal_v1`,
`time_v2_focal`), `main` vs este árbol, `selection_token`/timestamps/ids normalizados: **369 casos, 442 turnos,
verdicts distintos 0, turnos distintos 0** (pass/fail idénticos por corpus: 171/24, 35/1, 41/1, 40/0, 22/2, 31/1;
los 29 FAIL son los mismos que en `main`). Fixtures de diálogo e2e (`test_dialogue_policy_corpus_min.py`,
`test_dialogue_e2e_invariants.py`, `test_money_v2_invariants.py`): `101 passed` en `main` y `101 passed` aquí.

**Autoridad por cita tras la mudanza.** Corrí `test_time_authority.py test_time_authority_r1.py test_time_grammar_*.py
test_time_semantics_v2.py`: `10685 passed, 8 xfailed` (los 8 xfail son los estrictos del residuo congelado).

**Pins y no-importación.** `tests/test_m1_wave1_parity_pins.py`: `19 passed`. Matados por mutación (arriba, §1).

**Focales (5 procesos).** 161 archivos de test (todos los que importan los módulos cambiados en esta ronda o los de VALUE,
más `test_orchestrator_golden.py`, `test_truth_layer_public_parity.py`, `test_foundation_authority_contract.py`, los
pins y `test_time_semantics_v2.py`), repartidos en 5 procesos de pytest: `22 failed, 1992 passed, 8 skipped` /
`1185 passed, 2 xfailed` / `10493 passed, 4 xfailed` / `1945 passed, 19 xfailed, 1 error` / `46 failed, 1902 passed,
2 skipped, 9 xfailed`. Los 68 fallos y el 1 error son de 4 archivos (`test_buyer_actor_independent_boundaries.py`,
`test_buyer_actor_role_arbitration_v1.py`, `test_buyer_actor_role_controls_v1.py`,
`test_actor_conversation_corpus.py`) y son el rojo PREEXISTENTE `date_extract_y` (fixture sqlite sin la función del
universo): el mismo `pytest` sobre `main` da `68 failed, 69 passed, 1 error` con EL MISMO conjunto de ids (`comm` vacío).
El intermitente `test_actor_pending_document_selection.py` pasó. No corrí la suite global.

**Lint y semgrep.** `ruff check` sobre los 27 `.py` tocados: `All checks passed!`.
`olympus-semgrep-delta --base df36d686 --config .semgrep/secoppal-laws.yml` sobre los `app/` tocados:
`TOTAL=1 BASELINE=1 NEW_DELTA=0 FIXED=0`.

## 5. Lo que no verifiqué

* Suite global (no se pide en la ronda). Las suites de `main` fuera de los 161 archivos focales las asumo iguales porque
  los 10 archivos que vuelven son byte-idénticos a `main`; pero `query_router` importa `money_polarity` (VALUE) y ese sí
  difiere de `main` (ver sonda VALUE).
* Persistencia/live: ningún test contra SECOP real; las corridas HCA usan SECOP simulado.
* `scripts/audits/m1_search/authority_probes.py` (Fase A) cita `temporal_semantics.py:73/136/…` en comentarios: no se reeditó.
* `docs/audits/m1_d_time/CANDIDATE.md` y `R1..R6_REPAIR.md` describen el estado previo a esta ronda (historia); no se reescribieron.

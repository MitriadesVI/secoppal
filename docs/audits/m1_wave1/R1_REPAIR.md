# M1 wave 1 — R1 repair after ZEUS NO_GO

Base de la ronda: `7bd671e6` (main `4ec99bc1` + Fase A + H0 + C1.1 + D-MONEY + D-GEO + D-TIME).
Reloj de las sondas: 2026-10-01. Escritor único de la ronda: ARES. Sin tocar el núcleo Foundation
(`foundation_*`, `interpretation_contract.py`, contrato de autoridades): `foundation_value_direction.py` NO
se editó; el arreglo de VALUE vive en `money_comparators.py` (el léxico compartido).

## Qué se reparó

| Hallazgo | Clase | Reparación |
|---|---|---|
| P1-1 consumidores de la autoridad temporal | B (fondo) | Módulo nuevo `app/core/period_consistency.py` (una responsabilidad: ¿el consumidor aplica el periodo que la autoridad resolvió?) + dos enganches mínimos: Contract Activity y SUPPLIERS A2 fallan cerrado |
| P1-2 test de no-import en rojo | A (forma) | Comentario de `geo_authority.py:341` reescrito sin nombrar el módulo; el test no se tocó |
| P2-1 guarda VALUE inclusiva / flecha | A | `money_comparators.py`: sufijos «o mayor/menor/superior/inferior», «más a» no es comparador, «o más de valor» = sufijo, «más o menos» = aproximación, `->`/`=>`/`<-`/`<->` no son `>`/`<` |

### P1-1 — mecanismo y reparación

Mecanismo (ZEUS): `contract_activity/admission.py::_period` lee el año con una regex propia. En la base el
turno se salvaba por accidente (las palabras «antes», «partir», «años»… quedaban en `objeto` →
`UNSUPPORTED_MATERIAL_FILTER`); D-TIME consume esos spans, el residuo desaparece y el lector ejecutaba
«antes de 2024» como 2024 y «últimos 3 años» sin fecha.

Regla aplicada: un consumidor que no sabe representar el periodo resuelto **aclara**; nunca lo
reinterpreta con otra regex, nunca ejecuta otro año, nunca descarta la fecha.

* `period_consistency.period_conflict(text, represented, today)` compara lo que el consumidor va a aplicar
  `(desde, hasta)` con lo que `resolve_temporal_scope` resuelve en el texto. Devuelve
  `TEMPORAL_UNRESOLVED` (la autoridad marcó error), `PERIOD_NOT_REPRESENTED` (cualquiera de las dos cotas
  difiere; una cota abierta es `None` a ambos lados) o `PERIOD_AMBIGUOUS` (un segundo periodo sobrevive al
  enmascarar el primero). Puro; el reloj entra por parámetro.
* Contract Activity (`_period`, lector legado, `binding is None`): conserva su lectura de año/mes-año, y la
  acepta sólo si ES el periodo resuelto y coincide con las fechas que el parse lleva. Si no, aclara
  (`SIGNATURE_PERIOD_NOT_REPRESENTED` / `_AUTHORITY_CONFLICT` / `_UNRESOLVED` / `_AMBIGUOUS`). No usa el rango
  completo: `planning.build_plan` exige `date_from` y `date_to` y el lector legado no sabe representar cotas
  abiertas ni ventanas; ampliar la capacidad sería cambio de producto. La ruta con `binding` ya usaba la
  autoridad (`filters.accredit_filters`) y aclara con `SIGNATURE_PERIOD_INCOMPLETE` ante cota abierta.
* SUPPLIERS A2: `residual.account` calcula `period_conflict` (sobre el texto sin el nombre del comprador, el
  mismo que usa para el residuo) y `decide` aclara con `PERIOD_NOT_REPRESENTED` antes de ejecutar. `decide`
  recibe `today` opcional (inyección en tests; producción sigue con `civil_today`).

### Evidencia de A2 (28 consultas que pasaron de aclarar a ejecutar)

La consulta final de A2 es correcta para cotas abiertas y ventanas relativas, y por eso se **declara y no se
bloquea**. Se probó de punta a punta con el plan real y un cliente falso (`decide` → `classify_entity_profile_turn`
→ `build_execution_plan` → `execute_jobs` → SoQL emitida): el WHERE lleva exactamente las cotas que existen
(`fecha_de_firma >= 'X'` sólo si hay `desde`, `<= 'Y'` sólo si hay `hasta`) para las 17 formas del oráculo,
y la respuesta que lee la persona dice el periodo («Firmados desde 2023-01-01», «hasta 2023-12-31», «del
2023-10-01 al 2026-10-01»). Tests: `test_suppliers_final_soql_carries_exactly_the_resolved_bounds` y
`test_the_profile_the_user_reads_states_the_period_it_applied`. Dos precisiones declaradas: «desde 2024 hasta
hoy» ejecuta `>= 2024-01-01` sin cota superior (decisión de D-TIME: «hoy» es el fin de la cota abierta), y la
vista corrige además un descarte silencioso de la base («la última semana» ejecutaba sin fechas).

### Cambios de comportamiento declarados de Contract Activity (sonda `ca.py` de ZEUS, 68 consultas)

* Contra el candidato `7bd671e6`: las 44 que pasaban a READY vuelven a CLARIFY (ahora con razón de periodo).
* Contra la base `4ec99bc1`: 0 consultas pasan de CLARIFY a READY. 16 pasan de READY a CLARIFY y las 16 eran
  ejecuciones equivocadas EN SILENCIO en la base: «en los últimos 6 meses» y «la última semana» (sin fecha),
  «septiembre del 2025» (año 2025 completo) y «2023 en adelante» (sólo el año 2023). Se declaran como
  corrección de un defecto de la base; «legacy sólo cambia en lo declarado» se cumple por esta sección.
* El parse que lleva fechas cuando el texto no nombra periodo ya no se descarta: aclara
  (`SIGNATURE_PERIOD_AUTHORITY_CONFLICT`).

## Tabla de consumidores de `temporal_semantics` y de `fecha_desde`/`fecha_hasta`

Enumerados con `grep` sobre `app/` (15 archivos mencionan `temporal_semantics`; unos 40 leen los params).
Columnas: qué lee; cota abierta (`None` en un extremo); ventana relativa; año inferido; veredicto.
El oráculo de cada fila marcada «test» vive en `tests/test_m1_wave1_r1_temporal_consumers.py`.

| Consumidor | Qué lee | Cota abierta | Relativa | Año inferido | Veredicto |
|---|---|---|---|---|---|
| `query_router` (`parse`, `_temporal_params`, `_extract_dates`) | `resolve_temporal_scope` → params; consume el span | sólo escribe la cota que existe; `_temporal_replace_scope` atómico | dos fechas | dos fechas | OK (test) |
| `dialogue_policy._detect_temporal_scope` | señales `temporal_month` (precision month) / `temporal_range` / `temporal_invalid` | `desde`/`hasta` pueden ser `None` | idem | idem | OK |
| `query_plan` P5/P5b | rellena params desde la señal sólo si no hay fechas; si hay, bandera `temporal_conflict` (sin consumidor) | acepta `None` | idem | idem | OK |
| `followup_engine`, `followup_mutations`, `conversation_store` | `fecha_*`, `_temporal_replace_scope` | un scope nuevo reemplaza AMBOS extremos (la ausencia informa) | idem | idem | OK (test) |
| SMMLV en `query_router._extract_amounts` y `followup_engine` | año de referencia de las dos fechas | un extremo ausente → `unsupported_multi_year` (aclara) | multi-año → aclara | año único → usa | OK |
| `source_occurrence` | sólo `matched`/`span`, no fechas | reconoce más colas como ortogonales | idem | idem | OK |
| `workflow/context.py`, `workflow/graph.py` | sólo `civil_today` (reloj) | n/a | n/a | n/a | OK |
| `workflow/helpers._relax_params` | quita el filtro de fecha y lo AVISA («sin filtro de fechas (pediste …)») | avisa | avisa | avisa | OK |
| `accreditation/authorities.time_authority`, `assembly` | params = sólo las cotas que existen; span acreditado | sí | sí | sí | OK (test) |
| `entity_profile/claim_adapters._period_claims` | `Period(lower, upper)` por cláusula | `None`-seguro | sí | sí | OK (test) |
| `entity_profile/scope`, `sections._period`, `effective_scope.humanize_period`, `render` | `time_from`/`time_to`; dice «desde»/«hasta»/«del … al …» | sí | sí | sí | OK (test + render de punta a punta) |
| `entity_profile/frame_*` (`frame_predicates`, `frame_render`, `frame_migration`, `frame_turn`) | `Period` abierto → emite sólo la cota existente; render «Desde/Hasta»; `PERIOD_CONFLICT` si el request y los params difieren | sí | sí | sí | OK |
| `entity_profile/temporal_event` | `_has_time_range`: cualquiera de las dos cotas | sí | sí | sí | OK |
| `actor_profile/scope`, `scope_render` | `date_from`/`date_to` opcionales; «Desde X» / «Hasta Y» | sí | sí | sí | OK (test) |
| `soql_builder._build_where` | un predicado por cota presente | sí | sí | sí | OK (test) |
| `response_policy._format_temporal_scope` | día/mes/año/rango/abierto | sí | sí | sí | OK (test) |
| `legacy_shadow._legacy_time`, `formatter`, `contextual_suggestions`, `recall_audit`, `refinement_recovery`, `export_xlsx`, `negation_policy`, `opportunity_*_policy` | presencia/ausencia de fechas, comparación de año completo o ventana estrecha; ninguno reinterpreta el texto | `None`-seguro | una ventana ≤ N días ahora califica un barrido de oportunidades (igual que ya hacía «últimos 7 días») | n/a | OK |
| **`analytics/contract_activity/admission._period`** | regex propia de año / mes-año | la leía como el año citado | la descartaba (sin fecha) | n/a | **FIX** (`period_consistency`; aclara) |
| `analytics/contract_activity/filters._period` (ruta con `binding`) | `resolve_temporal_scope` + `SIGNATURE_PERIOD_INCOMPLETE` | aclara | usa las dos fechas | usa las dos fechas | OK (test) |
| **`capability_binding/residual.account` + `supplier_contract.decide` (A2)** | span temporal contabilizado; params del parse | ejecutaba lo que el parse llevara, aunque difiriera del texto | idem | idem | **FIX** (guarda `PERIOD_NOT_REPRESENTED`) + evidencia SoQL |
| `time_authority.py`, `time_relative.py`, `time_window.py` | nueva autoridad de tiempo (D-TIME) | n/a | n/a | n/a | sin consumidor en runtime (módulos dormidos) |
| `foundation_time_coverage`, `foundation_admission` (H0) | no importan `temporal_semantics`; leen años con su propia regex para el borde de cobertura | n/a | n/a | n/a | fuera de alcance (núcleo) |
| `foundation_basic_fallback._filters` | muestra «fecha: 2023-12-31» para «antes de 2024» | oculta que es una cota | n/a | n/a | **DIFERIDO P2-4** (dueño G, núcleo) |
| `actor_intent`, `actor_conversation`, `negation_binding`, `entity_profile/turn` | regex de año para recortar nombres o detectar el ámbito de una negación; no deciden un periodo | n/a | n/a | n/a | OK (no ejecutan un periodo) |

Diferencial de parse sobre 315 consultas (35 formas × 9 plantillas, base vs `7bd671e6`): los únicos cambios
fuera de `fecha_*`/`objeto` son `ordering_signal` («últimos treinta días» ya no se lee como orden, corrección)
y el comprador de «enero - marzo de 2025» (ya no queda sin resolver). Ningún consumidor adicional lee un año por su
cuenta y decide con él.

## P2-1 — VALUE

* «300 millones o más a la fecha» + `greater_than`: el guarda leía «más a la» como comparador GT y el sufijo
  GTE perdía; el valor se ejecutaba como exclusivo. Ahora «más/menos» sólo enlazan con «de/que», y
  «… o más/menos de valor|a la fecha|que eso» es el sufijo inclusivo (GTE/LTE). «o más de 500 millones» tras
  otro límite sigue siendo un segundo comparador (ambiguo).
* Faltaban los singulares «o mayor/menor/superior/inferior» (y «y …»): ahora inclusivos.
* «-> 300 millones» ya no se lee como «>» (ni en el guarda ni en `money_polarity`, que comparte patrón);
  `->`, `=>`, `<-`, `<->` quedan sin leer → `VALUE_OPERATOR_UNGROUNDED` (aclara).
* «más o menos 300 millones» ya no es «300 millones o menos» en el guarda (quedaba como LTE legítimo).
* Pre-existentes NO tocados (declarados): `money_polarity` lee «más o menos 300 millones» como cota superior
  (idéntico en la base); «nada menos que 300 millones» se lee como «menos que» (idéntico en la base).

## Diferido, con dueño

| Hallazgo | Por qué no se repara aquí | Dueño |
|---|---|---|
| P2-3 el comprador se traga la frase de dinero → aclaración engañosa («Departamento de atlantico que no pasen de 300 millones») | clase pre-existente de captura de comprador; es fail-closed | D-BUYER |
| P2-4 búsqueda básica muestra «fecha: 2023-12-31» para «antes de 2024» | `foundation_basic_fallback` es núcleo Foundation | G (núcleo) |
| P2-5 candidato sin `candidate freeze` | proceso del orquestador, tras el commit de esta ronda | orquestador |
| Rojo «001 proveedores» | PRE-EXISTENTE: idéntico en la base `4ec99bc1` (ZEUS lo reprodujo) | — |

## Gates (resultado literal)

* Focal: `tests/test_m1_wave1_r1_temporal_consumers.py` + `tests/test_m1_wave1_r1_value_inclusive.py` →
  `370 passed` (incluye la propiedad Hypothesis de Contract Activity sobre año × tipo de cota).
* Expandido (no la suite global): 178 archivos —los 93 que mencionan los módulos tocados o
  `temporal_semantics`/`resolve_temporal_scope`, más `test_foundation_*`, `test_geo*`, `test_time*`, `test_temporal*`,
  `test_money*`, `test_value*`, `test_a2*`, `test_supplier*`, `test_capability_binding*`, `test_contract_activity*`,
  `test_entity_profile*`, `test_query_router*`, `test_dialogue*`, `test_query_plan*`, `test_followup*`, `test_m1_*`—
  en 5 procesos: `24796 passed, 1 skipped, 5 xfailed`, 0 failed (24 802 recogidos). Incluye
  `test_foundation_authority_contract.py::test_c1_1_modules_are_not_imported_by_any_runtime_module` en verde y las
  guardas de SHA de `test_foundation_projection.py` y `test_supplier_semantics.py`.
* HCA (`run_conversation_holistic_audit.py`, 6 corpus, `PYTHONHASHSEED=0`) sobre el árbol de la ronda vs `7bd671e6`
  extraído: `171/195`, `31/32`, `22/24`, `41/42`, `35/36`, `40/40` en ambos; 0 verdicts distintos y 0 de 442 turnos
  distintos (normalizando `selection_token`, «consultado el <fecha>» y `_opportunity_reference_instant`).
* `ruff check` sobre los 8 archivos tocados: `All checks passed!`.
* `olympus-semgrep-delta --base 4ec99bc1 --config .semgrep/secoppal-laws.yml` sobre los 8 archivos:
  `TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0`.
* Mutantes de los cambios de la ronda (20): 20/20 muertos. Supervivientes iniciales (5) → se añadieron tests:
  el chequeo texto-vs-autoridad de Contract Activity sin depender del parse, `_temporal_error`, cada cota por
  separado, el segundo periodo, y «más a» sin «o». Mutantes: saltar el conflicto de autoridad, de params o de
  `_temporal_error` en CA; ignorar o anular `period_conflict` en A2; comparar sin cotas, sólo `desde`, sólo
  `hasta`, sin el segundo periodo o sin error en `period_consistency`; quitar los sufijos singulares inclusivos
  (GTE y LTE), la regla sufijo-vs-cabeza, la regla del enlace «a», «o más de valor», el contexto de `>` y `<`
  (tres variantes) y la regla de «más o menos».
* No se corrió la suite global (la corre el orquestador).

Rondas de reparación usadas: 2 de 4 (R-A: supervivientes de mutación → tests; R-B: ajustes de los propios tests).
Ningún hallazgo C–F.

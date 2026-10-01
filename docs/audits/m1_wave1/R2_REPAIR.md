# M1 wave 1 — R2 repair after ZEUS NO_GO on `7d9af549`

Base de la ronda: `7d9af549` (candidato R1). Reloj de las sondas: 2026-10-01. Escritor único: ARES.
Sin tocar el núcleo Foundation (`foundation_*`, `interpretation_contract.py`, contrato de autoridades C1.1).

## Qué se reparó

| Hallazgo | Clase | Reparación |
|---|---|---|
| P1-A ventana relativa anclada a un año/mes explícito | B (fondo) | Módulo nuevo `app/core/temporal_window_anchor.py` (una responsabilidad) + enganche en `resolve_temporal_scope`: **falla cerrado** con `relative_window_explicit_anchor` |
| P2-1 Contract Activity, «hoy» como vigencia | A | Módulo nuevo `contract_activity/currentness.py` + enganche en `admission._period` |
| P2-2 VALUE «y mayor/menor» seguido de sustantivo | A | `money_comparators.py`: los sufijos adjetivales sólo son cota ante fin de texto, puntuación o un conector de lista cerrada |
| P3 `money_polarity` «más o menos» | A (alineado) | `money_polarity.py` y `read_comparators`: «más o menos (de) X» = aproximación → se aclara |
| Mutantes M15 / M16 / M17 de R1 | tests | `test_m1_wave1_r2_ca_hoy.py`, `test_m1_wave1_r2_anchored_windows.py` |

### P1-A — decisión, mecanismo y alcance

Decisión del orquestador (no cambiada): fallar cerrado; no se interpreta la ventana anclada (sería capacidad nueva).

Mecanismo: `_resolve_base` resuelve UN periodo por texto y los relativos van antes que mes/año, así que
«los últimos 3 meses de 2025» resolvía la ventana desde el reloj (2026-07-01..2026-10-01) y descartaba el año.

Regla (autoridad compartida, punto único): una **ventana** relativa (`trailing_months`, `trailing_years`,
`trailing_weeks`, `trailing_days`, `last_7_days`: «últimos N meses/años/días», «últimas N semanas», «la última
semana») más **cualquier otro periodo** que la propia autoridad reconozca en el mismo turno devuelve
`TemporalResolution(error_code="relative_window_explicit_anchor")` con una aclaración que nombra las dos
lecturas («1) los últimos 3 meses contados hacia atrás desde hoy; 2) el periodo «de 2025» completo; si querías la
ventana dentro de ese periodo, dime las fechas exactas»).

* «Otro periodo» no se enumera: es lo que `_resolve_base` reconoce cuando se blanquea la ventana (y «hoy», que es
  vigencia). Año, mes, mes de año, trimestres, rangos, ISO, «del año pasado», «de este año», «este mes», «hace 2
  años», «antes de 2024»… salen del léxico de la autoridad, que no se copia (no puede divergir).
* **No es adyacencia.** Una prueba de adyacencia es un vocabulario finito que «contratos de 2025 de aseo en los
  últimos 3 meses» esquiva. Elección declarada: el mismo turno. Costo: «contratos de 2025 y los de los últimos 3
  meses» también pregunta (es ambiguo igualmente; la base descartaba el año en silencio).
* Orden inverso («de 2025, los últimos 3 meses») cubierto. Un error de la propia autoridad que va primero
  (`invalid_date`, `reverse_range`…) conserva su código. Un año dentro de un nombre protegido
  (`protected_spans`) no es ancla. «hoy» tampoco («los últimos 3 meses hasta hoy» resuelve como en R1).
* Relativos que NO son ventana (`this_month`, `last_month`, `months_ago`, `this_year`, `last_year`, `years_ago`,
  `this_week`, `last_week`, `today`, `yesterday`) no cambian: la prueba exige que cada patrón relativo esté
  clasificado (`test_every_relative_kind_has_a_decision`), así que un patrón nuevo obliga a decidir.
* Llega a cada consumidor como aclaración (verificado punta a punta, ver «Evidencia»): SEARCH/proveedores/perfil por
  el gate temporal del workflow (0 llamadas a la fuente), follow-up (el scope confirmado no se muta), Contract
  Activity (`SIGNATURE_PERIOD_UNRESOLVED`), SUPPLIERS A2 (`PERIOD_NOT_REPRESENTED`).
* **Hallazgo propio, cerrado:** `classify_entity_profile_turn` y `run_profile_turn` (Frame) ignoraban
  `_temporal_error` y, al ser llamados directamente con los params del parse, devolvían un perfil SIN fecha (en R1,
  con la ventana disjunta; con el error nuevo habrían soltado la fecha: widening silencioso). El workflow nunca
  llega ahí (el gate lo corta antes), pero el consumidor ahora usa el mismo `temporal_gate.temporal_plan_block` y
  pregunta. Vale para cualquier error de la autoridad, no sólo éste (p. ej. `invalid_date`).
* `time_authority.py` (D-TIME, sin consumidor en runtime) mapea el código nuevo a `TIME_MULTIPLE_TEMPORAL_FACTS` en
  vez de caer en `TIME_COUNT_OUT_OF_RANGE`.

### P2-1 — «hoy» como vigencia en Contract Activity

`_period` (lector legado) comparaba su periodo con el de la autoridad, y la autoridad resuelve «hoy» como el día
de hoy. Se aparta **sólo esa palabra** y **sólo donde es vigencia del estado** (`currentness.py`):

* junto a un verbo de estado («figuran hoy», «están hoy», «hoy figuran», «se encuentran hoy»…) en un texto que
  nombra un estado reportado o desglosa por estado; o
* «a hoy» en un desglose por estado cuando es el único periodo del texto.

Se aparta también el periodo de un día que el parse derivó de esa palabra, y ningún otro. Todo lo demás sigue
con la autoridad y con la misma razón que R1: «antes de hoy», «hasta hoy», «desde 2024 hasta hoy», «de 2024 a
hoy», «firmó hoy», **«firmados hoy … figuran suspendidos»** (la firma es hoy aunque haya un verbo de estado en otro
lado: una primera versión de R2 lo abría; se cerró con la regla de posición), «figuran hoy» sin estado nombrado,
«desglosa por modalidad … a hoy». Paridad con la base en las cuatro consultas del hallazgo y en «figuran hoy en
ejecución», «firmados en marzo de 2025 … figuran hoy suspendidos», «en 2025 que hoy están en ejecución».

### P2-2 — VALUE: «y/o mayor|menor|superior|inferior» + sustantivo

Mecanismo: la tabla de sufijos inclusivos de R1 los leía en cualquier posición. Regla cerrada: el sufijo
adjetival (singular o plural, «o»/«y») es cota sólo si lo que sigue es fin de texto, puntuación (`, . ; : ? ! )`)
o un conector de `SUFFIX_CONNECTORS` (conjunciones y preposiciones: `y e o u ni pero sino que a al ante con de del
desde durante en entre hasta para por segun sin sobre tras`). Un artículo, un sustantivo («plazo», «cuantía»,
«valor», «monto»…) o cualquier otra palabra lo anula: el monto queda sin operador y se aclara. Aplica a los dos
consumidores de la tabla (`money_polarity` por regex; `read_comparators` por post-filtro sobre el texto original,
porque `leftover` ya tiene en blanco las cabezas «desde»/«hasta»). Los sufijos cuantificadores («o más», «en
adelante», «para arriba»…) no cambian.

Límites declarados de la regla cerrada (fail-closed): «300 millones o mayor firmados en 2025» (participio) y «o
mayor valor» ahora aclaran; en la base aclaraban también (R1 los abrió). «y menores plazos» ejecutaba `valor_max`
en la base (el plural ya era sufijo): ahora aclara (corrección de un defecto de la base).

### P3 — «más o menos»

`money_polarity` leía «de más o menos 300 millones» como `valor_max=300M` (idéntico en la base) y
`read_comparators` leía «más o menos **de** 300 millones» como `LT` (el «menos de» que sigue a «más o»). Ambos
alineados: «más o menos (de) X» es aproximación y **se aclara** (`money_clarification`, sin `valor_min/max`); no
se inventa una banda. La aproximación legada «por 300 millones» (banda ±20 %) no se toca. «más o menos X o
menos» → ambiguo (aclara).

## Tabla de comportamiento (base `4ec99bc1` / R1 `7d9af549` / R2)

Periodos (reloj 2026-10-01). «Parse» = `fecha_desde..fecha_hasta` de `QueryRouter.parse`.

| Consulta | base | R1 | R2 |
|---|---|---|---|
| «contratos de … en los últimos 3 meses de 2025» (parse) | 2025-01-01..2025-12-31 | **2026-07-01..2026-10-01** (disjunto) | aclara (`relative_window_explicit_anchor`), sin fechas |
| «… en la última semana de marzo de 2025» | 2025-03-01..03-31 | 2026-09-24..2026-10-01 | aclara |
| «contratos de aseo en Barranquilla en los últimos 6 meses del 2024» | 2024 completo | 2026-04-01..2026-10-01 | aclara |
| «procesos de la Gobernación del Atlántico en los últimos 3 meses de 2025» | 2025 completo | 2026-07-01..2026-10-01 | aclara |
| «… los últimos 3 meses del año pasado» | 2025 completo | 2025 completo | aclara |
| «de 2025, los últimos 3 meses» (orden inverso) | 2025 completo | 2026-07-01..2026-10-01 | aclara |
| «perfil de la Alcaldía de Galapa en los últimos 3 meses de 2025» (workflow) | 2025 completo | 2026-07-01..2026-10-01 | aclara, 0 llamadas a la fuente |
| mismo, `classify_entity_profile_turn` directo | 2025 completo | 2026-07-01..2026-10-01 | aclara (antes: sin fecha tras el error) |
| «¿Cuántos contratos firmó … en los últimos 3 meses de 2025?» (CA) | READY 2025 | CLARIFY `…NOT_REPRESENTED` | CLARIFY `SIGNATURE_PERIOD_UNRESOLVED` |
| «proveedores de … en la última semana de marzo de 2025» (A2) | EXECUTE marzo | CLARIFY `UNACCREDITED_RESIDUAL` | CLARIFY `PERIOD_NOT_REPRESENTED` |
| follow-up «y en los últimos 3 meses de 2025» tras «… en 2024» | — | ventana disjunta | aclara; el scope 2024 queda, 0 llamadas nuevas |
| «en los últimos 3 meses» / «la última semana» (sin ancla) | sin fecha (descartaba) | 2026-07-01..10-01 / 09-24..10-01 | igual que R1 |
| «primer semestre de 2024», «enero a marzo de 2025», «desde marzo de 2025», «2025», «marzo de 2025», trimestres | — | = | = (11 834 textos, ver «Evidencia») |
| CA «¿… figuran hoy suspendidos?» / «están hoy suspendidos?» | READY, sin periodo | CLARIFY `…NOT_REPRESENTED` | READY, sin periodo (= base) |
| CA «¿… firmados en 2025 por … figuran hoy suspendidos?» | READY 2025 | CLARIFY | READY 2025 (= base) |
| CA «Desglosa por estado reportado … a hoy» | READY | CLARIFY | READY (= base) |
| CA «antes de hoy» / «hasta hoy» / «desde 2024 hasta hoy» / «firmó hoy» / «firmados hoy … figuran suspendidos» | clarifica o ejecuta sin fecha | CLARIFY | CLARIFY, misma razón que R1 |
| «de 300 millones y mayor plazo» / «y menor plazo» (router) | aclara | `valor_min`/`valor_max` = 300M | aclara |
| «… o mayor» / «o superior» / «o menor» / «o inferior» (fin de frase) | aclara | `valor_min`/`valor_max` | = R1 |
| «… y menores plazos» | `valor_max` = 300M (plural era sufijo) | `valor_max` = 300M | aclara |
| «de más o menos 300 millones» (router y polarity) | `valor_max` = 300M | `valor_max` = 300M | aclara |
| «de más o menos de 300 millones» (read_comparators) | `LT` | `LT` | sin comparador (aclara) |

Todo cambio frente a la base es «aclara donde la base ejecutaba algo equivocado o ancho» o una corrección
declarada. No hay ninguna consulta de las sondas de ZEUS (57 periodos × 5 plantillas, 3 listas de consultas, 40
de SRCH, 44 de VALUE) que pase de aclarar a ejecutar frente a R1 salvo las cinco de P2-1 (que vuelven a la base) y
ninguna que pase de ejecutar a aclarar frente a R1 salvo las anclas de P1-A y los sufijos de P2-2/P3.

## Evidencia

* **Propiedad exhaustiva (P1-A)**, derivada del léxico (`temporal_semantics._RELATIVE_PATTERNS`, `_MONTH_ALIASES`,
  `_COUNT_WORDS`): 356 frases de ventana (5 clases × 22 cuentas, palabra y dígito × artículo/sin × tilde/sin) ×
  9 clases de ancla × 13 formas de posición (41 652 textos); y 36 ventanas representativas (por clase, artículo y
  tilde) × las 235 anclas del léxico (4 años, 200 formas de mes: nombre y abreviatura, con año unido por 6
  separadores, «mes de …»; 10 deícticos; 21 periodos/rangos/cotas/ISO) × las mismas 13 formas (109 980 textos).
  Las 13 formas son 8 conectores tras la ventana más 5 con el ancla antes, incluidas las no adyacentes
  («contratos de 2025 de aseo en …»). Los ~151 600 textos piden con el código y sin fechas. Cada ventana sola con
  cada cuenta resuelve exactamente la aritmética de calendario esperada. Rechazos de rango (`121 meses`, `31
  años`, `53 semanas`, `366 días`, `0 meses`) conservan su código.
* **Diferencial R1 vs R2** sobre 11 834 textos (anclas solas, ventanas solas, ventana + «hoy», ventana × ancla):
  9 720 difieren y los 9 720 son exactamente `relative_window_explicit_anchor`; 0 difieren sin ancla.
* **Punta a punta**: `run_query` con cliente falso (SEARCH contratos, procesos, perfil con Frame ON y OFF, proveedores,
  conteo CA): 0 llamadas a la fuente y la pregunta de aclaración; follow-up de tres turnos (el scope 2024 se
  conserva, el tercer turno «y en los últimos 3 meses» ejecuta con 2026-07-01).
* **Sondas de ZEUS** (probe, probe2×3, probe_s, val, ep, wf) re-ejecutadas sobre base, `7bd671e6`, R1 y R2: la
  tabla de arriba resume los cambios; `ep.py` pasa de «2025 completo» (base) / «2026-07-01..» (R1) a
  «clar: True» (R2).

## Mutantes de la ronda (47)

| Grupo | Mutantes | Resultado |
|---|---|---|
| P1-A (hook, patrones, blanqueo, `hoy`, `protected_spans`, orden con errores previos, span, frase, primera ventana, clarificación) | 18 | 13 muertos al primer intento; A10 (clarificación sin el ancla) y A15 (primera ventana) sobrevivían → 2 tests nuevos, muertos. 3 equivalentes: A8, A13, A14 (abajo) |
| Perfil (guarda de `classify`, de `run_profile_turn`, clarificación vacía) | 3 | 3 muertos |
| P2-1 (contexto de posición, «a hoy» sin desglose / con otro periodo, día del parse, máscara de «hoy», reloj, `_MONEY.sub`, gobierno de estado) | 12 | 11 muertos; C9 equivalente (abajo) |
| P2-2 / P3 (cola sin conector/fin/puntuación, artículo como conector, sufijo menor, plural, «más o menos», `de`, polarity, reader) | 11 | 11 muertos |
| Supervivientes de R1 no cubiertos arriba: M16 (guarda A2 sobre `text`), M17b (CA `period_conflict(today=None)`); M15 = C7 y M17 = C6/C10 ya van en el grupo anterior | 2 | M16 muerto; M17b equivalente (abajo) |
| `time_authority` | 1 | muerto |

Equivalentes argumentados:

* **A8** (`_anchored_window(text…)` en vez de `masked`): `masked` sólo blanquea «hasta hoy», y «hoy» ya está en la
  lista de ignorados que el módulo blanquea.
* **A13** (exigir `fecha_desde` para el error): una ventana siempre resuelve con `fecha_desde`; sólo las cotas
  abiertas de mes/año no la tienen y esas ramas van DESPUÉS de los relativos.
* **A14** (no exigir `matched`): `_resolve_base` devuelve `span=None` cuando no resuelve nada.
* **C9** (quitar `_HOY.fullmatch(found.phrase)`): la regex de posición exige que «hoy» esté dentro del contexto,
  y una frase que no sea «hoy» no cae dentro; es defensa en profundidad.
* **M17b** (`period_conflict(..., today=None)` en CA): ningún texto que el lector legado represente (un año o un
  mes-año escrito) coincide con una resolución dependiente del reloj sin disparar además `PERIOD_AMBIGUOUS`. M17
  de R1 era, pues, de comportamiento equivalente; en R2 el reloj SÍ decide en `currentness_hoy` y esa versión
  (C6/C10) muere con `test_hoy_is_read_with_the_injected_clock` (reloj inyectado 2031-05-15).
* M15: el autoridad protege «2025 millones» pero no «2025 cop»; `_MONEY.sub` lo cubre y
  `test_an_amount_that_looks_like_a_year_is_not_read_as_the_period` («por al menos 2025 cop en 2024») lo mata.
* M16: `test_a_buyer_name_that_carries_a_date_is_not_a_period_of_the_turn` («7 de Agosto», «20 de Julio», «2025»
  en el `buyer_mention`). El router lee la fecha del nombre antes de resolver el comprador («Hospital 7 de Agosto» →
  2026-08-07, comprador sin resolver): límite upstream **pre-existente**, no tocado; el test entrega el comprador
  como lo haría un scope arrastrado.

## Gates (resultado literal)

* Focal: `tests/test_m1_wave1_r2_anchored_windows.py` + `test_m1_wave1_r2_ca_hoy.py` +
  `test_m1_wave1_r2_value_adjectives.py` + los dos archivos R1 → `1648 passed`.
  Un test de R1 se editó: `test_a_second_period_in_the_text_is_ambiguous…` usaba «contratos en 2025 la última
  semana», que ahora es el error de P1-A; se cambió por «el mes pasado y en 2025» (el mismo propósito, un periodo
  calendario, no una ventana).
* Expandido (no la suite global), 5 procesos, 149 archivos —los que mencionan `temporal_semantics`,
  `resolve_temporal_scope`, `money_comparators`, `money_polarity`, `period_consistency`, `contract_activity`,
  `temporal_gate`, `entity_profile`, `capability_binding`, `supplier_contract`, `geo_authority`,
  `foundation_value_direction`, `time_authority`, `value_authority`, más `test_foundation_*`, `test_geo*`,
  `test_time*`, `test_temporal*`, `test_money*`, `test_value*`, `test_a2*`, `test_supplier*`,
  `test_capability_binding*`, `test_contract_activity*`, `test_entity_profile*`, `test_query_router*`,
  `test_dialogue*`, `test_query_plan*`, `test_followup*`, `test_m1_*`, `test_analytic_control*`—:
  `22196 passed, 1 skipped, 3 xfailed`, 0 failed (22 200 recogidos; los cinco procesos:
  `10162 passed 1 xfailed`, `4068 passed`, `3052 passed 2 xfailed`, `2305 passed`, `2609 passed 1 skipped`).
  Corrida sobre el árbol final de la ronda (después de los 47 mutantes y de todos los tests).
* Ampliación (180 archivos más que mencionan `query_router`, `QueryRouter`, `dialogue_policy`, `followup_engine`,
  `query_plan`, `SecopalWorkflow`): `5278 passed, 71 fallos/errores`; los 71 son los **mismos 71 ids en la base
  `4ec99bc1`, en R1 y en R2** (`OperationalError('no such function: date_extract_y')`, el rojo pre-existente de
  fixture sqlite; cuatro archivos: `test_buyer_actor_role_controls_v1`, `test_buyer_actor_independent_boundaries`,
  `test_buyer_actor_role_arbitration_v1`, `test_actor_profile_render` + 1 error de `test_actor_conversation_corpus`).
  Verificado extrayendo `git archive` de cada commit y comparando los conjuntos con `diff`: idénticos.
* El rojo conocido `test_supplier_request_authority.py::test_arbitrary_digit_boundaries_never_raise_or_silently_authorize`
  («001 proveedores») no apareció en mis corridas (pasa); es pre-existente e idéntico en la base según ZEUS; no se tocó.
* HCA (`run_conversation_holistic_audit.py`, 6 corpus, `PYTHONHASHSEED=0`) árbol R2 vs `7d9af549`:
  `171/195`, `31/32`, `41/42`, `22/24`, `40/40`, `35/36` en ambos (conversation_holistic_v1, time_v2_focal,
  control_language_v1, negation_focal_v1, hca_final_p1_fixes, actor_hygiene_v1); estados por caso idénticos; 0 de
  442 turnos distintos (normalizando `selection_token`, «consultado el <fecha>», `_opportunity_reference_instant` y
  marcas de tiempo).
* `ruff check` sobre los 13 archivos tocados (9 de `app/` y 4 de `tests/`): `All checks passed!`.
* `olympus-semgrep-delta --base 7d9af549 --config .semgrep/secoppal-laws.yml` sobre los mismos 13:
  `TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0` (con `--base`; no es vacuo: compara contra el árbol de R1).
* No se corrió la suite global (la corre el orquestador). Archivos ensuciados por los tests: ninguno (el árbol
  quedó igual salvo los archivos de la ronda; `observations.jsonl` y `product_trace.jsonl` no se modificaron).

## Riesgos y límites declarados

* **Regla del mismo turno** (P1-A): puede preguntar de más en «contratos de 2025 y los de los últimos 3 meses» o
  cuando un nombre de entidad lleva una fecha («Hospital 7 de Agosto» + ventana). Es preguntar, no ejecutar mal.
  El segundo caso ya era defectuoso en R1 (resolvía el día 7 de agosto y descartaba el comprador): límite upstream
  del router, no tocado.
* El guardia corre sobre cualquier ventana, también si un rango explícito ganaría la precedencia («los últimos 3
  meses de enero a marzo de 2025» resolvía antes el rango en silencio; ahora pregunta).
* **A2 muestra su propio texto** (`PERIOD_NOT_REPRESENTED`, «Reconocí un periodo…»), no la pregunta de la
  autoridad, cuando se llama a `decide` directamente; en el workflow el gate temporal responde antes con las dos
  lecturas. Alinear el texto de A2 sería tocar su contrato; no se hizo.
* La clarificación cita el texto normalizado del router («los ultimos 3 meses», sin tilde).
* Pendiente de verificar: nada que dependa de red o de SECOP (la suite y las sondas usan clientes falsos).
* Declarados y NO tocados: P2-3 (captura del comprador), P2-4 (búsqueda básica «fecha: 2023-12-31»), P2-5 (freeze
  del candidato, del orquestador).

Rondas de reparación usadas: 3 de 4 (R-A: diseño + primeros fallos de la propiedad; R-B: «firmados hoy»
abría una puerta → regla de posición, y las cabezas «desde/hasta» como conectores; R-C: supervivientes A10/A15 y
el mapeo de `time_authority`). Ningún hallazgo C–F.

# M1 wave 1 — R5 (final) repair after ZEUS4 on `8578db2c` (R4)

Base de la ronda: `8578db2c` (R4). Reloj de las sondas: 2026-10-01. Escritor único: ARES. Sin tocar el núcleo Foundation.
Política: hardening acotado (lo finito y derivable de catálogos se repara; el lenguaje abierto se congela y se declara).

## Hallazgos y reparación

| # | Hallazgo (ZEUS4 / global de R4) | Clase | Reparación |
|---|---|---|---|
| 1 | Global roja: `test_golden_pagination_more` falla con `pagination_blocked_turn` | B (regresión de R4) | `pagination_blocked.is_blocked` bloquea SÓLO por la marca de `temporal_period_question`. La compuerta temporal (`temporal_gate`) deja la marca en TODO turno que un error temporal no ejecutó (`reopen` ya no filtra por código), de modo que la fecha inválida y la pregunta de ventana siguen bloqueadas; una aclaración de otra ruta (P15, actor ambiguo…) pagina como en la base/R3 |
| 2 | Global roja: guarda de bytes de `handlers/pagination.py` | declarada | Re-pin declarado en `tests/test_truth_layer_public_parity.py` (ver «Re-pins») |
| 3 | **P1 bloqueante**: el comprador con ventana/año se cuela como TEMA (`objeto LIKE '%alcaldia%'`) | B (R4 lo agravó; la base ya lo hacía con año explícito) | Dos módulos pequeños y dos enganches en `query_router.py`: `period_free_text.without_period` (la gramática de actor lee el texto SIN el periodo que la autoridad temporal consumió, y sin el «en» inicial que lo introduce) y `period_free_text.aligned_pattern` (el span del comprador se vuelve a encontrar en el residuo aunque cambien los separadores) |
| 4 | P2: una respuesta a la pregunta de periodo que CAMBIA el frame ejecutaba un híbrido | B | `temporal_period_question.changes_frame`: tema, comprador, territorio, valor, estado, dataset u otra condición distinta del frame bloqueado no es respuesta; se vuelve a preguntar nombrando la salida (`Block.changed_frame`); una `new_search` completa ejecuta lo nuevo sin fusionar |
| 5 | P3: mutante R4-16 superviviente (`TWO_DIGITS` en `_YEAR_FORMS`) | test | `tests/test_m1_wave1_r5_quantity_not_year.py` fija que «de 25» no adyacente a la ventana es cantidad |

### Premisa corregida de la tarea 1

La orden inicial decía que el turno del golden «SÍ ejecutó y tiene filas». Verificado: «procesos abiertos en Barranquilla» es una
**aclaración P15** (`outcome=clarification`, `result_count=0`, 0 filas): el golden fija que «muéstrame más» tras ella pagina el alcance
(comportamiento de la base, `0799d7a5 test(demo,golden): update first-turn flow to P15 clarification`). La enmienda del orquestador
acotó la regla a lo que wave 1 creó (error temporal), que es lo que está implementado. La señal persistida real del error temporal es la
marca `_temporal_open_question` en `parsed_params` del `Turn`: `invalid_date` y `relative_window_explicit_anchor` la llevan desde R5
(antes sólo la llevaban los códigos que abren pregunta); el `Turn` no persiste el `risk_flag` ni `query_plan.reason`, y `outcome_reason` es
`clarification_requested` también en P15, así que ninguna otra señal distingue los dos casos.

### Deuda declarada: `PAGINATION_AFTER_NON_TEMPORAL_CLARIFICATION` (dueño M2 / paginación)

«Ver más» tras una aclaración NO temporal (P15, actor ambiguo, etc.) re-ejecuta el alcance incompleto, como en la base y en R3. R4 lo
negaba (más amplio que lo temporal); R5 lo devuelve a la base por decisión del orquestador. Conocidos: AH-28-SHOW-MORE, HCA-NEG-006 y
las 23 ejecuciones silenciosas de la base que midió ZEUS4. El caso AH-28 vuelve a responder `contract_universe_review_required` (idéntico
a R3).

## Tablas base / R4 / R5

### P1 — comprador con periodo (router; `objeto` = tema que viaja a SECOP)

| Consulta | base | R4 | R5 |
|---|---|---|---|
| «contratos de aseo con la alcaldia de barranquilla los ultimos dos años» | `[aseo]`, sin periodo | `[aseo, alcaldia]` | `[aseo]` + 2024-10-01..2026-10-01 |
| «… con la alcaldia de barranquilla **en 2025**» | `[aseo, alcaldia]` | `[aseo, alcaldia]` | `[aseo]` (mejora sobre la base) |
| «… con la alcaldia de barranquilla **del último año**» | `[aseo, alcaldia]` | `[aseo, alcaldia]` | `[aseo]` (mejora sobre la base) |
| «contratos de aseo **de** la alcaldia de barranquilla los ultimos dos años» | `[aseo, anos]` | `[aseo]` | `[aseo]` |
| «… con el municipio de galapa los ultimos dos años» | `[aseo]` | `[aseo, municipio, galapa]` | `[aseo]` |
| «… con la gobernación del atlántico durante los últimos 2 años» | `[aseo]` | `[aseo, gobernacion, durante]` | `[aseo]` |
| «que ha contratado funsocom con la alcaldia de barranquilla los ultimos dos años?» (REAL) | `[funsocom]`, ruta `actor_profile` (comprador + NIT), **sin periodo** | `[funsocom, alcaldia]`, ruta `heuristic_only` (sin NIT) | `[funsocom]`, ruta **`actor_profile` con comprador + NIT** y periodo 2024-10-01..2026-10-01 |
| `g2` (16 consultas): con palabras del comprador/ventana como tema | 6/16 | 10/16 | **0/16** |
| `g4` (80 consultas: 20 municipios × 4 formas + 20 gobernaciones): ídem | 0/80 | 37/80 | **0/80** |

La consulta real de funsocom vuelve a la ruta de la base (`actor_profile`, comprador + NIT `900334689`) y además aplica el periodo
que la base descartaba en silencio (R1: los consumidores aplican el periodo resuelto o preguntan).

### P1 — propiedad exhaustiva (11 639 consultas; `run_prop.py`)

Compradores (155 alcaldías —15 de núcleo, 60 con nombre compuesto del catálogo DIVIPOLA y 80 al azar—, 33 «municipio de X»,
32 gobernaciones, 80 entidades del catálogo `entidades_raw` y 8 entidades frecuentes) × 17 formas temporales (año explícito, mes-año,
rangos, anclas, ventanas de ambos léxicos) × 4 conectores («con», «de/del», «para», «en») × 5 posiciones (después, antes, al inicio,
entre comprador y tema, tema al final). Por consulta se compara contra la MISMA consulta sin el periodo. Sólo cuentan consultas cuyo
comprador resuelve sin periodo y que no pregunta (`_temporal_error`) ni usan la forma no leída «el último mes»:

| | base | R4 | R5 |
|---|---|---|---|
| alcaldía / municipio / gobernación: tema con palabras del comprador causado por el periodo | 117 | 167 | **0** |
| alcaldía / municipio / gobernación: comprador distinto del de la consulta sin periodo | 232 | 31 | **0** |
| entidad frecuente (SENA, ICBF, U. Nacional…): tema con palabras del comprador | 12 | 16 | **0** |
| entidad frecuente: comprador distinto | 4 | 4 | **0** |
| entidad con nombre oficial del catálogo (sintético): tema con palabras del comprador | 28 | 34 | **0** |
| entidad con nombre oficial: comprador distinto | 22 | 11 | 2 (*) |

(*) «Ese Hospital Universitario Julio Méndez Barreneche»: la consulta SIN periodo ya resuelve a otro hospital (fuzzy); con periodo
resuelve al correcto. Es un defecto de resolución de entidad, no temporal, y no se toca.

`parse_all.py` sobre `allq.json` (4 394 consultas): **R4 → R5 = 6 cambios**, todos de la misma clase (palabras del span del comprador dejan
de ser tema): la consulta real de funsocom; cuatro «con quien ha contratado [la alcaldía de] Galapa/Cartagena en 2025/2026»
(`quien`, `alcaldia`, `galapa` dejaban de ser comprador para ser TEMA: el comprador sigue resuelto —`MUNICIPIO DE GALAPA.`— y el
`objeto` inventado desaparece); y «contratos de CARCEL Y PENITENCIARIA CON ALTA Y MEDIA SEGURIDAD DE LA DORADA CALDAS - INPEC»
(sin ventana: el span del comprador, que ya resolvía —mal, a `MUNICIPIO LA DORADA`, preexistente— no se reencontraba por la puntuación
y dejaba `media seguridad dorada inpec` como tema y `departamento=caldas`). Base → R5 = 32 cambios; los 26 restantes son los de R1–R4
declarados (fechas aplicadas, `anos`/`alcaldia` fuera del tema).

### P2 — respuesta a la pregunta de periodo que cambia el frame (workflow, cliente falso, `mt3.py`)

Frame bloqueado: «contratos de aseo de la Alcaldía de Galapa [por más de 100 millones] en los últimos 3 meses de 2025».

| Respuesta | base | R4 | R5 |
|---|---|---|---|
| F2 «contratos de **vigilancia** de la Alcaldía de **Soledad** de octubre a diciembre de 2025» | EXEC híbrido (Soledad + `aseo` viejo) | EXEC híbrido | **ASK**, 0 llamadas, salida «busca …» / `/reset` |
| F4 «… vigilancia de la **Gobernación del Atlántico** de octubre a diciembre de 2025» | EXEC híbrido | EXEC híbrido | **ASK** |
| F5 «… vigilancia de la Alcaldía de Soledad en 2024» | EXEC híbrido | EXEC híbrido | **ASK** |
| F6 (tras pregunta de seguimiento) «… vigilancia de la Alcaldía de Soledad en 2024» | EXEC híbrido | EXEC híbrido | **ASK** (frame y pregunta intactos) |
| F7 «contratos de aseo **de menos de 50 millones** de octubre a diciembre de 2025» (frame ≥ 100 M) | EXEC 2025 completo, valor ≥ 100 M | EXEC oct–dic con valor ≥ 100 M (pierde el «menos de 50») | **ASK** |
| F8 «**procesos** de vigilancia de la Alcaldía de Soledad de octubre a diciembre de 2025» | EXEC procesos | EXEC procesos | EXEC procesos (la ruta la clasifica `new_search`: ejecuta lo nuevo, sin fusionar) |
| «los de la Alcaldía de Soledad de octubre a diciembre de 2025» (sin tema) | EXEC con el comprador VIEJO (Galapa; ignora «Soledad») | ídem | **ASK** |
| F1/F3 sin pregunta abierta (follow-up directo) | EXEC | EXEC | EXEC (sin cambio: no hay pregunta abierta) |
| Repite el frame («… de la Alcaldía de Galapa de octubre a diciembre de 2025», con o sin el valor; «los de aseo de …») | EXEC 2025 completo (descarta oct–dic), salvo con el valor | EXEC oct–dic | **EXEC oct–dic** (C1/C5 de R4 intactos) |
| «octubre a diciembre de 2025» solo | EXEC oct–dic | EXEC oct–dic | EXEC oct–dic |
| «**busca** contratos de vigilancia de la Alcaldía de Soledad de octubre a diciembre de 2025» (la salida que nombra la pregunta) | EXEC Soledad + vigilancia | ídem | EXEC Soledad + vigilancia, sin fusionar (sin el valor ≥ 100 M del frame viejo) |

Sobre la salida: el motor de follow-up NO reconoce «nueva búsqueda: …» ni «olvida eso, …» como `new_search` (intent `change_scope`; el router
deja `busqueda` en el tema: preexistente). Como `NEW_SEARCH_VERBS` es una constante compartida con el núcleo Foundation (`foundation_route_owner`)
y no se toca, la pregunta nombra la salida que SÍ funciona: «escríbela completa empezando por «busca …»» (verificada: `new_search`, ejecuta
sin fusionar), o `/reset`, o «respóndeme sólo el periodo». Desviación declarada respecto del texto de la orden («nueva búsqueda: …»).
Las condiciones comparadas (`_CONDITION_KEYS`) son una lista cerrada derivada de las claves de alcance/modificador del `QueryFrame`;
un reply que sólo DICE menos que el frame (período solo, subconjunto del tema) sigue siendo respuesta.

### Paginación (corpus conversacional de ZEUS4: 224 conversaciones + «ver más», 586 turnos, `mt3.py`)

| | base → R4 | base → R5 |
|---|---|---|
| turnos distintos | 50 (27 `pagination_no_context`→`pagination_blocked_turn`, 23 `pagination_more`→`pagination_blocked_turn`) | **0** |

Las 141 paginaciones legítimas son idénticas en base/R4/R5. Corpus de 293 turnos sin «ver más»: 0 diferencias base/R4/R5.

## Re-pins declarados

* `app/core/workflow/handlers/pagination.py` en `CONTROL_LANGUAGE_REPINNED_SHA256`
  (`09767248…` → `3fc9512d1b8cb248efe21857d2ef298e42d07b54e3cbe0a1f4a9641e9517de72`): un retorno temprano al inicio de `handle_pagination` que
  niega «ver más» tras un turno que un error temporal dejó sin ejecutar (R4; R5 lo acota a la marca). Sin lógica de cursor, buffer, huella,
  ranking ni orden; los oráculos del módulo siguen idénticos. Comentario en el propio test con «M1 wave 1 R4/R5».
* No hay otros pins sobre archivos tocados por wave 1 R1–R5: `grep -rn sha256 tests | grep -i pin` y la búsqueda de cada archivo cambiado
  desde `7bd671e6` en `tests/` sólo dan `test_truth_layer_public_parity.py`, `test_module_size_limits.py` (límites de tamaño, verde) y
  `tests/contracts/invariants.yaml` (verde). `test_natural_alpha_evaluation.py` y `test_procurement_foundation_frozen_delivery.py` (pins de
  otros archivos) en verde.

## Evidencia (gates literales)

Entorno de todas las corridas: `TZ=America/Bogota PYTHONHASHSEED=0 SECOPPAL_FOUNDATION_MODE=legacy BURR_TELEMETRY_ENABLED=false`;
secuencias de actor con `SECOP_ACTOR_ALIAS_INDEX_PATH` apuntando al índice local (sólo lectura).
`BURR_TELEMETRY_ENABLED=false` es del harness: sin él Burr intenta una conexión a la red durante algunas pruebas.

* **Las dos rojas de la global de R4, en verde**: `tests/test_orchestrator_golden.py::test_golden_pagination_more` y
  `tests/test_truth_layer_public_parity.py::test_control_language_repinned_files_match_their_pinned_state` (corrí esos dos, más
  `test_module_size_limits.py` y `tests/contracts`: `10 passed`).
* Focal = 203 archivos (`tests/test_m1_wave1_r{1..5}_*.py`, `test_orchestrator_golden.py`, `test_truth_layer_public_parity.py` y todo test que
  menciona `pagination` o `query_router`), 4 procesos: `48 failed, 2992 passed, 8 xfailed`; `21 failed, 1807 passed, 10 skipped, 2 xfailed`;
  `1774 passed, 21 xfailed, 1 error`; `1739 passed, 3 xfailed` = **8 312 passed, 34 xfailed, 10 skipped, 69 failed, 1 error**. Los 69 fallos y el
  error son PRE-EXISTENTES e idénticos en el árbol R4 (misma lista, `comm` vacío; ver «Riesgos»): ninguno es de R1–R5. 0 XPASS.
* Expandido (no la suite global) = los 148 archivos de R2/R3 (`expanded_files.txt`) MÁS el focal = 285 archivos, 5 procesos:
  `22 failed, 4199 passed, 11 skipped, 12 xfailed`; `1 failed, 2411 passed, 1 error`; `26 failed, 4552 passed`; `13277 passed, 15 xfailed`;
  `20 failed, 2531 passed, 8 xfailed` = **26 970 passed, 35 xfailed, 11 skipped, 69 failed, 1 error**; los mismos 69 + 1 pre-existentes, 0 nuevos.
  (Esta corrida precede a tres aserciones añadidas a `test_aligned_pattern_tolerates_separators_but_not_other_words`; el focal, posterior,
  corre la versión final.)

* Sondas ZEUS4 (tablas arriba): `g1`–`g4` (P1), `e2` (P2), `conv_vm` + `corpus_conv` (paginación), `allq.json` (4 394), propiedad exhaustiva.
* HCA (`run_conversation_holistic_audit.py`, 6 corpus, `PYTHONHASHSEED=0`), R5 vs R4 (árbol `8578db2c` extraído):
  `171/195`, `40/40`, `35/36`, `22/24`, `31/32`, `41/42` en ambos (conversation_holistic, hca_final_p1_fixes, actor_hygiene,
  negation_focal, time_v2_focal, control_language); **0 verdicts distintos**; 442 turnos: 438 idénticos salvo 26 que difieren sólo en `selection_token` (aleatorio por corrida); **4 con diferencia real, explicadas**:
  `TIMEV2-017/018/019` turno 1 llevan la marca `_temporal_open_question` (código `invalid_date`/rango inverso) en `resolved_params` (efecto buscado de
  R5; la respuesta es la misma) y `AH-28-SHOW-MORE` turno 2: R4 `pagination_blocked_turn` → R5 «El universo contractual de esa página es
  anterior a la política vigente» = **idéntico a R3**.
* `ruff check app tests/test_m1_wave1_r*.py tests/test_truth_layer_public_parity.py`: `All checks passed!`.
* `olympus-semgrep-delta --base 8578db2c --config .semgrep/secoppal-laws.yml` sobre los 5 `.py` de `app/` tocados:
  `TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0`.
* No se corrió la suite global (la corre el orquestador). Los dos `jsonl` de ruido de `docs/audits` no cambiaron.

## Mutantes de la ronda (copia del árbol; cada uno corre los tests de su grupo con `-x`)

Árbol copiado, un mutante a la vez; cada uno corre los tests de su grupo con `-x`.

| Regla | Mutante | Resultado |
|---|---|---|
| P1 | R5-1 la gramática de actor lee el texto con el periodo | muerto (propiedad exhaustiva) |
| P1 | R5-2 `without_period` sin el «en» inicial | muerto (propiedad) |
| P1 | R5-3 `without_period` también enmascara «durante/desde» iniciales | muerto (`test_without_period_also_masks_the_leading_en_and_nothing_else`) |
| P1 | R5-4 `aligned_pattern` = `re.escape(span)` | muerto (`test_punctuation_in_the_buyer_name…`) |
| P1 | R5-5 separadores opcionales (`[\W_]*`) | **superviviente inicial** → aserciones «tokens separados» → muerto |
| P1 | R5-6 sin `\b` inicial | **superviviente inicial** → aserciones de frontera de palabra → muerto |
| P1 | R5-7 el router reemplaza con el span exacto | muerto |
| P1 | R5-8 enmascara el prefijo de cualquier «en» antes del span | muerto |
| paginación | PG-1 `is_blocked` ignora la marca | muerto |
| paginación | PG-2 `is_blocked` bloquea toda aclaración (R4) | muerto (`test_a_turn_is_blocked_only_by_the_temporal_marker`; el golden también) |
| paginación | PG-3 la compuerta marca sólo los códigos abiertos (R4) | muerto (fecha inválida sigue paginando) |
| paginación | PG-4 la puerta de señales `temporal_invalid` no marca | muerto (test nuevo de la segunda puerta) |
| paginación | PG-5 `take` abre pregunta para cualquier código | muerto |
| P2 | AN-1 un tema nuevo nunca cambia el frame | muerto |
| P2 | AN-2 las condiciones nunca cambian el frame | muerto |
| P2 | AN-3 / AN-4 `valor_max` / `valor_min` no son condición | muertos |
| P2 | AN-5 `estado_family` no es condición | muerto |
| P2 | AN-6 `entidad_resolved` no es condición | muerto |
| P2 | AN-7 el cambio de dataset se ignora | muerto |
| P2 | AN-8 `new_search` ya no está exenta | muerto |
| P2 | AN-9 nunca se nombra la salida | muerto |
| P2 | AN-10 los números comparan como vacíos (el primer bug de la ronda) | muerto |
| P2 | AN-11 condición ausente en el frame cuenta como igual | muerto |
| P2 | AN-12 sin el respaldo por `entidad` cruda | muerto |
| P2 | AN-13 el mensaje de salida no se usa | muerto |
| P2 | AN-14 el tema se compara por igualdad, no por subconjunto | muerto |
| P3 | Q-1 (R4-16) `TWO_DIGITS` en `_YEAR_FORMS` | muerto (`…_a_figure_not_adjacent_to_the_window_is_a_quantity[…entidades de 25…]`) |

28 mutantes, **28 muertos** (2 tras añadir tests). Equivalencias: ninguna argumentada (R5-8 en su primera forma —`.match` con `\s*en\s*$`— era
equivalente porque `match` ancla al inicio; se reescribió para que el mutante cambie la decisión y murió).

## Riesgos y límites declarados

* **La marca `_temporal_open_question`** sigue siendo la costura efímera de R4 (retirar con el Conversation IR, M2); ahora la llevan todos los
  turnos bloqueados por un error temporal, pero sólo `OPEN_CODES` dejan una pregunta abierta (`take` no abre pregunta para el resto). Los
  invariantes de R4 (nunca llega a SoQL, no cambia filtros, no sobrevive a una respuesta, un reset ni una búsqueda nueva) siguen en verde.
* **«durante» como tema** sin comprador delante («contratos de aseo durante los últimos 2 años» → `[aseo, durante]`) es preexistente (la
  base lo hace igual): la autoridad temporal consume la ventana pero no el conector. Con comprador delante se consume con su span. No se toca
  (dueño E-TIME); mascarar «durante/desde/hasta» dentro de `without_period` lo habría dejado de tema.
* **Formas temporales que la autoridad no lee** («el último mes», «durante el último trimestre» → `unsupported_temporal_expression`, «en el
  último año» → ídem) quedan igual que en R4; la propiedad las excluye y falla en voz alta si una forma de su lista deja de leerse como periodo.
* **Inicio de oración con ventana** («en los últimos 6 meses contratos de … con el ICBF»): el «en» inicial se leía como cambio de
  territorio y el comprador se perdía (base y R4 también). `without_period` lo enmascara; el patrón general «en <algo> contratos de … con X»
  sin periodo sigue leyéndose como territorio (preexistente, fuera de alcance).
* **Entidades con nombre oficial sintético** («Ese Hospital…», «(Secretaría Distrital…)») tienen defectos de resolución propios (la consulta sin
  periodo ya falla); no se corrigen aquí.
* P2: `_CONDITION_KEYS` es una lista finita; condiciones fuera de ella (p. ej. `ordering_signal`) no cuentan como «cambiar el frame».
* Pre-existentes e iguales en R4 y R5 (mismo conjunto, `diff` vacío): 69 fallos en `test_buyer_actor_role_controls_v1`,
  `test_buyer_actor_independent_boundaries`, `test_buyer_actor_role_arbitration_v1` y `contract_universe_independent_base_generator`, y
  1 error en `test_actor_conversation_corpus` (`no such function: date_extract_y`, memoria «suite roja pre-existente»; el worktree no tiene
  `data/`). No son de esta ronda.
* No verifiqué: red real ni SECOP (clientes falsos); la suite global; despliegue; el camino de Frame de perfil ON (flag OFF por defecto).

Rondas de reparación usadas: 2 de 2 internas (ronda 1: paginación acotada, P1, P2, P3; ronda 2: «en» inicial / alineación del span tras la
propiedad exhaustiva, supervivientes R5-5/R5-6 → tests). Ningún hallazgo C–F; la premisa de la tarea 1 se corrigió con la enmienda del orquestador.

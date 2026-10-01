# M1 wave 1 — R4 repair after ZEUS3 (blocking) on `0bb3ff23` (R3)

Base de la ronda: `0bb3ff23` (R3). Reloj de las sondas: 2026-10-01. Escritor único: ARES. Sin tocar el núcleo Foundation.
Política: hardening acotado (lo finito y derivable de catálogos existentes se repara; el lenguaje abierto se congela).

## Hallazgo bloqueante

Wave 1 introdujo un camino silencioso: ventana relativa + un periodo que la autoridad NO reconoce («de la vigencia
anterior», «del gobierno anterior», «de dos mil veinticinco», «de 2.025», «de 25»…). La base no resolvía periodo (histórico
completo, contiene lo pedido); R3 ejecutaba una ventana contada desde hoy, DISJUNTA del pedido, sin avisar.

## Qué se reparó

| Hallazgo | Clase | Reparación |
|---|---|---|
| Ancla temporal finita tras (o antes de) una ventana | B (regresión de wave 1, finito) | `temporal_noun_anchor.py` (léxico cerrado declarado) + `non_canonical_year.py` (años en palabras 2000–2099, «2.025»/«2,025», «de 25» al final de cláusula). Enganche mínimo: una línea en `_anchored_window` (`temporal_semantics.py`) tras la regla de ancla explícita; mismo código `relative_window_explicit_anchor` y misma pregunta |
| P2-2 paginación tras bloqueo | B | `pagination_blocked.py` + un bloque en `handlers/pagination.py`: «ver más» tras un turno con marca o `outcome=clarification` se niega (mensaje breve, 0 llamadas, el rechazo NO se persiste) |
| P2-1 la salida anunciada no existía | B | `temporal_period_question._is_answer` acepta un periodo explícito sin ventana con el tema del frame o con comprador+tema (búsqueda nueva); `settle/answered` hace que la respuesta ejecute el periodo que escribió (enganche: 3 líneas en `apply_context`) |
| Enmienda 2: única regresión VALUE de wave 1 | A | `money_suffix_place.py`: tras el sufijo plural inclusivo, un nombre que los catálogos existentes reconocen (gazetteer DIVIPOLA + alias, departamentos, regiones, gramática de comprador `ENTITY_REWRITES`) es frontera de la cota. Ninguna lista copiada. Se quitó el xfail |
| Enmienda 1: nombres de comprador | B | `gobierno/administración/gestión` (están en nombres de comprador: 326/32/77 en los catálogos) sólo anclan con calificador explícito (anterior, pasado/a, saliente, entrante, actual, previo/a); «de 25» sólo al final de cláusula |
| Enmienda 3: la marca | declarada | Se queda en `resolved_params` (no hay slot de aclaración pendiente fuera del frame: `actor_pending` viaja igual; un slot nuevo no es enganche mínimo). **Costura efímera a retirar en M2 (Conversation IR)**, con tests de invariantes |

### Léxico de la ancla (`temporal_noun_anchor.py`)

* `STRONG` (siempre periodo): vigencia, periodo/período, año, cuatrienio, semestre, trimestre, bimestre, cuatrimestre,
  quincena, pandemia, legislatura, bienio, trienio, década, temporada (+plurales). Ancla con determinante o con adjetivo
  temporal («de la vigencia», «del año», «de vigencia anterior»); «de año» desnudo no.
* `ENTITY_OFFICE` (gobierno, administración, gestión; también nombres de comprador): SOLO con calificador explícito
  (`PERIOD_QUALIFIERS`), nunca desnudo ni con posesivo. Un test lee los catálogos de compradores
  (`aliases_db`, `buyer_source_names`, `entidades_raw`, `buyer_groups`) y falla si un sustantivo del léxico aparece en un
  nombre de comprador sin estar en esta clase.
* `OFFICE` (mandato, ejercicio; no están en nombres de comprador): adjetivo temporal o posesivo/demostrativo («de su mandato»).
* `PERSON` (alcalde, gobernador, presidente…): sólo con calificador («del alcalde anterior»).
* Entre determinante y sustantivo se admiten ordinales/cuantificadores («del primer semestre», «de los dos años anteriores»).
* El sintagma puede estar antes o después de la ventana (la regla de R2 es del turno, no de adyacencia); un sintagma que
  solapa la ventana («los dos últimos años») o un span protegido (nombre propio, cita normativa) no ancla.
* Años no canónicos: «dos mil» … «dos mil noventa y nueve» (acento tolerante, «veinte y cinco»), «2.025»/«2,025»; una cifra
  seguida de unidad o de otro numeral es cantidad («2.025 millones», «dos mil quinientos», «más de 2.000»); «de 25» sólo
  inmediatamente tras la ventana y ante fin de texto o puntuación («de 25 municipios» no).

## Tabla base / R3 / R4 (`resolve_temporal_scope`, reloj 2026-10-01; ASK = `relative_window_explicit_anchor`, sin fechas)

| Consulta | base | R3 | R4 |
|---|---|---|---|
| … en los últimos 3 meses de la vigencia anterior | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 6 meses del gobierno anterior | None..None | 2026-04-01..2026-10-01 | ASK |
| … en los últimos 6 meses de dos mil veinticinco | None..None | 2026-04-01..2026-10-01 | ASK |
| … en los últimos 3 meses de 2.025 | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses de 2,025 | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses de 25 | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses de la administración anterior | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses de la administración pasada | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses de la vigencia pasada | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses del periodo anterior | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses del cuatrienio anterior | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses del mandato anterior | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses de su mandato | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 6 meses del año | None..None | 2026-04-01..2026-10-01 | ASK |
| … en los últimos 6 meses de este año | 2026-01-01..2026-12-31 | ASK | ASK |
| … en los últimos 3 meses de 2025 | 2025-01-01..2025-12-31 | ASK | ASK |
| … de la vigencia anterior en los últimos 3 meses | None..None | 2026-07-01..2026-10-01 | ASK |
| contratos de los últimos 3 meses del Gobierno del Tolima | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| contratos de los últimos 3 meses del Gobierno Departamental del Tolima | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| contratos de los últimos 3 meses de la Gobernación del Atlántico | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| contratos de los últimos 3 meses de la Administración Municipal de Galapa | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| procesos de los últimos 30 días de 25 municipios | 2026-09-01..2026-10-01 | 2026-09-01..2026-10-01 | 2026-09-01..2026-10-01 |
| procesos de los últimos 3 meses de 25 | None..None | 2026-07-01..2026-10-01 | ASK |
| … en los últimos 3 meses de aseo | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| … en los últimos 3 meses de la Alcaldía | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| … en los últimos 3 meses del Atlántico | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| … en los últimos 3 meses de Barranquilla | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| … en los últimos 3 meses de la entidad | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| … en los últimos 3 meses de 25 millones | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| … en los últimos 3 meses de más de 2.000 | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| … en los últimos 3 meses | None..None | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |
| … en los últimos 3 meses del último año | ASK | 2026-07-01..2026-10-01 | 2026-07-01..2026-10-01 |

Negativos de comprador exigidos (no cambian respecto a R3, 0 diferencias) y positivos exigidos: ver las filas «Gobierno del Tolima»,
«Gobierno Departamental del Tolima», «Gobernación del Atlántico», «Administración Municipal de Galapa», «procesos … de 25 municipios»
(sigue la ventana) y «gobierno anterior», «administración pasada», «vigencia anterior», «mandato anterior», «cuatrienio anterior» (ASK;
probados en `test_m1_wave1_r4_noun_anchor.py`, incl. `test_the_orchestrator_negatives_behave_as_in_r3` y
`..._positives_ask`). Controles: 1 603 continuaciones «de …» extraídas de los corpus del repo (NIT, nombres de entidad,
municipios), antes y después de la ventana: **R3 → R4 = 2 cambios de 1 603 por posición, ambos correctos** («de aseo del año»,
«de la vigencia fiscal»); 163 de ellas, muestreadas, fijadas como control en el test.

## Secuencias de ZEUS3 (workflow, cliente falso, `mt.py`; PF=false y PF=true en seqd)

| Turno | base | R3 | R4 |
|---|---|---|---|
| C1 «contratos de aseo de la Alcaldía de Galapa de octubre a diciembre de 2025» (repite el frame) | EXEC 2025 completo +aseo (descarta oct–dic) | ASK | **EXEC 2025-10-01..12-31 +aseo** |
| C5 «… vías de la Gobernación del Atlántico de abril a septiembre de 2026» (tras la pregunta de seguimiento) | EXEC 2025 +vías (descarta el periodo escrito) | ASK | **EXEC 2026-04-01..09-30 +vías** |
| C6 «los de aseo de septiembre de 2026» (frame con aseo) | EXEC 2025 +aseo | ASK | **EXEC 2026-09-01..09-30 +aseo** |
| C3 «nueva búsqueda: contratos de aseo de la Alcaldía de Soledad en 2024» / C4 «olvida eso, …» / B8 / S3 | EXEC Soledad 2024 | ASK | EXEC Soledad 2024 (*) |
| B9 «… vías de la Gobernación del Atlántico en 2024» | EXEC 2024 | ASK | EXEC 2024 |
| S5/B3/B4/G «ver más», «más resultados», «siguiente página», «muéstrame más» tras un turno bloqueado | PAGE (en B1 base: sin fecha) | PAGE sin fecha | **ASK, 0 llamadas** |
| B1 «31 de febrero de 2025» → «ver más» | PAGE sin fecha | PAGE sin fecha | **ASK, 0 llamadas** (mejora sobre la base, declarada) |

(*) Ejecuta Soledad 2024 pero **sin el tema «aseo»**: es el `change_scope` del motor de follow-up, idéntico sin ninguna pregunta
abierta y en la base («contratos de la Alcaldía de Galapa en 2025» → «contratos de aseo de la Alcaldía de Soledad en 2024» también
pierde «aseo» en base y R3). Preexistente, no tocado, fuera de alcance.

Resto: `seq_a/b/c/d/e/f/g/h/h4/h5/leak` = 0 diferencias R3 → R4 salvo las filas de arriba (las secuencias de actor/proveedor necesitan
`SECOP_ACTOR_ALIAS_INDEX_PATH` apuntando al índice local de la máquina; sin él cualquier árbol da «No tengo disponible el índice local» y
las diferencias son del entorno, no del código — comprobado y corregido antes de comparar).

`mt.py` sobre `corpus_conv.json` (224 conversaciones, 293 turnos): **0 diferencias R3 → R4**. `corp2.py` (2 778 textos): **0 diferencias**;
el mismo corpus con « en los últimos 3 meses» añadido: 0 diferencias.

## VALUE (router; `min`/`max` = cota inclusiva; C = aclara; ninguna polaridad invertida)

| «contratos de la Alcaldía de Galapa …» | base | R3 | R4 |
|---|---|---|---|
| de 300 millones o mayores Galapa | min | C | **min** |
| de 300 millones o mayores galapa | min | C | **min** |
| de 300 millones o superiores Barranquilla | min | C | **min** |
| de 300 millones o menores en Soledad | max | max | max |
| de 300 millones o menores Soledad | max | C | **max** |
| de 300 millones o mayores Alcaldía de Soledad | min | C | **min** |
| de 300 millones y mayores plazos | min | C | C |
| de 300 millones o mayor duración / y mayor plazo | C | C | C |
| de 300 millones y menores plazos | max | C | C |
| de 300 millones o mayores vigentes / firmados en 2025 | min | min | min |

52 consultas (las de ZEUS2 + las de R3 + las nuevas): R3 → R4 = 6 cambios, todos de C a la cota de la base; 0 inversiones. El
nombre se conserva para la autoridad de comprador/geografía (`objeto`/`ciudad`/`entidad_resolved` idénticos a la base; el sufijo es lo
único consumido).

## Xfail (frozen residue)

* Pasaron a tests normales (5): `test_window_of_the_previous_vigencia_asks`, `..._a_year_written_in_words_asks`,
  `..._a_year_with_a_thousands_separator_asks`, `..._a_two_digit_year_asks`, `..._the_previous_administration_asks`.
* `test_a_proper_noun_after_the_plural_suffix_keeps_the_bound` (VALUE): sin xfail, movido a `test_m1_wave1_r4_value_place.py`.
* Quedan 8 xfail strict en `test_m1_wave1_r3_frozen_residue.py` (dueño E-TIME): último bimestre, última quincena, «a finales de»,
  «hace 3 meses de 2025», la ventana sola de `time_relative`, una cita normativa sola, y dos nuevos de R4 que describen el residuo
  verdaderamente abierto: «antes de la pandemia» y «de la vigencia anterior» SIN ventana (la base los descarta igual; R4 sólo
  pregunta junto a una ventana relativa).

## Evidencia (gates literales)

* Focal R1+R2+R3+R4 (`tests/test_m1_wave1_r{1,2,3,4}_*.py`) + `test_newsearch_pagination`, `test_pagination_buffer*`,
  `test_pagination_transparency`: `2402 passed, 8 xfailed` (los 8 son los xfail estrictos de `test_m1_wave1_r3_frozen_residue.py`; 0 XPASS).
* Expandido (no la suite global): la misma lista de 149 archivos de R2/R3, 5 procesos: `671 passed 2 xfailed`, `638 passed`,
  `4221 passed 1 skipped`, `4867 passed`, `11783 passed 1 xfailed` = `22180 passed, 1 skipped, 3 xfailed`, 0 failed (sin el
  `DeadlineExceeded` de Hypothesis de R3).
* HCA (`run_conversation_holistic_audit.py`, 6 corpus, `PYTHONHASHSEED=0`), R4 vs R3 (árbol `0bb3ff23`): `171/195`, `31/32`, `41/42`,
  `22/24`, `40/40`, `35/36` en ambos; estados por caso idénticos; 441 de 442 turnos idénticos tras normalizar tokens/fechas. **Una
  diferencia, explicada**: `actor_hygiene_v1` AH-28-SHOW-MORE turno 2 («muéstrame más» tras la aclaración de actor ambiguo): R3 respondía «El universo
  contractual de esa página es anterior a la política vigente»; R4 responde el mensaje de paginación bloqueada. Ambos se niegan, ambos
  `expect: block` (PASS), y el turno 3 («1», selecciona candidato) sigue ejecutando porque el rechazo no se persiste. Consecuencia
  declarada de que la regla de paginación mire `outcome=clarification` y no sólo los bloqueos temporales.
* `ruff check app tests/test_m1_wave1_r*.py`: `All checks passed!`.
* `olympus-semgrep-delta --base 0bb3ff23 --config .semgrep/secoppal-laws.yml` sobre los 10 `.py` de `app/` tocados:
  `TOTAL=1 BASELINE=1 NEW_DELTA=0 FIXED=0` (con `--base`; el hallazgo es preexistente en un archivo tocado).
* No se corrió la suite global (la corre el orquestador). Los dos `jsonl` que ensucia la suite no cambiaron.

## Mutantes de la ronda (copia del árbol, 4 grupos; cada uno corre los tests de su grupo con `-x`)

43 mutantes, **43 muertos**; 1 más (N19, `_RANGE_AFTER` de «de 10 a 5») sobrevivió porque era redundante con «fin de cláusula» y se
**eliminó el código muerto** (equivalente argumentado; «de 10 a 5» y «de 25 de marzo» siguen sin anclar por el test de cantidades).

| Grupo | Mutantes |
|---|---|
| Ancla (20) | N1 sin ancla; N2 fuerte exige adjetivo; N3 fuerte exige determinante; N4 gobierno acepta posesivo; N5 gobierno ancla desnudo; N6 gobierno acepta «fiscal»/cualquier adjetivo; N7 mandato sin posesivo; N8 persona con determinante; N9 sintagma que solapa la ventana; N10 spans protegidos ignorados; N11 años en palabras; N12 años con separador; N13 dos dígitos sin fin de cláusula; N14 dos dígitos nunca; N15 contexto comparativo; N16 sin filtro de cantidad; N17 «fiscal» califica gobierno; N18 sin ordinales; N20 sintagma antes de la ventana |
| Respuesta (11) | A1 sin subconjunto de tema; A2 cualquier tema responde; A3 el comprador no rescata; A4 conserva el extremo viejo; A5 no reemplaza el periodo; A6 tema sensible a mayúsculas; A7 una ventana responde; A8 temas anidados; A9 `settle` no reemplaza; A10 `settle` ignora reset; A11 `settle` sin pregunta abierta |
| Paginación (4) | P1 ignora la marca; P2 ignora el outcome; P3 el handler nunca se niega; P4 el rechazo se persiste |
| VALUE (9) | V1 comparators sin lugares; V2 polarity sin lugares; V3 sin gramática de comprador; V4 sin departamentos; V5 sin regiones; V6 sin municipios; V7 sin artículo; V8 todo es lugar; V9 sólo la primera palabra |

## Riesgos y límites declarados

* **La marca `_temporal_open_question`** sigue en `resolved_params`: costura efímera de control, **a retirar con el Conversation IR (M2)**.
  Invariantes probados (`test_m1_wave1_r4_marker_invariants.py`): nunca llega a SoQL; no cambia filtros (misma consulta con y sin pregunta
  abierta ⇒ mismo SoQL); no sobrevive a una respuesta válida, a un reset ni a una búsqueda nueva completa; la paginación nunca
  ejecuta un turno marcado/bloqueado (0 llamadas).
* **Paginación**: la regla es «el último turno persistido es una aclaración». Cubre las preguntas de periodo, `invalid_date` y cualquier
  otra aclaración de `normal_flow` (P15, actor ambiguo, etc.): ahí «ver más» ya no ejecuta el alcance sin terminar. Es más amplio que lo
  temporal a propósito (todas esas aclaraciones dejan un frame sin resultados), y cambia el mensaje (no el estado) en AH-28.
* **Comportamiento preexistente descubierto** (no tocado salvo para las respuestas a la pregunta): el `contextual_requery` del
  follow-up conserva el periodo anterior aunque el turno escriba otro explícito («… de abril a septiembre de 2026» tras «… en 2025»
  ejecuta 2025, en la base y en R3, sin ninguna pregunta abierta). `answered()` lo corrige sólo para la respuesta a una pregunta de periodo;
  el caso general queda para el dueño del follow-up engine.
* **«Salida anunciada»**: «exportar», «más caros primero», «y en Barranquilla?» sin periodo, una ventana relativa, y un tema que el frame no
  tiene y sin comprador («los de aseo de octubre a diciembre de 2025» con un frame sin tema; «2025 completo») siguen preguntando de nuevo
  (declarado: una palabra de respuesta no se distingue de un tema).
* **Sobre-disparo aceptado (fail-closed)**: «los últimos 3 meses de la gestión anterior/del periodo …» en nombres de entidad que usen un
  calificador temporal literal; sustantivos fuertes con determinante dentro de un nombre propio no protegido («Colegio del Año Nuevo»
  sólo se salva si el router lo protege como span). «los últimos 3 meses del último año» no ancla (la ventana interior contiene a la
  exterior: mismo periodo).
* Residuo de lenguaje abierto congelado: ver la sección de xfail.
* Frame de perfil ON (flag OFF por defecto de código): no se probó el camino de pregunta/respuesta con el Frame de perfil encendido salvo `seqd` con PF=true (0 diferencias).
* No verifiqué: red real ni SECOP (clientes falsos); la suite global; despliegue.

Rondas de reparación usadas: 2 de 2 internas (ronda 1: ancla, paginación, respuesta; ronda 2: enmiendas del orquestador — comprador, VALUE, invariantes de la marca — y supervivientes N9/N10/A10/N19). Ningún hallazgo C–F.

# M1 wave 1 — R6 repair after ZEUS5 on `17d535b9` (R5)

Base de la ronda: `17d535b9` (R5). Reloj de las sondas: 2026-10-01. Escritor único: ARES. Sin tocar el núcleo Foundation.
Decisión de diseño del orquestador (no se discute aquí): **el comprador exacto gana al periodo**.

## Hallazgos y reparación

| # | Hallazgo (ZEUS5) | Clase | Reparación |
|---|---|---|---|
| P1-1 | `without_period(positive_text, span)` con el span ENVOLVENTE de `relative_window_explicit_anchor` tapaba el comprador entre las dos menciones; T1 ejecutaba sin comprador, sin aviso | B (nuevo de R5) | La captura del comprador se lee sobre el texto ORIGINAL (como la base). `without_period` desaparece del camino de captura |
| P1-2 | `scrubbed` enmascaraba el mismo span envolvente (R4): «… de los últimos 3 meses de la Alcaldía de Barranquilla de 2025» perdía Barranquilla | B (desde R4) | La autoridad temporal entrega cada MENCIÓN por separado (`TemporalResolution.mentions`; `AnchoredWindow.mentions`) y el router enmascara cada una, no el envolvente |
| ICBF | «en 2025 / los últimos dos años / desde enero de 2024 con quien ha contratado el ICBF»: base y R4 = grupo `icbf_national` (33), R5 = SEDE NACIONAL; el test R5 fijaba SEDE NACIONAL | B | Vuelven al grupo. `tests/test_m1_wave1_r5_buyer_span.py` corregido (declarado abajo) |
| P2 | «7 de agosto» tapado dentro de «Hospital 7 de Agosto» cambiaba de comprador | B | Precedencia: la captura que resuelve tal cual no se toca |

## Precedencia implementada (`app/core/period_free_text.py`, módulo pequeño + enganche mínimo en `query_router.py`)

1. **Captura sobre el texto original**; `exact_buyer_first` la resuelve tal cual con la MISMA resolución de la base
   (`_project_actor_relation_buyer`: scan exacto, `resolve_entidad` exacto, ciudad→entidad, grupos). Si resuelve, se queda, con sus
   palabras temporales («Hospital 7 de Agosto»).
2. **Sólo si no resuelve**: se recortan de la captura las menciones temporales UNA POR UNA (cada mención que la autoridad reconoce;
   bordes primero, derecho antes que izquierdo, luego las interiores; nunca el tramo que une dos menciones) y se vuelve a resolver tras
   cada corte (`trimmed_forms`; después de cada corte se prueba también la forma sin conectores huérfanos «en/durante/de…»).
   Una forma sólo se adopta si resuelve exacto.
3. **Tema filtrado contra el span del comprador, no enmascarando la entrada**: el span que se retira del texto residual es la captura
   MENOS las menciones ya enmascaradas allí (`without_mentions`), reencontrado tolerante a separadores (`aligned_pattern`). Ninguna palabra
   de un comprador acreditado produce tema.
4. **Periodo que abre la oración** («en 2025 con quien ha contratado …», «el año pasado …», «en el último año …», dos periodos delante):
   la gramática lee un cambio de territorio y no captura comprador. SÓLO entonces (`read_buyer`) se relee el texto con ese periodo inicial
   enmascarado y se adopta la nueva captura si resuelve exacto Y ningún grupo declarado la reclama (`consume_group_mention`): el grupo
   conserva el camino que tenía (ICBF = 33). Esto conserva la mejora de R5 («lead_en») sin tocar los grupos. **Decisión declarada**: la
   orden pedía retirar `without_period`; se deja sólo esta parte, que es la que usa el paso 3-bis de la precedencia.
5. **Dos periodos (ventana + ancla)**: cada mención se enmascara con su conector introductorio («durante», «en»…, con límite de palabra)
   y la puntuación sola entre ellas («de 2025, los últimos 3 meses») — `period_free_text.consumed`. Un periodo único conserva su span
   (el «durante» huérfano de la base no se toca: E-TIME).
6. Opcional (paso 5 de la orden, «7 de agosto» dentro del span de un comprador no es periodo): **NO hecho**, deuda E-TIME (abajo): exige
   mover el orden temporal/comprador o derivar de los catálogos spans protegidos; no es mínimo.

### Archivos

| Archivo | Cambio |
|---|---|
| `app/core/period_free_text.py` | reescrito: `aligned_pattern`, `mention_texts`, `without_mentions`, `trimmed_forms`, `exact_buyer_first`, `without_leading_period`, `consumed`, `read_buyer` (sin `without_period`) |
| `app/core/query_router.py` | ~25 líneas: `read_buyer` en lugar de `without_period`+`interpret`, máscara temporal por menciones (`period_spans`), patrón del span relación = captura menos menciones, `_claimed_by_declared_group` |
| `app/core/temporal_semantics.py` | `TemporalResolution.mention_spans` + propiedad `mentions` (una mención = el span salvo ventana+ancla) |
| `app/core/temporal_window_anchor.py`, `temporal_noun_anchor.py` | `AnchoredWindow.mentions` (ventanas + ancla, cada una sola) |
| `tests/test_m1_wave1_r6_exact_buyer_wins.py` | nuevo (ver Pruebas) |
| `tests/test_m1_wave1_r5_buyer_span.py` | **corregido**: retira los tests de `without_period`; el test del «en» inicial exige `buyer_group.id == icbf_national` (antes fijaba SEDE NACIONAL); el `entidad` capturado ya no se exige sin periodo (queda «tal cual», como la base); la propiedad no compara contra el control cuando el grupo gana |
| `tests/test_m1_wave1_r2_anchored_windows.py` | el `AnchoredWindow` esperado incluye `mentions` (campo nuevo) y se aserta que cubren sólo los dos periodos |

## Tablas base / R4 / R5 / R6 (workflow con cliente falso, `mt3.py`; `ent` = filtro `nombre_entidad`)

Resumen de las filas con diferencia (todas las demás: R6 = R5 = base o mejora de R1–R5 ya declarada).

| Consulta | base | R4 | R5 | **R6** |
|---|---|---|---|---|
| T1 tras «aseo de 2025 con la Alcaldía de **Soledad** de los últimos 3 meses» → «de octubre a diciembre de 2025» | MUNICIPIO DE SOLEDAD + aseo + oct–dic | ídem | aseo + oct–dic, **sin comprador** | **MUNICIPIO DE SOLEDAD + aseo + oct–dic** |
| ídem con **ICBF** («vigilancia de 2025 con el ICBF de los últimos 6 meses» → jul–dic) | ICBF SEDE NACIONAL | ídem | sin comprador | **ICBF SEDE NACIONAL** (igual a la base) |
| ídem con **INVIAS** | `%INVIAS%` | ídem | sin comprador | **`%INVIAS%`** |
| ídem con valor («más de 100 millones», frame vacío en R5) | Soledad + ≥100 M | ídem | «perdí el contexto» (frame vacío) | **Soledad + ≥100 M** |
| ventana primero («aseo de los últimos 3 meses con la Alcaldía de Soledad en 2025») → T1 | sin comprador, tema `Soledad alcaldia` | Soledad | sin comprador | **MUNICIPIO DE SOLEDAD, tema `aseo`** |
| «aseo de los últimos 3 meses de la Alcaldía de **Barranquilla** de 2025» → T1 | DISTRITO … BARRANQUILLA | sin comprador | sin comprador | **DISTRITO … BARRANQUILLA** |
| «aseo de 2025 de la alcaldía de **Galapa** en los últimos 3 meses» → T1 | MUNICIPIO DE GALAPA. | sin comprador | sin comprador | **MUNICIPIO DE GALAPA.** |
| «aseo con la Alcaldía de Soledad de 2025 de los últimos 3 meses» → T1 | sin comprador (tema `Soledad alcaldia`) | ídem | Soledad | **Soledad** |
| «¿Cuántos contratos firmó la Alcaldía de Galapa de 2025, los últimos 3 meses?» | pregunta de departamento | — | pregunta de periodo | pregunta de periodo (la coma no deja huérfano al comprador) |
| «en 2025 / los últimos dos años / en los últimos dos años / desde enero de 2024 con quien ha contratado el **ICBF**» | **grupo `icbf_national` (33)** | grupo | SEDE NACIONAL | **grupo `icbf_national` (33)** |
| «en los últimos 6 meses contratos de aseo con el ICBF» | grupo | grupo | SEDE NACIONAL | **grupo** |
| «contratos de aseo con la ESE **Hospital 7 de Agosto**» | E.S.E HOSPITAL 7 DE AGOSTO | ídem | HOSPITAL SAN CRISTOBAL DE CIENAGA | **E.S.E HOSPITAL 7 DE AGOSTO** |
| «con quien ha contratado el Hospital 7 de Agosto en 2025» | E.S.E HOSPITAL 7 DE AGOSTO (+ fecha 2026-08-07 inventada) | ídem | MANUEL URIBE ANGEL | **E.S.E HOSPITAL 7 DE AGOSTO** (la fecha inventada sigue: E-TIME) |
| **REAL** «que ha contratado funsocom con la alcaldia de barranquilla los ultimos dos años?» | `[funsocom]`, `actor_profile`, comprador + NIT, sin periodo | `[funsocom, alcaldia]` | comprador + NIT + periodo | comprador + NIT + periodo 2024-10-01..2026-10-01, `objeto=[funsocom]` |
| «aseo con la **contraloria general** los ultimos dos años» (recorte, paso 2) | sin comprador, tema `contraloria general anos` | — | CGR-NIVEL CENTRAL | **CGR-NIVEL CENTRAL** (resuelve sólo tras recortar la ventana) |
| «aseo con la alcaldia de barranquilla los ultimos dos años / en 2025», «con el municipio de galapa…», «con la gobernación del atlántico durante los últimos 2 años» | tema con palabras del comprador en varias | ídem | `[aseo]` | `[aseo]` |
| «en 2025 con quien ha contratado la Alcaldía de Galapa» | Galapa, tema `quien` | — | Galapa | Galapa (sin `quien`) |

## Propiedad exhaustiva ampliada (15 792 consultas; scratch `gen_prop.py`/`sum_prop.py`)

329 compradores × 18 formas temporales × (conector, posición) rotados + 12 disposiciones con el comprador ENTRE o alrededor de dos
menciones (ventana + ancla de 6 anclas; incluye «de la vigencia anterior», «del año pasado»): 15 alcaldías de núcleo + 60 compuestas +
80 al azar (DIVIPOLA), 33 «municipio de», 32 gobernaciones, 80 del catálogo `entidades_raw`, 8 frecuentes, 6 grupos declarados (ICBF
×3 formas, Bogotá×3), 9 nombres del catálogo con fecha/mes (Hospital 7 de Agosto y 5 alias, Julio Méndez Barreneche, Julio César Peñaloza)
y 6 sintéticos (Colegio 20 de Julio…). 7 posiciones (después, antes, inicio, medio, tema al final, «con quien ha contratado» detrás/delante).
Cada consulta se compara con la MISMA consulta sin periodo, en el mismo árbol. Base: N menor porque 3 140 formas no las lee (sin periodo = vacuo, se excluyen).

| | base | R5 | **R6** |
|---|---|---|---|
| consultas con periodo reconocido (N) | 12 652 | 15 792 | 15 792 |
| tema con palabras del comprador causado por el periodo | 759 | 50 | **17** (16 = «Empresa Social del Estado Hospital 7 de Agosto» sin relación `con`; 1 = Hospital San Vicente de Arauca ESE, el control ya resuelve a otro) |
| comprador ≠ sin periodo, turno que ejecuta | 656 | 76 | **72** (105 «=base» contando ambos tipos; ver abajo) |
| comprador ≠ sin periodo, turno con error (el frame que T1 hereda) | 16 | **1 435** | **44** |
| alcaldía/municipio/gobernación/frecuente: tema ← comprador | 319+57+102+22 | 0 | **0** |
| alcaldía/municipio/gobernación/frecuente: comprador perdido (T0 con error) | — | 769+126+168+31 | **4** (control irresoluble o = base) |

Los 72+44 restantes, explicados (todos = base, o el control mismo es inestable):
* 6 + 2: **grupo ICBF** con el periodo delante (`con quien ha contratado el ICBF` sin periodo = SEDE NACIONAL por relación, con periodo delante = grupo): es la base, por orden del orquestador (preexistente: «ICBF» sin periodo no siempre da el grupo).
* ~95: **hospitales** (7 de Agosto, Julio Méndez, San Vicente de Arauca ESE): la resolución es frágil AUN SIN periodo (un «sobre aseo» final la manda a otro hospital) y «7 de agosto» sigue leyéndose como periodo (E-TIME).
* 14 sintéticos (Colegio/Hospital 20 de Julio): el catálogo no los conoce; el fuzzy elige otro comprador con o sin periodo.
* 5: el control (sin periodo) no resuelve y con periodo sí (Palmar de Varela, Archipiélago…): mejora, no pérdida.
Nuevos tokens de tema que no son del comprador: `durante` 1 038 (R5) → **586** (R6; base 460; preexistente E-TIME), `quien` 61 → **0**.

## `fb.json` (596 consultas reales de feedback) y `fam.json` (373, Hospital 7 de Agosto y compradores)

* fb R5→R6: **6 cambios**: 4 son el campo CRUDO `entidad` («la alcaldia de barranquilla» → «… los ultimos dos anos»; `entidad_resolved` idéntico: la captura que resuelve se queda tal cual, como la base; incluye la consulta real de funsocom); «en abril que contratos por mas de 200 millones celebraron?» vuelve a `SET_TERRITORY` (base) en lugar de la lectura R5 con el «en abril» enmascarado; «mira en todo 2026» vuelve a la sonda de actor de la base (`en todo 2026`).
* fb base→R6: **8 cambios** (`objeto` 6, `fecha_hasta` 4, `fecha_desde` 2, `ciudad` 1): los de R1–R5 ya declarados (fechas aplicadas; `quien`/`anos`/`alcaldia` fuera del tema). Ninguno de comprador.
* fam R5→R6: 79 cambios (57 `entidad`, 27 `entidad_resolved`, 4 `buyer_group`): la familia Hospital 7 de Agosto vuelve a la base y los grupos ICBF/«los últimos dos años con quien ha contratado el Hospital 7 de Agosto» pasan de None/otro hospital a E.S.E HOSPITAL 7 DE AGOSTO.
* fam base→R6: 121 cambios: `fecha_*`/`_temporal_*`/`ordering_signal` son de R1–R5; en comprador sólo 10: 7 «los últimos dos años» donde la base ni leía la ventana (su acierto o su error dependían del texto sobrante: «contratos de la ESE Hospital 7 de Agosto» SIN periodo ya da SAN CRISTOBAL), 1 mejora («los últimos dos años con quien ha contratado …» ahora resuelve el hospital) y 4 «Hospital Primero de Mayo» (base: JULIO MENDEZ al azar; R6 sin comprador, como R5).
* `allq.json` (4 394 de R5) **no se reconstruyó**: el archivo estaba en `/private/tmp` (borrado). Se usó fb+fam+la propiedad + HCA.

## Pruebas añadidas (`tests/test_m1_wave1_r6_exact_buyer_wins.py`, 56 casos colectados)

Helper puro (precedencia, un corte a la vez, bordes primero —incluida una mención de borde MÁS CORTA que una interior—, nunca el tramo intermedio,
conectores, sin doble resolución, relectura del periodo inicial con veto de grupo); la autoridad entrega cada mención (4 formas, incl.
«de la vigencia anterior»); P1-1/P1-2 a nivel parser (9 frases, con valor, frecuentes, coma, «durante»); grupos ICBF (5 formas); Hospital 7 de Agosto
(4 + 1); recorte observable en el router (contraloría, defensoría); periodo inicial (5); funsocom real; la propiedad exhaustiva compacta
(10 compradores × 10 formas × 4 posiciones + 5 disposiciones «entre» × 3 ventanas, ≥150 turnos con error); hospitales con el periodo detrás;
y de extremo a extremo (T0 pregunta → T1 responde → el SQL lleva el comprador de la pregunta, 4 casos).

## Gates (literales)

Entorno: `TZ=America/Bogota PYTHONHASHSEED=0 SECOPPAL_FOUNDATION_MODE=legacy BURR_TELEMETRY_ENABLED=false`, índice de alias de sólo lectura. Todo scratch y
`Settings`/`SecopalWorkflow` fuera de pytest con `feedback_path`/`feedback_db_path` redirigidos a `~/.local/state/monte-olimpo/ares_r6/`.

* Focales tras cada cambio: `tests/test_m1_wave1_r6_exact_buyer_wins.py`, `test_m1_wave1_r5_buyer_span.py`, `test_declared_buyer_groups.py`,
  `test_m1_wave1_r2_anchored_windows.py` → `All passed`; corrida final (focales + golden + parity + module_size_limits): `284 passed in 63.57s`.
* **Expandido** (no la suite global) = 254 archivos RECONSTRUIDOS (todo test que nombra un módulo de `app/` tocado por wave 1 R1–R6 —26 módulos— más los
  `test_m1_wave1_r{1..6}_*`, `test_orchestrator_golden`, `test_truth_layer_public_parity`, `test_declared_buyer_groups` y
  `test_actor_pending_document_selection`; la lista de 285 de R5 se perdió con `/private/tmp`), 5 procesos:
  `12917 passed, 2 skipped, 11 xfailed`; `46 failed, 3906 passed, 8 skipped`; `4722 passed, 1 xfailed, 1 error`;
  `3 failed, 2095 passed, 23 xfailed, 1 error`; `22 failed, 2966 passed` = **26 606 passed, 71 failed, 2 errors**.
  Los 71+2 son los PRE-EXISTENTES de R5 (`date_extract_y` en `test_buyer_actor_role_controls_v1`/`independent_boundaries`/`role_arbitration_v1`,
  `test_actor_conversation_corpus`, y `test_actor_profile_render::test_a_specific_aggregate_question…`, rojo también en el árbol R5): `comm` del conjunto
  contra el mismo conjunto de archivos en el árbol R5 = sólo 2 nombres, ambos **intermitentes por plazo de Hypothesis bajo carga** (13 procesos en paralelo):
  `test_actor_pending_document_selection::test_a_document_outside_the_list_never_matches` (200,36–204,33 ms > 200 ms) y
  `test_foundation_runtime::test_count_never_credits_less_than_its_recorded_parent_page`; repetidos aislados 3/3 en R6 (`83 passed` ×3) y 3/3 en R5. 0 XPASS.
  (Primera corrida del expandido: los dos únicos rojos nuevos eran de esta ronda —`test_m1_wave1_r2_anchored_windows` por el campo `mentions`
  y el consumidor de contract_activity por la coma huérfana— y se repararon antes de la corrida de arriba.)
* HCA (`run_conversation_holistic_audit.py`, 6 corpus, `PYTHONHASHSEED=0`), R6 vs R5 (árbol R5 de ZEUS5): `171/195`, `40/40`, `35/36`, `22/24`, `31/32`, `41/42`
  en ambos; **0 verdicts distintos**; 442 turnos: 415 idénticos, 27 sólo difieren en `selection_token` (aleatorio), **0 con diferencia real**.
* `ruff check app tests/test_m1_wave1_r*.py tests/test_truth_layer_public_parity.py`: `All checks passed!`.
* `olympus-semgrep-delta --base 17d535b9 --config .semgrep/secoppal-laws.yml` sobre los 5 `.py` de `app/` tocados: `TOTAL=0 BASELINE=0 NEW_DELTA=0 FIXED=0`.
* No se corrió la suite global. Los dos `jsonl` de ruido de `docs/audits` no cambiaron (`git status` limpio de ellos).
* Re-pins de bytes: ningún archivo de `app/` tocado está pineado (`test_truth_layer_public_parity`, `test_module_size_limits` y `tests/contracts` verdes en el expandido).

## Mutantes de la ronda (copia del árbol; cada uno corre `r6` + `r5_buyer_span` + `declared_buyer_groups` con `-x`)

| # | Regla | Mutante | Resultado |
|---|---|---|---|
| M1 | precedencia exacta | la resolución tal cual nunca gana (siempre recorta) | muerto |
| M2 | recorte por mención | recorta todas las menciones de golpe | muerto |
| M3 | P1-2 | el router enmascara el span envolvente | muerto |
| M4 | filtro de tema | el span relación conserva las palabras del periodo | muerto |
| M5 | bordes primero | corta antes la mención interior | **superviviente inicial** → test de «borde más corto que interior» → muerto |
| M6 | conectores huérfanos | sin limpieza de «en/durante/de» | muerto |
| M7 | paso 2 | la captura nunca se recorta | muerto |
| M8 | P1-1 | `AnchoredWindow.mentions` sólo el ancla | muerto |
| M9 | grupos | el grupo declarado no veta la relectura | muerto |
| M10 | periodo inicial | nunca se relee | muerto |
| M11 | periodo inicial | sólo «en» abre la oración | muerto |
| M12 | P1-1 (nombres/años no canónicos) | `temporal_noun_anchor` sólo el ancla | muerto |
| M13 | eficiencia/contrato | se resuelve toda captura, tenga o no mención | muerto |
| M14 | filtro de tema | `without_mentions` no quita nada | muerto |
| M15 | exactitud | la relectura se adopta sin resolver | muerto |
| M16 | autoridad | un periodo único pierde su mención | muerto |
| M17 | «durante» | sin conectores ni puntuación entre menciones | muerto |
| M18 | «reten» | conector sin límite de palabra | muerto |

18 mutantes, **18 muertos** (1 tras añadir un test). Equivalencias: ninguna argumentada.

## Deuda declarada

* **E-TIME `DATE_IN_BUYER_NAME`** (paso 5 opcional no hecho): «7 de agosto»/«julio» dentro de un comprador del catálogo siguen leyéndose como periodo
  (fecha inventada 2026-08-07 como en la base) y, si hay otro periodo delante, el comprador se pierde como en la base. Camino: spans protegidos derivados
  de alias del catálogo que contienen una mención (la autoridad ya acepta `protected_spans`), previa decisión de orden temporal/comprador.
* **E-TIME `DURANTE_LEFTOVER`** (preexistente): «durante» antes de un periodo único queda de tema; en ventana+ancla ahora se consume con la mención.
* **`BUYER_GROUP_VS_RELATION`** (preexistente): «con (quien ha contratado) el ICBF» sin periodo da SEDE NACIONAL por relación y con el periodo delante da el grupo.
* **Resolución frágil de hospitales** (no temporal): Hospital San Vicente de Arauca ESE, «ESE Hospital 7 de Agosto» con cola «sobre aseo»: el control sin periodo ya resuelve a otro.
* El campo crudo `entidad` conserva las palabras del periodo cuando la captura resuelve tal cual (como la base); `entidad_resolved` es la autoridad.

## No verificado

Red real ni SECOP (clientes falsos); la suite global; despliegue; Frame de perfil ON; `allq.json` (no reconstruible); la lista exacta de 285 de R5 (reconstruida: 254).
Rondas de reparación usadas: 2 de 2 internas (1: diseño, relectura inicial, `AnchoredWindow.mentions`; 2: «durante»/coma entre menciones, `\b` del conector,
supervivientes). Sin hallazgos C–F.

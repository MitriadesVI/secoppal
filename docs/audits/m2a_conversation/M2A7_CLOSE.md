# M2A-7 — presentation/echo: `echo_items` y la regla de cobertura de §11

**Fecha:** 2026-10-03. **Rama:** `mission/m2a-7-echo`, base `d7f53e9b` (= `main` `5a829d5d` + registro del DAG de la ola 2).
**Contrato:** `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` §3.1, §3.6, §6, §10.1, §11, §14, §15, §16, §18 (INV-M2A-12, -21),
§19, Anexo A y Anexo D (U26/U27). **Gold:** `M2A_GOLD` sellado (`7dd39b4b…`). Sin cambio de runtime: ningún módulo fuera de M2a
importa lo nuevo (pin `consumer_problems`); la integración con la presentación es de M2A-6.

**Rondas.** Ronda 1: `3666cf6e` (ZEUS fresco: NO_GO sobre `44a99b56` = `3666cf6e` + prep `e269af2f`). Ronda 3 (reparación
del NO_GO; decisiones de HERA registradas como M2A7-D15…D21): la respuesta a una pendiente se lee contra el ancla,
`anchor_expired` pierde su valor por defecto, `CONTEXT_RESET` sólo sin pendiente vigente, el arnés Gold pasa a veredictos con §19
como única autoridad de los rojos declarados, y un recibo que contradice la procedencia es un error. Una parte de P1-2 se detuvo
por la salvaguarda de HERA (F-6). Ronda 4 (esta; decisiones M2A7-D22 y D23): HERA cierra F-6 con la opción (a), Gold §7 literal: la
respuesta a una pendiente sin ancla se lee contra su frame, y el `DELTA` que cancela una pendiente se lee como una respuesta; el
arnés separa `MEASURED_AT_INTEGRATION` de `FAIL_EXTERNAL`.

Este documento se escribe antes del freeze y no se toca después, para no invalidarlo. Los gates sobre el SHA congelado se
reportan en la entrega.

## 1. Qué se entrega

Dos módulos pequeños de una responsabilidad, registrados en `tests/m2a_steps/m2a_7.py` (`CLOSURE` exacta = ellos dos; leen los
tipos sólo por `foundation_accepted`):

- **`app/core/foundation_echo_diff.py`** — `ops_diff` / `composed_diff(base, effective, patch)`, `answer_diff` y `fresh_diff`. Lee la
  diferencia tipada: qué **arrastró** (idéntico salvo `request_ids`, P-1), qué **colocó** el parche, qué **salió** y por cuál de
  las tres salidas cerradas (`CITED`: un `CLEAR` la cubre o un `SET`/`SET_ORDER` escribe su ranura; `A81_OPPOSITE_BOUND`;
  `F16_POSITIVE_INVERTED`), y si el universo cambió. Es el **lector** de `compose`, no un segundo `compose`. Rechaza
  (`ContractError`) una terna que no sabe explicar. `answer_diff` es la misma lectura en dos pasos (ancla → frame por las
  operaciones que el turno que preguntó dejó en él; frame → efectiva por la respuesta).
- **`app/core/foundation_echo.py`** — dos entradas puras, sin leer el texto, sin resolver, sin decidir semántica:
  - `echo_items(base, effective, patch, record, *, anchor_expired)` para `NEW_SEARCH`, `DELTA`, `PROTOCOL_EVENT`, página y los
    turnos que no ejecutan; **rechaza** un `PENDING_ANSWER` y un `DELTA` con `CANCEL_PENDING` (componen sobre el frame).
  - `echo_items_for_answer(anchor, frame, frame_ops, effective, record, *, anchor_expired)` para el turno que compone sobre el
    frame de una pendiente: la respuesta (`PENDING_ANSWER`) o el `DELTA` que la cancela (`control = CANCEL_PENDING`). Con ancla,
    dos pasos contra el ancla (D15); sin ancla (`anchor = None`), contra el frame (D22).
  Orden determinista: clase (el de §11), luego ranura (§4.2).

### 1.1 Qué eco debe cada estado tipado

| Clase | Se emite cuando (fuente tipada) |
|---|---|
| `CARRIED` | `DELTA` o respuesta ejecutados: un ítem por condición **del ancla** (del frame, si la pendiente no tiene ancla) que sobrevive sin tocar (un `KEEP` no la toca) y uno por el universo dicho si ningún `REPLACE_UNIVERSE` lo toca. Un botón de orden/filtro no lleva `CARRIED` |
| `UNIT_INHERITED` / `UNIT_SHARED_IN_RANGE` | `Provenance.unit_source` = `PREVIOUS_ACCEPTED_VALUE` / `SIBLING_IN_RANGE` de una condición **colocada** (el segundo lleva las dos cotas) |
| `YEAR_INHERITED` / `YEAR_CLOCK_INFERRED` / `RELATIVE_WINDOW_RESOLVED` | `Provenance.year_source` = `PREVIOUS_ACCEPTED_TIME` / `CLOCK_INFERRED` / `RELATIVE_TO_CLOCK` de una condición colocada; además `RELATIVE_WINDOW_RESOLVED` con el recibo `time.relative_to_previous` (primera adición de §11) |
| `VALUE_RANGE_REPLACED` | un lado VALUE del ancla ausente de la efectiva: A8.1 (nadie lo cita y el parche pone el lado opuesto) o `CLEAR(VALUE)` de la dimensión entera seguido de un `SET` que no repone ese lado |
| `TOPIC_REPLACED` | un TOPIC positivo colocado deja sin su valor a otro TOPIC positivo del ancla (F3/F22) |
| `POLARITY_INVERTED` | el positivo arrastrado sale y entra el mismo valor canónico negado (F16) |
| `UNIVERSE_CHANGED` | los `(subject, universe_kind)` de las peticiones difieren de los del ancla (incluye ancla dual reducida); lleva los TIME arrastrados |
| `TOPN_CARRIED` | una CARDINALITY arrastrada y un `SET_ORDER` (F10) |
| `TOPN_CAPPED` | recibo `topn.capped` en una CARDINALITY de la efectiva |
| `ORDER_DIRECTION_DEFAULTED` | `order_direction_source = DEFAULTED` o recibo `order.direction_defaulted` en un ORDER de la efectiva; el recibo sobre una dirección `STATED` es `ContractError` |
| `CONTEXT_RESET` | búsqueda nueva ejecutada sobre un ancla vigente **sin pendiente vigente** (`control ≠ CANCEL_PENDING`) |
| `CONTEXT_EXPIRED` | `anchor_expired` y el turno no ejecuta ninguna efectiva (§10.1, R3c/R3d) |
| `VIEW_DERIVED` / `VIEW_REDERIVED` | **no representables hoy** (D10) |

## 2. Decisiones (IDs `M2A7-Dn`; D15–D23 son decisiones de HERA sobre el NO_GO de la ronda 1 y su seguimiento)

- **M2A7-D1. Firma y hechos inyectados.** El contrato fija `echo_items(base, efectiva, patch, record)`. `anchor_expired` es el hecho
  inyectado de §10 (mismo patrón que `delta_outcome`); decidirlo, sticky incluido, es de M2A-6. **Keyword obligatorio, sin valor por
  defecto (D16).** `record` manda: `patch` y `effective` deben ser los suyos y `effective.base_trace_id == record.base_trace_id`.
  `base` es el ancla en un turno con parche, y el ancla encontrada en una búsqueda nueva. El `echo` del propio registro es el campo
  que se calcula: no se lee.
- **M2A7-D2. Qué turnos ecoan.** Ejecutan una efectiva: `NEW_SEARCH`, `DELTA`, `PENDING_ANSWER`, `PROTOCOL_EVENT` (con efectiva).
  `PROTOCOL_PAGE` es el mismo plan: no ecoa. `CLARIFY`/`REASK`/`INFORM`/`HANDOFF_LEGACY` y un `DELTA` sin efectiva no ejecutan: sólo
  `CONTEXT_EXPIRED`. Un turno que ejecuta confirma o reemplaza el ancla (§10.2), así que nunca lleva `CONTEXT_EXPIRED`.
- **M2A7-D3. Granularidad de `CARRIED` y eventos de botón.** Un ítem por condición arrastrada más el universo dicho. `CARRIED` es de
  «delta o respuesta» (§11, Gold §7): el Gold no da línea al botón de orden ni al de quitar filtro aunque las condiciones sobrevivan
  (SF149–SF152), así que `PROTOCOL_EVENT` conserva sus **efectos** y no `CARRIED`.
- **M2A7-D4 (revisada). La procedencia es de lo que **ejecuta** el turno.** `UNIT_*`, `YEAR_*`, `RELATIVE_WINDOW_RESOLVED` leen las
  condiciones colocadas por el parche; en una búsqueda nueva, todas las suyas; en una respuesta, las colocadas por el frame o por la
  respuesta que siguen en pie (el frame nunca se ejecutó, así que nadie las anunció). La procedencia de una condición **arrastrada
  del ancla** la anunció el turno que la ejecutó y viaja bajo `CARRIED`. Antes decía «el turno que la produjo»: un frame produce y
  no ejecuta. Test y mutante (`carried_provenance_was_announced_when_it_was_executed`, `provenance_of_the_carried_reannounced`).
- **M2A7-D5. Tope y defecto se anuncian en cada ejecución (INV-M2A-21).** `TOPN_CAPPED` y `ORDER_DIRECTION_DEFAULTED` leen **toda**
  condición de la efectiva, colocada o arrastrada. Lectura con alternativa declarada; los tests la fijan.
- **M2A7-D6. `TOPN_CAPPED` por recibo.** `foundation_accepted` (protegido, U3) no expone un hecho de tope y el pin C1.x prohíbe
  decodificar el valor desde otro módulo. Sin el recibo no hay eco: SF021 es rojo §19 (`DEP-E-ORDER`) y verde con un recibo
  sintético, y el test que pide el anuncio de un tope sin recibo es `xfail(strict=True)` con esa causa (INV-M2A-21).
- **M2A7-D7. `VALUE_RANGE_REPLACED`.** Por diferencia, sin recalcular A8.1. Un `CLEAR(VALUE)` solo, o el `CLEAR` de un solo lado, no
  reemplaza (texto de Gold §6 y de §11). Ver F-1.
- **M2A7-D8. Por diferencia sólo con parche.** Las clases por diferencia no se emiten en una búsqueda nueva (el Gold da
  `[CONTEXT_RESET]` a SF124/128/130/134/184 aunque `search_effects` de M2A-3 reporte `TOPIC_REPLACED`; F-4).
- **M2A7-D9 (revisada). `CONTEXT_RESET` y `CONTEXT_EXPIRED`.** Ver D17 y D2. Una pendiente `CONTEXT_EXPIRED_CONFIRM` exige
  `anchor_expired = True`. El vencimiento pegajoso se lee en el eco del último registro (§10.1): es de M2A-6.
- **M2A7-D10. `VIEW_DERIVED` / `VIEW_REDERIVED` no representables.** `AcceptedRequest.view` sólo admite `None` y el sujeto
  `unspecified` falla cerrado (`HostViewDecision`, ola F, §19 `DEP-DERIVED-VIEW`). `NOT_REPRESENTABLE` y un test que fija la premisa.
- **M2A7-D11. Terna incoherente = error, no eco.** `ContractError` ante remoción que nadie cita ni sustitución declarada
  (INV-M2A-16), condición arrastrada cambiada (INV-M2A-02), condición colocada sin operación o al revés, universo cambiado sin
  `REPLACE_UNIVERSE` o distinto del pedido; y, para una respuesta, un frame que sus operaciones no explican.
- **M2A7-D12. `display_key`.** `"<CLASE>"` o `"<CLASE>:<ranura>"`: sólo selecciona qué mostrar; el display sale del valor tipado.
- **M2A7-D13 (revisada). `year_source` también en VALUE.** La vigencia de una cota SMMLV lleva `year_source`; §15.5 nombra
  `YEAR_CLOCK_INFERRED` para ella. El eco sigue el campo tipado. El Gold lee `inherited.year_source` sólo en operaciones TIME (O6):
  SF167 y R0301 salen con un `YEAR_INHERITED` que el Gold no lista porque el tipado del oráculo lo añade. Antes se cubría con una
  lista de adiciones justificadas (`JUSTIFIED_ADDITIONS`): **eliminada (D18)**; ahora es `FAIL_EXTERNAL` demostrado.
- **M2A7-D14. Lectura del Gold.** «Eco del Gold» = lo que guardan las etiquetas (A y B, unión) ∪ lo que deriva Gold §7 sobre los
  campos de la etiqueta (REAL no guarda campos derivados) más las dos adiciones de §11. Es **la** cota en ambos sentidos
  (`emitido == Gold`): ya no hay lista de extras permitidos. Disciplina de partición: pytest mide DEV; TEST se midió **una vez**,
  sobre el código congelado, con `python -m tests.test_foundation_echo_gold TEST` (honestidad: durante el diseño de la ronda 1 leí
  las etiquetas del sintético de ambas particiones; ninguna regla se ajustó a un resultado de TEST).
- **M2A7-D15. El eco de una respuesta se calcula contra el ancla (HERA, por texto sellado).** §11 define `CARRIED` como «delta
  **o respuesta** ejecutados con algo **del ancla** que sobrevive» e INV-M2A-12 es «eco = ejecución»: el frame es lo que el turno que
  preguntó compuso y nunca ejecutó. `echo_items_for_answer` lee la respuesta en dos pasos con las comprobaciones de cada uno
  (`answer_diff`): lo que el frame arrastró y la respuesta dejó queda arrastrado; lo que el frame colocó y la respuesta dejó queda
  colocado (se anuncia ahora, una vez); lo que salió del ancla en cualquiera de los dos pasos sale con la salida de ese paso; lo que
  el frame colocó y la respuesta reemplazó nunca se ejecutó y no es hecho de nadie. Entrada pura: `frame_ops` son las operaciones
  que el turno que preguntó dejó en el frame (`PendingQuestion.frame_ops` de `delta_outcome`; vacío cuando el frame es el ancla,
  p. ej. «continuar» de `CONTEXT_EXPIRED_CONFIRM`). `echo_items` rechaza un `PENDING_ANSWER` (un eco relativo al frame llamaría
  «arrastrado» a lo que el turno que preguntó cambió). Los frames de los tests salen de `delta_outcome`/`compose` reales de M2A-3
  (R3-1 con A8.1, año heredado y tema reemplazado en el frame; «continuar» tras vencimiento). La parte sin ancla, que la ronda 3
  detuvo (F-6), la cierra D22.
- **M2A7-D16. `anchor_expired` sin valor por defecto (HERA).** Omitirlo fallaba abierto y revalidaba el ancla vencida (§10.1):
  ahora es keyword obligatorio en las dos entradas y omitirlo es `TypeError`. Seguimiento para M2A-6:
  `foundation_delta_outcome.py:327` (`delta_outcome(..., anchor_expired: bool = False)`) tiene el mismo patrón (archivo de M2A-3, no tocado).
- **M2A7-D17. `CONTEXT_RESET` sólo sin pendiente vigente (HERA, por texto sellado).** Gold §6 y el checker sellado
  (`check_m2a_gold_contract.py:169-171`) lo derivan sólo cuando el registro previo es `EXECUTED`, `EXECUTED_EMPTY` o `NOT_EXECUTED`; la
  «Derivación exacta» de §11 remite al Gold §7 y prevalece sobre el resumen de la tabla. Una búsqueda nueva que cancela una pendiente
  lleva `control = CANCEL_PENDING` y no emite `CONTEXT_RESET` (SF047–SF057, SF190 esperan `[]`). Seguimiento: `search_effects` de
  M2A-3 comparte el supuesto contrario (`anchor is not None and not anchor_expired`, sin mirar la pendiente).
- **M2A7-D18. §19 es la única autoridad de los rojos declarados (HERA, U19/U23).** Ninguna excepción propia del carril vuelve verde ni
  rojo declarado un caso. Cuatro veredictos, y sólo cuatro: **GREEN** (`Gold == emitido`); **RED_S19** (el caso lo lista la
  extracción sellada `CONTRACT_S19_DEPENDENCIES.json` y el eco queda demostrado verde cuando el dueño de la dependencia aporta su
  entrada: recibo sintético `topn.capped` / `time.relative_to_previous`, o el oráculo no tipa el caso por una regla `DEP-*` que
  nombra); **FAIL_EXTERNAL** (el eco no puede coincidir y la causa está **demostrada** en un artefacto sellado ajeno: el oráculo no
  tipa el turno —su propio `rules`: pendientes, eventos de protocolo, `NO_MEDIBLE`—, su tipado contradice la etiqueta, o
  `derive_effects` del checker contradice el texto del Gold); **FAIL** (un defecto de este carril). Cuenta aparte y nunca vuelve verde un caso.
  SF029, R0202 y SF167/R0301 pasan a `FAIL_EXTERNAL`; tres pruebas del módulo sobre el estado que describe el contrato
  (`test_the_module_does_what_the_contract_says_where_a_sealed_artifact_does_not`).
- **M2A7-D19. Un umbral de vencimiento.** `gap_seconds > 1800` (Gold §5.3 y checker sellado) en todo el arnés. El enlace de M2A-3
  (`test_m2a3_oracle_link.facts_of`) lee `>=`: seguimiento, no se toca.
- **M2A7-D20. Un recibo que contradice el valor o la procedencia es un error, no un eco.** Hecho para `order.direction_defaulted`
  sobre una dirección `STATED` (`ECHO_RECEIPT_CONTRADICTS_PROVENANCE`). **No hecho para `topn.capped` con `n_requested <= n`:**
  exige el valor decodificado; `foundation_accepted` (protegido) no tiene un hecho de cardinalidad y el pin C1.x prohíbe decodificarlo
  desde otro módulo. Pedir a M2A-1 un `cardinality_capped(condition)`; con él, el mismo helper puede además derivar el tope (el
  `xfail` estricto pasaría a verde).
- **M2A7-D21. Primera adición de §11 en el arnés.** Un relativo al referente previo (§16.3, `time.relative_to_previous`) emite también
  `RELATIVE_WINDOW_RESOLVED`: el Gold derivado la exige en SF037, SF042 y SF174 (`gap_codes = RELATIVE_ANCHOR`, `year_source =
  PREVIOUS_ACCEPTED_TIME`); salen `RED_S19` (`DEP-TIME-RELATIVE`) demostrados con el recibo sintético, ya no verdes.
- **M2A7-D22. Una pendiente sin ancla se lee contra su frame; el `DELTA` que cancela se lee como una respuesta (HERA, opción
  (a), Gold §7 literal; cierra F-6).** El Gold es la autoridad de significado y seis casos lo fijan: SF056, SF058, SF059, SF060,
  SF188 (respuestas, `ANSWER_TO_CLARIFICATION`) y SF191 (`DELTA` con `CANCEL_PENDING`) esperan `["CARRIED"]`.
  1. La respuesta a una pendiente **sin ancla** (`base_trace_id = None`, §7.2 c): el «previo» es el frame y el eco se calcula contra el
     frame; lo que sobrevive del frame es `CARRIED`. No se pierde el motivo de D15: un frame sin ancla sólo contiene lo resuelto de
     P, sin ancla de la que esconder sustituciones o procedencias. `echo_items_for_answer` acepta el caso por una rama explícita
     (`anchor = None`): `frame_ops` debe ir vacío (`ECHO_FRAME_OPERATIONS_WITHOUT_AN_ANCHOR`: no hay diferencia de la que ser
     operaciones) y un frame ausente se rechaza (`ECHO_DIFF_NEEDS_INTERPRETATIONS`).
  2. La respuesta **con ancla** queda como estaba (D15: dos pasos contra el ancla).
  3. El `DELTA` con `CANCEL_PENDING` compone sobre el frame (§8.3 fila 2, §17.3): misma regla que una respuesta. Entra por la misma
     función (`over_a_frame`) y `echo_items` lo rechaza; con ancla, dos pasos; sin ancla, contra el frame (SF191). Una búsqueda nueva
     que cancela una pendiente no es un `DELTA`: es `NEW_SEARCH` y no emite `CONTEXT_RESET` (D17).
  4. Salvaguarda: ningún caso de `M2A_GOLD` contradice 1–3. Los seis están modelados por escenarios equivalentes (frame tipado como lo
     resolvió P, respuesta compuesta con el `compose` real) cuyo eco coincide con la etiqueta
     (`test_the_six_pendings_without_an_anchor_have_their_scenario_and_it_gives_the_label_echo`), y SF200 (con ancla: R3-1,
     `delta_outcome` real) da `CARRIED` en las dos lecturas.
  Propiedades: una respuesta, o un `DELTA` que cancela, sin ancla da exactamente el eco del `DELTA` sobre ese frame; con ancla, el
  `DELTA` que cancela da exactamente el eco de la respuesta.
- **M2A7-D23. `MEASURED_AT_INTEGRATION` es una categoría propia (HERA).** Los casos que el oráculo no tipa porque **sus `owners`
  son los carriles de integración** (M2A-4 crea la pendiente, M2A-6 construye el parche de un botón; este carril no puede importarlos)
  no son un defecto de un artefacto sellado: son una medición que sólo existe en integración. `FAIL_EXTERNAL` queda para causas
  demostradas en el tipado del oráculo (SF167, R0301, SF029, `NO_MEDIBLE`…) o en el checker (R0202). La pertenencia sale del propio
  `owners` del oráculo, no de una lista de este carril. Orden de veredictos de un caso no tipable: `RED_S19` (una regla `DEP-*` que
  §19 lista para él), `MEASURED_AT_INTEGRATION`, `FAIL_EXTERNAL`.
- **Declaradas, sin cambio (P3).** (a) El módulo y la referencia de las propiedades son dos lectores de la diferencia, y no divergen
  ni en los escenarios ni en las propiedades. (b) `CARRIED` es un ítem por condición (el Gold lo exige por clase). (c) La procedencia en
  `NEW_SEARCH` se anuncia más allá de O13 (que da `ops = []` a la búsqueda nueva): cada condición de la efectiva es del turno.

## 3. Pruebas

- `tests/test_foundation_echo.py` — 87 escenarios con la lista **exacta** de ítems que el contrato debe (compuestos con el `compose`/`delta_outcome`
  reales; 20 pasan por la entrada de respuesta: con ancla, sin ancla —los seis casos de F-6, SF200— y el `DELTA` que cancela), 34 rechazos de
  turnos incoherentes (`REFUSALS`), 2 omisiones de `anchor_expired` (`TypeError`), las fuentes de procedencia enumeradas desde los enums, cada
  `EchoClass` producida o declarada no representable, la premisa de VIEW y el `xfail` estricto del tope sin recibo.
- `tests/test_foundation_echo_properties.py` — INV-M2A-12 con Hypothesis: los turnos tipados del reducer (`st_case`) más cinco generadores
  focalizados (VALUE, plan/procedencia, diferencia, respuesta R3-1 con operaciones en el frame, búsqueda fresca); el eco se juzga contra
  una referencia escrita desde el contrato y desde `Composed`. Respuestas: contra una referencia con los dos `Composed`, un **diferencial**
  contra la entrada de delta cuando una sola composición sobre el ancla da la misma efectiva (821 respuestas generadas, 397 con operaciones
  en el frame, 821 reproducidas en un paso), la pendiente **sin ancla** igual al `DELTA` sobre su frame y el `DELTA` que cancela igual a la
  respuesta. Y que el generador **alcanza** cada clase.
- `tests/test_foundation_echo_mutants.py` — 97 mutantes (§5).
- `tests/test_foundation_echo_gold.py` — cobertura del Gold (§4).

## 4. Cobertura de `M2A_GOLD` (209 = 49 REAL + 160 sintéticos)

Medida con `tests/test_foundation_echo_gold.py`; REAL con el directorio privado (sólo ids y clases salen del módulo). Cinco veredictos
(D18, D23): GREEN; RED_S19 (§19 es la única autoridad); MEASURED_AT_INTEGRATION; FAIL_EXTERNAL (causa demostrada en el tipado del
oráculo o en el checker); FAIL (defecto de este carril).

| Medida | Casos | GREEN | RED_S19 | MEASURED_AT_INTEGRATION | FAIL_EXTERNAL | FAIL |
|---|---|---|---|---|---|---|
| DEV sintético | 77 | 59 | 8 | 8 | 2 | 0 |
| DEV + REAL | 101 | 76 | 11 | 8 | 6 | 0 |
| TEST sintético (una vez, en el freeze) | 83 | 60 | 7 | 10 | 6 | 0 |
| TEST + REAL (la misma medición) | 108 | 77 | 11 | 10 | 10 | 0 |
| **Total (DEV + TEST, con REAL)** | **209** | **153** | **22** | **18** | **16** | **0** |

**RED_S19 (22):** R0106, R0107, R0304, R0459, R0529, R0531, R0532, SF007, SF008, SF021, SF037, SF042, SF063, SF075, SF080, SF081, SF082,
SF126, SF160, SF174, SF185, SF200. Por fila de §19: `DEP-DERIVED-VIEW` (R0106, R0107, R0529, R0531, R0532, SF063, SF081, SF082, SF126,
SF185), `DEP-MODALITY-UNIVERSE` (R0459, SF007, SF008, SF080, SF160), `DEP-TIME-RELATIVE` (SF037, SF042, SF174: verdes con el recibo
`time.relative_to_previous`), `DEP-E-ORDER` (SF021: verde con `topn.capped`), `DEP-SMMLV-RECALC` (R0304), `DEP-O1B-OPPORTUNITIES` (SF075),
`DEP-E-ORDER-CANDIDATES` (SF200).

**MEASURED_AT_INTEGRATION (18):** SF047, SF048, SF049, SF050, SF052, SF055, SF056, SF057, SF058, SF059, SF060, SF149, SF150, SF151, SF152,
SF188, SF190, SF191. Los `owners` del oráculo son M2A-4 (`PENDING_PRIOR`: SF047, SF048, SF049, SF050, SF052, SF055, SF056, SF057, SF188,
SF190, SF191), M2A-4 y M2A-6 (`PENDING_PRIOR` + `EVENT_OR_PROTOCOL`: SF058, SF059, SF060) o M2A-6 (`EVENT_OR_PROTOCOL`: SF149, SF150,
SF151, SF152). Los seis de F-6 (SF056, SF058, SF059, SF060, SF188, SF191) tienen su escenario equivalente (D22). SF149–SF152 (botones de
orden y de quitar filtro) tienen los suyos (`protocol_event_*`, eco `[]`).

**FAIL_EXTERNAL (16), con su causa demostrada en el artefacto sellado:**

| Causa | n | Casos |
|---|---|---|
| El oráculo no tipa por razón propia: `OPS_AB_DISAGREE` | 7 | R0274, R0281, SF025, SF026, SF030, SF036, SF161 |
| El oráculo no tipa por razón propia: `PRIOR_AB_DISAGREE` | 3 | R0075, R0282, R0336 |
| El oráculo no tipa por razón propia: `HOST_FACT_NOT_IN_GOLD:STATE_LITERALS_OF_THE_UNIVERSE` | 2 | R0094, SF078 |
| El tipado del oráculo añade `year_source = PREVIOUS_ACCEPTED_TIME` a una cota SMMLV que la operación del Gold no declara | 2 | R0301, SF167 |
| El tipado del oráculo omite `year_source = RELATIVE_TO_CLOCK` en un TIME de varios años civiles | 1 | SF029 |
| `derive_effects` del checker contradice el texto de Gold §6/§11 (F-1) | 1 | R0202 |

Lecturas de TEST: la ronda 1 se midió sobre `c7d8e6ad` y `3666cf6e`; la ronda 3 sobre `0f260409`; esta ronda, **una sola vez**, sobre el
código de esta ronda (`4b5afabf`, `app/` idéntico al del freeze). Los GREEN y los FAIL no cambian respecto de la ronda 3: sólo se
reparten distinto los que el oráculo no tipa.

## 5. Mutantes

97 mutantes: 69 del eco y 28 del diff, cada uno con una sola sustitución textual exacta. Los 97 mueren con los escenarios, los
rechazos y las omisiones; 75 mueren también con las propiedades (los 22 restantes son guardas de llamadas incoherentes, el
`display_key`, o formas que ningún generador alcanza hoy: `SCENARIO_ONLY`, con su motivo en el archivo). Control sin mutar: pasa todos
los jueces. Siguen los seis del encargo original (eco omitido, eco sin justificación, clase cambiada, tope sin anunciar, defecto de
orden sin anunciar, `CONTEXT_EXPIRED` omitido) y los de la ronda 3: `answer_read_against_the_frame` (revierte D15),
`anchor_expired_default_restored` y `answer_anchor_expired_default_restored` (reponen el valor por defecto),
`context_reset_over_a_cancelled_pending`, `order_receipt_contradiction_accepted` y nueve del `answer_diff`. De la ronda 4 (D22):
`answer_without_an_anchor_read_against_the_anchor` (la rama sin ancla, aceptada pero leída contra el ancla ausente: muere),
`answer_without_an_anchor_refused` (la rama sin ancla rechazada otra vez), `answer_without_an_anchor_read_as_a_new_search` (sin
`CARRIED`), `answer_frame_operations_without_an_anchor_accepted`, `cancelling_delta_read_against_the_frame` (el `DELTA` que cancela,
con ancla, leído contra el frame: muere), `cancelling_delta_not_routed_to_the_frame_entry` y `every_delta_is_a_cancelling_delta`.

## 6. Hallazgos y seguimientos (se reportan, no se resuelven aquí)

- **F-6 (CERRADO por HERA, ronda 4: M2A7-D22).** Los seis casos del Gold que responden (o cancelan) una pendiente sin ancla fijaban
  `CARRIED`; la lectura «búsqueda nueva» no coincidía bajo «exactamente». HERA eligió el Gold §7 literal; ningún caso lo contradice.
- **F-7 (M2A-4/M2A-6): `frame_ops` no está en el tipo persistido.** `PendingClarification` guarda `frame`, no las operaciones que el
  turno que preguntó dejó en él (`PendingQuestion.frame_ops` sí las tiene al crearla). Con ancla, sin ellas el eco de una respuesta no
  distingue `CLEAR` de A8.1 ni un `CLEAR` + `SET` negado de F16 (el estado final es idéntico). M2A-4 debe conservarlas o M2A-6
  recomponerlas; cambiar el tipo (`m2a-pending/1`, protegido) es decisión de HERA. **Seguimiento declarado (M2A-6).**
- **F-9 (U3): `topn.capped` contra el valor.** Un recibo `topn.capped` con `n_requested <= n` debería ser `ContractError`
  (como `order.direction_defaulted` sobre `STATED`); exige decodificar la cardinalidad y `foundation_accepted` (protegido) no tiene ese
  hecho. Pedir a M2A-1 un `cardinality_capped(condition)`; con él, el mismo helper puede derivar el tope y el `xfail` estricto pasaría
  a verde. **Seguimiento declarado (U3).**
- **F-1 (HERA, checker sellado).** `check_m2a_gold_contract.derive_effects` marca `VALUE_RANGE_REPLACED` para un `CLEAR(VALUE)` de la dimensión
  entera sin `SET` (R0202), mientras Gold §6 dice «No se marca: CLEAR(VALUE) sin SET» y §11 «seguido de `SET`». El eco sigue el texto.
- **F-2 (oráculo M2A-8a0).** `m2a_oracle_typing.tcond` tipa `year_source` de un TIME sólo si cae en un único año civil (SF029), y añade
  `year_source = PREVIOUS_ACCEPTED_TIME` a la vigencia de una cota SMMLV que el Gold no declara (SF167, R0301).
- **F-3 (prep de la ola 2; externo).** `tests/test_m2a_boundaries.py::test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports` cae
  en cuanto un carril de la ola 2 registra un módulo. Corregido en `1d498be4` (`mission/m2a-wave2-prep`); aquí no se edita ni se fusiona.
- **F-4 (M2A-3/oráculo).** `search_effects` reporta `TOPIC_REPLACED` y `CONTEXT_RESET` sin mirar la pendiente; ninguno de los dos
  coincide con el eco (D8, D17). Quien alimente el eco desde el reporte del reducer no debe tomarlos.
- **F-5 (herramienta).** `olympus-state own claim`/`release` no respondieron en 60 s y se terminaron; no es bloqueante.
- **F-8 (M2A-3).** `delta_outcome(..., anchor_expired: bool = False)` (línea 327) y `test_m2a3_oracle_link.facts_of` (`>=`): D16 y D19.
- **F-10 (abierto, sin decidir): un frame ausente.** `PendingClarification.frame` puede ser `None` («nada resuelto»); sin ancla y sin
  frame no hay «previo» y `echo_items_for_answer` lo rechaza (`ECHO_DIFF_NEEDS_INTERPRETATIONS`). Ningún caso del Gold lo ejercita.

## 7. Para M2A-6

- Llamar `echo_items(base, effective, patch, record, anchor_expired=…)` (el keyword es obligatorio) con un registro borrador
  (`echo=()`) y guardar `dataclasses.replace(record, echo=items)`; comparar lo guardado es igualdad de tuplas.
- Para un `PENDING_ANSWER`, o un `DELTA` con `CANCEL_PENDING` (componen sobre el frame): `echo_items_for_answer(anchor, frame,
  frame_ops, effective, record, anchor_expired=…)`. Con ancla: el **ancla** (no el frame), el frame de la pendiente y las operaciones
  que el turno que preguntó dejó en él. Sin ancla (`base_trace_id = None`): `anchor = None`, `frame_ops = ()` y el frame de la pendiente.
- Pasar `anchor_expired` también en el turno que lo confirma o lo reemplaza (el eco no emite `CONTEXT_EXPIRED` en un turno que ejecuta).
- Una búsqueda nueva que cancela una pendiente lleva `control = CANCEL_PENDING` y no emite `CONTEXT_RESET`.
- `VIEW_*`, `TOPN_CAPPED` (recibo `topn.capped`), `RELATIVE_WINDOW_RESOLVED` por `time.relative_to_previous` y los `year_source` de varios
  años dependen de lo que emitan la ola F, E-ORDER y D-TIME; el eco ya mapea el recibo o la procedencia en cuanto existen.

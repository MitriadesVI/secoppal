# M2A-7 — presentation/echo: `echo_items` y la regla de cobertura de §11

**Fecha:** 2026-10-03. **Rama:** `mission/m2a-7-echo`, base `d7f53e9b` (= `main` `5a829d5d` + registro del DAG de la ola 2).
**Contrato:** `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` §3.1, §3.6, §6, §10.1, §11, §14, §15, §16, §18 (INV-M2A-12, -21),
§19, Anexo A y Anexo D (U26/U27). **Gold:** `M2A_GOLD` sellado (`7dd39b4b…`). Sin cambio de runtime: ningún módulo fuera de M2a
importa lo nuevo (pin `consumer_problems`); la integración con la presentación es de M2A-6.

Este documento se escribe antes del freeze y no se toca después, para no invalidarlo. Los gates sobre el SHA congelado se
reportan en la entrega.

## 1. Qué se entrega

Dos módulos pequeños de una responsabilidad, registrados en `tests/m2a_steps/m2a_7.py` (`CLOSURE` exacta = ellos dos; leen los
tipos sólo por `foundation_accepted`):

- **`app/core/foundation_echo_diff.py`** — `composed_diff(base, effective, patch)` y `fresh_diff(effective)`. Lee la diferencia
  tipada: qué **arrastró** (idéntico salvo `request_ids`, P-1), qué **colocó** el parche, qué **salió** y por cuál de las tres
  salidas cerradas (`CITED`: un `CLEAR` la cubre o un `SET`/`SET_ORDER` escribe su ranura; `A81_OPPOSITE_BOUND`; `F16_POSITIVE_INVERTED`),
  y si el universo cambió. Es el **lector** de `compose`, no un segundo `compose`: no recalcula la colisión de A8.1 ni la
  unicidad de F16. Rechaza (`ContractError`) una terna que no sabe explicar.
- **`app/core/foundation_echo.py`** — `echo_items(base, effective, patch, record, *, anchor_expired=False) -> tuple[EchoItem, ...]`.
  Puro: no lee el texto del turno, no resuelve, no decide semántica. Orden determinista: clase (el de §11), luego ranura (§4.2).

### 1.1 Qué eco debe cada estado tipado

| Clase | Se emite cuando (fuente tipada) |
|---|---|
| `CARRIED` | turno `DELTA` o `PENDING_ANSWER` ejecutado: un ítem por condición arrastrada (un `KEEP` no la toca) y uno por el universo dicho si ningún `REPLACE_UNIVERSE` lo toca |
| `UNIT_INHERITED` / `UNIT_SHARED_IN_RANGE` | `Provenance.unit_source` = `PREVIOUS_ACCEPTED_VALUE` / `SIBLING_IN_RANGE` de una condición **colocada** (el segundo lleva las dos cotas) |
| `YEAR_INHERITED` / `YEAR_CLOCK_INFERRED` / `RELATIVE_WINDOW_RESOLVED` | `Provenance.year_source` = `PREVIOUS_ACCEPTED_TIME` / `CLOCK_INFERRED` / `RELATIVE_TO_CLOCK` de una condición colocada; además `RELATIVE_WINDOW_RESOLVED` con el recibo `time.relative_to_previous` (adición de §11) |
| `VALUE_RANGE_REPLACED` | un lado VALUE de la base ausente de la efectiva: A8.1 (nadie lo cita y el parche pone el lado opuesto) o `CLEAR(VALUE)` de la dimensión entera seguido de un `SET` que no repone ese lado |
| `TOPIC_REPLACED` | un TOPIC positivo colocado deja sin su valor a otro TOPIC positivo de la base (F3/F22) |
| `POLARITY_INVERTED` | el positivo arrastrado sale y entra el mismo valor canónico negado (F16) |
| `UNIVERSE_CHANGED` | los `(subject, universe_kind)` de las peticiones difieren de los de la base (incluye ancla dual reducida); lleva los TIME arrastrados |
| `TOPN_CARRIED` | una CARDINALITY arrastrada y un `SET_ORDER` (F10) |
| `TOPN_CAPPED` | recibo `topn.capped` en una CARDINALITY de la efectiva |
| `ORDER_DIRECTION_DEFAULTED` | `order_direction_source = DEFAULTED` o recibo `order.direction_defaulted` en un ORDER de la efectiva |
| `CONTEXT_RESET` | búsqueda nueva ejecutada sobre un ancla vigente (`base` presente y `anchor_expired` falso) |
| `CONTEXT_EXPIRED` | `anchor_expired` y el turno no ejecuta ninguna efectiva (§10.1, R3c/R3d) |
| `VIEW_DERIVED` / `VIEW_REDERIVED` | **no representables hoy** (D10) |

## 2. Decisiones (IDs `M2A7-Dn`; ninguna cambia producto: leen el contrato y el Gold)

- **M2A7-D1. Firma y hechos inyectados.** El contrato fija `echo_items(base, efectiva, patch, record)`. `anchor_expired` es el hecho
  inyectado de §10 (mismo patrón que `delta_outcome`); decidirlo, sticky incluido, es de M2A-6. `record` manda: `patch` y
  `effective` deben ser los suyos y `effective.base_trace_id == record.base_trace_id` (si no, `ContractError`). `base` es la base
  de la composición (ancla o `frame` de la pendiente) en un turno con parche, y el ancla encontrada en una búsqueda nueva. El
  `echo` del propio registro es el campo que se calcula: no se lee.
- **M2A7-D2. Qué turnos ecoan.** Ejecutan una efectiva: `NEW_SEARCH`, `DELTA`, `PENDING_ANSWER`, `PROTOCOL_EVENT` (con efectiva).
  `PROTOCOL_PAGE` es el mismo plan: no ecoa. `CLARIFY`/`REASK`/`INFORM`/`HANDOFF_LEGACY` y un `DELTA` sin efectiva no ejecutan: sólo
  `CONTEXT_EXPIRED`. Un turno que ejecuta confirma o reemplaza el ancla (§10.2: se deja a sí mismo), así que nunca lleva `CONTEXT_EXPIRED`.
- **M2A7-D3. Granularidad de `CARRIED` y eventos de botón.** Un ítem por condición arrastrada más el universo dicho. `CARRIED` es de
  «delta o respuesta» (§11, Gold §7): el Gold no da ninguna línea al botón de orden ni al de quitar filtro aunque las condiciones
  sobrevivan (SF149–SF152), así que `PROTOCOL_EVENT` conserva sus **efectos** (A8.1, universo, `TOPN_CARRIED`, procedencia, plan) y
  no `CARRIED`.
- **M2A7-D4. La procedencia es de lo que el turno coloca.** `UNIT_*`, `YEAR_*`, `RELATIVE_WINDOW_RESOLVED` leen las condiciones
  colocadas por el parche (Gold §7: «alguna *operación* tiene…»); en una búsqueda nueva, todas las suyas. La procedencia de una
  condición arrastrada la anunció el turno que la produjo y viaja bajo `CARRIED`. Test directo y mutante
  (`provenance_of_the_carried_reannounced`).
- **M2A7-D5. Tope y defecto se anuncian en cada ejecución (INV-M2A-21).** `TOPN_CAPPED` y `ORDER_DIRECTION_DEFAULTED` leen **toda**
  condición de la efectiva, colocada o arrastrada: un tope y un defecto valen en cada ejecución, y el Gold los define sobre el
  resultado («el efecto del mismo nombre»). Lectura con alternativa (sólo lo colocado) declarada para ZEUS; los tests la fijan
  (`topn_capped_is_announced_at_every_execution`, `order_default_is_announced_at_every_execution`).
- **M2A7-D6. `TOPN_CAPPED` por recibo.** `foundation_accepted` (protegido, U3) no expone un hecho de tope y el pin C1.x prohíbe
  decodificar el valor desde otro módulo. El eco lee el recibo `topn.capped` (A.7), como dice el encargo. Sin el recibo no hay eco:
  hasta que E-ORDER lo emita, SF021 es rojo declarado y verde con un recibo sintético. `ORDER_DIRECTION_DEFAULTED` lee el campo de
  procedencia **o** el recibo (cualquiera de las dos señales basta para no ocultar un defecto).
- **M2A7-D7. `VALUE_RANGE_REPLACED`.** Por diferencia, sin recalcular A8.1. Un `CLEAR(VALUE)` solo, o el `CLEAR` de un solo lado, no
  reemplaza (texto de Gold §6 y de §11). Ver hallazgo F-1.
- **M2A7-D8. Por diferencia sólo con parche.** `TOPIC_REPLACED`, `POLARITY_INVERTED`, `UNIVERSE_CHANGED`, `TOPN_CARRIED`,
  `VALUE_RANGE_REPLACED` no se emiten en una búsqueda nueva (el Gold da `[CONTEXT_RESET]` a SF124/128/130/134/184 aunque
  `search_effects` de M2A-3 reporte `TOPIC_REPLACED` como efecto del reducer: no es clase de eco ahí; hallazgo F-4).
- **M2A7-D9. `CONTEXT_RESET` y `CONTEXT_EXPIRED`.** `CONTEXT_RESET`: `base` presente, `anchor_expired` falso (igual que
  `search_effects`). `CONTEXT_EXPIRED` según D2. Una pendiente `CONTEXT_EXPIRED_CONFIRM` exige `anchor_expired = True`
  (`ECHO_EXPIRED_PENDING_WITHOUT_EXPIRY`). El vencimiento pegajoso se lee en el eco del último registro (§10.1): es de M2A-6.
- **M2A7-D10. `VIEW_DERIVED` / `VIEW_REDERIVED` no representables.** `AcceptedRequest.view` sólo admite `None` y el sujeto
  `unspecified` falla cerrado (`HostViewDecision` es de la ola F, §19 `DEP-DERIVED-VIEW`). No se inventa: `NOT_REPRESENTABLE` y un
  test que fija la premisa (falla el día que el tipo admita una vista; entonces el eco debe las dos clases). Es rojo declarado, no
  dispensa (SF081/082/126/185, R0106/107/529/531/532, y SF063 con M2A-4).
- **M2A7-D11. Terna incoherente = error, no eco.** `ContractError` ante: remoción que ninguna operación cita ni sustitución
  declarada (INV-M2A-16), condición arrastrada cambiada (INV-M2A-02), condición colocada sin operación o una operación sin su
  condición, universo cambiado sin `REPLACE_UNIVERSE` o distinto del pedido. Un eco que escondiera un cambio no sería «exactamente» la ejecución.
- **M2A7-D12. `display_key`.** `"<CLASE>"` o `"<CLASE>:<ranura>"`: sólo selecciona qué mostrar; el display sale del valor tipado al
  que apuntan `condition_ids` (ids de la base o de la efectiva; nunca colisionan, `compose` no reutiliza ids).
- **M2A7-D13. `year_source` también en VALUE.** La vigencia de una cota SMMLV lleva `year_source`; §15.5 nombra `YEAR_CLOCK_INFERRED`
  para ella. El eco sigue el campo tipado; las etiquetas del Gold leen `inherited.year_source` sólo en operaciones TIME (O6), así que SF167 emite un
  `YEAR_INHERITED` que el Gold no lista (adición **justificada** por la procedencia tipada; `Gold ⊆ emitido` se cumple).
- **M2A7-D14. Lectura del Gold.** «Eco del Gold» = lo que guardan las etiquetas (A y B, unión: A dejó vacío el sintético) ∪ lo que
  deriva Gold §7 sobre los campos de la etiqueta (REAL no guarda campos derivados) más las dos adiciones del contrato. Se mide
  `Gold ⊆ emitido` y `emitido ⊆ justificado`, donde «justificado» es la referencia de las propiedades, escrita desde el contrato y
  desde la cuenta tipada de `compose` (`Composed`), no desde el módulo. Disciplina de partición como M2A-3: pytest mide DEV; TEST
  se mide en el freeze con `python -m tests.test_foundation_echo_gold TEST`. Honestidad: durante el diseño leí las etiquetas del
  sintético de ambas particiones; ninguna regla se ajustó a un resultado de TEST (la única lectura de TEST que hizo cambiar
  algo cambió la clasificación de SF029 en el harness, no el módulo; ver §4).

## 3. Pruebas (todas nuevas)

- `tests/test_foundation_echo.py` — 68 escenarios escritos a mano con la lista **exacta** de ítems que el contrato debe (compuestos con el `compose` real), 21 rechazos de
  turnos incoherentes (`REFUSALS`), las fuentes de procedencia enumeradas desde `UnitSource`/`YearSource`/`OrderDirectionSource`, cada
  `EchoClass` producida por un escenario o declarada no representable, y la premisa de VIEW.
- `tests/test_foundation_echo_properties.py` — INV-M2A-12 con Hypothesis: los turnos tipados del reducer (`st_case`) más tres
  generadores focalizados (VALUE, plan/procedencia, búsqueda fresca); el eco se juzga contra una referencia escrita desde el contrato y
  desde `Composed`: completitud, justificación, clases que `compose` reporta, INV-M2A-16, permutación de operaciones, determinismo,
  orden de §11, el registro acepta el eco, turnos silenciosos, página, DELTA no listo; y que el generador **alcanza** cada clase
  representable.
- `tests/test_foundation_echo_mutants.py` — 74 mutantes (§5), cada uno con una sola sustitución textual exacta.
- `tests/test_foundation_echo_gold.py` — cobertura del Gold (§4).

## 4. Cobertura de `M2A_GOLD` (209 = 49 REAL + 160 sintéticos)

Medida con `tests/test_foundation_echo_gold.py` (REAL con el directorio privado; sólo ids y clases salen del módulo).

| Medida | Casos | VERDE | ROJO declarado | FALLA |
|---|---|---|---|---|
| DEV sintético | 77 | 62 | 15 | 0 |
| DEV + REAL | 101 | 79 | 22 | 0 |
| TEST sintético (freeze) | 83 | 61 | 22 | 0 |
| TEST + REAL (freeze) | 108 | 79 | 29 | 0 |
| **Total (DEV + TEST, con REAL)** | **209** | **158** | **51** | **0** |

Los 158 verdes incluyen `CONTEXT_EXPIRED` (SF138/140/198), `CONTEXT_RESET`, `UNIT_INHERITED`, `YEAR_INHERITED`,
`YEAR_CLOCK_INFERRED`, `RELATIVE_WINDOW_RESOLVED`, `UNIT_SHARED_IN_RANGE`, `ORDER_DIRECTION_DEFAULTED`, `VALUE_RANGE_REPLACED`,
`TOPIC_REPLACED`, `POLARITY_INVERTED`, `UNIVERSE_CHANGED`, `TOPN_CARRIED`, las páginas (SF145/146) y todos los turnos que no ejecutan.
`TOPN_CAPPED` y `VIEW_*` no están en ningún verde (rojos declarados).

**Rojo declarado** (nunca dispensado; cada uno con su causa; `FALLA` = 0):

| Causa | n | Casos |
|---|---|---|
| El oráculo no tipa el turno: `DEPENDIENTE(M2A-4)` (pendientes) | 16 | SF047, SF048, SF049, SF050, SF052, SF055, SF056, SF057, SF058, SF059, SF060, SF063, SF188, SF190, SF191, SF200 |
| `DEPENDIENTE(F)`: vista derivada (`DEP-DERIVED-VIEW`; VIEW_* no representable) | 9 | R0106, R0107, R0529, R0531, R0532, SF081, SF082, SF126, SF185 |
| `NO_MEDIBLE(OPS_AB_DISAGREE)` | 7 | R0274, R0281, SF025, SF026, SF030, SF036, SF161 |
| `DEPENDIENTE(E-MODALITY)` | 5 | R0459, SF007, SF008, SF080, SF160 |
| `DEPENDIENTE(M2A-6)`: eventos de botón sin parche (el host los tipa) | 4 | SF149, SF150, SF151, SF152 |
| `NO_MEDIBLE(PRIOR_AB_DISAGREE)` | 3 | R0075, R0282, R0336 |
| `NO_MEDIBLE(HOST_FACT…STATE_LITERALS_OF_THE_UNIVERSE)` | 2 | R0094, SF078 |
| `DEPENDIENTE(D-MONEY)` | 1 | R0304 |
| `DEPENDIENTE(FOUNDATION_O1B_EXPLICIT_TIME_PARITY)` | 1 | SF075 |
| Falta el recibo `topn.capped` (E-ORDER, `DEP-E-ORDER`): **verde con el recibo sintético** | 1 | SF021 |
| El tipado del oráculo omite `year_source` en un TIME de varios años civiles: **verde con la procedencia** | 1 | SF029 |
| Derivación del checker sellado contradice el texto de Gold §6/§11 (hallazgo F-1) | 1 | R0202 |

En los casos que el oráculo no tipa, el eco depende de turnos que otro carril debe tipar. Las formas de SF149/SF151 (botón de orden y de quitar
filtro) y del botón de universo están reproducidas por escenarios (`protocol_event_*`), con su eco `[]` / `UNIVERSE_CHANGED`.
**Lecturas de TEST:** la primera, sobre `c7d8e6ad`, dio 61 verdes, 21 rojos y **1 FALLA (SF029)**; el módulo no tuvo nada que ver
(causa en el tipado del oráculo) y el harness pasó SF029 a rojo declarado con su prueba; la segunda lectura, tras ese cambio y con
`app/` idéntico, es la de la tabla.

## 5. Mutantes

74 mutantes: 55 del eco y 19 del diff. Los 74 mueren con los escenarios y los rechazos; 59 mueren también con las propiedades
(los 15 restantes son guardas de llamadas incoherentes o estados que `compose` no alcanza: `SCENARIO_ONLY`, con su motivo en el
archivo). Control sin mutar: pasa todos los jueces. Entre ellos están los seis del encargo: eco omitido (`carried_omitted`), eco sin
justificación (`provenance_of_the_carried_reannounced`), clase cambiada (`unit_inherited_class_changed`), tope sin anunciar
(`topn_cap_unannounced`), defecto de orden sin anunciar (`order_default_unannounced`) y `CONTEXT_EXPIRED` omitido
(`context_expired_omitted`).

## 6. Hallazgos fuera de alcance (se reportan, no se resuelven aquí)

- **F-1 (HERA, checker sellado).** `check_m2a_gold_contract.derive_effects` marca `VALUE_RANGE_REPLACED` para un `CLEAR(VALUE)`
  de la dimensión entera sin `SET` (R0202), mientras Gold §6 dice «No se marca: CLEAR(VALUE) sin SET» y §11 «seguido de `SET`». El eco
  sigue el texto; R0202 queda rojo declarado hasta que el checker y el texto coincidan.
- **F-2 (oráculo M2A-8a0).** `m2a_oracle_typing.tcond` tipa `year_source` de un TIME sólo si cae en un único año civil: una ventana
  entre años («últimos 12 meses», SF029) llega al turno tipado sin la procedencia que la operación del Gold declara. Probablemente
  también tras los `NO_MEDIBLE` de SF025/026/030/036/161.
- **F-3 (prep de la ola 2; externo).** `tests/test_m2a_boundaries.py::test_the_pins_of_a_world_ignore_what_a_real_lane_file_imports`
  cae en cuanto cualquier carril de la ola 2 registra un módulo: el registro monkeypatcheado de ese test omite los pasos de la ola 2.
  Corregido en `1d498be4` (`mission/m2a-wave2-prep`); aquí no se edita (protegido) ni se fusiona. Verificado en una copia: con esa línea
  pasan 305/305.
- **F-4 (M2A-3/oráculo).** `search_effects` reporta `TOPIC_REPLACED` en una búsqueda nueva (el oráculo lo espera como efecto del
  reducer), pero no es clase de eco ahí (D8). Quien alimente el eco desde el reporte del reducer no debe tomar ese efecto.
- **F-5 (herramienta).** `olympus-state own claim` no respondió en 60 s y se terminó; la propiedad se coordinó sólo por el scope
  declarado en el encargo (archivos nuevos disjuntos). No es bloqueante.

## 7. Para M2A-6

- Llamar `echo_items(base, effective, patch, record, anchor_expired=…)` con un registro borrador (`echo=()`) y guardar el resultado con
  `dataclasses.replace(record, echo=items)`. El lector estricto del registro acepta el resultado; comparar lo guardado es igualdad de tuplas.
- Pasar `anchor_expired` también en el turno que lo confirma o lo reemplaza (el eco no emite `CONTEXT_EXPIRED` en un turno que ejecuta).
- Las clases `VIEW_*`, `TOPN_CAPPED` (recibo `topn.capped`), `RELATIVE_WINDOW_RESOLVED` por `time.relative_to_previous` y los `year_source`
  de varios años dependen de lo que emitan la ola F, E-ORDER y D-TIME; el eco ya mapea el recibo o la procedencia en cuanto existen.

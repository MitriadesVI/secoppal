# M2a — Conversation mínimo antes del CUTOVER de SEARCH (v2.1)

Fecha: 2026-10-01. Autor: METIS. Sólo lectura de código y datos; sin pytest, LLM ni red. Sin número de ADR.
Sustituye a `M2A_DESIGN_v2.md` (se conserva). El Gold REAL se cita sólo por id y conteo.

Fuentes de código: `main` = `secoppal` @ `4ec99bc1`; `c12` = `secoppal-m1-c12` @ `67c0badb`; `par` =
`secoppal-m1-int-par` @ `2dde6489`; `m1` = `secoppal-m1` @ `4940b73e`. Decisiones de producto: `F1`–`F30` en
`PRODUCT_DECISIONS_2026-10-01.md` (consolidadas, con sus tensiones `X1`–`X18`, en
`02_gold_followups/kit_v2_1/CONTRACT_ADDENDUM_FOLLOWUPS_v2.md`). Gold: `02_gold_followups/GOLD_FOLLOWUP_SCHEMA_v2_1.md`.
HECHO / INFERENCIA / DECISIÓN como en v1.

---

## 0. Cambios v2 → v2.1

| Sección | Qué cambió | Por qué |
|---|---|---|
| §5.5 | La aclaración pendiente guarda el **span ambiguo** y admite **dos ranuras candidatas** para un mismo span; las opciones se resuelven al preguntar. Nueva directiva `CANCEL_PENDING`. | Corrección de diseño; F12, F23. Sin el span, «como comprador / como lugar» no se puede ejecutar |
| §5.2 | **Alcance del turno** (`TurnScope`) como valor cerrado para todo turno, también los que no son delta, con precedencia fija. Mismo vocabulario que el Gold 2.1. | Los etiquetadores no tenían valor para búsqueda nueva, protocolo ni «sin ancla» |
| §5.2 | **Delta o búsqueda nueva**: «consulta completa» = universo citado + condición de contenido propia. Tema, comprador o periodo sin sustantivo de universo es delta. Desaparece `DELTA_OR_NEW_AMBIGUOUS`. | F22, F23; v2 trataba como búsqueda nueva cualquier `SEARCH` con contenido |
| §5.2 | Polaridad: negar el único positivo vigente **invierte**; negar otro valor **añade** la exclusión. Sustituye a `DELTA_CONTRADICTS_CARRIED`. | F16, F17 |
| §5.2 | `CLEAR` de ranura vacía ⇒ informar sin re-consultar; delta que vacía la búsqueda ⇒ aclarar. | F18, F19 |
| §5.2 | Top-N > 100 ⇒ ejecutar con 100 y anunciar; top-N arrastrado ante orden nuevo se conserva; orden por fecha sin dirección ⇒ más reciente primero. | F20, F10, F21 |
| §5.3 | Herencia: SMMLV nunca se hereda ni se ejecuta; herencia que deja el rango sin interior o da una magnitud inverosímil ⇒ aclarar (no se encadena con el reemplazo de F2). **Revierte** lo que v2 decía de R0531. | F14, F26 |
| §5.2 | Estado inexistente en el universo ⇒ aclarar ofreciendo cambiar de universo; territorio nuevo con comprador territorial de otro territorio ⇒ aclarar. | F27, F30 |
| §5.4 | Ancla = última interpretación **aceptada** de Foundation, se haya ejecutado o haya fallado la fuente. Contexto vencido: «continuar» o «reset». | Acuerdo de los dos etiquetadores sintéticos; F11 |
| §5.5 | Respuestas: «cualquiera» ⇒ re-preguntar; respuesta que edita otra ranura ⇒ re-preguntar; rechazo con reformulación ⇒ cancelar y aplicar. | F24, F25, F23, F29 |
| §5.6 | Eco alineado con la lista cerrada del Gold 2.1. | Gold 2.1 §7 |
| §5.7 | Disposición `INFORM` para lo que M2a no hace sobre un ancla de Foundation (otra operación, referente de fila, radar). | Gold 2.1 §5.2; D14 |
| §3 | Tensiones nuevas T10–T16. | F14, F20, F21, F23, F26, F29, F30 contra el código y el Gold |
| §12 | D15 **cerrado**: «anunciar en vez de aclarar» muere como regla general. P1–P4 de v2 cerradas por F6, F8, F10, F11, F13, F14. | `06_experimento_anunciar/` |
| §13 | «Volver atrás»: idea no decidida, con su coste sobre el almacén. | `PRODUCT_DECISIONS` |

### 0.1 Registro de decisiones (estado en v2.1)

| Id | Decisión | Estado |
|---|---|---|
| D1–D3 | Conjunto atómico; VALUE por lado con «rango sin interior»; TOPIC reemplaza | Adoptadas (F1–F3) |
| D4, D5 | Unidad y año heredados por el host | Adoptadas con la extensión de §5.3; acotadas por F14 y F26 |
| D6 | Vista recalculada sólo si derivada | Adoptada (F9) |
| D7 | Tarjeta sin valores; relativos aclaran | Adoptada (F15); garantía limitada (T5) |
| D8 | Remoción sólo citada | Adoptada; excepciones declaradas: F2 (rango sin interior) y F16 (inversión) |
| D9 | `followup_engine` no muere; anclas no-SEARCH fuera | Adoptada (9 de 65 con el dato de trazas) |
| D10 | Cobertura fuera como gate | Adoptada |
| D11 | Eco mínimo como requisito | Adoptada; lista cerrada en §5.6 |
| D12 | Vigencia 30 min | **Cerrada por F11** |
| D13 | CUTOVER con Frame apagado | Adoptada |
| D14 | Radar con mensaje; Export y paginación | Adoptada |
| D15 | «Anunciar en vez de aclarar» | **Cerrada: no se adopta** (§12) |
| F1–F30 | Reglas de producto | Incorporadas; 18 tensiones de la adenda listadas, 6 afectan al diseño (§3) |

---

## 1. Hechos que sostienen el diseño

| Id | Hecho | Evidencia |
|---|---|---|
| H1 | Un turno Foundation escribe `parsed_params = {}`; todo follow-up tras él termina en `FOUNDATION_CONTINUATION_UNSUPPORTED`. | `main:app/core/foundation_runtime.py:221-258`; `main:app/core/foundation_route_owner.py:93-114` |
| H2 | La aceptación no se persiste (traza: sólo telemetría; ledger: digest). | `main:app/core/feedback.py:263-264`; `main:app/core/turn_ledger.py:193` |
| H3 | La IR v2 ya tiene `Operation.FOLLOWUP_DELTA` y `referent_evidence`. | `c12:app/core/foundation_operations.py:28`; `c12:app/core/interpretation_contract.py:171-202` |
| H4 | El guard ata la aceptación a un único texto. | `c12:app/core/foundation_guard.py:41-49` |
| H5 | `HostResolution` transporta sólo `resolved_ids`; el recibo de la autoridad se pierde. | `c12:app/core/interpretation_contract.py:498-515`; `c12:app/core/foundation_authority_contract.py:277-280` |
| H6 | `MoneyValue` no lleva unidad; `TimeValue` congelado no lleva «año inferido»; `BuyerValue` (`canonical`, `kind`, `members`, `membership`) **no lleva territorio**. | `c12:app/core/foundation_authority_values.py:198-245,317-376` |
| H7 | Cifra sin unidad bajo el piso ⇒ `CLARIFY` `VALUE_AMOUNT_UNGROUNDED` sin valor; SMMLV sin año ya se pregunta; la autoridad temporal acepta un año implícito internamente. | `par:app/core/value_authority.py:14-20,191-197`; `par:app/core/time_authority.py:250-264`; `c12:app/core/money_admission.py:62` |
| H8 | `MAX_TOP_N = 100` con el comentario «exceeds it and is clarified, never clamped» (P8). | `c12:app/core/foundation_authority_values.py:33` |
| H9 | La tabla cerrada de ORDER sólo tiene operadores con dirección (`date_desc`, `date_asc`…). | `c12:app/core/foundation_operator_table.py:28` |
| H10 | `is_followup`: ventana de 30 min. | `main:app/core/conversation_store.py:626-640`; `main:app/core/_followup_constants.py:15` |
| H11 | Radar-SAVE salta el `{}` de un turno Foundation; la hoja de filtros del Excel no tiene insumos; «Ver más» llega como texto. | `main:app/core/workflow/handlers/radar.py:54-66`; `03_mapa_consumidores_m3/M3_CONSUMER_MAP.md` §2.7, §2.8, §2.10 |
| H12 | El Gold M1 sellado tiene 2 búsquedas REAL con SMMLV etiquetadas `EXECUTE` (R0302, R0305). | Gold sellado, conteo por id |
| H13 | Errores silenciosos sobre 446 búsquedas: Foundation 23, legacy 170; un eco haría visibles 20 de los 23. Legacy acierta 194/449 (43 %). | `05_hades_alternativas/HADES_ALTERNATIVES.md` E1, E4; `m1:…/PHASE_A_CLOSE.md:23-26` |
| H14 | Etiquetado v2: REAL 60/65 de acuerdo primario; sintético 150/162. Casi todo el desacuerdo restante venía de convenciones no cerradas. | `02_gold_followups/real/comparacion_v2/`, `sintetico/comparacion/` |

---

## 2. DECISIÓN (resumen)

1. `AcceptedInterpretation`: condiciones con valor codificado, origen y procedencia; un único camino de proyección.
2. Almacén propio por chat, append-only y estricto, clave `trace_id`; `Turn.parsed_params` sigue `{}`.
3. Todo turno recibe un **alcance** cerrado (`TurnScope`); un delta es un conjunto atómico de
   `SET / CLEAR / KEEP / SET_ORDER / SET_UNIVERSE`; lo no mencionado se conserva byte-idéntico.
4. El LLM propone con una tarjeta de enumerados; las mismas autoridades acreditan. Unidad y año heredados: los decide
   el host y los calcula la autoridad por una entrada separada y marcada.
5. Aclaración pendiente con su span: respuesta, cancelación explícita, búsqueda nueva completa o re-pregunta; nunca
   híbrido.
6. Referente = la consulta aceptada. Eco mínimo obligatorio y verificable.
7. Lo que M2a no hace sobre un ancla de Foundation se **dice** (`INFORM`); nada se entrega a legacy.

---

## 3. Tensiones

| Id | Tensión | Evidencia | Resolución en v2.1 |
|---|---|---|---|
| T1 | D4/D5 «el host hereda» no es implementable tal cual: la autoridad no devuelve número para una cifra sin unidad, el valor previo no guarda unidad y el recibo se pierde. | H5–H7 | Protocolo aditivo `resolve_with_inherited` (§5.3); requisitos a E-VALUE y E-TIME |
| T2 | F2 literal «vacío» no cubría el rango de un solo punto. | R0205 | Ya recogido por F2 («sin interior») |
| T3 | D8 frente a F2 y F16: una condición desaparece sin `CLEAR`. | F2, F16 | Dos excepciones declaradas y anunciadas (INV-M2A-16) |
| T4 | Guard atado a un solo texto ⇒ tipo nuevo; `interpretation_hash` cambia. | H4 | Igual que v2 |
| T5 | D7/F15: nada estructural impide que un «mes siguiente» se resuelva contra el reloj. | `par:time_authority.py:22-25`; X18 | Garantía limitada: `UNSUPPORTED` `relative_to_previous`; gate 0 sobre el slice sintético |
| T6 | Cambio de universo y condiciones dependientes de dataset. | `c12:foundation_authority_contract.py:162-176` | F27 para STATE; MODALITY igual; TIME se conserva y se anuncia el campo de fecha |
| T7 | `v5_previous` debe leer el almacén o tratarlo como ancla inválida. | decisión 7 | Igual que v2 |
| **T10** | **F20 contradice el contrato congelado y ADR-103 P8**: el comentario de `MAX_TOP_N` y P8 dicen «por encima, aclarar; nunca recortar». | H8; X10 | La autoridad de CARDINALITY resuelve `n = 100` con regla de recibo `topn.capped`. `MAX_TOP_N` no cambia; **sí cambian** el comentario del archivo congelado, el caso del kit que espera `CLARIFY` y P8. Va a la enmienda. |
| **T11** | **F21 contra la tabla de operadores**: «por fecha» sin dirección no tiene operador. | H9 | El productor propone `date_desc`; la autoridad de ORDER, si la cita no trae dirección, resuelve descendente y emite `order.direction_defaulted`. Sin tocar la tabla. |
| **T12** | **F30 no es computable con los tipos actuales**: hace falta el territorio del comprador arrastrado y saber si es territorial o nacional. | H6; X12 | Requisito a E-BUYER: regla de recibo `buyer.territory.<código>` (o `buyer.national`), guardada en `Provenance`. Sin ella F30 no se puede aplicar y el delta **ejecuta conservando** (F1) con eco `CARRIED`; se declara como hueco. |
| **T13** | **F26 revierte R0531**: A y B lo ejecutaban con unidad heredada y reemplazo de rango; F26 manda aclarar. Su siguiente (R0532) queda sobre un pendiente. | `02_gold_followups/…SCHEMA_v2_1.md` §12.2 | El diseño sigue F26 (decisión de producto posterior). Pide confirmación: era la evidencia de D4. |
| **T14** | **F14 alcanza al primer turno y al Gold M1 sellado**: 2 búsquedas REAL con SMMLV están selladas `EXECUTE`. Con F14 Foundation aclara y legacy ejecuta ⇒ `L-only-OK` en la clase VALUE. | H12 | No es de M2a: exige adjudicar y volver a sellar el Gold M1 (ADR-103 enmienda 2: sin dispensas). Va a la enmienda. |
| **T15** | **F29 frente a INV-M2A-3 de v2** («sólo sobre lo ejecutado») y frente a F23: (a) previo aceptado cuya fuente falló: no hay pregunta que re-preguntar (X4); (b) «no, mejor X»: F29 diría re-preguntar, F23 cancelar (X1); (c) la base del delta de F23 no está dicha (X2). | SF135, SF136 (SA y SB: ejecutar); SF063 (SA y SB discrepan) | (a) el ancla pasa a ser la última interpretación **aceptada** (§5.4). (b) F23 prevalece cuando el turno trae un rechazo citado (`CANCEL_PENDING`). (c) la base es el **marco pendiente** (§5.5). Las tres lecturas piden confirmación. |
| **T16** | **Cambio de operación sobre un ancla de Foundation**: C0 lo entregaría a un dueño legacy, que no puede leer el ancla (ejecutaría sin contexto). | C0 §5.3.2; H11 | `INFORM` explícito en M2a (coherente con D14). Pide confirmación de producto. |

---

## 4. Opciones (sólo lo que v2.1 decide de nuevo)

### 4.1 Qué cuenta como ancla (T15a)

| Opción | Efecto | Decisión |
|---|---|---|
| A. Sólo interpretaciones ejecutadas (v2) | Tras «la fuente no respondió», «y en 2024?» obtiene una pregunta sin contenido | No |
| **B. Última interpretación aceptada (READY), ejecutada o con fallo de fuente** | El delta se compone sobre lo que el host ya admitió; el eco muestra lo conservado | **Sí** (coincide con SA y SB) |
| C. Cualquier turno, también los que quedaron en aclaración | Compondría sobre condiciones sin resolver | No (eso es la aclaración pendiente) |

### 4.2 Base del delta tras «no, mejor X» (T15c)

| Opción | Efecto | Decisión |
|---|---|---|
| **A. El marco pendiente** (lo ya resuelto del turno que preguntó) | Conserva lo que el usuario dijo en ese turno y no fue rechazado | **Sí**, a confirmar |
| B. La última búsqueda aceptada anterior al turno que preguntó | Descarta todo el turno que preguntó; si era el primero, no hay base | Alternativa |

### 4.3 Cambio de operación y referente de fila sobre ancla de Foundation (T16)

| Opción | Efecto | Decisión |
|---|---|---|
| A. Entregar al dueño legacy (C0 §5.3.2) | El dueño no ve el ancla: responde sobre otra cosa sin decirlo | No |
| **B. `INFORM`: decir que eso no se hace aún sobre esta búsqueda y cómo lograrlo** | Pérdida visible de función; nada silencioso | **Sí**, a confirmar |
| C. Reconstruir params legacy desde el ancla | Es el puente descartado | No |

El resto (puente, qué se persiste, dónde vive, qué ve el LLM, cómo heredar) no cambia respecto de v2 §4.

---

## 5. Diseño

### 5.1 Tipos y registro

`app/core/foundation_accepted.py` (núcleo); registros inmutables.

```text
ConditionOrigin:  kind: SPAN | OPTION_EVENT | VIEW_ACTION | HOST_POLICY
                  trace_id; raw_sha256 (SPAN); condition_id (SPAN); event_id (eventos)

Provenance:       # hechos cerrados sobre cómo se obtuvo el valor; nunca texto libre
  unit: str | None                 # VALUE: PESOS | MILES | MILLONES | MIL_MILLONES | BILLONES | SMMLV
  unit_source: STATED | SIBLING_IN_RANGE | PREVIOUS_ACCEPTED_VALUE | None
  year_source: STATED | CLOCK_INFERRED | RELATIVE_TO_CLOCK | PREVIOUS_ACCEPTED_TIME | None
  buyer_scope: str | None          # BUYER: código de territorio, o NATIONAL (T12)
  order_direction_source: STATED | DEFAULTED | None
  topn_requested: int | None       # CARDINALITY, cuando hubo tope
  inherited_from: str | None       # trace_id del ancla que aportó unidad o año

AcceptedCondition: slot; request_ids; dimension; operator; polarity; role; level
                   resolved: tuple[str, ...]   # HostResolution.resolved_ids tal cual
                   authority; policies; origin; provenance

AcceptedRequest:   request_id; scope_id; subject; dataset
                   universe_origin: ConditionOrigin | None   # None sí y sólo sí UNSPECIFIED
                   view: HostViewDecision | None             # obligatorio sí y sólo sí UNSPECIFIED

AcceptedInterpretation: schema "m2a-accepted/1"; requests; conditions (conjunción de primer nivel);
                        base_trace_id | None; delta: tuple[AppliedOp, ...]
```

`from_acceptance(acceptance, trace_id, receipts)` es el único constructor de un primer turno (`receipts`: el recibo por
condición que la admisión tiene en los pasos E y hoy descarta, H5). La proyección recibe `AcceptedInterpretation`. El
guard valida un **enlace por origen**: lo del turno actual contra el texto actual; lo arrastrado contra el registro de
su `trace_id`.

```text
AcceptedTurnRecord: schema "m2a-record/1"; trace_id; chat_id; turn_id; created_at_utc; profile; prompt_sha256
  scope: TurnScope                         # §5.2
  turn_acceptance: dict                    # InterpretationAcceptance.to_dict() v2; se relee con load_acceptance
  effective: AcceptedInterpretation | None # None si el turno no llegó a READY
  execution: ExecutionReceipt | None       # accepted: bool; executed: bool; reference_instant; hashes de plan
  echo: tuple[EchoItem, ...]               # §5.6
  result_ref: ResultSetRef | None          # §5.7
  pending: PendingClarification | None     # §5.5
  anchor_trace_id: str | None
  record_sha256: str
```

Almacén (`app/core/foundation_accepted_store.py`): `<CONVERSATION_STORE_DIR>/<chat>.accepted.jsonl`, append-only;
lectura estricta (cola ilegible ⇒ continuidad inválida; esquema desconocido ⇒ ancla inválida); registro antes que el
`Turn`, bajo `conversation_lock`; si el último `Turn` ya no es el ancla leída, se responde con aviso de contexto no
conservado; `ConversationStore.clear()` borra el archivo; nunca en git, traza ni ledger.

### 5.2 Alcance del turno y álgebra

**Alcance (`TurnScope`).** Lo calcula el host para **todo** turno que llega a Foundation, con el primero que aplique:

| # | `TurnScope` | Cuándo | Resultado |
|---|---|---|---|
| 1 | `OUT_NO_ANCHOR` | no hay turno previo, o su registro no se lee, y el turno no es una consulta completa | pregunta: `NO_ANCHOR` (F28) / `ANCHOR_UNREADABLE` |
| 2 | `OUT_ANCHOR_NOT_SEARCH` | el último turno es de un dueño legacy | entrega de la operación a `LEGACY_FOLLOWUP` (C0 §5.3.2, R1–R4) |
| 3 | `IN_PROTOCOL` | evento (opción, vista, orden, quitar filtro) o paginación sobre un resultado Foundation | sin LLM (§5.5, §5.7) |
| 4 | `OUT_OPERATION_CHANGE` | la propuesta trae un job que no es SEARCH ni `FOLLOWUP_DELTA` | `INFORM` `OPERATION_CHANGE:<op>` |
| 5 | `OUT_REFERENT_EXTENSIONAL` | el referente es una fila o un subconjunto mostrado | `INFORM` `REFERENT_EXTENSIONAL` |
| 6 | `OUT_RELATIVE_TO_PREVIOUS` | condición `UNSUPPORTED` con etiqueta `relative_to_previous` (F15) | pregunta |
| 7 | `IN_NEW_SEARCH` | consulta completa | búsqueda nueva: no hereda nada; cancela un pendiente; eco `CONTEXT_RESET` si había ancla |
| 8 | `IN` | lo demás | delta, respuesta o re-pregunta |

**Consulta completa** (F22, F23; convención a confirmar, X3): job `SEARCH` sin `referent_evidence`, con **sujeto
explícito** (universo citado) **y** al menos una condición de contenido propia (TOPIC, BUYER o ACTOR). Con ancla o
pendiente vigentes, **todo lo demás es delta**, lo proponga el productor como `FOLLOWUP_DELTA` o como `SEARCH`: tema
solo, comprador solo, periodo solo (F22), o sustantivo de universo sin contenido (`SET_UNIVERSE`). Un fragmento nunca
se ejecuta solo. Sin ancla ni pendiente, un `SEARCH` sin universo sigue el camino normal de M1 (Product View).

**Ranuras.** `(dimensión, polaridad)` para TOPIC, BUYER, ACTOR, GEOGRAPHY, TIME, STATE, MODALITY,
CONTRACT_TYPE_OR_NATURE; `VALUE.lower`, `VALUE.upper`; `ORDER`; `CARDINALITY`; `UNIVERSE`. Sólo son direccionables los
átomos de la conjunción de primer nivel (objetivo dentro de un OR/NOT ⇒ `TARGET_NOT_ADDRESSABLE`).

| Operación | Efecto | Gold |
|---|---|---|
| `SET(slot, condición)` | Pone la condición; sustituye lo que hubiera en la ranura | ADD = ranura vacía; REPLACE = ocupada (lo deriva el host) |
| `CLEAR(dimensión)` | Quita toda la dimensión; exige cita | REMOVE |
| `KEEP(dimensión)` | No cambia nada; afirma que la ranura existe y da dueño a la cita | — |
| `SET_ORDER` | Sustituye ORDER; cursor a cero | — |
| `SET_UNIVERSE` | Cambia el sujeto; con ancla dual se queda con el nombrado | — |

Reglas (fijas; sin precedencias configurables):

1. **Atómico.** `compose(base, operaciones) → AcceptedInterpretation | DeltaRefusal`. Una operación no acreditada anula
   todo el delta; la base no cambia.
2. **Orden fijo**: `CLEAR`, luego `SET*`, luego comprobación de `KEEP`. `KEEP` + `CLEAR` de la misma dimensión, o dos `SET`
   de la misma ranura ⇒ `DELTA_SLOT_CONFLICT`.
3. **Arrastre por identidad** (F1): lo que ninguna operación toca pasa byte-idéntico. Nunca se re-resuelve.
4. **VALUE por lado** (F2): una cota sustituye la de su lado; la del lado que faltaba completa el rango; «entre X y Y» son
   dos `SET`. Rango sin interior (inferior ≥ superior) ⇒ la cota nueva reemplaza la restricción, con eco. «Cambia el
   valor a…» = `CLEAR(VALUE)` + `SET`.
5. **TOPIC** (F3, F22): `SET` positivo reemplaza los temas positivos del ancla; el eco muestra el anterior.
6. **Polaridad** (F16, F17), sobre valores canónicos tipados: un `SET` negativo cuyo valor es el **único** positivo
   vigente de la dimensión lo **invierte** (sale el positivo, entra la exclusión; eco `POLARITY_INVERTED`); si es otro
   valor, se **añade** como exclusión y el positivo se conserva. Con varios positivos, o con un valor contenido en el
   vigente, se aplica F17 (añadir); la adenda no lo dice (X7).
7. **`CLEAR` de ranura vacía** (F18): si es la única operación ⇒ `INFORM` `CLEAR_ON_EMPTY_SLOT`, 0 llamadas, el ancla no
   cambia. Si viene con otras operaciones válidas, éstas se aplican y el `CLEAR` vacío se anuncia (lectura propia; X8).
8. **Búsqueda vacía** (F19): si tras componer no queda ninguna condición (sólo universo, orden o cardinalidad) ⇒
   pregunta `EMPTY_SEARCH_AFTER_DELTA`.
9. **Top-N** (F10, F20): CARDINALITY es su propia ranura y se arrastra ante un `SET_ORDER` (eco `TOPN_CARRIED`). Un top-N
   pedido por encima de 100 se resuelve a 100 y se anuncia (`TOPN_CAPPED`; T10). `CLEAR(ORDER)` con cardinalidad arrastrada
   ⇒ pregunta.
10. **Orden por fecha sin dirección** (F21): descendente, anunciado (T11).
11. **Herencia** (F5–F7, F26): §5.3.
12. **Vista** (F9): tras componer, si el sujeto es `UNSPECIFIED` y `view.rule ∈ PV1–PV6`, se ejecuta `decide_view` sobre el
    estado compuesto y se anuncia. Sujeto dicho o `USER_OVERRIDE`: no se recalcula; sólo `SET_UNIVERSE` lo cambia.
13. **Estado y universo** (F27): si un STATE (citado o arrastrado) no tiene literales para el dataset resultante
    (`StateValue` por dataset) ⇒ pregunta `STATE_NOT_IN_UNIVERSE` ofreciendo el universo donde sí existe. MODALITY: igual.
    Las reglas ACTOR×STATE del contrato M1 se evalúan antes (X17: orden a confirmar).
14. **Comprador y territorio** (F30): `SET GEOGRAPHY` positivo con un BUYER arrastrado cuyo `provenance.buyer_scope` es un
    territorio que ni contiene ni está contenido en el nuevo ⇒ pregunta `TERRITORY_VS_CARRIED_BUYER` con dos opciones
    (cambiar de comprador / buscar con ambos). `buyer_scope = NATIONAL` o desconocido ⇒ no aplica (T12, X12).
15. **«con X» tras ACTOR** (F12): si el ancla tiene ACTOR y la condición nueva es un lugar cuya cita resuelve a la vez
    como BUYER y como GEOGRAPHY ⇒ pregunta `BUYER_OR_PLACE_AFTER_ACTOR` con las dos opciones ya resueltas (§5.5).
16. **Relativo al valor previo** (F15): `OUT_RELATIVE_TO_PREVIOUS`.
17. **Políticas de petición** (ACTOR×STATE, ACTOR×TIME, `universe-c5@1`): se re-deciden sobre el estado compuesto. H7 no
    se re-ejecuta sobre lo arrastrado.

Precedencia cuando aplican varias reglas en `IN` (misma que el Gold 2.1 §5.3): vencido (F11) → pendiente (F29, F24,
F25) → F14 → F15 → F12 → F30 → F13 → F27 → F5/F26 → F7 → F19 → F18 → ejecutar.

**Propuesta en la IR v2.** Job `FOLLOWUP_DELTA` o job `SEARCH`; condiciones = `SET`. Pieza nueva en
`interpretation_contract.py`: `ProposedSlotDirective(kind: CLEAR | KEEP | CANCEL_PENDING, request_id, dimension | None,
evidence)`, validada por `validate_raw`.

**Tarjeta de estado (D7).** `{anchor: bool, subject, view_derived: bool, slots: [(dimensión, polaridad, operador,
nivel?)], order: bool, cardinality: bool, pending: (ranuras candidatas, razón) | null}`. Sólo enumerados.

### 5.3 Herencia de unidad y de año (F5, F7, F26)

Archivo nuevo `app/core/foundation_authority_inheritance.py`. Los archivos congelados no cambian de bytes por esto.

```text
Inherited:  unit: str | None;  year: int | None;  source_trace_id: str
class InheritingAuthority(Protocol):      # opcional; hoy VALUE y TIME
  def resolve_with_inherited(self, evidence, ctx, inherited: Inherited) -> AuthorityVerdict: ...
```

| Regla | Contenido |
|---|---|
| Quién llama | Sólo `compose`. Un primer turno no puede heredar por construcción. |
| Unidad (F5) | `resolve` dio no-RESOLVED con `detail = VALUE_AMOUNT_UNGROUNDED` **y** todas las cotas VALUE del ancla tienen la misma `provenance.unit`, que es de escala (`MILES`, `MILLONES`, `MIL_MILLONES`, `BILLONES`). `PESOS` y `SMMLV` no se heredan. Una unidad heredada antes cuenta como activa. |
| Freno (F26) | Tras heredar, si la cota resultante deja el rango sin interior, o el monto queda bajo el piso de montos del producto (`MIN_BARE_AMOUNT_COP`) ⇒ pregunta `INHERITED_UNIT_IMPLAUSIBLE`. **No** se aplica la regla 4 (reemplazo) sobre un valor heredado. |
| Año (F7) | `resolve` dio RESOLVED con la regla de recibo `year.inferred` (o no-RESOLVED `TIME_YEAR_NOT_STATED`) **y** el ancla tiene una sola condición TIME positiva, de un único año civil, no `trailing_*` ni abierta ⇒ hereda. TIME previo plurianual o relativo ⇒ pregunta `MONTH_YEAR_AMBIGUOUS`. Sin TIME previo ⇒ vale `resolve` (reloj), anunciado. |
| SMMLV (F14) | Cualquier VALUE en SMMLV, en cualquier turno ⇒ pregunta `SMMLV_UNSUPPORTED` pidiendo pesos. Ningún ancla aceptada contiene SMMLV. |
| Unidad en un rango (F6) | Es de la autoridad VALUE en `resolve` (primer turno incluido): la unidad dicha una vez cubre las dos cotas; regla de recibo `unit.sibling`. No es herencia entre turnos. |
| Qué hace la autoridad | Calcula con el insumo y marca el recibo: `unit.inherited` / `year.inherited`. Con herencia vacía equivale a `resolve`. |
| Qué registra el host | `Provenance` (`unit_source`, `year_source`, `inherited_from`) y el `EchoItem` con el valor completo. |

Requisitos a los carriles (reglas de recibo; el formato ya existe, `c12:foundation_authority_contract.py:49,209-219`):
VALUE emite `unit.<escala>` y `unit.sibling`; TIME emite `year.inferred`; BUYER emite `buyer.territory.<código>` o
`buyer.national` (T12); ORDER emite `order.direction_defaulted` (T11); CARDINALITY emite `topn.capped` (T10).

### 5.4 Ancla y vigencia

El ancla es la efectiva de la última interpretación **aceptada** de Foundation en el chat.

| Turno | `anchor_trace_id` que deja |
|---|---|
| búsqueda nueva o delta aceptados (ejecuten, den cero filas o falle la fuente) | él mismo |
| página; acción de vista | él mismo |
| pregunta, re-pregunta, `INFORM`, caída del proveedor | el anterior (no se consume) |
| búsqueda nueva que queda en pregunta | el anterior se conserva hasta que la pendiente se resuelva o se cancele |
| turno de un dueño legacy | no escribe registro ⇒ no hay ancla Foundation |

Vigencia (F11): 30 min desde el **turno anterior del chat**, del tipo que sea, con el instante inyectado. Vencida:
pregunta `CONTEXT_EXPIRED_CONFIRM` con dos opciones-evento, «continuar sobre la búsqueda anterior» y «reset». «Continuar»
compone sobre el ancla vencida sin otra llamada al LLM (la propuesta del turno quedó en el registro). Si además había
una pendiente, gana F11 (X15: a confirmar).

### 5.5 Aclaración pendiente

```text
PendingClarification:
  pending_id; asked_trace_id; base_trace_id | None
  frame: AcceptedInterpretation          # lo ya resuelto del turno que preguntó, sin lo abierto
  open_span: tuple[RawSpan, ...]         # el span ambiguo, del turno que preguntó
  open_raw_sha256: str                   # enlace al texto de ese turno
  candidate_slots: tuple[Slot, ...]      # 1; o 2 cuando la duda es de dimensión (F12)
  open_condition: (operator, polarity, level) por ranura candidata
  reason (cerrado); detail
  options: tuple[PendingOption, ...]     # puede ser vacío
  expires_at_utc; asks (tope 2)
PendingOption: option_id; slot; display_key; resolved: tuple[str, ...]; authority; provenance
```

Se crea con **un** span sin resolver y todo lo demás resuelto. Las opciones se resuelven **al preguntar**, cada una por
la autoridad de su ranura sobre la ventana del span (para F12: la de BUYER y la de GEOGRAPHY sobre el mismo span); por
eso una opción elegida por evento no necesita LLM ni releer texto.

| Entrada | Regla |
|---|---|
| Evento de opción (`option_id` + `pending_id`) | Sin LLM: `frame + opción` (origen `OPTION_EVENT`). |
| Texto: **respuesta** | Toda condición propuesta es de una ranura candidata, o de una ranura del marco con el **mismo** valor codificado. Se ejecuta `frame + respuesta`. |
| Texto: «cualquiera» o respuesta que no elige (F24) | Re-pregunta `OPTION_NOT_CHOSEN`; el producto no elige. |
| Texto: respuesta **que además edita otra ranura** (F25) | Re-pregunta `ANSWER_EDITS_OTHER_SLOT` nombrando la salida; 0 llamadas. |
| Texto: **rechazo citado** + fragmento (F23; directiva `CANCEL_PENDING`) | Cancela la pendiente; el fragmento es un delta cuya base es `frame` (T15c). Si el resultado no queda completo y seguro, pregunta. |
| Texto: consulta completa (con o sin rechazo) (F23) | Búsqueda nueva; cancela la pendiente; no fusiona. |
| Texto: no responde ni rechaza (F29) | Re-pregunta `PENDING_UNANSWERED`; marco y pregunta intactos. |
| La respuesta no resuelve | Re-pregunta `ANSWER_UNRESOLVED`. |
| Caducidad | Como el ancla (30 min); `asks` llega a 2 y vuelve a fallar ⇒ se cancela y se dice. |

Reproduce las 11 filas de `secoppal-m1-int-r5/docs/audits/m1_wave1/R5_REPAIR.md` §P2, comparando por valor tipado. No
reemplaza `actor_pending` (dueño legacy).

### 5.6 Eco mínimo (D11)

`app/core/foundation_echo.py` (puro): `echo_items(base, efectiva, operaciones) → tuple[EchoItem, ...]`, desde valores
tipados y procedencia. Mismas clases y definiciones que el Gold 2.1 §7.

| Clase | Cuándo es obligatoria |
|---|---|
| `CARRIED` | delta o respuesta ejecutados con algo del ancla que sobrevive sin tocar (una línea compacta, con acción «quitar») |
| `UNIT_INHERITED` / `YEAR_INHERITED` | valor heredado del ancla (F5 / F7); muestra el valor completo y de dónde sale |
| `UNIT_SHARED_IN_RANGE` | unidad repartida en un rango (F6): las dos cotas con su unidad |
| `YEAR_CLOCK_INFERRED` / `RELATIVE_WINDOW_RESOLVED` | año por reloj; periodo relativo con sus fechas absolutas (también en primer turno) |
| `VALUE_RANGE_REPLACED` | regla 4, rango sin interior |
| `TOPIC_REPLACED` / `POLARITY_INVERTED` | reglas 5 y 6 |
| `VIEW_DERIVED` / `VIEW_REDERIVED` | vista derivada (siempre) / vista que cambió (F9) |
| `UNIVERSE_CHANGED` | `SET_UNIVERSE` efectivo; si hay TIME arrastrado, a qué fecha aplica ahora |
| `TOPN_CARRIED` / `TOPN_CAPPED` / `ORDER_DIRECTION_DEFAULTED` | F10 / F20 / F21 |
| `CONTEXT_RESET` | búsqueda nueva completa con ancla vigente; acción «aplicar a la búsqueda anterior» |
| `CONTEXT_EXPIRED` | pregunta de F11 |

Los `EchoItem` se guardan en el registro y se comparan con la efectiva ejecutada (INV-M2A-12). No es el eco general de
la capa 6. Los avisos del contrato M1 (`host_notice`) no son eco de M2a.

### 5.7 Referente al resultado y consumidores

```text
ResultSetRef: trace_id; interpretation_hash; groups: tuple[GroupRef, ...]
GroupRef: request_id; dataset; plan_hash_query; order_contract; page_limit; shown_ids; next_offset | None; total; total_quality
```

El referente es la consulta aceptada y su plan (F4: «los de mayor valor» re-consulta).

| Consumidor | Qué da M2a | Fuera, declarado |
|---|---|---|
| Paginación | `handle_foundation_page` antes de `handle_pagination` (`main:orchestrator.py:410-417`): re-proyecta la efectiva guardada con el instante guardado, exige `plan_hash` igual, pide la página bajo un permiso del guard, descarta ids ya mostrados. Sin LLM. Con ancla dual pregunta qué grupo. | `PaginationBuffer`, `handle_pagination` |
| Orden | texto: `SET_ORDER` = nueva ejecución ordenada en la fuente; botón `[Mayor valor]`: evento con `OrderValue` de tabla cerrada | — |
| `[Elegir período]` | evento que abre una pendiente de ranura TIME | — |
| Botones de vista (F) | `handle_foundation_view_action` lee este almacén; `USER_OVERRIDE` pegajoso (F9) | — |
| Export | filas mostradas; publica `foundation_applied_conditions` (los `EchoItem` de la efectiva) | cablearlo en la hoja de filtros de `export_xlsx`; exportar «todo»; dos grupos |
| Radar-SAVE | barrera: si el último turno es Foundation ⇒ `INFORM` `OPERATION_CHANGE:FOLLOW`; nunca mirar turnos anteriores | puente IR → `QueryFrame` |
| Otra operación sobre el ancla (contar, sumar, proveedores) | `INFORM` `OPERATION_CHANGE:<op>` (T16) | ejecutarla con el contexto: M2b/M3 |
| Referente de fila o subconjunto | `INFORM` `REFERENT_EXTENSIONAL` | M2b |
| Chips legacy (`seed_resolved_params`) | ninguno | declarado |

**R-BARRERA**: un turno Foundation es opaco para legacy y ningún lector legacy usa como referente un turno anterior a él.

---

## 6. Invariantes

Se preservan P-1…P-7 e INV-C0-1…12 (INV-C0-7 sigue valiendo para `resolve`).

| Id | Invariante | Verificación |
|---|---|---|
| INV-M2A-1 | **Cierre**: `compose` da una efectiva válida o un rechazo de código cerrado; nunca parcial. | Hypothesis; mutante «aplica la primera y falla la segunda» |
| INV-M2A-2 | **Identidad de lo no tocado** (valor, autoridad, políticas, origen, procedencia). | Propiedad; mutantes: re-resolver, cambiar origen |
| INV-M2A-3 | **Sólo sobre lo aceptado y vigente**: base = efectiva aceptada del mismo chat, legible, de esquema conocido y dentro de plazo; o el marco de una pendiente cancelada por rechazo citado. Nunca un turno que quedó en pregunta. | Un test y un mutante por causa |
| INV-M2A-4 | **Nada cruza a legacy** con ancla Foundation; `Turn.parsed_params == {}`. | Espías; mutante «puente»; mutante «entrega una operación con ancla Foundation» |
| INV-M2A-5 | **Un fragmento nunca se ejecuta solo**: con ancla o pendiente vigentes, sólo una consulta completa (universo citado + contenido) inicia una búsqueda sin heredar. | Propiedad sobre propuestas; mutantes: aceptar tema sin universo como búsqueda nueva; aceptar universo sin contenido |
| INV-M2A-6 | **El primer turno no cambia**: mismo SoQL, cláusulas y disposiciones que la base. | Replay del corpus + 620 capturas |
| INV-M2A-7 | **`resolve` no depende del ancla.** | Propiedad; mutante que pasa el ancla |
| INV-M2A-8 | **Sin híbridos**: tras una pendiente, lo ejecutado es `marco + respuesta`, `marco + delta` con rechazo citado, o sólo lo nuevo; cualquier otro caso hace 0 llamadas. | 11 filas de R5 §P2 + clase sintética; mutantes: F25 que ejecuta, F24 que elige, F29 que aplica |
| INV-M2A-9 | **Registro inmutable**; `trace_id` único; digest verificado; cola ilegible ⇒ inválida. | Mutantes |
| INV-M2A-10 | **Paginar es el mismo plan**: sin LLM, instante guardado, `plan_hash` igual, ids disjuntos. | Productor que lanza; mutantes |
| INV-M2A-11 | **Vista**: se recalcula sí y sólo sí era derivada; todo recálculo se anuncia. | Tabla PV × `SET`; mutantes |
| INV-M2A-12 | **Eco = ejecución**: los `EchoItem` cubren exactamente lo arrastrado, lo cambiado y toda procedencia heredada, inferida, por defecto o recortada; cada `display` sale del valor tipado. | Propiedad `echo_items` ↔ diferencia base/efectiva; mutantes por clase |
| INV-M2A-13 | **La tarjeta no lleva valores.** | Propiedad; mutante |
| INV-M2A-14 | **Un origen por condición.** | Contrato; mutante |
| INV-M2A-15 | **Barrera** para lectores legacy. | Test por lector |
| INV-M2A-16 | **Remoción sólo citada**: toda condición del ancla ausente de la efectiva tiene una operación citada sobre su dimensión. Desapariciones sin `CLEAR`: la cota opuesta por rango sin interior (F2) y el positivo invertido (F16); ambas con su `EchoItem`. | Propiedad `dimensiones(base − efectiva) ⊆ dimensiones(operaciones)`; mutantes: omitir una ranura la borra; `SET` de TIME borra GEOGRAPHY; inversión sin eco |
| INV-M2A-17 | **Herencia acotada**: sólo desde `compose`; vacía ≡ `resolve`; con recibo, procedencia y eco; nunca de dos unidades, de varios años, de ventana relativa, de `PESOS` ni de SMMLV; **nunca seguida de reemplazo de rango** (F26). | Bloque del kit; mutantes, incluido «hereda y reemplaza» |
| INV-M2A-18 | **VALUE por lado**: tabla exhaustiva lado previo × lado nuevo × relación de cifras. | IR oráculo sobre la clase sintética «VALUE por lado» |
| INV-M2A-19 | **Polaridad** (F16, F17): la inversión sólo ocurre con igualdad de valor canónico y un único positivo; en otro caso se añade. | Tabla exhaustiva; mutantes «invierte con dos positivos», «añade y deja contradicción» |
| INV-M2A-20 | **Alcance total y único**: todo turno recibe exactamente un `TurnScope`, por la precedencia de §5.2; un alcance `OUT_*` o `INFORM` hace 0 llamadas a la fuente. | Propiedad sobre turnos generados; mutante que cambia el orden |
| INV-M2A-21 | **Topes y defectos anunciados**: top-N > 100 ejecuta con 100; orden por fecha sin dirección es descendente; ambos con su `EchoItem`. | Tests; mutantes sin eco |

---

## 7. Plan

| Paso | Contenido | Dueño y archivos | Depende de | Gates |
|---|---|---|---|---|
| **M2A-0** | Correcciones del autor (Gold 2.1 §12.5), re-etiquetado con el kit 2.1, adjudicación, sello, partición | HERA + etiquetadores: `scripts/audits/m1_search/`, `docs/audits/m1_search_foundation/gold/followup_*.jsonl` | confirmaciones de §12 que afectan etiquetas | validador 0 errores (incluye la tabla F# → campos); acuerdo por campo; sello |
| **M2A-1** | `foundation_accepted.py`; `ProposedSlotDirective` + lector; proyección y guard sobre `AcceptedInterpretation`. Sin cambio de comportamiento | NÚCLEO | C1.2 integrado; antes de congelar el wire v5 (C1.3) | INV-M2A-6, -14; `tests/test_foundation_*.py` verdes |
| **M2A-2** | `foundation_accepted_store.py`; `_record` escribe el registro; línea de `clear()` | NÚCLEO + una línea en `conversation_store.py` | M2A-1 | INV-M2A-9; escrituras concurrentes |
| **M2A-3** | `foundation_delta.py` (ranuras, `compose`, reglas 1–17), `foundation_turn_scope.py` | NÚCLEO | M2A-1; Gold M2A | INV-M2A-1, -2, -3, -11, -16, -18, -19, -20 con IR oráculo: 100 % |
| **M2A-3b** | `foundation_authority_inheritance.py` + bloque del kit | NÚCLEO | M2A-3 | INV-M2A-17 |
| **M2A-3c** | Reglas de recibo y `resolve_with_inherited` | D-MONEY, D-TIME; reglas de recibo también D-BUYER y el dueño de ORDER/CARDINALITY (núcleo, paso E) | M2A-3b | kit por autoridad |
| **M2A-4** | Tarjeta de estado; ancla; fila `FOLLOWUP_DELTA` por dueño del ancla; `foundation_echo.py`; sección de estado del prompt v5; retirar `continues_foundation_turn` sólo en v5 | NÚCLEO (`foundation_runtime.py`, `foundation_route_owner.py`, `foundation_operation_host.py`, `foundation_operation_owners.py`, `foundation_presentation.py`, `foundation_messages.py`, `foundation_prompts_v5.py`, `foundation_state_card.py`) | M2A-2, M2A-3; C1.3, C1.4; E-* por clase | INV-M2A-4, -5, -7, -12, -13, -21; gates de §7.1 con productor real |
| **M2A-5** | `foundation_pending.py` (con span y ranuras candidatas); handler de opción | NÚCLEO | M2A-4 | INV-M2A-8 |
| **M2A-6** | `ResultSetRef`; `handlers/foundation_page.py`; permiso de página; eventos de orden, período, quitar filtro, continuar/reset | NÚCLEO (guard) + F | M2A-4; E-ORDER | INV-M2A-10 |
| **M2A-8** | Barrera de Radar-SAVE con mensaje | ARES no-núcleo: `app/core/workflow/handlers/radar.py` | — | INV-M2A-15 |
| **M2A-9** | Freeze | HERA | todo | global pareada ‖ ZEUS fresco sobre el SHA congelado; semgrep delta con `--base` |

Dependencias por clase: herencia de unidad y F6 ⇒ E-VALUE con `unit.*`; herencia de año, F8 y relativos ⇒ E-TIME;
F13 y regiones ⇒ E-GEO; F30 ⇒ E-BUYER con `buyer.territory`; F10, F20, F21 ⇒ E-ORDER+CARDINALITY; F27 ⇒ E-STATE/MOD;
anclas con ACTOR y F12 ⇒ E-ACTOR y E-BUYER/E-GEO; vista ⇒ F; caída del proveedor en un turno delta ⇒ G.

### 7.1 Gates (tolerancia cero, peor de ≥2 pasadas, Gold M2A sellado; se mide la efectiva resultante)

| Gate | Exigencia |
|---|---|
| Fragmento ejecutado solo | 0 |
| Condición arrastrada perdida sin operación citada | 0 |
| Híbrido tras aclaración; respuesta elegida por el producto (F24) | 0 |
| Valor heredado, inferido, por defecto o recortado sin su `EchoItem` | 0 |
| Herencia seguida de reemplazo (F26); SMMLV ejecutado (F14) | 0 |
| Composición sobre base inválida; turno con ancla Foundation que llega a legacy | 0 |
| Consulta completa tratada como delta, en casos `CLEAR` | 0 |
| Relativo al valor previo ejecutado | 0 |
| Alcance `OUT_*`/`INFORM` que llama a la fuente | 0 |
| Por clase: casos que el follow-up legacy acierta y M2a no (ni ejecuta bien ni responde con la disposición Gold) | 0 |
| IR oráculo por el host | 100 % de disposición, código y efectiva resultante |

### 7.2 Precondición de CUTOVER que M2a añade a C0 §5.3.6

Sustituir el punto 5 por:

> 5. **Conversation mínimo (M2a).** (a) Todo turno Foundation persiste su `AcceptedTurnRecord`; (b) gates de §7.1 en 0
> sobre el Gold M2A sellado (REAL re-etiquetado con el esquema 2.1 + sintético), ≥2 pasadas; (c) con ancla Foundation,
> 0 turnos llegan a `handle_normal_flow`; con ancla de un dueño legacy, la entrega cumple INV-C0-4/6; (d) paginación y
> selección de opción sobre resultados Foundation funcionan con el Frame de Entity Profile apagado; (e) lo que M2a no
> hace sobre un ancla Foundation responde con mensaje explícito (radar, otra operación, referente de fila); (f)
> `v5_previous` lee `m2a-record/1` o lo trata como ancla inválida; (g) el Gold M1 está re-sellado con F14 (T14).

En el mismo commit se borran `continues_foundation_turn` y `last_foundation_turn`. No se borra `followup_engine`.

---

## 8. Mapa archivo → dueño

| Archivo | Dueño |
|---|---|
| `foundation_accepted.py`, `foundation_accepted_store.py`, `foundation_delta.py`, `foundation_turn_scope.py`, `foundation_authority_inheritance.py`, `foundation_state_card.py`, `foundation_echo.py`, `foundation_pending.py` (nuevos, en `app/core/`) | NÚCLEO |
| `interpretation_contract.py`, `foundation_acceptance_loader.py`, `foundation_projection.py`, `foundation_guard.py`, `foundation_runtime.py`, `foundation_route_owner.py`, `foundation_operation_host.py`, `foundation_operation_owners.py`, `foundation_presentation.py`, `foundation_messages.py`, `foundation_prompts_v5.py` | NÚCLEO |
| `foundation_authority_contract.py`, `foundation_evidence_window.py`, `foundation_prompts.py` | **nadie** |
| `foundation_authority_values.py` | **nadie**, salvo el comentario de `MAX_TOP_N` (T10): cambio declarado, sin tocar tipos, por el NÚCLEO tras la enmienda |
| `tests/foundation_authority_kit.py` (bloque de herencia; caso de tope) | NÚCLEO |
| Autoridades VALUE, TIME, BUYER, ORDER, CARDINALITY (reglas de recibo; método de herencia en VALUE y TIME) | D-MONEY, D-TIME, D-BUYER, NÚCLEO (E-ORDER) |
| `conversation_store.py` (una línea), `orchestrator.py` (tupla de handlers) | NÚCLEO, adyacentes declarados |
| `app/core/handlers/foundation_page.py`, botones y anuncio | F |
| `app/core/workflow/handlers/radar.py` | ARES no-núcleo |
| `followup_engine.py`, `followup_mutations.py`, `followup_control_tokens.py`, `handlers/pagination.py`, `pagination_buffer.py`, `export_xlsx.py`, `workflow/continuation.py`, `admission_rules.py` | **nadie en M2a** |
| `scripts/audits/m1_search/*`, `docs/audits/m1_search_foundation/gold/*` | HERA |

---

## 9. Irreversibilidad y fronteras de confianza

| Cambio | Tipo | Mitigación |
|---|---|---|
| El intérprete recibe estado | Frontera de confianza | Tarjeta sólo de enumerados |
| El LLM decide la forma del turno | Frontera de confianza | El host decide el alcance por estructura (consulta completa o no); eco `CARRIED` y `CONTEXT_RESET` |
| Un valor depende de un turno anterior (unidad, año) | Frontera | Sólo desde `compose`, con procedencia, recibo y eco; frenada por F26 |
| El host aplica defectos (dirección de orden, tope) | Frontera | Regla de recibo + eco obligatorio (INV-M2A-21) |
| Opciones de aclaración con valor resuelto y guardado | Persistencia | El valor lo produjo la autoridad al preguntar; token ligado a `pending_id`; caduca |
| Almacén con datos personales (citas, spans ambiguos, `ActorValue`) | Persistencia | Mismo directorio, retención y borrado que el historial; fuera de git; ADR-060 |
| Esquemas `m2a-record/1`, `m2a-accepted/1` | Persistencia | Lector estricto; desconocido ⇒ ancla inválida |
| `interpretation_hash` = digest de la efectiva | Declarado | `plan_hash` de un primer turno no cambia |
| `ProposedSlotDirective` (con `CANCEL_PENDING`) | Contrato del núcleo | Dentro del esquema 2 si entra antes de activar v5 |
| `InheritingAuthority` y reglas de recibo nuevas | Interfaz de autoridades | Aditivo; contrato congelado sin cambio de tipos |

Rollback: M2A-1…3b no cambian comportamiento. Después: `foundation_v3` conserva el bloqueo actual; tras el CUTOVER,
`v5_previous`, que debe leer el almacén o tratarlo como ancla inválida.

---

## 10. Condiciones de STOP

1. Hace falta cambiar tipos de `foundation_authority_contract.py` o `foundation_authority_values.py` → METIS.
2. Un carril no puede emitir su regla de recibo sin cambiar su semántica de primer turno → STOP; la regla F que
   depende de ella queda en su caída segura (F5/F7: preguntar; F30: conservar con eco) y se informa.
3. INV-M2A-6 rojo no explicable por `from_acceptance` → METIS.
4. Tras 3 iteraciones del prompt sobre DEV queda algún fragmento ejecutado solo, híbrido o arrastrada perdida → NO_GO.
5. Algún paso necesita escribir params legacy desde el estado Foundation → STOP.
6. El Gold re-etiquetado contradice una regla de §5.2 → se cambia la regla o se eleva a producto; nunca la etiqueta.
7. Una tensión de la adenda (X1–X18) decide un caso y no está confirmada → no se implementa esa rama: pregunta al
   usuario.
8. Una sola pérdida silenciosa de ancla, un híbrido o un valor heredado sin eco en dogfood → volver a `v5_previous`.

---

## 11. Non-goals

- Frame de Entity Profile, CUTOVER con el Frame encendido (D13).
- Follow-ups con ancla de dueño legacy (9 de 65); cambio de operación por follow-up; palabra sola tras un conteo.
- Ejecutar otra operación, o un referente de fila, sobre un ancla de Foundation (sólo `INFORM`).
- Borrar módulos de follow-up o paginación legacy.
- ADD que estrecha dentro de una ranura positiva; deltas dentro de grupos OR/NOT.
- Operadores relativos al valor previo. SMMLV ejecutable (tarea futura del carril de dinero, F14).
- LLM con valores visibles. Cobertura de spans como gate.
- Radar-SAVE y hoja de filtros del Excel desde un resultado Foundation.
- Eco general de la capa 6. «Anunciar en vez de aclarar». «Volver atrás» (§13).
- Redactar la enmienda de ADR-103 o asignar número.

---

## 12. Cerrado desde v2, y lo que sigue abierto

**Cerrado.** P1 (vigencia) por F11. P2 (top-N arrastrado) por F10. P3 (unidad en un rango) por F6. P4 en parte: años
inclusivos (F8), región (F13), SMMLV (F14). **D15 / P5**: el experimento se hizo (`06_experimento_anunciar/`): en los 58
casos el filtro no aplicado era decisivo en la gran mayoría según dos jueces (45 y 47 de 58), así que «anunciar en vez
de aclarar» **muere como regla general**. Sobrevive sólo «subir un nivel con envolvente único» (≈16 de 446), como
candidata **posterior** al CUTOVER; no entra en M2a ni cambia sus gates.

**Abierto** (el diseño toma la lectura indicada; cada una pide confirmación):

| Id | Pregunta | Lectura del diseño | Origen |
|---|---|---|---|
| Q1 | ¿Se confirma F26 sobre la combinación «unidad heredada + rango sin interior», que los dos etiquetadores ejecutaban? ¿Y cómo se lee el follow-up siguiente? | Aclarar (F26) | T13; R0531, R0532 |
| Q2 | «No, mejor X» con fragmento: ¿la base es el marco pendiente? | Sí | T15c; X2 |
| Q3 | ¿«Consulta completa» = universo citado + contenido propio? | Sí | X3 |
| Q4 | Follow-up tras un turno aceptado cuya fuente falló: ¿se aplica? | Sí | T15a; X4 |
| Q5 | Otra operación o referente de fila sobre ancla Foundation: ¿mensaje explícito? | Sí (`INFORM`) | T16 |
| Q6 | F30 con compradores nacionales y con territorios contenidos; F16 con varios positivos; F18 con otras operaciones; F19 con sólo universo u orden; F11 con pendiente | las de §5.2 y §5.4 | X7, X8, X9, X12, X15 |
| Q7 | «El año anterior»: al valor previo o al reloj | sin lectura: hoy lo decide el productor (T5) | X18 |
| Q8 | F14 obliga a re-sellar el Gold M1 (2 registros): ¿quién adjudica? | fuera de M2a | T14 |
| Q9 | Lagunas del contrato M1: cota negada, cota sin comparador en un rango, comparador desordenado, «en adelante» | fuera de M2a | Gold 2.1 §13 |

---

## 13. «Volver atrás» (idea registrada, NO decidida)

No se diseña. Coste sobre este almacén, si se adopta:

1. **Deshacer el último paso**: cada registro ya guarda `base_trace_id`; es un handler de protocolo que relee esa
   efectiva y la re-ejecuta como turno nuevo (origen de evento), con eco. Sin LLM.
2. **Volver a la búsqueda inicial**: hoy exige recorrer la cadena de `base_trace_id`; un campo `root_trace_id` en el
   registro lo hace directo. Añadirlo después es un cambio de esquema; añadirlo ahora cuesta una línea.
3. Hay que definir «paso»: ¿una página, una acción de vista o una pregunta cuentan? ¿Deshacer un deshacer?
4. La vigencia: ¿se puede volver a un ancla vencida? (F11 diría que con confirmación.)
5. Fuera del almacén: frases cerradas y botón por canal, una fila en la adenda y ≈8 casos sintéticos.

---

## 14. Qué haría cambiar la decisión

- El productor no separa consulta completa de fragmento con la tarjeta de enumerados tras 3 iteraciones → añadir al
  prompt el texto del usuario del turno anterior, nunca valores; si tampoco basta, no hay CUTOVER.
- Producto responde Q2–Q5 en sentido contrario → cambian filas de §5.2, §5.4 o §5.5 (datos y una rama), no los tipos.
- Las reglas de recibo no pueden emitirse → las reglas F que dependen de ellas quedan en su caída segura y se
  reabre la opción de ampliar `AuthorityContext` con una enmienda expresa.
- El Gold re-etiquetado muestra que VALUE por lado o TOPIC que reemplaza producen resultados que el usuario no pidió →
  cambia la tabla de ranuras.
- El orquestador adelanta M2b → el almacén pasa a ser lo que el contrato de continuación referencia.

## Apéndice — límites

- No ejecuté tests, runtime ni productor. Lo marcado INFERENCIA se confirma con un test en su paso.
- Las etiquetas del sintético son anteriores a F16–F30: donde una regla F cambia una etiqueta que SA y SB compartían
  (F18, F19, F25, F26, F30), el diseño sigue la regla y el Gold 2.1 §12 lo lista.
- Las autoridades VALUE y TIME leídas son las del worktree de integración; sus adaptadores al contrato aún no existen.
- T12 (territorio del comprador) no lo verifiqué contra el directorio de entidades: sólo contra el tipo `BuyerValue`.

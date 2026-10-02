# M2a — Conversation mínimo antes del CUTOVER de SEARCH (v2, diseño cerrado)

Fecha: 2026-10-01. Autor: METIS. Sólo lectura de código y datos; sin pytest, LLM ni red. Sin número de ADR.
Sustituye a `M2A_DESIGN.md` (v1, que se conserva). El Gold REAL se cita sólo por id y conteo.

Fuentes: `main` = `secoppal` @ `4ec99bc1`; `c12` = `secoppal-m1-c12` @ `67c0badb`; `m1` = `secoppal-m1` @ `4940b73e`;
`par` = `secoppal-m1-int-par` @ `2dde6489`; `r5` = `secoppal-m1-int-r5`. Evidencia nueva (scratchpad de la sesión):
`HADES` = `hades/HADES_ALTERNATIVES.md`; `LA`/`LB` = informes de los etiquetadores; `CMP` = `gold_followups/comparison/`;
`OP` = `omission_probe/OMISSION_PROBE.md`; `M3` = `m3_consumers/M3_CONSUMER_MAP.md`.
HECHO / INFERENCIA / DECISIÓN como en v1.

---

## 0. Cambios respecto de v1

| Sección v1 | Qué cambió | Por qué |
|---|---|---|
| §1.1 | **Corrección**: v1 decía que ningún registro guarda el turno anterior. Los dos etiquetadores recuperaron los 65 previos de las trazas (`LA:57-59`, `LB:38`). La clasificación propia de v1 queda reemplazada por los dos etiquetados ciegos. | `LA`, `LB`, `CMP` |
| §1.1 | Anclas no-SEARCH: v1 decía 6 (incluía R0435). Los etiquetados dan **7 por acuerdo y 2 más sólo en A** (§3, T2). R0435 tiene ancla SEARCH y entra. | `labels.jsonl` A y B |
| §2, §5.2 | «Una condición» → **conjunto atómico de operaciones**; vocabulario `SET / CLEAR / KEEP / SET_ORDER / SET_UNIVERSE` (`REMOVE` pasa a llamarse `CLEAR`). | D1 |
| §4.5, Q1 | **Revocada** la ranura única de VALUE. VALUE tiene dos lados; regla de rango sin interior. | D2; R0310, R0464 |
| §5.2, Q2 | TOPIC nuevo reemplaza (ya era la recomendación); marcado reversible. | D3 |
| T3, Q3 | Monto sin unidad: de «aclarar» a **herencia por el host con procedencia**. | D4; R0531, R0532 |
| T3, Q4 | Mes sin año: de «preguntar si difieren» a **heredar el único año activo**. | D5; R0496–R0498 |
| §5.2 regla 4, Q5 | Vista: se recalcula **sólo si era derivada**; lo demás es pegajoso. | D6; R0081, R0106, R0276 |
| §5.2 (nuevo) | Extensión aditiva `resolve_with_inherited` para que la **misma** autoridad aplique la unidad o el año heredados (§5.3). El contrato congelado no cambia de bytes. | D4, D5 contra el código (T1) |
| §6 | Invariante nuevo de remoción citada (INV-M2A-16); INV-M2A-12 pasa de «eco completo» a **eco mínimo verificable** (§5.6). | D8, D11; `HADES` E4 |
| §5.6 | La cobertura de spans sale del diseño; queda una nota. | D10; `OP` |
| §5.2 | Guarda «fragmento nunca solo»: «búsqueda completa» exige **condición de contenido**, no basta el sujeto (v1 habría tratado como búsqueda nueva un fragmento con sustantivo de universo). | Gold R0094 (ambos: delta) |
| §8.2 | Se elimina el paso M2A-7 (Frame encendido). | D13 |
| §8.3 | El gate mide el **estado resultante**, no la forma del delta. | `HADES` E12 (58/65 frente a 56/65) |
| §14 | De 10 preguntas quedan 5 (§12). | D1–D15 |
| §8.1 | El esquema del Gold sale a `GOLD_FOLLOWUP_SCHEMA_v2.md`. | entregable 3 |

## 0.1 Registro de decisiones

| Id | Decisión | Estado en v2 |
|---|---|---|
| D1 | Conjunto atómico; vocabulario cerrado; correspondencia con el Gold | **Adoptada** (§5.2) |
| D2 | VALUE por lado; rango vacío ⇒ reemplazo anunciado; cambio explícito ⇒ reemplazo | **Adoptada con una precisión obligada por el Gold** (T3) |
| D3 | TOPIC reemplaza, con eco | **Adoptada**, reversible (dato de la tabla de ranuras) |
| D4 | Unidad heredada por el host, con procedencia y eco | **Adoptada**; exige la extensión de §5.3 (T1) |
| D5 | Año heredado del TIME previo; sin TIME previo, la regla de `time_authority` | **Adoptada**; misma extensión (T1) |
| D6 | Vista recalculada sólo si derivada | **Adoptada** |
| D7 | Tarjeta sin valores; relativos al valor previo aclaran | **Adoptada**, con garantía limitada declarada (T5) |
| D8 | Remoción sólo citada | **Adoptada**; una excepción declarada, la de D2 (T4) |
| D9 | `followup_engine` no muere; anclas no-SEARCH fuera | **Adoptada**; el conteo es 7–9, no 6 (T2) |
| D10 | Cobertura fuera como gate | **Adoptada** |
| D11 | Eco mínimo como requisito | **Adoptada** (§5.6) |
| D12 | Vigencia alineada con legacy | **Propuesta: 30 min**; contradice dos etiquetas (T6) → pregunta abierta |
| D13 | CUTOVER con Frame apagado | **Adoptada**; Frame encendido declarado fuera |
| D14 | Radar con mensaje explícito; Export y paginación | **Adoptada** (§5.7) |
| D15 | «Anunciar en vez de aclarar» | **No adoptada**; pregunta abierta con su experimento (§12) |

---

## 1. Hechos que sostienen el diseño

| Id | Hecho | Evidencia |
|---|---|---|
| H1 | Un turno Foundation escribe `parsed_params = {}` y `outcome_reason = "foundation_v3"`. | `main:app/core/foundation_runtime.py:221-234` |
| H2 | Todo follow-up tras un turno Foundation termina en `FOUNDATION_CONTINUATION_UNSUPPORTED`; la decisión usa `QueryRouter.parse` y `NEW_SEARCH_VERBS`. | `main:app/core/foundation_route_owner.py:93-114` |
| H3 | La aceptación no se persiste: la traza guarda sólo telemetría y el ledger un digest. | `main:app/core/feedback.py:263-264`; `main:app/core/turn_ledger.py:193` |
| H4 | La IR v2 ya tiene `Operation.FOLLOWUP_DELTA` y `ProposedJob.referent_evidence`. | `c12:app/core/foundation_operations.py:28`; `c12:app/core/interpretation_contract.py:171-202` |
| H5 | El guard ata la aceptación a un único texto (`validate_raw`). | `c12:app/core/foundation_guard.py:41-49` |
| H6 | `HostResolution` transporta sólo `resolved_ids`; **el recibo de la autoridad se pierde** en `to_resolution`. | `c12:app/core/interpretation_contract.py:498-515`; `c12:app/core/foundation_authority_contract.py:277-280` |
| H7 | `MoneyValue` = `amount_cop` + `comparator`: **no lleva unidad**. `TimeValue` congelado = `start`, `end_exclusive`, `precision`, `excluded`: **no lleva «año inferido»** (el tipo propio del carril sí: `par:app/core/time_authority.py:124-136`). | `c12:app/core/foundation_authority_values.py:317-376` |
| H8 | Una cifra sin unidad bajo el piso da `CLARIFY` con `VALUE_AMOUNT_UNGROUNDED` y **ningún valor**; la autoridad temporal ya acepta internamente un año implícito. | `par:app/core/value_authority.py:191-197`; `par:app/core/time_authority.py:250-264`; `c12:app/core/money_admission.py:62` |
| H9 | `AuthorityContext` = `reference_instant` + `datasets`; ADR-103 §4 dice que el contexto sólo retira autoridad. | `c12:app/core/foundation_authority_contract.py:161-180`; `m1:docs/decisions/2026-09-30_ADR-103…:30-32` |
| H10 | `is_followup` usa una ventana de 30 min; se llama en el flujo normal. | `main:app/core/conversation_store.py:626-640`; `main:app/core/_followup_constants.py:15`; `main:app/core/workflow/handlers/normal_flow.py:93` |
| H11 | `get_history` omite líneas corruptas en silencio; el sobre estricto sólo existe con el Frame encendido (default apagado). | `main:app/core/conversation_store.py:241-256`; `main:app/config.py:83-85` |
| H12 | Tras un turno Foundation: Radar-SAVE recorre 10 turnos y toma el primero con alcance o tema (salta el `{}`); la hoja «Búsqueda» del Excel lista 20 claves de `resolved_params` y Foundation sólo publica el sello de dataset; «Ver más» en Streamlit envía texto. | `main:app/core/workflow/handlers/radar.py:54-66`; `M3` §2.7.5, §2.8.4, §2.10.7, H2 |
| H13 | Mediciones sobre 446 búsquedas: errores silenciosos Foundation 23, legacy 170; un eco haría visibles 20 de los 23. Legacy acierta 194 (43 %), no 53 %. | `HADES` E1, E1b, E4 |
| H14 | La ventana de ≤4 tokens es casi siempre no vacía (126/176 propuestas con TIME tienen BUYER/GEO a ≤4 tokens; adyacencia real 10/176). | `OP:224-233` |

### 1.1 Lo que dicen los dos etiquetados (65 registros, ciegos)

| Medida | Valor |
|---|---|
| Acuerdo en `turn_kind` + disposición + (dimensión, operación) | 56/65; **58/65 en la IR resultante** (`CMP`; `HADES` E12) |
| Disposición | A: 62 EXECUTE / 3 CLARIFY; B: 64 / 1 |
| Ancla no-SEARCH | Ambos: R0189, R0319, R0375, R0441, R0483, R0560, R0561 (7). Sólo A (previos de cadena): R0320, R0321 |
| Dimensiones tocadas (B) | TIME 22, VALUE 20, GEOGRAPHY 8, ORDER 7, BUYER 4, STATE 2, MODALITY 2, TOPIC 2, ACTOR 1, universo 1 |
| Más de una dimensión | A: 5; B: 7 |
| Remoción | 1 registro (R0202), sólo VALUE |
| Valor que depende del previo | R0496–R0498 (año), R0531, R0532 (unidad), R0099 (referencia al año) |
| Vista recalculada | R0081, R0106, R0276: ambos pasan de oportunidades vigentes a procesos históricos |
| Hueco largo entre previo y follow-up | R0223 (≈4,5 h) y R0286 (≈12,5 h); ambos orgánicos, ambos etiquetados EXECUTE |
| Ausente en los 65 | polaridad negativa, top-N, tiempo relativo, respuesta a aclaración de SEARCH, referentes de fila, `CLEAR` distinto de VALUE, orden ascendente |

---

## 2. DECISIÓN (resumen)

1. `AcceptedInterpretation` (aceptación efectiva): condiciones con valor codificado, **origen** y **procedencia**;
   un único camino de proyección para todo turno.
2. Almacén propio por chat, append-only y estricto, clave `trace_id`; `Turn.parsed_params` sigue `{}`.
3. Delta = conjunto atómico de `SET / CLEAR / KEEP / SET_ORDER / SET_UNIVERSE` sobre ranuras; lo no mencionado se
   conserva byte-idéntico; sólo desaparece lo que una operación citada toca.
4. El LLM propone con una tarjeta de enumerados (sin valores); las mismas autoridades acreditan. La unidad y el año
   heredados los decide el host y los aplica la autoridad de la dimensión por una entrada separada y marcada.
5. Aclaración pendiente de una ranura: respuesta, búsqueda nueva completa o re-pregunta; nunca híbrido.
6. Referente = la consulta aceptada (intensión), no las filas visibles.
7. Eco mínimo obligatorio y verificable en todo turno delta.
8. Puente a `parsed_params` descartado (v1 §4.1; `HADES` §4 lo confirma).

---

## 3. Tensiones (decisiones adjudicadas contra el código o el Gold)

| Id | Tensión | Evidencia | Resolución en v2 |
|---|---|---|---|
| **T1** | D4 y D5 dicen «el host hereda», pero el host no puede: (a) ante un monto sin unidad la autoridad devuelve `CLARIFY` sin número (H8); (b) `MoneyValue` no guarda la unidad del valor previo (H7); (c) el recibo se pierde (H6); (d) el contexto de autoridad no admite insumos y ADR-103 §4 lo declara monótono (H9). | H6–H9 | Extensión **aditiva** fuera de los archivos congelados (§5.3): el host decide si hay herencia y con qué; la autoridad de la dimensión calcula. Exige que E-VALUE y E-TIME emitan dos reglas de recibo. Sin ellas, D4/D5 aclaran. Va a la enmienda 2 como excepción explícita. |
| **T2** | D9 dice «6/65». Los etiquetados dan 7 por acuerdo y 9 según A. | §1.1 | El conteo no cambia el diseño (el ancla la decide el último turno, no una lista). Corregir la cifra donde se cite. |
| **T3** | D2 literal («rango vacío») **no reproduce R0205**: cota superior inclusiva previa y cota inferior inclusiva nueva con la misma cifra dejan un rango de un solo punto, no vacío; los dos etiquetadores reemplazan. | `labels.jsonl` A y B, R0205 | Regla precisada: se reemplaza cuando el rango **no tiene interior** (cota inferior ≥ cota superior, sin mirar inclusividad). Reproduce R0187, R0205, R0531 y conserva R0310, R0464. El Gold manda. |
| **T4** | D8 («sólo desaparece con remoción citada») frente a D2 (la cota del lado opuesto desaparece sin `CLEAR`). | D2, D8 | «Citada» = existe en el turno una operación citada sobre **la misma dimensión**. La regla de D2 es la única desaparición sin `CLEAR` y siempre se anuncia. INV-M2A-16 lo fija así. |
| **T5** | D7: «el mes siguiente», «bájalo un poco» deben aclarar, pero nada estructural impide que el productor los proponga como TIME relativo o VALUE y que la autoridad los resuelva contra el reloj. | `par:time_authority.py:22-25` (ventanas relativas resueltas por reloj) | Garantía **limitada y declarada**: el prompt los emite como `UNSUPPORTED` (`relative_to_previous`) ⇒ `DELTA_RELATIVE_TO_PREVIOUS_UNSUPPORTED`. Se mide con el slice sintético (0 ejecuciones silenciosas como gate). No hay casos en los 65. |
| **T6** | D12 (30 min, como legacy) contradice dos etiquetas: R0223 y R0286 tienen huecos de ≈4,5 h y ≈12,5 h y **ambos etiquetadores** los marcan delta EXECUTE (confianza: A alta/baja, B baja/baja; atribución de sesión por proximidad en parte, `LA:154`). | `LB:40`; `labels.jsonl` | Propongo 30 min con salida de un toque (§5.4). Con ello el diseño **no satisface** esas dos etiquetas tal como están. No propongo cambiarlas: es la pregunta abierta P1. |
| **T7** | Guard atado a un solo texto (H5) ⇒ la efectiva no puede ser una `InterpretationAcceptance`; `interpretation_hash` cambia otra vez. | H5 | Igual que v1 (tipo nuevo, un camino de proyección). |
| **T8** | `v5_previous` como rollback debe leer el almacén o tratarlo como ancla inválida. | decisión 7 | Igual que v1. |
| **T9** | Cambio de universo y condiciones dependientes de dataset (STATE, MODALITY) o de campo (TIME: firma frente a publicación; hueco [UNIVTIME] de `LA:113`). | `c12:foundation_authority_contract.py:162-176`; R0094 | El valor se arrastra; si no cubre el dataset nuevo, aclara. El cambio de campo de fecha se anuncia (§5.6). |

---

## 4. Opciones comparadas (sólo lo que v2 decide de nuevo; el resto está en v1 §4)

### 4.1 Cómo aplicar la unidad o el año heredados (T1)

| Opción | Fallos | Decisión |
|---|---|---|
| A. Añadir `inherited` a `AuthorityContext` | Cambia el archivo congelado; un primer turno podría recibir herencia por error; contradice «el contexto sólo retira» | No |
| B. Que el host lea la cifra de la cita y la multiplique | Segunda lectura de VALUE fuera de su autoridad (dos autoridades para una dimensión) | No |
| **C. Protocolo aditivo `InheritingAuthority.resolve_with_inherited(evidence, ctx, inherited)` en un archivo nuevo; sólo lo llama `compose`** | Un bloque más en el kit; dos carriles añaden un método | **Sí** |
| D. No heredar: aclarar | Contradice a los dos etiquetadores (5 registros EXECUTE) | No (queda como caída segura cuando falta la procedencia) |

### 4.2 Vigencia del ancla (T6)

| Opción | Efecto | Decisión |
|---|---|---|
| **A. 30 min (igual que `is_followup`), y vencida ⇒ pregunta con opción de un toque «aplicar a la búsqueda anterior»** | 2/65 pasan de EXECUTE a una pregunta de un toque | **Propuesta** |
| B. Hasta el siguiente reset o búsqueda nueva, con tope de 24 h | Satisface las dos etiquetas; el riesgo es arrastre indebido **visible** (hay eco y «fragmento nunca solo») | Alternativa si producto confirma las etiquetas |
| C. Sin vigencia | Contexto de días atrás aplicado sin aviso de antigüedad | No |

El resto de opciones (puente, qué se persiste, dónde vive, qué ve el LLM) no cambia: `HADES` §4 concluye que event
sourcing es este diseño con otro nombre, que la recompilación completa colapsa en él en cuanto se exige remoción citada, y
que ningún follow-up de los 65 necesita que el LLM vea valores.

---

## 5. Diseño

### 5.1 Tipos y registro

Módulo `app/core/foundation_accepted.py` (núcleo). Registros inmutables.

```text
ConditionOrigin:      kind: SPAN | OPTION_EVENT | VIEW_ACTION | HOST_POLICY
                      trace_id; raw_sha256 (SPAN); condition_id (SPAN); event_id (eventos)

Provenance:           # hechos cerrados sobre CÓMO se obtuvo el valor; nunca texto libre
  unit: str | None            # VALUE: etiqueta de escala de una lista cerrada (regla de recibo «unit.<x>»)
  unit_source: STATED | PREVIOUS_ACCEPTED_VALUE | None
  year_source: STATED | CLOCK_INFERRED | PREVIOUS_ACCEPTED_TIME | None     # TIME
  inherited_from: str | None  # trace_id del ancla que aportó unidad o año

AcceptedCondition:    slot: Slot; request_ids; dimension; operator; polarity; role; level
                      resolved: tuple[str, ...]      # HostResolution.resolved_ids tal cual
                      authority; policies; origin: ConditionOrigin; provenance: Provenance

AcceptedRequest:      request_id; scope_id; subject; dataset
                      universe_origin: ConditionOrigin | None      # None sí y sólo sí subject is UNSPECIFIED
                      view: HostViewDecision | None                # obligatorio sí y sólo sí UNSPECIFIED

AcceptedInterpretation: schema "m2a-accepted/1"; requests; conditions (conjunción de primer nivel);
                        base_trace_id | None; delta: tuple[AppliedOp, ...]
```

`from_acceptance(acceptance, trace_id, receipts)` es el único constructor de un primer turno; `receipts` es el mapa
`condition_id → AuthorityReceipt` que la admisión ya tiene en la mano en los pasos E (H6: hoy se descarta). La
proyección recibe `AcceptedInterpretation`. El guard valida un **enlace por origen**: lo del turno actual contra el
texto actual; lo arrastrado contra el registro de su `trace_id` (existe, mismo chat, valor byte-idéntico).

Registro por turno (`app/core/foundation_accepted_store.py`):

```text
AcceptedTurnRecord:   schema "m2a-record/1"; trace_id; chat_id; turn_id; created_at_utc; profile; prompt_sha256
  turn_acceptance: dict                    # InterpretationAcceptance.to_dict() v2; se relee con load_acceptance
  effective: AcceptedInterpretation | None
  execution: ExecutionReceipt | None       # executed; reference_instant; interpretation_hash; plan_hash por petición
  echo: tuple[EchoItem, ...]               # §5.6; lo anunciado, para verificar que coincide con lo ejecutado
  result_ref: ResultSetRef | None          # §5.7
  pending: PendingClarification | None     # §5.5
  anchor_trace_id: str | None
  record_sha256: str
```

| Aspecto | Decisión |
|---|---|
| Ubicación | `<CONVERSATION_STORE_DIR>/<chat>.accepted.jsonl`, append-only; nunca en git, traza ni ledger |
| Lectura | Estricta; cola ilegible ⇒ continuidad inválida; esquema desconocido ⇒ `UNSUPPORTED_SCHEMA_VERSION` ⇒ ancla inválida |
| Escritura | Registro antes que el `Turn`, bajo `conversation_lock`; si el último `Turn` ya no es el ancla leída, se responde con aviso de contexto no conservado y `anchor_trace_id = None` |
| Borrado | `ConversationStore.clear()` añade el archivo a su tupla (`main:conversation_store.py:283-285`) |
| `Turn.parsed_params` | `{}` en todo turno Foundation |

### 5.2 Álgebra

**Ranuras.** `(dimensión, polaridad)` para TOPIC, BUYER, ACTOR, GEOGRAPHY, TIME, STATE, MODALITY,
CONTRACT_TYPE_OR_NATURE; **`VALUE.lower` y `VALUE.upper`** (D2); `ORDER`; `CARDINALITY`; `UNIVERSE`. Sólo son
direccionables los átomos de la conjunción de primer nivel.

| Operación | Efecto | Correspondencia con el Gold | Inválido ⇒ |
|---|---|---|---|
| `SET(slot, condición)` | Pone la condición; si la ranura estaba ocupada, sustituye todo lo que había en ella | `ADD` = SET sobre ranura vacía; `REPLACE` = SET sobre ranura ocupada (lo deriva el host del ancla; el productor no lo dice) | razón cerrada de la autoridad |
| `CLEAR(dimensión)` | Quita toda la dimensión (los dos lados en VALUE); exige cita | `REMOVE` | `DELTA_TARGET_ABSENT`, `DELTA_LEAVES_UNSAFE_QUERY` |
| `KEEP(dimensión)` | No cambia nada; afirma que la ranura existe y da dueño a la cita («de ese mismo periodo») | sin etiqueta propia; ver esquema v2 | `REFERENT_CONDITION_ABSENT` |
| `SET_ORDER` | Sustituye ORDER; cursor a cero | `ADD`/`REPLACE` de ORDER | razón de la autoridad |
| `SET_UNIVERSE` | Cambia el sujeto; con ancla dual se queda con el nombrado | `REPLACE` de UNIVERSE | `CARRIED_CONDITION_NOT_IN_UNIVERSE` |

Reglas (todas fijas; no hay precedencias configurables ni lenguaje de flujo):

1. **Atómico.** `compose(ancla, operaciones) → AcceptedInterpretation | DeltaRefusal`. Una operación no acreditada
   anula todo el delta; el ancla no cambia.
2. **Orden fijo de aplicación**: `CLEAR`, luego `SET*`, luego comprobación de `KEEP`. `KEEP` y `CLEAR` sobre la misma
   dimensión, o dos `SET` sobre la misma ranura ⇒ `DELTA_SLOT_CONFLICT`.
3. **Arrastre por identidad.** Lo que ninguna operación toca pasa byte-idéntico (valor, autoridad, políticas, origen,
   procedencia). Nunca se re-resuelve.
4. **VALUE por lado (D2).** Una cota nueva sustituye la de su lado; la del lado que faltaba completa el rango;
   «entre X y Y» son dos `SET`. Si tras aplicar el rango **no tiene interior** (inferior ≥ superior), la cota nueva
   reemplaza la restricción entera y el eco lo dice (T3). «Cambia el valor a…» se propone como `CLEAR(VALUE)` + `SET`:
   la instrucción explícita es la cita del `CLEAR`.
5. **TOPIC (D3).** `SET` reemplaza todos los temas positivos del ancla; el eco muestra el anterior. Reversible: es una
   fila de la tabla de ranuras.
6. **Herencia (D4, D5).** §5.3.
7. **Vista (D6).** Tras componer: si la petición tiene sujeto `UNSPECIFIED` y su `HostViewDecision.rule ∈ PV1–PV6`,
   se vuelve a ejecutar `decide_view` sobre el estado compuesto y el resultado se anuncia. Con sujeto dicho por el
   usuario o `rule = USER_OVERRIDE` no se recalcula; sólo un `SET_UNIVERSE` lo cambia.
8. **Políticas de petición.** Las políticas puras que leen el conjunto (ACTOR×STATE, ACTOR×TIME, `universe-c5@1`) se
   re-deciden sobre el estado compuesto. H7 no se re-ejecuta sobre lo arrastrado.
9. **Consistencia tipada** (lista cerrada): mismo valor canónico con polaridades opuestas en una dimensión ⇒
   `DELTA_CONTRADICTS_CARRIED`; `CLEAR(ORDER)` con CARDINALITY arrastrada ⇒ `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`.
10. **Relativo al valor previo (D7).** Condición `UNSUPPORTED` con etiqueta `relative_to_previous` ⇒
    `DELTA_RELATIVE_TO_PREVIOUS_UNSUPPORTED`.

**Propuesta en la IR v2.** Job `FOLLOWUP_DELTA` (evidencia obligatoria) con una petición cuyas condiciones son los
`SET`; o job `SEARCH` con `referent_evidence`. Pieza nueva: `ProposedSlotDirective(kind: CLEAR | KEEP, request_id,
dimension, evidence)` en `interpretation_contract.py`, validada por `validate_raw`.

**Tarjeta de estado (D7).** `{anchor: bool, subject, view_derived: bool, slots: [(dimensión, polaridad, operador,
nivel?)], order: bool, cardinality: bool, pending: (dimensión, razón) | null}`. Sólo enumerados; ningún valor, cita ni
nombre para mostrar.

**Delta o búsqueda nueva, por estructura.**

| Propuesta | Ancla Foundation vigente | Resultado |
|---|---|---|
| job `FOLLOWUP_DELTA` o job con `referent_evidence` | sí | delta |
| ídem | no hay / ilegible | aclarar: `FOLLOWUP_WITHOUT_HISTORY` / `ANCHOR_UNREADABLE` |
| ídem | vencida | aclarar `ANCHOR_EXPIRED` con opción de un toque (§5.4) |
| ídem | último turno de un dueño legacy | entrega de la **operación** a `LEGACY_FOLLOWUP` (C0 §5.3.2, R1–R4): disposición explícita `HANDOFF_LEGACY` |
| job `SEARCH` sin referente y con ≥1 **condición de contenido** (TOPIC, BUYER o ACTOR) | cualquiera | búsqueda nueva; no hereda nada. Si había ancla vigente, el eco lo dice y ofrece «aplicar a la búsqueda anterior» (§5.6) |
| job `SEARCH` sin referente y sin condición de contenido (sólo modificadores, con o sin sustantivo de universo) | sí | **no se ejecuta solo**: `DELTA_OR_NEW_AMBIGUOUS` con dos opciones-evento |
| ídem | no | camino SEARCH normal de M1 |

Sustituye a `continues_foundation_turn` sólo en el perfil v5.

### 5.3 Herencia de unidad y de año (D4, D5)

Archivo nuevo del núcleo `app/core/foundation_authority_inheritance.py`. Los archivos congelados no cambian.

```text
Inherited(frozen):   unit: str | None      # etiqueta de escala de la lista cerrada
                     year: int | None
                     source_trace_id: str  # ancla que lo aporta

class InheritingAuthority(Protocol):       # opcional; hoy sólo VALUE y TIME
  def resolve_with_inherited(self, evidence: DimensionEvidence, ctx: AuthorityContext,
                             inherited: Inherited) -> AuthorityVerdict: ...
```

| Regla | Contenido |
|---|---|
| Quién llama | Sólo `compose`, nunca la admisión de un primer turno. Un primer turno no puede heredar por construcción. |
| Cuándo (unidad, D4) | `resolve` devolvió no-RESOLVED con `detail = VALUE_AMOUNT_UNGROUNDED` **y** todas las cotas VALUE del ancla tienen la **misma** `provenance.unit`, que es una unidad de escala (no SMMLV, no «pesos» sin escala). Una unidad heredada antes cuenta como unidad activa (cadena R0531 → R0532). |
| Cuándo (año, D5) | `resolve` devolvió RESOLVED con la regla de recibo `year.inferred` (o no-RESOLVED con `detail = TIME_YEAR_NOT_STATED`) **y** el ancla tiene una sola condición TIME positiva cuyo intervalo cae en un único año civil y cuya `precision` no es `trailing_*` ni cota abierta. |
| Si no se cumple | Unidad: aclarar (pendiente de ranura VALUE). Año sin TIME previo: vale el veredicto de `resolve` (regla vigente de `time_authority`: ocurrencia más reciente no posterior a hoy), anunciado. Año con TIME previo de varios años o relativo: aclarar. |
| Qué hace la autoridad | Calcula el valor con el insumo y **marca** el recibo: `unit.inherited` o `year.inherited`. `resolve_with_inherited(e, c, Inherited vacío)` ≡ `resolve(e, c)`. |
| Qué registra el host | `provenance.unit_source = PREVIOUS_ACCEPTED_VALUE` o `year_source = PREVIOUS_ACCEPTED_TIME`, `inherited_from`, y un `EchoItem` obligatorio con el **valor completo** (§5.6). |
| El LLM | No aporta ni ve unidad ni año: cita el fragmento y propone operador. |

Requisitos sobre los carriles (sin tocar el contrato): la autoridad VALUE emite la regla de recibo `unit.<etiqueta>`
en todo RESOLVED; la autoridad TIME emite `year.inferred` cuando el año salió del reloj; ambas implementan
`resolve_with_inherited` (TIME ya tiene el parámetro interno: `par:time_authority.py:250-258`). El formato de regla de
recibo ya lo admite el contrato (`c12:foundation_authority_contract.py:49,209-219`).

### 5.4 Ancla y vigencia

| Turno | `anchor_trace_id` que deja |
|---|---|
| búsqueda nueva ejecutada; delta ejecutado; página; acción de vista | él mismo |
| búsqueda nueva no ejecutada | `None` |
| delta rechazado o con error de fuente; re-pregunta; caída del proveedor | el anterior |
| turno de un dueño legacy | no escribe registro ⇒ no hay ancla Foundation |

«Ejecutó» = todas las peticiones contestadas por la fuente (incluye cero filas). Vigencia propuesta: **30 min**
(`FOLLOWUP_WINDOW_MINUTES`, H10), medida con el instante inyectado. Vencida: `ANCHOR_EXPIRED` con dos opciones-evento
(«aplicar a la búsqueda anterior», «buscar sólo esto»); la primera compone sobre el ancla vencida sin otra llamada al
LLM (la propuesta del turno quedó guardada en el registro). Ver T6 y P1.

### 5.5 Aclaración pendiente

```text
PendingClarification: pending_id; asked_trace_id; base_trace_id | None
  frame: AcceptedInterpretation       # lo ya resuelto, sin la ranura abierta
  open_slot: Slot                     # exactamente una
  open_condition: (dimension, operator, polarity, level)
  reason (cerrado); detail; options: tuple[PendingOption, ...]; expires_at_utc; asks (tope 2)
PendingOption: option_id; display_key; resolved: tuple[str, ...]; authority   # valor ofrecido por la autoridad
```

Se crea sólo con una ranura sin resolver y todo lo demás resuelto.

| Entrada | Regla |
|---|---|
| Evento de opción (`option_id` + `pending_id`) | Sin LLM; el host toma `resolved` (origen `OPTION_EVENT`) y ejecuta `frame + opción`. |
| Texto: **respuesta** | Toda condición propuesta es de la ranura abierta, o de una ranura del marco y resuelve al **mismo** valor codificado. Ejecuta `frame + respuesta`. |
| Texto: **búsqueda nueva completa** | Job `SEARCH` con condición de contenido y sin referente: ejecuta sólo lo nuevo; cancela el pendiente; no fusiona. |
| Texto: **cambia el marco** | Cualquier otra cosa: 0 llamadas; re-pregunta nombrando la salida; `asks += 1`. |
| Caducidad / cancelación | Misma vigencia que el ancla; segundo cambio de marco; reset; cualquier turno que no sea respuesta ni re-pregunta. |

Reproduce las 11 filas de `r5:docs/audits/m1_wave1/R5_REPAIR.md` §P2 comparando por valor tipado en cada ranura, no por
lista de claves. No reemplaza `actor_pending` (dueño legacy).

### 5.6 Eco mínimo (D11)

Módulo `app/core/foundation_echo.py` (núcleo, puro): `echo_items(ancla, efectiva) → tuple[EchoItem, ...]`, generado
desde valores tipados y procedencia, nunca desde texto. No es el eco general de la capa 6: sólo cubre lo de esta tabla.

```text
EchoItem: kind (cerrado); slot; display (render del valor tipado); before_display | None; source (cerrado); action | None
```

| `kind` | Cuándo es obligatorio | Qué dice | Acción |
|---|---|---|---|
| `CARRIED` | Todo turno delta ejecutado: una línea compacta con las condiciones conservadas | dimensión y valor | quitar (evento `CLEAR`) |
| `CHANGED` | Toda ranura con `SET` sobre ranura ocupada (incluye TOPIC, D3) | valor nuevo y el anterior | — |
| `ADDED` / `CLEARED` | `SET` sobre ranura vacía / `CLEAR` | valor | — |
| `VALUE_RANGE_REPLACED` | Regla de rango sin interior (D2) | la cota nueva y que el rango anterior se sustituyó | restaurar el rango anterior no se ofrece (fuera) |
| `UNIT_INHERITED` | D4 | **valor completo con unidad** y que la unidad viene del filtro anterior | — |
| `YEAR_INHERITED` | D5 con `PREVIOUS_ACCEPTED_TIME` | mes y año completos y de dónde sale el año | — |
| `YEAR_CLOCK_INFERRED` | regla vigente de `time_authority` (también en primer turno) | el año asumido | — |
| `VIEW_DERIVED` | D6: vista recalculada (extiende INV-C0-10) | vista nueva y por qué | botones de vista |
| `UNIVERSE_CHANGED` | `SET_UNIVERSE` | universo nuevo; si hay TIME arrastrado, que el periodo aplica ahora a la fecha de ese universo (T9) | — |
| `CONTEXT_RESET` | Búsqueda nueva completa con ancla vigente | que no se conservaron los filtros anteriores | «aplicar a la búsqueda anterior» |
| `ANCHOR_EXPIRED` | §5.4 | antigüedad del contexto | las dos opciones |

Garantía verificable: el conjunto de `EchoItem` se guarda en el registro y se compara con la efectiva ejecutada
(INV-M2A-12). El texto visible se deriva de los `EchoItem`; un canal que no puede mostrar acciones muestra las líneas.

### 5.7 Referente al resultado y consumidores (D14)

```text
ResultSetRef: trace_id; interpretation_hash; groups: tuple[GroupRef, ...]
GroupRef: request_id; dataset; plan_hash_query; order_contract; page_limit; shown_ids; next_offset | None; total; total_quality
```

El referente es la consulta aceptada y su plan. `shown_ids` sirve para paginar sin solapes y para exportar.

| Consumidor | Insumo que da M2a | Fuera, declarado |
|---|---|---|
| **Paginación** («ver más», que en Streamlit llega como texto: `M3` §2.10.7) | Handler de protocolo `handle_foundation_page` **antes** de `handle_pagination` (`main:orchestrator.py:410-417`), activo si el último turno es Foundation con `result_ref`: re-proyecta la efectiva guardada con el `reference_instant` guardado, exige `plan_hash` igual, pide la página bajo un permiso de página del guard, descarta ids ya mostrados, escribe un registro nuevo. Sin LLM. | `PaginationBuffer` y `handle_pagination` no se tocan |
| **Orden** | Por texto: delta `SET_ORDER` = nueva ejecución ordenada en la fuente (nunca ordenar la página visible). Botón `[Mayor valor]`: evento → `SET_ORDER` con `OrderValue` de tabla cerrada (origen `VIEW_ACTION`). | — |
| **`[Elegir período]`** | Evento que abre un pendiente de ranura TIME; la respuesta entra por texto. | — |
| **Botones de vista (F)** | `handle_foundation_view_action` lee este almacén; `USER_OVERRIDE` produce una efectiva nueva y pegajosa (D6). | — |
| **Export** | Filas: las mostradas (`shown_ids`), como hoy. Hoja de filtros: M2a publica en el resultado `foundation_applied_conditions` (los `EchoItem` de la efectiva). | Cablear esa lista en `export_xlsx._write_search_sheet`; exportar «todo»; export con dos grupos (sigue bloqueado: `main:foundation_presentation.py:32-39`). Riesgo declarado: hasta entonces el Excel de un turno delta no lista sus filtros (`M3` §2.8.4). |
| **Radar-SAVE** | Ninguno. Guarda de barrera: si el último turno es Foundation, mensaje explícito «todavía no puedo guardar esta búsqueda»; nunca mirar turnos anteriores (H12). | Puente IR → `QueryFrame` (`M3` §2.7.4) |
| **Chips de refinamiento** (`seed_resolved_params`) | Ninguno: un turno Foundation no publica `resolved_params`. | Declarado: sin chips legacy tras un turno Foundation |
| **Acción de perfil sin texto** (`M3` H3) | — | No es de M2a; se anota para M2b/M3 |

Regla **R-BARRERA**: un turno Foundation es opaco para legacy y ningún lector legacy usa como referente un turno
anterior a él. Lectores afectados según `M3` §3: R5 (radar), R6 (paginación, sustituida por el handler propio), R1/R4
(flujo normal; no se alcanzan con ancla Foundation). R7 y R9 ya devuelven vacío.

Referentes fuera de M2a, con disposición explícita: fila («el segundo»), subconjunto de lo visible, comparación entre
resultados, deshacer, anáfora a entidad de otra capacidad ⇒ `REFERENT_EXTENSIONAL_UNSUPPORTED` o la fila de C0 §5.3.2.

---

## 6. Invariantes

Se preservan P-1…P-7 e INV-C0-1…12. INV-C0-7 sigue valiendo para `resolve`; la herencia es una entrada distinta y
marcada (INV-M2A-17).

| Id | Invariante | Verificación |
|---|---|---|
| INV-M2A-1 | **Cierre.** `compose` devuelve una efectiva válida o un rechazo de código cerrado; nunca parcial. | Hypothesis anclas × deltas; mutante «aplica la primera y falla la segunda» |
| INV-M2A-2 | **Identidad de lo no tocado** (valor, autoridad, políticas, origen, procedencia). | Propiedad; mutantes: re-resolver, cambiar origen |
| INV-M2A-3 | **Sólo sobre lo ejecutado y vigente.** | Un test y un mutante por causa (no ejecutada, vencida, otro chat, esquema, ilegible) |
| INV-M2A-4 | **Nada cruza a legacy** con ancla Foundation; `Turn.parsed_params == {}`. | Espías en `handle_normal_flow` y `QueryRouter.parse`; mutante «puente» |
| INV-M2A-5 | **Un fragmento nunca se ejecuta solo**: con ancla vigente, `SEARCH` sin condición de contenido ⇒ 0 llamadas. | Propiedad; mutante que acepta el sujeto como contenido |
| INV-M2A-6 | **El primer turno no cambia**: mismo SoQL, cláusulas y disposiciones que la base. | Replay del corpus de regresión + 620 capturas |
| INV-M2A-7 | **`resolve` no depende del ancla.** | Propiedad: misma ventana, anclas distintas; mutante que pasa el ancla |
| INV-M2A-8 | **Sin híbridos** tras una aclaración. | 11 filas de R5 §P2 + propiedad; mutantes AN-1…AN-14 sobre ranuras |
| INV-M2A-9 | **Registro inmutable**; `trace_id` único; digest verificado; cola ilegible ⇒ inválida. | Mutantes: sobrescribir, saltar la línea corrupta |
| INV-M2A-10 | **Paginar es el mismo plan**: sin LLM, instante guardado, `plan_hash` igual, ids disjuntos. | Productor que lanza si se le llama; mutantes de reloj y de offset |
| INV-M2A-11 | **Vista**: se recalcula sí y sólo sí era derivada (PV1–PV6); `USER_OVERRIDE` y sujeto explícito no cambian sin `SET_UNIVERSE`; todo recálculo se anuncia. | Tabla PV × cada `SET`; mutantes «recalcula tras override» y «arrastra una derivada» |
| INV-M2A-12 | **Eco = ejecución.** Los `EchoItem` del registro cubren exactamente: toda condición arrastrada, toda ranura cambiada, y toda procedencia heredada, inferida o derivada; cada `display` se genera del valor tipado ejecutado. | Propiedad: `echo_items` ↔ diferencia ancla/efectiva; mutantes: omitir `UNIT_INHERITED`, omitir arrastradas, `display` desde la cita |
| INV-M2A-13 | **La tarjeta no lleva valores.** | Propiedad sobre registros generados; mutante que añade un nombre |
| INV-M2A-14 | **Un origen por condición** (span de su turno, o evento del host). | Validación de contrato; mutante «origen vacío» |
| INV-M2A-15 | **Barrera** para lectores legacy. | Un test por lector (radar; paginación legacy tras turno Foundation) |
| INV-M2A-16 | **Remoción sólo citada (D8).** Si una condición del ancla no está en la efectiva, el turno contiene una operación citada (`CLEAR` o `SET`) sobre **su dimensión**; lo no mencionado se conserva. La única desaparición sin `CLEAR` es la cota opuesta de VALUE por la regla de rango sin interior, y lleva `VALUE_RANGE_REPLACED`. | Propiedad: `dimensiones(ancla − efectiva) ⊆ dimensiones(operaciones citadas)`. Mutantes: propuesta que omite una ranura la borra; `SET` de TIME borra GEOGRAPHY; rango sin interior sin eco |
| INV-M2A-17 | **Herencia acotada.** `resolve_with_inherited` sólo se alcanza desde `compose`; con herencia vacía equivale a `resolve`; todo valor heredado lleva la regla de recibo, la procedencia y su `EchoItem`; nunca se hereda de un ancla con dos unidades, varios años o ventana relativa. | Bloque del kit de conformidad; mutantes: herencia en primer turno, heredar con dos unidades, heredar año de un rango plurianual, valor heredado sin eco |
| INV-M2A-18 | **VALUE por lado.** Una cota sustituye sólo la de su lado salvo rango sin interior. | Tabla exhaustiva lado previo × lado nuevo × orden de cifras; reproduce R0187, R0205, R0310, R0464, R0531 con IR oráculo |

---

## 7. Plan

Gold antes del código: esquema y hoja de adjudicación en `GOLD_FOLLOWUP_SCHEMA_v2.md`.

| Paso | Contenido | Dueño y archivos | Depende de | Gates |
|---|---|---|---|---|
| **M2A-0** | Re-etiquetado con esquema v2, adjudicación, slice sintético, sello, partición DEV/TEST | HERA + etiquetadores (no el implementador): `scripts/audits/m1_search/gold_schema_m2a.py`, `docs/audits/m1_search_foundation/gold/followup_*.jsonl` | P1 y P2 (§12) decididas o aparcadas | validador 0 errores; acuerdo por campo; sello |
| **M2A-1** | `foundation_accepted.py`; `ProposedSlotDirective` + lector; proyección y guard sobre `AcceptedInterpretation`. Sin cambio de comportamiento | NÚCLEO | C1.2 integrado; **antes de congelar el wire v5 (C1.3)** | INV-M2A-6, -14; `tests/test_foundation_*.py` verdes; mutantes |
| **M2A-2** | `foundation_accepted_store.py`; `_record` escribe el registro; línea de `clear()` | NÚCLEO + una línea en `conversation_store.py` (adyacente declarado) | M2A-1 | INV-M2A-9; prueba de dos escrituras concurrentes; traza y ledger sin campos nuevos con texto |
| **M2A-3** | `foundation_delta.py` (ranuras, `compose`, rechazos, VALUE por lado, vista) | NÚCLEO | M2A-1; Gold M2A | INV-M2A-1, -2, -3, -11, -16, -18 con **IR oráculo**: 100 % de estado resultante |
| **M2A-3b** | `foundation_authority_inheritance.py` + bloque del kit | NÚCLEO | M2A-3 | INV-M2A-17 con autoridades juguete |
| **M2A-3c** | `resolve_with_inherited` y reglas de recibo `unit.<x>` / `year.inferred` | **D-MONEY** y **D-TIME**, cada uno en su archivo de autoridad | M2A-3b | kit de conformidad por autoridad |
| **M2A-4** | `foundation_state_card.py`; ancla; fila `FOLLOWUP_DELTA` por dueño del ancla; guarda de fragmento; `foundation_echo.py`; sección de estado del prompt v5; retirar `continues_foundation_turn` sólo en v5 | NÚCLEO (`foundation_runtime.py`, `foundation_route_owner.py`, `foundation_operation_host.py`, `foundation_operation_owners.py`, `foundation_presentation.py`, `foundation_messages.py`, `foundation_prompts_v5.py`) | M2A-2, M2A-3; C1.3, C1.4; E-* por clase | INV-M2A-4, -5, -7, -12, -13; gates de §7.1 con productor real |
| **M2A-5** | `foundation_pending.py`; handler de opción conectado en la tupla de handlers | NÚCLEO | M2A-4 | INV-M2A-8; clase «respuesta a aclaración» del slice sintético |
| **M2A-6** | `ResultSetRef`; `handlers/foundation_page.py`; permiso de página en `foundation_guard.py`; eventos de orden, período y quitar filtro | NÚCLEO (guard) + **F** (handlers, botones) | M2A-4; E-ORDER | INV-M2A-10 |
| **M2A-8** | Barrera de Radar-SAVE con mensaje explícito | ARES no-núcleo: `app/core/workflow/handlers/radar.py` | — (paralelo) | INV-M2A-15 |
| **M2A-9** | Freeze | HERA | todo | global pareada por nodeid ‖ ZEUS fresco sobre el SHA congelado; semgrep delta con `--base` |

Paralelo: M2A-0 ‖ M2A-1; M2A-8 ‖ cualquiera; M2A-3c ‖ M2A-4. El resto, serie en el núcleo. No existe M2A-7 (D13).

Dependencias por clase: TIME rango/mes y herencia de año ⇒ E-TIME activa; VALUE inclusivos, rangos y herencia de unidad
⇒ E-VALUE activa (sin ella no hay `provenance.unit` y D4 aclara); GEOGRAPHY municipio/región ⇒ E-GEO; ORDER ⇒
E-ORDER+CARDINALITY; anclas con ACTOR (R0418, R0431, R0435) ⇒ E-ACTOR; vista ⇒ F; caída del proveedor en un turno delta ⇒ G (ancla intacta, misma
tarjeta en el proveedor B). La sección de estado del prompt entra antes de congelar el SHA de v5; si ya está
congelado, es una versión nueva con su corrida ACTIVATE.

### 7.1 Gates (tolerancia cero, peor de ≥2 pasadas, Gold M2A sellado; se mide la **efectiva resultante**)

| Gate | Exigencia |
|---|---|
| Fragmento ejecutado solo | 0 |
| Condición arrastrada perdida sin operación citada (INV-M2A-16) | 0 |
| Híbrido tras aclaración | 0 |
| Valor heredado, inferido o derivado sin su `EchoItem` | 0 |
| Delta sobre ancla inválida; turno con ancla Foundation que llega a legacy | 0 |
| Búsqueda nueva completa tratada como delta, en casos `CLEAR` | 0 |
| Relativo al valor previo ejecutado (clase sintética) | 0 |
| Por clase: casos que el follow-up legacy acierta y M2a no (ni ejecuta bien ni aclara con la razón Gold) | 0 |
| IR oráculo por el host | 100 % de disposición y de efectiva resultante |

### 7.2 Precondición de CUTOVER que M2a añade a C0 §5.3.6

Sustituir el punto 5 por:

> 5. **Conversation mínimo (M2a).** (a) Todo turno Foundation persiste su `AcceptedTurnRecord`; (b) gates de §7.1 en 0
> sobre el Gold M2A sellado (REAL re-etiquetado + sintético), ≥2 pasadas; (c) con ancla Foundation, 0 turnos llegan a
> `handle_normal_flow`; con ancla de un dueño legacy, la entrega cumple INV-C0-4/6 y su disposición es explícita;
> (d) paginación y selección de opción sobre resultados Foundation funcionan con el Frame de Entity Profile en su
> default (apagado); (e) Radar-SAVE tras un turno Foundation responde con mensaje explícito; (f) `v5_previous` lee
> `m2a-record/1` o lo trata como ancla inválida.

En el mismo commit se borran `continues_foundation_turn` y `last_foundation_turn`. **No** se borra `followup_engine` (D9).

---

## 8. Mapa archivo → dueño

| Archivo | Dueño |
|---|---|
| `app/core/foundation_accepted.py`, `foundation_accepted_store.py`, `foundation_delta.py`, `foundation_authority_inheritance.py`, `foundation_state_card.py`, `foundation_echo.py`, `foundation_pending.py` (nuevos) | NÚCLEO |
| `interpretation_contract.py`, `foundation_acceptance_loader.py`, `foundation_projection.py`, `foundation_guard.py`, `foundation_runtime.py`, `foundation_route_owner.py`, `foundation_operation_host.py`, `foundation_operation_owners.py`, `foundation_presentation.py`, `foundation_messages.py`, `foundation_prompts_v5.py` | NÚCLEO |
| `foundation_authority_contract.py`, `foundation_authority_values.py`, `foundation_evidence_window.py`, `foundation_prompts.py` | **nadie** (congelados; M2a no los cambia) |
| `tests/foundation_authority_kit.py` (bloque aditivo de herencia) | NÚCLEO |
| Autoridad VALUE y autoridad TIME (método de herencia y reglas de recibo) | D-MONEY, D-TIME |
| `conversation_store.py` (una línea), `orchestrator.py` (tupla de handlers) | NÚCLEO, adyacentes declarados |
| `app/core/handlers/foundation_page.py`, botones y anuncio | F |
| `app/core/workflow/handlers/radar.py` | ARES no-núcleo |
| `followup_engine.py`, `followup_mutations.py`, `followup_control_tokens.py`, `handlers/pagination.py`, `pagination_buffer.py`, `export_xlsx.py`, `workflow/continuation.py`, `admission_rules.py` | **nadie en M2a** |
| `scripts/audits/m1_search/*`, `docs/audits/m1_search_foundation/gold/*` | HERA |

---

## 9. Irreversibilidad y fronteras de confianza

| Cambio | Tipo | Mitigación |
|---|---|---|
| El intérprete recibe estado | Frontera de confianza | Tarjeta sólo de enumerados (INV-M2A-13) |
| El LLM decide «delta o nueva» | Frontera de confianza | Guarda estructural, ancla verificada, eco `CONTEXT_RESET` y `CARRIED`: el error residual es visible |
| Un valor ejecutable depende de un turno anterior (unidad, año) | **Frontera nueva** | Sólo desde `compose`, sólo de un ancla ejecutada, con procedencia, regla de recibo y eco obligatorio (INV-M2A-17) |
| Almacén con datos personales (citas, `ActorValue`) | Persistencia | Mismo directorio, retención y borrado que el historial; fuera de git; ledger y traza con digests; ADR-060 |
| Esquemas `m2a-record/1`, `m2a-accepted/1` | Persistencia | Lector estricto; desconocido ⇒ ancla inválida; la vida útil de un ancla es corta, caducar es barato |
| `interpretation_hash` = digest de la efectiva | Declarado | `plan_hash` de un primer turno no cambia (INV-M2A-6) |
| `ProposedSlotDirective` | Contrato del núcleo | Dentro del esquema 2 si entra antes de activar v5 |
| Extensión `InheritingAuthority` | Interfaz de autoridades | Aditiva, archivo nuevo, opcional; el contrato congelado no cambia de bytes; decisión METIS registrada para la enmienda 2 |
| Eventos de protocolo como origen | Frontera de confianza | Token ligado a `pending_id`/`trace_id`; valor producido por el host |

Rollback: M2A-1…3b no cambian comportamiento. Después: `foundation_v3` conserva el bloqueo actual; tras el CUTOVER,
`v5_previous` (T8). Los `.accepted.jsonl` son inertes para versiones que no los leen.

---

## 10. Condiciones de STOP

1. Hace falta cambiar los bytes de `foundation_authority_contract.py` o `foundation_authority_values.py` → METIS.
2. Un carril no puede emitir `unit.<x>` / `year.inferred` sin cambiar su semántica de primer turno → STOP; D4/D5 se
   quedan en aclarar y se informa.
3. INV-M2A-6 rojo no explicable por `from_acceptance` → METIS.
4. Tras 3 iteraciones del prompt sobre DEV queda algún fragmento ejecutado solo, híbrido o arrastrada perdida → NO_GO.
5. Algún paso necesita escribir params legacy desde el estado Foundation → STOP (es el puente).
6. El Gold M2A re-etiquetado contradice una regla de §5.2 (por ejemplo, TOPIC que estrecha, o cardinalidad que no se
   arrastra) → se cambia la tabla o la regla **antes** de seguir; nunca la etiqueta.
7. Un lector legacy de `Turn.parsed_params` no admite barrera con un guard pequeño → METIS.
8. Una sola pérdida silenciosa de ancla, un híbrido o un valor heredado sin eco en dogfood → volver a `v5_previous`.

---

## 11. Non-goals

- Frame de Entity Profile, `ContinuationContract` como modelo único, CUTOVER con el Frame encendido (D13: exigiría un
  dueño `FOUNDATION` en el contrato de continuación y reglas de admisión; `main:admission_rules.py:161-181`).
- Follow-ups con ancla no-SEARCH (7–9 de 65): siguen en su dueño legacy, con entrega explícita. Cambio de operación por
  follow-up (conteo → lista, perfil → búsqueda).
- Borrar `followup_engine` o cualquier módulo de follow-up o paginación legacy.
- ADD que estrecha dentro de una ranura; deltas sobre grupos OR/NOT; deshacer.
- Operadores relativos al valor previo; referentes de fila o de subconjunto visible.
- LLM con valores visibles. Cobertura de spans como gate (D10): instrumento a re-medir con v5; M2a no depende de ella.
- Radar-SAVE y hoja de filtros del Excel desde un resultado Foundation (sólo insumo o mensaje, §5.7).
- Eco general de la capa 6. «Anunciar en vez de aclarar» (D15).
- Redactar la enmienda 2 o asignar número de ADR.

---

## 12. Preguntas abiertas (sólo lo no decidido)

| Id | Pregunta exacta | Recomendación | Evidencia |
|---|---|---|---|
| P1 | Un fragmento que llega horas después de la búsqueda (≈4,5 h y ≈12,5 h en los datos): ¿se aplica solo a la búsqueda anterior, o se pregunta con una opción de un toque? | 30 min + un toque (opción 4.2-A). Si se confirma «se aplica solo», la vigencia pasa a 24 h o hasta la siguiente búsqueda nueva: es una constante. | R0223, R0286: ambos etiquetadores EXECUTE; confianza baja |
| P2 | Con top-N arrastrado («los 10 más caros») y un orden nuevo («ahora los más recientes»): ¿se conserva el 10? | Conservar (D8: lo no mencionado se conserva) y anunciarlo. | 0 casos en los 65: lo decide el slice sintético |
| P3 | Dentro de un mismo rango, una cifra sin unidad junto a otra con unidad (por ejemplo «entre 200 y 3 mil millones»): ¿la unidad se reparte? Es regla de la autoridad VALUE en primer turno, no de M2a. | Repartir sólo si la otra cota del mismo rango la trae; anunciado. | R0192 (ambos EXECUTE); hueco [UNIT] |
| P4 | Lagunas del contrato Gold que no son de M2a pero bloquean el re-etiquetado: inclusividad de «de A a B» en años, «X en adelante», comparador desordenado, SMMLV ejecutable, vocabulario de región. | Fijarlas en el contrato Gold por adjudicación; ver hoja en `GOLD_FOLLOWUP_SCHEMA_v2.md`. | `LB:136-147`; `LA:121` |
| P5 | (D15) ¿Ejecutar el superconjunto más estrecho acreditable y anunciarlo, en vez de aclarar, para modificadores no hospedables? Cambia el contrato del gate de CUTOVER. | No decidir sin el experimento H1.5 de `HADES` §3: sobre los 58 ids, derivar el envolvente y adjudicar a ciegas; muere si <30 tienen envolvente único o si en >1/3 la condición no aplicada define la intención. | `HADES` E3 (58/446) |

---

## 13. Qué haría cambiar la decisión

- El productor no distingue delta de búsqueda nueva con la tarjeta de enumerados tras 3 iteraciones → añadir al prompt
  el **texto del usuario** del turno anterior, nunca valores; si tampoco basta, no hay CUTOVER.
- El Gold M2A muestra que TOPIC estrecha o que VALUE por lado produce rangos que el usuario no pidió → cambia la tabla
  de ranuras, no la arquitectura.
- Las reglas de recibo no pueden emitirse sin tocar los carriles a fondo → D4/D5 quedan en aclarar con opción y se
  reabre la opción 4.1-A con una enmienda expresa del contrato.
- El orquestador adelanta M2b → el almacén M2a pasa a ser lo que el contrato de continuación referencia; los tipos no
  cambian.
- P5 sobrevive a su experimento → se separan los gates «ensanche silencioso = 0» y «paridad por clase»; M2a no cambia,
  gana un `EchoItem` más.

## Apéndice — límites

- No ejecuté tests, runtime ni productor. Todo lo marcado INFERENCIA se confirma con un test en su paso.
- Leí los `labels.jsonl` para contrastar D2, D4 y D5 con los valores etiquetados; no modifiqué ninguno.
- Las autoridades VALUE y TIME leídas son las del worktree de integración (`par`), bajo una suite en curso; no afirmo
  nada de su estado final ni de sus adaptadores al contrato, que aún no existen (registro vacío, H6).
- T3 se apoya en un solo registro (R0205) con acuerdo de ambos etiquetadores.

# M2A_CONVERSATION_CONTRACT_V1 — contrato conversacional mínimo de Foundation (M2a)

**Fecha:** 2026-10-01. **Enmienda R3:** 2026-10-02 (representabilidad; R3b repara el NO_GO de ZEUS; registro y motivo
en el Anexo D).
**Estado:** normativo. Es la **única autoridad normativa de M2a**, reconocida por ADR-103 Enmienda 5. Sin efecto en
runtime hasta que cada paso M2A-n se integre con autorización expresa. Tras R3 rige la congelación de §20.4.
**Base de código verificada:** `main` = `8c1f8c54` (IR v2 / C1.2 integrado; STATE y MODALITY integradas); la enmienda
R3 se verificó sobre `fa0b6de8` (Paso 0 de la ola M2a).
**Gold que lo interpreta:** `docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json` (`M2A_GOLD` = 49 REAL + 160 SYNTHETIC = 209;
`FOLLOWUP_FRONTIER` = 16 + 41 = 57; release r3b: copia sellada en `docs/audits/m2a_conversation/gold/`, que difiere del
candidato asesor por las adjudicaciones U1/U2/D1 del 2026-10-02, `gold/ADJUDICATION_2026-10-02.md`, por las cuatro
adiciones de U3 y por las seis de U25, `gold/ADJUDICATION_R3.md`; sellado en el paso B4, re-sellado en R3b-2 tras el
NO_GO de ZEUS y por última vez en R3c, Anexo D.3). El sello lleva el
sha256 de este archivo: cambiar una línea de este contrato rompe el sello y exige re-sellar con motivo.
**Evidencia de origen (no normativa):** `docs/audits/m2a_advisory_2026-10-01/` (carril asesor B, importado).
**Trazabilidad:** `docs/audits/m2a_conversation/M2A_TRACEABILITY_MATRIX.md`. **Plan:** `docs/audits/m2a_conversation/M2A_IMPLEMENTATION_PLAN.md`.

Este documento es autosuficiente: para implementar M2a no hace falta leer los informes asesores. Donde cita una
regla de origen (F#, A#, D#, OQ-#) es para trazar, no para delegar.

---

## 0. Una autoridad por cuestión

| Cuestión | Autoridad única | Lo que queda sustituido |
|---|---|---|
| Comportamiento conversacional de M2a (tipos, álgebra, alcance, pendientes, referentes, eco, gates M2a) | **este contrato** | `M2A_DESIGN_v2_1.md` y `GOLD_FOLLOWUP_SCHEMA_v2_1.md` donde difieran; F2, F14, F26 de la adenda; ADR-103 Enm. 4 D7 en las cláusulas de la tabla C.1 |
| Forma de la IR del primer turno, autoridades por dimensión, tipos `ExecutableValue`, tope top-N, pines de frontera | ADR-103 (Enm. 1–4) y el código de `main` | nada |
| Etiquetas esperadas de cada caso | `M2A_GOLD` sellado | nada (el Gold no se adapta al contrato; el contrato tampoco se adapta a un caso: ver §20.3) |
| Huecos diferidos | `FOLLOWUP_GAPS_LEDGER` (§19), con el comportamiento provisional que fija este contrato | nada |

Reglas de producto que este contrato hace normativas, con su origen: F1–F30 (adenda del 2026-10-01) **corregidas por**
A1–A9.1 (adjudicaciones del usuario del 2026-10-01; A8.1 y A9.1 son las últimas y prevalecen) y por las decisiones
OQ-1…OQ-6 de ADR-103 Enm. 4. Cuando dos reglas de origen chocan, manda la posterior; la tabla C.1 dice cuál.

---

## 1. Alcance y non-goals

### 1.1 Dentro (M2a)

1. Persistir, por chat, la **interpretación aceptada efectiva** de cada turno Foundation (`AcceptedInterpretation`).
2. Componer **follow-ups sobre un ancla SEARCH de Foundation** mediante un `SemanticPatch` atómico.
3. Distinguir **búsqueda nueva** de **delta** por estructura (§7).
4. **Aclaración pendiente** con `candidate_patches[]` genéricos y su protocolo de respuesta y cancelación (§8, §17).
5. **Referentes**: `semantic_ref` y `result_set_ref` separados (§9); paginación y eventos de protocolo sobre un
   resultado Foundation (vista, orden, quitar filtro, continuar/reset).
6. **Eco mínimo** verificable de todo lo arrastrado, heredado, inferido, por defecto, recortado o sustituido (§11).
7. **Comportamiento provisional fail-closed** para todo lo que M2a no hace sobre un ancla Foundation (`INFORM` o
   `CLARIFY`, nunca legacy, nunca la fuente) (§19).

El alcance medible es exactamente el slice `M2A_GOLD` del sello (todos sus casos tienen `m2a_scope ∈ {IN,
IN_NEW_SEARCH, IN_PROTOCOL}`, salvo `OUT_UNSUPPORTED_PROPOSAL`: la fila 4b de §7.1 con ancla o pendiente la responde M2a
misma, `INFORM` con la pendiente intacta; SF199, U25).

### 1.2 Fuera (non-goals)

- Follow-ups con ancla de un **dueño legacy** (PROFILE_ACTOR, COUNT, RANK_SUPPLIERS…): `HANDOFF_LEGACY` /
  `LEGACY_FOLLOWUP`, fuera de M2a (G-FU-11, M3).
- Ejecutar **otra operación** sobre un ancla Foundation (contar, sumar, seguir, exportar «todo»): `INFORM` (G-FU-16, M3).
- **Referentes extensionales** (filas, «el segundo», subconjuntos de lo mostrado): `INFORM` en cuanto el wire 5.1
  los distinga (señal de M2A-5; G-FU-14); su ejecución es M2b. M2A-6 no se activa sin esa señal (§7.2).
- **Anáforas entre capacidades**, Frame de Entity Profile, listas pendientes generales, **Suppliers**, **MULTI** (M2b).
  Con ancla o pendiente vigente, una propuesta MULTI recibe `INFORM UNSUPPORTED_PROPOSAL:MULTI_REQUEST` (§7.1 fila 4b).
- **Relativos no temporales** («bájalo», «el doble»): `CLARIFY` (G-FU-15, M2b).
- **Relativos temporales complejos** (ventana relativa previa, varios referentes): `CLARIFY` (G-FU-08, E-TIME).
- **Composición GEO↔BUYER compleja** (territorio contenido/contenedor, comprador nacional): `CLARIFY` (G-FU-07,
  E-GEO/E-BUYER).
- **Álgebra de conjuntos por ranura** (unión de valores, negación sobre varios positivos): `CLARIFY` (G-FU-09, M2b).
- **Detalles internos de SMMLV** (catálogo, eco y procedencia de la conversión): E-VALUE (G-FU-06). M2a sólo conserva
  la procedencia (§15.5).
- **Paginación avanzada** (ancla dual, «otros diez más»): `CLARIFY` (G-FU-05, protocolo/M2b).
- **Deshacer** («volver atrás»): no decidido (G-FU-20).
- Un **lenguaje de workflows**: el parche es un conjunto cerrado de operaciones sobre ranuras, no un programa.
- Cualquier camino `AcceptedInterpretation → parsed_params → FollowupEngine/QueryRouter` (§3.7, INV-M2A-04).

---

## 2. Vocabulario base: la IR v2 integrada en `main`

M2a no inventa vocabulario semántico. Usa el de `main`:

| Concepto | Fuente en `main` | Valores |
|---|---|---|
| Dimensión | `app/core/semantic_dimensions.py` `SemanticDimension` | TOPIC, BUYER, ACTOR, GEOGRAPHY, TIME, VALUE, STATE, MODALITY, CONTRACT_TYPE_OR_NATURE, ORDER, CARDINALITY (+ UNSUPPORTED, RETRIEVAL_INTENT, CONTROL_LANGUAGE, que no son ranuras) |
| Operador | `app/core/foundation_operator_table.py` `OPERATORS` | tabla cerrada por dimensión |
| Valor ejecutable | `app/core/foundation_authority_values.py` `ExecutableValue` | `topic@1`, `buyer@1`, `actor@1`, `geo@1`, `time@1`, `money@1`, `state@1`, `modality@1`, `contract_type@1`, `order@1`, `cardinality@2` |
| Sujeto (universo) | `app/core/foundation_operations.py` `Subject` | `processes`, `contracts`, `unspecified` (+ `universe_kind` ∈ {records, opportunities} como condición `REQUESTED_UNIVERSE`) |
| Operación | `Operation` | SEARCH, FOLLOWUP_DELTA, PROTOCOL, … (sólo SEARCH ejecuta) |
| Tope top-N | `MAX_TOP_N = 100`; `cardinality@2(n, basis, n_requested)` con `n == min(n_requested, 100)` | P8 / ADR-103 D9 |
| Digest canónico | `foundation_projection._digest` | `json.dumps(sort_keys=True, ensure_ascii=False, allow_nan=False)` → UTF-8 → sha256 hex |

**Correspondencia con el Gold 2.1** (para el harness; el runtime nunca lee `value_hint`):

| Gold 2.1 | Contrato / IR v2 |
|---|---|
| `universe = CONTRACTS / PROCESSES` | una petición con `Subject.contracts` / `Subject.processes` |
| `universe = BOTH` (ancla dual) | dos peticiones (`processes`, `contracts`) con las mismas condiciones (`SHARES_SCOPE`) |
| `universe = UNSPECIFIED` + `product_view` | `Subject.unspecified` + `HostViewDecision` (ola F) |
| `order`, `cardinality` | condiciones de dimensión ORDER y CARDINALITY (átomos del AND de primer nivel) |
| `VALUE.lower` / `VALUE.upper` | condición VALUE cuyo `money@1.comparator` ∈ {`>`, `>=`} / {`<`, `<=`} (la negación ya está absorbida en el comparador) |
| condición `SOURCE` | `source_evidence` de la petición (no es ranura) |
| `unit` de VALUE | `Provenance.unit` (el tipo `money@1` sólo lleva `amount_cop`) |
| op `SET_UNIVERSE` | `REPLACE_UNIVERSE` (§5) |
| op `ANSWER` | respuesta a la pendiente: selecciona o completa un `CandidatePatch` (§8) |
| op `CANCEL_PENDING` | control de la pendiente (§17) |
| `ops = []` en `NEW_SEARCH` | aceptación fresca; si había pendiente, el registro lleva `control = CANCEL_PENDING` (§17.2) |

---

## 3. Tipos persistidos

Todos inmutables (`frozen`), serializables con `to_dict()` y releídos por un lector **estricto** (clave desconocida,
esquema desconocido o digest incorrecto ⇒ registro inválido). Esquemas versionados: `m2a-record/1`,
`m2a-accepted/1`, `m2a-patch/1`, `m2a-pending/1`, `m2a-resultset/1`.

### 3.1 `AcceptedTurnRecord` (`m2a-record/1`) — una línea por turno Foundation

```text
AcceptedTurnRecord:
  schema: "m2a-record/1"
  trace_id; chat_id; turn_id; created_at_utc; reference_instant   # instante inyectado; nunca se recalcula
  profile; prompt_sha256
  scope: TurnScope                                 # §7.1, cerrado
  resolution: TurnResolution                       # §5.4, cerrado
  control: CANCEL_PENDING | None                   # §17; explícito, nunca inferido de una lista vacía
  base_trace_id: str | None                        # registro cuya efectiva fue la base (ancla o marco pendiente)
  patch: SemanticPatch | None                      # §5; sólo con resolution ∈ {DELTA, PENDING_ANSWER, PROTOCOL_EVENT}
  effective: AcceptedInterpretation | None         # §4; None ⇔ el turno no llegó a READY
  semantic_ref: str | None                         # = interpretation_hash(effective) si effective existe
  execution: ExecutionReceipt | None               # accepted, executed, source_ok, plan_hashes
  result_set_ref: ResultSetRef | None              # §9; sólo si la fuente respondió
  pending: PendingClarification | None             # §8; la pendiente que deja ESTE turno
  echo: tuple[EchoItem, ...]                       # §11
  anchor_trace_id: str | None                      # ancla vigente que deja este turno (§10.2)
  record_sha256                                    # digest del registro sin este campo
```

### 3.2 `AcceptedInterpretation` (`m2a-accepted/1`)

```text
AcceptedInterpretation:
  schema: "m2a-accepted/1"
  requests: tuple[AcceptedRequest, ...]            # 1, o 2 con ancla dual (processes + contracts, SHARES_SCOPE)
  conditions: tuple[AcceptedCondition, ...]        # conjunción de primer nivel (+ grupos OR/NOT no direccionables)
  logic: ConditionExpression | None                # None = AND de todas; si hay OR/NOT, la expresión de la IR v2
  policies: tuple[HostPolicy, ...]                 # HostPolicy de la IR v2 (top-N, orden por defecto, año por reloj…)
  base_trace_id: str | None                        # None en un primer turno o una búsqueda nueva
  root_trace_id: str                               # primer turno de la cadena; informativo (trazas), no habilita ningún comportamiento

AcceptedRequest:
  request_id; scope_id
  subject: Subject                                 # processes | contracts | unspecified
  universe_kind: records | opportunities | None    # None ⇔ subject = unspecified
  universe_origin: ConditionOrigin | None          # None ⇔ subject = unspecified
  view: HostViewDecision | None                    # obligatorio ⇔ subject = unspecified (tipo de la ola F)
  dataset: str                                     # el que proyecta la vista o el sujeto

AcceptedCondition:
  condition_id; request_ids
  slot: Slot                                       # §4.2, derivado del valor tipado
  dimension: SemanticDimension; operator; polarity; level: GeoLevel | None
  value: tuple[str, ...]                           # ExecutableValue.encode(): la única fuente del valor
  authority; receipt_rules: tuple[str, ...]        # reglas de recibo con efecto en el host (ADR-103 §4.3)
  policies: tuple[str, ...]
  origin: ConditionOrigin
  provenance: Provenance

ConditionOrigin:  kind: SPAN | OPTION_EVENT | VIEW_ACTION | HOST_POLICY
                  trace_id; raw_sha256 (SPAN); span (start, end) (SPAN); event_id (eventos)

Provenance:       # hechos cerrados; nunca texto libre
  unit: PESOS | MILES | MILLONES | MIL_MILLONES | BILLONES | SMMLV | None      # sólo VALUE
  source_amount: str | None                        # cantidad en la unidad dicha (p. ej. SMMLV), para recalcular
  unit_source: STATED | SIBLING_IN_RANGE | PREVIOUS_ACCEPTED_VALUE | None
  year: int | None                                 # vigencia usada por una conversión SMMLV
  year_source: STATED | CLOCK_INFERRED | RELATIVE_TO_CLOCK | PREVIOUS_ACCEPTED_TIME | None
  buyer_scope: <código de territorio> | NATIONAL | None     # requisito E-BUYER (receipt buyer.territory.*/buyer.national)
  order_direction_source: STATED | DEFAULTED | None
  inherited_from: trace_id | None
```

### 3.3 `SemanticPatch` (`m2a-patch/1`) — §5

### 3.4 `PendingClarification` (`m2a-pending/1`) — §8

### 3.5 `ResultSetRef` (`m2a-resultset/1`) — §9

### 3.6 `EchoItem`

```text
EchoItem: echo_class (lista cerrada §11); slot | None; condition_ids; display_key   # display sale del valor tipado
```

### 3.7 Lo que NO se persiste ni se crea

- **`Turn.parsed_params` sigue siendo `{}`** para todo turno Foundation (barrera para lectores legacy; ADR-103 D7).
- No existe ninguna función, adaptador ni campo que traduzca `AcceptedInterpretation` o `SemanticPatch` a params
  legacy, a `QueryFrame`, a `ConversationContext` ni a una entrada de `FollowupEngine`/`QueryRouter`.
- Nada del almacén va a git, a la traza ni al ledger (el ledger sólo lleva conteos y digests).

### 3.8 Almacén

- Un archivo por chat, append-only, junto al historial (`<CONVERSATION_STORE_DIR>/<chat>.accepted.jsonl`), escrito
  antes que el `Turn` y bajo el mismo `conversation_lock`.
- Lector estricto: cola ilegible ⇒ continuidad inválida; esquema desconocido ⇒ ancla inválida (fail-closed:
  `ANCHOR_UNREADABLE`).
- Retención = la del historial: borrar el historial borra el almacén (ver plan M2A-2: el único punto de enganche está
  en el cierre legacy y exige regenerar el manifiesto con motivo declarado).
- Un registro sólo es base si su `trace_id` sigue presente en el historial del chat.
- Contiene datos personales (citas, `ActorValue`): misma política que el historial (ADR-060).

---

## 4. `AcceptedInterpretation`

### 4.1 Construcción

- **Primer turno o búsqueda nueva:** `from_acceptance(InterpretationAcceptance READY, trace_id, receipts)` es el
  único constructor. Toma los valores **ya resueltos** por las autoridades (`resolved_ids` = `encode()`), sus reglas
  de recibo y su procedencia. No re-resuelve nada.
- **Delta, respuesta o evento:** `compose(base, patch) → AcceptedInterpretation | DeltaRefusal` (§6). Es el único
  constructor de una efectiva con `base_trace_id ≠ None`.
- La proyección (SoQL) recibe siempre una `AcceptedInterpretation`; nunca una propuesta ni params.

### 4.2 Ranuras

Una **ranura** es la unidad direccionable de un parche:

| Ranura | Contenido |
|---|---|
| `(TOPIC, POSITIVE)`, `(TOPIC, NEGATIVE)`, ídem BUYER, ACTOR, GEOGRAPHY, TIME, STATE, MODALITY, CONTRACT_TYPE_OR_NATURE | condiciones de esa dimensión y polaridad |
| `VALUE.lower` | la condición VALUE con `comparator ∈ {>, >=}` (como mucho una) |
| `VALUE.upper` | la condición VALUE con `comparator ∈ {<, <=}` (como mucho una) |
| `ORDER` | la condición ORDER (como mucho una por petición) |
| `CARDINALITY` | la condición CARDINALITY (exige ORDER) |
| `UNIVERSE` | `subject` + `universe_kind` + vista de cada petición |

El lado de VALUE se deriva del **comparador del valor tipado**, nunca del texto. Sólo son direccionables los átomos de
la conjunción de primer nivel: un objetivo dentro de un grupo OR/NOT ⇒ `CLARIFY TARGET_NOT_ADDRESSABLE`.

### 4.3 Identidad

`interpretation_hash(e) = _digest(e.to_dict())`. Mismo `base` + mismo `patch` ⇒ mismo `interpretation_hash`
(composición determinista). `semantic_ref` es ese hash (§9).

---

## 5. `SemanticPatch`

### 5.1 Forma

```text
SemanticPatch:
  schema: "m2a-patch/1"
  ops: tuple[PatchOp, ...]                         # conjunto atómico; puede tocar varias ranuras
  origin_kind: TEXT | EVENT

PatchOp:
  op: SET | CLEAR | KEEP | SET_ORDER | REPLACE_UNIVERSE
  slot: Slot                                       # §4.2 (CLEAR acepta la dimensión entera; VALUE = los dos lados)
  condition: AcceptedCondition | None             # SET / SET_ORDER: la condición nueva ya resuelta por su autoridad
  universe: (subject, universe_kind) | None        # REPLACE_UNIVERSE
  origin: ConditionOrigin                          # SPAN del turno actual, u OPTION_EVENT / VIEW_ACTION
```

### 5.2 Vocabulario cerrado (cinco operaciones semánticas)

| Op | Efecto | Exige |
|---|---|---|
| `SET(slot, c)` | pone `c` en la ranura; sustituye lo que hubiera **en esa ranura** (ADD si estaba vacía, REPLACE si no; lo deriva el host) | `c` resuelta por la autoridad de su dimensión sobre un span del turno actual (o un evento) |
| `CLEAR(slot \| dimensión)` | quita la ranura o la dimensión entera | una cita del turno o un evento «quitar filtro» |
| `KEEP(slot)` | no cambia nada; afirma que la ranura existe y da dueño a la cita deíctica («con el mismo comprador») | la ranura existe en la base |
| `SET_ORDER(c_order)` | sustituye ORDER; el cursor vuelve a cero; la CARDINALITY se conserva salvo que el parche la toque (F10) | `order@1` |
| `REPLACE_UNIVERSE(subject, kind)` | cambia sujeto y clase de universo; con ancla dual se queda con el nombrado; el universo pasa a ser dicho (vista no derivada) | evidencia de universo con dueño, o evento de vista |

`CANCEL_PENDING` **no** es una operación semántica: es control de la pendiente (§17) y vive en
`AcceptedTurnRecord.control`. La respuesta a una pendiente no es una operación nueva: es un `SemanticPatch`
seleccionado o completado (§8). No hay más operaciones; un parche que necesite otra cosa no es M2a.

### 5.3 Origen del parche

- **Texto:** las operaciones salen de la propuesta del turno (spans con dueño, D2/INV-OWN) y de las directivas del
  wire 5.1 (`CLEAR`, `KEEP`, `CANCEL_PENDING`, ADR-103 §4.4). El productor ve una tarjeta de estado **sin valores**
  (sólo enumerados); los valores vienen siempre de las autoridades.
- **Evento:** botón u opción (`[Mayor valor]`, «Quitar: X», `[Contratos]`, opción de una pendiente): el host construye
  el parche sin LLM y sin releer texto.

### 5.4 `TurnResolution` (cerrado)

| Resolución | Qué es | Llama a la fuente |
|---|---|---|
| `NEW_SEARCH` | aceptación fresca (§7); `base_trace_id = None` | sí |
| `DELTA` | `compose(ancla, patch)` | sí si READY |
| `PENDING_ANSWER` | `compose(marco, candidate.patch)` (§8) | sí si READY |
| `PROTOCOL_PAGE` | misma efectiva, siguiente página del `result_set_ref` | sí (misma `plan_hash`) |
| `PROTOCOL_EVENT` | `compose(ancla, patch de evento)` | sí si READY |
| `CLARIFY` / `REASK` | deja (o mantiene) una `PendingClarification` | no |
| `INFORM` | explica lo que M2a no hace; el ancla no cambia | no |
| `HANDOFF_LEGACY` | sólo con ancla de dueño legacy; fuera de M2a | — |

---

## 6. Atomicidad y composición

`compose(base, patch) → AcceptedInterpretation | DeltaRefusal(code)`:

1. **Todo o nada.** Si una operación no puede acreditarse (autoridad no RESOLVED, ranura inexistente para `KEEP`,
   objetivo no direccionable), el parche entero se rechaza y la base no cambia. Nunca hay efectiva parcial.
2. **Orden fijo:** `CLEAR` → `SET`/`SET_ORDER`/`REPLACE_UNIVERSE` → comprobación de `KEEP` → causas de rechazo
   (paso 3b) → reglas del host (paso 4). Dos `SET` sobre la misma ranura, o `KEEP` + `CLEAR` de la misma ranura, o
   `KEEP` + `SET`/`SET_ORDER`/`REPLACE_UNIVERSE` de la misma ranura **aunque los valores coincidan** (R3b, U22) ⇒
   `DELTA_SLOT_CONFLICT`. Un `CLEAR` de la dimensión entera cubre cada una de sus ranuras. El desenlace no depende del
   orden de las operaciones dentro del parche.
3. **Arrastre por identidad (F1).** Lo que ninguna operación toca pasa **byte-idéntico** (valor, autoridad, políticas,
   origen, procedencia). Nunca se re-resuelve una condición arrastrada. Única excepción (R3, P-1): `REPLACE_UNIVERSE`
   con ancla dual reduce los `request_ids` de cada condición arrastrada a la petición que queda (§5.2); nada más cambia.
3b. **Causas de rechazo, antes de las reglas del host (R3b, U20).** Primero se decide si la transformación tiene
   sentido; después, las reglas del estado conversacional (paso 4). Las causas son las de `DeltaRefusal` y tienen esta
   **precedencia cerrada**, de mayor a menor:

   | Precedencia | Causa (`DeltaRefusal`) | Cuándo |
   |---|---|---|
   | 1 | `TARGET_NOT_ADDRESSABLE` | una operación apunta a un átomo dentro de un grupo OR/NOT (§4.2) |
   | 2 | `DELTA_SLOT_CONFLICT` | dos `SET`, dos `SET_ORDER` o dos `REPLACE_UNIVERSE` sobre la misma ranura; `KEEP` + `CLEAR`, o `KEEP` + `SET`/`SET_ORDER`/`REPLACE_UNIVERSE`, de la misma ranura (paso 2) |
   | 3 | `KEEP_ON_EMPTY_SLOT` | `KEEP` de una ranura vacía en la base |
   | 4 | `OP_NOT_ACCREDITED:<DIM>` | una operación cuya autoridad no resolvió, una política sin dueño (§6.6), la rama fail-closed de F16 (§6.5) o la de A8.1 con unidades no comparables (§15.2) |
   | 5 | `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` | el estado compuesto deja una CARDINALITY sin ORDER (§6.7) |

   `BASE_INVALID` no entra en la lista: lo decide §7.1 fila 1 antes de componer (`CLARIFY NO_ANCHOR` /
   `ANCHOR_UNREADABLE`, con una pendiente siempre posible, §8.4).
   Una causa es una ranura en conflicto, un `KEEP` sobre una ranura vacía, una operación no direccionable o no
   acreditada, o el estado CARDINALITY sin ORDER. Dos operaciones no acreditadas son dos causas. Entre causas de la
   misma precedencia decide el orden de ranuras de §4.2.
   - **Ninguna causa:** sigue el paso 4.
   - **Una causa:** `CLARIFY` con su código de A.3 (tabla de abajo) y su pendiente, sólo si §8.4 la permite y el ancla
     no está vencida. Si no, `INFORM` con el mismo código, sin pendiente.
   - **Dos o más causas:** `INFORM` con el código de mayor precedencia, sin pendiente, 0 llamadas y el ancla intacta.
     El mensaje nombra **todas** las causas en el orden de la lista; se derivan de la base y del parche guardados, y el
     registro guarda sólo el código de mayor precedencia.
   - **Con una pendiente vigente que el turno no cancela** (§8.3): `REASK ANSWER_UNRESOLVED` (fila 7), con la pendiente
     intacta. Si el turno la cancela (fila 2), las causas se deciden sobre el marco con las mismas reglas.
4. **Reglas del host tras componer**, con esta precedencia (la primera que dispare fija la disposición):

| # | Regla | Resultado |
|---|---|---|
| H1 | Vigencia (§10): ancla vencida | `CLARIFY CONTEXT_EXPIRED_CONFIRM` **sólo si** su candidato «continuar» (el parche sobre el ancla vencida) compone sin causas (paso 3b) y ejecuta: no dispara ninguna de H3–H12. Si dispara una, `INFORM` con su código (H12 ya es `INFORM`); las causas ya dieron `INFORM` en el paso 3b. Si ejecutar depende de una fila `COMPOSICION` de §19, decide esa dependencia. Sobre un ancla vencida no nace ninguna otra pendiente (R3b, U20) |
| H2 | Pendiente (§8) | `REASK` según F24/F25/F29 |
| H3 | SMMLV sin vigencia única (§15.5) | `CLARIFY SMMLV_YEAR_AMBIGUOUS` |
| H4 | Relativo no temporal (F15) | `CLARIFY RELATIVE_TO_PREVIOUS_VALUE` (frontera M2b) |
| H5 | «con X» tras ACTOR, X comprador o lugar (F12) | `CLARIFY BUYER_OR_PLACE_AFTER_ACTOR` con 2 `candidate_patches` |
| H6 | Territorio nuevo con comprador territorial de otro territorio arrastrado (F30) | `CLARIFY TERRITORY_VS_CARRIED_BUYER` con 2 `candidate_patches` |
| H7 | Región fuera de la lista cerrada (F13) | `CLARIFY REGION_NOT_IN_LIST` |
| H8 | Estado o modalidad inexistente en el universo resultante (F27) | `CLARIFY STATE_NOT_IN_UNIVERSE` ofreciendo el universo |
| H9 | Unidad no heredable (§15.4) | `CLARIFY UNIT_AMBIGUOUS` |
| H10 | Mes sin año con TIME previo plurianual o relativo (F7) | `CLARIFY MONTH_YEAR_AMBIGUOUS` |
| H11 | Búsqueda vacía tras el delta (F19) | `CLARIFY EMPTY_SEARCH_AFTER_DELTA` |
| H12 | `CLEAR` de ranura vacía como única operación (F18) | `INFORM CLEAR_ON_EMPTY_SLOT`, 0 llamadas, ancla intacta |
| — | si no | ejecutar, con las sustituciones declaradas de §6.5 y su eco |

5. **Sustituciones declaradas** (las únicas desapariciones sin `CLEAR` citado; cada una con su eco):
   - **A8.1** (§15.2): la cota opuesta cae cuando conservarla dejaría el intervalo vacío o degenerado →
     `VALUE_RANGE_REPLACED`.
   - **F16**: `SET` negativo cuyo valor canónico es el **único** positivo vigente de la ranura invierte la polaridad →
     `POLARITY_INVERTED`. Con varios positivos o valor contenido, el `SET` no se acredita en M2a (G-FU-09, M2b):
     `OP_NOT_ACCREDITED:<DIM>` ⇒ `CLARIFY UNRESOLVED_VALUE:<DIM>`, con la dimensión de la ranura (R3-4). F16 sólo
     invierte un positivo **arrastrado** de la base; un positivo creado por el mismo parche no cuenta. Un parche que
     introduce a la vez +X y −X (mismo valor canónico, misma dimensión) no lo resuelve F16: cae en la misma rama
     fail-closed y con el mismo código (R3, P-3).
   - **F3/F22**: `SET` positivo de TOPIC sobre TOPIC positivo distinto lo sustituye → `TOPIC_REPLACED`.
   - **F9**: con vista derivada, la vista se recalcula sobre la efectiva compuesta → `VIEW_REDERIVED` si cambia.
6. **Políticas de petición** (ACTOR×STATE, ACTOR×TIME, `universe-c5@1`, top-N, dirección de orden por defecto) se
   re-deciden sobre la efectiva compuesta; H7 de ADR-101 no se re-ejecuta sobre lo arrastrado.
7. **Nunca se ejecuta** un intervalo VALUE vacío o degenerado generado por un follow-up con la cota opuesta arrastrada
   (A8.1 lo sustituye o, con unidades no comparables, aclara), una efectiva con dos cotas en el mismo lado, ni una
   CARDINALITY sin ORDER.

   **CARDINALITY sin ORDER (R3-1).** La regla se decide sobre el **estado** resultante, no sobre la operación: si la
   efectiva compuesta dejaría en alguna petición una CARDINALITY vigente sin ORDER, el parche se rechaza con
   `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` y la base no cambia. Nunca se borra la CARDINALITY sin cita (INV-M2A-16) ni
   se inventa un ORDER. `CLEAR(ORDER)` + `CLEAR(CARDINALITY)` en el mismo parche, o `CLEAR(ORDER)` + `SET_ORDER` (F10),
   no dejan ese estado y se componen.
   - Vía alcanzable hoy: `CLEAR(ORDER)` con la CARDINALITY arrastrada y sin `CLEAR(CARDINALITY)` en el parche. Su
     pendiente (§8.1, D-1 opción 1) lleva:
     - `ambiguous_span` = la cita que retira el ORDER (`None` si viene de un evento);
     - `frame` = la base con el resto del parche compuesto;
     - `candidate_patches`, resueltos al preguntar (§8.2):
       - **C1** «quitar orden y límite»: `CLEAR(ORDER)` + `CLEAR(CARDINALITY)`;
       - **un `SET_ORDER` por cada criterio de orden alternativo**: cada clave de orden de la lista cerrada que la
         autoridad ORDER ofrece para el dataset de la petición, **salvo la vigente**. La dirección la da la autoridad
         (con `order.direction_defaulted` si aplica). La CARDINALITY se conserva (F10);
     - `answer_slots = {ORDER, CARDINALITY}` (⊇ las ranuras de los candidatos).

     Las respuestas siguen §8.3. Un criterio escrito («por fecha») selecciona su candidato por identidad semántica
     (fila 4, exactamente uno; «Selección», §8.3). «Quítales también el límite» selecciona C1 porque el **productor**
     emite las operaciones completas de C1, `CLEAR(ORDER)` + `CLEAR(CARDINALITY)`. El host nunca lee la palabra
     «también» (sin léxico en el host, INV-M2A-24): sólo compara operaciones. «Déjalo como estaba» es
     `CANCEL_PENDING` (§17): base intacta. Una respuesta que no selecciona ninguno es la fila 7
     (`REASK ANSWER_UNRESOLVED`); nunca se compone sobre el ORDER de la base.

     **«Dame 20» (R3b, P1-2).** Mientras IR v2 rechace un `FOLLOWUP_DELTA` que sólo trae CARDINALITY
     (`interpretation_contract.py:438-439`), «dame 20» no llega a §8.3. La propuesta es inválida y el turno termina en
     `INFORM UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL` por §7.1 fila 4b, con la pendiente intacta. Si M2A-5 cambia el wire,
     aplican las reglas normales.

     **Provisional fail-closed mientras no exista la autoridad ORDER** (E-ORDER, §19): la pendiente lleva **sólo C1**
     y la cancelación; no hay candidatos `SET_ORDER`.
   - Por cualquier otra vía (hoy inalcanzable: la propuesta IR v2 rechaza una CARDINALITY sin ORDER, salvo en
     `RANK_SUPPLIERS`), la pendiente sigue §8.2.

Códigos de rechazo (`DeltaRefusal`, cerrados): `DELTA_SLOT_CONFLICT`, `TARGET_NOT_ADDRESSABLE`, `KEEP_ON_EMPTY_SLOT`,
`OP_NOT_ACCREDITED:<DIM>`, `BASE_INVALID`, `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`. Un rechazo nunca ejecuta.

- Si había una pendiente vigente que el turno no cancela, produce `REASK ANSWER_UNRESOLVED` (§8.3 fila 7). Es el
  alcance decidido en R3-2 («`REASK` si había pendiente»); la redacción «filas 1 y 4» de B1 lo estrechaba sin decisión
  y se retira (R3b).
- Si no, sigue el paso 3b, con este código de A.3 (R3-2):

| `DeltaRefusal` | `CLARIFY` (A.3) |
|---|---|
| `OP_NOT_ACCREDITED:<DIM>` | `UNRESOLVED_VALUE:<DIM>` (el código de la autoridad; `<DIM>` según A.3); `VALUE_WITHOUT_COMPARATOR` cuando la autoridad VALUE lee una cota sin comparador |
| `BASE_INVALID` | `NO_ANCHOR` o `ANCHOR_UNREADABLE` (§3.8, §7.1 fila 1) |
| cualquier otro (estructural, sin autoridad) | el mismo código: `DELTA_SLOT_CONFLICT`, `TARGET_NOT_ADDRESSABLE`, `KEEP_ON_EMPTY_SLOT`, `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` |

**`VALUE_WITHOUT_COMPARATOR` (R3b-2, U25).** Es el código de la autoridad VALUE para una cota sin comparador (A.6,
`value_without_comparator`) y pasa tal cual por esta tabla: `OP_NOT_ACCREDITED:VALUE` ⇒ `CLARIFY
VALUE_WITHOUT_COMPARATOR`, o `INFORM` con el mismo código cuando el paso 3b o §8.4 lo piden.

---

## 7. Búsqueda nueva frente a delta (A9 + A9.1, estructural)

### 7.1 Alcance del turno (`TurnScope`, cerrado; el primero que aplique)

| # | `TurnScope` | Cuándo | Resolución |
|---|---|---|---|
| 1 | `OUT_NO_ANCHOR` | no hay ancla legible ni pendiente, y P no forma por sí sola una SEARCH (ver «Admisible sola», §7.2) | `CLARIFY NO_ANCHOR` (F28) o `ANCHOR_UNREADABLE` |
| 2 | `OUT_ANCHOR_NOT_SEARCH` | el último turno es de un dueño legacy | `HANDOFF_LEGACY` (fuera de M2a) |
| 3 | `IN_PROTOCOL` | evento, o paginación sobre un resultado Foundation | `PROTOCOL_PAGE` / `PROTOCOL_EVENT` / `PENDING_ANSWER` por evento |
| 4 | `OUT_OPERATION_CHANGE` | la propuesta trae un job cuya operación no es SEARCH ni FOLLOWUP_DELTA | `INFORM OPERATION_CHANGE:<op>` |
| 4b | `OUT_UNSUPPORTED_PROPOSAL` | hay ancla o pendiente vigente y P trae, por sí misma, un motivo de no admisión de la lista cerrada de abajo (R3, D-6) | `INFORM UNSUPPORTED_PROPOSAL:<motivo>` |
| 5 | `OUT_REFERENT_EXTENSIONAL` | el referente es una fila o un subconjunto mostrado por posición o identidad, según la señal estructural extensional del wire 5.1 (se define en M2A-5; G-FU-14 diferido) | `INFORM REFERENT_EXTENSIONAL` |
| 6 | `OUT_RELATIVE_TO_PREVIOUS` | condición relativa a un valor previo no temporal (F15) | `CLARIFY RELATIVE_TO_PREVIOUS_VALUE` |
| 7 | `IN_NEW_SEARCH` | §7.2 dice búsqueda nueva | `NEW_SEARCH` |
| 8 | `IN` | lo demás: delta o respuesta | §6, §8 |

Un alcance `OUT_*` o `INFORM` hace **0 llamadas** a la fuente y no cambia el ancla.

**Fila 4b: propuesta no admitida (R3, D-6).** La regla es genérica: no hay una fila por capacidad. Un motivo es una
propiedad de P sola, que ninguna composición repara. No están aquí los motivos que otra fila o regla ya decide:

- operación distinta de SEARCH o FOLLOWUP_DELTA: fila 4;
- etiquetas reservadas de A.6: `relative_to_previous` (fila 6), `no_selection` con pendiente vigente (§8.3 fila 6) y
  `value_without_comparator` (§6: `OP_NOT_ACCREDITED:VALUE` ⇒ `CLARIFY VALUE_WITHOUT_COMPARATOR`);
- una condición que su autoridad no acredita (p. ej. una familia de modalidad sin política): §6.1,
  `OP_NOT_ACCREDITED:<DIM>`;
- un universo no dicho: en un delta lo aporta el ancla;
- la caída del proveedor: §13.

La disposición es `INFORM`: 0 llamadas, sin delta parcial, sin legacy, y ancla y pendiente intactas (no se consumen
ni se cancelan: §10.2, §17). Un ancla vencida también cuenta: confirmar no repara un motivo de esta fila (R3b).

| Motivo (cerrado) | P… | Dueño |
|---|---|---|
| `MULTI_REQUEST` | trae varias peticiones que no forman un ancla dual admitida (`processes` + `contracts` con `SHARES_SCOPE`), o varios jobs | M2b |
| `UNSUPPORTED_CONDITION` | trae una condición `UNSUPPORTED` cuya etiqueta no está en A.6, o `no_selection` sin pendiente vigente (no elige entre nada; U25) | extensión o versionado del lenguaje (IR; §20.4) |
| `UNSUPPORTED_SOURCE` | atribuye la consulta a una fuente distinta de SECOP II | contrato M1 (fuente única; definitivo) |
| `INVALID_PROPOSAL` | no pasa el contrato del wire (p. ej. una cita que no liga con el texto del turno) | productor (M2A-5) |
| `NO_ACTION` | no trae petición, condición ni directiva; con pendiente vigente no aplica esta fila sino §8.3 fila 8 (F29) | contrato M2a (definitivo) |

### 7.2 La regla

Definiciones, todas sobre la **forma** de la interpretación, nunca sobre palabras:

- **P** = la propuesta del turno actual: sólo sus propios spans con dueño (los valores arrastrados no están en la
  propuesta; viven en el almacén). P es, por construcción, la lectura **sin historial** del turno.
- **Admisible sola(P)** = P, **sin base**, forma una SEARCH del contrato M1 (A9): un job SEARCH con petición válida
  (universo explícito, o `unspecified` + Product View) y al menos una condición propia. **No exige READY**: la admisión
  puede quedar en `READY` (se ejecuta) o en `NEEDS_CLARIFICATION` por el valor de una dimensión (se pregunta, como en
  M1). Admisible sola ≠ ejecutable.
- **Continuación(P)** = P trae evidencia estructural de dependencia: job `FOLLOWUP_DELTA`, `referent_evidence`
  (incluye «los que…», «esos»), directiva `KEEP` o `CLEAR`, o condición que depende de un valor previo
  (`depends_on_prior_value`). Es evidencia que el productor emite como span con dueño; **el host no la detecta con
  léxico**. La señal que separa un referente **extensional** (una fila, «el segundo», por posición o identidad) de uno
  **intensional** («los que…», re-consulta F4) es G-FU-14, diferida a M2A-5 (wire 5.1); la ejecución sobre filas es
  M2b. **No hay regla provisional que trate un referente como intensional por defecto:** la señal es gate de M2A-5 y
  precondición de la activación de M2A-6 (sin ella, ningún turno con `referent_evidence` se ejecuta en runtime).
- **Nuevo(P, A)** = P trae una condición TOPIC o ACTOR (de cualquier polaridad) cuyo par (polaridad, valor canónico)
  no está en el ancla A, o un job de operación nueva (que el paso 4 de §7.1 ya envió a `INFORM`). Es la lista de A9.1
  leída sin filtro de polaridad.
- **Marco(P)** = P trae evidencia de universo con dueño, o evidencia de operación del job (verbo de petición citado),
  o **dos o más** condiciones propias.

Con un ancla SEARCH aceptada **y sin pendiente** (si el ancla está vencida, la clasificación es la misma y §10 decide
después si un `DELTA` se confirma):

| Paso | Condición | Resultado | Origen |
|---|---|---|---|
| a | Continuación(P) | `DELTA` | A9 («gana la marca») |
| b | Nuevo(P, A) = ∅ — P sólo aporta condiciones o modificadores (TIME, VALUE, GEOGRAPHY, BUYER, STATE, MODALITY, CONTRACT_TYPE_OR_NATURE, ORDER, CARDINALITY, universo) | `DELTA`, **aunque Admisible sola(P)** | **A9.1** |
| c | Nuevo(P, A) ≠ ∅ **y** Marco(P) **y** Admisible sola(P) | `NEW_SEARCH` + eco `CONTEXT_RESET`; si la admisión de P queda en `NEEDS_CLARIFICATION`, búsqueda nueva **que pregunta**: deja una `PendingClarification` con `frame` = lo resuelto de P y `base_trace_id = None`, y el ancla anterior se conserva hasta que la pendiente se resuelva o se cancele (§10.2) | A9 |
| d | Nuevo(P, A) ≠ ∅ sin Marco(P) (una sola condición de contenido) | `DELTA` (el TOPIC sustituye, F3) | F22, A4 |

Con una **pendiente** vigente rige §8 (A7): A9.1 no se aplica al marco pendiente; una P **sin Continuación(P)**,
con Marco(P) y admisible sola, que no sea una respuesta compatible (§8.3 fila 3), cancela la pendiente y es búsqueda
nueva (`control = CANCEL_PENDING`); si esa búsqueda nueva también pregunta, su pendiente sustituye a la cancelada. Con
Continuación(P) nunca es búsqueda nueva: «gana la marca» (A9) también ante una pendiente.

Sin ancla ni pendiente: P admisible sola ⇒ primer turno, con la disposición que M1 le dé (`EXECUTE`, o `CLARIFY` con
su código y su pendiente); si P no forma una SEARCH ⇒ `CLARIFY NO_ANCHOR`. M2a no cambia el primer turno (INV-M2A-06).

Suelo del host (ADR-103 D7, se mantiene): con ancla o pendiente vigentes, una P sin ninguna condición propia nunca es
búsqueda nueva. Toda búsqueda nueva con ancla vigente lleva el eco `CONTEXT_RESET` con la acción «aplicar a la búsqueda
anterior».

**Garantía limitada declarada.** El paso (a) depende de que el productor emita la evidencia de continuación. El host
no la sustituye por léxico. Si tras 3 iteraciones del prompt v5.1 sobre DEV el productor no la emite, no hay CUTOVER
(ADR-103 §4.8).

---

## 8. `PendingClarification` y `candidate_patches[]`

### 8.1 Tipo

```text
PendingClarification (m2a-pending/1):
  pending_id; asked_trace_id; created_at_utc; expires_at_utc; asks: 1 | 2
  frame: AcceptedInterpretation | None    # lo ya resuelto del turno que preguntó, SIN lo abierto
                                          # (None sólo si el turno que preguntó no tenía nada resuelto)
  base_trace_id: str | None               # el ancla sobre la que se componía, si la había
  ambiguous_span: (start, end) | None     # span del turno que preguntó; None sólo si la pregunta no nace de un
  ambiguous_raw_sha256: str | None        #   span (p. ej. CONTEXT_EXPIRED_CONFIRM)
  reason: <código cerrado de §A.3>
  candidate_patches: tuple[CandidatePatch, ...]   # 0..n
  answer_slots: frozenset[Slot]           # ranuras que una respuesta abierta puede fijar; ⊇ ranuras de los candidatos

CandidatePatch:
  candidate_id; display_key
  patch: SemanticPatch                    # completo: valores ya resueltos por la autoridad AL PREGUNTAR
```

**Genérico por construcción.** No existen tipos «BUYER o GEO», «TIME o X», «doble ranura» ni similares. Una
ambigüedad de dimensión (F12) son dos `CandidatePatch` (`SET BUYER=X` / `SET GEOGRAPHY=X`); una de territorio (F30)
son dos parches (cambiar de comprador / buscar con ambos); una pregunta por el año son cero candidatos y
`answer_slots = {TIME}`; una confirmación de vigencia (F11) es un candidato con el parche del turno sobre el ancla
vencida («continuar»), y «reset» es `CANCEL_PENDING`. Retirar el ORDER con una CARDINALITY vigente (§6.7, R3-1) da
un candidato «quitar orden y límite» más un `SET_ORDER` por cada criterio de orden alternativo, con
`answer_slots = {ORDER, CARDINALITY}`; «déjalo como estaba» es `CANCEL_PENDING`. Un candidato puede tocar varias
ranuras (el Gold tiene alternativas GEOGRAPHY+TIME).

### 8.2 Creación

Una pendiente nace con **un** span sin resolver y todo lo demás resuelto en `frame`. Su plazo es de 30 minutos desde
su creación (`expires_at_utc = created_at_utc` + 30 min), igual que F11 (R3c, U26). Las opciones se resuelven **al
preguntar**, cada una por la autoridad de su ranura sobre la ventana del span; por eso elegir por evento no necesita
LLM ni releer texto. Un turno que pregunta no consume el ancla (§10.2).

### 8.3 Respuesta (A7, F23, F24, F25, F29) — en este orden

| # | Entrada | Resultado |
|---|---|---|
| 1 | Evento de opción (`pending_id`, `candidate_id`) | `PENDING_ANSWER`: `compose(frame, candidate.patch)`; sin LLM |
| 2 | P trae directiva `CANCEL_PENDING` (rechazo citado, F23) | `control = CANCEL_PENDING`; el resto de P se resuelve como `NEW_SEARCH` si ¬Continuación(P) ∧ Marco(P) ∧ Admisible sola(P), si no como delta **sobre `frame`** (A7) |
| 3 | ¬Continuación(P) ∧ Marco(P) ∧ Admisible sola(P), **salvo** que todas las condiciones de P caigan en `answer_slots` (respuesta compatible, A7: pasa a la fila 4) | `NEW_SEARCH`, `control = CANCEL_PENDING` (A7/A9; la marca de continuación gana, así que una respuesta con anáfora o conector nunca llega aquí); si la nueva búsqueda pregunta, deja su propia pendiente |
| 4 | Todas las condiciones de P caen en `answer_slots` y su valor selecciona **exactamente un** candidato (comparado por valor codificado), o no hay candidatos | `PENDING_ANSWER`: `compose(frame, patch)` |
| 5 | P responde sobre `answer_slots` **y además** edita otra ranura (F25) | `REASK ANSWER_EDITS_OTHER_SLOT`, nombrando la salida; 0 llamadas |
| 6 | P declara no elegir (condición `UNSUPPORTED` con etiqueta cerrada `no_selection`, F24) | `REASK OPTION_NOT_CHOSEN`; el producto no elige |
| 7 | La respuesta no resuelve (autoridad no RESOLVED, o selecciona 0 o ≥2 candidatos) | `REASK ANSWER_UNRESOLVED` |
| 8 | P no toca `answer_slots`, no rechaza y no es búsqueda nueva (F29) | `REASK PENDING_UNANSWERED`; marco y pregunta intactos |

Tras aplicar una respuesta, si el resultado sigue ambiguo ⇒ `REASK` (nunca un híbrido). `asks` llega a 2 y vuelve a
fallar ⇒ la pendiente se cancela (`control = CANCEL_PENDING`) y se dice. Nunca se ejecuta `frame + respuesta + otra
edición`.

**Selección (R3b, U22; R3c, U26).** Con candidatos, la respuesta en lenguaje se lee así. Primero se normaliza P quitando
los `KEEP` de ranuras que existen en `frame` (un `KEEP` es la identidad). Si P toca ranuras fuera de `answer_slots`, es
la fila 5 (o la 8 si no toca ninguna de dentro). Si no, se compara el multiconjunto de sus operaciones, por coincidencia
**exacta** de (op, ranura, valor canónico codificado) o (`REPLACE_UNIVERSE`, destino), con el parche de cada candidato:
exactamente uno coincide ⇒ fila 4, y se compone su parche completo sobre `frame`; ninguno o varios ⇒ fila 7. Nunca se
decide por texto, `display_key`, `candidate_id` ni ids de condición. La fila 1 es un evento que ya nombra el candidato;
un evento de opción cuya pendiente ya no existe (cancelada, vencida o sustituida) ⇒ `INFORM STALE_OPTION`, 0 llamadas,
ancla intacta.

### 8.4 Ley de la pendiente posible (R3b, U20)

Una `PendingClarification` sólo puede existir si se cumplen las dos condiciones:
1. su `frame` (la base más el parche **sin** las operaciones del span abierto) es `None`, porque no había nada
   resuelto, o una `AcceptedInterpretation` válida;
2. cada `candidate_patch` compone válidamente sobre ese `frame`, sin causas del paso 3b de §6;
3. existe **al menos una** respuesta que ejecuta: un candidato, o, sin candidatos, una respuesta sobre `answer_slots`
   que compone sobre `frame` sin causas y sin otra pregunta (R3c, U26). `VALUE_WITHOUT_COMPARATOR` lleva
   `answer_slots = {VALUE.lower, VALUE.upper}`.

Si no se cumplen: `INFORM` con el código de la regla que habría preguntado, 0 llamadas a la fuente, ancla intacta y
**sin pendiente**; el usuario reformula. `REASK` mantiene la pendiente que ya existía y no crea otra.

Con una pendiente vigente no nace otra. Una fuente que preguntaría da `INFORM` con su código y la pendiente intacta,
salvo que §8.3 la cancele antes (filas 2 y 3). Hoy sólo llega ahí §7.1 fila 6. Sobre un ancla vencida sólo puede nacer
`CONTEXT_EXPIRED_CONFIRM` (H1); las demás fuentes dan `INFORM`. Una operación que pregunta o no está acreditada nunca
entra en un `frame`: si quedan dos, el `frame` de la primera no es válido y el turno es `INFORM`.

Las fuentes de pendiente, revisadas contra la ley:

| Fuente | `frame` | Candidatos | ¿Pendiente siempre posible? |
|---|---|---|---|
| §7.1 fila 1 (`NO_ANCHOR`, `ANCHOR_UNREADABLE`) | `None` | ninguno | sí |
| §7.1 fila 6 (`RELATIVE_TO_PREVIOUS_VALUE`) | el ancla con el resto del turno compuesto (§8.2) | ninguno | no: si el resto no compone, con el ancla vencida o con una pendiente vigente ⇒ `INFORM` |
| §6 paso 3b, una causa | base + parche sin las operaciones de la causa | R3-1: C1 (+ `SET_ORDER` con E-ORDER); las demás causas, ninguno | no: con un `frame` inválido (p. ej. `CLEAR(ORDER)` junto a otra causa, con la CARDINALITY arrastrada) ⇒ `INFORM` |
| H1 `CONTEXT_EXPIRED_CONFIRM` | el ancla vencida | «continuar» = el parche del turno | sólo si «continuar» compone y ejecuta (H1); si no, `INFORM` |
| H3–H11 | base + parche sin la operación del span abierto (H11: el span abierto es el turno entero y `frame` = la base) | H5 y H6: dos; las demás, ninguno (H8 ofrece el universo en el mensaje) | no: `frame` inválido o candidato que no compone ⇒ `INFORM` |
| §7.2 (c), búsqueda nueva que pregunta | lo resuelto de P, sin base | los de la autoridad | no: `frame` inválido ⇒ `INFORM` |
| primer turno sin ancla | lo resuelto de P (M1) | los de M1 | la disposición es de M1 (INV-M2A-06); si su pendiente no se puede construir, es dependencia de M2A-1b (§19, `DEP-M1-PENDING-FRAME`) |
| §8.3 `REASK` | la pendiente vigente | los suyos | sí: ya existía |
| §17 cancelación | — | — | no crea pendiente |

La propiedad universal sobre el `compose` real, con permutaciones de las operaciones y `check_transition` del
oráculo, es gate de M2A-3 fase 2. En este contrato la comprueba el gate multi-op de INV-M2A-25 sobre un modelo
simbólico (§18). Barrer las respuestas a todas las pendientes es gate de M2A-4, incluido que una respuesta se compone
sobre `frame` y nunca sobre la base (el modelo simbólico no lo distingue: sus pendientes previas tienen `frame` = la
base). Las propiedades de varios turnos (vencimiento pegajoso, plazo de la pendiente) son gate de M2A-6/8b por
replay (R3c, U26).

---

## 9. Referentes: `semantic_ref` frente a `result_set_ref`

| | `semantic_ref` | `result_set_ref` |
|---|---|---|
| Qué es | la `AcceptedInterpretation` efectiva (su `interpretation_hash`) | el manifiesto de la llamada + el cursor |
| Existe | tras **todo** turno READY, ejecute, dé cero filas o **falle la fuente** | **sólo** si la fuente respondió y materializó el conjunto |
| Lo usan | deltas, re-consultas («los de mayor valor» re-consulta: F4), respuestas, eventos de vista/orden/filtro | paginación («ver más»), identidad de filas (M2b), export de lo mostrado |
| Tipo | `str` (hash) | `ResultSetRef` |

```text
ResultSetRef (m2a-resultset/1):
  trace_id; interpretation_hash
  groups: tuple[GroupRef, ...]
GroupRef: request_id; dataset; plan_hash; order_contract; page_limit; shown_ids; next_offset | None; total; total_quality
```

Reglas:
1. Un turno que necesita filas o página y no hay `result_set_ref` ⇒ `INFORM` (o `CLARIFY` si hay algo que elegir),
   **0 llamadas**. El código de razón propio se fija al implementar el protocolo (G-FU-04); mientras tanto el gate es
   «disposición INFORM y 0 llamadas».
2. Paginar re-proyecta la efectiva guardada **con el `reference_instant` guardado**, exige `plan_hash` idéntico, pide la
   página bajo un permiso del guard y descarta ids ya mostrados. Sin LLM. Ancla dual al paginar ⇒ `CLARIFY` (G-FU-05).
3. Un delta semántico sobre un ancla cuya fuente falló **sí** se compone y se ejecuta (A6).
4. `semantic_ref` nunca se usa para identificar filas; `result_set_ref` nunca se usa para componer un delta.

---

## 10. Vigencia

### 10.1 Plazo

30 minutos desde el **turno anterior del chat**, del tipo que sea, medidos con el instante inyectado (F11). Vencido el
plazo, un turno que §7.2 resolvería como `DELTA` no se ejecuta ni descarta el contexto en silencio: `CLARIFY
CONTEXT_EXPIRED_CONFIRM` con una `PendingClarification` cuyo único candidato es «continuar» (el parche del turno sobre
el ancla vencida, ya resuelto; continuar no vuelve a llamar al LLM) y «reset» como `CANCEL_PENDING`. Un turno que §7.2
resuelve como `NEW_SEARCH` se ejecuta sin preguntar.

No se pregunta «¿continuar?» si «continuar» no puede de verdad reanclar y ejecutar (R3b, U20). Si el parche tiene
causas de rechazo, o si continuar no ejecutaría (H3–H12), el turno es `INFORM` con esa causa, sin pendiente y sin
bucle (§6 H1, §8.4).

**Vencimiento pegajoso (R3c, U26).** Un turno que encuentra el ancla vencida y no la confirma ni la reemplaza lleva el
eco `CONTEXT_EXPIRED`, guardado en su registro. Mientras el último registro lo lleve y el ancla no haya cambiado, el
ancla sigue **vencida** para los turnos siguientes, sea cual sea el plazo transcurrido. Vuelve a estar vigente sólo con
«continuar» (`CONTEXT_EXPIRED_CONFIRM`), con «reset» o una cancelación, o con una búsqueda nueva. No hay campos nuevos
en el tipo: lo decide el eco del último registro.

Si además había una pendiente: provisionalmente gana F11 (`CONTEXT_EXPIRED_CONFIRM`); la precedencia definitiva es
G-FU-12 (M2b).

### 10.2 Qué ancla deja cada turno

| Turno | `anchor_trace_id` que deja |
|---|---|
| búsqueda nueva o delta READY (ejecute, dé cero o falle la fuente) | él mismo |
| página; evento de vista/orden/filtro READY | él mismo |
| `CLARIFY`, `REASK`, `INFORM`, caída del proveedor | el anterior (no se consume) |
| búsqueda nueva que queda en pregunta | el anterior, hasta que la pendiente se resuelva o se cancele |
| turno de un dueño legacy | no escribe registro ⇒ no hay ancla Foundation |

---

## 11. Eco

`echo_items(base, efectiva, patch, record)` es puro y sale de valores tipados y procedencia. Lista cerrada:

| Clase | Obligatoria cuando |
|---|---|
| `CARRIED` | delta o respuesta ejecutados con algo del ancla que sobrevive sin tocar (una línea, acción «quitar») |
| `UNIT_INHERITED` | `unit_source = PREVIOUS_ACCEPTED_VALUE` (valor completo y de dónde sale) |
| `UNIT_SHARED_IN_RANGE` | `unit_source = SIBLING_IN_RANGE` (las dos cotas con su unidad) |
| `YEAR_INHERITED` | `year_source = PREVIOUS_ACCEPTED_TIME` |
| `YEAR_CLOCK_INFERRED` | `year_source = CLOCK_INFERRED` |
| `RELATIVE_WINDOW_RESOLVED` | periodo relativo con sus fechas absolutas: al reloj (`RELATIVE_TO_CLOCK`) o al referente previo (§16.3, D7) |
| `VALUE_RANGE_REPLACED` | sustitución A8.1, o `CLEAR(VALUE)` explícito seguido de `SET` que no repone un lado |
| `TOPIC_REPLACED` / `POLARITY_INVERTED` | §6.5 |
| `VIEW_DERIVED` / `VIEW_REDERIVED` | toda vista derivada se anuncia; además, si cambió (F9) |
| `UNIVERSE_CHANGED` | `REPLACE_UNIVERSE` efectivo; si hay TIME arrastrado, a qué campo de fecha aplica ahora |
| `TOPN_CARRIED` / `TOPN_CAPPED` / `ORDER_DIRECTION_DEFAULTED` | F10 / P8 (`n_requested > n`) / F21 |
| `CONTEXT_RESET` | búsqueda nueva con ancla vigente; acción «aplicar a la búsqueda anterior» |
| `CONTEXT_EXPIRED` | todo turno que encuentra el ancla vencida y no la confirma ni la reemplaza: `CLARIFY CONTEXT_EXPIRED_CONFIRM`, o el `INFORM` de H1 o del paso 3b; mantiene vencida el ancla (§10.1, R3c) |

**Derivación exacta.** El conjunto de clases de eco de un turno es exactamente el que deriva el esquema Gold 2.1 §7
(«sí y sólo sí») **más** una única adición de autoridad A: un relativo al referente previo (§16.3, regla de recibo
`time.relative_to_previous`) emite también `RELATIVE_WINDOW_RESOLVED` con las fechas absolutas (ADR-103 D7), además del
`YEAR_INHERITED` que deriva el Gold (`year_source = PREVIOUS_ACCEPTED_TIME`). INV-M2A-12 («exactamente») se mide contra
esta derivación.

No son eco de M2a: los `host_notice` del contrato M1, el aviso `SMMLV_CONVERTED` de D10 (lo emite la presentación de
E-VALUE, en primer turno y en M2a), `PENDING_CANCELLED` (efecto sin anuncio: se ejecuta lo nuevo) y las exclusiones de
F17 (condiciones nuevas corrientes). El eco se guarda en el registro y se compara con lo ejecutado (INV-M2A-12).

---

## 12. Hashes

| Hash | Definición | Notas |
|---|---|---|
| `interpretation_hash` | `_digest(AcceptedInterpretation.to_dict())` | pasa a ser el digest de la **efectiva** (cambio declarado, ADR-103 D7); es el `semantic_ref` |
| `plan_hash` | sin cambio (`foundation_projection`) | el de un primer turno **no** cambia (misma SoQL) |
| `patch_sha256` | `_digest(SemanticPatch.to_dict())` | determinista |
| `pending_sha256` | `_digest(PendingClarification.to_dict())` | liga evento ↔ pendiente |
| `record_sha256` | `_digest(record sin record_sha256)` | lector estricto |
| `raw_sha256` | sha256 del texto del turno | enlace del origen SPAN; el texto no viaja al registro de ocurrencias ni al ledger |

Todos usan la misma canonicalización (`_digest`). Mismo `(base, patch)` ⇒ mismo `interpretation_hash`.

---

## 13. Fallo de fuente

- La efectiva de un turno READY cuya fuente falló **es ancla** y es refinable (A6): existe `semantic_ref`, no existe
  `result_set_ref`.
- Un delta semántico sobre ella se compone y se ejecuta (`EXECUTE`); lo que necesita filas o página ⇒ `INFORM`, 0
  llamadas (§9.1).
- La caída del **proveedor** (LLM) en un turno M2a no produce efectiva: rige D1 de ADR-103 («no pude interpretar
  ahora»), sin legacy; el ancla no cambia.

---

## 14. Product View derivada frente a universo explícito

- **Universo dicho** (sujeto ≠ `unspecified`, o vista elegida por botón = `USER_OVERRIDE`): pegajoso. Sólo
  `REPLACE_UNIVERSE` (texto o evento) lo cambia; nunca se recalcula.
- **Universo no dicho** (`unspecified` + `HostViewDecision` PV1–PV6): tras componer, `decide_view` se ejecuta sobre la
  efectiva compuesta; si la vista cambia ⇒ `VIEW_REDERIVED`; toda vista derivada lleva `VIEW_DERIVED` (F9).
- `REPLACE_UNIVERSE` con ancla dual se queda con el universo nombrado. TIME arrastrado se conserva y se anuncia a qué
  campo de fecha aplica ahora (`UNIVERSE_CHANGED`).
- Tras cambiar el universo, F27 (H8) comprueba STATE y MODALITY sobre el dataset resultante.
- El tipo `HostViewDecision` y `decide_view` son de la ola F (C0 §5.6); M2a los consume, no los define. Interacciones
  vista/universo no adjudicadas (sustantivo coloquial que podría pedir la vista de oportunidades, estado redundante con
  la lente) son G-FU-19: provisionalmente se conserva el universo vigente y se anuncia.

---

## 15. VALUE

### 15.1 Por lado (A1, F2 vigente salvo su cláusula de «rango sin interior»)

Una cota nueva sustituye la cota **de su mismo lado**; la del lado opuesto **se conserva**; una cota del lado que
faltaba completa el rango. «Entre X y Y» son dos `SET` (lower `>=`, upper `<=`) del mismo turno. «Cambia el valor a…»
= `CLEAR(VALUE)` + `SET`.

### 15.2 A8.1 — intervalo contradictorio

**Comparabilidad (A8).** A8.1 sólo se aplica cuando las dos cotas están expresadas en unidades comparables: las dos en
unidades de escala de pesos (`PESOS`, `MILES`, `MILLONES`, `MIL_MILLONES`, `BILLONES`, por `Provenance.unit`), o las dos
en `SMMLV`. Si una cota está en `SMMLV` y la otra en pesos, el caso **no está adjudicado** (A8: «unidades distintas no
convertibles, p. ej. millones frente a SMMLV»): no se compara, no se sustituye ni se conserva en silencio ⇒
`CLARIFY UNRESOLVED_VALUE:VALUE` (provisional fail-closed de G-FU-06, frontera E-VALUE).

Definiciones, con dos cotas comparables (`money@1.amount_cop`; para dos cotas `SMMLV` de la misma vigencia equivale a
comparar `Provenance.source_amount`):

- Tras aplicar la cota nueva y conservar la opuesta, sean `lo` (comparador `>` o `>=`) y `hi` (`<` o `<=`).
- El intervalo es **vacío** si `lo.amount > hi.amount`, o si son iguales y algún comparador es exclusivo.
- El intervalo es **degenerado** si `lo.amount == hi.amount` (cualquier comparador).

**Regla.** Si el intervalo lo **genera un follow-up** (la cota nueva viene del parche y la opuesta está **arrastrada**
de la base) y es vacío o degenerado, la cota nueva **sustituye la restricción VALUE completa** (la opuesta desaparece)
y se anuncia `VALUE_RANGE_REPLACED`.

**Igualdad pedida explícitamente.** Si las dos cotas vienen del **mismo turno** (las dos operaciones están en el mismo
parche o en la misma aceptación de primer turno), el intervalo es lo que el usuario pidió: un punto único inclusivo se
ejecuta tal cual («entre 500 y 500»). El criterio es el **origen** de las cotas, no una lista de palabras. En `main`
ningún operador VALUE admite igualdad (`money_comparators.py`: EQ sin operador; `Comparator` = `>`, `>=`, `<`, `<=`):
leer «exactamente X» como el par `>= X` + `<= X` del mismo turno es trabajo de la autoridad VALUE (D-MONEY, E-VALUE;
dependencia de §19, sin caso en el Gold); mientras no exista, esa forma cae en `CLARIFY` de la autoridad (fail-closed).

Un intervalo vacío con las dos cotas del mismo turno («entre 500 y 100») no es un conflicto de follow-up: su lectura es
de la autoridad VALUE (contrato M1 / E-VALUE, G-FU-18) y M2a no la altera ni la adjudica.

Esta regla **sustituye** la cláusula «Rango sin interior ⇒ CLARIFY con dos opciones-evento» de ADR-103 Enm. 4 D7 (y
OQ-4 = a), la cláusula «sin interior» de F2 y F26 (ver C.1).

### 15.3 Unidad compartida en un rango (F6)

La unidad dicha una vez en un rango cubre las dos cotas (`unit_source = SIBLING_IN_RANGE`, regla de recibo
`unit.sibling`, eco `UNIT_SHARED_IN_RANGE`). Es lectura de la autoridad VALUE en `resolve`, no herencia entre turnos.

### 15.4 Herencia de unidad (A1/F5)

Un monto sin unidad hereda la unidad sólo si hay **una única unidad activa inequívoca** en el VALUE de la base
(SMMLV incluida, A2). La calcula la autoridad VALUE por la entrada separada de insumos del host (ADR-103 §4.3, D10),
con regla `unit.inherited`, `unit_source = PREVIOUS_ACCEPTED_VALUE`, `inherited_from` y eco `UNIT_INHERITED`. Si no hay
unidad única ⇒ `CLARIFY UNIT_AMBIGUOUS`. La herencia sí puede ir seguida de A8.1 (F26 retirada; A1).

### 15.5 SMMLV (A2, D10)

- Ejecutable cuando la efectiva tiene **un único año** autoritativo (un TIME de un solo año civil: `year.from_time`;
  sin TIME: año por reloj, `year.inferred`, eco `YEAR_CLOCK_INFERRED`, OQ-1; heredado: `year.inherited`).
- Un follow-up sobre una base con SMMLV hereda la unidad (§15.4) y, si cambia el año, la cota se **recalcula** sobre la
  nueva vigencia a partir de `Provenance.source_amount` (nunca del COP previo).
- TIME de varios años, o año fuera del catálogo ⇒ `CLARIFY SMMLV_YEAR_AMBIGUOUS`. Nunca se usa el salario de otro año.
- El catálogo es el de ADR-103 D10: **2024–2026** (`app/data/smmlv.py`, copiado a `app/host_data/value/` por E-VALUE).
  SF166 («y en 2023?») es el **control de no extrapolación** del Gold (U1): aclara. Añadir un año **anterior** al
  catálogo (2023 o antes) es una decisión de producto separada y futura, que re-adjudica SF166 y re-sella; añadir el
  año nuevo cada enero (2027…) es el mantenimiento que D10 asigna a D-MONEY/núcleo y no cambia SF166.
- M2a conserva `unit = SMMLV`, `source_amount`, `year`, `year_source` y el COP convertido (en `money@1`). El catálogo,
  la conversión y su anuncio son de E-VALUE (G-FU-06).
- Retirados: `SMMLV_UNSUPPORTED`, `INHERITED_UNIT_IMPLAUSIBLE` (A1, A2).

---

## 16. Temporales adjudicados

1. **Mes sin año (F7).** Hereda el único año activo del TIME de la base (`year.inherited`, `YEAR_INHERITED`). Sin TIME
   previo: ocurrencia más reciente no posterior a hoy (`year.inferred`, `YEAR_CLOCK_INFERRED`). TIME previo plurianual o
   ventana relativa ⇒ `CLARIFY MONTH_YEAR_AMBIGUOUS`.
2. **Rango de años (F8).** «de A a B» / «entre A y B» es inclusivo: el año final entra completo.
3. **Relativo al referente previo (A5; ADR-103 D7, OQ-3 = b).** En un delta, una expresión relativa temporal
   («el mes siguiente», «los seis meses siguientes») se resuelve respecto del **único** TIME positivo activo de la
   base, de granularidad compatible, con aritmética de periodo (predecesor/sucesor sobre `time@1`), y se anuncia con
   fechas absolutas (`RELATIVE_WINDOW_RESOLVED`, además de `YEAR_INHERITED`: §11). La calcula la autoridad TIME por la entrada separada de insumos del
   host (el `TimeValue` previo como insumo tipado), con regla de recibo `time.relative_to_previous`
   (`year_source = PREVIOUS_ACCEPTED_TIME`). Sin referente activo, con varios, o con un referente que es a su vez una
   ventana relativa ⇒ `CLARIFY RELATIVE_TO_PREVIOUS_VALUE` (G-FU-08, E-TIME). El reloj sólo vale para consultas
   autosuficientes.
4. **Relativos al reloj** («este año», «últimos seis meses»): autoridad TIME del primer turno (`relative_period`),
   eco `RELATIVE_WINDOW_RESOLVED`.
5. **Relativos no temporales** (F15): fuera, `CLARIFY` (G-FU-15).

---

## 17. Protocolo de cancelación

1. `CANCEL_PENDING` es la **única** representación de que una pendiente deja de existir. Se escribe en
   `AcceptedTurnRecord.control` del turno que la cancela. Una lista vacía de operaciones nunca significa cancelar.
2. Se emite cuando: (a) P trae la directiva `CANCEL_PENDING` (rechazo citado, F23); (b) P es búsqueda nueva con una
   pendiente vigente (A7) — el host lo escribe aunque el productor no emita directiva; (c) «reset» ante
   `CONTEXT_EXPIRED_CONFIRM`; (d) `asks` agota el tope; (e) la pendiente vence (se anuncia al siguiente turno).
3. Base tras cancelar con un fragmento: el **marco pendiente** (`frame`), nunca un turno anterior al que preguntó (A7).
4. Tras `CANCEL_PENDING` no queda pendiente; el ancla sigue siendo la que había (§10.2) salvo que el mismo turno deje
   una nueva efectiva.
5. Efecto `PENDING_CANCELLED` (sin eco).

---

## 18. Invariantes

| Id | Invariante | Verificación (implementación) |
|---|---|---|
| INV-M2A-01 | **Cierre atómico**: `compose` da una efectiva válida o un `DeltaRefusal`; nunca parcial | propiedad; mutante «aplica la primera op y falla la segunda» |
| INV-M2A-02 | **Identidad de lo no tocado** (valor, autoridad, políticas, origen, procedencia; `request_ids` salvo la excepción de §6.3) | propiedad; mutantes «re-resuelve», «cambia origen» |
| INV-M2A-03 | **Sólo sobre lo aceptado y vigente**: base = efectiva aceptada del mismo chat, legible, de esquema conocido, en plazo; o el `frame` de una pendiente | un test y un mutante por causa |
| INV-M2A-04 | **Nada cruza a legacy** con ancla Foundation; `Turn.parsed_params == {}`; ningún símbolo de M2a importa `query_router`, `followup_engine` ni los detectores de dueño | espías; pin de imports; mutantes «puente» y «entrega a legacy» |
| INV-M2A-05 | **Un fragmento nunca se ejecuta solo**: con ancla o pendiente, sólo §7.2(c) o §8.3(2–3) inician una búsqueda sin heredar, y nunca con Continuación(P) | propiedad sobre propuestas; mutantes A9.1 |
| INV-M2A-06 | **El primer turno no cambia**: misma SoQL, `plan_hash` y disposiciones que `main` | replay del corpus + capturas |
| INV-M2A-07 | **`resolve` no depende del ancla**; lo heredado entra sólo por la entrada separada de insumos | propiedad; mutante que pasa el ancla |
| INV-M2A-08 | **Sin híbridos**: tras una pendiente se ejecuta `frame + candidato`, `frame + delta` con `CANCEL_PENDING`, o sólo lo nuevo | filas de §8.3; mutantes F24/F25/F29 |
| INV-M2A-09 | **Registro inmutable**; `trace_id` único; digest verificado; cola ilegible ⇒ inválida | mutantes |
| INV-M2A-10 | **Paginar es el mismo plan**: sin LLM, instante guardado, `plan_hash` igual, ids disjuntos | mutantes |
| INV-M2A-11 | **Vista**: se recalcula sí y sólo sí era derivada; todo recálculo se anuncia | tabla PV × parche |
| INV-M2A-12 | **Eco = ejecución**: los `EchoItem` cubren exactamente lo arrastrado, lo cambiado y toda procedencia heredada, inferida, por defecto, recortada o sustituida | propiedad `echo ↔ diff(base, efectiva)` |
| INV-M2A-13 | **La tarjeta de estado no lleva valores** | propiedad; mutante |
| INV-M2A-14 | **Un origen por condición** | contrato |
| INV-M2A-15 | **Barrera**: ningún lector legacy usa como referente un turno Foundation ni uno anterior a él | test por lector |
| INV-M2A-16 | **Remoción sólo citada**: toda condición de la base ausente de la efectiva tiene un `CLEAR`/`SET` sobre su ranura, o es una sustitución declarada de §6.5 con su eco | propiedad `ranuras(base − efectiva) ⊆ ranuras(patch) ∪ sustituciones` |
| INV-M2A-17 | **Herencia acotada**: sólo desde `compose`, con recibo, procedencia y eco; nunca de dos unidades ni de varios años | kit por autoridad |
| INV-M2A-18 | **VALUE por lado + A8.1**: nunca se ejecuta un intervalo vacío o degenerado generado por un follow-up con la cota opuesta arrastrada; con unidades no comparables (SMMLV frente a pesos) se aclara; un punto único sólo si sus dos cotas son del mismo turno | tabla exhaustiva lado previo × lado nuevo × relación de cifras × origen |
| INV-M2A-19 | **Polaridad**: invertir sólo con igualdad de valor canónico y un único positivo | tabla exhaustiva |
| INV-M2A-20 | **Alcance total y único**: todo turno recibe exactamente un `TurnScope`; `OUT_*`/`INFORM` hacen 0 llamadas | propiedad |
| INV-M2A-21 | **Topes y defectos anunciados** (top-N, dirección de orden) | tests |
| INV-M2A-22 | **Referentes separados**: nada que necesite filas se resuelve con `semantic_ref`; nada se compone con `result_set_ref` | mutantes |
| INV-M2A-23 | **Cancelación explícita**: toda pendiente que deja de existir tiene un registro con `control = CANCEL_PENDING` | propiedad sobre secuencias |
| INV-M2A-24 | **Sin léxico nuevo en el host**: A8.1 y A9.1 se deciden sobre valores tipados, origen de cotas y dimensiones de P; el host no lee texto para decidirlos | pin AST: los módulos de M2a no importan léxicos ni usan regex sobre el turno |
| INV-M2A-25 | **Invariante mayor (R3-5). Toda respuesta alcanzable es representable**: cada `DeltaRefusal` de §6, cada fila de §7.1 y cada `CLARIFY`/`REASK`/`INFORM` que prescribe este contrato tiene un código de A.3 que el tipo admite (`PendingClarification.reason` para `CLARIFY`/`REASK`); ningún estado que los tipos permiten alcanzar queda sin respuesta | barrido generado de los tipos (`tests/test_m2a_contract_r3_sweep.py`): invariantes del AST, op × ranura construido, §6, §7.1, A.1 y A.3 parseados del contrato; un invariante nuevo falla hasta barrerse, y lo que falte en el tipo es un rojo declarado (xfail estricto), nunca un verde. **Multi-op (R3b, U21):** (a) barrido exhaustivo por parejas (op_i, ranura_i) × (op_j, ranura_j) × ocupación de la base (vacía, ocupada, dependientes CARDINALITY↔ORDER, dos lados de VALUE, ancla dual, ancla vencida, con y sin pendiente), sobre un modelo simbólico de ocupación de ranuras derivado de este contrato y generado desde las tablas del código; (b) Hypothesis con parches de N operaciones sobre el mismo modelo, más invariancia a la permutación de las operaciones (`tests/test_m2a_contract_r3_multiop.py`). Cada combinación tiene exactamente una respuesta representable: efectiva válida, respuesta cerrada construible (`CLARIFY` con pendiente que cumple §8.4, `INFORM`, `REASK`) o dependencia declarada (§19); nunca una pendiente inválida, un candidato imposible, una excepción ni un estado inválido. Sobre el `compose` real (permutaciones + `check_transition`) es gate de M2A-3 fase 2. Gate `CONTRACT_COMPLETE_FOR_M2A3` ⇔ INV-M2A-25 satisfecho por ese barrido (usuario, 2026-10-02) |
| INV-M2A-26 | **Pendiente posible (R3b, U20)**: una `PendingClarification` sólo existe si su `frame` es `None` o válido y cada candidato compone sobre él (§8.4); si no, `INFORM` sin pendiente. Las causas de rechazo se deciden antes de H1 con precedencia cerrada (§6 paso 3b); sobre un ancla vencida sólo puede nacer `CONTEXT_EXPIRED_CONFIRM`, y sólo si «continuar» compone y ejecuta | gate multi-op de INV-M2A-25 (modelo simbólico); mutante «sin la ley» que el gate debe cazar (la pendiente de `CLEAR(ORDER)` + otra causa, ZEUS R3) |

---

## 19. Frontera: M2b, M3 y E-*

`FOLLOWUP_FRONTIER` (57 casos: 16 REAL + 41 SYNTHETIC) **no es backlog de M2a**: ningún caso de la frontera entra en
los gates de M2a y M2a no se amplía para absorberlos. M2a sólo garantiza el **comportamiento provisional** de la
tabla (fail-closed, sin legacy y sin fuente salvo donde se dice).

| gap_id | Hueco | Ids (frontera) | Conteo | Provisional en M2a | Dueño | Salida |
|---|---|---|---|---|---|---|
| G-FU-04 | filas/página tras fallo de fuente | [R0276, SF179, SF180, SF181] | 4 | `INFORM`, 0 llamadas; el delta semántico sí se ejecuta | protocolo M2a (M2A-6) | código de razón propio al implementar paginación |
| G-FU-05 | paginar ancla dual; página frente a top-N | [SF147, SF148] | 2 | `CLARIFY` | protocolo / M2b | regla y código de razón |
| G-FU-06 | conversión SMMLV: eco, procedencia, años; **montos no comparables (millones frente a SMMLV)** | [R0306, SF169, SF170] | 3 | `CLARIFY` sin año único o con cotas no comparables (§15.2, fail-closed: más estricto que la mejor lectura `EXECUTE` que los etiquetadores dieron a SF170 marcándola AMBIGUOUS); M2a conserva unidad, año, cantidad fuente y COP | E-VALUE | catálogo por vigencia + Gold E-VALUE |
| G-FU-07 | territorio contenido/contenedor frente a comprador | [SF068, SF069, SF139, SF162] | 4 | `CLARIFY` | E-GEO / E-BUYER | Gold de composición de autoridades |
| G-FU-08 | referente temporal relativo (casos ambiguos o en disputa) | [SF038, SF045, SF173, SF175, SF176, SF177] | 6 | resolver con referente único (§16.3); si no hay referente único o es una ventana relativa, `CLARIFY` | E-TIME | aritmética de periodo + Gold E-TIME |
| G-FU-09 | negación sobre conjuntos | [SF162] | 1 | `CLARIFY` (fail-closed): `DELTA_SLOT_CONFLICT` para la unión (dos `SET` en una ranura); `UNRESOLVED_VALUE:<DIM>` para la negación sobre varios positivos o un valor contenido (§6.5; R3-2, R3-4) | M2b | álgebra de conjuntos por ranura |
| G-FU-11 | follow-up tras PROFILE/COUNT/RANK | [R0189, R0319, R0320, R0321, R0375, R0418, R0435, R0441, R0483, R0560, R0561, SF062, SF143, SF144, SF153, SF154, SF181] | 17 | `HANDOFF_LEGACY` | M3 | misma Conversation IR para todas las capacidades |
| G-FU-12 | contexto vencido con pendiente | [SF064, SF139] | 2 | `CLARIFY CONTEXT_EXPIRED_CONFIRM` (gana F11) | M2b | precedencia F11/F29 |
| G-FU-13 | qué cuenta como «búsqueda vacía» | [SF114, SF116] | 2 | `CLARIFY` | M2b | búsqueda mínima admisible |
| G-FU-14 | referentes extensionales (7 extensionales: SF065–SF067, SF070–SF072, SF180; 4 re-consultas en disputa o ambiguas: SF068, SF069, R0081, R0192; R3, P-2) | [R0081, R0192, SF065, SF066, SF067, SF068, SF069, SF070, SF071, SF072, SF180] | 11 | `INFORM` (sin llamada) sólo para lo que la señal extensional del wire 5.1 clasifique como extensional, una vez exista; sin la señal, M2A-6 no se activa | señal: M2A-5 (gate); ejecución: M2b | señal estructural extensional/intensional en el wire + `result_set_ref` con identidad de fila |
| G-FU-15 | relativos no temporales | [SF039, SF040, SF043, SF044, SF046, SF175, SF176] | 7 | `CLARIFY` | M2b | operadores relativos sobre `ExecutableValue` |
| G-FU-16 | otra operación sobre ancla SEARCH | [SF062, SF153, SF154, SF181] | 4 | `INFORM` | M3 | manifiestos de capacidad sobre la efectiva |
| G-FU-17 | fragmento sin ancla o ancla ilegible | [SF137, SF142] | 2 | `CLARIFY` (pedir la búsqueda completa) | protocolo M2a | regresión fail-closed |
| G-FU-18 | formas de superficie VALUE (comparador desordenado/negado, cota sin comparador en rango) | [R0187, R0192, R0375, SF013, SF170] | 5 | mejor lectura de la autoridad, marcada | contrato M1 / E-VALUE | enmienda del contrato Gold M1 |
| G-FU-19 | vista y universo | [R0081, R0276, SF084, SF132] | 4 | recalcular la vista sólo si era derivada (F9) y anunciar; ante un sustantivo coloquial que podría pedir otra vista (SF132), conservar el universo vigente y anunciarlo | ola F | tabla PV adjudicada para follow-ups |

La tabla de huecos no lleva `impacto_m2a3`: la frontera no entra en ningún gate de M2a (U25).

Además, sin casos: G-FU-10 (`observed_executor = LEGACY_SEARCH`, esquema de observación), G-FU-20 (deshacer, sin
decidir), G-FU-21 (desempate de previos en replay, método del Gold). G-FU-01/01b/02/02b/03 están **cerrados** por A8,
A8.1, A9, A9.1 y §8 y sus casos están en `M2A_GOLD`. El conteo por gap suma más de 57 porque un caso puede tocar
varios huecos.

**BLOCKER.** Si un paso de implementación descubre que un caso de `FOLLOWUP_FRONTIER` es necesario para cumplir este
contrato (un gate de M2a no puede pasar sin resolverlo), se registra `BLOCKER` y se para; no se amplía M2a.

**Dependencias declaradas de casos del Gold M2a con otros carriles** (no son frontera; son insumos). Cada fila
declara sus ids explícitos (también `[]`), un conteo que se verifica contra ellos, un dueño de la lista cerrada y
`impacto_m2a3` (R3b, U23):
- `LECTURA`: hace falta para convertir el lenguaje en IR. Dado un IR tipado, el reducer **sí** es medible.
- `COMPOSICION`: hace falta para decidir o componer el estado. El reducer **no** es medible todavía en esos casos.

SF166, SF168 y R0303 llevan SMMLV y **no** son dependencia: H3 los decide sobre el catálogo cerrado 2024–2026 que fija
§15.5 (regla pura; SF166 es el control de no extrapolación, U1). Un caso puede estar en varias filas.

| Id | Alcance | Ids | Conteo | impacto_m2a3 | Necesitan | Dueño |
|---|---|---|---|---|---|---|
| `DEP-TIME-RELATIVE` | A5: relativo al referente temporal único (§16.3) | [SF037, SF042, SF174] | 3 | LECTURA | entrada separada de TIME con `time.relative_to_previous` | D-TIME (E-TIME) |
| `DEP-INHERITANCE` | herencia de unidad o año y mes sin año (F5–F7, §15.4, §16.1) | [R0496, R0497, R0498, R0531, R0532, SF031, SF032, SF033, SF034, SF035, SF101, SF103, SF104, SF106, SF167, SF183] | 16 | LECTURA | `unit.inherited`, `year.inherited`, `year.inferred` por la entrada separada; lectura del mes | D-MONEY, D-TIME |
| `DEP-SMMLV-READ` | SMMLV en follow-ups: leer o tipar una cota SMMLV (§15.5) | [R0301, SF165, SF167] | 3 | LECTURA | conversión por vigencia de D10 dentro del catálogo 2024–2026 | D-MONEY (E-VALUE) |
| `DEP-SMMLV-RECALC` | SMMLV arrastrado cuando cambia el año (§15.5) | [R0304] | 1 | COMPOSICION | recálculo de la cota arrastrada a partir de `source_amount` sobre la nueva vigencia | D-MONEY (E-VALUE) |
| `DEP-VALUE-FORMS` | formas «entre X y Y», «mayores de», «pasen de» | [SF087, SF093, SF098, SF106, SF107, SF129, SF136, SF191] | 8 | LECTURA | lectura de la autoridad VALUE del primer turno | D-MONEY (E-VALUE) |
| `DEP-VALUE-EQUALITY` | igualdad «exactamente X» (§15.2) | [] | 0 | LECTURA | lectura como par `>=`/`<=` del mismo turno | D-MONEY (E-VALUE) |
| `DEP-BUYER-TERRITORY` | F30: territorio nuevo con comprador arrastrado (H6) | [SF123, SF155] | 2 | LECTURA | `buyer.territory.<código>` / `buyer.national` | D-BUYER |
| `DEP-DERIVED-VIEW` | vista derivada (§14) | [R0106, R0107, R0529, R0531, R0532, SF063, SF081, SF082, SF126, SF185] | 10 | COMPOSICION | `HostViewDecision`, `decide_view` | ola F |
| `DEP-REGION` | región (`GeoValue` REGION; H7) | [R0274, R0281, R0282, SF003, SF004, SF123, SF124, SF159] | 8 | LECTURA | miembros y lista cerrada de regiones | E-GEO |
| `DEP-STATE-LITERALS` | estado sobre el universo de la petición (H8) | [R0094, SF009, SF010, SF078, SF079, SF159] | 6 | LECTURA | `state@1.literals` por dataset | E-STATE |
| `DEP-MODALITY-UNIVERSE` | modalidad sobre el universo resultante (H8) | [R0459, SF007, SF008, SF080, SF160] | 5 | COMPOSICION | catálogo de modalidad por dataset (`modality@1` no lleva el dataset) | E-MODALITY |
| `DEP-O1B-OPPORTUNITIES` | SF075: `REPLACE_UNIVERSE` a oportunidades con TIME arrastrado (R3-3) | [SF075] | 1 | COMPOSICION | re-decidir O1B / `opportunity-explicit-state` sobre la efectiva (su representación es de M2A-1b) y TIME explícito sobre oportunidades (`foundation_projection.py:235-237`). Mientras falte, `compose` rechaza con `OP_NOT_ACCREDITED:UNIVERSE` ⇒ `CLARIFY UNRESOLVED_VALUE:UNIVERSE` y el caso es rojo declarado (§20.2) | `FOUNDATION_O1B_EXPLICIT_TIME_PARITY` (+ M2A-1b) |
| `DEP-E-ORDER` | follow-ups con ORDER/CARDINALITY: leer el criterio o la cifra (R3, D-5) | [R0079, R0083, R0087, R0089, R0099, R0515, SF015, SF016, SF017, SF018, SF019, SF020, SF021, SF022, SF117, SF118, SF119, SF120, SF122, SF140, SF160, SF161, SF171, SF172, SF195] | 25 | LECTURA | autoridad ORDER/CARDINALITY (`order@1`, `cardinality@2`, `order.direction_defaulted`, `topn.capped`); `main` aún no las admite en el primer turno (`UNRESOLVED_HOST_DIMENSION:ORDER`). Mientras falte, la pendiente de §6.7 (R3-1) es provisional fail-closed: **sólo C1** «quitar orden y límite» y la cancelación, sin candidatos `SET_ORDER` | E-ORDER |
| `DEP-E-ORDER-CANDIDATES` | candidatos `SET_ORDER` de la pendiente de R3-1 (§6.7) | [SF192, SF199, SF200, SF201] | 4 | COMPOSICION | enumerar al preguntar los criterios de orden alternativos del dataset; SF199–SF201 traen esa opción `SET_ORDER` en su pendiente previa (R3c). Las alternativas selladas de SF192 son **PARCIALES** (sólo C1) y el registro lo declara (`partial_alternatives`); los `SET_ORDER` no se sellan | E-ORDER |
| `DEP-M2A5-WIRE-CARDINALITY` | «dame 20» con la pendiente de R3-1: IR v2 rechaza un `FOLLOWUP_DELTA` que sólo trae CARDINALITY (`interpretation_contract.py:438-439`), §7.1 fila 4b `INVALID_PROPOSAL` (§6.7; R3c) | [SF199] | 1 | LECTURA | que el wire admita una CARDINALITY sola en un `FOLLOWUP_DELTA`; mientras no lo haga, la etiqueta sellada es la fila 4b | M2A-5 |
| `DEP-M1-PENDING-FRAME` | primer turno sin ancla cuya pendiente de M1 no tiene `frame` construible (§8.4) | [] | 0 | COMPOSICION | representar la pendiente de M1 en `from_acceptance` | M2A-1b |

**Una autoridad para las dependencias (R3, U19).** §19 es la única fuente de dependencias y huecos diferidos. Su extracción
generada y sellada (`docs/audits/m2a_conversation/gold/CONTRACT_S19_DEPENDENCIES.json`, generador
`scripts/audits/m2a_conversation/extract_contract_dependencies.py`) es lo único que consumen el oráculo, los
verificadores y los carriles; ninguno mantiene una lista ni un override propio. La extracción falla cerrada si:
- estas tablas no se pueden leer;
- un id no tiene la forma exacta (`sf075` o `SF-075` fallan) o no existe en el índice sellado;
- un conteo no coincide con sus ids (también con 0);
- un dueño no está en la lista cerrada del generador;
- un valor de `impacto_m2a3` no es `LECTURA` ni `COMPOSICION`;
- hay una discrepancia con el índice sellado (los ids de cada hueco son exactamente los casos de FRONTIER con ese
  `gap_id`) o con los registros sellados (todo `partial_alternatives` nombra una fila `COMPOSICION` que contiene su
  caso).

---

## 20. Gates

### 20.1 Gates de esta misión (M2A-0, documental)

| # | Gate | Cómo se verifica |
|---|---|---|
| 1 | Los 209 casos de `M2A_GOLD` (release r3b) son **representables** por este contrato (vocabulario, forma de turno, arrastre, sustituciones declaradas, coherencia estructural de la disposición) | `scripts/audits/m2a_conversation/check_m2a_gold_contract.py` (con `--real-dir` local): 0 violaciones; probado por mutantes (`mutate_check_m2a_gold_contract.py`). **No** prueba que §7.2/§8.3 **deriven** cada disposición: eso exige la evidencia estructural del productor (continuación, marco) y es gate de M2A-8 |
| 2 | Ningún caso requiere `parsed_params` | ídem (G2) |
| 3 | Cada comportamiento tiene una regla contractual | `M2A_TRACEABILITY_MATRIX.md` |
| 4 | Ningún caso FRONTIER absorbido | ídem (G8): GOLD ∩ FRONTIER = ∅; ejemplos de huecos diferidos ∉ GOLD |
| 5 | A8.1 y A9.1 sin léxico nuevo | §15.2 (valores tipados + origen de cotas), §7.2 (dimensiones de P); verificador G5 |
| 6 | `candidate_patches[]` genérico | §8.1; verificador G6 |
| 7 | `semantic_ref` y `result_set_ref` no se confunden | §9; verificador G7 |
| 8 | Material REAL fuera de git | `docs/audits/m2a_conversation/M2A0_CLOSE.md` |
| 9 | Hashes y manifiestos reproducen los artefactos | `scripts/audits/m2a_conversation/m2a_gold_seal.py verify`; `build_sealed_gold.py --check` (la copia sellada sale del candidato importado + U1/U2/D1) |
| 10 | El contrato no contradice IR v2/C1.2 de `main` | §2; el verificador comprueba operadores, dimensiones y tope contra los módulos de `main` |

### 20.2 Gates de implementación (M2A-1…M2A-9; tolerancia cero, peor de ≥2 pasadas, sólo casos `M2A_GOLD`)

Se mide la **efectiva resultante** contra la IR oráculo del Gold. 0 en: fragmento ejecutado solo; condición arrastrada
perdida sin operación citada ni sustitución declarada; híbrido tras una pendiente o respuesta elegida por el producto;
composición sobre base inválida; turno con ancla Foundation que llega a legacy o escribe `parsed_params`; alcance
`OUT_*`/`INFORM` que llama a la fuente; relativo no temporal ejecutado; relativo temporal ejecutado sin referente único
o sin eco; uso de filas sin `result_set_ref`; búsqueda nueva y delta confundidos; intervalo VALUE vacío ejecutado;
valor heredado, inferido, por defecto, recortado o sustituido sin su eco; pendiente que desaparece sin
`CANCEL_PENDING`; follow-up que legacy acierta y M2a no, por clase; estado alcanzable sin respuesta representable
(INV-M2A-25); **0 discrepancias** de disposición, código y
efectiva con la IR oráculo. Las particiones DEV/TEST son las del sello: el prompt itera sólo sobre DEV; la activación
mide TEST; nunca se reutiliza un TEST.

Estos gates son los de M2a en ADR-103 Enm. 4 §4.5, medidos sobre el `M2A_GOLD` sellado (A1–A9.1).

### 20.3 Si un gate falla

No se adapta el Gold ni se parcha el contrato para que pase un caso. Se registra `BLOCKER` con el caso y la regla, y
se eleva: o la regla de producto cambia (adjudicación del usuario + re-sello con log) o el caso se reclasifica por
regla escrita (nunca por conveniencia).

### 20.4 Congelación tras R3 (usuario, 2026-10-02)

Tras R3, el contrato de M2A-3 queda congelado para el conjunto actual de primitivas, operaciones y alcance. No se
reabre para hacer pasar una implementación, incorporar una capacidad downstream ni resolver un caso fuera de alcance.
Sólo se modifica ante evidencia de una inconsistencia del propio contrato o mediante una extensión/versionado explícito
del lenguaje.

---

## Anexo A — Listas cerradas

**A.1 `TurnScope`:** `IN`, `IN_NEW_SEARCH`, `IN_PROTOCOL`, `OUT_NO_ANCHOR`, `OUT_ANCHOR_NOT_SEARCH`,
`OUT_OPERATION_CHANGE`, `OUT_UNSUPPORTED_PROPOSAL`, `OUT_REFERENT_EXTENSIONAL`, `OUT_RELATIVE_TO_PREVIOUS`.

**A.2 Disposiciones:** `EXECUTE`, `NEW_SEARCH`, `CLARIFY`, `REASK`, `INFORM`, `HANDOFF_LEGACY` (sólo ancla legacy).

**A.3 Códigos de razón:**

| Disposición | Códigos |
|---|---|
| `CLARIFY` | `CONTEXT_EXPIRED_CONFIRM`, `BUYER_OR_PLACE_AFTER_ACTOR`, `REGION_NOT_IN_LIST`, `SMMLV_YEAR_AMBIGUOUS`, `RELATIVE_TO_PREVIOUS_VALUE`, `UNIT_AMBIGUOUS`, `MONTH_YEAR_AMBIGUOUS`, `EMPTY_SEARCH_AFTER_DELTA`, `STATE_NOT_IN_UNIVERSE`, `TERRITORY_VS_CARRIED_BUYER`, `NO_ANCHOR`, `ANCHOR_UNREADABLE`, `TARGET_NOT_ADDRESSABLE`, `VALUE_WITHOUT_COMPARATOR`, `DELTA_SLOT_CONFLICT`, `KEEP_ON_EMPTY_SLOT`, `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`, `UNRESOLVED_VALUE:<DIM>` |
| `REASK` | `OPTION_NOT_CHOSEN`, `ANSWER_EDITS_OTHER_SLOT`, `PENDING_UNANSWERED`, `ANSWER_UNRESOLVED` |
| `INFORM` | `CLEAR_ON_EMPTY_SLOT`, `OPERATION_CHANGE:<op>`, `REFERENT_EXTENSIONAL`, `UNSUPPORTED_PROPOSAL:<motivo>`, `STALE_OPTION` (+ el código de G-FU-04 cuando exista) |
| `HANDOFF_LEGACY` | `ANCHOR_NOT_SEARCH:<op>` |

Parámetros (R3):

- `<DIM>`, en `UNRESOLVED_VALUE:<DIM>` y en `OP_NOT_ACCREDITED:<DIM>` de §6, recorre las claves de dimensión de las
  ranuras de §4.2: las once dimensiones de ranura (`TOPIC` … `CARDINALITY`) y `UNIVERSE` (R3-3). Nunca una dimensión
  que no es ranura (`UNSUPPORTED`, `RETRIEVAL_INTENT`, `CONTROL_LANGUAGE`). Es una forma, no un comportamiento: quién
  emite cada código lo dice su regla.
- `<motivo>`, en `UNSUPPORTED_PROPOSAL:<motivo>`, recorre la lista cerrada de §7.1 fila 4b: `MULTI_REQUEST`,
  `UNSUPPORTED_CONDITION`, `UNSUPPORTED_SOURCE`, `INVALID_PROPOSAL`, `NO_ACTION` (D-6).
- `<op>` recorre `Operation`.

**`INFORM` con un código de `CLARIFY` (R3b, U20).** Cuando la pendiente que prescribe una regla no puede existir
(§8.4), cuando el paso 3b de §6 reúne dos o más causas, o cuando el ancla está vencida (§6 H1), la disposición es
`INFORM` con el código de la regla y sin pendiente. Sólo estos códigos de `CLARIFY` son también válidos con `INFORM`:
`DELTA_SLOT_CONFLICT`, `TARGET_NOT_ADDRESSABLE`, `KEEP_ON_EMPTY_SLOT`, `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`,
`UNRESOLVED_VALUE:<DIM>`, `SMMLV_YEAR_AMBIGUOUS`, `RELATIVE_TO_PREVIOUS_VALUE`, `BUYER_OR_PLACE_AFTER_ACTOR`,
`TERRITORY_VS_CARRIED_BUYER`, `REGION_NOT_IN_LIST`, `STATE_NOT_IN_UNIVERSE`, `UNIT_AMBIGUOUS`, `MONTH_YEAR_AMBIGUOUS`,
`EMPTY_SEARCH_AFTER_DELTA`, `VALUE_WITHOUT_COMPARATOR`.

Nunca lo son `CONTEXT_EXPIRED_CONFIRM`, `NO_ANCHOR` ni `ANCHOR_UNREADABLE`, porque su pendiente siempre es posible
(§8.4). Ningún código de `REASK` ni de `INFORM` es válido con `CLARIFY`.

Retirados: `SMMLV_UNSUPPORTED` (A2), `INHERITED_UNIT_IMPLAUSIBLE` (A1), `TOPN_CAP_EXCEEDED` (P8),
`DELTA_OR_NEW_AMBIGUOUS`, `SELF_CONTAINED_REQUEST`, `OPERATION_SWITCH:*`, `OPERATION_HANDOFF:*`.

**A.4 Efectos (sin anuncio propio salvo su eco homónimo):** `VALUE_RANGE_REPLACED`, `TOPIC_REPLACED`,
`POLARITY_INVERTED`, `VIEW_REDERIVED`, `UNIVERSE_CHANGED`, `TOPN_CARRIED`, `TOPN_CAPPED`, `ORDER_DIRECTION_DEFAULTED`,
`PENDING_CANCELLED`, `CONTEXT_RESET`.

**A.5 Eco:** §11.

**A.6 Etiquetas `UNSUPPORTED` reservadas para el wire 5.1:** `relative_to_previous` (F15/A5), `no_selection` (F24),
`value_without_comparator` (B5).

**A.7 Reglas de recibo con efecto en el host que M2a consume:** las de ADR-103 §4.3 (`unit.*`, `unit.sibling`,
`unit.inherited`, `unit.smmlv`, `year.from_time`, `year.inferred`, `year.inherited`, `order.direction_defaulted`,
`topn.capped`, `buyer.territory.<código>`, `buyer.national`, `buyer.exact`, `actor.authoritative`, `geo.exact`,
`reanchored`) **más** `time.relative_to_previous` (registrada por ADR-103 Enm. 5).

## Anexo B — Lectura del Gold 2.1 por este contrato

- `ops` del Gold ⇒ `SemanticPatch.ops` (tabla §2); `ANSWER` ⇒ `PENDING_ANSWER`; `CANCEL_PENDING` ⇒ `control`.
- `ops = []` + `NEW_SEARCH` con `prior.state = PENDING` ⇒ `control = CANCEL_PENDING` (§17.2b). No hubo re-etiquetado:
  es la lectura estructural de las dos convenciones O12/O13 que el Gold conserva.
- `effects` y `expected_echo` son derivados: donde una etiqueta no los guardó, se derivan con §6.5/§11 (el verificador
  lo hace y comprueba que, donde sí se guardaron, coinciden).
- `gap_codes` de forma de superficie (`B5RANGE`, `COMPARATOR_FORM`) en casos del Gold son marcas de contacto: los dos
  etiquetadores coinciden en la operación tipada y la lectura es de la autoridad VALUE (§19, dependencias).
- `RELATIVE_ANCHOR` en casos del Gold: sólo con referente único (A5); el resto está en la frontera (G-FU-08).
- R0205: en `M2A_GOLD` por A8.1 y por el contrato VALUE vigente (`app/core/money_comparators.py`,
  `_QUANTIFIER_SUFFIXES`: «en adelante» ⇒ `GTE`); las dos etiquetas ya leían `>=`; sin re-etiquetado.
- Release r2 (usuario, 2026-10-02): **U1** re-etiqueta SF166 a `CLARIFY SMMLV_YEAR_AMBIGUOUS` (§15.5, control de no
  extrapolación); **U2** normaliza el preestado de SF063 a uno alcanzable (`asks = 2`, §8.1); **D1** declara SF168
  prueba de estado persistido inválido (un ancla SMMLV plurianual no es alcanzable bajo §15.5; §6.6 falla cerrado).
  Todo preestado del Gold es alcanzable bajo este contrato salvo los declarados así (verificador G11).
- Release r3 (usuario, 2026-10-02; enmienda R3, Anexo D): **U3** añade cuatro casos sintéticos, SF192–SF195.
  - Los etiquetaron dos etiquetadores ciegos (A y B) contra este contrato enmendado. Sus etiquetas se conservan
    literales en `docs/audits/m2a_conversation/labeling_r3/labels_{A,B}.jsonl`.
  - El usuario adjudicó las diferencias (`gold/ADJUDICATION_R3.md`), y `build_sealed_gold.py` reproduce la copia
    sellada a partir de esas etiquetas más U3.
  - En SF192 se sella sólo C1: los candidatos `SET_ORDER` los determina E-ORDER (§19, `DEP-E-ORDER`).
  - Ningún registro existente cambia y ningún id existente cambia de lado DEV/TEST. Sus preestados son alcanzables
    (G11).
- Release r3b (usuario, 2026-10-02; enmienda R3b, Anexo D.3): **U25** añade seis casos sintéticos, SF196–SF201.
  - Son los controles mínimos de R3b, etiquetados a ciegas por A y B contra este contrato enmendado. Las etiquetas se
    conservan literales en `docs/audits/m2a_conversation/labeling_r3b/labels_{A,B}.jsonl`.
  - El usuario adjudicó sus diferencias (`gold/ADJUDICATION_R3.md`, sección R3b): SF199 toma la lectura de B, §7.1
    fila 4b; SF202 se etiquetó y se retira; la forma de las ops sigue la de R3.
  - Cambian dos registros existentes: SF192, que declara sus alternativas parciales (`partial_alternatives`, R3b-1), y
    SF195, cuya polaridad de `SET_ORDER` se adjudica `POSITIVE`.
  - Ningún id existente cambia de lado DEV/TEST.
- Release r3c (usuario, 2026-10-02; U26): SF198 lleva el eco `CONTEXT_EXPIRED` (vencimiento pegajoso, §10.1 y §11).
  SF199–SF201 dependen de E-ORDER y SF199 además de M2A-5 (§19). Ningún id cambia de lado.

## Anexo C — Cláusulas sustituidas

### C.1 Tabla

| Texto anterior | Dónde | Sustituido por |
|---|---|---|
| «Rango sin interior ⇒ CLARIFY con dos opciones-evento: "sustituir el rango" y "mantener y ajustar"» | ADR-103 Enm. 4 D7, viñeta «VALUE por lado»; OQ-4 = a | §15.2 (A8, A8.1) |
| «Búsqueda nueva = consulta autosuficiente (A4). Decide el productor.» | ADR-103 Enm. 4 D7 | §7.2 (A9, A9.1): decide el host por la forma de P; el productor aporta la evidencia de continuación |
| Gate M2a medido sobre «Gold de follow-ups 2.1 con A1–A7 sellado» | ADR-103 Enm. 4 §4.5 y §4.4 (M2A-0); C0 §5.3.6 punto 5(b) | `M2A_GOLD` sellado con A1–A9.1 (§20.2) |
| F2 «rango sin interior (vacío o de un solo punto) ⇒ sustituir» y F26 | adenda F1–F30 | §15.2 (A8.1 con la excepción de igualdad pedida) y §15.4 |
| F14 «SMMLV no ejecutable» | adenda | §15.5 (A2, D10) |
| «Consulta completa = universo citado + condición de contenido» | `M2A_DESIGN_v2_1` §5.2, Gold 2.1 §5.1 | §7.2 |
| `candidate_slots` + «doble ranura» | `M2A_DESIGN_v2_1` §5.5, Gold 2.1 §2.3 | §8.1 `candidate_patches[]` |
| «PESOS y SMMLV no se heredan» | `M2A_DESIGN_v2_1` §5.3 | §15.4 (A1, A2) |
| `SET_UNIVERSE` | F1, Gold 2.1 §4 | `REPLACE_UNIVERSE` (mismo efecto) |
| «Un rechazo produce `CLARIFY` con el código de la autoridad (p. ej. `UNRESOLVED_VALUE:<DIM>`) o `REASK` si había pendiente» | este contrato, §6, antes de R3 | §6, tabla `DeltaRefusal` → A.3 (R3-2) |
| «Con varios positivos o valor contenido: fail-closed (`CLARIFY`, G-FU-09, M2b)» | este contrato, §6.5 F16, antes de R3 | §6.5: `OP_NOT_ACCREDITED:<DIM>` ⇒ `CLARIFY UNRESOLVED_VALUE:<DIM>` (R3-4); `DELTA_CONTRADICTS_CARRIED` (`M2A_DESIGN_v2`) sigue sustituido por F16/F17 |
| `<DIM>` limitado a las dimensiones de ranura | este contrato, A.3, antes de R3 | A.3: claves de dimensión de las ranuras de §4.2, incluida `UNIVERSE` (R3-3) |
| «byte-idéntico» sin excepción | este contrato, §6.3 / INV-M2A-02, antes de R3 | §6.3: excepción de `request_ids` al estrechar un ancla dual (R3, P-1) |

### C.2 Lo que sigue vigente de ADR-103 Enm. 4 D7

Alcance por exclusión; referentes (T3.6/A6); aclaración pendiente (A7); relativos temporales (OQ-3 = b); VALUE por lado
salvo la viñeta sustituida; unidad heredada única, SMMLV incluida; recálculo SMMLV (A2); sin entrega a legacy con ancla
Foundation; persistencia; eco y hashes; tensiones abiertas (OQ-5). Este contrato los precisa sin cambiarlos.

## Anexo D — Registro de enmiendas

### D.1 Enmienda R3 (usuario, 2026-10-02): representabilidad

**Motivo.** Decisión del usuario: «STOP-2 revela un hueco del contrato: no existe una respuesta representable para un
estado que sus propios tipos permiten alcanzar». Barrido generado de los tipos (`tests/test_m2a_contract_r3_sweep.py`;
propuesta y evidencia en `docs/audits/m2a_conversation/M2A_CONTRACT_ENM_R3_PROPOSAL.md`). Principio que se conserva:
el reducer nunca arregla carencias de otras capas inventando semántica. Una capacidad aguas abajo sólo se registra
(§19).

**Cambios.**

| Id | Dónde | Qué |
|---|---|---|
| R3-1 | §6.7, §8.1, A.3 | `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`, decidida sobre el estado resultante (D-2); su pendiente (D-1) |
| R3-2 | §6, A.3 | cada `DeltaRefusal` tiene un código de A.3; entran `DELTA_SLOT_CONFLICT` y `KEEP_ON_EMPTY_SLOT` |
| R3-3 | A.3, §19 | `<DIM>` recorre las claves de dimensión de las ranuras de §4.2, incluida `UNIVERSE`; SF075 registrado como dependencia |
| R3-4 | §6.5, §19 | rama fail-closed de F16 ⇒ `OP_NOT_ACCREDITED:<DIM>` ⇒ `UNRESOLVED_VALUE:<DIM>` (D-3: sin código nuevo) |
| R3-5 | §18, §20.2 | INV-M2A-25, invariante mayor: toda respuesta alcanzable es representable |
| P-1 | §6.3, INV-M2A-02 | excepción de `request_ids` al estrechar un ancla dual |
| P-2 | §19 G-FU-14 | precisión del «11» (deuda de `M2A0_CLOSE.md` §8) |
| D-5 | §19 | E-ORDER registrado como dependencia |
| P-3 | §6.5 F16 | F16 sólo invierte un positivo arrastrado; +X y −X en el mismo parche caen en la rama fail-closed de R3-4 (U18) |
| U19 | §19 | una autoridad para las dependencias: `Id` por fila y extracción generada y sellada (`gold/CONTRACT_S19_DEPENDENCIES.json`) |
| D-6 | §1.2, §7.1 fila 4b, A.1, A.3 | `OUT_UNSUPPORTED_PROPOSAL` / `INFORM UNSUPPORTED_PROPOSAL:<motivo>`, genérica, con lista cerrada de motivos y dueño por motivo |
| — | §20.4 | congelación tras R3 |
| D-4 | cabecera, §20.1, Anexo B | release r3: SF192–SF195 (U3), etiquetado ciego y re-sellado único |

**Estado del sello.** Sellado como release r3 en el paso B4 (2026-10-02): un único re-sellado, después del etiquetado
ciego y de la adjudicación. Registro en `gold/ADJUDICATION_R3.md`.

**Lado del tipo.** `CLARIFY_CODES`, el `<DIM>` con `UNIVERSE`, `OUT_UNSUPPORTED_PROPOSAL` (con su resolución `INFORM`)
y `UNSUPPORTED_PROPOSAL:<motivo>` faltan en `app/core/foundation_accepted.py`. Son de M2A-3 (fase 1c, tras el
re-sellado). El barrido los marca como rojo declarado (xfail estricto).

### D.2 Puntos de revisión del usuario: RESUELTOS (2026-10-02)

1. **D-1: RESUELTO con la opción 1** (propuesta del orquestador, aprobada por el usuario el 2026-10-02).
   - La pendiente de R3-1 lleva C1 «quitar orden y límite» (`CLEAR(ORDER)` + `CLEAR(CARDINALITY)`) y un `SET_ORDER`
     por cada criterio de orden alternativo del dataset, resueltos por la autoridad ORDER al preguntar, sin el criterio
     vigente, con `answer_slots = {ORDER, CARDINALITY}` (§6.7). Mientras no exista E-ORDER, sólo C1 y la cancelación
     (§6.7, §19).
   - §8.3 fila 4 conserva su texto sellado: el protocolo general de pendientes no cambia (es terreno de M2A-4).
   - **Antecedente (cláusula retirada).** El borrador de B1 (`cb32361a`) había escrito `answer_slots = {ORDER,
     CARDINALITY}`, porque §8.1 y el tipo exigen ⊇ y la decisión original `{ORDER}` no se podía representar. Para que
     «dar otro criterio» fuera compatible con un candidato presente, añadió a §8.3 fila 4 la cláusula «sólo fijan con
     valor ranuras de `answer_slots` que ningún candidato fija con valor».
   - **Por qué se retiró.** Esa cláusula dejaba pasar «dame 20» como respuesta compatible, y
     `compose(frame, SET CARDINALITY 20)` conserva el ORDER de la base: se ejecutaría «top 20 por mayor valor» justo
     después de que el usuario pidió quitar el orden. Era una fuga semántica silenciosa. Con candidatos `SET_ORDER`
     explícitos, la fila 4 sellada basta: «por fecha» selecciona su candidato y «dame 20» no selecciona ninguno
     (fila 7, `REASK ANSWER_UNRESOLVED`).
2. **D-6: RESUELTO, aprobado tal como se propuso** (usuario, 2026-10-02).
   - Fila y código: `OUT_UNSUPPORTED_PROPOSAL` / `INFORM UNSUPPORTED_PROPOSAL:<motivo>`; 0 llamadas, ancla y pendiente
     intactas.
   - Motivos y dueños: `MULTI_REQUEST` → M2b; `UNSUPPORTED_CONDITION` → extensión o versionado del lenguaje;
     `UNSUPPORTED_SOURCE` → contrato M1; `INVALID_PROPOSAL` → productor (M2A-5); `NO_ACTION` → contrato M2a (sólo sin
     pendiente).
   - Exclusiones: un universo no dicho (Q-R8-3 de M2A-3 sigue abierta) y `UNACCREDITED_MODALITY_FAMILY` (va por §6.1).
   - Por qué `INFORM` y no `CLARIFY`: no hay ranura que preguntar, y M1 trata esos estados como mensajes terminales.

### D.3 Enmienda R3b (usuario, 2026-10-02): reparación del NO_GO de ZEUS sobre `4cf1cad3`

Sigue siendo R3 y aún no está en `main`; queda un único re-sellado final (R3b-2).

| Id | Hallazgo de ZEUS | Dónde | Qué |
|---|---|---|---|
| U20 | P1-1 (fondo): pendientes imposibles | §6 paso 3b, H1; §8.4; §10.1; A.3; INV-M2A-26 | ley de la pendiente posible; precedencia cerrada de causas antes de H1; varias causas ⇒ `INFORM` del código de mayor precedencia (el mensaje nombra todas); sobre un ancla vencida sólo `CONTEXT_EXPIRED_CONFIRM` si «continuar» compone y ejecuta; códigos de `CLARIFY` válidos con `INFORM` (lista cerrada) |
| U20 | P1-2 (forma): «dame 20» | §6.7 | mientras IR v2 rechace un `FOLLOWUP_DELTA` sólo con CARDINALITY: `INFORM UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL` por §7.1 fila 4b, pendiente intacta |
| U23 | P1-3 (forma): §19, única autoridad de dependencias | §19 | ids explícitos (también `[]`), conteo verificable, dueño de lista cerrada, `impacto_m2a3` = `LECTURA` / `COMPOSICION`; E-ORDER enumera sus casos; SF166/SF168/R0303 no son dependencia (H3 puro); alternativas parciales de SF192 (`DEP-E-ORDER-CANDIDATES`) |
| U22 | P2: `KEEP` + `SET` | §6 pasos 2 y 3b | `DELTA_SLOT_CONFLICT` aunque los valores coincidan |
| U22 | P2: selección de candidatos | §8.3 «Selección» | por identidad semántica (op, ranura, valor canónico / destino); se compone el parche completo; una edición de otra ranura ⇒ fila 5 |
| U22 | P2 / P3: alcance de `REASK` | §6, códigos de rechazo | vuelve a «`REASK` si había pendiente» (R3-2) |
| U21 | gate multi-op | §18 INV-M2A-25 | barrido exhaustivo por parejas + Hypothesis con N operaciones e invariancia a la permutación, sobre un modelo simbólico; sobre el `compose` real, gate de M2A-3 fase 2 |
| U21 | precisiones que forzó el barrido multi-op: **DECIDIDAS** (usuario, 2026-10-02, U25) | §6 pasos 2 y 3b, H1; §7.1 fila 4b; §8.3 «Selección»; §8.4; §10.1 | (a) `KEEP` + `REPLACE_UNIVERSE` del universo es conflicto; (b) el `CLEAR` de una dimensión entera cubre cada una de sus ranuras; (c) dos operaciones no acreditadas son dos causas ⇒ `INFORM`; (d) la rama de A8.1 con unidades no comparables va en la precedencia 4; (e) H1 exige que «continuar» pase H3–H12 y las dependencias de §19; (f) §7.1 fila 6 con pendiente vigente o ancla vencida ⇒ `INFORM` sin nueva pendiente (`frame` = ancla + resto del turno); (g) la fila 4b aplica también con el ancla vencida; (h) en H11 el `frame` es la base; (i) la selección semántica no aplica a la fila 1 (evento) |
| U25 | cierre de R3b (usuario, 2026-10-02) | §6 (tabla de rechazos), §7.1 fila 4b, §19, §1.1, Anexo B | `VALUE_WITHOUT_COMPARATOR` pasa tal cual por la tabla rechazo→A.3 (`OP_NOT_ACCREDITED:VALUE` ⇒ `CLARIFY VALUE_WITHOUT_COMPARATOR`); `no_selection` sin pendiente ⇒ 4b `UNSUPPORTED_PROPOSAL:UNSUPPORTED_CONDITION`; la tabla de huecos G-FU queda sin `impacto_m2a3`; SF195 `SET_ORDER` = `POSITIVE`; SF196–SF201 adjudicados y sellados (SF199 = lectura de B; SF202 retirado); `M2A_GOLD` = 209 |
| U26 | R3c, último arreglo (ZEUS NO_GO sobre `7b30b221`) | §8.2, §8.3 «Selección», §8.4, §10.1, §11, §19, A.3, Anexo B | vencimiento pegajoso por el eco `CONTEXT_EXPIRED` del último registro; selección por coincidencia exacta tras quitar los `KEEP` de ranuras del `frame`; una pendiente exige una respuesta que ejecute (`VALUE_WITHOUT_COMPARATOR`: `answer_slots = {VALUE.lower, VALUE.upper}`); evento de opción de una pendiente que ya no existe ⇒ `INFORM STALE_OPTION`; plazo de la pendiente: 30 min desde su creación; §19: SF199 depende de M2A-5 y SF199–SF201 de E-ORDER. Gates de otro paso: respuestas a todas las pendientes (M2A-4), propiedades de varios turnos (M2A-6/8b por replay) |
| U24 | controles mínimos del Gold | Anexo B (release r3b) | dos causas a la vez ⇒ `INFORM`; `KEEP` + `SET` ⇒ `DELTA_SLOT_CONFLICT`; ancla vencida con causa ⇒ `INFORM` sin bucle; «dame 20» con pendiente; selección por clave semántica y edición de otra ranura. SF196–SF202 etiquetados a ciegas (A/B) desde `docs/audits/m2a_conversation/labeling_r3b/R3B_LABELER_INPUT.md`; SF196–SF201 sellados y SF202 retirado (U25) |

**Estado del sello.** R3b rompió el sello r3 de `4cf1cad3` a propósito. Se re-selló en R3b-2 (U25), tras el
etiquetado ciego y la adjudicación de los controles, y por última vez en R3c (U26); el `seal_sha256` está en el sello.
Tras R3c, un ZEUS que sólo encuentre P1 de forma se repara sin reabrir el diseño (barandilla del usuario, U26).

# M2A — Enmienda R3 del contrato conversacional: barrido y borradores (FASE A)

**Fecha:** 2026-10-02. **Rama:** `mission/m2a-contract-enm-r3` (base `fa0b6de8`, Paso 0 de la ola M2a). **Rol:** ARES.
> **Estado posterior (B1, 2026-10-02).**
>
> - El usuario aprobó R3-1…R3-5, P-1 y P-2, y decidió D-1…D-6 y la congelación de §20.4.
> - La enmienda aplicada y sus ajustes marcados para revisión están en el Anexo D del contrato. Las tablas de §2 son
>   las de la fase A; el barrido vigente lo imprime `python -m tests.test_m2a_contract_r3_sweep`.
> - El procedimiento de re-sellado está en `M2A_CONTRACT_ENM_R3_B4_RESEAL.md`.
> - **Este documento no es entrada para los etiquetadores:** asocia cada caso a su regla. La entrada es
>   `labeling_r3/R3_LABELER_INPUT.md`.

**Estado (fase A):** PROPUESTA. Este documento **no** edita el contrato (`docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`), el
Gold ni el sello: `m2a_gold_seal.py verify` sigue dando `seal OK bb2e9812…`. La fase B aplicará lo que el usuario
apruebe, con adjudicación independiente y **un único** re-sellado.

**Decisión que motiva el carril (usuario, 2026-10-02).** «STOP-2 revela un hueco del contrato: no existe una respuesta
representable para un estado que sus propios tipos permiten alcanzar». Se recupera `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`
(`docs/audits/m2a_advisory_2026-10-01/01_diseno_m2a/M2A_DESIGN_v2.md:240`): `CLEAR(ORDER)` con CARDINALITY vigente y no
retirada en el mismo parche ⇒ `CLARIFY`, base intacta. Nunca se borra CARDINALITY implícitamente.

**Principio que se conserva.** El reducer nunca arregla carencias de otras capas inventando semántica. Una capacidad
pendiente aguas abajo (p. ej. SF075, dependiente de `FOUNDATION_O1B_EXPLICIT_TIME_PARITY`) no es un hueco del contrato
y no se «arregla» en él: sólo se registra.

---

## 0. Resumen

- **Criterio, hecho operativo.** Una respuesta es *representable* si existe (i) un código de `DeltaRefusal` de §6, o
  una regla del host, que la produzca, y (ii) un código de A.3 que `PendingClarification` acepte como `reason`. Un
  `CLARIFY` siempre deja una pendiente (§5.4) y `AcceptedTurnRecord` no tiene otro campo de razón, así que un `CLARIFY`
  sin código admitido **no se puede escribir**: el tipo lanza `UNKNOWN_PENDING_REASON`.
- **Barrido generado del código: 27 filas.** CUBIERTO 12 · HUECO 7 · DEPENDENCIA 4 · NO_ALCANZABLE 4.
- **Los 7 HUECO se reducen a 4 propuestas**, cada una genérica y con origen:
  - R3-1: CARDINALITY sin ORDER (fila T08; cubre también T09 si llega a ser alcanzable);
  - R3-2: rechazos estructurales sin código en A.3 (T07, R01, R02);
  - R3-3: `<DIM>` no recorre UNIVERSE (S02, S03);
  - R3-4: la rama fail-closed de F16 no tiene código (S04).

  Además: R3-5 (invariante de representabilidad en §18), dos precisiones de forma (P-1, P-2) y el registro de
  dependencias en §19.
- **Candidatos del encargo.**
  - (a) HUECO confirmado → R3-1.
  - (b) NO ALCANZABLE hoy. La P es inválida en IR v2 (`interpretation_contract.py:438-439`, también con job
    `FOLLOWUP_DELTA`) y ningún evento fija CARDINALITY. Es condicionalmente alcanzable si M2A-5 relaja esa validación;
    entonces es el mismo estado de (a). R3-1 formulada **sobre el estado** lo cubre sin otro re-sellado (D-2).
  - (c) La capacidad es DEPENDENCIA (ya decidida por el usuario para SF075). El rechazo fail-closed es un HUECO de
    representación, distinto de la capacidad → R3-3, genérica y sin código ad hoc.
- **Gold.** Un caso obligatorio (SF192, R3-1) y tres opcionales (SF193 → R3-2, SF194 → R3-4, SF195 → control de D-2).
  Todos van sin etiqueta: la adjudicación es de otro agente, ciego al autor.

---

## 1. Método: generado del código, no a mano

**Herramienta:** `tests/test_m2a_contract_r3_sweep.py`. Vive en `tests/` porque el pin de consumo
(`tests/test_m2a_boundaries.py`) trata `scripts/` como externo y le prohíbe importar `foundation_accepted`. Regenera
las tablas de §2 con `python -m tests.test_m2a_contract_r3_sweep`; la salida es determinista (idéntica con
`PYTHONHASHSEED=0` y `=123`).

| Qué | De dónde sale |
|---|---|
| invariantes de la efectiva | AST de `AcceptedInterpretation.__post_init__` (y de las funciones del módulo a las que llama) y de `AcceptedRequest.__post_init__`. `TWO_CONDITIONS_IN_<ranura>` se expande con las ranuras de lado único del tipo. Se excluyen los `INVALID_<etiqueta>` de forma de los helpers genéricos: un `compose` bien tipado nunca los alcanza |
| ranuras | `SLOT_DIMENSIONS`, `POLARIZED_SLOT_DIMENSIONS`, `ValueSide` y `UNIVERSE` (§4.2): 30 |
| op × ranura | se **construye** cada `PatchOp` (5 × 30); decide el tipo |
| rechazos | la lista cerrada de `DeltaRefusal` se **parsea** de §6; A.3 se parsea del contrato y se contrasta con `CLARIFY_CODES` / `REASK_CODES` / `INFORM_CODES` |
| alcanzabilidad | sondas con los tipos reales: efectivas, `PatchOp`, `ProposedSlotDirective` e `InterpretationProposal` del wire |

**Pines.** Fijan el contrato de HOY. Las filas HUECO y `REPRESENTABLE_TODAY` se invierten adrede en la fase B, nunca en
silencio.

1. `test_every_type_invariant_is_swept`: todo invariante generado tiene fila. Un invariante nuevo en el tipo hace fallar
   el test hasta que se barre.
2. `test_the_op_slot_matrix_is_the_one_swept`: SET 19 · CLEAR 29 · KEEP 21 · SET_ORDER 1 · REPLACE_UNIVERSE 1.
3. `test_each_probe_reaches_exactly_the_swept_state`.
4. `test_the_refusal_list_and_annex_a3_are_the_ones_swept`: §6 y A.3 del contrato coinciden con las constantes del
   tipo.
5. `test_representability_is_today_what_the_sweep_says`.

**No vacuidad.** Corrí 6 mutantes en scratch y murieron los 6:

- una fila pierde un invariante;
- se inyecta un invariante nuevo en el AST;
- se cuela un código en `CLARIFY_CODES` sin pasar por el barrido (muere en los pines 4 y 5);
- `reason_code_allowed` admite `UNIVERSE` en silencio;
- una expectativa de sonda es falsa.

**Las dos líneas que se integrarán.** El test se validó también contra el `foundation_accepted.py` de la rama M2A-3 en
`f30e91ff` (fase 1b). Esa rama añade `MULTI_CONDITION_POLICY`, `NONCANONICAL_CONDITION_ORDER` y `SLOT_WITHOUT_RANK` y
retira `UNSORTED_POLICY_CONDITION_IDS`. Los 5 pines pasan en las dos líneas.

---

## 2. Barrido

Las tablas de esta sección las genera `python -m tests.test_m2a_contract_r3_sweep` en `fa0b6de8`; no se editan a mano. Las filas T11 y T12 barren invariantes que sólo existen en la rama M2A-3. Las columnas «Estado» y «Evidencia» son el análisis; los códigos, las ops y las sondas salen del código.

### Invariantes generados del AST

| Código | Constructor | Fila |
|---|---|---|
| `CARDINALITY_WITHOUT_ORDER` | `AcceptedInterpretation` | T08, T09 |
| `DUAL_ANCHOR_CONDITIONS_DIFFER` | `AcceptedInterpretation` | T02 |
| `DUPLICATE_CONDITION_ID` | `AcceptedInterpretation` | T04 |
| `DUPLICATE_REQUEST_ID` | `AcceptedInterpretation` | T01 |
| `EMPTY_REQUESTS` | `AcceptedInterpretation` | T01 |
| `FOREIGN_CONDITION_REQUEST` | `AcceptedInterpretation` | T03 |
| `INVALID_DUAL_ANCHOR` | `AcceptedInterpretation` | T01 |
| `LOGIC_ATOMS_MISMATCH` | `AcceptedInterpretation` | T04 |
| `NONCONJUNCTIVE_ORDER` | `AcceptedInterpretation` | T10 |
| `TOO_MANY_REQUESTS` | `AcceptedInterpretation` | T01 |
| `TWO_CONDITIONS_IN_CARDINALITY` | `AcceptedInterpretation` | T06 |
| `TWO_CONDITIONS_IN_ORDER` | `AcceptedInterpretation` | T05 |
| `TWO_CONDITIONS_IN_VALUE_LOWER` | `AcceptedInterpretation` | T07 |
| `TWO_CONDITIONS_IN_VALUE_UPPER` | `AcceptedInterpretation` | T07 |
| `UNSORTED_POLICY_CONDITION_IDS` | `AcceptedInterpretation` | T04 |
| `VIEW_REQUIRED` | `AcceptedRequest` | T13 |
| `VIEW_TYPE_UNAVAILABLE` | `AcceptedRequest` | T13 |

### Op × ranura (construido con `PatchOp`)

| Op | Ranuras admitidas | Rechazos del tipo |
|---|---|---|
| `SET` | 19: ACTOR/POSITIVE, ACTOR/NEGATIVE, BUYER/POSITIVE, BUYER/NEGATIVE, CARDINALITY, CONTRACT_TYPE_OR_NATURE/POSITIVE, CONTRACT_TYPE_OR_NATURE/NEGATIVE, GEOGRAPHY/POSITIVE, GEOGRAPHY/NEGATIVE, MODALITY/POSITIVE, MODALITY/NEGATIVE, STATE/POSITIVE, STATE/NEGATIVE, TIME/POSITIVE, TIME/NEGATIVE, TOPIC/POSITIVE, TOPIC/NEGATIVE, VALUE/lower, VALUE/upper | `INVALID_PATCH_CONDITION` ×1, `ORDER_NEEDS_SET_ORDER` ×1, `PATCH_CONDITION_OUTSIDE_SLOT` ×9 |
| `CLEAR` | 29: ACTOR/POSITIVE, ACTOR/NEGATIVE, ACTOR, BUYER/POSITIVE, BUYER/NEGATIVE, BUYER, CARDINALITY, CONTRACT_TYPE_OR_NATURE/POSITIVE, CONTRACT_TYPE_OR_NATURE/NEGATIVE, CONTRACT_TYPE_OR_NATURE, GEOGRAPHY/POSITIVE, GEOGRAPHY/NEGATIVE, GEOGRAPHY, MODALITY/POSITIVE, MODALITY/NEGATIVE, MODALITY, ORDER, STATE/POSITIVE, STATE/NEGATIVE, STATE, TIME/POSITIVE, TIME/NEGATIVE, TIME, TOPIC/POSITIVE, TOPIC/NEGATIVE, TOPIC, VALUE/lower, VALUE/upper, VALUE | `UNIVERSE_NEEDS_REPLACE_UNIVERSE` ×1 |
| `KEEP` | 21: ACTOR/POSITIVE, ACTOR/NEGATIVE, BUYER/POSITIVE, BUYER/NEGATIVE, CARDINALITY, CONTRACT_TYPE_OR_NATURE/POSITIVE, CONTRACT_TYPE_OR_NATURE/NEGATIVE, GEOGRAPHY/POSITIVE, GEOGRAPHY/NEGATIVE, MODALITY/POSITIVE, MODALITY/NEGATIVE, ORDER, STATE/POSITIVE, STATE/NEGATIVE, TIME/POSITIVE, TIME/NEGATIVE, TOPIC/POSITIVE, TOPIC/NEGATIVE, VALUE/lower, VALUE/upper, UNIVERSE | `DIMENSION_WIDE_SLOT_OUTSIDE_CLEAR` ×9 |
| `SET_ORDER` | 1: ORDER | `INVALID_PATCH_CONDITION` ×1, `PATCH_CONDITION_OUTSIDE_SLOT` ×9, `SET_ORDER_OUTSIDE_ORDER` ×19 |
| `REPLACE_UNIVERSE` | 1: UNIVERSE | `REPLACE_UNIVERSE_OUTSIDE_UNIVERSE` ×29 |

### Barrido

| Fila | Estado | Op × ranura | ¿Alcanzable? | Respuesta hoy (DeltaRefusal → A.3) | Veredicto | Evidencia | Propuesta |
|---|---|---|---|---|---|---|---|
| T01 | efectiva sin peticiones, con ids repetidos, >2 o dual inválida (`EMPTY_REQUESTS`, `DUPLICATE_REQUEST_ID`, `TOO_MANY_REQUESTS`, `INVALID_DUAL_ANCHOR`) | REPLACE_UNIVERSE/UNIVERSE | NO | — | NO_ALCANZABLE | UniverseTarget es un único (sujeto, clase) y §5.2 deja la petición nombrada; ningún op crea ni quita peticiones (PatchOp no tiene campo de petición) |  |
| T02 | ancla dual con una condición nueva en una sola petición (`DUAL_ANCHOR_CONDITIONS_DIFFER`) | SET, SET_ORDER sobre ancla dual | sólo si compose acotara la condición | — | CUBIERTO | §5.1: PatchOp y Slot no llevan petición; compose da a la condición nueva todas las peticiones de la base (sonda P-dual-set). Una P que reparte condiciones por petición es MULTI (§1.2): ver O-3 |  |
| T03 | dual → una petición sin estrechar request_ids (`FOREIGN_CONDITION_REQUEST`) | REPLACE_UNIVERSE con ancla dual | SÍ, si se aplica §6.3 al pie de la letra | EXECUTE + UNIVERSE_CHANGED | CUBIERTO | §5.2 «se queda con el nombrado» exige reescribir request_ids de lo arrastrado (sonda P-dual-replace); §6.3/INV-M2A-02 lo prohíben literalmente («byte-idéntico»). M2A-3 ya aplica la excepción R5 | P-1 |
| T04 | ids, átomos de lógica y forma canónica (`DUPLICATE_CONDITION_ID`, `LOGIC_ATOMS_MISMATCH`, `UNSORTED_POLICY_CONDITION_IDS`) | todos | sólo por defecto de construcción | — | CUBIERTO | obligación de construcción de compose (§4.3 determinismo; forma canónica de 1a.1) |  |
| T05 | dos ORDER en una petición (`TWO_CONDITIONS_IN_ORDER`) | SET_ORDER/ORDER | NO | — | NO_ALCANZABLE | SET_ORDER sustituye; dos SET_ORDER en un parche no caben: la P los rechaza (IR v2 MULTIPLE_ORDER) y un evento lleva una op; si ocurriera, DELTA_SLOT_CONFLICT (R01) |  |
| T06 | dos CARDINALITY en una petición (`TWO_CONDITIONS_IN_CARDINALITY`) | SET/CARDINALITY | NO | — | NO_ALCANZABLE | SET sustituye; IR v2 MULTIPLE_CARDINALITY rechaza la P |  |
| T07 | dos cotas VALUE del mismo lado (`TWO_CONDITIONS_IN_VALUE_LOWER`, `TWO_CONDITIONS_IN_VALUE_UPPER`) | SET/VALUE.lower ×2 o SET/VALUE.upper ×2 en un parche | SÍ (la P con dos cotas del mismo lado es IR v2 válida: sonda P-two-lower) | DELTA_SLOT_CONFLICT → ninguno | HUECO | §6.2 rechaza con DELTA_SLOT_CONFLICT; §6 manda «CLARIFY con el código de la autoridad», que no existe para un rechazo estructural; reason_code_allowed lo rechaza | R3-2 |
| T08 | (a) CLEAR(ORDER) con CARDINALITY arrastrada (`CARDINALITY_WITHOUT_ORDER`) | CLEAR/ORDER (directiva CLEAR del wire 5.1 o evento «Quitar») | SÍ (sonda P-a) | ninguno → ninguno | HUECO | ningún DeltaRefusal lo nombra y A.3 no tiene código; conservar CARDINALITY la inventa sin ORDER, quitarla viola INV-M2A-16 (STOP-2 de M2A-3) | R3-1 |
| T09 | (b) SET CARDINALITY sin ORDER en base ni SET_ORDER (`CARDINALITY_WITHOUT_ORDER`) | SET/CARDINALITY | NO hoy: la P es IR v2 inválida (sonda P-b-wire); ningún evento fija CARDINALITY | — | NO_ALCANZABLE | condicional: si M2A-5 relaja CARDINALITY_WITHOUT_ORDER para FOLLOWUP_DELTA, es el mismo estado de (a); R3-1 formulada sobre el estado lo cubre sin re-sellar (D-2) | R3-1 |
| T10 | ORDER dentro de OR/NOT (`NONCONJUNCTIVE_ORDER`) | SET_ORDER; CLEAR/SET/KEEP sobre átomos de un OR/NOT | SÍ (objetivos dentro de OR/NOT) | TARGET_NOT_ADDRESSABLE → TARGET_NOT_ADDRESSABLE | CUBIERTO | §4.2; SET_ORDER añade un átomo del AND de primer nivel (sonda P-order-or) |  |
| T11 | política que cita una condición quitada (rama M2A-3 30d073a4..f30e91ff, no en la base fa0b6de8) (`FOREIGN_POLICY_CONDITION`) | CLEAR/SET de una condición citada | SÍ | OP_NOT_ACCREDITED:<DIM> → UNRESOLVED_VALUE:<DIM> | CUBIERTO | §6.6: la política se va con su condición o se re-decide; sin dueño, fail-closed con la dimensión de la condición citada (ranura). Excepción O1B: cita RETRIEVAL_INTENT (no es ranura) → S02 |  |
| T12 | forma canónica de políticas, condiciones y lógica (rama M2A-3 30d073a4..f30e91ff, no en la base fa0b6de8) (`CONFLICTING_POLICY_EFFECT`, `DUPLICATE_POLICY`, `UNSORTED_POLICIES`, `NONCANONICAL_LOGIC`, `MULTI_CONDITION_POLICY`, `NONCANONICAL_CONDITION_ORDER`, `SLOT_WITHOUT_RANK`) | todos | sólo por defecto de construcción | — | CUBIERTO | canonical_interpretation es el único constructor (M2A3_CANONICAL_FORM.md, decisiones D-CANON-1/2 del usuario); main emite políticas de una condición y compose nunca las fusiona |  |
| T13 | universo no dicho / vista derivada (F9) (`VIEW_TYPE_UNAVAILABLE`, `VIEW_REQUIRED`) | REPLACE_UNIVERSE; recálculo de vista | NO hoy (UniverseTarget prohíbe unspecified; la base no puede serlo) | — | DEPENDENCIA | ola F (HostViewDecision), ya declarada en §19 «vista derivada» |  |
| S01 | REPLACE_UNIVERSE a oportunidades (SF075): la CAPACIDAD | REPLACE_UNIVERSE/UNIVERSE | SÍ | — | DEPENDENCIA | re-decidir O1B/opportunity-explicit-state (no representables: citan RETRIEVAL_INTENT) y paridad de TIME explícito (foundation_projection.py:235-237 UNSUPPORTED_OPPORTUNITY_OVERRIDE); dueño FOUNDATION_O1B_EXPLICIT_TIME_PARITY (+ M2A-1b) | registro §19 |
| S02 | REPLACE_UNIVERSE a oportunidades: el RECHAZO fail-closed | REPLACE_UNIVERSE/UNIVERSE | SÍ (el tipo admite la efectiva: sonda P-c) | OP_NOT_ACCREDITED:UNIVERSE → UNRESOLVED_VALUE:UNIVERSE (rechazado) | HUECO | <DIM> sólo recorre SLOT_DIMENSIONS; UNIVERSE es ranura (§4.2) pero no dimensión: la pendiente no se puede construir (UNKNOWN_PENDING_REASON) | R3-3 |
| S03 | primer turno o búsqueda nueva que pregunta por conflicto de universo (H7) | — (P sin base) | SÍ (foundation_runtime.py:290-292 HOST_UNIVERSE_RULE_CONFLICT) | → UNRESOLVED_VALUE:UNIVERSE (rechazado) | HUECO | §7.2 (c) y primer turno dejan una pendiente; la reducción M1 → UNRESOLVED_VALUE:<DIM> es la del esquema Gold 2.1 (O-2) y para el universo no es representable | R3-3 |
| S04 | F16 fail-closed: negativo sobre varios positivos o valor contenido | SET/<DIM>/NEGATIVE con ≥2 positivos en la ranura | SÍ (el tipo admite la efectiva: sonda P-f16) | «CLARIFY (G-FU-09)» sin código | HUECO | §6.5 prescribe CLARIFY sin código; A.3 no tiene ninguno | R3-4 |
| S05 | REPLACE_UNIVERSE con STATE/MODALITY arrastrado inexistente en el destino | REPLACE_UNIVERSE | SÍ | H8 → STATE_NOT_IN_UNIVERSE | CUBIERTO | §6 H8 (SF079); MODALITY depende de E-MODALITY con el mismo código |  |
| S06 | CLEAR que deja la efectiva sin condiciones / CLEAR de ranura vacía | CLEAR | SÍ | H11 EMPTY_SEARCH_AFTER_DELTA / H12 INFORM CLEAR_ON_EMPTY_SLOT | CUBIERTO | con otra op, el CLEAR vacío no cambia nada (OQ-5 X8, tensión viva declarada, no hueco de representación) |  |
| R01 | rechazo DELTA_SLOT_CONFLICT (dos SET en una ranura; KEEP+CLEAR; dos REPLACE_UNIVERSE) | SET×2, KEEP+CLEAR, REPLACE_UNIVERSE×2 | SÍ | DELTA_SLOT_CONFLICT → ninguno | HUECO | incluye la unión de valores de G-FU-09 y «procesos y contratos» sobre un ancla simple | R3-2 |
| R02 | rechazo KEEP_ON_EMPTY_SLOT | KEEP | SÍ (directiva KEEP del wire 5.1: sonda P-keep) | KEEP_ON_EMPTY_SLOT → ninguno | HUECO | §6.1 lo rechaza; A.3 no tiene código | R3-2 |
| R03 | rechazo TARGET_NOT_ADDRESSABLE | CLEAR/SET/KEEP | SÍ | → TARGET_NOT_ADDRESSABLE | CUBIERTO | homónimo en A.3 |  |
| R04 | rechazo OP_NOT_ACCREDITED:<DIM> sobre una dimensión de ranura | SET/SET_ORDER | SÍ | → UNRESOLVED_VALUE:<DIM> | CUBIERTO | 11 dimensiones de ranura admitidas por reason_code_allowed |  |
| R05 | rechazo BASE_INVALID | todos | defensivo (§7.1 fila 1 decide antes) | → NO_ANCHOR / ANCHOR_UNREADABLE | CUBIERTO | §3.8 y §7.1 fila 1 |  |
| F01 | G-FU-05 paginar ancla dual / página frente a top-N | evento de página (M2A-6) | SÍ (M2A-6) | CLARIFY, código diferido al dueño | DEPENDENCIA | §19 ya declara «Salida: regla y código de razón»; con R3-3 existe forma representable (SF148, FRONTIER, ya usa UNRESOLVED_VALUE:UNIVERSE) |  |
| F02 | G-FU-07 territorio contenido / comprador nacional | SET GEOGRAPHY/BUYER | sólo con E-GEO/E-BUYER | código de la autoridad (UNRESOLVED_VALUE:<GEOGRAPHY\|BUYER>) | DEPENDENCIA | detección aguas abajo |  |
| F03 | G-FU-04 filas/página sin result_set_ref | protocolo | SÍ (M2A-6) | INFORM, código diferido (A.3 lo declara) | CUBIERTO | INFORM no deja pendiente ni se persiste su razón; el gate es «INFORM y 0 llamadas» (§9.1) |  |

Totales: CUBIERTO 12, HUECO 7, DEPENDENCIA 4, NO_ALCANZABLE 4

### Sondas

| Sonda | Resultado del tipo |
|---|---|
| P-a | `CARDINALITY_WITHOUT_ORDER` |
| P-a-SF023 | admitido |
| P-a-set-order | admitido |
| P-b | `CARDINALITY_WITHOUT_ORDER` |
| P-b-wire | `CARDINALITY_WITHOUT_ORDER` |
| P-b-wire-rank | admitido |
| P-two-lower | `TWO_CONDITIONS_IN_VALUE_LOWER` |
| P-two-lower-wire | admitido |
| P-dual-set | `DUAL_ANCHOR_CONDITIONS_DIFFER` |
| P-dual-replace | `FOREIGN_CONDITION_REQUEST` |
| P-dual-replace-narrowed | admitido |
| P-order-or | `NONCONJUNCTIVE_ORDER` |
| P-order-and | admitido |
| P-c | admitido |
| P-c-target | admitido |
| P-f16 | admitido |
| P-keep | admitido |
| P-clear-order | admitido |

### Representabilidad hoy (`PendingClarification.reason`)

| Código | ¿Representable? |
|---|---|
| `TARGET_NOT_ADDRESSABLE` | sí |
| `NO_ANCHOR` | sí |
| `ANCHOR_UNREADABLE` | sí |
| `STATE_NOT_IN_UNIVERSE` | sí |
| `EMPTY_SEARCH_AFTER_DELTA` | sí |
| `UNRESOLVED_VALUE:ACTOR` | sí |
| `UNRESOLVED_VALUE:BUYER` | sí |
| `UNRESOLVED_VALUE:CARDINALITY` | sí |
| `UNRESOLVED_VALUE:CONTRACT_TYPE_OR_NATURE` | sí |
| `UNRESOLVED_VALUE:GEOGRAPHY` | sí |
| `UNRESOLVED_VALUE:MODALITY` | sí |
| `UNRESOLVED_VALUE:ORDER` | sí |
| `UNRESOLVED_VALUE:STATE` | sí |
| `UNRESOLVED_VALUE:TIME` | sí |
| `UNRESOLVED_VALUE:TOPIC` | sí |
| `UNRESOLVED_VALUE:VALUE` | sí |
| `UNRESOLVED_VALUE:UNIVERSE` | NO |
| `DELTA_SLOT_CONFLICT` | NO |
| `KEEP_ON_EMPTY_SLOT` | NO |
| `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` | NO |
| `UNRESOLVED_VALUE:RETRIEVAL_INTENT` | NO |
| `UNRESOLVED_VALUE:UNSUPPORTED` | NO |
| `UNRESOLVED_VALUE:CONTROL_LANGUAGE` | NO |

### Adjudicación de los tres candidatos del encargo

**(a) `CLEAR(ORDER)` con CARDINALITY arrastrada: HUECO DEL CONTRATO (T08).**

- **Es alcanzable.** La directiva `CLEAR` sobre ORDER existe en el wire 5.1 de M2A-1a (sonda P-clear-order) y §5.3
  prevé el evento «Quitar: X». El tipo admite una base con ORDER + CARDINALITY, y es la de preestados ya sellados
  (SF018, SF019, SF023, SF024, SF172).
- **El resultado es inválido.** Por §6.2/§6.3 la efectiva arrastra la CARDINALITY byte-idéntica y sin ORDER. El tipo la
  rechaza con `CARDINALITY_WITHOUT_ORDER` (sonda P-a).
- **Hoy no hay respuesta.** Ningún `DeltaRefusal` la nombra y A.3 no tiene código. `UNRESOLVED_VALUE:CARDINALITY` sí
  pasa `reason_code_allowed`, pero §6 lo reserva como código «de la autoridad», y aquí la autoridad sí resolvió: usarlo
  sería reinterpretarlo. Conservar la CARDINALITY la ejecutaría sin ORDER (§6.7); quitarla viola INV-M2A-16.
- **Controles.** `CLEAR(ORDER)` + `CLEAR(CARDINALITY)` (SF023) da una efectiva válida (sonda P-a-SF023), y también
  `CLEAR(ORDER)` + `SET_ORDER` con la CARDINALITY arrastrada (sonda P-a-set-order). Este segundo control es el que
  decide D-2.

**(b) `SET CARDINALITY` sin ORDER en la base ni `SET_ORDER` en el parche: NO ALCANZABLE hoy (T09).**

- `main` no tiene autoridad ORDER/CARDINALITY de primer turno: la admisión deja `UNRESOLVED_HOST_DIMENSION:ORDER`
  (`tests/test_foundation_ir_v2.py:303-308`). Lo que `main` garantiza es el tipo de la propuesta:
  `_validate_order_and_cardinality` (`app/core/interpretation_contract.py:425-439`) rechaza una petición con CARDINALITY
  y sin ORDER, salvo en `RANK_SUPPLIERS`.
  - Sonda P-b-wire: la P «dame solo 10» con job `FOLLOWUP_DELTA` ⇒ `CARDINALITY_WITHOUT_ORDER`.
  - Sonda P-b-wire-rank (control): con `RANK_SUPPLIERS`, admitida.
- ADR-103 Enm. 1 sólo da CARDINALITY con un superlativo (singular ⇒ ORDER + 1) o con una cifra junto a un orden («los 10
  más caros»). Ningún evento de §5.3 fija CARDINALITY, y ninguna regla H crea candidatos de CARDINALITY.
- **Consecuencia para M2A-5, no para el contrato** (O-7). Con esa validación, «dame sólo 10» sobre un ancla **ordenada**
  (un estado válido) tampoco se puede expresar en el wire. Si M2A-5 la relaja para `FOLLOWUP_DELTA`, (b) pasa a ser
  alcanzable y es exactamente el estado de (a). Por eso R3-1 se propone sobre el **estado resultante** y no sobre la op
  (D-2).

**(c) `REPLACE_UNIVERSE` a oportunidades (SF075): la CAPACIDAD es DEPENDENCIA (S01); el RECHAZO es HUECO de
representación (S02).**

- **La capacidad no es del reducer.** Hay que re-decidir O1B / `opportunity-explicit-state`, que no son representables
  en M2a: citan la condición REQUESTED_UNIVERSE, de dimensión `RETRIEVAL_INTENT`, que no es ranura. Falta además la
  paridad de TIME explícito sobre oportunidades (`foundation_projection.py:235-237`,
  `UNSUPPORTED_OPPORTUNITY_OVERRIDE`).
  - El usuario ya la declaró rojo con dueño `FOUNDATION_O1B_EXPLICIT_TIME_PARITY`: rama M2A-3, `M2A3_PHASE1_CLOSE.md`
    §5 (`f30e91ff`); carril 8a0, `APPLICABILITY_DECISIONS.json` (sin commit en su worktree). Aquí sólo se registra (§19,
    R3-3).
- **El rechazo fail-closed no es representable.**
  - El tipo admite la efectiva (sondas P-c y P-c-target), así que la parada tiene que venir de §6.6:
    `OP_NOT_ACCREDITED:<DIM>` → `CLARIFY UNRESOLVED_VALUE:<DIM>`.
  - `reason_code_allowed` sólo acepta `<DIM>` ∈ `SLOT_DIMENSIONS`. UNIVERSE es ranura de §4.2 pero no dimensión ⇒
    `UNKNOWN_PENDING_REASON`.
- **La misma falta aparece en otros estados alcanzables.**
  - El primer turno o la búsqueda nueva que pregunta por un conflicto de universo (H7, `foundation_runtime.py:290-292`;
    fila S03).
  - El provisional de G-FU-05 (paginar un ancla dual, fila F01). Los dos etiquetadores de SF148 (FRONTIER) ya usaron
    `UNRESOLVED_VALUE:UNIVERSE`, y el ledger también
    (`docs/audits/m2a_advisory_2026-10-01/02_gold_followups/comparacion_final/build_slices.py:13`).

---

## 3. Borrador de la enmienda (NO aplicado)

Diff sobre `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md` en `fa0b6de8`. Hay una propuesta por hueco, con su origen.
Lo marcado **[D-n]** depende de una decisión de §7.

### R3-1 — `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` (T08; T09 si llega a ser alcanzable)

**Origen:** decisión del usuario del 2026-10-02 (STOP-2 de M2A-3) y `M2A_DESIGN_v2.md:240`.

§6, punto 7:

```diff
 7. **Nunca se ejecuta** un intervalo VALUE vacío o degenerado generado por un follow-up con la cota opuesta arrastrada
    (A8.1 lo sustituye o, con unidades no comparables, aclara), una efectiva con dos cotas en el mismo lado, ni una
-   CARDINALITY sin ORDER.
+   CARDINALITY sin ORDER. Si la efectiva compuesta dejaría en alguna petición una CARDINALITY sin ORDER (p. ej.
+   `CLEAR(ORDER)` con la CARDINALITY arrastrada y sin `CLEAR(CARDINALITY)` en el mismo parche), el parche se rechaza
+   con `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` y la base no cambia: nunca se borra la CARDINALITY sin cita
+   (INV-M2A-16) ni se inventa un ORDER. La pendiente lleva `ambiguous_span` = la cita que retira el ORDER, `frame` =
+   el resto del parche compuesto sobre la base, `answer_slots = {ORDER, CARDINALITY}` y [D-1] un candidato «quitar
+   orden y límite» (`CLEAR(ORDER)` + `CLEAR(CARDINALITY)`); «mantener» es `CANCEL_PENDING`.
```

- **[D-2] La regla se formula sobre el estado, no sobre la op.** Así cubre (b) si M2A-5 la hace alcanzable y no dispara
  con `CLEAR(ORDER)` + `SET_ORDER`, que es válido por F10. La forma literal («`CLEAR(ORDER)` con CARDINALITY no retirada»)
  sí dispararía en ese caso.
- **Lista de rechazos de §6 y A.3:** el código entra en ambas por R3-2.
- **Por qué la pendiente tiene esa forma (D-1).** `frame` tiene que ser una `AcceptedInterpretation` válida (§8.1), y
  «base menos ORDER» no lo es (`CARDINALITY_WITHOUT_ORDER`). Una respuesta abierta aplicada sobre `frame` no puede
  reponer el `CLEAR(ORDER)` original salvo que el usuario lo repita.
  - Con un candidato completo, basta el evento, como en F11 («continuar» / «reset»).
  - Con cero candidatos, la pregunta es pura y la respuesta tiene que traer los dos `CLEAR`.

### R3-2 — Todo `DeltaRefusal` tiene un código en A.3 (T07, R01, R02)

**Origen:** §6 manda «`CLARIFY` con el código de la autoridad», y ese código no existe para un rechazo estructural.

**Estados alcanzables hoy:**

- dos `SET` del mismo lado de VALUE: la P es IR v2 válida (sonda P-two-lower-wire), y `main` llega a ejecutar una P así
  en primer turno (O-1);
- dos `SET` en una ranura polarizada (la unión de valores de G-FU-09);
- `KEEP` + `CLEAR` de una misma ranura;
- dos `REPLACE_UNIVERSE` («procesos y contratos» sobre un ancla simple);
- `KEEP` de una ranura vacía (la directiva `KEEP` existe en el wire: sonda P-keep).

§6, párrafo de códigos:

```diff
-Códigos de rechazo (`DeltaRefusal`, cerrados): `DELTA_SLOT_CONFLICT`, `TARGET_NOT_ADDRESSABLE`, `KEEP_ON_EMPTY_SLOT`,
-`OP_NOT_ACCREDITED:<DIM>`, `BASE_INVALID`. Un rechazo produce `CLARIFY` con el código de la autoridad (p. ej.
-`UNRESOLVED_VALUE:<DIM>`) o `REASK` si había pendiente; nunca ejecuta.
+Códigos de rechazo (`DeltaRefusal`, cerrados): `DELTA_SLOT_CONFLICT`, `TARGET_NOT_ADDRESSABLE`, `KEEP_ON_EMPTY_SLOT`,
+`OP_NOT_ACCREDITED:<DIM>`, `BASE_INVALID`, `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`. Un rechazo nunca ejecuta. Si el
+parche era la respuesta a una pendiente vigente (§8.3 filas 1 y 4), produce `REASK ANSWER_UNRESOLVED` (fila 7); si no,
+`CLARIFY` con este código de A.3:
+
+| `DeltaRefusal` | `CLARIFY` (A.3) |
+|---|---|
+| `OP_NOT_ACCREDITED:<DIM>` | `UNRESOLVED_VALUE:<DIM>` (el código de la autoridad) |
+| `BASE_INVALID` | `NO_ANCHOR` o `ANCHOR_UNREADABLE` (§3.8, §7.1 fila 1) |
+| cualquier otro (estructural, sin autoridad) | el mismo código: `DELTA_SLOT_CONFLICT`, `TARGET_NOT_ADDRESSABLE`, `KEEP_ON_EMPTY_SLOT`, `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` |
```

- **A.3, `CLARIFY`:** se añaden `DELTA_SLOT_CONFLICT`, `KEEP_ON_EMPTY_SLOT` y `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`
  (`TARGET_NOT_ADDRESSABLE` ya está).
- **Precisión sin código nuevo:** la frase sobre la pendiente concreta el «`REASK` si había pendiente» vigente con el
  código que §8.3 fila 7 ya define.

### R3-3 — `<DIM>` recorre las claves de dimensión de las ranuras de §4.2, incluida UNIVERSE (S02, S03; habilita F01)

**Origen:**

- STOP-1 de M2A-3, opción (a) («código con dimensión de universo»), sin reinterpretar O1B como STATE, como decidió el
  usuario;
- la forma genérica que pide el encargo;
- la etiqueta que ya usaron los dos etiquetadores de SF148 (FRONTIER) y el ledger de G-FU-05.

A.3, debajo de la tabla:

```diff
+`<DIM>` (en `UNRESOLVED_VALUE:<DIM>` y en `OP_NOT_ACCREDITED:<DIM>` de §6) recorre las claves de dimensión de las
+ranuras de §4.2: las once dimensiones de ranura (`TOPIC` … `CARDINALITY`) y `UNIVERSE`. Nunca una dimensión que no es
+ranura (`UNSUPPORTED`, `RETRIEVAL_INTENT`, `CONTROL_LANGUAGE`). Es una forma, no un comportamiento: quién emite cada
+código lo dice su regla.
```

§19, tabla «Dependencias declaradas de casos del Gold M2a con otros carriles» (registro de S01; no arregla la
capacidad):

```diff
 | vista derivada | `HostViewDecision`, `decide_view` | ola F |
+| 1 (SF075: `REPLACE_UNIVERSE` a oportunidades con TIME arrastrado) | re-decidir O1B / `opportunity-explicit-state` sobre la efectiva (su representación es de M2A-1b) y TIME explícito sobre oportunidades (`foundation_projection.py:235-237`). Mientras falte, `compose` rechaza con `OP_NOT_ACCREDITED:UNIVERSE` ⇒ `CLARIFY UNRESOLVED_VALUE:UNIVERSE` y el caso es rojo declarado (§20.2) | `FOUNDATION_O1B_EXPLICIT_TIME_PARITY` (+ M2A-1b) |
```

Nota de implementación (fase B, no es texto del contrato): en `reason_code_allowed`, la cola de `UNRESOLVED_VALUE:`
pasa a ser `{d.value for d in SLOT_DIMENSIONS} | {"UNIVERSE"}`. La constante `_UNIVERSE_KEY` ya existe en el módulo.

### R3-4 — Código de la rama fail-closed de F16 (S04)

**Origen:** §6.5 F16 prescribe «fail-closed (`CLARIFY`, G-FU-09, M2b)» sin código. El estado es alcanzable con los
tipos (sonda P-f16): una base con dos TOPIC/POSITIVE y un `SET` TOPIC/NEGATIVE igual a uno de ellos es una efectiva
válida, y sólo la regla la detiene.

**Opción recomendada [D-3 = iii], sin código nuevo.** La operación no se puede acreditar en M2a porque falta el álgebra
de conjuntos (M2b). Es la vía que §6.1 ya usa, y la misma que M2A-3 aplica a las políticas sin dueño.

```diff
    - **F16**: `SET` negativo cuyo valor canónico es el **único** positivo vigente de la ranura invierte la polaridad →
-     `POLARITY_INVERTED`. Con varios positivos o valor contenido: fail-closed (`CLARIFY`, G-FU-09, M2b).
+     `POLARITY_INVERTED`. Con varios positivos o valor contenido: el `SET` no se acredita en M2a (G-FU-09, M2b):
+     `OP_NOT_ACCREDITED:<DIM>` ⇒ `CLARIFY UNRESOLVED_VALUE:<DIM>`, con la dimensión de la ranura.
```

§19, fila G-FU-09 (consecuencia de R3-2 y R3-4):

```diff
-| G-FU-09 | negación sobre conjuntos | 1 | `CLARIFY` (fail-closed) | M2b | álgebra de conjuntos por ranura |
+| G-FU-09 | negación sobre conjuntos | 1 | `CLARIFY` (fail-closed): `DELTA_SLOT_CONFLICT` para la unión (dos `SET` en una ranura); `UNRESOLVED_VALUE:<DIM>` para la negación sobre varios positivos o sobre un valor contenido (§6.5) | M2b | álgebra de conjuntos por ranura |
```

**Alternativa [D-3 = i]: recuperar `DELTA_CONTRADICTS_CARRIED`** (`M2A_DESIGN_v2.md:239-240`) para esa rama.

- A favor: el código nombra la causa, que es una regla del host y no una lectura de autoridad, y eso ayuda al eco y a la
  presentación.
- En contra: añade un código y desdice en parte una sustitución registrada. `M2A_DESIGN_v2_1.md:21` lo declaró
  sustituido por F16/F17, y su definición de origen («mismo valor canónico con polaridades opuestas») es más estrecha
  que la rama actual, que incluye el valor contenido. Exige además una fila nueva en C.1.

### R3-5 — Invariante de representabilidad (§18)

**Origen:** el criterio del usuario, convertido en regla permanente con verificación automática.

```diff
 | INV-M2A-24 | **Sin léxico nuevo en el host**: … | pin AST: … |
+| INV-M2A-25 | **Toda respuesta es representable**: cada `DeltaRefusal` de §6 y cada `CLARIFY`/`REASK` que prescribe este contrato tiene un código de A.3 que `PendingClarification` admite; ningún estado que los tipos permiten alcanzar queda sin respuesta | barrido generado de los tipos (`tests/test_m2a_contract_r3_sweep.py`): invariantes del AST, op × ranura construido, §6 y A.3 parseados; un invariante nuevo falla hasta barrerse |
```

### P-1 — Precisión de forma: `request_ids` al estrechar un ancla dual (T03)

§6.3 e INV-M2A-02 prohíben reescribir lo arrastrado, y §5.2 exige quedarse con la petición nombrada. Sin estrechar
`request_ids`, la efectiva es inválida (`FOREIGN_CONDITION_REQUEST`, sonda P-dual-replace); estrechada, es válida
(sonda P-dual-replace-narrowed). M2A-3 ya aplica esa excepción como R5. La precisión no cambia el comportamiento; sólo
quita una contradicción literal.

```diff
 3. **Arrastre por identidad (F1).** Lo que ninguna operación toca pasa **byte-idéntico** (valor, autoridad, políticas,
-   origen, procedencia). Nunca se re-resuelve una condición arrastrada.
+   origen, procedencia). Nunca se re-resuelve una condición arrastrada. Única excepción: `REPLACE_UNIVERSE` con ancla
+   dual reduce los `request_ids` de cada condición arrastrada a la petición que queda (§5.2); nada más cambia.
```

INV-M2A-02: «**Identidad de lo no tocado** (valor, autoridad, políticas, origen, procedencia; `request_ids` salvo
§6.3)».

### P-2 — Precisión de forma ya registrada como deuda: G-FU-14 (`M2A0_CLOSE.md` §8)

La deuda pedía precisarlo «en el próximo re-sellado (forma)». Este re-sellado es el siguiente.

```diff
-| G-FU-14 | referentes extensionales | 11 | `INFORM` (sin llamada) una vez exista la señal extensional del wire 5.1; …
+| G-FU-14 | referentes extensionales | 11 (7 extensionales: SF065–SF067, SF070–SF072, SF180; 4 re-consultas en disputa o ambiguas: SF068, SF069, R0081, R0192) | `INFORM` (sin llamada) sólo para lo que la señal extensional del wire 5.1 clasifique como extensional; …
```

### Arrastres, si entra al menos un caso (k = número de casos nuevos)

- **Cabecera:** `M2A_GOLD` = 49 REAL + (150 + k) SYNTHETIC = 199 + k. «release r3: copia sellada que difiere del
  candidato asesor por U1/U2/D1 y por las adiciones de R3, `gold/ADJUDICATION_<fecha>_R3.md`».
- **§20.1, gate 1:** «Los 199 casos» pasa a «Los 199 + k casos».
- **Anexo B:** viñeta «Release r3 (usuario, 2026-10-02): **U3** añade SF192 (R3-1) [y SF193–SF195 si se aprueban], con
  adjudicación independiente ciega al autor».
- **§19:** los provisionales no cambian, salvo G-FU-09 (R3-4) y G-FU-14 (P-2). G-FU-05 conserva su salida («regla y
  código de razón» del dueño): R3-3 sólo garantiza que existe una forma representable.
- **C.1:** sin filas nuevas con D-3 = iii. Con D-3 = i, una fila para `DELTA_CONTRADICTS_CARRIED`.

### Matriz de trazabilidad (borrador de filas nuevas)

| # | Regla (§) | Decisión | Casos Gold | Estructura | Gate futuro |
|---|---|---|---|---|---|
| T14 | CARDINALITY sin ORDER ⇒ rechazo y `CLARIFY`, base intacta (§6.7) | usuario 2026-10-02 (STOP-2); `M2A_DESIGN_v2.md:240` | SF192 (+ SF195, control) | `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` | M2A-3: INV-M2A-01, -16, -25; mutantes «conserva la CARDINALITY sin ORDER» y «la borra sin cita» |
| T15 | todo rechazo tiene código de A.3 (§6) | barrido R3 | (SF193) | tabla `DeltaRefusal` → A.3 | M2A-3/M2A-4: INV-M2A-25 |
| T16 | `<DIM>` incluye UNIVERSE (A.3) | barrido R3; STOP-1 | SF075 (rojo declarado) | `UNRESOLVED_VALUE:UNIVERSE` | M2A-3: INV-M2A-25; pin de tipo |
| M10 | rama fail-closed de F16 con código (§6.5) | barrido R3 | (SF194) | `OP_NOT_ACCREDITED:<DIM>` | M2A-3: INV-M2A-19, -25 |

En §8 (reconciliaciones A↔B), una fila más:

| Cuestión | A (`main`) | B (asesoría) | Resolución | Dónde |
|---|---|---|---|---|
| `CLEAR(ORDER)` con CARDINALITY | contrato v1: sin regla ni código | `M2A_DESIGN_v2.md:240` | recuperada (usuario, 2026-10-02) | §6.7 |

La fila T06 («M2A-3: `KEEP_ON_EMPTY_SLOT`») pasa a citar su código de A.3.

---

## 4. Borradores de casos sintéticos (Gold 2.1, SIN etiqueta)

**Regla de adjudicación propuesta.**

- Dos etiquetadores ciegos, A y B, que no ven al autor ni se ven entre sí, producen `label_A` y `label_B` con
  `kit_v2_2/SCHEMA_v2_1.md` y el contrato **enmendado**.
- Reciben sólo el bloque de entrada de abajo; nunca este documento.
- El usuario adjudica los desacuerdos. El registro lleva `adjudicated_by: ["U3"]`, y `primary_agreement` refleja si A y
  B coincidieron.
- Los campos a etiquetar son: `turn_kind`, `m2a_scope`, `ops`, `expected_disposition`, `disposition_reason`,
  `resulting_ir`, `effects`, `expected_echo`, `alternatives`, `ambiguity`, `confidence`, `gap_codes` y `notes`.

**SF192 (obligatorio; R3-1).**

```json
{"gold_id": "SF192", "slice": "SYNTHETIC_FOLLOWUP", "schema": "m2a-followup-gold/2.1", "source_kind": "SYNTHETIC",
 "input_kind": "TEXT", "followup_text": "ya no los ordenes por valor",
 "followup_sha256": "252601648ad05238c0f56c28540c9e8acb5953e3abc7273d734d8534311e56db",
 "prior": {"chain": [], "gap_seconds": 50, "gold_id": null, "owner": "FOUNDATION_SEARCH", "pending": null,
  "selection_rule": "SYNTHETIC_AUTHORED", "state": "EXECUTED",
  "ir": {"aspect": null, "host_notice": [], "logic": null, "operation": "SEARCH", "product_view": null,
   "universe": "CONTRACTS", "view": "RECORDS", "view_source": "USER",
   "order": {"direction_source": "STATED", "operator": "value_desc"},
   "cardinality": {"n": 10, "n_requested": 10, "operator": "top_n"},
   "conditions": [
    {"dimension": "TOPIC", "id": "c1", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "vigilancia"},
    {"dimension": "BUYER", "id": "c2", "level": null, "operator": "is", "polarity": "POSITIVE", "unit": null, "value_hint": "Gobernación del Meta"},
    {"dimension": "TIME", "id": "c3", "level": null, "operator": "during_year", "polarity": "POSITIVE", "unit": null, "value_hint": "2025"}]}}}
```

**SF193 (opcional; R3-2, `KEEP` de ranura vacía).**

```json
{"gold_id": "SF193", "slice": "SYNTHETIC_FOLLOWUP", "schema": "m2a-followup-gold/2.1", "source_kind": "SYNTHETIC",
 "input_kind": "TEXT", "followup_text": "y los de 2024 con la misma entidad contratante",
 "followup_sha256": "45ad7a07b3dc7d039f230db53699884c8533100027c767bc5e9db4ae84298bfa",
 "prior": {"chain": [], "gap_seconds": 35, "gold_id": null, "owner": "FOUNDATION_SEARCH", "pending": null,
  "selection_rule": "SYNTHETIC_AUTHORED", "state": "EXECUTED",
  "ir": {"aspect": null, "host_notice": [], "logic": null, "operation": "SEARCH", "product_view": null,
   "universe": "CONTRACTS", "view": "RECORDS", "view_source": "USER", "order": null, "cardinality": null,
   "conditions": [
    {"dimension": "TOPIC", "id": "c1", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "mantenimiento de vías"},
    {"dimension": "TIME", "id": "c2", "level": null, "operator": "during_year", "polarity": "POSITIVE", "unit": null, "value_hint": "2025"}]}}}
```

**SF194 (opcional; R3-4, negación sobre varios positivos).**

```json
{"gold_id": "SF194", "slice": "SYNTHETIC_FOLLOWUP", "schema": "m2a-followup-gold/2.1", "source_kind": "SYNTHETIC",
 "input_kind": "TEXT", "followup_text": "pero que no sean de cafetería",
 "followup_sha256": "a889ce1303b771cc80d3f41f25e39e567bbfd8d07d456ac9c14d3e445d8b9399",
 "prior": {"chain": [], "gap_seconds": 40, "gold_id": null, "owner": "FOUNDATION_SEARCH", "pending": null,
  "selection_rule": "SYNTHETIC_AUTHORED", "state": "EXECUTED",
  "ir": {"aspect": null, "host_notice": [], "logic": null, "operation": "SEARCH", "product_view": null,
   "universe": "CONTRACTS", "view": "RECORDS", "view_source": "USER", "order": null, "cardinality": null,
   "conditions": [
    {"dimension": "TOPIC", "id": "c1", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "aseo"},
    {"dimension": "TOPIC", "id": "c2", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "cafetería"},
    {"dimension": "BUYER", "id": "c3", "level": null, "operator": "is", "polarity": "POSITIVE", "unit": null, "value_hint": "Alcaldía de Pereira"}]}}}
```

**SF195 (opcional; control de D-2: `CLEAR(ORDER)` + `SET_ORDER` con CARDINALITY arrastrada).**

```json
{"gold_id": "SF195", "slice": "SYNTHETIC_FOLLOWUP", "schema": "m2a-followup-gold/2.1", "source_kind": "SYNTHETIC",
 "input_kind": "TEXT", "followup_text": "quítales el orden por valor y ordénalos por fecha",
 "followup_sha256": "4917df8b6294041d34380b3e3ab999b0be81b5d7798530eac400a97437467503",
 "prior": {"chain": [], "gap_seconds": 45, "gold_id": null, "owner": "FOUNDATION_SEARCH", "pending": null,
  "selection_rule": "SYNTHETIC_AUTHORED", "state": "EXECUTED",
  "ir": {"aspect": null, "host_notice": [], "logic": null, "operation": "SEARCH", "product_view": null,
   "universe": "CONTRACTS", "view": "RECORDS", "view_source": "USER",
   "order": {"direction_source": "STATED", "operator": "value_desc"},
   "cardinality": {"n": 10, "n_requested": 10, "operator": "top_n"},
   "conditions": [
    {"dimension": "TOPIC", "id": "c1", "level": null, "operator": "contains", "polarity": "POSITIVE", "unit": null, "value_hint": "vigilancia"},
    {"dimension": "BUYER", "id": "c2", "level": null, "operator": "is", "polarity": "POSITIVE", "unit": null, "value_hint": "Gobernación del Meta"}]}}}
```

### 4.1 Partición DEV/TEST que les toca

La regla fija del sello (`m2a_gold_seal.partition`, sal `m2a-gold-partition/1`) funciona así: se estratifica por slice
y por disposición; dentro del estrato, los ids se ordenan por `sha256(sal:id)`; las posiciones pares van a TEST y las
impares a DEV. Es pegajosa: un id existente nunca cambia de lado.

**Consecuencia que conviene saber:** el lado de un id nuevo **no** es función sólo de su hash. Depende de la
disposición que adjudiquen (el estrato) y de qué otros ids nuevos entren en el **mismo** estrato. Por eso se calcula
después de adjudicar, en un solo re-sellado, con todos los casos que entren. Tabla calculada con la función real del
sello (cada id solo, por disposición posible; en todos los casos, 0 movimientos de ids existentes):

| id | `sha256(sal:id)[:12]` | EXECUTE | CLARIFY | NEW_SEARCH | REASK | INFORM |
|---|---|---|---|---|---|---|
| SF192 | `a19031928f58` | DEV | DEV | TEST | TEST | DEV |
| SF193 | `e0c01bd8b805` | DEV | TEST | TEST | DEV | DEV |
| SF194 | `0e2657253a5f` | DEV | TEST | TEST | TEST | TEST |
| SF195 | `110f156f5fb2` | DEV | TEST | TEST | TEST | TEST |

Ejemplo de la dependencia. SF192 solo en `CLARIFY` cae en DEV; SF192 junto con SF194, los dos en `CLARIFY`, cae en
TEST.

---

## 5. Plan de re-sello (fase B, paso a paso)

**Cómo entra un caso nuevo.** No es un override del candidato asesor: ese árbol es evidencia inmutable, fijada por
`IMPORT_MANIFEST.sha256` y ligada por el sello. Entra como **adjudicación registrada**, igual que U1/U2/D1, pero como
**adición**:

- un archivo nuevo `docs/audits/m2a_conversation/gold/ADDITIONS_R3_synthetic.jsonl`, con los registros completos tras
  la adjudicación ciega;
- un documento `gold/ADJUDICATION_<fecha>_R3.md`, con la decisión, el origen y el digest de cada registro.

Hoy `build_sealed_gold.py` sólo sabe **parchear** ids existentes: comprueba que los parcheados existan, y no actualiza
`ids` ni `n` del índice. Necesita un mecanismo `ADDITIONS` mínimo:

- añade los registros al sintético GOLD, ordenados por id, y comprueba que **no** existan en el candidato;
- actualiza `ids`, `n`, `by_disposition` y `sha256` del slice en el índice;
- añade `idx["additions"]` con el digest de cada uno.

**Orden.** Un solo escritor por archivo.

1. **Contrato:** se aplica el diff aprobado de §3. Cambia su sha256.
2. **Tipo (`app/core/foundation_accepted.py`).** Se añaden los códigos nuevos a `CLARIFY_CODES` y UNIVERSE a la cola de
   `UNRESOLVED_VALUE:` en `reason_code_allowed`. Es el **adyacente exclusivo de M2A-3** (`M2A_WAVE1_PREP.md` §4): lo
   escribe el carril M2A-3, o HERA lo serializa; este carril no.
   - Tests que se tocan:
     - `tests/test_foundation_accepted_contract.py::test_reason_codes_are_the_closed_annex_lists`: positivo para
       UNIVERSE y negativos para `RETRIEVAL_INTENT` / `WHATEVER`.
     - `tests/test_m2a_contract_r3_sweep.py`: las filas HUECO pasan a CUBIERTO y `REPRESENTABLE_TODAY` se invierte en
       los códigos aprobados. El pin 4 (A.3 ⇔ tipo) obliga a que contrato y tipo cambien juntos.
3. **Etiquetado ciego** de los casos aprobados contra el contrato enmendado; el usuario adjudica (U3).
4. **Verificador (`check_m2a_gold_contract.py`).**
   - `REASONS` += los códigos nuevos de A.3. Sin eso, G1 marca `reason …` en SF192.
   - Recomendado (O-6): alinear `REASON_PREFIXES` con el tipo, de modo que `UNRESOLVED_VALUE:<x>` sólo acepte claves de
     dimensión de ranura y UNIVERSE.
   - No hace falta una regla G nueva para R3-1. Si una etiqueta ejecutara SF192 conservando la CARDINALITY, la mata
     `check_ir` («cardinality without order»); si la ejecutara quitándola sin op, la mata G3 («cardinality changed
     without an op»).
   - Cambia su sha (está en `RULE_FILES`).
5. **Mutantes (`mutate_check_m2a_gold_contract.py`)**, que deben morir por su regla:
   - `R3_SF192_executed_keeps_cardinality` (EXECUTE con `resulting_ir` sin ORDER y con CARDINALITY);
   - `R3_SF192_executed_drops_cardinality` (EXECUTE sin ninguna de las dos y sin `CLEAR(CARDINALITY)`);
   - `R3_SF192_reason_outside_A3`.
   - Opcionales: `R3_SF194_executed_contradiction`, `R3_SF195_*` según D-2.
6. **Constructor (`build_sealed_gold.py`):** el mecanismo `ADDITIONS`. Después, `build_sealed_gold.py` y
   `build_sealed_gold.py --check`.
7. **Informe de representabilidad:** `check_m2a_gold_contract.py --real-dir <privado> --json
   gold/REPRESENTABILITY_REPORT.json`, con 0 violaciones.
8. **Sello (`m2a_gold_seal.py`).**
   - `ADJUDICATIONS += {"U3": …}`.
   - `RULE_FILES` y/o `PUBLIC_FILES` += `ADDITIONS_R3_synthetic.jsonl` y `ADJUDICATION_<fecha>_R3.md`.
   - Revisar `sealed_on` y `base_commit` (hoy fijos en `2026-10-01` / `8c1f8c54`).
   - `m2a_gold_seal.py build --real-dir <privado>`.
   - Antes de actualizar `PARTITION_SHA256` (en `m2a_gold_seal.py` **y** en `tests/test_m2a_gold_seal.py`), comprobar
     con un diff de la partición vieja frente a la nueva que **ningún id existente cambió de lado**, y declararlo en el
     commit.
9. **`tests/test_m2a_gold_seal.py`.**
   - Totales: 150 → 150 + k y 199 → 199 + k.
   - `test_gold_is_representable_by_the_contract`: `{"gold": 150 + k, "frontier": 41}`.
   - Pin de la partición.
   - Una aserción para U3 en `test_sealed_gold_reproduces_from_candidate_and_adjudications`.
10. **Documentos:** `ADJUDICATION_<fecha>_R3.md`, filas de la matriz (§3) y un cierre del re-sellado.
11. **Gates:**
    - `build_sealed_gold.py --check`;
    - `m2a_gold_seal.py verify`, con y sin `--real-dir`;
    - `check_m2a_gold_contract.py --real-dir`, con 0 violaciones;
    - `mutate_check_m2a_gold_contract.py`, con `ALL_KILLED True`;
    - `privacy_scan_public_tree.py`;
    - pytest focal (`test_m2a_gold_seal`, `test_foundation_accepted_contract`, `test_m2a_contract_r3_sweep`,
      `test_m2a_boundaries`, `test_foundation_authority_contract`, `test_m1_wave1_parity_pins`);
    - ruff;
    - `olympus-semgrep-delta --base fa0b6de8`.

**Hashes que cambian:**

- en `interpreting_rules`: el contrato, el verificador, `build_sealed_gold.py` y los documentos de adjudicación;
- en `public_files`: los `*.jsonl` sellados y el índice;
- en `record_sha256.SYNTHETIC`: + k entradas;
- en `slices.M2A_GOLD.SYNTHETIC`: `ids`, `n`, `by_disposition` e `index_sha256`;
- `totals`, `adjudications.rules`, `adjudications.records` y `partition` (+ k ids);
- el sha de `REPRESENTABILITY_REPORT.json`;
- `seal_sha256`.

**Quién fija el sha del contrato.** Sólo `interpreting_rules` del sello, que comprueban `m2a_gold_seal.py verify` y
`tests/test_m2a_gold_seal.py::test_seal_recomputes_byte_for_byte`; ningún otro test fija ese sha. Dos pines leen el
**texto** del contrato y obligan a que contrato y tipo cambien juntos: el pin 4 de `tests/test_m2a_contract_r3_sweep.py`
(§6 y A.3 frente a las constantes del tipo) y, del lado del tipo,
`tests/test_foundation_accepted_contract.py::test_reason_codes_are_the_closed_annex_lists`.

**No cambian:** el candidato asesor, `IMPORT_MANIFEST.sha256`, los digests y archivos REAL y FRONTIER. Con k = 0 (sólo
enmienda, sin casos) cambian únicamente el contrato, el verificador y `seal_sha256`; no es lo recomendado, porque R3-1
quedaría sin caso en el Gold.

**Impacto en el carril oráculo M2A-8a0.** El oráculo lee el sello como entrada (`m2a_oracle_gold.py`): ejecuta
`m2a_gold_seal verify` antes de construir casos, toma ids, partición y FRONTIER del sello, y valida razones con los
`REASONS` del verificador sellado. Consecuencias:

- **sin cambios de código:** el oráculo se regenera; su manifiesto (sin commit en el worktree 8a0 al escribir esto)
  fija `gold_seal_sha256 = bb2e9812…` y cambiará;
- **orden con la congelación:** el plan exige congelar 8a0 **antes** de `compose`. Si 8a0 ya está congelado cuando
  entre R3, la congelación queda invalidada y hay que re-congelar. Recomiendo sellar R3 antes de congelar 8a0;
- **los códigos nuevos** tienen que estar en los `REASONS` del verificador sellado antes de regenerar el oráculo, o su
  validación de vocabulario fallará;
- **aplicabilidad:** SF192 entra por las reglas mecánicas del oráculo (es pura). SF075 sigue con su decisión de usuario
  (rojo, `FOUNDATION_O1B_EXPLICIT_TIME_PARITY`).

**Impacto en M2A-3.** Itera sólo contra DEV. Un caso nuevo que caiga en DEV entra en su conjunto de iteración; uno que
caiga en TEST queda reservado para congelar. Su fase 2 implementa R3-1 «cuando la enmienda esté sellada»
(`M2A3_PHASE1_CLOSE.md` §5 de su rama).

**Impacto en M2A-2.** Ninguno: el almacén no lee A.3.

---

## 6. Observaciones fuera del barrido (para HERA; sin propuesta)

- **O-1. Primer turno → efectiva (M2A-1b).** `main` admite en `READY` estados que el tipo M2a rechaza. Ejecutado: una P
  con dos cotas VALUE del mismo lado da `READY` en `admit_proposal` (scratch). Ya conocidos por M2A-3: copias por
  petición en ancla dual (NC-002) y O1B. Con esos estados, `from_acceptance` no puede persistir un primer turno que
  `main` ejecuta; INV-M2A-06 obliga a definir su fail-closed en M2A-1b. No es `compose`.
- **O-2. Reducción M1 → A.3.** Que las aclaraciones de M1 (`UNRESOLVED_HOST_DIMENSION:<DIM>`, `AMBIGUOUS_BUYER`,
  `CONTRADICTORY_STATE`, `HOST_UNIVERSE_RULE_CONFLICT`…) se escriban en la pendiente como `UNRESOLVED_VALUE:<DIM>` es
  una convención del esquema Gold 2.1 (`kit_v2_2/SCHEMA_v2_1.md:232`), no texto del contrato. Toca a M2A-1b/M2A-4.
  R3-3 hace representable el caso del universo.
- **O-3. P no admitida con ancla vigente.** Una P cuya aceptación es `UNSUPPORTED` (p. ej. dos peticiones sin
  `SHARES_SCOPE` sobre un ancla dual: es MULTI, non-goal de §1.2) no tiene fila en §7.1, y su disposición no está
  escrita. Es un posible hueco de la misma clase, pero fuera del eje tipo × op; **a decidir si entra en este
  re-sellado** (D-6).
- **O-4. Dependencia E-ORDER sin registrar.** Los follow-ups con ORDER/CARDINALITY del Gold (matriz T07) necesitan la
  autoridad ORDER/CARDINALITY (E-ORDER, NÚCLEO, plan M2A-3c), y `main` aún no las admite en primer turno. §19 no la
  lista (D-5).
- **O-5. Partición.** El lado de un id nuevo depende de su disposición y de los demás ids nuevos del estrato (§4.1):
  todos los casos deben entrar en el mismo re-sellado.
- **O-6. El verificador y el tipo divergen en `UNRESOLVED_VALUE:<x>`.** El verificador acepta cualquier cola; el tipo,
  sólo dimensiones de ranura. Alineados, el verificador habría señalado S02 en el sellado M2A-0.
- **O-7. Wire de M2A-5.** `CARDINALITY_WITHOUT_ORDER` sobre `FOLLOWUP_DELTA` impide expresar «sólo 10» sobre un ancla
  ordenada. Es una decisión de M2A-5; si la relaja, (b) se vuelve alcanzable y lo cubre R3-1 (D-2).

---

## 7. Lo que el usuario tiene que aprobar

**Alcance de la enmienda** (cada propuesta es independiente):

| Propuesta | Recomendación | Coste en el re-sellado |
|---|---|---|
| R3-1 `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER` | **sí** (es la decisión del 2026-10-02) | contrato + A.3 + SF192 |
| R3-2 totalidad `DeltaRefusal` → A.3 | **sí**: M2A-3 se para en cuanto implemente `DELTA_SLOT_CONFLICT` o `KEEP_ON_EMPTY_SLOT` | contrato + A.3 (+ SF193 opcional) |
| R3-3 `<DIM>` con UNIVERSE + registro de SF075 en §19 | **sí**: es la forma genérica del encargo y desbloquea el fail-closed de STOP-1 sin inventar código | contrato + tipo (M2A-3) |
| R3-4 rama fail-closed de F16 | **sí**, con D-3 | contrato (+ SF194 opcional) |
| R3-5 INV-M2A-25 | **sí** (convierte el criterio en un check) | contrato |
| P-1 y P-2 (forma) | **sí**: el sha del contrato cambia igualmente; P-2 es deuda registrada | contrato |

**Decisiones:**

- **D-1. Forma de la pendiente de R3-1.** (i) Un candidato «quitar orden y límite» y «mantener» = `CANCEL_PENDING`
  (recomendado: se responde con un toque, como F11). (ii) Cero candidatos y `answer_slots = {ORDER, CARDINALITY}`: la
  respuesta tiene que repetir los dos `CLEAR`.
- **D-2. R3-1 sobre el estado o sobre la op.** Recomiendo **el estado**: cubre (b) sin re-sellar y no dispara con
  `CLEAR(ORDER)` + `SET_ORDER`. La forma literal de la decisión del 2026-10-02 sí dispararía con SF195.
- **D-3. Código de R3-4.** (iii) `OP_NOT_ACCREDITED:<DIM>` ⇒ `UNRESOLVED_VALUE:<DIM>`, sin código nuevo (recomendado),
  o (i) recuperar `DELTA_CONTRADICTS_CARRIED`.
- **D-4. Casos que entran.** SF192 (obligatorio); SF193, SF194 y SF195 según R3-2, R3-4 y D-2. Todos en el mismo
  re-sellado (O-5).
- **D-5. Registrar E-ORDER** en la tabla de dependencias de §19 (O-4). Es un registro, no una capacidad.
- **D-6. O-3** (P no admitida con ancla): dentro de este re-sellado o fuera.
- **Escritor de `foundation_accepted.py` en la fase B:** el carril M2A-3 (adyacente exclusivo) o HERA, serializado.

---

## 8. Gates de esta fase (resultado literal)

- `pytest -p no:cacheprovider -q tests/test_m2a_contract_r3_sweep.py`: **5 passed**. Los mismos 5 pines pasan con el
  `foundation_accepted.py` de la rama M2A-3 en `f30e91ff` (scratch). Mutantes en scratch: 6/6 muertos.
- `ruff check tests/test_m2a_contract_r3_sweep.py` (configuración del repo, y `--select E,W,F --ignore E501` como en
  `make lint`): limpio.
- `m2a_gold_seal.py verify`: `seal OK bb2e9812…`, sin tocar nada sellado.
- La suite global no se corrió (entrega documental + una herramienta de test).

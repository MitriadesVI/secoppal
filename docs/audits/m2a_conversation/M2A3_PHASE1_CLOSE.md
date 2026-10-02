# M2A-3 — Cierre de la fase 1 (antes de `compose`)

**Fecha:** 2026-10-02. **Rama:** `mission/m2a-3-reducer` (base Paso 0 `fa0b6de8`). **Alcance de la fase:** inventario
§6.6, forma canónica (`M2A3_CANONICAL_FORM.md`), invariantes compartidas en `app/core/foundation_accepted.py` y diseño
(sin implementar) de la entrada de `classify_turn`. `compose` y `classify_turn` esperan al oráculo M2A-8a0 congelado;
la fase 2 itera sólo contra DEV.

No se importó ni se copió `foundation_admission.py`; se leyó para inventariar. No se tocaron el contrato, el Gold, el
store, el runtime ni los prompts. El REAL privado sólo se consultó para R7, imprimiendo ids y estructura de ops, nunca
texto.

## 1. Inventario §6.6: política → condiciones que cita → dueño → fail-closed en M2a

Fuente: todos los `HostPolicy(` de `app/` y `scripts/` en la base (`foundation_admission.py:84, 141, 152, 154`;
`foundation_acceptance_loader.py:145` sólo lee aceptaciones grabadas), las políticas que consume
`foundation_projection.py` y las que nombra el contrato §6.6.

Regla general propuesta para `compose` (fase 2; no reproduce la lógica de admisión, sólo la cita):

- una política se **arrastra** byte-idéntica si todas las condiciones que cita se arrastran intactas y el parche no
  toca ninguna de sus entradas de decisión;
- se **va con su condición** cuando todas las condiciones que cita salen por una op citada o por una sustitución
  declarada (`⊆` lo hace obligatorio);
- en cualquier otro caso hay que re-decidirla. Como en M2a no existe su autoridad, el parche se **rechaza**:
  `OP_NOT_ACCREDITED:<DIM>` ⇒ `CLARIFY UNRESOLVED_VALUE:<DIM>`.

| Política | La crea en `main` | Cita | Entradas de su decisión | Autoridad dueña | Comportamiento en M2a |
|---|---|---|---|---|---|
| `O1B@1` | `foundation_admission.py:84` | la condición REQUESTED_UNIVERSE (`RETRIEVAL_INTENT`, no es ranura) | lente de oportunidades sin un STATE `family` en la misma petición | política de universo / oportunidades (ADR-101 H7, O1B; contrato §14), con la representación a cargo de M2A-1b | **No representable**: con `⊆` no hay condición que citar (pin `test_the_request_universe_policies_cannot_be_persisted`). Ningún ancla de oportunidades se persiste hasta que M2A-1b decida. En `compose`: un `SET`/`CLEAR` de STATE sobre una petición de oportunidades ⇒ `OP_NOT_ACCREDITED:STATE`; para `REPLACE_UNIVERSE` hacia oportunidades no hay código: ver **STOP-1** |
| `opportunity-explicit-state@1` | `foundation_admission.py:84` | ídem | lente de oportunidades con un STATE `family` en la misma petición | ídem | ídem |
| `modality-lp-family@1`, `modality-direct-family@1` | `foundation_admission.py:141` | la condición MODALITY con operador `family` | la familia de sus ids resueltos (y el dataset, en la proyección) | D-STATE/MOD (`host_authorities/modality.py` emite la ref como veredicto en `AcceptedCondition.policies`) | se arrastra con su condición; se va con ella; un `SET` de MODALITY `family` nueva, o un `REPLACE_UNIVERSE` con la condición arrastrada, ⇒ `OP_NOT_ACCREDITED:MODALITY`. El Gold usa MODALITY `is` (SF008, SF080, SF160), que no necesita política |
| `ADR-100@1` | `foundation_admission.py:152` | la condición STATE `oferta_abierta` citada con más de un span (embudo) | el número de spans de su evidencia (sólo la política lo registra; la `AcceptedCondition` lleva un origen) | D-STATE (`host_authorities/state.py`, `FUNNEL_POLICY`) | se arrastra con su condición; se va con ella; un embudo nuevo por `SET` ⇒ `OP_NOT_ACCREDITED:STATE` |
| `celebrated-state-complement@<v>` | `foundation_admission.py:154` | la condición STATE `Terminado` NEGATIVE | la condición más el universo de contratos (complemento sobre `CELEBRATED_STATES`) | D-STATE (`contract_universe`) | se arrastra mientras el universo siga siendo contratos; `REPLACE_UNIVERSE` con ella arrastrada, un `SET` nuevo o la inversión F16 que la necesite ⇒ `OP_NOT_ACCREDITED:STATE` (antes, H8 puede dar `STATE_NOT_IN_UNIVERSE`, que es PURA sobre `state@1.literals`) |
| `topic-aseo-morph@1`, `convocatoria-vocab@1` | ninguna fábrica en `app/` (la proyección las consume si vienen; el loader puede traerlas) | la condición TOPIC con ese valor | el valor de TOPIC | cierres de vocabulario del host (C1.5, familia `topic-k1@1`) | se arrastran con su condición; `compose` nunca las crea, igual que la admisión de `main`: un `SET` TOPIC «aseo» compone como un primer turno de `main` |
| `contract-universe@<v>`, `published-opening@1` | internas de la proyección | — | dataset / valor de STATE | proyección | no son `HostPolicy`, no se persisten; las recalcula la proyección de la efectiva (M2A-6) |
| ACTOR×STATE, ACTOR×TIME | no existen en `main` | — | — | D-ACTOR (carril prohibido aquí) | `compose` nunca las fabrica. Además la proyección de `main` no tiene rama ACTOR (`UNSUPPORTED_CONDITION`): una efectiva con ACTOR no es ejecutable hasta D-ACTOR. Dependencia declarada |
| `universe-c5@1` | no existe en `main` (C1.5) | — | — | núcleo C1.5 | nunca se fabrica; la condición que añadiría no puede aparecer |
| tope top-N (P8) | no es `HostPolicy`: `cardinality@2` (`n = min(n_requested, 100)`) + `topn.capped` | — | el valor | autoridad CARDINALITY | PURA: viaja con el valor; un valor nuevo lo resuelve la autoridad |
| dirección de orden por defecto (F21) | no es `HostPolicy`: `order.direction_defaulted` + `Provenance.order_direction_source` | — | el valor | autoridad ORDER | PURA, como la anterior |
| año por reloj (SMMLV) | `year.inferred` (E-VALUE) | — | reloj inyectado | E-VALUE | DEPENDE(E-VALUE): H3 |
| PV1–PV6 (`HostViewDecision`) | ola F | — | — | F | no representable (`VIEW_REQUIRED`, R3) |

**Hallazgo para HERA / oráculo.** SF075 («mejor muéstrame las oportunidades», Gold `EXECUTE`) arrastra TIME 2025 hacia
PROCESSES/OPPORTUNITIES. Además de O1B (STOP-1), la proyección de `main` rechaza oportunidades con TIME
(`UNSUPPORTED_OPPORTUNITY_OVERRIDE`, `foundation_projection.py:235-237`). Ese caso no es ejecutable sin trabajo de
E-STATE y del dueño de la política de universo.

## 2. Matriz de aplicabilidad (antes del detector)

`PURA` = sobre valores tipados y la forma de P; `INSUMO_INYECTADO(<tipo>)` = pura con un insumo que entra tipado;
`DEPENDE(<carril>)` = su provisional falla cerrado y queda como rojo declarado (§20.2).

| Regla | Clase | Provisional fail-closed / nota |
|---|---|---|
| §6.1 todo o nada (INV-M2A-01) | PURA | — |
| §6.2 orden fijo; `DELTA_SLOT_CONFLICT` | PURA | — |
| §6.3 arrastre por identidad (+`condition_id`; R5 excepción `request_ids` con ancla dual) | PURA | — |
| H1 vigencia | INSUMO_INYECTADO(instante de referencia + instante del turno anterior) | 30 min (§10.1) |
| H2 pendiente | DEPENDE(M2A-4) | — |
| H3 SMMLV sin vigencia única | DEPENDE(E-VALUE / M2A-3b) | `TIME` de varios años con VALUE `SMMLV` ⇒ `CLARIFY SMMLV_YEAR_AMBIGUOUS` es PURA; el recálculo, no |
| H4 relativo no temporal | PURA sobre P (etiqueta `relative_to_previous`, que emite M2A-5) | — |
| H5 «con X» tras ACTOR | DEPENDE(M2A-4) | candidatos resueltos por autoridad |
| H6 territorio frente a comprador arrastrado | DEPENDE(D-BUYER) | `buyer.territory.*` |
| H7 región fuera de lista | DEPENDE(E-GEO) | — |
| H8 estado fuera del universo | PURA para STATE (`state@1.literals` por dataset); DEPENDE(E-MODALITY) para MODALITY (`modality@1` no lleva el dataset) | — |
| H9 unidad no heredable | DEPENDE(E-VALUE / M2A-3b) | — |
| H10 mes sin año | DEPENDE(E-TIME) | — |
| H11 búsqueda vacía tras el delta | PURA si la efectiva queda sin condiciones; DEPENDE(G-FU-13 / M2b) para el resto | — |
| H12 `CLEAR` de ranura vacía como única op | PURA | — |
| §6.5 A8.1 | PURA (cifras, inclusividad, origen; comparabilidad por `Provenance.unit`) | mixto SMMLV/pesos ⇒ `CLARIFY UNRESOLVED_VALUE:VALUE` |
| §6.5 F16 / F3 / F22 | PURA | varios positivos o valor contenido ⇒ `CLARIFY` (G-FU-09) |
| §6.5 F9 `VIEW_REDERIVED` | DEPENDE(F) | no representable (R3) |
| §6.6 políticas de petición | PURA (arrastre) / DEPENDE(dueño de cada política, §1) | STOP-1 |
| §6.7 nunca ejecutar vacío/degenerado/dos cotas/CARDINALITY sin ORDER | PURA (tipo + A8.1) | STOP-2 (R7) |
| §7.1 filas 1–4, 6 | PURA sobre hechos (filas 1–2 con ancla inyectada por M2A-2/M2A-6) | — |
| §7.1 fila 5 extensional | DEPENDE(M2A-5) | señal tri-estado; `ABSENT` nunca es intensional |
| §7.2 a–d | PURA sobre hechos; la producción de la evidencia de continuación es DEPENDE(M2A-5) (garantía limitada) | Q-R8-1…3 |
| §15.1 por lado | PURA | — |
| §15.2 A8.1 / igualdad del mismo turno | PURA; «exactamente X» DEPENDE(D-MONEY) | — |

## 3. R7: `CLEAR(ORDER)` con una CARDINALITY arrastrada

Recorrido de los 199 casos de `M2A_GOLD` (sintéticos en git; REAL por la ruta privada, sólo estructura):

- `CLEAR(ORDER)` aparece **únicamente** junto a `CLEAR(CARDINALITY)` (SF023, A y B);
- `SET_ORDER` con CARDINALITY arrastrada: SF018, SF019, SF172 (F10: se conserva);
- `CLEAR(ORDER)` **solo** con CARDINALITY arrastrada: **ningún caso**, y el contrato no da código.

El tipo ya rechaza `CARDINALITY_WITHOUT_ORDER`, así que `compose` no puede producir ese estado. Conservar la
CARDINALITY sería inventarla; quitarla sin cita violaría INV-M2A-16. ⇒ **STOP-2** para la fase 2: hace falta una regla
del usuario, o un código que no está en la lista cerrada.

## 4. R8: diseño de la entrada cerrada de `classify_turn` (sin implementar)

`classify_turn(facts: TurnFacts) -> TurnClassification`. Es pura, no lee texto (INV-M2A-24), devuelve exactamente un
`TurnScope` (INV-M2A-20) y no añade ninguno.

```text
TurnFacts (frozen; todo campo cerrado)
  input: TEXT | EVENT
  event: OPTION | PAGE | VIEW | ORDER | REMOVE_FILTER | CONTINUE | RESET | None      # sólo con EVENT
  anchor: NONE | UNREADABLE | LEGACY | FOUNDATION_SEARCH                             # lo inyecta M2A-2/M2A-6
  anchor_operation: Operation | None                                                 # sólo LEGACY (ANCHOR_NOT_SEARCH:<op>)
  pending: bool                                                                       # pendiente vigente
  # hechos de P: sólo sus spans con dueño (la propuesta del turno + sus ProposedSlotDirective)
  foreign_operation: Operation | None   # job con operación ∉ {SEARCH, FOLLOWUP_DELTA}
  continuation: bool                    # job FOLLOWUP_DELTA ∨ referent_evidence ∨ directiva KEEP/CLEAR ∨ depends_on_prior_value
  referent: NONE | EXTENSIONAL | INTENSIONAL | ABSENT  # ABSENT = hay referent_evidence y falta la señal de M2A-5
  relative_non_temporal: bool           # condición propia UNSUPPORTED con etiqueta relative_to_previous
  new_content: YES | NO | UNKNOWN       # Nuevo(P, A) sobre (polaridad, valor canónico) de TOPIC/ACTOR
  frame: bool                           # Marco(P): evidencia de universo ∨ evidencia de operación del job ∨ ≥ 2 condiciones propias
  admissible_alone: bool                # A9: job SEARCH con petición válida y ≥ 1 condición propia; no exige READY
  own_conditions: int                   # suelo de D7
  cancel_directive: bool                # directiva CANCEL_PENDING citada
  within_answer_slots: bool             # todas las condiciones de P ⊆ pending.answer_slots (§8.3 fila 3)

TurnClassification: scope: TurnScope; resolution: TurnResolution | None; reason: str | None (A.3);
                    control: PendingControl | None; context_reset: bool
```

Derivación (`turn_facts(...)`, pura). Usa `InterpretationProposal` (jobs, `referent_evidence`, evidencias de universo
y de operación, condiciones y sus etiquetas `UNSUPPORTED`), `tuple[ProposedSlotDirective]`, el `AcceptanceStatus` de
P, la efectiva del ancla y `PendingClarification.answer_slots`. Los valores canónicos se decodifican a través de
`foundation_accepted` (R1). Precedencia: §7.1 filas 1→8; con ancla y sin pendiente, §7.2 a→d; con pendiente, §8.3
filas 2–3 deciden `IN_NEW_SEARCH` + `CANCEL_PENDING`, y el resto es `IN` para M2A-4.

Test previsto: la tabla exhaustiva del dominio cerrado (no casos a mano), INV-M2A-05 como propiedad (con ancla o
pendiente sólo §7.2 c / §8.3 2–3 inician búsqueda, y nunca con Continuación) e INV-M2A-20.

Dudas que la tabla exhaustiva ya deja a la vista (para decidir antes de implementar):

- **Q-R8-1.** `depends_on_prior_value` no tiene campo en `main`. El único portador tipado es la etiqueta
  `relative_to_previous` (A.6), que es no temporal. El relativo temporal A5 no tiene marca en el wire ⇒
  DEPENDE(M2A-5/E-TIME).
- **Q-R8-2.** `new_content = UNKNOWN`: un TOPIC/ACTOR de P no RESOLVED no se puede comparar por valor canónico. §7.2
  no decide si es (b) o (c)/(d). Las dos ramas aclaran, pero con marco y pendiente distintos.
- **Q-R8-3.** Nuevo ≠ ∅ ∧ Marco ∧ ¬Admisible sola ∧ ¬Continuación (con ancla y sin pendiente) no está en la tabla
  a–d. En la práctica es P con universo `unspecified` sin Product View, es decir, DEPENDE(F). Ejemplo: ancla
  «papelería del SENA 2025» y P «y de software en la Gobernación de Boyacá».
- **Q-R8-4.** `referent = ABSENT` (hay `referent_evidence` y falta la señal): sin regla provisional (§7.2). Propuesta:
  el tipo `TurnFacts` lo rechaza (`ContractError`), lo que no es un `TurnScope` nuevo. Es coherente con que M2A-6 no
  se activa sin la señal. Los casos F4 del Gold que exigen la lectura intensional quedan como rojo declarado de
  M2A-5.
- **Q-R8-5.** La fila 2 (`OUT_ANCHOR_NOT_SEARCH`) precede a la 7. Hay que confirmar que una P admisible sola tras un
  turno legacy la enruta el dueño de ruta (C1.4) antes de M2a, y no `classify_turn`.

## 5. STOP previstos para la fase 2 (no bloquean la fase 1)

- **STOP-1 (§6.6, R2).** `REPLACE_UNIVERSE` hacia `opportunities` exige re-decidir O1B /
  `opportunity-explicit-state` sobre la efectiva compuesta. En M2a no hay dueño, y la lista cerrada no tiene código
  con dimensión de universo: `OP_NOT_ACCREDITED:<DIM>` / `UNRESOLVED_VALUE:<DIM>`, y el tipo sólo admite dimensiones
  de ranura. Ejemplo: SF075. Opciones para el usuario:
  - (a) un código con dimensión de universo (cambio de contrato);
  - (b) usar `STATE` como dimensión, lo que reinterpreta O1B (semántica);
  - (c) declarar SF075 dependiente y decidir qué devuelve `compose`.
- **STOP-2 (R7).** `CLEAR(ORDER)` solo con CARDINALITY arrastrada: no está en el Gold ni tiene código (§3).
- Q-R8-2, Q-R8-3, Q-R8-4: deciden filas de la tabla exhaustiva de §7.2.
- D-CANON-1 y D-CANON-2 (`M2A3_CANONICAL_FORM.md` §3): residuos de canonicidad, a decidir.

## 6. Gates de la fase 1 (resultado literal)

- `pytest -p no:cacheprovider --hypothesis-seed=0` sobre `tests/test_foundation_accepted_contract.py`,
  `tests/test_foundation_accepted_canonicality.py`, `tests/test_foundation_accepted_m2a3_canonical.py`,
  `tests/test_m2a3_order_observability.py`, `tests/test_m2a_boundaries.py`,
  `tests/test_foundation_authority_contract.py`, `tests/test_m1_wave1_parity_pins.py` y `tests/test_m2a_gold_seal.py`:
  **550 passed**. La base daba 517 en los seis archivos previos; ahora son 517 + 23 + 10 nuevos.
- `ruff check` (configuración del repo) sobre los cuatro `.py` tocados: limpio.
- `olympus-semgrep-delta --base fa0b6de8` sobre los mismos: `TOTAL=0 BASELINE=0 NEW_DELTA=0`. Cruzado con
  `semgrep --config .semgrep/secoppal-laws.yml`: 0.
- `m2a_gold_seal.py verify --real-dir <privado>`: `seal OK … REAL recomputed`.
- `check_m2a_gold_contract.py --real-dir <privado>`: `"violations": 0`.
- `privacy_scan_public_tree.py <REAL privado> <copia de los archivos del commit>`: limpio.

Corrí esos tests, no la suite global (módulo sin consumidor; la global corresponde a la integración).

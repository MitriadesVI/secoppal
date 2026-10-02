# M2A ola 1: registro de decisiones (2026-10-02)

**Qué es.** El registro durable de todas las decisiones de la ola 1 de M2a, pedido por el usuario «por si toca ajustar
algún .md». **No es normativo:** manda el contrato `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`, y el plan
`M2A_IMPLEMENTATION_PLAN.md` ordena el trabajo. Si una decisión cambia, se actualizan aquí su fila **y** los `.md` que
se listan en su ficha.

**Quién.**

- `usuario`: decisión de producto o de gobierno.
- `orquestador (no-producto)`: decisión de ingeniería o de proceso. No cambia el comportamiento del producto; si
  llegara a cambiarlo, deja de serlo y pasa al usuario.

**SHA.** Los conocidos en esta rama (`mission/m2a-contract-enm-r3`) o en las ramas de la ola, vistos el 2026-10-02.
Los marcados «a completar» los completa el orquestador al integrar.

## Decisiones del usuario

### U1. Paso 0 de la ola
- **Decisión.** Registros de módulos M2a por paso (`tests/m2a_steps/m2a_<n>.py`, cada carril escribe sólo el suyo).
  Invariante definitiva de consumo: sólo los módulos M2a registrados se consumen entre sí, a lo largo de `DEPENDS_ON`;
  nada externo (`app/`, `scripts/`, `streamlit_app`), ni el cierre legacy ni el runtime Foundation alcanzan M2a.
  Enmienda C1.x: los importadores bajo `app/` ⊆ los módulos registrados, y `CLOSED_TYPE_CONSUMERS` se mantiene.
  Mutantes obligatorios en cada pin.
- **Quién.** Usuario.
- **Motivo.** Dos carriles en paralelo sin una lista compartida, y sin puertas abiertas por registrarse.
- **Dónde.** `mission/m2a-wave1-prep`, `fa0b6de8`, con la reparación R1 en `fc2abc78`: `tests/m2a_steps/`,
  `tests/test_m2a_boundaries.py`, `tests/test_foundation_authority_contract.py`.
- **Si cambia, tocar.** `M2A_WAVE1_PREP.md` §1–§3 y §9; `M2A_IMPLEMENTATION_PLAN.md` §0.7.

### U2. El manifiesto legacy no se regenera
- **Decisión.** `tests/fixtures/legacy_closure_manifest_88653752.json` **no** se regenera. `conversation_store` entra
  como `ALLOWED_DIVERGENCE`, con un pin que sólo permite el delta de `clear()`.
- **Quién.** Usuario.
- **Motivo.** El manifiesto es la referencia congelada de `main`; regenerarlo rompe los pines 4–6.
- **Dónde.** Decisión en `fa0b6de8`. Implementación en `mission/m2a-2-store`: `76881287` (divergencia declarada) y
  `e20f0ebf` (pin del delta de `clear()`).
- **Si cambia, tocar.** `M2A_WAVE1_PREP.md` §7; `M2A_IMPLEMENTATION_PLAN.md` §0.6, filas M2A-2 y M2A-6, y §3.

### U3. `foundation_accepted.py`, adyacente exclusivo de M2A-3
- **Decisión.** `app/core/foundation_accepted.py` es adyacente **exclusivo** de M2A-3 (invariantes y canonicidad
  compartidas con M2A-1b). M2A-2 lo importa y no lo modifica.
- **Quién.** Usuario.
- **Motivo.** Un escritor por archivo.
- **Dónde.** Decisión en `fa0b6de8`. Implementación en `mission/m2a-3-reducer`: `30d073a4` y `0de2b9a2`. El lado del
  tipo de la enmienda R3 (fase 1c) también es suyo.
- **Si cambia, tocar.** `M2A_WAVE1_PREP.md` §4; `M2A_CONTRACT_ENM_R3_B4_RESEAL.md` §2.

### U4. Oráculo independiente M2A-8a0
- **Decisión.** Autor distinto del de M2A-3. No importa ni llama al reducer, y se congela antes de `compose`. M2A-3
  itera sólo contra DEV y corre TEST al congelar.
- **Quién.** Usuario.
- **Motivo.** Que el oráculo no se adapte a la implementación.
- **Dónde.** Decisión en `fa0b6de8`. Implementación en `mission/m2a-8a0-reducer-oracle`, `b6178ad5`.
- **Si cambia, tocar.** `M2A_WAVE1_PREP.md` §5; `M2A_IMPLEMENTATION_PLAN.md` §2.

### U5. Deuda de retención de `.generations`
- **Decisión.** `ConversationStore.clear()` no borra `.generations/<chat>/`. M2A-2 la caracteriza; no se repara sin
  autorización.
- **Quién.** Usuario.
- **Motivo.** Es preexistente y está fuera del alcance de M2a.
- **Dónde.** `DEBT_CLEAR_GENERATIONS_RETENTION.md` (`fa0b6de8`); registrada en M2A-2, `c6ca1ac6`.
- **Si cambia, tocar.** `DEBT_CLEAR_GENERATIONS_RETENTION.md`; `M2A_WAVE1_PREP.md` §8.

### U6. §6.6 no autoriza `foundation_admission`
- **Decisión.** No se importa ni se copia `foundation_admission`. Las políticas que dependen de E-* o de F quedan
  fail-closed, con dueño declarado.
- **Quién.** Usuario.
- **Motivo.** El reducer no re-decide lo que no es suyo.
- **Dónde.** `M2A_WAVE1_PREP.md` §6 (`fa0b6de8`); inventario en `M2A3_PHASE1_CLOSE.md` §1 (`1d7c282d`, `f30e91ff`).
- **Si cambia, tocar.** `M2A_WAVE1_PREP.md` §6; `M2A3_PHASE1_CLOSE.md` §1 (rama M2A-3); contrato §6.6 (sólo si cambia
  la regla).

### U7. STOP-1 (SF075)
- **Decisión.** Dependencia de `FOUNDATION_O1B_EXPLICIT_TIME_PARITY`; SF075 queda como rojo declarado. El TIME
  arrastrado se conserva (F1), no se re-sella por él, y O1B no se reinterpreta como STATE.
- **Quién.** Usuario.
- **Motivo.** Es una capacidad pendiente aguas abajo, no un hueco del contrato (U11).
- **Dónde.**
  - `M2A3_PHASE1_CLOSE.md` §5 (`f30e91ff`);
  - contrato §19, fila SF075 (`cb32361a`);
  - oráculo, `APPLICABILITY_DECISIONS.json` (`b6178ad5`).
- **Si cambia, tocar.** Contrato §19 y Anexo D; `M2A3_PHASE1_CLOSE.md` §5; matriz T16; `APPLICABILITY_DECISIONS.json`.

### U8. STOP-2: enmienda `DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER`
- **Decisión.** `CLEAR(ORDER)` con CARDINALITY vigente ⇒ `CLARIFY`, base intacta; nunca se borra la CARDINALITY
  implícitamente. Caso sintético, adjudicación independiente y re-sellado.
- **Quién.** Usuario.
- **Motivo.** Un estado alcanzable sin respuesta representable es un hueco del contrato.
- **Dónde.** Contrato §6.7 y A.3 (`cb32361a`, `a90d6ad0`); `M2A3_PHASE1_CLOSE.md` §5 (`f30e91ff`); caso SF192
  sellado en r3 (`379ca3db`).
- **Si cambia, tocar.** Contrato §6.7, A.3 y Anexo D; matriz T14; `M2A_CONTRACT_ENM_R3_B4_RESEAL.md`;
  `labeling_r3/R3_LABELER_INPUT.md`.

### U9. D-CANON-1: orden de las condiciones y de la lógica
- **Decisión.** Las condiciones van en orden canónico por ranura, según una tabla cerrada de precedencia (no
  alfabética), y se conserva el orden dentro de cada ranura. El constructor y el lector rechazan lo no canónico. En
  `logic`, sólo una conjunción pura pasa a `None`; los árboles OR/NOT quedan intactos.
- **Quién.** Usuario.
- **Motivo.** Un significado, una forma persistida, sin mover `soql` ni `plan_hash`.
- **Dónde.** `mission/m2a-3-reducer`: `0de2b9a2` (código) y `f30e91ff` (`M2A3_CANONICAL_FORM.md` §1, §2 y §6).
- **Si cambia, tocar.** `M2A3_CANONICAL_FORM.md`.

### U10. D-CANON-2: forma de las políticas
- **Decisión.** Las políticas van en forma PARTIDA, como en `main`: ordenadas y sin duplicados. Una política de varias
  condiciones se rechaza, salvo declaración expresa de su dueño.
- **Quién.** Usuario.
- **Motivo.** Nunca fusionar por equivalencia de proyección.
- **Dónde.** Como U9 (`0de2b9a2`, `f30e91ff`).
- **Si cambia, tocar.** `M2A3_CANONICAL_FORM.md`.

### U11. Principio: el reducer no inventa semántica
- **Decisión.** El reducer nunca arregla carencias de otras capas inventando semántica. Una capacidad pendiente no es
  un hueco del contrato.
- **Quién.** Usuario.
- **Motivo.** Separar una capacidad aguas abajo de un hueco del contrato.
- **Dónde.** Contrato, Anexo D.1 (`cb32361a`); `M2A3_PHASE1_CLOSE.md` §5 (`f30e91ff`);
  `M2A_CONTRACT_ENM_R3_PROPOSAL.md` (`507103cb`).
- **Si cambia, tocar.** Contrato, Anexo D.1; `M2A3_PHASE1_CLOSE.md` §5.

### U12. Alcance de la enmienda R3
- **Decisión.**
  - Entran R3-1…R3-5, P-1 y P-2.
  - D-2: la regla se decide sobre el estado resultante.
  - D-3: se reutiliza el código existente (`OP_NOT_ACCREDITED:<DIM>` ⇒ `UNRESOLVED_VALUE:<DIM>`).
  - D-4: SF192–SF195 entran en un único re-sellado (hecho en `379ca3db`, sello `3fdd81df…`; adjudicación en
    `gold/ADJUDICATION_R3.md`).
  - D-5: E-ORDER se registra en §19.
  - D-6, genérica: `OUT_UNSUPPORTED_PROPOSAL` / `INFORM UNSUPPORTED_PROPOSAL:<motivo>`, 0 llamadas, ancla intacta.
    Motivos y dueños: `MULTI_REQUEST` → M2b; `UNSUPPORTED_CONDITION` → extensión o versionado del lenguaje;
    `UNSUPPORTED_SOURCE` → contrato M1; `INVALID_PROPOSAL` → productor (M2A-5); `NO_ACTION` → contrato M2a (sólo sin
    pendiente). Exclusiones: un universo no dicho (Q-R8-3) y `UNACCREDITED_MODALITY_FAMILY` (va por §6.1).
- **Quién.** Usuario.
- **Motivo.** Re-sellar una sola vez, con todos los huecos de representabilidad cerrados.
- **Dónde.** Contrato: §6, §6.3, §6.5, §6.7, §7.1 fila 4b, §18, §19, §20, A.1, A.3, Anexo B, C.1 y Anexo D
  (`cb32361a`, `a90d6ad0`). Matriz (`cb32361a`). Propuesta (`507103cb`).
- **Si cambia, tocar.** Contrato, Anexo D y las secciones citadas; `M2A_TRACEABILITY_MATRIX.md`;
  `M2A_CONTRACT_ENM_R3_B4_RESEAL.md`; `tests/test_m2a_contract_r3_sweep.py` (los pines leen el texto).

### U13. D-1 final: opción 1
- **Decisión.**
  - La pendiente de R3-1 lleva C1 «quitar orden y límite» y un `SET_ORDER` por cada criterio de orden alternativo
    del dataset, resueltos por la autoridad ORDER al preguntar y sin el criterio vigente, con
    `answer_slots = {ORDER, CARDINALITY}`.
  - §8.3 fila 4 conserva su texto sellado.
  - «Dame 20» ⇒ `REASK ANSWER_UNRESOLVED`; «déjalo como estaba» ⇒ `CANCEL_PENDING`.
  - Sin E-ORDER, sólo C1.
- **Quién.** Usuario, que aprobó la opción del orquestador.
- **Motivo.** La cláusula genérica de §8.3 del borrador `cb32361a` dejaba ejecutar «top 20 por el orden viejo».
- **Dónde.** Contrato §6.7, §8.1, §19 y Anexo D.2 (`a90d6ad0`); matriz P11; barrido (`a90d6ad0`).
- **Si cambia, tocar.** Contrato §6.7, §19 y Anexo D.2; matriz P11; `M2A_CONTRACT_ENM_R3_B4_RESEAL.md` B4.4.3 y B4.5.

### U14. Congelación tras R3
- **Decisión.** Tras R3, el contrato de M2A-3 queda congelado para las primitivas, operaciones y alcance actuales.
  Sólo se modifica por una inconsistencia del propio contrato o por una extensión o versionado explícito del lenguaje.
- **Quién.** Usuario.
- **Motivo.** Que no se reabra para hacer pasar una implementación o absorber una capacidad aguas abajo.
- **Dónde.** Contrato §20.4 (`cb32361a`).
- **Si cambia, tocar.** Contrato §20.4 y cabecera; matriz X04.

### U15. INV-M2A-25, invariante mayor
- **Decisión.** INV-M2A-25 («toda respuesta alcanzable es representable») es invariante mayor. Gate
  `CONTRACT_COMPLETE_FOR_M2A3` ⇔ INV-M2A-25 satisfecho por el barrido.
- **Quién.** Usuario.
- **Motivo.** Convertir el criterio de STOP-2 en un check permanente.
- **Dónde.** Contrato §18 y §20.2 (`cb32361a`; el gate, en `a90d6ad0`); `tests/test_m2a_contract_r3_sweep.py`, donde
  el lado del tipo son xfail estrictos hasta M2A-3 1c.
- **Si cambia, tocar.** Contrato §18; matriz X03; `M2A_CONTRACT_ENM_R3_B4_RESEAL.md` §2.

### U16. Principio de oráculos
- **Decisión.** El GOLD decide el significado; el CONTRACT, las invariantes mecánicas; el ORACLE verifica ambas. Hay
  un oráculo Gold y otro de transición (regla completa de `AcceptedRequest`; mutantes M1–M5).
- **Quién.** Usuario.
- **Motivo.** Que ninguna autoridad se sustituya por otra.
- **Dónde.** Carril M2A-8a0: oráculo Gold en `b6178ad5`; oráculo de transición y mutantes M1–M5, SHA a completar.
- **Si cambia, tocar.** Documentos del oráculo (`docs/audits/m2a_conversation/oracle/`, rama 8a0);
  `M2A_IMPLEMENTATION_PLAN.md` §2 (fila M2A-8).

### U17. Secuencia de la ola
- **Decisión.** R3 → etiquetado ciego A/B → adjudicación → re-sellado único → ZEUS → M2A-3 fase 1c → regenerar el
  oráculo → ZEUS del oráculo (GO requerido) → congelar → `compose`. `compose` queda bloqueado hasta ese GO.
- **Quién.** Usuario.
- **Motivo.** Que el oráculo y el tipo estén cerrados antes del reducer.
- **Dónde.** `M2A_CONTRACT_ENM_R3_B4_RESEAL.md` §2 (`a90d6ad0`).
- **Si cambia, tocar.** `M2A_CONTRACT_ENM_R3_B4_RESEAL.md` §2; `M2A_IMPLEMENTATION_PLAN.md` §2 (secuencia; pendiente
  de reflejar al integrar).

### U18. P-3: F16 sólo invierte un positivo arrastrado
- **Decisión.** F16 sólo invierte un positivo **arrastrado** de la base; un positivo creado por el mismo parche no
  cuenta. Un parche que introduce a la vez +X y −X (mismo valor canónico, misma dimensión) cae en la rama fail-closed de
  R3-4 (`OP_NOT_ACCREDITED:<DIM>` ⇒ `UNRESOLVED_VALUE:<DIM>`).
- **Quién.** Usuario (dentro de R3).
- **Motivo.** Evitar que F16 «arregle» una contradicción que el propio turno crea.
- **Dónde.** Contrato §6.5 F16 y Anexo D.1 (`379ca3db`); barrido, fila S09, sonda P-p3 y pin del texto
  (`tests/test_m2a_contract_r3_sweep.py`); verificador G10 y mutante `R3_P3_SF001_same_patch_both_polarities`.
- **Si cambia, tocar.** Contrato §6.5; matriz M11; `check_m2a_gold_contract.py` (G10) y el mutante; barrido S09.

### U19. Una autoridad para las dependencias
- **Decisión.** «Una cuestión → una autoridad → todos los demás consumen». SF075 y todas las dependencias salen
  exclusivamente de §19 del contrato sellado. Un único generador (`extract_contract_dependencies.py`, con el parser único
  `m2a_contract_text.py`) escribe `gold/CONTRACT_S19_DEPENDENCIES.json`.
  - Falla si §19 no se puede leer, si un dueño no está en la lista cerrada declarada o si un caso citado no existe en
    el Gold.
  - La extracción queda sellada (gate `contract_dependencies_reproduce`) y no admite ningún override local.
  - El oráculo deja su `APPLICABILITY_DECISIONS.json` / `USER_DECISIONS.jsonl`; lo cambia su carril, tras el
    re-sellado.
- **Quién.** Usuario.
- **Motivo.** Que ninguna decisión de dependencia viva en dos sitios.
- **Dónde.** Contrato §19, columna `Id` y párrafo U19 (`379ca3db`); generador, parser y extracción sellados
  (`379ca3db`).
- **Si cambia, tocar.** Contrato §19; `extract_contract_dependencies.py` (`OWNERS` / `OWNER_CELLS`); matriz X05;
  documentación del oráculo (rama 8a0).

### U20. Ley de la pendiente posible (ZEUS P1-1 y P1-2, 2026-10-02)
- **Decisión.** Una `PendingClarification` sólo existe si su `frame` (base + parche sin las operaciones del span
  abierto) es una `AcceptedInterpretation` válida y cada candidato compone sobre él. Si no, `INFORM`, 0 llamadas, ancla
  intacta y sin pendiente.
  - Las causas de rechazo tienen una precedencia cerrada y se deciden antes de H1. Con dos o más causas, `INFORM` con
    el código de mayor precedencia; el mensaje nombra todas y el registro guarda sólo ese código.
  - Sobre un ancla vencida sólo puede nacer `CONTEXT_EXPIRED_CONFIRM`, y sólo si «continuar» compone y ejecuta.
  - Una lista cerrada de códigos de `CLARIFY` vale también con `INFORM`.
  - «Dame 20» con la pendiente de R3-1: mientras IR v2 rechace un `FOLLOWUP_DELTA` sólo con CARDINALITY, es §7.1
    fila 4b (`INFORM UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL`) con la pendiente intacta.
- **Quién.** Usuario (dentro de R3).
- **Motivo.** NO_GO de ZEUS sobre `4cf1cad3`: el contrato permitía pendientes imposibles y un test construía la
  respuesta saltándose el wire.
- **Dónde.** Contrato §6 paso 3b y H1, §6.7, §8.4, §10.1, A.3, INV-M2A-26 y Anexo D.3 (R3b-1); barrido, filas S08 y
  S10–S12 y pins R3b (`tests/test_m2a_contract_r3_sweep.py`); verificador G9 (pares disposición↔código) y sus mutantes.
- **Si cambia, tocar.** Contrato §6, §8.4, A.3 y D.3; matriz (filas R3b); barrido; `check_m2a_gold_contract.py`.

### U21. Gate multi-op de INV-M2A-25
- **Decisión.** Antes del re-sellado, INV-M2A-25 se comprueba con varias operaciones, a nivel contrato, sobre un modelo
  simbólico de ocupación de ranuras derivado del contrato y de las tablas del código:
  - barrido exhaustivo por parejas × ocupación de la base;
  - Hypothesis con N operaciones e invariancia a la permutación.

  Cada combinación tiene una sola respuesta representable. La misma propiedad sobre el `compose` real es gate de M2A-3
  fase 2. Las precisiones fail-closed que el barrido forzó constan en el Anexo D.3, pendientes de confirmación del
  usuario.
- **Quién.** Usuario.
- **Motivo.** El hallazgo de ZEUS era multi-operación y el barrido de R3 era de una operación.
- **Dónde.** `tests/test_m2a_contract_r3_multiop.py`; contrato §8.4 y §18 (R3b-1).
- **Si cambia, tocar.** Contrato §18 y D.3; matriz; `M2A_IMPLEMENTATION_PLAN.md` (fila M2A-3, fase 2) al integrar.

### U22. `KEEP` + `SET`, selección semántica y alcance de `REASK`
- **Decisión.**
  - `KEEP` + `SET`/`SET_ORDER` de la misma ranura ⇒ `DELTA_SLOT_CONFLICT`, aunque los valores coincidan.
  - Un candidato se selecciona por identidad semántica: (op, ranura, valor canónico) o (`REPLACE_UNIVERSE`, destino).
    Nunca por texto ni ids. Seleccionar compone el parche completo del candidato. Una edición de otra ranura es §8.3
    fila 5 (`REASK ANSWER_EDITS_OTHER_SLOT`).
  - Un rechazo con pendiente vigente vuelve al alcance decidido en R3-2: `REASK` si había pendiente.
- **Quién.** Usuario.
- **Motivo.** ZEUS P2.
- **Dónde.** Contrato §6 paso 2, §8.3 «Selección» y párrafo de códigos de rechazo (R3b-1); barrido
  (`test_d1_option1_answers_go_through_the_wire_and_select_by_semantic_identity`).
- **Si cambia, tocar.** Contrato §6 y §8.3; matriz; barrido.

### U23. §19, única autoridad de dependencias, con impacto en M2A-3
- **Decisión.** Cada fila de §19 declara:
  - ids explícitos (también `[]`) y un conteo verificable;
  - un dueño de la lista cerrada;
  - `impacto_m2a3`:
    - `LECTURA` si hace falta para pasar del lenguaje al IR; el reducer es medible;
    - `COMPOSICION` si hace falta para decidir o componer el estado; el reducer no es medible todavía.

  E-ORDER enumera sus casos en §19. SF166, SF168 y R0303 no son dependencia (H3 puro). Las alternativas selladas de
  SF192 se marcan parciales y dependientes de E-ORDER (`partial_alternatives`, `DEP-E-ORDER-CANDIDATES`), sin sellar
  lo provisional como normativo.

  El extractor falla cerrado ante:
  - un id desconocido o con otra forma;
  - un conteo que no coincide con los ids;
  - un dueño o un impacto desconocidos;
  - cualquier discrepancia con el índice o los registros sellados.

  Hay un test por cada modo de fallo.
- **Quién.** Usuario.
- **Motivo.** ZEUS P1-3.
- **Dónde.** Contrato §19 (R3b-1); `extract_contract_dependencies.py` (esquema `m2a-contract-s19/2`);
  `build_sealed_gold.py` (`U3_RECORD_FIELDS`); `tests/test_m2a_contract_s19_extraction.py`.
- **Si cambia, tocar.** Contrato §19; extractor; matriz X05; documentación del oráculo (rama 8a0).

### U24. Controles mínimos del Gold
- **Decisión.** Antes del re-sellado se etiquetan a ciegas (A/B) los controles SF196–SF202:
  - dos causas a la vez;
  - `KEEP` + `SET` de la misma ranura, con valor distinto y con el mismo valor;
  - ancla vencida cuyo candidato no puede resolverse;
  - «dame 20» con la pendiente de R3-1;
  - selección por clave semántica y edición de otra ranura.

  El Gold puede pasar de 203 casos.
- **Quién.** Usuario.
- **Motivo.** Que el Gold sellado fije el contrato R3b, no sólo el barrido.
- **Dónde.** `docs/audits/m2a_conversation/labeling_r3b/R3B_LABELER_INPUT.md` (R3b-1); adjudicación y re-sellado en
  R3b-2.
- **Si cambia, tocar.** Entrada de etiquetado; `ADJUDICATION_R3.md`; `build_sealed_gold.py` (U3); sello.

### U25. Cierre de R3b y re-sellado único (R3b-2, 2026-10-02)
- **Decisión.**
  - Las nueve precisiones que forzó el barrido multi-op quedan **decididas** tal cual (Anexo D.3):
    1. `KEEP` + `REPLACE_UNIVERSE` del universo es conflicto.
    2. El `CLEAR` de una dimensión entera cubre cada una de sus ranuras.
    3. Dos operaciones no acreditadas son dos causas, y por tanto `INFORM`.
    4. La rama de A8.1 con unidades no comparables va en la precedencia 4.
    5. H1 exige que «continuar» pase H3–H12 y las dependencias de §19.
    6. §7.1 fila 6 con pendiente vigente o ancla vencida da `INFORM`, sin pendiente nueva.
    7. La fila 4b aplica también con el ancla vencida.
    8. En H11 el `frame` es la base.
    9. La selección semántica no aplica a la fila 1 (evento).
  - Cierres de INV-M2A-25:
    - `VALUE_WITHOUT_COMPARATOR` es el código de la autoridad VALUE y pasa tal cual por la tabla rechazo→A.3
      (`OP_NOT_ACCREDITED:VALUE` ⇒ `CLARIFY VALUE_WITHOUT_COMPARATOR`).
    - `no_selection` sin pendiente es la fila 4b, `UNSUPPORTED_PROPOSAL:UNSUPPORTED_CONDITION`.

    Los dos están en el modelo multi-op y en el barrido (fila S14).
  - Adjudicaciones:
    - SF195: `SET_ORDER` con polaridad `POSITIVE`.
    - La tabla de huecos G-FU de §19 se queda sin `impacto_m2a3`.
    - La premisa de SF193 queda registrada como ya está en `ADJUDICATION_R3.md`.
    - SF196–SF201 se sellan desde las etiquetas ciegas A (Sonnet) y B (Opus), literales, con la forma de R3. SF199 toma
      la lectura de B (§7.1 fila 4b; la restricción del wire es autoridad por §0, no una dependencia de §19).
    - SF202 se etiquetó y se **retira**: los dos la marcan `AMBIGUOUS`, y la ley es mecánica (barrido y verificador).
  - `M2A_GOLD` = 209. Re-sellado único: `seal_sha256 = 3693d19f1dc9dc43d49f778ac0e7fde430846450b37909cdbe59d742fd6ae459`.
    Hay 0 movimientos DEV/TEST de ids existentes.
- **Nota para M2A-5 (productor).** Una aposición que nombra el valor vigente («con la misma entidad contratante, la
  Gobernación del Meta») se emite como un único `KEEP`, no como `KEEP` + `SET`.
- **Quién.** Usuario.
- **Motivo.** Cerrar R3 con un solo re-sellado tras la reparación del NO_GO de ZEUS.
- **Dónde.**
  - Contrato: §1.1, §6 (tabla de rechazos), §7.1 fila 4b, §19, Anexo B y Anexo D.3.
  - `build_sealed_gold.py` (`u25_additions`); `check_m2a_gold_contract.py` (G8 con `OUT_UNSUPPORTED_PROPOSAL` sólo como
    4b, dos causas ⇒ `INFORM`, `inherited` nulo sólo en `KEEP`) y sus mutantes `U25_*`.
  - `gold/ADJUDICATION_R3.md` (sección R3b); `labeling_r3b/labels_{A,B}.jsonl`; el sello.
- **Si cambia, tocar.** Contrato D.3 y Anexo B; `ADJUDICATION_R3.md`; `build_sealed_gold.py`; el sello (re-sellar con
  motivo).

## Estado

- **M2A-3 LANGUAGE R3 = FROZEN.** Se activa con el GO de ZEUS sobre el re-sellado de R3b-2 (contrato §20.4). Hasta
  entonces, R3 sigue abierto sólo para R3b.

## Decisiones del orquestador (no-producto)

### O1. El almacén va a `.gitignore`
- **Decisión.** `data/conversations/*.accepted.jsonl` se ignora en git.
- **Quién.** Orquestador (no-producto).
- **Motivo.** El almacén tiene datos personales (contrato §3.7 y §3.8); nunca va a git.
- **Dónde.** `mission/m2a-2-store`, `c6ca1ac6`.
- **Si cambia, tocar.** Documento de cierre de M2A-2; contrato §3.8 sólo si cambia la política.

### O2. `VALUE_RANGE_REPLACED` en `compose`
- **Decisión.** A `compose` se le exige `VALUE_RANGE_REPLACED` sólo con A8.1. La variante `CLEAR(VALUE)` + `SET` es eco
  de §11 (M2A-7).
- **Quién.** Orquestador (no-producto).
- **Motivo.** Separar la sustitución declarada (§6.5) del eco.
- **Dónde.** Carriles M2A-3 / 8a0 (SHA a completar).
- **Si cambia, tocar.** Plan, filas M2A-3 y M2A-7; documentación del oráculo.

### O3. Aplicabilidad del oráculo
- **Decisión.** La aplicabilidad del oráculo se ancla a §19 más reglas estructurales. `RELATIVE_TO_CLOCK` ⇒
  `NO_MEDIBLE`. La clasificación se mide sólo donde los hechos la determinan; el gate completo es de M2A-8b.
- **Quién.** Orquestador (no-producto).
- **Motivo.** No medir lo que el oráculo no puede derivar.
- **Dónde.** Rama `mission/m2a-8a0-reducer-oracle` (SHA a completar).
- **Si cambia, tocar.** Documentación del oráculo; plan, fila M2A-8.

### O4. Huecos de cobertura del Gold
- **Decisión.** Los huecos de cobertura del Gold se cubren con propiedades de M2A-3, no con casos nuevos en el Gold.
- **Quién.** Orquestador (no-producto).
- **Motivo.** El Gold sellado no crece por conveniencia del implementador (contrato §20.3).
- **Dónde.** M2A-3, fase 2 (SHA a completar).
- **Si cambia, tocar.** Plan, fila M2A-3.

### O5. Importaciones de los carriles y rutas de M2A-2
- **Decisión.** Los carriles pueden importar el vocabulario IR v2 heredado del cierre de M2A-1. M2A-2 recibe rutas
  inyectadas y no importa `conversation_store`.
- **Quién.** Orquestador (no-producto).
- **Motivo.** Que el cierre de imports siga siendo el de cada paso (U1).
- **Dónde.** `mission/m2a-2-store`, `5f86357f` (`foundation_accepted_store.py`, ruta inyectada).
- **Si cambia, tocar.** `M2A_WAVE1_PREP.md` §2; plan §0.7; documento de cierre de M2A-2.

## SHAs de esta rama (`mission/m2a-contract-enm-r3`, base `fa0b6de8`)

| SHA | Qué |
|---|---|
| `507103cb` | fase A: barrido y borradores (propuesta) |
| `cb32361a` | B1: enmienda R3 aplicada al contrato; barrido sobre el texto enmendado |
| `44a3a01c` | B1: entrada para el etiquetado ciego y procedimiento B4 |
| `a90d6ad0` | B1-fix: D-1 opción 1, D-6 aprobado, Anexo D.2 resuelto |
| `76303e02` | B1-fix: este registro (U1–U17, O1–O5) |
| `379ca3db` | B4: re-sellado único r3 (sello `3fdd81df…`), U3, P-3 (U18) y §19 sellado (U19) |
| `4cf1cad3` | B4: U18, U19 y casos adjudicados en la matriz |
| `4636a73a`, `b8372757` | R3b-1: reparación del NO_GO de ZEUS (U20–U24), sin re-sellar: contrato, herramientas y gates; después, entrada de etiquetado, registro y matriz |
| commit siguiente a `b8372757` | R3b-2: cierre y re-sellado único (U25), sello `3693d19f…` |

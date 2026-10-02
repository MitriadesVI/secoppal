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
| commit siguiente a `379ca3db` | B4: U18, U19 y casos adjudicados en la matriz |

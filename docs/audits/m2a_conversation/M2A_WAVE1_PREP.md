# M2A — Preparación de la ola 1: decisiones del usuario (2026-10-02)

**Rama:** `mission/m2a-wave1-prep` (desde `6be95aaf`). Sólo tests y docs; nada bajo `app/`. Este documento registra
decisiones; no edita el contrato (`docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`) ni el Gold sellado. El plan
(`M2A_IMPLEMENTATION_PLAN.md`) quedó enmendado en §0.6, §0.7, M2A-2, M2A-6 y §2.

## 1. Registros por paso

Los módulos M2a se registran en `tests/m2a_steps/m2a_<n>.py`, uno por paso (`STEP`, `MODULES`, `DEPENDS_ON`,
`CLOSURE`). Cada carril (M2A-2, M2A-3) escribe **sólo su archivo**: ninguna lista central compartida. `CLOSURE` es el
cierre transitivo exacto de `MODULES` **menos** el de los pasos de los que depende (su incremento propio), de modo que un
cambio en M2A-1 no rompe el registro de M2A-2. El registro se valida (`registry_problems`): módulos bajo `app.core.`,
existentes, registrados una sola vez; pasos únicos; `DEPENDS_ON` conocido, no propio, sin ciclos.

## 2. Invariante definitiva de consumo (sustituye `test_m2a_has_no_consumer_yet`)

El consumo interno de M2a está permitido **únicamente entre módulos explícitamente registrados** y a lo largo de
`DEPENDS_ON`. Ningún módulo externo (`app/`, `scripts/`, `streamlit_app`), el cierre legacy ni el runtime Foundation
alcanza M2a. Registrar un módulo no abre ninguna puerta: un módulo legacy registrado sigue prohibido en el cierre y sigue
siendo consumido por legacy. Pin: `tests/test_m2a_boundaries.py`.

## 3. Enmienda C1.x (`tests/test_foundation_authority_contract.py`)

Antes: nada bajo `app/` importa `foundation_accepted`. Ahora: **los importadores bajo `app/` ⊆ módulos M2a registrados**.
`CLOSED_TYPE_CONSUMERS == {"foundation_accepted"}` se mantiene: los demás módulos M2a **nunca** importan
`foundation_authority_values` ni `foundation_geo_level` directamente, decodifican vía `foundation_accepted`; registrarse no
concede el consumo de tipos cerrados. Se mantienen el resto de comprobaciones (sin `resolve`, sin módulos de autoridad).

## 4. `foundation_accepted.py`: adyacente exclusivo de M2A-3

`app/core/foundation_accepted.py` es adyacente **exclusivo** de M2A-3, sólo para invariantes y canonicidad compartidas
con el futuro M2A-1b. M2A-2 lo importa y **no** lo modifica. Nada se ordena a ciegas: primero se demuestra qué
colecciones son set-like y cuáles tienen orden observable. Las policies duplicadas sí se rechazan.

## 5. Carril M2A-8a0 (oráculo del reducer)

Autor distinto del de M2A-3. El oráculo no importa ni llama al reducer. Se congela **antes** de implementar `compose`.
M2A-3 itera sólo contra **DEV**; **TEST** se usa al congelar.

## 6. El contrato §6.6 no autoriza `foundation_admission`

§6.6 del contrato (las políticas de petición se re-deciden sobre la efectiva compuesta) **no** autoriza importar ni copiar
`foundation_admission`. Toda política que dependa de E-\*/F queda **fail-closed con dueño declarado** hasta que exista su
autoridad.

## 7. Regla del manifiesto

`tests/fixtures/legacy_closure_manifest_88653752.json` **no** se regenera para legitimar un módulo legacy tocado. El módulo
se declara como divergencia explícita (`ALLOWED_DIVERGENCE` de `tests/test_m1_wave1_parity_pins.py`) y se protege con un pin
que sólo permita el delta declarado (para M2A-2, el de `ConversationStore.clear()`; patrón del pin 6 de `foundation_guard`:
fuente de `main` como fixture + comparación AST). Razón verificable: `test_the_allowed_divergences_are_real_and_inside_the_closure`
(`tests/test_m1_wave1_parity_pins.py:960-963`) exige que toda `ALLOWED_DIVERGENCE` difiera del manifiesto; regenerarlo
rompe los pines 4–6.

## 8. Deuda separada

`DEBT_CLEAR_GENERATIONS_RETENTION.md`: `ConversationStore.clear()` no borra `.generations/<chat>/`. Preexistente; M2A-2 la
caracteriza y no la repara sin autorización del usuario.

## 9. Evidencia de los pines de esta preparación

Cada pin nuevo o cambiado trae mutantes que caza y controles positivos (registro malformado, cierre con un elemento de
menos o de más, paso que importa a otro sin `DEPENDS_ON`, módulo no registrado que importa M2a, legacy y runtime que lo
alcanzan, módulo registrado que importa `foundation_authority_values`). Los mutantes usan módulos **virtuales** (fuentes en
memoria): ningún mutante crea archivos. Se verificó además por meta-mutación (neutralizar cada comprobación por turnos y
confirmar que sus mutantes caen en rojo).

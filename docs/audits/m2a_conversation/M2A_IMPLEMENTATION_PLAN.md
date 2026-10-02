# M2A — Plan de implementación (propuesta; NO implementado)

**Fecha:** 2026-10-01. **Contrato:** `docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`. **Gold:** `M2A_GOLD` sellado
(`docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json`). Este plan no es normativo: el contrato manda; el **orden relativo** a
C1/E/F/G, ACTIVATE y CUTOVER es el de ADR-103 Enm. 4 §4.4, que este plan respeta.

Principios: commits pequeños y revertibles; un escritor por archivo; ningún paso hasta M2A-6 cambia el comportamiento
de runtime; cada paso trae sus mutantes (un test que pasa sin matar su mutante no cuenta).

## 0. Prohibiciones que todos los pasos heredan

1. No existe ningún camino `AcceptedInterpretation`/`SemanticPatch` → `parsed_params` / `QueryFrame` /
   `ConversationContext` / `FollowupEngine` / `QueryRouter`. `Turn.parsed_params` sigue `{}`.
2. Ningún módulo nuevo de M2a importa `query_router`, `followup_engine`, `followup_mutations`, los detectores de dueño
   ni `accreditation` (salvo la hoja `accredited_span`). Lo fija un pin de imports en M2A-1 (INV-M2A-04).
3. Ningún módulo nuevo de M2a usa regex ni léxicos sobre el texto del turno (INV-M2A-24, pin AST).
4. Ningún caso de `FOLLOWUP_FRONTIER` entra en los gates; si un gate de M2a lo necesita ⇒ `BLOCKER`.
5. No se toca `app/data/**`, `foundation_authority_contract.py` ni los tipos de `foundation_authority_values.py`
   (ADR-103 §4.3, §4.8).
6. Tocar un módulo `.py` del cierre legacy (`conversation_store.py`, `orchestrator.py`, …) exige regenerar el
   manifiesto `legacy_closure_manifest_88653752.json` con motivo declarado en el commit y ZEUS (ADR-103 D1(b)).

## 1. Pasos

| Paso | Contenido | Archivos (dueño exclusivo) | Depende de | Cambia runtime | Gates del paso |
|---|---|---|---|---|---|
| **M2A-1 tipos/contrato** | `AcceptedInterpretation`, `AcceptedRequest`, `AcceptedCondition`, `ConditionOrigin`, `Provenance`, `SemanticPatch`, `PatchOp`, `PendingClarification`, `CandidatePatch`, `ResultSetRef`, `EchoItem`, `AcceptedTurnRecord`, `TurnScope`, `TurnResolution` con `to_dict`/lector estricto; `from_acceptance`; directivas del wire 5.1 (`CLEAR`, `KEEP`, `CANCEL_PENDING`) y etiquetas `UNSUPPORTED` reservadas (`relative_to_previous`, `no_selection`) | NÚCLEO: `app/core/foundation_accepted.py` (nuevo), `app/core/interpretation_contract.py` (directiva `ProposedSlotDirective`), `tests/test_foundation_accepted_contract.py` (nuevo), pin de imports/AST en `tests/test_m2a_boundaries.py` (nuevo) | C1.2 en `main` (hecho) | No | ida y vuelta de todos los tipos; lector estricto (clave/esquema desconocido ⇒ error); determinismo de `interpretation_hash`; INV-M2A-04, -14, -24 (pines); `test_m1_wave1_parity_pins.py` verde |
| **M2A-2 store append-only** | `foundation_accepted_store.py`: append bajo `conversation_lock`, lector estricto, registro antes del `Turn`, base sólo si su `trace_id` sigue en el historial; borrado con el historial | NÚCLEO: `app/core/foundation_accepted_store.py` (nuevo), `tests/test_foundation_accepted_store.py` (nuevo); **adyacente declarado**: una línea en `ConversationStore.clear()` + regeneración del manifiesto | M2A-1 | No (nadie escribe aún) | INV-M2A-09; concurrencia (dos escritores, cola truncada, esquema desconocido ⇒ ancla inválida); retención = historial |
| **M2A-3 reducer puro** | `compose(base, patch)`, ranuras, orden fijo, arrastre por identidad, reglas H1–H12, sustituciones declaradas (A8.1, F16, F3, F9), `DeltaRefusal`; `classify_turn` (§7: A9/A9.1/F22) sobre P ya admitida | NÚCLEO: `app/core/foundation_delta.py`, `app/core/foundation_turn_scope.py` (nuevos) + tests | M2A-1 | No | INV-M2A-01, -02, -03, -05, -11, -16, -18, -19, -20 con IR oráculo del Gold (vía M2A-8a): **100 %** de disposición, código y efectiva en los casos sin dependencia de carril; los dependientes (contrato §19) se miden al existir su regla de recibo y hasta entonces cuentan como rojo declarado, nunca dispensado; mutantes por regla |
| M2A-3b herencia | `foundation_authority_inheritance.py`: entrada separada de insumos del host (unidad, año, TimeValue previo) | NÚCLEO (nuevo) + bloque del kit | M2A-3 | No | INV-M2A-07, -17 |
| M2A-3c reglas de recibo | `unit.*`, `unit.sibling`, `unit.inherited`, `unit.smmlv`, `year.*`, `time.relative_to_previous`, `buyer.territory.*`/`buyer.national`, `order.direction_defaulted`, `topn.capped` | carriles: D-MONEY, D-TIME, D-BUYER, NÚCLEO (E-ORDER) — **cada uno en sus propios archivos de autoridad** | M2A-3b; E-* | No | kit por autoridad; casos V05–V09, M01–M05 |
| **M2A-4 pending clarification** | `foundation_pending.py`: creación con span y `candidate_patches` resueltos al preguntar; protocolo §8.3; `CANCEL_PENDING` §17; caducidad; tope de `asks` | NÚCLEO: `app/core/foundation_pending.py` (nuevo) + tests | M2A-3 | No | INV-M2A-08, -23; P01–P10; mutantes F24/F25/F29 y «lista vacía como cancelación» |
| **M2A-5 producer/wire de delta** | tarjeta de estado sin valores; sección de estado del prompt v5.1; emisión de directivas y de evidencia de continuación (`FOLLOWUP_DELTA`, `referent_evidence`); **señal estructural extensional/intensional del referente** (G-FU-14 diferido aquí; la ejecución sobre filas sigue en M2b) | NÚCLEO: `app/core/foundation_state_card.py` (nuevo), `app/core/foundation_prompts_v5.py` | M2A-1; C1.3 (wire v5) | No (perfil v5.1 no activo) | **señal extensional/intensional del referente emitida y probada (G-FU-14): gate de M2A-5 y precondición de activar M2A-6**; INV-M2A-13; hasta 3 iteraciones sobre **DEV** de los dos Gold (ADR-103 §4.4 «M2A-4 = v5.1»); N01–N04 con productor real |
| **M2A-7 presentation/echo** | `foundation_echo.py` puro: `echo_items(base, efectiva, patch, record)`; regla de cobertura §11 | NÚCLEO: `app/core/foundation_echo.py` (nuevo) + tests | M2A-3 | No | INV-M2A-12, -21; eco mínimo del Gold ⊆ emitido ⊆ justificado |
| **M2A-6 runtime integration** | `foundation_runtime`: escribir el registro en todo turno Foundation; alcance §7.1; componer/ejecutar; ancla y vigencia §10; `INFORM` de lo que M2a no hace; fila `FOLLOWUP_DELTA` por dueño del ancla; página y eventos (`handle_foundation_page`, eventos de orden/filtro/continuar/reset) antes de `handle_pagination`; barrera Radar-SAVE (`INFORM OPERATION_CHANGE:FOLLOW`); retirar `continues_foundation_turn` **sólo** en el perfil v5.1 | NÚCLEO: `foundation_runtime.py`, `foundation_route_owner.py`, `foundation_operation_host.py`, `foundation_presentation.py`, `foundation_messages.py`, `app/core/handlers/foundation_page.py` (nuevo); **adyacente declarado**: tupla de handlers en `orchestrator.py` (+ regeneración del manifiesto); ARES no-núcleo: `app/core/workflow/handlers/radar.py` | M2A-2, -3, -4, -5 (con la señal extensional en verde), -7; C1.4; E-* por clase; F (vista); G (caída del proveedor) | **Sí**, tras flag del perfil | INV-M2A-04 (espías: 0 turnos con ancla Foundation en `handle_normal_flow`; `parsed_params == {}`), -06 (replay del primer turno), -10, -15, -20, -22; R01–R06 |
| **M2A-8 Gold/replay/gates** | 8a: harness de replay offline con IR oráculo (`scripts/audits/m2a_conversation/`) que alimenta a M2A-3/-4/-7; 8b: gates de §20.2 del contrato con productor real, peor de ≥2 pasadas, DEV para iterar y TEST para activar | HERA: `scripts/audits/m2a_conversation/*`, `tests/test_m2a_*.py` de gates | 8a: M2A-3; 8b: M2A-6 | No | gates §20.2 = 0; el sello verifica (`m2a_gold_seal.py verify`) |
| M2A-9 freeze | freeze del SHA, ZEUS fresco ‖ suite global pareada, semgrep delta con `--base` | HERA + ZEUS | todo | — | ADR-103 §4.4 «M2A-9» |

### 1.1 Correspondencia con los nombres de ADR-103 Enm. 4 §4.4

| ADR §4.4 | Este plan |
|---|---|
| M2A-0 (Gold, H7b, sello, partición) | **hecho en esta misión** |
| M2A-1 (núcleo; directivas en el wire 5.1) | M2A-1 |
| M2A-2 | M2A-2 |
| M2A-3 → M2A-3b/3c | M2A-3, M2A-3b, M2A-3c |
| M2A-4 = v5.1 (tarjeta, ancla, fila `FOLLOWUP_DELTA`, eco, prompt v5.1) | M2A-5 (tarjeta y prompt) + M2A-7 (eco) + la parte de ancla de M2A-6 |
| M2A-5 (pendiente) | M2A-4 |
| M2A-6 (página y eventos, F) | dentro de M2A-6 |
| M2A-8 (barrera de Radar, ARES no-núcleo) | dentro de M2A-6 (archivo propio, escritor propio) |
| M2A-9 | M2A-9 |

La renumeración es interna a este plan y no cambia ninguna dependencia de ADR-103 (C1.3 antes del wire v5.1; E-* por
clase; F antes del CUTOVER; activación de v5.1 sobre TEST_B y sobre TEST del Gold M2a).

## 2. Secuencia propuesta y paralelismo

```text
M2A-1 ──┬── M2A-2 (store) ─────────────────────────────┐
        ├── M2A-3 (reducer) ─┬─ M2A-3b ─ M2A-3c (carriles, en sus archivos)
        │                    ├─ M2A-4 (pending) ───────┤
        │                    ├─ M2A-7 (echo) ──────────┤
        │                    └─ M2A-8a (replay oráculo, HERA)
        └── M2A-5 (wire/prompt; espera C1.3) ──────────┴── M2A-6 (runtime) ── M2A-8b ── M2A-9
```

Paralelo **sin dos escritores sobre el mismo archivo**:
- tras M2A-1: M2A-2 ‖ M2A-3 ‖ M2A-5 (archivos disjuntos: store / delta+turn_scope / state_card+prompts_v5);
- tras M2A-3: M2A-4 ‖ M2A-7 ‖ M2A-3b ‖ M2A-8a (pending / echo / inheritance / scripts de HERA);
- M2A-3c corre en cada carril D-* sobre sus propios archivos de autoridad.
- M2A-6 es **un solo escritor** (toca el runtime y los dos adyacentes del cierre legacy).

**Mejora sobre el orden sugerido:** el eco (M2A-7) y el harness de replay (M2A-8a) van **antes** de la integración
(M2A-6), no después: el eco es puro y la integración lo necesita; el harness es el oráculo con el que M2A-3 y M2A-4
demuestran el 100 % sin productor ni runtime.

## 3. Riesgos y decisiones que el plan deja visibles

| Riesgo | Mitigación |
|---|---|
| Dos adyacentes en el cierre legacy (`conversation_store.clear`, tupla de `orchestrator`) | cambio declarado + regeneración del manifiesto + ZEUS (ADR-103 D1(b), §4.6); alternativa a evaluar en M2A-2: purga perezosa ligada a `trace_id` (no cumple «retención = historial» estricta, por eso no se recomienda) |
| El productor no emite evidencia de continuación (§7.2 a) | garantía limitada declarada; 3 iteraciones sobre DEV; si no, no hay CUTOVER (ADR-103 §4.8) |
| Dependencias de carriles (E-TIME `time.relative_to_previous`; E-VALUE D10; E-BUYER `buyer.territory`; F `HostViewDecision`) | casos dependientes listados en el contrato §19; mientras falte la regla, su caída segura es `CLARIFY` y el gate de esos casos queda en rojo declarado, nunca se dispensa |
| Almacén con datos personales | misma política que el historial; nunca a git/traza/ledger |
| `foundation_v5_previous` | debe tratar el almacén M2a como ancla inválida (ADR-103 D8); test en M2A-6 |

## 4. Fuera de este plan

Todo lo de `FOLLOWUP_FRONTIER` (contrato §19), D-BUYER, D-ACTOR, Suppliers, MULTI, M2b, M3.

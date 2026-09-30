# Inventario selectivo — FOUNDATION_V3_SELECTIVE_PROMOTION_V1

Origen: `audit/foundation-v3-heldout-da6-zeus` = `f703504b` (102 commits sobre
`main` = `68090765`, merge-base = main). Diff total: 154 archivos, +312 745 líneas.
Nada se copia por commit; cada archivo promovido se restaura desde `f703504b` y se
adapta con deltas explícitos sobre `main`.

Clasificaciones: `PROMOTE_RUNTIME`, `PROMOTE_TEST`, `PROMOTE_DOC`,
`SEALED_REGRESSION`, `EXPERIMENT_ONLY`, `DO_NOT_COPY`, `SUPERSEDED`.

## 1. Runtime (`app/`)

| Archivo | Commit origen | Clase | Delta frente al origen | Consumidor | Motivo / evidencia | Riesgo | Alternativa descartada |
|---|---|---|---|---|---|---|---|
| `app/core/interpretation_contract.py` | `a1b16f2d` | PROMOTE_RUNTIME | ninguno | productor, admisión, H7, proyección, guard | contrato inmutable propuesta→aceptación→proyección; base de todo el held-out | bajo (módulo nuevo) | reescribir contrato: pierde la evidencia acreditada |
| `app/core/semantic_dimensions.py` | `a1b16f2d` | PROMOTE_RUNTIME | +2 miembros (`UNSUPPORTED`, `RETRIEVAL_INTENT`) | contrato Foundation | necesarios para el contrato; ninguna iteración sobre el enum en `app/`/`tests/` (grep) | bajo | enum paralelo: duplica autoridad |
| `app/core/foundation_prompts.py` | `56f67755` | PROMOTE_RUNTIME (recortado) | sólo v4.2 brazo B; se retiran v3/v4/v4.1 y el brazo C; texto byte-idéntico, hash fijado en test | productor B | prompt canónico (SHA `a5867d40…`) | medio: un byte cambia el prompt → test de hash | copiar las 4 versiones: deja perfiles experimentales alcanzables |
| `app/core/foundation_universe_kind.py` | `5c85a510` | PROMOTE_RUNTIME | ninguno (sin default; `MISSING_UNIVERSE_KIND`) | decodificador | `universe_kind` obligatorio (ADR-101 §2) | bajo | `request.pop("universe_kind","records")`: prohibido |
| `app/core/foundation_producers.py` | `56f67755` | PROMOTE_RUNTIME (recortado) | productor sólo brazo B y versión 4.2; se elimina la selección B/C por config; el decodificador conserva el brazo C sólo como formato de fixture de tests | runtime | productor medido en held-out = B v4.2 | medio | dejar `SECOPPAL_FOUNDATION_PRODUCER` (default C en origen): perfil no medido |
| `app/core/foundation_admission.py` | `5c85a510` | PROMOTE_RUNTIME + fixes Fase 4 | G1/T4, disyunción de estados, operador monetario | runtime | resolución determinista del host | medio (fixes) | nuevo parser monetario global: prohibido |
| `app/core/foundation_projection.py` | `25383f84` | PROMOTE_RUNTIME | fix G2 (si aplica) + guarda global explícita | runtime, guard | proyección con recibos por cláusula | medio | — |
| `app/core/foundation_guard.py` | `25383f84` | PROMOTE_RUNTIME | ninguno | secop_client, runtime | guard de ejecución y de llamadas efectivas (no es B_GUARDAS) | bajo | — |
| `app/core/foundation_token_plan.py` | `25383f84` | PROMOTE_RUNTIME | ninguno | proyección, runtime | catálogo cerrado de tokens (N7) | bajo | — |
| `app/core/foundation_text_predicates.py` | `8d84f565` | PROMOTE_RUNTIME | ninguno | token plan, soql_builder | predicado Unicode + cláusula LIKE plegada | bajo | — |
| `app/core/foundation_universe_policy.py` | `5c85a510` | PROMOTE_RUNTIME (recortado) | se elimina el campo `complements` del resultado (el complemento deja de ser representable) | runtime | H7 núcleo obligatorio | bajo | conservar `complements=()`: deja un hueco estructural para reactivar |
| `app/core/foundation_runtime.py` | `5c85a510` | PROMOTE_RUNTIME (reescrito en la integración) | sin `with_tender_complement`; H7 siempre; entrada después de los dueños; mensajes humanos; traza | orchestrator `_dispatch` | ver `ROUTE_OWNERSHIP_MATRIX.md` | alto (ruteo) | hook en `_admit_and_dispatch` antes de `coordinate_turn` (preempta dueños) |
| `app/core/foundation_tender_complement.py` | `5c85a510` | DO_NOT_COPY | — | — | `HISTORICAL_TENDER_COMPLEMENT` = NOT_EXERCISED, WIDENING, requiere continuación inexistente | — | — |
| `app/core/source_call_receipts.py` | `05632d9c` | PROMOTE_RUNTIME | ninguno | secop_client, helpers, observer, feedback, orchestrator | recibos por llamada lógica | bajo: no-op sin colector | — |
| `app/core/secop_client.py` | `4968dec2` | PROMOTE_RUNTIME | ninguno | todo el producto | recibos + guard efectivo; sin colector/guard = ruta legado idéntica | medio (superficie global) | — |
| `app/core/soql_builder.py` | `168f309b` | PROMOTE_RUNTIME + delta | la exención de la guarda anti-global por O1B queda **sólo** con opt-in explícito de Foundation (`allow_opportunity_only`) | legado + Foundation | en `f703504b` O1B satisfacía la guarda para todos los llamadores: cambio observable con flag OFF | alto si no se acota | copiar tal cual: rompe paridad flag OFF |
| `app/core/workflow/helpers.py` | `bc030d1b` | PROMOTE_RUNTIME | ninguno | ejecución legado | etiqueta de recibos; no-op sin colector | bajo | — |
| `app/core/workflow/actions/execution.py` | `bc030d1b` | PROMOTE_RUNTIME | ninguno | ejecución legado | etiqueta H4/relajación | bajo | — |
| `app/core/workflow/dual_dataset.py` | `bc030d1b` | PROMOTE_RUNTIME | ninguno (re-pin del SHA en `test_truth_layer_public_parity.py`) | dual | etiqueta `dual_child:<ds>` | bajo | — |
| `app/core/observer.py` | `bc030d1b` | PROMOTE_RUNTIME | ninguno | observer | propagación de contexto de recibos | bajo | — |
| `app/core/feedback.py` | `bc030d1b` | PROMOTE_RUNTIME | ninguno | traza | añade `source_call_receipts` sólo con colector activo | bajo | — |
| `app/core/orchestrator.py` | `05632d9c` | PROMOTE_RUNTIME (parcial) | se toma el ciclo de recibos; **no** se toma el hook previo a `coordinate_turn`; entrada nueva en `_dispatch` | workflow | ver Fase 3 | alto | copiar el hook: preempción |
| `app/config.py` | `5c85a510` | PROMOTE_RUNTIME (recortado) | se conservan `SECOPPAL_FOUNDATION_MODE` (`legacy`/`foundation_v3`, default `legacy`) y `SECOPPAL_FOUNDATION_SOURCE_RECEIPTS_ENABLED`; se eliminan `SECOPPAL_FOUNDATION_PRODUCER` y `SECOPPAL_FOUNDATION_UNIVERSE_POLICY` | runtime | H7 inseparable; productor único | medio | perfil `foundation_v3 + H7 off`: prohibido |

## 2. Tests

| Archivo | Commit | Clase | Nota |
|---|---|---|---|
| `tests/test_interpretation_contract.py` | origen | PROMOTE_TEST | contrato |
| `tests/test_foundation_prompts.py` | `56f67755` | PROMOTE_TEST (adaptado) | sólo v4.2/B; hash `a5867d40…` fijado |
| `tests/test_foundation_runtime.py` | origen | PROMOTE_TEST (adaptado) | sin perfil H7 OFF productivo; sin complemento |
| `tests/test_foundation_projection.py` | origen | PROMOTE_TEST | — |
| `tests/test_foundation_universe_policy.py` | origen | PROMOTE_TEST (adaptado) | casos del complemento retirados |
| `tests/test_foundation_text_predicates.py` | origen | PROMOTE_TEST | — |
| `tests/test_foundation_token_runtime.py` | origen | PROMOTE_TEST (adaptado) | — |
| `tests/test_source_call_receipts.py` | origen | PROMOTE_TEST | — |
| `tests/product_contract/test_source_call_receipts_workflow.py` | origen | PROMOTE_TEST | — |
| `tests/product_contract/harness.py`, `frozen_source.py` | `762de463`, `168f309b` | PROMOTE_TEST | soporte de `foundation_producer`, `settings_overrides`, `schemas`, `snapshot` |
| `tests/product_contract/test_product_contract_v1.py` | origen | PROMOTE_TEST | firma `_execute(**kwargs)` |
| `tests/test_truth_layer_public_parity.py` | `aaec1e7f` | PROMOTE_TEST (re-pin) | SHA de `soql_builder.py` y `dual_dataset.py` se re-pinean al byte promovido, con motivo |
| `tests/test_foundation_h7_heldout_metrics.py`, `tests/test_foundation_v3_heldout_package.py`, `tests/foundation_v3_heldout/*` | varios | EXPERIMENT_ONLY | instrumento de medición del held-out |
| `tests/product_contract/foundation_v3_*` (adapters, bc_*, e2a, gold_package, oracles, replay, sealed_loader, verifier, …) y `test_foundation_v3_*`, `test_interpretation_foundation_v3.py` | varios | EXPERIMENT_ONLY | laboratorio Gold/B-C; dependen de 300 k líneas de fixtures y capturas pagadas |

## 3. Fixtures y evidencia

| Elemento | Clase | Decisión |
|---|---|---|
| `tests/fixtures/interpretation_foundation_v3_heldout/*` (paquete DA-6) | SEALED_REGRESSION → **no se copia** | Opción **B**: evidencia externa referenciada por digest `bc0381e4…` y acta `908eb552…`. Motivo: el paquete sólo es ejercitable con propuestas capturadas del proveedor (pagadas, DO_NOT_COPY); sin ellas no es una regresión offline, y copiarlo a CI lo presentaría como algo que no es. Se crea un corpus de regresión separado, sintético y sin la palabra held-out. |
| `tests/fixtures/interpretation_foundation_v3/*` (development.yaml, GOLD_*) | EXPERIMENT_ONLY | Gold de desarrollo |
| capturas, ledgers, freezes, `metrics_replay.json`, `proximity_result.json` | DO_NOT_COPY | gasto y salidas de proveedor |
| `artifacts/audits/foundation_v3_da6/ACTA_DA6_HELDOUT_20260929.json` | PROMOTE_DOC (referencia) | se cita por hash en PREFLIGHT; no se copia |

## 4. Scripts

Todos `EXPERIMENT_ONLY`: `scripts/foundation_v3_*`, `scripts/foundation_h7_*`,
`scripts/audits/foundation_h7_mutations.py` (constructor del held-out, medición B/C,
costo, sondas N7 en vivo).

## 5. Documentación

| Documento | Clase |
|---|---|
| `docs/decisions/2026-09-29_ADR-101_foundation-host-universe-policy.md` | PROMOTE_DOC (byte-idéntico; su estado queda superado por ADR-102) |
| Aclaración de ADR-100 (commit `241e2540`) | PROMOTE_DOC (decisión humana que la proyección de estado de Foundation aplica) |
| entrada ADR-101 de `docs/decisions_log.md` | PROMOTE_DOC |
| `docs/audits/foundation_v3_bc_v4/*`, `docs/audits/foundation_v3_n7_whole_token_v1/*` | EXPERIMENT_ONLY (se citan por ruta y commit; no se copia el cementerio documental) |

## 6. Símbolos explícitamente excluidos del runtime

- `with_tender_complement`, `RequestComplement`, `HISTORICAL_TENDER_COMPLEMENT`.
- `host_universe()` compuesto con el complemento.
- `LinguisticProducer("C", …)` como productor configurable; prompts v3/v4/v4.1.
- `secoppal_foundation_universe_policy` (`off`/`h7`) y `secoppal_foundation_producer`.
- B_GUARDAS (`tests/product_contract/foundation_v3_bc_guards.py`): G1/G2 → invariantes de admisión (Fase 4); G3 → absorbida por H7; G4 → telemetría sin poder ejecutivo.

## 7. Dependencias verificadas

`foundation_runtime` → admission, guard, projection, token_plan, universe_policy,
producers, source_call_receipts, `socrata_transport.Soda3OrderingSpecifier`,
`workflow.results.empty_result`. `foundation_projection` →
`buyer_source_names.project_buyer_names`, `contract_universe`, `modality_dataset_projection`,
`modality_filter`, `opportunity_retrieval_policy`, `data.departamentos` (todos en `main`).
`foundation_admission` → `contract_type_resolver`, `entity_resolver`, `modality_resolver`,
`utils.money` (en `main`).

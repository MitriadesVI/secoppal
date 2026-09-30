# Gates por componente — FOUNDATION_V3_SELECTIVE_PROMOTION_V1

Intérprete `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python` (3.14.7), pytest 9.0.2,
red bloqueada con `scripts/audits/pytest_netblock.py` (`-p netblock`), sin `.env` ni claves.

## Focales Foundation (candidato `0dc5985a` y posteriores sólo-docs)

Comando: `python -m pytest -p netblock -p no:cacheprovider tests/test_foundation_*.py
tests/test_interpretation_contract.py tests/test_source_call_receipts.py
tests/product_contract/test_source_call_receipts_workflow.py -q` → **1855 passed**, exit 0.

| Componente | Archivo | Resultado |
|---|---|---|
| Contrato de interpretación | `tests/test_interpretation_contract.py` | 20 passed |
| Prompt B v4.2 congelado + productor/wire | `tests/test_foundation_prompts.py` | 20 passed |
| Fronteras (complemento, `universe_kind`, perfil único) | `tests/test_foundation_promotion_boundary.py` | 8 passed |
| Proyección | `tests/test_foundation_projection.py` | 23 passed |
| H7 (y exclusión del complemento en runtime) | `tests/test_foundation_universe_policy.py` | 86 passed |
| Predicados de texto | `tests/test_foundation_text_predicates.py` | 38 passed |
| Plan de tokens | `tests/test_foundation_token_runtime.py` | 11 passed |
| Runtime, guard de ejecución, H4, conteo, SELECT, builder opt-in | `tests/test_foundation_runtime.py` | 56 passed |
| Recibos de llamadas | `tests/test_source_call_receipts.py`, `tests/product_contract/test_source_call_receipts_workflow.py` | 4 + 5 passed |
| Propiedad de rutas (flag ON vs OFF, Frame ON/OFF, Alpha) | `tests/test_foundation_route_ownership.py` | 23 passed |
| Rutas silenciosas (G1/T4, G2, estados, operador monetario) | `tests/test_foundation_silent_paths.py` | 49 passed (incl. 3072 composiciones de estado y el léxico monetario completo) |
| UX de aclaraciones | `tests/test_foundation_messages.py` | 1478 passed |
| Fallback rotulado | `tests/test_foundation_basic_fallback.py` | 18 passed |
| Puertas de producto (corpus de regresión, primera infancia) | `tests/test_foundation_product_gates.py` | 16 passed |

## Preservación de capacidades (comando único, verde en Fase 3)

`tests/test_capability_binding_suppliers_contract.py`,
`tests/test_capability_binding_suppliers_workflow.py`, `tests/test_a2_suppliers_dogfood_promotion.py`,
`tests/test_contract_activity_alpha_replay_v2.py`, `tests/test_natural_follow_binding.py`,
`tests/test_entity_profile_action_admission.py`, `tests/test_actor_flag_off.py`,
`tests/test_truth_layer_public_parity.py`, `tests/product_contract/` → verdes. La suite global
(`GLOBAL_DELTA.json`) los incluye junto con Radar, direct lookup, follow-up, paginación,
estados de proceso, recuperación de oportunidades y configuración.

## Corpora de auditoría (`make audit-*`, red bloqueada, base vs candidato)

| Corpus | Casos | Base P/F/E | Candidato P/F/E | Diferencias de veredicto |
|---|---|---|---|---|
| negation_focal_v1 | 24 | 22/2/0 | 22/2/0 | 0 |
| time_v2_focal | 32 | 31/1/0 | 31/1/0 | 0 |
| control_language_v1 | 42 | 41/1/0 | 41/1/0 | 0 |
| actor_hygiene_v1 | 36 | 35/1/0 | 35/1/0 | 0 |
| hca_final_p1_fixes | 40 | 40/0/0 | 40/0/0 | 0 |

`results.jsonl` idéntico en los tres primeros; en los dos últimos sólo difieren campos de
reloj o aleatorios (hora «consultado el …», `selection_token`, `_opportunity_reference_instant`).
Los corpora pytest (query, dialogue, modality, money, actor, rated/eval, product contract)
corren dentro de la suite global.

## Estáticos

- `ruff check --select E,W,F --ignore E501` sobre todos los `.py` del diff: 1 hallazgo, el
  F841 preexistente de `app/core/observer.py:106` (también en `main`).
- `make lint-core`: `app/core/ limpio`.
- `olympus-semgrep-delta --base 68090765…` sobre el diff: TOTAL=1, BASELINE=1, NEW_DELTA=1,
  FIXED=1; es el mismo `except-exception-que-silencia` de `degrade_query`
  (`execution.py` 404–422 en base, 406–425 en candidato), desplazado dos líneas por el
  argumento `call_reason`. Delta efectivo: 0.
- `git diff --check 68090765 HEAD`: limpio.
- Greps: 0 referencias al complemento en `app/`; 0 defaults de `universe_kind` en `app/`;
  sólo `secoppal_foundation_mode` y `secoppal_foundation_source_receipts_enabled` en config.
- Sin formateador configurado en el repo (`ruff format` no es gate); no se reformateó código.

## Mutaciones

20/20 muertos por aserción del test de la propiedad (`MUTATION_REPORT.json`).

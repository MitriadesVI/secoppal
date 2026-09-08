# V1.2-B — Inventario de archivos y responsabilidad de cada cambio

Registrado ANTES de editar, como exige la orden. Base congelada
`4c86d5bafaa3296b73c6523465b33efa4311910a`; HEAD de codigo probado del cierre
R3d `e4b85db855d50f7e5671a515bfe1b26c882c2867` (ancestro verificado).

## Archivos nuevos en `app/`

| Archivo | Responsabilidad unica |
|---|---|
| `app/core/entity_profile/profile_state.py` | `ProfileConversationState` = confirmed_frame + pending. Sustituye a `conversation_state.py` en la ruta V1.2. |
| `app/core/entity_profile/patch_codec.py` | Serializar/deserializar `EntityProfilePatch` y alternativas para que un pendiente sobreviva a la recarga. |
| `app/core/entity_profile/frame_runner.py` | Ejecutar SOLO los jobs/SQL de un plan READY y devolver evidencia atada al plan. |
| `app/core/entity_profile/frame_sections.py` | Plan + Evidence -> secciones acreditadas, con periodo POR FUENTE (no scope global). |
| `app/core/entity_profile/frame_response.py` | Plan + Evidence + alcance compilado -> texto de respuesta. |
| `app/core/entity_profile/execution_record.py` | Snapshot inmutable serializable de Plan + resumen de Evidence para publicar. |
| `app/core/entity_profile/frame_turn.py` | Un turno de perfil: claims -> patch -> frame -> plan -> evidencia -> resultado + estado + continuacion. |
| `app/core/workflow/continuation.py` | `ContinuationContract`: dueno, resultado visible y capacidades del turno publicado. |
| `app/core/workflow/admission.py` | Admision superior (grafo Burr pequeno) antes de `_dispatch`, tambien para seed/UI. |
| `app/core/conversation_envelope.py` | El sobre versionado del turno (respuesta, profile_state, continuation, execution_record) en JSON estricto. |
| `app/core/store_lock.py` | Exclusion por conversacion ENTRE PROCESOS sobre el almacen de archivos. |

## Archivos existentes modificados

| Archivo | Cambio |
|---|---|
| `app/core/conversation_store.py` | `publish_turn(expected_revision, ...)` + `load_envelope`/`current_revision` bajo la exclusion nueva. No se toca `append_turn` ni el sidecar de paginacion. |
| `app/core/orchestrator.py` | Admision antes de `_dispatch`, incluida la rama `seed_resolved_params`. |
| `app/config.py` | `secoppal_entity_profile_frame_enabled` (default **False**) + lector `entity_profile_frame_enabled()`, reutilizando el mecanismo de AP2. |
| `app/core/entity_profile/render.py` | Se expone `profile_fact_lines()` (era `_overview`/`_focused`) para que B reuse la MISMA autoridad de render de hechos sin copiarla. |
| `streamlit_app.py` | El chip de refinamiento transporta la referencia de continuacion publicada en vez de rebuscar params en el historial de la UI. |

## Tests nuevos del carril

| Archivo | Que acredita |
|---|---|
| `tests/entity_profile_frame_b_harness.py` | Adaptador de observacion E2E (workflow real, cliente simulado, disco temporal). |
| `tests/test_entity_profile_frame_b_integration.py` | Los ocho escenarios `INTEGRATION_B_REQUIRED` como pruebas reales. |
| `tests/test_entity_profile_frame_b_admission.py` | La tabla de transiciones de la seccion 10, disjuncion de predicados incluida. |
| `tests/test_entity_profile_frame_b_publication.py` | Publicacion, recarga en otra instancia, revisiones intercaladas, cola corrupta, rollback. |
| `tests/test_entity_profile_frame_b_semantics.py` | La semantica conversacional obligatoria de la seccion 6 de la orden. |
| `tests/test_entity_profile_frame_b_mutations.py` | Campana de mutaciones conductuales (15.2). |
| `tests/test_entity_profile_frame_b_properties.py` | Hypothesis determinista sobre las invariantes nuevas. |

## Explicitamente NO tocado

- `app/core/query_router.py`, `app/core/query_plan.py`, `app/core/soql_builder.py`.
- Gramaticas R3, catalogos, identidades, territorios.
- `tests/test_entity_profile_v1_1_reaudit*.py`, `tests/test_v12a1_independent_counterexamples.py`,
  `tests/test_entity_profile_frame_*.py` de A2/R3d, `tests/test_entity_profile_r3d_postposed_source.py`.
- `docs/decisions_log.md`, `docs/decisions/`, `docs/orquestacion/CARRILES.md`, `AGENTS.md`,
  `Makefile`, `ruff.toml`, `.semgrep/`.
- Baseline global de Semgrep.

# Matriz de propiedad de rutas — Foundation V3 (ADR-102)

**Punto de entrada.** `SecopalWorkflow._dispatch()`, después de `handle_refinement`,
de los handlers estructurados (reset, selección de actor, Radar, paginación,
sugerencias, direct lookup), de la observación Natural Alpha (multi-job terminal)
y de `handle_bound_follow`; inmediatamente antes de `handle_normal_flow`. El hook de
`f703504b` en `_admit_and_dispatch()` (antes de `coordinate_turn()`) **no** se
promueve. Con Frame ON, la admisión (`admit`) decide PROFILE/CLARIFY antes; Foundation
sólo ve turnos `DELEGATED`.

**Regla de reclamo** (`foundation_runtime.run_foundation_search` +
`foundation_route_owner`), en este orden:

1. `SECOPPAL_FOUNDATION_MODE == foundation_v3`; si no, `None` sin ninguna lectura.
2. Compuerta congelada `eligible_first_search` (sin seed, sin selección de actor o
   de acción de perfil, sin historial, forma léxica de `f703504b`). No se amplía.
3. `natural_alpha_owner`: un `binding` de capability → declina; Alpha `MATCHED`
   con un job que no sea exactamente un `SEARCH` → declina.
4. `specialized_owner`, con el mismo `QueryRouter.parse` que usa `admit()`:
   `actor_scope`/`actor_pending`/`contratista` → ACTOR (igual que
   `external_owner`); `detect_entity_profile_turn` → ENTITY_PROFILE;
   `admit_contract_activity(...)` ≠ HANDOFF con evidencia semántica →
   CONTRACT_ACTIVITY; `REGISTRY[*].detect` → ANALYTICS.
5. Sólo si todos declinan, Foundation produce (1 llamada al proveedor), aplica H7,
   admite, proyecta y ejecuta bajo guard.

No hay lectura del texto crudo para decidir el trabajo: los pasos 3–4 son los
detectores de cada dueño, sin reglas nuevas.

| # | Fila | Dueño efectivo con Foundation ON | Mecanismo | Evidencia (test) |
|---|---|---|---|---|
| 1 | reset | `handle_reset` | pre-Foundation | `test_existing_owners_keep_their_turn_with_foundation_on[reset]` |
| 2 | actor candidate selection | `handle_actor_selection` | pre-Foundation; además `eligible_first_search` exige `actor_candidate_selection is None` | compuerta (código) |
| 3 | profile action selection | carril exclusivo de `coordinate_turn` | nunca llega a `_dispatch` | código (`_execute_profile_action`) |
| 4 | radar literal (RUN) | `handle_radar_command` | pre-Foundation | `[radar-run]` |
| 5 | radar natural LIST | `handle_radar_command` / follow ligado | pre-Foundation | `[radar-list]` |
| 6 | radar natural RUN | `handle_bound_follow` (FOLLOW) | pre-Foundation; binding → declina | `natural_alpha_owner` |
| 7 | paginación | `handle_pagination` | pre-Foundation | `test_pagination_keeps_its_owner_with_foundation_on` |
| 8 | suggestion selection | `handle_suggestion_selection` | pre-Foundation | código |
| 9 | direct lookup | `handle_direct_lookup` | pre-Foundation | `[direct-lookup]` |
| 10 | refinement | `handle_refinement` (seed) | seed → compuerta falsa | código |
| 11 | Actor Profile por NIT | ACTOR | `specialized_owner` | `[actor-profile-nit]` |
| 12 | Actor Profile por nombre | ACTOR | `specialized_owner` | `[actor-profile-name]`, `[actor-profile-contracts-noun]` |
| 13 | Entity Profile overview | admisión (Frame ON) / `detect_entity_profile_turn` (Frame OFF) | `specialized_owner` | `[entity-profile-overview]`, `test_frame_admission_keeps_the_entity_profile_and_delegates_search` |
| 14 | Entity Profile suppliers | A2 SUPPLIERS | binding → declina | `[entity-profile-suppliers]` |
| 15 | Contract Activity total | Contract Activity | evidencia semántica / binding | `[contract-activity-count]` |
| 16 | Contract Activity modality | Contract Activity | job Alpha no-SEARCH | `[alpha-non-search-job]` |
| 17 | Contract Activity state | Contract Activity | mismo mecanismo que 15–16 | código |
| 18 | Natural Alpha multi-job | aclaración escalonada | terminal antes de Foundation | `[natural-alpha-multi-job]` |
| 19 | SEARCH/PROCESSES | **Foundation** | todos declinan | `test_a_simple_first_turn_search_is_foundation_only_when_on` (patrón) |
| 20 | SEARCH/CONTRACTS | **Foundation** | idem | idem |
| 21 | SEARCH/dual | **Foundation** (dos peticiones con `SHARES_SCOPE`) | idem | `test_foundation_projection` |
| 22 | opportunity search | **Foundation** + H7 | idem | `test_foundation_universe_policy` |
| 23 | aggregate analytics | analytics | `REGISTRY.detect` | `[aggregate-analytics]` |
| 24 | first-turn simple search | **Foundation** | idem | `test_a_simple_first_turn_search_is_foundation_only_when_on` |
| 25 | follow-up con historial | legado (nunca Foundation) | compuerta: historial | `test_a_follow_up_with_history_is_never_foundation` |
| 26 | fragmento tras un turno Foundation | **aclaración** `FOUNDATION_CONTINUATION_UNSUPPORTED`, 0 llamadas | ver abajo | `test_a_fragment_after_a_foundation_turn_clarifies_instead_of_losing_the_topic` |

Con flag OFF todas las filas quedan idénticas a `main`: el flag se lee antes que
cualquier otra cosa y `run_foundation_search` devuelve `None` sin parsear, sin
producir y sin recibos (ver `FLAG_OFF_PARITY` en `FINAL_REPORT.md`).

## Hallazgo nuevo: continuación silenciosa tras un turno Foundation

En `f703504b` el turno Foundation se guardaba en el historial con sus metadatos
como `parsed_params`. Un follow-up legado posterior (`is_followup` = turno reciente)
fusionaría el fragmento con un contexto que no contiene filtros legados y ejecutaría
el fragmento solo: «contratos de primera infancia de Barranquilla» → «y de Cali?»
buscaría todo Cali, perdiendo el tema sin decirlo. Corrección acotada (sin
implementar follow-ups de Foundation, que siguen fuera de alcance): el turno
Foundation se guarda con `parsed_params = {}` y `outcome_reason = "foundation_v3"`;
con Foundation ON, un turno siguiente que ningún dueño reclama y que no es una
búsqueda nueva completa (mismos detectores que `external_owner`) recibe una
aclaración legible y cero llamadas. Una búsqueda nueva completa sigue al legado.

**Límite declarado:** tras un rollback a `legacy`, un turno Foundation ya guardado
deja un contexto vacío; el legado trataría un fragmento como búsqueda nueva.

## Hallazgo nuevo: frases de comprador leídas como actor

`QueryRouter.parse("busca licitaciones de alcaldía de barranquilla en 2024")` fija
`actor_scope` (contratista sin resolver, `ACTOR_CONVERSATION_OPERATION`). Como
`external_owner`, Foundation respeta al dueño ACTOR y declina. Es pérdida de
cobertura, no preempción; se mide en `FOUNDATION_ENTRY_ADMISSION_V1`
(`route_false_negative`). La variante sin «busca» y con artículo no se marca.

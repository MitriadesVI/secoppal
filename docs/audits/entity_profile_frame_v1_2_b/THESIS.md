# V1.2-B — Tesis y mapa de lectores sustituidos

## 1. La tesis

A2 y R3d construyeron el núcleo: de un texto salen ocurrencias vinculadas, de
ellas un parche tipado, de él un Frame, y del Frame un plan con su SQL ya
congelado. Ese núcleo estaba **completo y apagado**: ninguna ruta productiva lo
usaba, y el perfil seguía atendiéndose dentro de `execute_query`, después de
que `apply_context` hubiera fusionado los params del turno anterior.

B no añade semántica. B conecta el trayecto y **retira de él a los lectores que
el Frame sustituye**. Tres cambios estructurales lo hacen posible:

1. **El universo anterior entra por una sola puerta.** No hay merge de params,
   no se lee `_entity_profile_request` ni `_entity_profile_state`, y no se
   reconstruye significado desde `raw_query`. Lo único que un turno hereda es
   el Frame confirmado que el turno anterior **publicó**.

2. **La ruta se decide por dueño, no por forma de la frase.** «más» no lo
   reclama la paginación por parecerse a una paginación; lo reclama —o no— el
   dueño del resultado que la persona está viendo, declarado en un contrato de
   continuación. La admisión corre antes de `_dispatch`, también para el
   refinamiento sembrado por la interfaz.

3. **El turno se publica entero o no se publica.** Respuesta, estado del
   perfil, contrato y registro de ejecución van en un único registro versionado,
   con la revisión esperada comprobada bajo una exclusión que cruza procesos.
   Un fallo de publicación no avanza el Frame durable ni entrega un cursor nuevo.

## 2. Mapa de lectores sustituidos

| Lector de V1.1 | Qué leía | Estado en la ruta V1.2 |
|---|---|---|
| `turn.classify_entity_profile_turn` | texto + params fusionados | **No participa.** `frame_turn.run_profile_turn` compone desde texto y params propios. Sigue viva para la ruta legacy con el flag apagado. |
| `turn._composed_request` / `inherit_request` | petición anterior + params | **Retirado de la ruta.** Su función la hace `compose_patch` + `apply_patch` sobre el Frame confirmado. |
| `entity_profile/conversation_state.py` | dos `EntityProfileSemanticRequest` | **Sustituido** por `profile_state.ProfileConversationState` (Frame confirmado + parche pendiente). El módulo antiguo no se importa desde ninguna pieza nueva. |
| `semantic_request.EntityProfileSemanticRequest` | universo material | **Sin autoridad runtime en V1.2.** Sólo lo usan la ruta legacy y el migrador. |
| `assembler.build_entity_profile` + `_request_from` | `_entity_profile_request` de params | **Sustituido** por `frame_response.build_profile_sections`, que consume Plan + Evidence. |
| `sections.build_sections` con `EntityProfileScope` | fecha global del scope | **Sustituido** por `frame_sections.build_frame_sections`, con periodo POR FUENTE y `expected_sections` del plan. |
| `render.render_entity_profile` + `effective_scope` | scope legacy | **Sustituido** por `frame_response.render_frame_profile`. Se reutiliza `profile_fact_lines` —la misma autoridad de render de hechos— para no tener dos formas de decir lo mismo. |
| `query_runner.execute_jobs` con `job.params` | params por job, SQL construido al llamar | **Sustituido** por `frame_runner.execute_plan`, que emite el SQL ya congelado en `CompiledJob`. |
| `_dispatch` como decisor de ruta | forma de la frase | **Precedido** por `workflow/admission.py`. Sigue siendo la ruta delegada, con sus handlers y sus permisos intactos. |
| `radar._last_search_params` / `suggestion` / `refinement` | historial hacia atrás | **No cambian.** Lo que cambia es que la admisión decide si les toca el turno, leyendo el contrato publicado. |
| `ConversationStore.append_turn` | params heredables | **Intacto** para las rutas que ya lo usaban. Las rutas que la admisión atiende por su cuenta **no escriben turno legacy**: el sobre es la autoridad. |

## 3. Lo que NO se tocó, a propósito

- `QueryRouter`, `query_plan.py`, la política escalar de Search y `SoQLBuilder`.
- Las gramáticas de R3, los catálogos de modalidad, las identidades y el
  territorio.
- El motor de paginación: el contrato declara **de quién** es el resultado; el
  cursor, la generación y la vigencia los sigue validando `handle_pagination`
  en su propio sidecar.
- El baseline global de Semgrep.
- Los adversariales congelados de A2/R3d y sus pins.

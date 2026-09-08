# Alcance previo a edición runtime

Base: 52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386. Rama/worktree nuevos solicitados.
AGENTS.md y CARRILES.md leídos. No se edita zona compartida ni instancia dogfood.
Main: tracked/staged limpios; untracked preexistentes preservados. Worktree nuevo limpio.
No pytest concurrente observado; otros worktrees/agentes existen y se preservan.

Archivos exactos previstos:
- app/core/buyer_actor_role.py: predicado puro de pertenencia de la mención completa al comprador acreditado.
- app/core/actor_intent.py: reutilizar gramática verbal para consumir petición contractual del comprador.
- app/core/query_router.py: enganche antes de crear name_unresolved/SET_ACTOR y antes de extraer tema.
- app/core/actor_arbitration.py: aplicar el mismo predicado antes de devolver una sonda opcional.
- app/core/workflow/actions/actor_resolution.py: retirar excepción article_led_buyer obsoleta y dependiente del resultado del índice.
- tests/test_buyer_actor_role_arbitration_v1.py y tests adicionales buyer_actor_role_*: regresión, controles y propiedades.
- docs/audits/buyer_actor_role_arbitration_v1/: evidencia y reporte.

Mapa manual (Codegraph 0.9.6 con Node 22; status: Not initialized; MCP no cargado):
QueryRouter.parse -> _detect_named_actor -> detect_actor_name -> name_unresolved;
parse_query/apply_context -> can_resolve_new_actor -> resolve_actor_name -> resolver -> project_actor_resolution;
resolve_actor_name -> detect_bare_actor_probe -> resolve_exact -> bare_exact_probe.
Frame ON: admit_turn -> QueryRouter.parse -> external_owner -> delegado -> mismo workflow.
No se prevé reparseo posterior al Frame ni nueva autoridad paralela.

Preflight: ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10.
Codegraph launcher del PATH rechaza Node 26.8.1; ejecutable probado con Node 22.
Fallback declarado a rg/Git/manual por índice ausente en worktree.

# SECOPPAL profile baseline — 2026-05-12 21:48:28

## Objetivo
Congelar el estado real del perfil `secoppal` antes de intervenir documentación, tests MCP o flujo operativo.

## Perfil
- Wrapper: `/Users/rodrigoortiz/.local/bin/hermes-secoppal`
- Perfil: `/Users/rodrigoortiz/.hermes/profiles/secoppal`
- Repo de trabajo: `/Users/rodrigoortiz/Documents/secoppal`
- Modelo baseline original al inicio de auditoría: `claude-opus-4.6` vía `copilot`
- Modelo operativo corregido para uso actual: `claude-sonnet-4.6` vía `copilot`
- Gateway: `running`
- Skills reportadas por `hermes profile show secoppal`: `102`
- `.env`: existe
- `SOUL.md`: existe

## Tools MCP públicas expuestas hoy
Verificado en `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/mcp_server.py`:
- `memory_query`
- `memory_write`
- `session_bootstrap`
- `workspace_brief`
- `memory_update`

No están expuestas públicamente en este perfil:
- `get_entity_subgraph(...)`
- `memory_brief`
- `resume_work`
- `timeline_view`
- `complete_task_with_summary`
- `cross_profile_query`

## Componentes internos relevantes verificados
- `graph_ops.py` sí contiene:
  - `CANONICAL_RELATIONS`
  - `link_entities(...)`
  - `get_entity_subgraph(...)`
- `artifacts_ops.py` es mínimo:
  - `create_artifact(...)`
  - `list_artifacts(...)`

## Estado del repo SECOPPAL
Comando:
- `cd /Users/rodrigoortiz/Documents/secoppal && .venv/bin/python -m pytest tests/ -q`

Resultado:
- `60 passed in 1.58s`

## Tablas SQLite presentes
Consultadas en:
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/memory.db`

Tablas principales observadas:
- `artifacts`
- `attempts`
- `branches`
- `build_runs`
- `decision_log`
- `dream_cycles`
- `edits`
- `entity_links`
- `fact_references`
- `facts`
- `failures`
- `files`
- `idempotency_keys`
- `interaction_log`
- `memory_pending`
- `plans`
- `repositories`
- `schema_migrations`
- `skill_suggestions`
- `symbols`
- `tasks`
- `test_runs`
- `tool_health`
- `tool_telemetry`

Además existen tablas FTS asociadas para varias entidades.

## Conteos baseline
Comando ejecutado:
- `SELECT 'tasks', COUNT(*) ... UNION ALL ...`

Resultado:
- `tasks = 4`
- `plans = 1`
- `facts = 8`
- `artifacts = 1`
- `entity_links = 0`
- `idempotency_keys = 1`

## Backup previo a intervención
- DB original: `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/memory.db`
- Tamaño original: `507904 bytes`
- Backup: `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/backups/memory-2026-05-12_214828.db`
- Tamaño backup: `507904 bytes`

## Hallazgos de línea base relevantes
- El perfil como agente existe y arranca, pero su superficie MCP es corta: 5 tools.
- Hay capacidades internas no expuestas por MCP, especialmente `get_entity_subgraph(...)`.
- La DB está viva y no vacía; no requiere reinicialización.
- El repo pasa tests, pero eso no prueba todavía confiabilidad del perfil Hermes.
- Durante la intervención se detectó un drift operativo adicional: `claude-opus-4.6` ya no es válido para este integrador Copilot/opencode; el perfil quedó corregido a `claude-sonnet-4.6`.
- La siguiente intervención debe priorizar:
  1. drift documental,
  2. smoke tests MCP reales,
  3. validación de idempotencia,
  4. E2E del agente.

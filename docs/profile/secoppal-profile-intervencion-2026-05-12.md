# SECOPPAL profile — intervención 2026-05-12

## Objetivo
Dejar el perfil Hermes `secoppal` listo para uso como perfil desarrollador confiable, sin perseguir paridad completa con `default`.

## Qué se intervino

### 1. Drift documental corregido
Se alinearon repo y perfil contra el runtime real:
- `/Users/rodrigoortiz/Documents/secoppal/README.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/AGENTS.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/SOUL.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memories/MEMORY.md`
- `/Users/rodrigoortiz/Documents/secoppal/docs/profile/secoppal-profile-baseline.md`

Correcciones clave:
- fallback LLM condicional, no narrativa de "100% determinístico"
- `streamlit_app.py` en raíz, no en `app/`
- no existe `.env.example`
- suite real: `60` tests
- ajuste de ADRs y variables de entorno documentadas
- baseline actualizado con drift operativo de modelo

### 2. Validación MCP real
Se crearon y ejecutaron smoke tests reales por stdio contra el MCP server del perfil:
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_mcp_real_secoppal.py`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_idempotency_entity_links_secoppal.py`

Cobertura probada:
- tools públicas visibles
- side effects reales en SQLite
- tool_telemetry
- cleanup
- idempotencia de `memory_write`
- idempotencia de `memory_update`
- relaciones canónicas vía validación Python + trigger SQL

### 3. Fix runtime real
Se corrigió bug probado por smoke en:
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/mcp_server.py`

Fix aplicado:
- `_with_idempotency()` ahora extrae `target_id` también cuando el resultado del tool llega como `str/int`, evitando registros de idempotencia con `target_id = NULL`.

### 4. Hardening operativo del perfil Hermes
Se detectó que el perfil estaba configurado con un modelo ya no válido para el integrador actual:
- antes: `claude-opus-4.6`
- después: `claude-sonnet-4.6`

Archivo corregido:
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/config.yaml`

Causa raíz verificada con request dumps reales:
- `model_not_available_for_integrator` para `claude-opus-4.6`
- `claude-sonnet-4.6` sí aceptado
- `gpt-5.4` también aceptado

### 5. Prueba E2E real del perfil
Se creó y ejecutó:
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/e2e_hermes_profile_secoppal.py`

La prueba valida:
- `hermes -p secoppal chat -q ...`
- uso real de tools MCP públicas desde el agente
- escritura real de task en `memory.db`
- cierre real vía `memory_update`
- verificación de `workspace_brief`
- verificación de `tool_telemetry`
- cleanup sin residuos

## Evidencia principal

### Estado del perfil
- `hermes profile show secoppal` → `Model: claude-sonnet-4.6 (copilot)`
- `hermes status --all --profile secoppal` → `Gateway: running`

### Estado del repo
- `cd /Users/rodrigoortiz/Documents/secoppal && .venv/bin/python -m pytest tests/ -q`
- resultado conocido: `60 passed`

### E2E CLI real
Pruebas reales exitosas:
- `hermes -p secoppal chat -Q -q 'Responde solo OK'`
- `hermes -p secoppal chat -Q -m claude-sonnet-4.6 -q 'Responde solo OK'`
- `e2e_hermes_profile_secoppal.py --model gpt-5.4`

### Cleanup verificado
Post-E2E:
- `tasks WHERE title LIKE 'E2E_HERMES_%' = 0`
- `tool_telemetry LIKE '%E2E_HERMES_%' = 0`
- `idempotency_keys LIKE '%E2E_HERMES_%' = 0`

## Criterio explícito de “listo para uso”
Se considera que `secoppal` quedó listo para uso si cumple todos estos puntos:

1. El perfil arranca con `hermes -p secoppal`
2. El modelo por defecto del perfil responde sin error de integrador
3. El repo objetivo sigue sano (`60/60` tests)
4. Las 5 tools MCP públicas funcionan de verdad fuera de tests unitarios
5. La idempotencia mínima está probada
6. El enforcement de relaciones canónicas está probado
7. Existe al menos una prueba E2E real vía CLI Hermes
8. Las pruebas dejan cleanup verificable

Resultado actual: todos los criterios anteriores están cumplidos.

## Qué NO significa este cierre
No significa:
- paridad completa con `default`
- exposición de más tools MCP
- cobertura de todas las rutas auxiliares del perfil
- cierre de deuda histórica documental en logs/reportes antiguos

Significa algo más preciso:
- el perfil hoy es utilizable, consistente y verificablemente operativo para trabajo de desarrollo pequeño/mediano en SECOPPAL.

## Pendientes razonables
1. Actualizar referencias históricas que todavía nombren `claude-opus-4.6` como modelo operativo actual.
2. Decidir si conviene exponer más tools MCP en el perfil o mantener la superficie mínima.
3. Si se quiere mayor rigor futuro: agregar esta E2E a rutina recurrente de regresión del perfil.

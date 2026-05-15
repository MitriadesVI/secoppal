# Checklist Sprint 1 — SECOPPAL usable sin mentiras

> Para Hermes: este documento es plan solamente. No ejecutar cambios hasta aprobación.

**Goal:** Convertir el plan grande de intervención en una secuencia ejecutable, conservadora y verificable para dejar el perfil `secoppal` listo para empezar a usarse.

**Architecture:** Sprint 1 no busca paridad con `default`. Busca confiabilidad operacional mínima: snapshot + backup, corrección de drift documental, smoke tests MCP reales, validación de idempotencia, prueba E2E del perfil, y bitácora de cierre. No se toca schema ni módulos internos sensibles salvo prueba fallando.

**Tech Stack:** Hermes profiles, MCP memory server de `secoppal`, SQLite, markdown docs, smoke tests Python, CLI `hermes -p secoppal`, pytest del repo SECOPPAL.

---

## Regla madre del sprint

SECOPPAL no necesita ser tan poderoso como `default`; necesita ser confiable como perfil desarrollador.

## Restricciones explícitas

- No perseguir full parity con `default`.
- No tocar `db.py`, `graph_ops.py` ni `artifacts_ops.py` en Sprint 1 salvo que una prueba falle y demuestre necesidad real.
- No hacer migraciones de schema por gusto.
- No introducir `content_json` ni artifacts ricos.
- No aceptar PASS basados solo en funciones Python internas; debe haber validación MCP real.

---

# Entregable 1 — Snapshot + backup de la memoria viva

**Objetivo:** Congelar una línea base reproducible antes de cualquier intervención.

**Estado de aprobación:** Obligatorio.

**Files:**
- Crear: `/Users/rodrigoortiz/Documents/secoppal/docs/profile/secoppal-profile-baseline.md`
- Crear: backup fechado de `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/memory.db`
- Leer/verificar:
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/config.yaml`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/mcp_server.py`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/memory.db`

**Checklist de ejecución:**
1. Registrar tools MCP expuestas realmente.
2. Registrar schema/tablas reales.
3. Registrar conteos mínimos:
   - `tasks`
   - `plans`
   - `facts`
   - `artifacts`
   - `entity_links`
   - `idempotency_keys`
4. Registrar evidencia del runtime:
   - `hermes profile show secoppal`
   - pytest repo
5. Crear backup fechado de `memory.db`.

**PASS si:**
- existe baseline escrita;
- existe backup fechado;
- los conteos y tools quedan documentados.

**FAIL si:**
- se modifica cualquier cosa antes del baseline;
- no hay backup verificable.

---

# Entregable 2 — Drift documental corregido

**Objetivo:** Alinear identidad declarada, docs del perfil y docs del repo con el estado real.

**Estado de aprobación:** Obligatorio.

**Files:**
- Crear: `/Users/rodrigoortiz/.hermes/profiles/secoppal/README.md`
- Modificar:
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/AGENTS.md`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/SOUL.md`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memories/MEMORY.md`
  - `/Users/rodrigoortiz/Documents/secoppal/README.md`

**Checklist de ejecución:**
1. Crear README del perfil con:
   - propósito del perfil;
   - wrapper;
   - tools MCP reales;
   - límites frente a `default`.
2. Corregir AGENTS.md para que no prometa tools que no existen.
3. Corregir SOUL.md para bajar claims grandilocuentes no operacionales.
4. Corregir MEMORY.md para eliminar facts driftados.
5. Corregir README del repo en:
   - path real de Streamlit;
   - `.env.example` inexistente;
   - número real de tests;
   - estado coherente de ADR-008/ADR-009;
   - comandos de ejecución reales.

**PASS si:**
- ningún documento contradice runtime/código en:
  - entrypoint Streamlit,
  - fallback LLM,
  - tools MCP,
  - tests reales.

**FAIL si:**
- queda aunque sea una contradicción gruesa conocida.

---

# Entregable 3 — Suite mínima de smoke tests MCP reales

**Objetivo:** Demostrar que el perfil funciona por la interfaz que realmente usará Hermes, no solo por llamadas internas.

**Estado de aprobación:** Obligatorio.

**Files:**
- Crear dir: `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/`
- Crear:
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_mcp_reading.py`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_mcp_writing.py`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_idempotency.py`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_canonical_relations.py`
- Modificar solo si hace falta harness mínimo:
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/mcp_server.py`

**Cobertura mínima obligatoria:**
- `memory_query`
- `memory_write(kind="fact")`
- `memory_write(kind="artifact")`
- `memory_write(kind="link")`
- `memory_update`
- `session_bootstrap`
- `workspace_brief`

**Checklist de ejecución:**
1. Probar lectura MCP real.
2. Probar escritura MCP real con cleanup o entidad de prueba controlada.
3. Probar update MCP real.
4. Probar que las 5 tools públicas responden desde el server MCP.
5. Guardar tests de forma persistente y redescubrible.

**PASS si:**
- las tools públicas actuales responden por MCP real;
- los scripts quedan guardados en `memory_db/tests/`;
- no dependen de inspección manual ad hoc.

**FAIL si:**
- el “verde” sale solo por funciones Python internas;
- no queda script repetible.

---

# Entregable 4 — Idempotencia y relaciones canónicas validadas

**Objetivo:** Probar los dos mecanismos que hacen al perfil seguro para retries y links.

**Estado de aprobación:** Obligatorio.

**Files:**
- Crear o ampliar:
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_idempotency.py`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_canonical_relations.py`
- Documentar resultado en:
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/README.md`
  - `/Users/rodrigoortiz/Documents/secoppal/docs/profile/secoppal-profile-baseline.md`

**Checklist de ejecución:**
1. Repetir `memory_write` con misma idempotency key.
2. Repetir `memory_update` con misma idempotency key.
3. Verificar replay controlado / no duplicación.
4. Validar rechazo de relación no canónica en Python.
5. Validar enforcement/consistencia en SQLite trigger.
6. Mantener artifacts en modo mínimo:
   - referencia por `file_path`
   - sin enriquecer schema.

**PASS si:**
- retry no duplica;
- relación no canónica falla como debe;
- artifacts básicos funcionan sin tocar schema.

**FAIL si:**
- se detectan duplicados por retry;
- una relación inválida entra a la DB;
- para hacer pasar esto toca migrar schema sin evidencia fuerte.

---

# Entregable 5 — Prueba E2E del perfil como agente

**Objetivo:** Probar al agente real con `hermes -p secoppal`, no solo al backend de memoria.

**Estado de aprobación:** Obligatorio.

**Files:**
- Verificar:
  - `/Users/rodrigoortiz/.local/bin/hermes-secoppal`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/config.yaml`
- Documentar en:
  - `/Users/rodrigoortiz/Documents/secoppal/docs/profile/secoppal-intervention-log.md`

**Checklist de ejecución:**
1. Abrir `hermes -p secoppal`.
2. Verificar cwd correcto.
3. Verificar tono/identidad razonables.
4. Ejecutar `session_bootstrap`.
5. Ejecutar `workspace_brief`.
6. Hacer una escritura real de prueba vía MCP.
7. Hacer un `memory_update` real.
8. Confirmar persistencia real en DB.
9. Pedir una tarea de desarrollo pequeña o inspección corta y verificar que usa repo + memoria sin drift visible.

**PASS si:**
- el perfil funciona de punta a punta sin hacks;
- usa el repo correcto;
- la memoria responde y persiste;
- la documentación alcanza para repetir el flujo.

**FAIL si:**
- el agente arranca pero no puede operar su memoria real;
- depende de conocimiento tácito del auditor.

---

# Entregable 6 — Bitácora de intervención + criterio de “listo para uso”

**Objetivo:** Cerrar el sprint sin depender de memoria informal.

**Estado de aprobación:** Obligatorio.

**Files:**
- Crear: `/Users/rodrigoortiz/Documents/secoppal/docs/profile/secoppal-intervention-log.md`
- Actualizar:
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/README.md`
  - `/Users/rodrigoortiz/Documents/secoppal/README.md`

**Checklist de ejecución:**
1. Registrar qué se corrigió.
2. Registrar qué se verificó.
3. Registrar qué no se tocó deliberadamente.
4. Registrar qué quedó diferido para Sprint 1.5 / Sprint 2.
5. Registrar criterio explícito de “perfil listo para uso”.
6. Dejar recomendación de siguiente mejora única:
   - `entity_subgraph` solo si no requiere migración.

**PASS si:**
- un tercero puede entender estado actual, límites y próximos pasos sin releer toda la auditoría.

**FAIL si:**
- el sprint queda cerrado solo “porque ya nos acordamos”.

---

# Orden exacto de ejecución

## Secuencia aprobada
1. Entregable 1 — Snapshot + backup
2. Entregable 2 — Drift documental
3. Entregable 3 — Smoke tests MCP reales
4. Entregable 4 — Idempotencia + canonical relations
5. Entregable 5 — E2E con `hermes -p secoppal`
6. Entregable 6 — Bitácora de intervención

## Regla de avance
No pasar al siguiente entregable si el anterior no está en PASS explícito.

---

# Definition of Done — Sprint 1

Sprint 1 termina cuando se cumplen estas 6 condiciones:

- [ ] existe baseline + backup verificable
- [ ] docs del perfil y repo ya no se contradicen con el runtime conocido
- [ ] existe suite mínima de smoke tests MCP reales
- [ ] idempotencia y relaciones canónicas están demostradas
- [ ] el perfil pasa una prueba E2E real con `hermes -p secoppal`
- [ ] existe bitácora de intervención y criterio explícito de “listo para uso”

---

# Qué queda fuera de Sprint 1

No hacer ahora:
- full parity con `default`
- `resume_work`
- `complete_task_with_summary`
- `cross_profile_query`
- artifacts ricos
- migraciones de schema por gusto
- refactor grande de `mcp_server.py`
- tocar `db.py`, `graph_ops.py`, `artifacts_ops.py` sin prueba fallando

---

# Sprint 1.5 sugerido

Única mejora candidata después de Sprint 1:
- exponer `entity_subgraph`

**Condición:** solo si no requiere migración de schema y reutiliza la implementación actual con tests nominales y depth > 1.

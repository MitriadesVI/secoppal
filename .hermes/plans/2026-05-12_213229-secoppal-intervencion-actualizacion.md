# Plan de intervención para actualizar el perfil SECOPPAL

> Para Hermes: este documento es plan solamente. No ejecutar cambios hasta aprobación.

## Goal

Dejar el perfil `secoppal` listo para empezar a usarse como agente desarrollador real, cerrando el gap entre:
1. lo que el perfil dice que es,
2. lo que realmente expone su stack MCP/memoria,
3. y lo que la documentación del repo/perfil promete.

## Resultado esperado

Al terminar la intervención:
- el perfil `secoppal` tendrá documentación coherente con el estado real del proyecto;
- su capa MCP/memoria tendrá tests mínimos de confianza para uso diario;
- quedará explícito qué capacidades tiene hoy y cuáles quedan diferidas respecto a `default`;
- habrá una ruta de uso segura para comenzar a operarlo sin falsas expectativas.

## Contexto actual verificado

### Hechos ya confirmados
- El wrapper existe: `/Users/rodrigoortiz/.local/bin/hermes-secoppal`.
- El `cwd` del perfil apunta al repo correcto: `/Users/rodrigoortiz/Documents/secoppal`.
- El repo SECOPPAL está funcional: `60 passed` en pytest.
- El perfil expone solo 5 tools MCP: `memory_query`, `memory_write`, `session_bootstrap`, `workspace_brief`, `memory_update`.
- `graph_ops.py` sí tiene:
  - `CANONICAL_RELATIONS`
  - `link_entities(...)`
  - `get_entity_subgraph(...)`
- Pero `get_entity_subgraph(...)` no está expuesto como tool MCP pública.
- `artifacts_ops.py` es mínimo: `create_artifact(...)` + `list_artifacts(...)`.
- Hay drift documental entre:
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/AGENTS.md`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/SOUL.md`
  - `/Users/rodrigoortiz/.hermes/profiles/secoppal/memories/MEMORY.md`
  - `/Users/rodrigoortiz/Documents/secoppal/README.md`
- Drift concreto ya visto:
  - README menciona `app/streamlit_app.py`, pero la app real está en `streamlit_app.py` en raíz.
  - README habla de `.env.example`, pero no existe.
  - README/MEMORY/AGENTS no están alineados sobre LLM fallback vs “pipeline sin LLM”.
  - README y MEMORY tienen conteos/tests/versiones viejas.
- No existe una carpeta formal de smoke tests para `memory_db/tests/`; solo existe `test_memory.py` como script suelto.

## Supuestos de trabajo

- La prioridad NO es lograr paridad total con el perfil `default` en un primer sprint.
- La prioridad SÍ es dejar a `secoppal` utilizable con confianza operacional y documentación honesta.
- Cualquier migración de schema debe ser formal y reversible por backup, no manual en caliente.
- La documentación debe seguir al código real y a los tests, no al revés.

## Enfoque propuesto

Dividir la intervención en dos niveles:

### Nivel 1 — Obligatorio antes de empezar a usar el perfil
Cerrar inconsistencias de documentación, exponer el estado real del perfil, y agregar una base mínima de validación MCP/memoria.

### Nivel 2 — Mejora posterior, no bloqueante
Acercar `secoppal` a `default` donde sí agregue valor real: tools compuestas, subgraph público, artifacts más ricos, mejores smoke tests.

---

# Fase 0 — Congelamiento y línea base

## Objetivo
Tomar una línea base reproducible antes de tocar docs, prompts o memory stack.

## Archivos involucrados
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/config.yaml`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/AGENTS.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/SOUL.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memories/MEMORY.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/mcp_server.py`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/memory.db`
- `/Users/rodrigoortiz/Documents/secoppal/README.md`

## Pasos
1. Registrar snapshot del perfil actual:
   - tools MCP expuestas
   - schema SQLite actual
   - conteos principales (`tasks`, `plans`, `facts`, `artifacts`, `entity_links`, `idempotency_keys`)
2. Hacer backup de `memory.db` antes de cualquier migración.
3. Congelar evidencia de ejecución actual:
   - `hermes profile show secoppal`
   - pytest del repo
   - lectura del `mcp_server.py`

## Validación
- Debe existir un inventario escrito del estado actual antes del primer cambio.
- Debe existir backup fechado de `memory.db`.

---

# Fase 1 — Reconciliar documentación y prompts del perfil

## Objetivo
Eliminar drift entre identidad del perfil, memoria plana y docs del repo.

## Archivos a modificar
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/AGENTS.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/SOUL.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memories/MEMORY.md`
- `/Users/rodrigoortiz/Documents/secoppal/README.md`
- Nuevo: `/Users/rodrigoortiz/.hermes/profiles/secoppal/README.md`

## Cambios concretos propuestos

### 1. Crear un README del perfil
Crear un documento específico del perfil con:
- propósito del perfil;
- wrapper y cómo lanzarlo;
- herramientas MCP reales disponibles hoy;
- relación entre repo SECOPPAL y perfil Hermes `secoppal`;
- limitaciones conocidas frente a `default`.

### 2. Corregir AGENTS.md del perfil
Ajustar para que refleje solo verdades verificadas:
- stack real vigente;
- rutas reales con path correcto;
- tools MCP realmente expuestas (5, no más);
- uso recomendado de memoria del perfil;
- aclaración explícita de que no tiene paridad completa con `default`.

### 3. Corregir SOUL.md
Reducir afirmaciones excesivas tipo “conozco cada módulo al dedillo” si no ayudan operacionalmente.
Mantener tono/identidad, pero sin claims que comprometan rigor.

### 4. Corregir MEMORY.md
Eliminar o corregir hechos driftados, en especial:
- narrativa “pipeline sin LLM” si el código actual tiene fallback reintroducido;
- conteo de tests viejo;
- claims de ordering/defaults que no coincidan con código vigente.

### 5. Corregir README del repo
Alinear:
- instalación real;
- ruta real de Streamlit;
- ausencia/presencia real de `.env.example`;
- número real de tests;
- estado coherente de ADR-008/ADR-009;
- comando real de ejecución.

## Validación
- Ningún documento debe contradecir el código/runtime en estos puntos:
  - entrypoint Streamlit
  - estado del fallback LLM
  - conteo de tests
  - tools MCP disponibles
- Debe existir un documento canónico del perfil.

---

# Fase 2 — Hardening mínimo del MCP stack para uso diario

## Objetivo
Subir `secoppal` de “tiene piezas internas” a “puedo confiar en usarlo”, con enfoque conservador.

## Regla de control de alcance
En Sprint 1 no tocar `db.py`, `graph_ops.py` ni `artifacts_ops.py` salvo que una prueba falle y demuestre necesidad real.

La prioridad de esta fase es validar lo existente vía MCP real, no rediseñar el stack.

## Archivos a modificar en Sprint 1
- Nuevo dir: `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/mcp_server.py` solo si hace falta un harness o ajuste mínimo para probar exposición MCP real
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/README.md`

## Fase 2A — Validar lo existente

### Objetivo
Demostrar que el stack actual funciona de extremo a extremo sin agregar features nuevas.

### Smoke tests obligatorios
Crear smoke tests persistentes para validar:
1. `memory_query` PASS
2. `memory_write(kind="fact")` PASS
3. `memory_write(kind="artifact")` PASS
4. `memory_write(kind="link")` PASS
5. `memory_update(...)` PASS
6. `session_bootstrap(...)` PASS
7. `workspace_brief(...)` PASS
8. idempotency en `memory_write` / `memory_update` PASS
9. canonical relations PASS

### Regla crítica de validación
No aceptar falso verde basado solo en llamadas directas a funciones Python.

Cada prueba relevante debe distinguir entre:
- prueba de módulo interno;
- prueba de wrapper MCP;
- prueba de uso real del agente.

La validación mínima exigida en Sprint 1 es por servidor MCP real para las 5 tools públicas declaradas.

### Relaciones canónicas
Validar dos caminos:
- rechazo en Python para relación no canónica;
- rechazo/consistencia en SQLite trigger.

### Artifacts
Mantener opción mínima en Sprint 1:
- `artifacts = referencias a archivos vía file_path`
- documentar claramente esa restricción

No introducir `content_json` ni artifacts inline en esta fase.

## Fase 2B — Exponer una sola mejora útil

### Objetivo
Cerrar un gap de exposición real sin convertir Sprint 1 en un rediseño.

### Única mejora candidata
Exponer `get_entity_subgraph(...)` como tool MCP pública.

Nombre sugerido:
- `entity_subgraph`

Schema mínimo sugerido:
- `entity_type`
- `entity_id`
- `depth`

### Regla de aprobación
Solo exponer `entity_subgraph` si:
- no requiere migración de schema;
- reutiliza la implementación interna existente;
- pasa test nominal y test de profundidad > 1.

Si cualquiera de esas condiciones falla, se difiere a Sprint 1.5.

## Validación
- Deben existir smoke tests descubribles dentro de `memory_db/tests/`.
- Debe poder demostrarse qué está expuesto por MCP y qué sigue siendo interno.
- Sprint 1 queda completo con Fase 2A; Fase 2B es opcional y solo entra si no abre superficie riesgosa.

---

# Fase 3 — Definir alcance funcional real vs backlog de paridad

## Objetivo
Evitar que `secoppal` prometa lo que aún no tiene.

## Documento a crear
- Nuevo: `/Users/rodrigoortiz/.hermes/profiles/secoppal/docs/roadmap-paridad-mcp.md`
  o, si prefieres mantenerlo simple,
- sección equivalente en `/Users/rodrigoortiz/.hermes/profiles/secoppal/README.md`

## Contenido
Matriz explícita con columnas:
- capacidad
- existe en código interno
- expuesta por MCP
- probada por tests
- usada/poblada en la DB
- prioridad de port a `secoppal`

## Capacidades a incluir en la matriz
- `link_entities`
- `get_entity_subgraph`
- canonical relation enforcement
- idempotency
- artifacts
- `memory_brief`
- `resume_work`
- `timeline_view`
- `complete_task_with_summary`
- `cross_profile_query`

## Decisión sugerida
No portar todo de `default` de inmediato.
Clasificar así:

### Must-have para empezar a usar `secoppal`
- docs consistentes
- smoke tests del MCP actual
- idempotency validada
- graph links validados
- artifacts documentados

### Should-have temprano
- `entity_subgraph` como tool
- `timeline_view`
- `memory_brief`

### Deferred
- `resume_work`
- `complete_task_with_summary`
- `cross_profile_query`
- artifacts ricos tipo `content_json`

## Validación
- Debe quedar una frontera explícita entre “listo para usar” y “todavía no”.

---

# Fase 4 — Verificación end-to-end del perfil como agente

## Objetivo
Validar que el perfil funcione como agente real, no solo como colección de archivos.

## Archivos/capas bajo prueba
- `/Users/rodrigoortiz/.local/bin/hermes-secoppal`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/config.yaml`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/mcp_server.py`
- `/Users/rodrigoortiz/Documents/secoppal`

## Escenarios de prueba

### Escenario 1 — inicio de sesión del perfil
- arrancar `hermes -p secoppal`
- verificar que toma el cwd correcto
- verificar que responde en el tono esperado
- verificar que session bootstrap funciona

### Escenario 2 — flujo de trabajo mínimo
- consultar estado del workspace
- crear una task/fact de prueba vía MCP
- registrar artifact/link si aplica
- actualizar entidad
- confirmar persistencia real en DB

### Escenario 3 — trabajo de desarrollo real pequeño
- pedirle una inspección de código o diagnóstico corto
- verificar que usa contexto del repo y memoria del perfil sin drift documental

## Validación
- El perfil debe superar un smoke end-to-end sin hacks manuales fuera del flujo normal.
- La documentación debe ser suficiente para repetir la prueba otro día.

---

# Fase 5 — Cierre documental y bitácora de intervención

## Objetivo
No dejar la intervención como conocimiento tácito.

## Archivos a crear o actualizar
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/README.md`
- `/Users/rodrigoortiz/Documents/secoppal/README.md`
- Nuevo: `/Users/rodrigoortiz/Documents/secoppal/docs/profile/secoppal-intervention-log.md`

## Qué dejar escrito
- qué se corrigió;
- qué se verificó;
- qué no se tocó;
- qué quedó pendiente para paridad con `default`;
- criterio de “perfil listo para uso”.

## Validación
- Un tercero debe poder entender el estado del perfil sin releer toda la auditoría previa.

---

# Secuencia recomendada de ejecución

## Sprint 1 — obligatorio antes de usarlo
1. Fase 0
2. Fase 1
3. Fase 2 (solo prioridad mínima)
4. Fase 4
5. Fase 5

## Sprint 2 — mejora temprana
1. Exponer `entity_subgraph`
2. Agregar `memory_brief`
3. Agregar `timeline_view`
4. Repetir smoke tests
5. Actualizar matriz de paridad

## Sprint 3 — si el uso real lo justifica
1. `resume_work`
2. `complete_task_with_summary`
3. artifacts más ricos
4. cross-profile capabilities, solo si aportan valor real

---

# Files likely to change

## Perfil Hermes
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/README.md` (nuevo)
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/AGENTS.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/SOUL.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memories/MEMORY.md`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/mcp_server.py`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/graph_ops.py`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/artifacts_ops.py`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/db.py`
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/` (nuevo)
- `/Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/migrations/0xx_*.sql` (solo si cambia schema)

## Repo SECOPPAL
- `/Users/rodrigoortiz/Documents/secoppal/README.md`
- `/Users/rodrigoortiz/Documents/secoppal/docs/profile/secoppal-intervention-log.md` (nuevo)

---

# Tests y validación

## Repo SECOPPAL
- `cd /Users/rodrigoortiz/Documents/secoppal && .venv/bin/python -m pytest tests/ -q`

## Memory stack del perfil
Sugeridos:
- `PYTHONPATH=/Users/rodrigoortiz/.hermes/profiles/secoppal /Users/rodrigoortiz/.hermes/.venv/bin/python3 /Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_memory_write.py`
- `PYTHONPATH=/Users/rodrigoortiz/.hermes/profiles/secoppal /Users/rodrigoortiz/.hermes/.venv/bin/python3 /Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_entity_links.py`
- `PYTHONPATH=/Users/rodrigoortiz/.hermes/profiles/secoppal /Users/rodrigoortiz/.hermes/.venv/bin/python3 /Users/rodrigoortiz/.hermes/profiles/secoppal/memory_db/tests/smoke_idempotency.py`

## Validación funcional del perfil
- `hermes profile show secoppal`
- abrir sesión `hermes -p secoppal`
- ejecutar flujo corto con `session_bootstrap`, `workspace_brief`, `memory_query`, `memory_write`, `memory_update`

## Criterios de aceptación
- 0 contradicciones gruesas entre docs del perfil y código real
- 0 contradicciones gruesas entre README y entrypoints reales
- tools MCP actuales con smoke tests PASS
- si se agrega `entity_subgraph`, también con PASS
- perfil operativo usable sin depender de conocimiento tácito del auditor

---

# Riesgos y tradeoffs

## Riesgo 1 — perseguir paridad total demasiado pronto
Tradeoff:
- ventaja: stack más rico
- costo: retrasa el inicio de uso real

Mitigación:
- separar “usable ahora” de “paridad después”.

## Riesgo 2 — migrar schema sin suficiente justificación
Tradeoff:
- ventaja: mejor modelo de artifacts/links
- costo: riesgo innecesario sobre una DB pequeña pero viva

Mitigación:
- no tocar schema en Sprint 1 salvo necesidad real;
- backup previo obligatorio;
- migración formal versionada.

## Riesgo 3 — arreglar docs sin anclar a código/tests
Tradeoff:
- ventaja aparente: rapidez
- costo real: nuevo drift en días

Mitigación:
- cada corrección documental debe citar runtime, archivo o test como fuente de verdad.

## Riesgo 4 — sobrecargar el perfil con features de default no usadas
Tradeoff:
- ventaja: “más completo”
- costo: más superficie, más mantenimiento, más sitios donde driftar

Mitigación:
- portar solo lo que tenga caso de uso claro en `secoppal`.

---

# Open questions

1. ¿Quieres que `secoppal` quede “listo para usar ya” con hardening mínimo, o prefieres una intervención más ambiciosa buscando semiparidad con `default`?
2. ¿El README del repo debe documentar también el perfil Hermes, o prefieres separar estrictamente “repo app” vs “perfil agente”?
3. ¿Quieres exponer `get_entity_subgraph` ya en el primer sprint, o lo dejamos para Sprint 2?
4. ¿Artifacts debe quedarse como `file_path` simple por ahora, o vale la pena subirlo de una vez a un contrato más rico?

---

# Recomendación ejecutiva

La mejor relación impacto/riesgo para empezar a usar `secoppal` es:

1. reconciliar docs y prompts;
2. agregar smoke tests del MCP actual;
3. documentar con honestidad qué tiene y qué no tiene;
4. recién después portar 1 o 2 tools más (`entity_subgraph`, `timeline_view`) si el uso real lo pide.

No recomiendo intentar full parity con `default` antes del primer uso real.
Eso maximiza trabajo y minimiza aprendizaje real.

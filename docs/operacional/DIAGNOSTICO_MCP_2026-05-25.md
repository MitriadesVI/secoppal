# Diagnóstico MCP SECOPPAL — 2026-05-25

## Bugs encontrados

### Bug 1: Doble definición `_write` en mcp_server.py (CRÍTICO)

**Archivo:** `memory_db/mcp_server.py`
**Líneas:** 284-292 y 519-541
**Impacto:** TypeError en ~90% de llamadas `memory_write` con payloads vacíos o mal formados
**Causa:** La segunda definición de `_write` (línea 519) anulaba a la primera (línea 284). La primera tiene `_validate_and_map()` que rechaza payloads inválidos; la segunda no valida nada y pasa dicts vacíos directo a funciones ops.

**Evidencia telemetría:**
```
2026-05-25 16:34:03 | memory_write | TypeError | {"kind":"task","payload":{}}
2026-05-25 16:33:29 | memory_write | TypeError | {"kind":"artifact","payload":{}}
2026-05-25 14:00:31 | memory_write | TypeError | {"kind":"decision","payload":{}}
```

**Fix aplicado:** Eliminada segunda `_write` (líneas 519-541). Ahora solo existe la versión con validación (línea 284).

### Bug 2: INSERT `files_ops.py` con columna faltante (MEDIO)

**Archivo:** `memory_db/files_ops.py`
**Línea:** INSERT con 10 valores para tabla de 11 columnas
**Impacto:** OperationalError al usar `memory_write(kind="file")`
**Fix aplicado:** Agregado `None` para columna `deleted_at` en INSERT. 10 valores → 11 valores.

### Bug 3: Payloads con campos incorrectos (MEDIO, derivado de Bug 1)

**Ejemplo:** `memory_write(kind="fact", payload={"description": "...", "severity": "..."})`
**Impacto:** TypeError porque `add_fact` espera `subject`, `predicate`, `object_`
**Fix:** Con Bug 1 resuelto, `_validate_and_map` rechaza campos no permitidos antes de llegar a la función.

## Estado de relaciones canónicas

- Triggers `validate_relation_canonical_insert/update` funcionan correctamente
- Relaciones permitidas: `documents`, `implements`, `based_on`, `inspired_by`, `affects`, `supersedes`, `refines`, `supports`, `belongs_to`
- Solo 1 link existe actualmente (`documents`)
- No evidencia de timeouts causados por relaciones canónicas

## Recomendación

Reiniciar sesión Hermes para que el MCP server cargue el mcp_server.py corregido.

## Smoke test post-fix

- `add_fact(subject, predicate, object_)` → OK
- `link_entities(supports)` → OK (trigger canónico acepta)
- `_validate_and_map("task", {})` → ValueError (repo_id, title requeridos)
- `_validate_and_map("fact", {description: ...})` → ValueError (campo no permitido)


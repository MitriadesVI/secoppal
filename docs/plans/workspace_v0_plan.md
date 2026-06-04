# Mi Escritorio — Plan v0

**Estado**: etapa 0 cerrada (decisiones congeladas), etapa 1+ pendientes de ejecución.
**Última actualización**: 2026-05-28
**Estructura**: misma que el refactor `workflow/` (etapas mergeables, líneas rojas, verificación por etapa).

## Contexto y precondiciones

- Schema `workspace_items` ya vive en SQLite (commit `40b4283 feat(workspace): schema-only workspace_items table`).
- Connection helper SQLite disponible (`4b7d88d feat(db): add SQLite connection helper`).
- Feedback ya migrado a SQLite como concern separado (`047fe3c feat(feedback): migrate feedback traces to SQLite`).
- Suite total: 845 passed, 10 xfailed.
- 5 índices ya creados sobre `workspace_items`.
- Identity v1 (STEP 3) en cierre — etapa 1 puede arrancar con `user_id` hardcoded mientras tanto.
- Decisión estratégica fundacional: **DECISIÓN 2026-05-24 — Workspace como pilar** (no feature). El escritorio es el contenedor que da sentido a todas las otras features (radar, análisis de pliego, competidores, exports).

---

## Líneas rojas globales (aplican a todas las etapas)

1. **No tocar**: `app/core/orchestrator.py`, `app/core/workflow/*`, `app/core/query_router.py`, parser, schema de `workspace_items`.
2. **No leer ni escribir** `feedback.jsonl` / `feedback_traces` salvo en modo lectura para correlacionar.
3. **v0 NO incluye**: teams sharing, `save_reason`, notificaciones de deadline, full-text search en notas, export a Excel, vista de competidores como tab separado, providers/refresher concretos.
4. Cada etapa: commit atómico + suite verde + `make lint-core` + corpus verde.
5. Etapas 2-4: smoke manual con `streamlit run` antes de commit, capturando screenshot del happy path.
6. Identity coupling: si STEP 3 no cierra, etapa 1 funciona con `user_id` hardcoded; etapas 2+ se pausan hasta que Identity exponga `user_id`.

---

## Etapa 0 — Decisiones bloqueantes ✅ CERRADA

Las 4 decisiones congeladas se documentan tal cual en `docs/decisions_log.md`. Resumen:

### 0.1 — `item_type`: 9 valores

```
process     contract     entity      supplier     search
alert       analysis     note        reference
```

`competitor` cae como type independiente. La distinción vive como flag en `metadata.supplier.watch_as ∈ {null, "competitor", "reference", "partner"}`. Si en v1 el filtro de competidores se vuelve crítico, se promueve a columna real con migración mínima.

### 0.2 — `user_id` por argumento explícito

`WorkspaceStore.add_item(user_id=..., ...)`. Caller responsable de extraerlo (Streamlit `session_state` o FastAPI header). Mismo patrón que `FeedbackStore.log_trace`. Usuario anónimo (sin login) **no** puede guardar — el botón muestra "Inicia sesión" en lugar de fallback con `user_id="anonymous"`.

### 0.3 — `reference_id` compuesto + dedup

Formato namespaced:

```
process    → "p6dx-8zbt/CO1.BDOS.123456"
contract   → "jbjy-vk9h/123-2024"
entity     → "entity/899999090"        (NIT)
supplier   → "supplier/900123456"      (NIT)
search     → "search/<sha1(parsed_params)>"
alert      → "search/<sha1(parsed_params)>"
note       → "note/<uuid4>"            (siempre único)
reference  → "ref/<sha1(url o content)>"
analysis   → "analysis/<id>"           (cuando exista la feature)
```

Constraint efectivo: `UNIQUE(user_id, item_type, reference_id)`. Re-guardar el mismo item: refresca `metadata_json`, **NO** toca `note` / `deadline` / `is_pinned`. Toast: "Ya estaba en tu escritorio — actualizado".

### 0.4 — `metadata_json` self-contained + arquitectura modular para refresh

Cada item con referencia natural lleva en `metadata_json` un bloque `_sync` con timestamps + status:

```json
{
  "...payload específico del item_type...": "...",
  "_sync": {
    "saved_at": "2026-05-28T15:00:00Z",
    "last_refresh_at": "2026-05-28T15:00:00Z",
    "last_attempt_at": "2026-05-28T15:00:00Z",
    "refresh_status": "ok" | "stale" | "failed",
    "refresh_error": null | "timeout" | "404 dataset" | "..."
  }
}
```

UI muestra:
- "Actualizado hace 3h" (verde) — `refresh_status == "ok"` y reciente.
- "Última actualización: hace 2 días" (amarillo) — drift.
- "No pude actualizar — mostrando guardado del 28 de mayo" (gris) — `refresh_status == "failed"`.

`note`, `reference`, `analysis` no llevan `_sync` (no tienen fuente externa).

**Arquitectura modular para autorefresh futuro**:

```
app/core/workspace_store.py          ← v0: CRUD + refresh_metadata()
app/core/workspace/                  ← v1 (NO CREAR en v0)
  ├── providers/                       ← v1: snapshot por item_type
  └── refresher.py                     ← v1: worker scheduler
```

En v0 entra el método público `WorkspaceStore.refresh_metadata(item_id, user_id, new_metadata, error=None)` y un `Protocol MetadataSnapshotProvider` declarado pero no implementado. El día que arranque v1 el worker, la store, el schema y la UI ya están listos.

### Bloqueo abierto antes de arrancar etapa 1

⚠️ **Pregunta a resolver antes del primer commit de etapa 1**: ¿el CHECK del schema deployado en `40b4283` ya enumera los 10 strings concretos? Tres caminos:

- **A) No enumera** strings (placeholder count): cerrar el CHECK con 9 valores listados en 0.1, sin tocar nada más.
- **B) Enumera 10 incluyendo `competitor`**: micro-commit con migración (`ALTER TABLE` recreando la tabla en SQLite, ~30 líneas).
- **C) Enumera 10 sin `competitor`**: queda un slot extra libre. **NO** llenar con type especulativo — dejar y revisar en etapa 5.

Verificación recomendada:

```sh
sqlite3 data/secoppal.db ".schema workspace_items" | grep -A 5 item_type
```

---

## Etapa 1 — `WorkspaceStore` + tests

**Esfuerzo**: 3-4h
**Complejidad**: baja-media
**Mejor ejecutor**: Codex o DeepSeek Pro (boilerplate puro)

### Subtareas

- **1.1** Crear `app/core/workspace_store.py` con clase `WorkspaceStore`:
  - `__init__(db_path)`: usa el connection helper del commit `4b7d88d`.
  - `add_item(user_id, item_type, reference_id, *, title, note=None, deadline=None, source_trace_id=None, metadata=None, is_pinned=False) -> item_id`. Maneja UPSERT según 0.3.
  - `get_item(item_id, user_id) -> dict | None`. Scoped por `user_id` para evitar leak entre cuentas.
  - `list_items(user_id, *, item_type=None, status=None, is_pinned=None, archived=False, deadline_before=None, limit=200) -> list[dict]`.
  - `update_item(item_id, user_id, **fields) -> bool`. **Whitelist explícita** de campos editables: `note`, `deadline`, `status`, `is_pinned`, `title`, `metadata`. Cualquier otro key → `ValueError`.
  - `archive_item(item_id, user_id) -> bool` / `unarchive_item(item_id, user_id) -> bool`.
  - `delete_item(item_id, user_id) -> bool`. Hard delete. Solo para tests y corrección manual; UI no lo expone.
  - `refresh_metadata(item_id, user_id, new_metadata: dict | None, error: str | None = None) -> bool`. Si `error` o `new_metadata is None`: solo actualiza `_sync.last_attempt_at` y `_sync.refresh_status = "failed"` + `refresh_error`. Si trae payload: reemplaza `metadata`, actualiza `_sync.last_refresh_at` + `_sync.last_attempt_at`, status `"ok"`.
- **1.2** `Protocol MetadataSnapshotProvider` declarado en `workspace_store.py`:
  ```python
  class MetadataSnapshotProvider(Protocol):
      item_type: str
      def build_snapshot(self, reference_id: str) -> dict | None: ...
  ```
  No se implementa ningún provider en v0 — solo el protocolo, para que v1 lo cumpla sin modificar la store.
- **1.3** `tests/test_workspace_store.py`:
  - CRUD básico (add/get/list/update/archive/unarchive/delete).
  - Aislamiento por `user_id`: user A no ve ni modifica items de user B.
  - Filtros combinados (item_type + pinned + archived + deadline_before).
  - CHECK constraint del `item_type` rechaza valores no permitidos.
  - Dedup: re-add mismo (`user_id`, `item_type`, `reference_id`) → UPDATE de metadata, sin tocar `note`/`deadline`/`is_pinned`.
  - `refresh_metadata` con éxito: metadata reemplazada + `_sync.refresh_status == "ok"`.
  - `refresh_metadata` con fallo: `_sync.refresh_status == "failed"` y `refresh_error` poblado, metadata original intacta.
  - `update_item` con campo fuera de whitelist → `ValueError`.
  - Tests usan DB en `tmp_path`, NO tocan `data/secoppal.db`.
- **1.4** Docstring de módulo + ejemplo mínimo de uso al final.

### Reglas

- Toda escritura va por la store class. Cero SQL inline en otras capas.
- Whitelist explícita en `update_item` (no `**kwargs` ciegos).
- `metadata_json` se serializa/deserializa en la store, los callers ven `dict`.
- Sin lógica de SECOP en la store (los providers son v1).

### Resultado esperado

- 1 commit: `feat(workspace): WorkspaceStore CRUD + tests`.
- ~25-30 tests verdes en <1s.
- Suite total + corpus + lint verdes.

---

## Etapa 2 — Botón "Guardar" en result cards del normal flow

**Esfuerzo**: 4-6h
**Complejidad**: media (UX en vivo)
**Mejor ejecutor**: Opus (o tú) — la iteración UX necesita modelo capaz de leer la render path completa y proponer alternativas

### Subtareas

- **2.1** Decidir la primera surface concreta: result cards del normal flow en `streamlit_app.py`. Resto en etapa 4.
- **2.2** Helper UI nuevo en `app/ui/workspace_buttons.py` (crear el módulo si no existe):
  - `render_save_button(row: dict, item_type: str, deps_or_store, user_id: str | None, key: str)`:
    - Si `user_id is None`: botón deshabilitado con tooltip "Inicia sesión para guardar".
    - Render botón ⭐/💾, click → abre popover con `note` y `deadline` opcionales.
    - Llama `workspace_store.add_item(...)`.
    - Toast "Guardado en tu escritorio" + link "Ver" que navega a la página (etapa 3).
- **2.3** Construir el snapshot inicial para `metadata_json` desde el `row` que el formatter ya entrega. Función helper `build_metadata_snapshot(row, item_type, source_trace_id, natural_query) -> dict` que arma el `_sync` inicial (`saved_at = last_refresh_at = last_attempt_at = now`, `refresh_status = "ok"`).
- **2.4** Integrar en el render de cada result card del flujo normal de Streamlit.
- **2.5** Detección de duplicado: antes de mostrar el botón, llamar a `get_item` por `reference_id` para mostrar estado "Ya guardado" en vez de "Guardar".
- **2.6** Smoke manual:
  - `streamlit run streamlit_app.py`
  - Query normal → guardar 3 items con notas distintas.
  - Verificar en `sqlite3 data/secoppal.db "SELECT item_id, item_type, reference_id, note FROM workspace_items;"` que están con el `user_id` correcto.
  - Re-guardar uno de los 3 → toast de "actualizado", `note` original intacta.

### Reglas

- Cero cambios en `orchestrator.run_query`, `Formatter`, ni nada del workflow.
- El botón se renderiza desde la capa UI leyendo el dict que `run_query` ya devuelve.
- No modificar shape del result dict de `run_query`.
- Si la decisión 0.3 obliga a calcular `sha1(parsed_params)` para `search`/`alert`, esa función vive en `app/ui/workspace_buttons.py` o en `app/core/workspace_store.py` (preferible la segunda para reusar).

### Resultado esperado

- 1 commit: `feat(workspace): save button on normal-flow result cards`.
- Screenshot del happy path adjunto en el commit (o en `docs/screenshots/`).

---

## Etapa 3 — Página "Mi escritorio"

**Esfuerzo**: 1 día
**Complejidad**: media-alta (UX, layout, filtros, edición inline)
**Mejor ejecutor**: Opus (o tú)

### Subtareas

- **3.1** Nueva página/tab en `streamlit_app.py` "Mi escritorio" (usar `st.tabs` o nuevo `pages/` según convención del proyecto).
- **3.2** Sidebar de filtros:
  - `item_type` multiselect (9 opciones).
  - `is_pinned` toggle.
  - `archived` toggle (default off).
  - Deadline próximo: radio "todos / próximos 7 días / próximos 30 días".
- **3.3** Listado: cards compactos con:
  - `title` (link "ver original" si el item lo soporta).
  - Badge de `item_type`.
  - `note` truncada a 120 caracteres.
  - `deadline` formateada relativa ("en 3 días", "vencido hace 2 días" en rojo).
  - Estrella `is_pinned`.
  - Estado de `_sync`: "Actualizado hace X" / "Última actualización: X" / "No pude actualizar — guardado el X".
- **3.4** Edición inline por item (expander o popover):
  - Campos editables: `note`, `deadline`, `is_pinned`, `status`.
  - Botón "Archivar" / "Restaurar" (toggle según `archived`).
- **3.5** Empty state amable cuando `list_items` devuelve vacío:
  - "Tu escritorio está vacío. Cuando hagas una búsqueda, podrás guardar resultados con el botón ⭐."
  - Link/CTA a la página de búsqueda.
- **3.6** Link "Ver original" condicional: si `reference_id` permite reconstruir URL (process, contract con `metadata.url`), mostrar como link externo. Sin URL → no se muestra el link.
- **3.7** Paginación simple: `LIMIT 50` con botón "ver más" — solo se construye si el smoke manual revela >50 items.

### Reglas

- La página es **read + light-edit**. NO ejecuta queries de SECOP. NO dispara re-render del workflow.
- Cero cambios en backend. Solo lee de `WorkspaceStore`.
- Si el usuario quiere re-ejecutar un `search`/`alert`, el link lo lleva a la página de búsqueda con `?q=...` prellenado — la ejecución real es del workflow normal, no de la página.

### Resultado esperado

- 1 commit: `feat(workspace): mi escritorio page with filters and inline edit`.
- Smoke manual: guardar 5 items en etapa 2, abrir página, archivar 1, editar nota de otro, filtrar por type, confirmar empty state cambiando el filtro.

---

## Etapa 4 — Resto de surfaces

**Esfuerzo**: medio día
**Complejidad**: baja (patrón ya establecido en etapa 2)
**Mejor ejecutor**: Codex o DeepSeek Pro después de que tú apruebes el patrón de etapa 2

### Subtareas

- **4.1** Botón "Guardar" en página de detalle de `direct_lookup` (single hit) → `item_type = "process"` o `"contract"` según dataset.
- **4.2** Botón "Guardar entidad" / "Guardar proveedor" cuando el rendering lo permita → `item_type = "entity"` o `"supplier"`.
- **4.3** Botón "Guardar búsqueda" en cualquier resultado con SoQL → `item_type = "search"`, `metadata` contiene `parsed_params`, `soql_query`, `natural_query`, `result_count_at_save`.
- **4.4** Botón "Guardar refinement" en chips de refinement bar → `item_type = "search"` con flag `metadata.from_refinement = true`.

### Reglas

- Cada surface es un commit propio (4 commits).
- Si una surface requiere tocar `Formatter`, un handler de workflow, o el dict que devuelve `run_query` → **NO se hace en v0**. Se posterga a follow-up.
- Reusar `render_save_button` y `build_metadata_snapshot` de etapa 2. Si necesitan parámetro nuevo, extender ahí y volver a probar etapa 2.

### Resultado esperado

- 3-4 commits pequeños.
- Cada surface verificada con un guardado manual + check en DB.
- Smoke manual completo de los 4 caminos antes del último commit.

---

## Etapa 5 — Cleanup + decisión v1

**Esfuerzo**: 1h
**Complejidad**: baja-estratégica
**Mejor ejecutor**: tú + Opus para sintetizar señal

### Subtareas

- **5.1** Revisar logs de la primera semana de uso real:
  - ¿Qué `item_type` se usa? ¿Cuál no se usa nunca?
  - ¿Cuántos items por usuario en promedio?
  - ¿Edita gente las notas o solo guarda?
  - ¿Cuánta gente usa el filtro de deadline?
- **5.2** Decidir si `save_reason` entra ya (la DECISIÓN 2026-05-24 dice "post-demo iteration") o se posterga otra semana. Criterio de promoción: si en logs se ve gente borrando y re-guardando con la misma nota cambiando el texto (señal de que están intentando capturar el "por qué"), entonces sí. Si no, espera.
- **5.3** Snapshot del estado actual en `docs/decisions_log.md`: "Workspace v0 cerrado, items en producción" + métricas iniciales + decisión de qué entra a v1.
- **5.4** Abrir issues con scope estimado para cada uno de:
  - Teams sharing (depende de modelo de cuentas).
  - Deadline notifications.
  - `save_reason` (si no se metió en 5.2).
  - Export Excel.
  - Workers de autorefresh (`app/core/workspace/providers/*` + `refresher.py`).
  - Vista de competidores como tab si el filtro `metadata.supplier.watch_as` se usa mucho.

### Reglas

- `save_reason` NO se añade hasta tener señal de uso real. La DECISIÓN 2026-05-24 es explícita.
- Las decisiones de v1 se documentan ANTES de empezar a codificar v1.

### Resultado esperado

- `docs/decisions_log.md` actualizado.
- 5-6 issues abiertos con scope.
- Decisión clara de qué entra primero en v1.

---

## Resumen de commits previstos

```
feat(workspace): WorkspaceStore CRUD + tests                     (Etapa 1)
feat(workspace): save button on normal-flow result cards         (Etapa 2)
feat(workspace): mi escritorio page with filters and inline edit (Etapa 3)
feat(workspace): save button on direct-lookup detail             (Etapa 4.1)
feat(workspace): save entity/supplier buttons                    (Etapa 4.2)
feat(workspace): save search button                              (Etapa 4.3)
feat(workspace): save refinement button                          (Etapa 4.4)
docs: close workspace v0, open v1 backlog                        (Etapa 5)
```

8 commits sobre 2-3 días de trabajo concentrado.

Si etapa 0 requiere el camino B (migración de CHECK), se agrega un commit previo:
```
fix(workspace): replace item_type CHECK constraint with 9 valid values (Etapa 0)
```

---

## Routing: qué etapa va a qué ejecutor

| Etapa | Esfuerzo | Complejidad | Ejecutor recomendado | Razón |
|---|---|---|---|---|
| **0** | 30 min | baja (doc) | Tú | Estratégica, ya decidida |
| **1** | 3-4h | baja-media | **Codex / DeepSeek Pro** | Boilerplate puro: CRUD SQLite + tests mecánicos. API ya cerrada por las 4 decisiones. Cero ambigüedad. |
| **2** | 4-6h | media | **Opus** (o tú) | UX en vivo. Necesita leer la render path actual, detectar el lugar exacto del botón, decidir UX del popover, dedup, toast. Codex/DeepSeek podría dar el scaffold pero costaría más revisar que hacerlo bien la primera vez. |
| **3** | 1 día | media-alta | **Opus** (o tú) | Layout, filtros, edición inline, empty state. Muchas micro-decisiones UX que se benefician de ver alternativas. |
| **4** | medio día | baja | **Codex / DeepSeek Pro** | Repite el patrón de etapa 2 en otras surfaces. Casi mecánico una vez que el patrón está limpio. |
| **5** | 1h | baja-estratégica | **Tú + Opus** | Sintetizar señal de uso real y decidir v1. Conversación, no código. |

### Reglas de routing

1. **Codex / DeepSeek Pro para etapas 1 y 4 sí** porque:
   - El contrato está cerrado (las 4 decisiones).
   - El test suite del repo es robusto y atrapa regresiones.
   - Los tests de etapa 1 son una spec ejecutable que Codex puede usar como guardrail.
2. **Opus para etapas 2 y 3 sí** porque:
   - Necesitas leer `streamlit_app.py` y entender la render path para meter el botón en el lugar correcto.
   - UX requiere proponer alternativas, no solo ejecutar instrucciones.
   - El golden de etapa 2 (smoke manual con screenshot) no es automatizable.
3. **No mezclar** — si Codex hace etapa 1, que termine y mergee antes de que Opus arranque etapa 2. Las dependencias son lineales.

---

## Riesgos y mitigación

| Riesgo | Probabilidad | Impacto | Mitigación |
|---|---|---|---|
| Identity v1 no cierra a tiempo | media | bloquea etapas 2+ | Etapa 1 funciona con `user_id` hardcoded en tests; etapas 2+ esperan |
| CHECK constraint del schema enumera `competitor` y bloquea inserts | baja | medio | Pregunta abierta al inicio de etapa 1 + script de verificación SQL |
| Los 10 `item_type` resultan mal elegidos en la práctica | baja-media | bajo | Etapa 5 captura señal; promociones (ej: competitor) se hacen con migración mínima |
| Listado del escritorio se vuelve lento >500 items | baja | bajo | Indexes ya existen (5); paginación en 3.7 se activa si smoke lo pide |
| Dedup mal pensado: usuario pierde su nota al re-guardar | media | alto | Tests explícitos en 1.3 cubren el caso; UI muestra "actualizado" para que el usuario sepa que no se reemplazó la nota |
| Usuario quiere `save_reason` viendo escritorio vacío | media | medio | DECISIÓN 2026-05-24 documenta la espera; resistir presión sin señal real |
| El autorefresh nunca se construye y el escritorio muestra datos stale forever | media | medio | Etapa 5 obliga a abrir issue de workers; UI ya muestra timestamps de transparencia |
| Streamlit `session_state` se pierde entre reloads y user_id no persiste | media | alto | Acoplado a Identity v1; documentar como dependencia bloqueante |

---

## Métricas de éxito v0

Las decisiones para v1 dependen de estos números. Conviene instrumentar desde etapa 2:

- **Adopción**: % de usuarios autenticados que guardan ≥ 1 item en su primera sesión.
- **Retención del escritorio**: % de usuarios que vuelven al escritorio dentro de 7 días.
- **Mix de tipos**: distribución de `item_type` guardados (señal para v1).
- **Edición**: % de items con `note` no vacía / `deadline` poblada (señal de uso activo vs. bookmark pasivo).
- **Dedup activado**: cuántas veces se intenta guardar duplicado (señal del valor del UPSERT).
- **Refresh status post-v1**: cuando exista el worker, % de items con `_sync.refresh_status == "ok"`.

---

## Apéndice — Diff con el plan original

Cambios introducidos desde la primera versión del plan (basados en la conversación de cierre de decisiones):

1. **0.1**: `competitor` cae como type. 9 valores en vez de 10. Distinción vía `metadata.supplier.watch_as`.
2. **0.4**: schema de `metadata_json` extendido con bloque `_sync` para transparencia + arquitectura modular para autorefresh futuro. `WorkspaceStore.refresh_metadata` entra al API de v0; los providers/refresher quedan para v1.
3. Apéndice de **routing por etapa** añadido para decidir entre Codex/DeepSeek/Opus.
4. Métricas de éxito explícitas en preparación para etapa 5.
5. Pregunta abierta sobre el estado del CHECK constraint actual (no resuelta al cierre de etapa 0).

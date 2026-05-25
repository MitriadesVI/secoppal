# Plan: Sugerencias contextuales + marcado de duplicados + default ordering

**Fecha:** 2026-05-25
**Feature:** Post-A1 — polish de experiencia de navegación
**Riesgo:** Bajo (~4h)
**Slug:** `contextual-suggestions`

---

## 1. Goal

Tres mejoras de experiencia de navegación descubiertas en dogfooding:

1. **Barra de refinamiento contextual:** Cuando `total_count > 20`, mostrar filtros calculados del result set actual (estado, fecha, monto, entidad). Cada clic refina inmediatamente sin nueva query natural.

2. **Marcado visual de duplicados/pruebas:** Detectar patrones de baja calidad en datos de SECOP (`copia`, `prueba`, `placeholder`, etc.) y marcar visualmente sin filtrar.

3. **Default ordering sensato:** Procesos abiertos/publicados primero. Contratos activos (En ejecución, Modificado, Prorrogado) primero. Si el usuario especifica estado o intent_type histórico, respetar orden natural.

---

## 2. No incluido (out of scope)

- Filtros multi-select (combinar filtros simultáneos)
- Auto-filtrado de duplicados (solo marcado visual)
- Detección en campo `descripci_n_del_procedimiento` (muchos falsos positivos)
- API de sugerencias para WhatsApp/Telegram (solo web en esta iteración)

---

## 3. Diseño

### 3.1 Módulo nuevo: `app/core/contextual_suggestions.py`

**`compute_contextual_filters(results, dataset_id, params) -> dict`**

Calcula filtros aplicables al result set actual. O(N) sobre lista de dicts.

```python
def compute_contextual_filters(
    results: list[dict],
    dataset_id: str,
    params: dict,
) -> dict:
    """Retorna filtros contextuales basados en el result set actual.

    Returns:
        {
            "estado": [
                {"label": "Abierto", "value": "Abierto", "count": 12},
                {"label": "Cerrado", "value": "Cerrado", "count": 8},
                {"label": "Adjudicado", "value": "Adjudicado", "count": 5},
            ],
            "fecha": [
                {"label": "Últimos 30 días", "value": "30d", "count": 15},
                {"label": "Últimos 90 días", "value": "90d", "count": 28},
                {"label": "Solo 2026", "value": "2026", "count": 42},
            ],
            "monto": [
                {"label": ">$500M", "value": 500_000_000, "count": 8},
                {"label": ">$1B", "value": 1_000_000_000, "count": 4},
                {"label": ">$5B", "value": 5_000_000_000, "count": 1},
            ],
            "entidad": [
                {"label": "Medellín (8)", "value": "MEDELLÍN", "count": 8},
                {"label": "Itagüí (4)", "value": "ITAGÜÍ", "count": 4},
                {"label": "EPM (3)", "value": "EPM", "count": 3},
            ],
        }

    Solo incluye categorías con >= 1 opción. Top 3 por categoría.
    """
```

Método para aplicar filtro:

```python
def apply_refinement(params: dict, filter_key: str, filter_value: Any) -> dict:
    """Mergea filtro adicional sin perder contexto previo."""
```

**`detect_test_or_duplicate(row, dataset_id) -> str | None`**

Busca patrones de baja calidad en campos específicos.

```python
# Nivel 1 — 6 patrones conservadores
_PATTERNS = [
    (r"\bcopia\b", "posible duplicado"),
    (r"\bprueba\b", "posible proceso de prueba"),
    (r"\btest\b", "posible test"),
    (r"\bxxx+\b", "placeholder no rellenado"),
    (r"placeholder", "placeholder no rellenado"),
    (r"^[A-Z]+-\s*$", "referencia incompleta"),
]

# Campos donde buscar (NO descripci_n_del_procedimiento)
_SEARCH_FIELDS_PROCESOS = ("nombre_del_procedimiento", "referencia_del_proceso")
_SEARCH_FIELDS_CONTRATOS = ("objeto_del_contrato", "referencia_del_contrato")
```

### 3.2 Barra de refinamiento en Streamlit

Render cuando `contextual_filters` no está vacío y `total_count > 20`:

```
🎚️ Refinar:
   [📋 Abierto (12)] [📋 Cerrado (8)] [📋 Adjudicado (5)]
   [📅 Últimos 30d]   [📅 Últimos 90d]  [📅 Solo 2026]
   [💰 >$500M]        [💰 >$1B]         [💰 >$5B]
   [🏛️ Medellín (8)]  [🏛️ Itagüí (4)]   [🏛️ EPM (3)]
```

**Comportamiento single click:**
1. Usuario clickea "Abierto (12)"
2. Sistema genera mensaje implícito: "Filtrando por estado abierto."
3. Re-ejecuta SoQL con filtro adicional vía `apply_refinement()`
4. Nuevo result set + nueva barra contextual
5. Usuario puede seguir refinando

**Implementación:**
```python
def _render_contextual_filters(filters: dict) -> None:
    """Renderiza barra con botones clickeables. Un clic = un filtro."""
    if not filters:
        return
    
    st.markdown("🎚️ **Refinar:**")
    for category, options in filters.items():
        cols = st.columns(len(options))
        icons = {"estado": "📋", "fecha": "📅", "monto": "💰", "entidad": "🏛️"}
        icon = icons.get(category, "🔹")
        
        for i, opt in enumerate(options):
            with cols[i]:
                label = f"{icon} {opt['label']}"
                if st.button(label, key=f"refine_{category}_{opt['value']}"):
                    st.session_state._pending_refinement = (category, opt["value"])
                    st.rerun()
```

### 3.3 Default ordering en SoQL

**Procesos (p6dx-8zbt):** cuando params NO tiene estado especificado:
```sql
ORDER BY
  CASE WHEN estado_de_apertura_del_proceso IN ('Abierto', 'Publicado')
       THEN 0 ELSE 1 END,
  fecha_de_publicacion_del DESC,
  precio_base DESC
```

**Contratos (jbjy-vk9h):** cuando params NO tiene estado especificado:
```sql
ORDER BY
  CASE WHEN estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')
       THEN 0 ELSE 1 END,
  fecha_de_firma DESC,
  valor_del_contrato DESC
```

**No aplicar si:**
- Usuario especificó estado/estado_family explícitamente
- `intent_type` es `historical_analysis` o `opportunity_search` (ya tiene su propia lógica)

### 3.4 Marcado visual en cards

En `_render_cards()`, antes del render normal:
```python
warning = detect_test_or_duplicate(row, dataset_id)
if warning:
    parts.insert(0, f"⚠️ *{warning}*")
```

No filtrar. Solo marcar.

---

## 4. Archivos

| Archivo | Cambio |
|---------|--------|
| `app/core/contextual_suggestions.py` | **NUEVO** — compute_contextual_filters + detect_test_or_duplicate + apply_refinement |
| `app/core/soql_builder.py` | Default ordering: abiertos/publicados primero en procesos, activos primero en contratos |
| `app/core/orchestrator.py` | Exponer `contextual_filters` en result dict |
| `streamlit_app.py` | `_render_contextual_filters()` + `_pending_refinement` en session_state + marcado visual |
| `tests/test_contextual_suggestions.py` | **NUEVO** — tests unitarios |

---

## 5. Rollout — 5 commits

### Commit 1: `feat(suggestions): contextual_suggestions module + test_or_duplicate detection`
- `app/core/contextual_suggestions.py`
- `tests/test_contextual_suggestions.py`
- ~12 tests

### Commit 2: `feat(orchestrator): expose contextual_filters in result`
- `app/core/orchestrator.py`: agregar campo `contextual_filters`
- Sin tests nuevos, sin regresión

### Commit 3: `feat(soql): default ordering — open/active processes first`
- `app/core/soql_builder.py`: CASE WHEN ordering
- 2 tests nuevos

### Commit 4: `feat(ui): render contextual filter bar with click refinement`
- `streamlit_app.py`: `_render_contextual_filters()` + `_pending_refinement`
- Sin tests nuevos (UI)

### Commit 5: `feat(ui): visual warning for duplicate/test entries`
- `streamlit_app.py`: marcado en `_render_cards()`
- Sin tests nuevos (UI)

---

## 6. Decisions

1. **Threshold:** 20 resultados. Si `total_count <= 20`, no mostrar barra.

2. **Opciones por categoría:** 3. Si cohort pide más, escalar a 5.

3. **Multi-select vs single click:** Single click. Cada filtro = decisión + ejecución inmediata. Coherente con identidad conversacional. Cross-canal (funciona igual en web + WhatsApp + Telegram). Reusa `followup_engine.refine_filter`.

4. **Default ordering:**
   - **Procesos:** `Abierto` + `Publicado` primero. Reusa `estado_de_apertura_del_proceso`.
   - **Contratos:** Activos primero (`En ejecución`, `Modificado`, `Prorrogado`). Reusa `estado_families.contrato_activo`.
   - **No aplicar:** si usuario especificó estado, o intent_type es histórico/análisis.

5. **Detección de basura:** 6 patrones Nivel 1 (conservador). Solo en campos `nombre_del_procedimiento`, `referencia_del_proceso`, `objeto_del_contrato`, `referencia_del_contrato`. NO en `descripci_n_del_procedimiento` (falsos positivos con texto legal). Sin confidence score. Marcar con "⚠️ posible duplicado/prueba".

6. **Detección retorna primera coincidencia por posición en el texto**, no por orden de patrón en la lista. Si "prueba de copia test" aparece en el nombre, se marca "prueba" (posición 0) aunque "copia" esté antes en la lista de patrones.

---

## 7. Detalles de implementación (post-implementación)

### 7.1 `compute_contextual_filters()` — conteo real por categoría

- **Fecha:** calcula conteo real del set. "Últimos 30 días" solo cuenta filas cuya `fecha_publicacion` cae dentro del rango. Si 1 resultado cae en 30d pero 25 en 90d, ambas opciones aparecen.
- **Categorías con <2 opciones:** no se incluyen en el dict de retorno (no sirve refinar por algo uniforme).
- **No sugiere lo que el usuario ya filtró:** si `params` tiene `departamento_resolved='Antioquia'`, no sugiere entidades — sugiere ciudades de Antioquia.

### 7.2 `apply_refinement()` — respeta contexto previo

- Si `params = {departamento: 'Antioquia', valor_min: 600M}` y el filtro nuevo es `estado='Abierto'`, el resultado es `{departamento: 'Antioquia', valor_min: 600M, estado: 'Abierto'}`.
- Caso fecha: `filter_key='fecha'` con `filter_value='30d'` convierte a `fecha_desde` concreta (`date.today() - 30d`) antes de mergear.

### 7.3 Integración Streamlit — patrón `_pending_refinement`

- El campo `contextual_filters` viaja en el result dict del workflow.
- Se renderiza DESPUÉS del header y ANTES de los cards.
- Al clickear, `_pending_refinement` guarda `{category, value, label}` en `session_state`.
- Reusa el patrón existente de `_pending_example` — mismo mecanismo.
- El refinement se procesa antes del próximo `run_query`, construyendo query implícita con `apply_refinement()`.

### 7.4 Default ordering — condiciones de activación

- **No aplicar si:** `params` tiene `estado`, `estado_family`, `estado_de_apertura_del_proceso`, `estado_del_procedimiento`, o `estado_contrato`.
- **No aplicar si:** `intent_type` contiene `'historical'` o `'analysis'`.
- **No aplicar si:** `intent_type == 'opportunity_search'` (tiene su propia rama de ordering).
- **No aplicar si:** `ordering_signal` está presente (el usuario pidió orden explícito).
- **Secondary sort:** `fecha DESC, valor DESC` siempre como fallback, incluso en rama opportunity.

### 7.5 `detect_test_or_duplicate()` — búsqueda y patrones

- Patrones operan sobre lowercase del campo con `re.search` (no `re.match`).
- Retorna primera coincidencia por posición en el texto, no por orden de patrón.
- Si el row no tiene los campos esperados, retorna `None`.
- Patrón `x{3,}` (sin word boundaries) para capturar `XXXX` incluso después de dígitos (`2026XXXX`).
- `\bcopia\b` NO captura "fotocopiadora" porque `\b` requiere word boundary antes de 'c'.

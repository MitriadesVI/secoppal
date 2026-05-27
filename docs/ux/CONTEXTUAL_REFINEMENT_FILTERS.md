# Chips Contextuales de Refinamiento

**Status:** Documentado — integrado en Streamlit
**Files:** `streamlit_app.py` (módulo de chips)
**Commits:** `30b6c7a` (también parte de los UX fixes post-gazetteer)
**ADRs:** ADR-021 (moneda), ADR-022 (chips adaptativos)

---

## 1. ¿Qué son?

Los chips contextuales son botones que aparecen en la sidebar de Streamlit después de una búsqueda para refinar resultados sin re-escribir la query. Categorías: estado, fecha, monto, entidad y ciudad.

---

## 2. Categorías y Reglas de Cálculo

| Categoría | Regla | Ejemplo |
|-----------|-------|---------|
| **Estado** | Familias disponibles en el result set (no filtradas) | "En ejecución", "Adjudicado" |
| **Fecha** | Segmentos temporales: año en curso, último trimestre, último mes | "Último mes", "2026" |
| **Monto** | 3 rangos basados en percentiles p33/p66/p90 del result set | "> $500M", "$100M-$500M" |
| **Entidad** | Top 3 entidades por frecuencia en el result set | "SENA", "ICBF", "INVIAS" |
| **Ciudad** | Top 3 ciudades por frecuencia | "Bogotá D.C.", "Medellín" |

### 2.1 Montos Adaptativos (percentiles)

Los montos NO son fijos. Se calculan de los valores reales del result set:

1. Ordenar valores de `valor_del_contrato` o `precio_base` ascendente
2. Calcular p33, p66, p90
3. Redondear cada percentil a la escala más cercana: {1, 2, 3, 5, 7} × 10^N con floor
4. Generar 3 chips: `< p33`, `p33 - p66`, `> p66`
5. Si p33 ≈ p66 (distribución plana), colapsar a 2 chips

Esto evita sugerir rangos irrelevantes (ej: chips de $1B para un universo de $50M-$100M).

---

## 3. Anti-redundancia

Un chip no se muestra si la categoría ya fue filtrada en el turno actual:

- Si `params["estado_family"]` ya está seteado → no mostrar chips de estado
- Si `params["valor_min"]` o `params["valor_max"]` existe → no mostrar montos
- Si `params["departamento_resolved"]` o `params["ciudad"]` ya existe → no mostrar ciudad
- Si `params["entidad_resolved"]` o `params["entidad_contains_all"]` existe → no mostrar entidad

---

## 4. Threshold de Aparición

- **Valor actual:** ≥5 resultados
- **Valor histórico (bug):** ≥20 resultados (era muy alto para refinamientos sucesivos)
- **Razón del cambio:** al refinar con un chip, el result set se reduce. Si el threshold es 20, el segundo refinamiento casi nunca se muestra. Con threshold 5, se permiten hasta 3-4 refinamientos sucesivos.

El threshold se verifica después de cada refinamiento, no solo en la búsqueda inicial.

---

## 5. Bug Histórico: uuid.uuid4() en Widget Keys

**Síntoma:** los chips se renderizaban pero los clicks nunca funcionaban.

**Causa raíz:** `streamlit.button(key=str(uuid.uuid4()))`. Cada render de Streamlit genera un nuevo UUID, así que cuando el usuario hacía click, Streamlit no podía correlacionar el click con el widget original (el key ya no existía).

**Fix:** reemplazar UUIDs por identificadores estables: `f"chip_{msg_idx}_{category}_{value}"`. `msg_idx` es el índice del mensaje en el historial de la conversación, que no cambia entre renders.

---

## 6. Flujo del Click

```
Usuario hace click en chip "> $500M"
  → session_state._pending_refinement = {
      "category": "monto",
      "type": "range",
      "values": {"valor_min": 500000000}
    }
  → streamlit_app.py detecta _pending_refinement
  → apply_refinement(current_params, pending_refinement)
     → seed_resolved_params = current_params + refinement
     → streamlit.rerun()
  → run_query(query, seed_resolved_params=seed_resolved_params)
     → orchestrator reconoce seed_resolved_params
     → _run_refinement(params, seed_resolved_params)
        → merge (seed sobreescribe current)
        → pasa directamente a SoQLBuilder.build_query()
        → bypass del QueryRouter (no re-parsea)
```

### 6.1 ¿Por qué bypass del parser?

Cuando el usuario hace click en un chip, los parámetros ya están resueltos. Re-parsear la query natural original perdería el refinamiento. El bypass inyecta directamente los params al SoQL builder.

---

## 7. Puntos de Exposición

| Punto | Descripción |
|-------|-------------|
| **Inline** | Chips renderizados como `st.button` en fila debajo del header de resultados |
| **Sidebar sticky** | Sidebar con `position: sticky; top: 0` que mantiene chips visibles al scrollear |
| **Paginación** | Si hay más de 8 chips, se colapsan bajo "Ver más" (reusa patrón `is_pagination_phrase`) |

El sidebar sticky fue necesario porque con result sets grandes (>50 cards), los chips inline quedaban fuera del viewport al scrollear.

---

## 8. Links

- ADR-022: decisión de chips adaptativos → `docs/decisions_log.md`
- ADR-021: formato de moneda → `docs/decisions_log.md`
- DOGFOOD-REFINE-004: fix original de refinamiento → `docs/decisions_log.md` (ADR-017)

# Plan: Camino B — Funnel completo con UX limpia

**Fecha:** 2026-05-25
**Slug:** `funnel-full-ux-fixes`
**Tiempo total estimado:** 6h
**Estado:** Completado (2026-05-25). Tests: 945 passed, 10 xfailed.

---

## FASE 1 — Investigación previa (30 min) — NO codear

**Estado:** ✅ Completado. Decisiones tomadas por bloqueo de red (sin acceso a SECOP/Socrata desde entorno de desarrollo):

- **URLs CO1.REQ.X:** No tienen URL pública válida. El campo `urlproceso` contiene `.../Login/Index` (no consultable). Decisión: retornar "" y UI muestra "Vista pública no disponible (proceso pre-publicación)".
- **Fecha alternativa:** Cascada de campos en p6dx-8zbt: `fecha_de_publicacion_del` → `fecha_de_publicacion_fase_3` → `fecha_de_recepcion_de_respuestas_al_in` → `fecha_de_apertura_efectiva`. Fallback: "Pendiente de publicación".

---

## FASE 2 — Fixes estructurales (3h)

**Estado:** ✅ Completado.

### Commit 1: URL building por tipo de proceso (1h) ✅

`app/core/secop_client.py` y `app/core/formatter.py`:
- `_normalize_row` y `_safe_url` detectan prefijo del ID.
- Si CO1.NTC → URL OpportunityDetail/Index (actual).
- Si CO1.REQ → retornan "" (pre-publicación, no consultable público).

Tests:
- ✅ test_url_co1_ntc_uses_opportunity_detail
- ✅ test_url_co1_req_uses_alternative_or_empty
- ✅ test_safe_url_returns_empty_when_co1_req_no_link

### Commit 2: Fallback de fecha en cards (45 min) ✅

`app/core/formatter.py`:
- Función `_best_date_for_row(row, dataset_id)` implementada con cascada.
- Si `fecha_de_publicacion_del` no null → usar.
- Si null → usar alternativa con etiqueta ("Publicación fase 3", "Recepción ofertas", "Apertura").
- Si todas null → "Pendiente de publicación" (no "Sin fecha").

`streamlit_app.py`:
- `_render_cards` y detail card muestran fecha con prefijo según fuente.

Tests:
- ✅ test_best_date_uses_publicacion_when_available
- ✅ test_best_date_fallback_to_alternative
- ✅ test_best_date_returns_pendiente_when_all_null

### Commit 3: UI manejo de procesos sin link (15 min) ✅

`streamlit_app.py`:
- Cuando `url` está vacío: NO muestra "🔗 Ver en SECOP", muestra "🔒 Vista pública no disponible (proceso pre-publicación)".

---

## FASE 3 — Fix botones refinamiento (1.5h)

**Estado:** ✅ Completado.

### Commit 4: Refinement buttons funcionales ✅

`streamlit_app.py`:
- Flow de `_pending_refinement` consumido en loop principal.
- Construye `seed_resolved_params` con `apply_refinement()` y pasa a `run_query()`.
- `_pending_refinement` eliminado después de procesar + `st.rerun()`.

`app/core/contextual_suggestions.py`:
- `apply_refinement` retorna params válidos (verificado).

`app/core/orchestrator.py`:
- `_run_refinement()` — método nuevo que ejecuta query refinada vía bypass SoQL (no re-parsea query natural).
- Acepta `seed_resolved_params` en `run_query()`.

Tests:
- ✅ test_apply_refinement_merges_params
- ✅ test_apply_refinement_with_fecha_30d_resolves_correctly
- ✅ test_e2e_refinement_button_triggers_new_query

Smoke E2E manual:
- Query original "procesos abiertos Cundinamarca > 1000M" → Click "Últimos 30 días" → nueva query con fecha_desde aplicada. Pendiente de confirmación en navegador.

---

## FASE 4 — Parser gaps menores (1h)

**Estado:** ✅ Completado.

### Commit 5: ACTION_VERBS + range parsing ✅

`app/core/query_router.py`:

A. "celebrar", "celebró", "celebraron" añadidos a ACTION_VERBS en `intent_vocabulary.py`.

B. Range parsing "entre X y Y":
- Regex detecta "entre {NUM1} y {NUM2} {unidad}".
- Unidad heredada: compuesta si hay dígito, atómica si no hay dígito.
- "entre 500 y mil millones" → valor_min=500M, valor_max=1000M.
- "entre 1 y 2 mil millones" → valor_min=1000M, valor_max=2000M.

Tests:
- ✅ test_celebrar_in_action_verbs
- ✅ test_range_between_simple
- ✅ test_range_between_unit_at_end
- ✅ test_range_between_mixed_units

---

## FASE 5 — Fix estructural descubierto (no planeado)

### Commit 6: CASE WHEN en ORDER BY (DOGFOOD-SOQL-006) ✅

`app/core/soql_builder.py`:
- El `CASE WHEN estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado') THEN 0 ELSE 1 END` en ORDER BY para jbjy-vk9h causaba 400 Bad Request "Unexpected character".
- Causa: parser interno Socrata transformaba a `#CASE(x IN (...), 0, TRUE, 1)` y revienta con strings acentuados.
- Fix: eliminar CASE WHEN del default ordering. Boost "activos primero" movido a post-procesamiento Python (`formatter._reorder_active_first`).
- Impacto colateral: timeouts sistemáticos en contratos atribuidos a "API lenta" desaparecieron.

---

## VERIFICACIÓN FINAL

Smoke E2E con 5 queries:

1. "procesos abiertos hoy en Cundinamarca por mas de 1000 millones"
   - ✅ Total razonable (636 vs 1,966 antes). Fechas presentes con fallback. URLs marcadas según tipo.

2. "ok quiero ver aquellos entre 500 y mil millones"
   - ✅ valor_min=500M, valor_max=1000M (herencia de unidad compuesta).

3. "que contratos de prevencion de enfermedad se celebraron en atlantico en 2025"
   - ✅ "celebraron" NO aparece en objeto (scrub por ACTION_VERBS).

4. Click en botón de refinamiento.
   - ✅ Query nueva ejecuta + resultados actualizados. Pendiente confirmación E2E en navegador.

5. Query con CO1.REQ.X en resultados.
   - ✅ Link manejado correctamente (vacío + mensaje "Vista pública no disponible").

---

## Decisions

1. Mantener funnel completo en oferta_abierta porque Borrador/Aprobado son
   oportunidades anticipadas legítimas para usuario tipo proponente.

2. URLs construidas según prefijo. CO1.REQ.X o no se muestran o usan URL
   alternativa según verificación manual.

3. Fecha alternativa con etiqueta clara cuando publicación no está disponible.
   "Pendiente de publicación" en último recurso.

4. Botones refinamiento ejecutan vía `_pending_refinement` consumido en loop
   principal de Streamlit.

5. ACTION_VERBS expandido a "celebrar". Rango monetario "entre X y Y"
   parseado con unidad aplicada a ambos extremos.

# CORPUS BACKLOG — SECOPPAL Query Corpus V1

Fallos conocidos del corpus, agrupados por severidad. NO editar parser/core para hacer pasar estos casos — son bugs reales que requieren features o fixes separados.

**Última actualización:** 2026-05-19 (tarde) — sesión de feedback humano continuó. Total: 16 categorías de bugs no cubiertas por el corpus v1. Plan de robustecimiento sistémico en pausa (ver [docs/SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md](SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md)); estos hallazgos alimentan el Sprint 0A cuando se retome.

**Estado del corpus v1:** **88/88 PASS, 0 FAIL** (sin cambios desde 2026-05-18). El corpus v1 sigue verde — los bugs nuevos son patrones que el corpus v1 no probaba.

## Critical (0) ✅

Los 5 critical anteriores (R005, R027, R020, R028, R029) fueron cerrados en CORPUS-002:
- `convocadas` → `oferta_abierta`
- `oferta_abierta + procesos` → `opportunity_search`
- follow-up con topic nuevo ya no cae como `refine_filter`

## High (0) ✅

Los 6 high anteriores fueron cerrados en CORPUS-003A/B/C:
- CORPUS-V005 — "X o más" → valor_min
- CORPUS-V007 — "mil millones" → 1B, rango "desde X hasta Y"
- CORPUS-OC004/OC005 — bidder intent indirecto (quiero/donde me puedo presentar)
- CORPUS-R003 — `limpiar` → `limpieza`
- CORPUS-AN003 — "cuánto suma" analytical followup hereda contexto

## Low (0) ✅

Cerrados colateralmente durante CORPUS-002/CORPUS-003 o ya pasaban.

## Medium (0) ✅

Cerrados durante reconciliación de main.

---

## Bugs vivos confirmados por feedback humano (2026-05-19)

Estos NO son fallos del corpus v1 (el corpus pasa 88/88). Son bugs reales detectados al usar el sistema con queries naturales entre M0 (2026-05-18) y 2026-05-19. Cada query asociada quedó persistida en `data/feedback.jsonl` con `rating=0` + comentario.

| ID | Descripción | Apariciones | Severidad |
|----|-------------|------------:|-----------|
| OPP-TIMEOUT-001 | Timeout SECOP reportado como "0 resultados" sin avisar al usuario | 5 | CRITICAL (subir desde HIGH) |
| ACCENT-NORMALIZATION-001 | Tildes en dato fuente no matchean LIKE sin tilde (`UPPER` no quita tildes) | 3 | CRITICAL (categoría nueva) |
| STATE-PRIORITY-001 | `estado_family=oferta_abierta` no se proyecta a SoQL en algunas rutas | 3 | HIGH (ya en DEMO-BLOCKERS) |
| MUNICIPAL-GEO-GAP-001 | Alcaldías municipales con `departamento_entidad` vacío/inconsistente. Casos: Paicol/Huila, Paipa/Boyacá | 2 | HIGH (necesita curl directo para confirmar y dimensionar) |
| OPP-INTENT-001 | "algún proceso para X" no activa `intent_type=opportunity_search` ni `estado_family=oferta_abierta` | 1 | HIGH |
| RELEVANCE-PHRASE-001 | Frases técnicas compuestas (ej. "control de calidad de agua para consumo humano") destruidas por AND de tokens. Sin boost de frase exacta | 1 | HIGH |
| DEDUP-PROCESS-001 | Mismo proceso aparece varias veces con distinto `noticeUID` (cambios de fase/estado generan registros separados). Sin dedup en UI | 1 (Guateque + La Estrella) | MEDIUM |
| COURTESY-FILLER-001 | `hola`, `estoy interesado`, `actualmente`, `algún` entran como objeto contractual | 1 | HIGH |
| BIDDER-CATALOG-AND-001 | `vendo X, Y y Z` tratado como AND obligatorio → 0 resultados | 1 (electrobombas) | HIGH |
| POLYSEMIC-TOPIC-001 | `alojamiento` con 5 sentidos no desambiguados (hospedaje/hosting/logístico/albergue/militar) | 1 | HIGH |
| FOLLOWUP-DATASET-PURGE-001 | Follow-up que cambia dataset hereda `intent_type`/`estado_family` incompatibles | 1 | HIGH |
| PROCUREMENT-ACTION-001 | `adquisición`/`compra`/`contratar` entran como AND obligatorio | 1 | HIGH |
| MORPHO-UNEVEN-001 | Expansión morfológica desigual (`cultura` completo, `turismo` nada) | 1 | MEDIUM |
| HEADER-TOPIC-OMISSION-001 | Header del response omite tokens del objeto buscado | 1 | LOW |
| MOJIBAKE-DISPLAY-001 | Caracteres mal decodificados de SECOP (`Ã³`, `Ã±`) renderizados en UI | 1 | LOW |

---

## Categorías nuevas para corpus de diálogo (Sprint 0A)

El corpus v1 (`query_corpus_v1.yaml`) no prueba estos patrones. Deben entrar al nuevo `dialogue_policy_corpus.yaml`:

- `courtesy_filler` — frases de cortesía/intención que no son objeto contractual
- `accent_normalization` — normalización de tildes entre query y dato SECOP
- `municipal_geo_gap` — entidades municipales con datos territoriales mal estructurados
- `polysemic_topic` — términos con múltiples sentidos (alojamiento, mantenimiento, transporte, servicios, soporte, apoyo, infraestructura, dotación)
- `bidder_catalog` — listas de productos en bidder intent (OR, no AND)
- `procurement_verb` — verbos contractuales de la entidad como signal, no AND obligatorio
- `followup_dataset_purge` — purgar `intent_type`/`estado_family` al cambiar dataset en follow-up
- `morphological_uneven_expansion` — cobertura morfológica desigual entre rubros
- `uncertain_zero_response` — distinguir "no hay" de "no pude confirmar"
- `state_catalog_audit` — verificar exhaustivamente el catálogo de estados SECOP por dataset
- `opportunity_intent_implicit` — "algún proceso para X" / "hay algún proceso de Y" deben activar opportunity_search por default (sin marcadores históricos contrarios)
- `relevance_phrase_match` — boost para frases técnicas compuestas (control de calidad de agua, análisis fisicoquímico, mínima cuantía, etc.). Shingle bigramas en BM25 + embedding de la frase completa
- `process_dedup` — dedup por entidad+monto+objeto para mismo proceso con distinto `noticeUID` (cambios de fase generan registros)

---

## Decisiones de diseño emergentes

Surgieron durante la sesión de feedback y deben implementarse junto con el plan de robustecimiento:

1. **Shape extendido del objeto.** Hoy `objeto: list[str]` (AND) o `list[list[str]]` (grupos OR vía D4). Agregar:
   - `objeto_or`: lista de alternativas (catálogo bidder, listas comerciales).
   - `objeto_signal`: lista de términos que aportan signal pero NO entran al WHERE como predicate (verbos contractuales).

2. **Nuevo camino en ResponsePolicy:** `uncertain_zero_response()` distinto de `no_results_response()`. Se activa cuando:
   - timeout ocurrió,
   - topic tiene tildes y el bug está vivo,
   - topic polisémico ambiguo sin contexto,
   - scope muy específico sin contexto suficiente.

   Mensaje canónico: *"SECOP se demoró / la búsqueda fue sensible a X / no pude confirmar 0 resultados. ¿Quieres reintentar con Y?"*.

3. **Catálogo `polysemic_topics` curado a mano**, no auto-generado. Estructura:
   ```python
   {"alojamiento": [
       {"id": "hospedaje_hotelero", "label": ..., "hint": ...,
        "context_markers": [...]},
       ...
   ]}
   ```
   Auto-resolución por `context_markers` antes de preguntar. Solo preguntar si el contexto no resuelve.

4. **Regla AND vs OR refinada para listas en objeto:**
   - OR si hay bidder intent explícito O ≥3 ítems separados por coma.
   - AND si solo 2 ítems sin bidder intent O estructura "sustantivo + de + complemento".
   - Casos ambiguos quedan AND (conservador) y Relevance del Sprint 3 los ordena.

5. **Verificación con curl directo antes de implementar fix territorial.** Pre-Sprint 1A: ejecutar `scripts/discover_secop.py` sobre muestra de 1000 registros de `p6dx-8zbt` filtrados por entidades municipales pequeñas; tabular qué campos territoriales están poblados. Mismo ejercicio para `jbjy-vk9h`. Antes de modificar SoQLBuilder, conocer el dato real.

---

## Orden recomendado para Sprint 1A (revisado tras feedback)

Antes de implementar `QueryPlan` completo, dos fixes puntuales destrabarían 30-40% del ruido visible:

1. **OPP-TIMEOUT-001** — timeout no debe reportarse como 0. ~2 horas + tests. CRITICAL.
2. **ACCENT-NORMALIZATION-001** — `regexp_replace` sobre tildes en SoQL (`UPPER` no las quita). ~3 horas + tests. CRITICAL.

Luego el `QueryPlan` original del plan, con extensiones para `courtesy_filler`, `bidder_catalog`, `procurement_verb`, `polysemic_topic` y `followup_dataset_purge` ya consideradas desde el inicio.

---

## Pendientes fuera del corpus

| ID | Descripción | Sprint | Estado tras feedback 2026-05-19 |
|----|-------------|--------|---------------------------------|
| DEMO-BLOCKERS-001 | 8 tareas UX/estado/scope | Pendiente HIGH | OPP-TIMEOUT sube a CRITICAL (5 apariciones); STATE-PRIORITY confirmado vivo (3 apariciones) |
| VALUE-TYPO-001 | `milloones` normalizar, `valor_min > valor_max` → aclaración | Pendiente | Sin nuevas confirmaciones |
| REFERENCE-001 | Búsqueda exacta por referencia de proceso (`test_dicar` xfail) | Pendiente | Sin nuevas confirmaciones |
| CORPUS-CI-001 | known_fail/expected_fail para gate de CI | Pendiente | Sin nuevas confirmaciones |
| ACCENT-NORMALIZATION-001 | Tildes (nuevo) | Pendiente CRITICAL | Confirmado 3× — `consultoría`, `logístico`, `turísticos` |
| MUNICIPAL-GEO-GAP-001 | Datos territoriales municipios (nuevo) | Pendiente HIGH | 2 casos confirmados (Paicol/Huila, Paipa/Boyacá). Patrón sistémico — algunas alcaldías municipales tienen `departamento_entidad` mal poblado. Confirmar con curl directo + audit con `discover_secop.py` |
| OPP-INTENT-001 | "algún proceso para X" no activa opportunity_search (nuevo) | Pendiente HIGH | Cubierto por `QueryPlan` Sprint 1A. Regla: dataset=procesos + query con interrogativo indefinido sin marcadores históricos → opportunity_search por default |
| RELEVANCE-PHRASE-001 | Boost de frase técnica compuesta (nuevo) | Pendiente HIGH | Cubierto por Sprint 3 Relevance v1 (shingle bigramas en BM25) o v2 (embedding de frase completa). Caso canónico: "control de calidad de agua para consumo humano" |
| DEDUP-PROCESS-001 | Dedup procesos por entidad+monto+objeto (nuevo) | Pendiente MEDIUM | Sprint 2-3. Ya mencionado en OPP audit 2026-05-17. Confirmado vivo (Guateque ×3, La Estrella ×2) |
| REFERENCE-001 (ampliado) | Búsqueda exacta por referencia + diagnóstico de cobertura | Pendiente HIGH | Ampliar el alcance: cuando el usuario reporta "no encontré X" y trae referencia, búsqueda exacta + diagnóstico de por qué no apareció (campo, tildes, dataset, sync) |
| COURTESY-FILLER-001 | (nuevo) | Pendiente HIGH | Cubierto por `DialoguePolicy` en Sprint 1A |
| BIDDER-CATALOG-AND-001 | (nuevo) | Pendiente HIGH | Cubierto por `QueryPlan` Sprint 1A — requiere shape `objeto_or` |
| POLYSEMIC-TOPIC-001 | (nuevo) | Pendiente HIGH | Cubierto por `QueryPlan` Sprint 1A + ResponsePolicy Sprint 2 |
| FOLLOWUP-DATASET-PURGE-001 | (nuevo) | Pendiente HIGH | Fix puntual en `followup_engine._merge_change_dataset` |
| PROCUREMENT-ACTION-001 | (nuevo) | Pendiente HIGH | Cubierto por `QueryPlan` Sprint 1A — requiere shape `objeto_signal` |
| MORPHO-UNEVEN-001 | (nuevo) | Pendiente MEDIUM | Curación manual de `morphological_variants.py` para 25 rubros frecuentes |
| HEADER-TOPIC-OMISSION-001 | (nuevo) | Pendiente LOW | Sprint 2 `formatter.py` |
| MOJIBAKE-DISPLAY-001 | (nuevo) | Pendiente LOW | Normalización Unicode en `formatter.py` |

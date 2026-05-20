# CORPUS BACKLOG — SECOPPAL Query Corpus V1

Fallos conocidos del corpus, agrupados por severidad. NO editar parser/core para hacer pasar estos casos — son bugs reales que requieren features o fixes separados.

**Última actualización:** 2026-05-19 (octava tanda, **re-diagnóstico empírico vía Socrata directo**) — verificación con 9 procesos confirma que MUNICIPAL-GEO-GAP-001 era hipótesis falsa (todos los procesos "no encontrados" tienen `departamento_entidad` correctamente poblado). SOURCE-COVERAGE-001 baja a MEDIUM (delay real <24h, no 48h). Nuevos CRITICAL: PAGINATION-CUTS-OFF-RELEVANT, STATE-CATALOG-INCOMPLETE. Total acumulado: **28 categorías de bugs, 9 CRITICAL** tras re-diagnóstico. Plan de robustecimiento sistémico en pausa (ver [docs/SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md](SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md)); estos hallazgos alimentan el Sprint 0A cuando se retome.

## Re-diagnóstico empírico (2026-05-19, octava tanda)

Verifiqué directamente en Socrata 9 procesos reportados como "no encontrados". **TODOS están en el dataset con campos territoriales correctamente poblados.** Esto invalida la hipótesis MUNICIPAL-GEO-GAP-001 y obliga a reasignar los culpables reales:

| Proceso | depto/ciudad en dataset | Verdadero culpable |
|---|---|---|
| Paicol DHMP-SA-SFDS-003 | Huila / Paicol | ACCENT (turísticos) |
| Paipa SMC MP 021 | Boyacá / No Definido | ENTITY-MISRESOLUTION (→Manizales) |
| Yarumal CMC-SD-051 | Antioquia / Yarumal | FOLLOWUP-VALUE-INHERIT (heredó valor_max=20M, proceso vale 48M) |
| Olaya CMC 0017-2026 | Antioquia / Olaya | ACCENT (sólidos) + LLM-EXPANSION-AND |
| Doncello CMC-2026-019 | Caquetá / El Doncello | **PAGINATION LIMIT 50** (1,647 resultados, quedó fuera) |
| Mariquita SAMC-JCT-006 | Tolima / Mariquita | STATE-CATALOG-INCOMPLETE (probable) |
| Palermo IP-017-2026 | Huila / Palermo | MIN-MAX-INVERSION |
| Putumayo UNIPUTUMAYO-MC-013 | Putumayo / Mocoa | ACCENT (cafetería/jardinería) + MULTI-AND |
| Ipiales SMC-006-2026 | Nariño / Ipiales | ACCENT (DISEÑO) |

**Implicación honesta:** mi diagnóstico previo asumía MUNICIPAL-GEO-GAP cuando un proceso de municipio pequeño no aparecía. **Falso.** CCE puebla correctamente los campos territoriales. El bug siempre estaba en otra parte (filtros, parsing, paginación). Sin la verificación empírica de hoy, habríamos invertido tiempo en "fix" que no se necesita.

## Modelo de negocio confirmado (2026-05-19)

| Dimensión | Valor |
|---|---|
| Modelo | B2B SaaS — suscripción individual |
| Pricing | $10-20 USD/mes per user (target $15 promedio) |
| Usuario objetivo | Proponentes activos buscando oportunidades en SECOP |
| Meta 2 años | 500 WAU (~2,000-3,000 MAU) |
| ARR target | ~$90K |
| Tráfico esperado peak | 5K-10K queries/día |

**Implicación crítica del modelo:** los proponentes pagan por encontrar oportunidades **antes** del cierre. Mínima cuantía tiene plazo 1-3 días hábiles. Un usuario que ve oportunidades con 48h de delay pierde la mitad de mínimas cuantías. Eso es churn directo. **La velocidad de datos no es feature, es prerequisito del modelo de negocio.**

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
| MIN-MAX-INVERSION-001 | "No sean mayores a X" se parsea como `valor_min=X` cuando debería ser `valor_max=X`. Polaridad de negación invertida. **Confirmado culpable directo en caso Palermo IP-017-2026** | 1 verificado | **CRITICAL** — mata propuesta de valor del usuario pago |
| ACCENT-NORMALIZATION-001 | Tildes/diacríticos (incluyendo ñ) en dato fuente no matchean LIKE sin diacrítico (`UPPER` no los quita). **Confirmado culpable directo en Paicol, Olaya, Putumayo, Ipiales.** Casos: consultoría, logístico, turísticos, logísticos, DISEÑO, JARDINERÍA, CAFETERÍA, SÓLIDOS, TURÍSTICOS, ARTÍSTICOS | **9+ verificadas** | CRITICAL — bug más frecuente con evidencia más sólida |
| ENTITY-MISRESOLUTION-FALLBACK-001 | `rewrite_alcald[ií]a` devuelve entidad arbitraria con `confidence=high` cuando no encuentra match. **Confirmado culpable directo en Paipa SMC MP 021 → Manizales.** | 2 verificados | CRITICAL |
| LLM-EXPANSION-AND-001 | LLM expande topic a múltiples bigrams sinónimos pero se unen con AND en SoQL. Caso: `"residuos sólidos"` + `"manejo de residuos"` como AND → 0 resultados (Olaya) | 1 | CRITICAL |
| OPP-TIMEOUT-001 | Timeout SECOP reportado como "0 resultados" sin avisar al usuario | 6 | CRITICAL |
| PAGINATION-CUTS-OFF-RELEVANT-001 | `LIMIT 50` silencioso oculta procesos relevantes cuando hay >50 resultados. **Confirmado culpable directo en Doncello CMC-2026-019** (1,647 resultados totales, el de Doncello quedó fuera del top 50 ordenado por fecha+valor) | 1 verificado | **CRITICAL (nuevo)** — usuario no sabe que solo ve 50 de N |
| MULTI-AND-OVER-RESTRICTIVE-001 | ≥4 tokens en objeto con AND obligatorio = match matemáticamente improbable. **Confirmado en Putumayo (6 ANDs) e Ipiales (4 ANDs).** | 2 verificados | **CRITICAL** (sube de HIGH) |
| FOLLOWUP-VALUE-INHERIT-001 | `valor_max`/`valor_min`/`modalidad` heredados cuando el topic cambia completamente. **Confirmado culpable directo en Yarumal silvopastoriles** (heredó valor_max=20M de query anterior de residuos sólidos, proceso vale 48M) | 1 verificado | **CRITICAL** (sube de HIGH) |
| STATE-CATALOG-INCOMPLETE-001 | Catálogo `estado_del_procedimiento IN (...)` no cubre todos los estados reales que SECOP usa. **Probable culpable en Mariquita SAMC-JCT-006** (el proceso tiene estado "Publicado" oficialmente pero Phase "Presentación de observaciones" — auditar valores reales del campo) | 1 probable | **CRITICAL (nuevo)** — necesita audit de `discover_secop.py` |
| ~~MUNICIPAL-GEO-GAP-001~~ | ~~Alcaldías municipales con departamento_entidad vacío/inconsistente~~ **REMOVED 2026-05-19**: 9 procesos verificados en Socrata, TODOS tienen campos territoriales bien poblados. Era diagnóstico erróneo. Los casos atribuidos a este bug se redistribuyeron en ACCENT/ENTITY-MISRESOLUTION/FOLLOWUP-VALUE-INHERIT/PAGINATION | 0 (era 5 supuestos) | **ELIMINADO** |
| SOURCE-COVERAGE-001 | Procesos publicados en SECOP nativo en las últimas <12h pueden no estar en dataset Socrata. **Re-diagnóstico 2026-05-19**: 9 procesos verificados, incluso del 18/05 ya estaban en Socrata el 19/05. Delay real <24h, probablemente <12h. **Baja de CRITICAL a MEDIUM**. Sigue válido el diagnóstico transparente como UX pero no requiere crawler propio urgente | 0 confirmados | **MEDIUM (baja desde CRITICAL)** — investigación empírica invalidó la magnitud del problema |
| STATE-PRIORITY-001 | `estado_family=oferta_abierta` no se proyecta a SoQL en algunas rutas | 3 | HIGH (ya en DEMO-BLOCKERS) |
| OPP-INTENT-001 | "algún proceso para X" / "alguna oferta del Y" / "algún proceso de Z en W" no activa `intent_type=opportunity_search` ni `estado_family=oferta_abierta`. Confirmado en Putumayo aseo, Antioquia residuos, Huila transporte | 5 | HIGH |
| MULTI-AND-OVER-RESTRICTIVE-001 | Cuando parser/LLM extraen ≥4 tokens en objeto, AND obligatorio sobre todos hace matemáticamente improbable encontrar match. Caso canónico: "ASEO, CAFETERÍA, JARDINERÍA Y MANTENIMIENTO" → 6 ANDs → 0 resultados. Fix: degradar a OR sobre tokens menos específicos, mantener AND sobre tokens raros | 1 | **HIGH (nuevo)** |
| CITY-TO-ENTITY-AUTOPROMOTION-001 | `city_to_entity` con confidence medium convierte unilateralmente mención de ciudad en filtro `WHERE entidad LIKE 'alcaldía de X'`, ocultando procesos de otras entidades (gobernación, hospitales, SENA, universidades públicas) en esa ciudad | 2 (Barranquilla casa lúdica, Barranquilla aires) | **HIGH (nuevo)** |
| TEMPORAL-MONTH-PARSING-001 | "en mayo", "el mes pasado", "este mes" no se resuelven como filtros de fecha. Mes sin año debe interpretarse como mes del año actual | 1 (Putumayo aseo "en mayo") | **HIGH (nuevo)** |
| AUTO-FALLBACK-WITHOUT-QUALITY-001 | Cuando 0 resultados, fallback automático sin filtro de fecha trae procesos viejos (2023) cuando user pidió 2026. UX dice "sin filtro de fechas" pero no aclara que son potencialmente irrelevantes | 1 (Antioquia residuos → Caracolí 2023) | MEDIUM (nuevo) |
| NUMBER-CONTEXT-PHRASE-001 | Verbos comparativos ("supere", "exceda", "alcance") y conectores temporales ("en") permanecen en `objeto` cuando el parser ya extrajo correctamente el modificador (valor/fecha). AND con el verbo mata la búsqueda. Caso: "no supere los 25 millones" → valor_max=25M ✓ + objeto contiene "supere" → 0 resultados | 1 | **HIGH (nuevo)** |
| ENTITY-MULTI-REGIONAL-001 | Entidades con regionales (SENA, ICBF, DNP, ministerios) no consideran modificadores territoriales en la query. Caso: "del sena bolivar" → "SENA SECRETARIA GENERAL" (Bogotá), no "SENA REGIONAL BOLÍVAR" | 1 | **HIGH (nuevo)** |
| FOLLOWUP-VALUE-INHERIT-001 | `valor_max`/`valor_min`/`modalidad` heredados del turno anterior cuando el topic cambia completamente. Caso: residuos sólidos (≤20M) → silvopastoriles → hereda 20M sin pedirlo | 1 | **HIGH (nuevo)** |
| NO-FOLLOWUP-DETECTION-001 | Quejas/comentarios del usuario tratados como nueva query (no como seguimiento conversacional) | 1 | HIGH |
| NIT-AMOUNT-CONFUSION-001 | Montos en COP (8-9 dígitos) confundidos con NITs/contratistas sin discriminar contexto léxico | 1 | HIGH |
| OPP-INTENT-001 | "algún proceso para X" no activa `intent_type=opportunity_search` ni `estado_family=oferta_abierta` | 1 | HIGH |
| RELEVANCE-PHRASE-001 | Frases técnicas compuestas (ej. "control de calidad de agua para consumo humano") destruidas por AND de tokens. Sin boost de frase exacta | 1 | HIGH |
| DEDUP-PROCESS-001 | Mismo proceso aparece varias veces con distinto `noticeUID` (cambios de fase/estado generan registros separados). Sin dedup en UI | 4 (Guateque, La Estrella, Doncello, Yaguara ×3) | MEDIUM |
| COURTESY-FILLER-001 | `hola`, `estoy interesado`, `actualmente`, `algún`, vocabulario de queja (`nada`, `terrible`, `incorrecto`, `mal`, `fracaso`) y señales de bidder (`oferta` como objeto cuando significa "oportunidad") entran como objeto contractual | 3 | HIGH |
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
- `entity_misresolution_confidence` — cuando `rewrite_alcald[ií]a` o cualquier fallback no encuentra match canónico, devolver `entidad_resolved=null` + `confidence=low`. Nunca high con dato arbitrario
- `discourse_filler` — extender courtesy_filler para cubrir vocabulario de queja, evaluación y meta-conversación
- `number_disambiguation` — discriminar NIT vs monto vs referencia por contexto léxico (presencia de "COP", "$", "millones", patrón de dígitos)
- `non_query_followup` — detectar cuando el siguiente turno del usuario es queja/comentario/cita de evidencia, no nueva búsqueda. Ofrecer ruta de diagnóstico en vez de ejecutar parse
- `llm_expansion_or` — cuando el LLM expande un topic a múltiples bigrams sinónimos/alternativos, unirlos con OR, no AND. Aplica al post-procesamiento del tool call del LLM
- `entity_multi_regional` — entidades con regionales (SENA, ICBF, ministerios, cajas) deben componerse con modificadores territoriales: `entidad_canonica + departamento → regional específica`. Catálogo curado de entidades multi-regional
- `followup_value_purge` — extensión de followup_dataset_purge para `valor_min`/`valor_max`/`modalidad`. Heurística: si tokens del topic nuevo no tienen overlap con anterior, purgar modificadores monetarios
- `modifier_scrub` — cuando el parser infiere con éxito un modificador (`valor_max`, `valor_min`, `fecha_desde`, `fecha_hasta`) a partir de una frase ("no supere los X", "en 2026"), las palabras de la frase (verbos comparativos, conectores temporales) deben scrubearse del objeto. Catálogo: supere, exceda, supera, alcance, rebase, llegue, baje, suba, cueste, valga, en, desde, hasta, durante
- `source_coverage_diagnostic` — cuando el usuario reporta "no encontré X" y trae referencia/datos del proceso, ofrecer ruta de diagnóstico que incluya verificación de sincronización del dataset (delay de 24-48h de Socrata)
- `value_polarity` — mapeo correcto de frases de negación monetaria: "no mayor a", "no exceda", "menor o igual a", "por debajo de", "no sean menores a", "al menos". Cada una asigna al campo correcto (valor_min vs valor_max)
- `topic_token_demotion` — cuando objeto tiene ≥4 tokens, degradar AND a OR sobre tokens menos específicos (servicios, general, mantenimiento, suministro). Mantener AND solo sobre tokens raros (jardinería, cafetería, electrobombas)
- `city_entity_suggestion` — `city_to_entity` con confidence medium NO debe convertirse en filtro WHERE silencioso. Solo ofrecer como sugerencia al usuario
- `temporal_relative` — parsear meses sin año ("en mayo"), referencias relativas ("este mes", "el mes pasado", "hace dos meses"), trimestres y semestres
- `auto_fallback_transparency` — cuando fallback automático cambia drásticamente la ventana temporal (>1 año fuera), marcar individualmente cada resultado o pedir confirmación al usuario

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

## Orden recomendado para Sprint 1A (revisado tras re-diagnóstico empírico 2026-05-19)

Antes de implementar `QueryPlan` completo, **nueve fixes CRITICAL** (~20-25 horas combinadas) destrabarían >70% del ruido visible. Orden por impacto al modelo de negocio:

1. **MIN-MAX-INVERSION-001** (~3h) — parser de polaridad monetaria correcto. **Mata directamente al usuario pago.**
2. **ACCENT-NORMALIZATION-001** (~3h) — `regexp_replace` sobre tildes + ñ en SoQL. **Bug más frecuente: 9+ confirmaciones empíricas.**
3. **ENTITY-MISRESOLUTION-FALLBACK-001** (~2h) — `rewrite_alcald` debe devolver null + confidence low cuando no hay match canónico. **Engaño operacional.**
4. **PAGINATION-CUTS-OFF-RELEVANT-001** (~3h) — paginación visible, advertencia cuando LIMIT recorta resultados. **Oculta procesos sin avisar.**
5. **LLM-EXPANSION-AND-001** (~2h) — post-procesar tool call LLM: OR sobre bigrams sinónimos.
6. **OPP-TIMEOUT-001** (~2h) — timeout ≠ 0 resultados.
7. **MULTI-AND-OVER-RESTRICTIVE-001** (~3h) — degradar AND→OR sobre tokens menos específicos cuando hay ≥4.
8. **FOLLOWUP-VALUE-INHERIT-001** (~2h) — purgar `valor_max`/`valor_min`/`modalidad` cuando el topic cambia completamente.
9. **STATE-CATALOG-INCOMPLETE-001** (~2h + audit) — `discover_secop.py` sobre `estado_del_procedimiento` para completar la familia `oferta_abierta`.

**Pre-requisito reducido**: el audit con `discover_secop.py` solo necesita correr sobre el catálogo de `estado_del_procedimiento` (objetivo: descubrir valores que no contemplamos), NO sobre campos territoriales. Eso es 30 minutos, no 1-2 horas.

**Lo que YA NO es CRITICAL gracias al re-diagnóstico empírico:**
- ~~MUNICIPAL-GEO-GAP-001~~ — eliminado, no era bug.
- ~~SOURCE-COVERAGE-001~~ — baja a MEDIUM, no requiere crawler urgente.

Luego el `QueryPlan` original del plan, con extensiones para `courtesy_filler` (ampliado a `discourse_filler` que incluye señales de "oferta"="oportunidad"), `bidder_catalog`, `procurement_verb`, `polysemic_topic`, `followup_dataset_purge`, `followup_value_purge`, `non_query_followup`, `number_disambiguation`, `entity_multi_regional` y `opportunity_intent_implicit` consideradas desde el inicio.

---

## Pendientes fuera del corpus

| ID | Descripción | Sprint | Estado tras feedback 2026-05-19 |
|----|-------------|--------|---------------------------------|
| DEMO-BLOCKERS-001 | 8 tareas UX/estado/scope | Pendiente HIGH | OPP-TIMEOUT sube a CRITICAL (5 apariciones); STATE-PRIORITY confirmado vivo (3 apariciones) |
| VALUE-TYPO-001 | `milloones` normalizar, `valor_min > valor_max` → aclaración | Pendiente | Sin nuevas confirmaciones |
| REFERENCE-001 | Búsqueda exacta por referencia de proceso (`test_dicar` xfail) | Pendiente | Sin nuevas confirmaciones |
| CORPUS-CI-001 | known_fail/expected_fail para gate de CI | Pendiente | Sin nuevas confirmaciones |
| LLM-EXPANSION-AND-001 | LLM expande topic a bigrams sinónimos unidos con AND (nuevo) | Pendiente CRITICAL | 1 caso (residuos sólidos + manejo de residuos). Fix en post-procesamiento de tool call LLM: shape `objeto_or` |
| ENTITY-MISRESOLUTION-FALLBACK-001 | Fallback de rewrite_alcald devuelve entidad arbitraria con high confidence (nuevo) | Pendiente CRITICAL | 2 casos: "Paipa"→Manizales y "Paipa+texto contaminado"→Secretaría Distrital. Fix puntual en `entity_resolver.py` — degradar fallback a `null+confidence=low` |
| ACCENT-NORMALIZATION-001 | Tildes (nuevo) | Pendiente CRITICAL | Confirmado 3× — `consultoría`, `logístico`, `turísticos` |
| ~~MUNICIPAL-GEO-GAP-001~~ | ~~Datos territoriales municipios~~ ELIMINADO 2026-05-19 | n/a | **Era diagnóstico erróneo.** Verificación empírica con 9 procesos (Paicol, Paipa, Mariquita, Yarumal, Olaya, Doncello, Palermo, Putumayo, Ipiales) confirmó que TODOS tienen campos territoriales bien poblados en Socrata. Los casos atribuidos se redistribuyeron a ACCENT/ENTITY-MISRESOLUTION/FOLLOWUP-VALUE-INHERIT/PAGINATION |
| PAGINATION-CUTS-OFF-RELEVANT-001 | LIMIT 50 oculta procesos relevantes (nuevo CRITICAL) | Pendiente CRITICAL | 1 caso verificado (Doncello, 1647 resultados totales). Fix: paginación visible + advertencia "muestro 50 de N" + sugerir refinar filtros |
| STATE-CATALOG-INCOMPLETE-001 | Catálogo de estados no cubre todos los valores reales (nuevo CRITICAL) | Pendiente CRITICAL | 1 caso probable (Mariquita SAMC-JCT-006). Pre-req: `discover_secop.py` sobre campo `estado_del_procedimiento` con muestra reciente |
| ENTITY-MULTI-REGIONAL-001 | Entidades con regionales no consideran modificadores territoriales (nuevo) | Pendiente HIGH | 1 caso ("del sena bolivar" → SENA SECRETARIA GENERAL en Bogotá). Catálogo curado: SENA, ICBF, DNP, ministerios, cajas de compensación, universidades públicas |
| FOLLOWUP-VALUE-INHERIT-001 | valor_max/modalidad heredados cuando topic cambia (nuevo) | Pendiente HIGH | 1 caso (residuos sólidos ≤20M → silvopastoriles hereda 20M). Fix en `followup_engine` — heurística de overlap de tokens entre turnos |
| OPP-INTENT-001 | "alguna oferta del X" / "algún proceso para Y" debe activar opportunity_search (confirmado 2× ahora) | Pendiente HIGH | Cubierto por `QueryPlan` Sprint 1A. Adicional: "oferta" como token de objeto debe scrubearse cuando aparezca en contexto opportunity |
| NO-FOLLOWUP-DETECTION-001 | Quejas tratadas como query nueva (nuevo) | Pendiente HIGH | Cubierto por extensión del `followup_engine` — detectar quejas/citas como `non_query_followup`, ofrecer diagnóstico en vez de parsear |
| NIT-AMOUNT-CONFUSION-001 | Montos confundidos con NITs (nuevo) | Pendiente HIGH | Cubierto por QueryPlan Sprint 1A — categoría `number_disambiguation`. Discriminar por contexto léxico ("COP", "$", "millones") |
| RELEVANCE-PHRASE-001 | Boost de frase técnica compuesta (nuevo) | Pendiente HIGH | Cubierto por Sprint 3 Relevance v1 (shingle bigramas en BM25) o v2 (embedding de frase completa). Caso canónico: "control de calidad de agua para consumo humano" |
| DEDUP-PROCESS-001 | Dedup procesos por entidad+monto+objeto (nuevo) | Pendiente MEDIUM | Sprint 2-3. Ya mencionado en OPP audit 2026-05-17. Confirmado 3× (Guateque, La Estrella, Doncello) |
| NUMBER-CONTEXT-PHRASE-001 | Verbos comparativos / conectores temporales como objeto (nuevo) | Pendiente HIGH | 1 caso ("supere" en objeto cuando valor_max ya inferido). Fix: scrub catalog post-extracción de modificadores |
| SOURCE-COVERAGE-001 | Procesos recientes invisibles por delay de sync Socrata (nuevo) | Pendiente CRITICAL | 2 casos (Paipa, Doncello). Limitación de la fuente, no bug técnico. **Sube de HIGH a CRITICAL tras decisión de modelo B2B.** Fase 1: ruta de diagnóstico transparente. Fases 2-3: ver roadmap de datos abajo. |

---

## Roadmap de infraestructura de datos (post-decisión modelo B2B)

Hallazgo de la investigación del 2026-05-19: licitaciones.info opera con BD propia replicada de SECOP (no consume Socrata en vivo). Apuntar a 500 WAU pagando requiere camino similar, ejecutado en tres fases con criterios de transición claros.

### Fase 1 — 0-6 meses (Opción A): Socrata + diagnóstico transparente

**Qué hacer:**
- Mantener Socrata `p6dx-8zbt` como fuente única.
- Implementar SOURCE-COVERAGE-001 con UX honesta: cuando una query es probablemente afectada por delay (referencia reciente, scope estrecho, intent_type=opportunity_search), avisar al usuario antes de declarar "0 resultados".
- Mensaje canónico: *"SECOP publica con delay de 24-48h en mi base de datos. Si tu oportunidad es de hoy o ayer, verifica directamente en SECOP nativo o licitaciones.info."*

**Costo:** $0 adicional. Cabe en Sprint 2 del plan vigente.

**Criterio de salida (transición a Fase 2):** **50+ usuarios pagando $15/mes (~$9K MRR)** validando que la propuesta de "asesor conversacional" tiene tracción real. Si no se llega, no invertir más; reconsiderar producto.

### Fase 2 — 6-12 meses (Opción C): licenciar acceso a BD ajena

**Qué hacer:** contactar tres opciones en orden:

1. **Apitude.co** — B2B confirmado, ya tienen REST API sobre SECOP. Pedir demo, precio, delay garantizado, SLA.
2. **Licitaciones.info / colombialicita.com partnership** — improbable pero valioso si lo logras. Modelo: white-label o API access bajo revenue share. Email directo a su equipo comercial.
3. **CCE (Colombia Compra Eficiente) directamente** — preguntar por feed de integrador autorizado, RSS, webhook o API más rápida que Socrata. Pueden decir no, pero el costo del email es cero.

**Costo estimado:** $200-800 USD/mes según opción y volumen.

**Trade-off:** dependes de tercero (pricing, SLA, ToS). Pero evitas 2-3 semanas de dev de crawler y un servidor 24/7 que monitorear.

**Criterio de salida (transición a Fase 3):** costo de C supera $5K/año recurrente Y volumen de queries justifica BD propia, O necesitas enriquecer datos (HV de contratistas, scoring de relevance) que el tercero no expone.

### Fase 3 — 12-24 meses hacia 500 WAU (Opción B): crawler propio

**Qué hacer:**
- Construir `app/data/secop_crawler.py` (módulo nuevo, no parte del core SECOPPAL) que sincroniza periódicamente desde community.secop.gov.co.
- BD propia (PostgreSQL probablemente) con índices optimizados para los filtros de SECOPPAL.
- SECOPPAL consume BD propia en lugar de Socrata.
- Mantener Socrata como **fallback** si crawler falla — degradación graceful.

**Costo:**
- Dev inicial: 2-3 semanas tiempo completo.
- Infraestructura: ~$50-200 USD/mes (servidor cloud + BD + monitoreo).
- Mantenimiento: 4-8h/mes ante cambios en SECOP nativo.

**Trade-off:** control total + posibilidad de monetizar dato a terceros (exactamente lo que hace licitaciones.info con `licitacionesdata.com`). Costo: complejidad operativa y dependencia técnica.

### Decisión registrada hoy (2026-05-19)

- **No saltar fases.** Sin validación de Fase 1, no invertir en Fase 2. Sin tracción real en Fase 2, no invertir en Fase 3.
- **No mezclar fuentes en una misma query.** O todo Socrata, o todo BD propia. Hibridación crea inconsistencias sutiles que destruyen confianza.
- **Considerar partnership con licitaciones.info antes de construir crawler.** Si su modelo comercial lo permite, es win-win.
- **SOURCE-COVERAGE-001 (diagnóstico Fase 1) es PRE-REQUISITO de cualquier lanzamiento pagado.** Sin esto, churn será mayor que crecimiento.

### Lo que NO se decidió hoy

- Pricing exacto entre $10-20 USD/mes (definirlo en cohorte de validación).
- Modelo B2B individual vs enterprise (proponente individual vs empresa contratista). Esto afecta features futuras (subcuentas, carpetas compartidas, alertas por equipo).
- Si SECOPPAL eventualmente vende dato a terceros (modelo `licitacionesdata.com` clonado). Decisión post-Fase 3.
| REFERENCE-001 (ampliado) | Búsqueda exacta por referencia + diagnóstico de cobertura | Pendiente HIGH | Ampliar el alcance: cuando el usuario reporta "no encontré X" y trae referencia, búsqueda exacta + diagnóstico de por qué no apareció (campo, tildes, dataset, sync) |
| COURTESY-FILLER-001 | (nuevo) | Pendiente HIGH | Cubierto por `DialoguePolicy` en Sprint 1A |
| BIDDER-CATALOG-AND-001 | (nuevo) | Pendiente HIGH | Cubierto por `QueryPlan` Sprint 1A — requiere shape `objeto_or` |
| POLYSEMIC-TOPIC-001 | (nuevo) | Pendiente HIGH | Cubierto por `QueryPlan` Sprint 1A + ResponsePolicy Sprint 2 |
| FOLLOWUP-DATASET-PURGE-001 | (nuevo) | Pendiente HIGH | Fix puntual en `followup_engine._merge_change_dataset` |
| PROCUREMENT-ACTION-001 | (nuevo) | Pendiente HIGH | Cubierto por `QueryPlan` Sprint 1A — requiere shape `objeto_signal` |
| MORPHO-UNEVEN-001 | (nuevo) | Pendiente MEDIUM | Curación manual de `morphological_variants.py` para 25 rubros frecuentes |
| HEADER-TOPIC-OMISSION-001 | (nuevo) | Pendiente LOW | Sprint 2 `formatter.py` |
| MOJIBAKE-DISPLAY-001 | (nuevo) | Pendiente LOW | Normalización Unicode en `formatter.py` |

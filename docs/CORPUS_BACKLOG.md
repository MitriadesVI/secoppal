# CORPUS BACKLOG — SECOPPAL Query Corpus V1

Fallos conocidos del corpus, agrupados por severidad. NO editar parser/core para hacer pasar estos casos — son bugs reales que requieren features o fixes separados.

**Última actualización:** 2026-05-25 (Dogfooding UX fixes ADR-017 + CASE WHEN BadRequest fix)

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
| MIN-MAX-INVERSION-001 | "No sean mayores a X" se parsea como `valor_min=X` cuando debería ser `valor_max=X`. Polaridad de negación invertida. **Confirmado culpable directo en caso Palermo IP-017-2026** | 1 verificado | **CLOSED Sprint 4 (A4)** — 3 patrones nuevos (menor o igual, mayor o igual, a lo sumo). Commit `77dbb9c` |
| ACCENT-NORMALIZATION-001 | Tildes/diacríticos (incluyendo ñ) en dato fuente no matchean LIKE sin diacrítico (`UPPER` no los quita). **Confirmado culpable directo en Paicol, Olaya, Putumayo, Ipiales.** Casos: consultoría, logístico, turísticos, logísticos, DISEÑO, JARDINERÍA, CAFETERÍA, SÓLIDOS, TURÍSTICOS, ARTÍSTICOS | **9+ verificadas** | **CLOSED Sprint 4 (A1)** — unaccent() confirmado en Socrata (PostgreSQL backend), NO regexp_replace. strip_accents() helper. Commit `61f5dfb` |
| ENTITY-MISRESOLUTION-FALLBACK-001 | `rewrite_alcald[ií]a` Y `rewrite_gobernaci[oó]n` devuelven entidad arbitraria con `confidence=high` cuando no encuentran match. **Confirmados:** Paipa→Manizales, Medellín→INDER, Bolívar→ICBF. Patrón sistémico en ambas funciones de reescritura | **3 verificados** (2 alcaldía, 1 gobernación) | **CLOSED Sprint 1A (B3)** — `_unresolved_rewrite()` devuelve null+low+clarification_needed. Commit `f55eced`. **RE-OPENED 2026-05-24:** fix solo cubría `resolve_entidad()`, no `scan_entity()`. Caso confirmado en prod: "alcaldía local de teusaquillo" → Manizales. **RE-CLOSED 2026-05-24:** `_is_unqualified_generic()` en `scan_entity()`. Commit `da94a96`. Suite 836/0. |
| LLM-EXPANSION-AND-001 | LLM expande topic a múltiples bigrams sinónimos pero se unen con AND en SoQL. Caso: `"residuos sólidos"` + `"manejo de residuos"` como AND → 0 resultados (Olaya) | 1 | **CLOSED Sprint 1A (B5)** — `_promote_synonym_expansion()` mueve a `objeto_or` (OR). Commit `20d2ffd`. Auditoría 2026-05-23: verificado en código + tests (3/3) |
| OPP-TIMEOUT-001 | Timeout SECOP reportado como "0 resultados" sin avisar al usuario | 6 | **CLOSED Sprint 3 fase 1 (B5)** — timeout default 15s, degradación tras 1er fallo, observer partial=True |
| GOB-DEPTO-RESOLUTION-001 | "gobernación de Boyacá" resuelve a UPTC en lugar de GOBERNACIÓN DE BOYACÁ por colisión de alias exacto en gazetteer | 1 | **CLOSED Sprint 3 fase 1 (B6)** — skip alias exacto si strict_rewrite falla + fuzzy top-3 |
| RACE-TIMEOUT-MUTATION-001 | SecopClient.aggregate() mutaba timeout global del cliente subyacente, causando race condition en ThreadPoolExecutor del observer que afectaba queries principales con timeout=4s | 1 (causa raíz) | **CLOSED Sprint 3 fase 1 (B5)** — SecopClient.with_timeout() crea instancia nueva sin estado mutable compartido |
| COUNT-QUERY-LATENCY-001 | count(*) sobre datasets de millones con WHERE compuesto tarda 10-15s, bloqueando respuesta al usuario | 2 (causa raíz smoke) | **CLOSED Sprint 3 fase 1** — query-first, count-lazy: skip count si <limit rows; si ==limit y count falla, mostrar "X+" resultados |
| PAGINATION-CUTS-OFF-RELEVANT-001 | `LIMIT 50` silencioso oculta procesos relevantes cuando hay >50 resultados. **Confirmado culpable directo en Doncello CMC-2026-019** (1,647 resultados totales, el de Doncello quedó fuera del top 50 ordenado por fecha+valor) | 1 verificado | **CLOSED Sprint 1A (B4)** — header "Encontre 1,647 resultados. Te muestro los 50; hay 1,597 mas" + `pagination_more` follow-up. Commit `ee28365`. Auditoría 2026-05-23: verificado en código + tests (8/8) |
| MULTI-AND-OVER-RESTRICTIVE-001 | ≥4 tokens en objeto con AND obligatorio = match matemáticamente improbable. **Confirmado en Putumayo (6 ANDs) e Ipiales (4 ANDs).** | 2 verificados | **CLOSED Sprint 4 (A2) / REFINED Sprint 6 Fase 5** — Sprint 4 expandió COMMON_TOKENS (~50) pero no cubría el caso de 4+ términos TODOS raros (ej. "control de calidad de agua para consumo humano"). Sprint 6 Fase 5 agregó OR merge cuando rare_terms ≥3. Commit `dd0b2bf`. Validado contra API real (0→50 results). ADR-014. |
| FOLLOWUP-VALUE-INHERIT-001 | `valor_max`/`valor_min`/`modalidad` heredados cuando el topic cambia completamente. **Confirmado culpable directo en Yarumal silvopastoriles** (heredó valor_max=20M de query anterior de residuos sólidos, proceso vale 48M) | 1 verificado | **CLOSED Sprint 1A (B8)** — `_purge_stale_inherited_params()` purga modificadores cuando no hay overlap de topic. Commit `b5f6a80`. Auditoría 2026-05-23: verificado en código + tests (8/8) |
| FOLLOWUP-ENTITY-PURGE-001 | Follow-up que cambia scope territorial (depto → ciudad) no purga la entidad heredada mal resuelta. Caso: gobernación Bolívar (mal resuelta a ICBF) → "magangue" → mantuvo ICBF + agregó "magangue" como objeto contractual | 1 verificado | **HIGH (nuevo)** — extension de FOLLOWUP-DATASET-PURGE-001 a campo entidad |
| FOLLOWUP-ENTITY-PURGE-001 | Follow-up que cambia scope territorial (depto → ciudad) no purga la entidad heredada mal resuelta. Caso: gobernación Bolívar (mal resuelta a ICBF) → "magangue" → mantuvo ICBF + agregó "magangue" como objeto contractual | 1 verificado | **CLOSED Sprint 1A (B4)** — `_purge_stale_inherited_params()` purga entidad al cambiar scope. Commit `b5f6a80`. Auditoría 2026-05-23: verificado |
| MORPHO-EXPANSION-INCONSISTENT-001 | Expansión morfológica de sinónimos se aplica en algunas rutas (heuristic_only) pero no en otras (heuristic_plus_llm). Caso adulto mayor: con LLM solo buscó "adulto mayor" sin expandir a "personas mayores", "centros de vida", etc. | 1 verificado | **CLOSED PRE-Fase 5** — Verificado: ambas rutas usan ROOT_TO_VARIANTS en SoQL builder (`_object_term_conditions` usa `ROOT_TO_VARIANTS.get(term, frozenset([term]))` independientemente del path de parseo). Tests en `test_morpho_expansion_consistency.py` (3 tests). Falso positivo: la expansión siempre fue consistente. Commit `29ac24b` |
| ENTITY-ECOSYSTEM-EXPANSION-001 | Alcaldías mayores y gobernaciones grandes (Bogotá, Medellín, Cali, Valle, Antioquia, etc.) tienen 30-60+ entidades dependientes. Modelo actual solo busca entidad central. Pregunta abierta: ¿strict, ecosistema, híbrido con sugerencia? | observación arquitectónica | **HIGH — APLAZADO A SPRINT 7+** con interfaz preparada en Sprint 1A |
| STATE-CATALOG-INCOMPLETE-001 | Catálogo `estado_del_procedimiento IN (...)` no cubre todos los estados reales que SECOP usa. **Probable culpable en Mariquita SAMC-JCT-006** (el proceso tiene estado "Publicado" oficialmente pero Phase "Presentación de observaciones" — auditar valores reales del campo) | 1 probable | **CLOSED Sprint 5** — +Aprobado/+En aprobación/+Borrador (246K registros). Smoke extendido 5/5 confirma. Commit `3761126` |
| SOURCE-COVERAGE-001 | Procesos publicados en SECOP nativo en las últimas <12h pueden no estar en dataset Socrata. Delay real <24h. | 0 confirmados | **MEDIUM — VIVO**. 0% implementado. UX honesta de delay pendiente. |
| REGION-MACRO-001 | Queries con regiones macro: "costa atlántica", "eje cafetero", "pacífico", "interior", "amazonía", "orinoquía". Sin soporte en parser ni SoQL. | 0 (feature) | **HIGH — POST-COHORTE (Sprint 7+)**. ~3-6h: catálogo curado + regex + SoQL IN clause + políticas de ambigüedad. Dependencia: auditar valores reales de departamento en SECOP. Extensible a modismos: ciudades capitales, intermedias, PDET, ZOMAC. |
| EXPORT-XLSX-001 | Usuarios piden exportar resultados a Excel. Streamlit no tiene export nativo; Telegram menos. | 0 (feature) | **MEDIUM — POST-COHORTE (Sprint 7+)**. ~3-4h: openpyxl, 2 sheets (Resultados + Búsqueda), Streamlit download_button, Telegram /excel command. Módulo nuevo aislado, sin riesgo de regresión. |
| MARKET-NODES-001 | Diccionario semántico de procurement: sinónimos de dominio que el buscador de texto no captura (adulto mayor → ancianos, tercera edad, centros día). Diferente de ROOT_TO_VARIANTS (morfología) y POLYSEMIC (ambigüedad). | 0 (feature) | **HIGH — POST-COHORTE (Sprint 7+)**. ~12-16h MVP: Opción B (OR group + BM25 re-rank), 7 nodes iniciales curados con discovery script sobre feedback real, transparencia obligatoria en respuesta. 20-25h versión completa con jerarquía broad categories. Valor estratégico: diferencial competitivo contra licitaciones.info. |
| RADAR-NODES-001 | Suscripciones a nodos semánticos para monitoreo diario de nuevos procesos. Basado en MARKET-NODES-001 pero para tracking, no búsqueda. | 0 (feature) | **HIGH — Sprint 8+ post-cohorte** (necesita modelo de pricing definido). Tier 1 MVP 15-20h: SQLite subscriptions, cron 5am, detección nuevos por noticeUID, Telegram digest + xlsx. Depende de MARKET-NODES-001, REGION-MACRO-001, ENTITY-ECOSYSTEM, EXPORT-XLSX-001. Tiers: free 3 suscripciones/weekly, paid $15/mes ilimitado/daily, premium $30/mes real-time/predictive. Valor estratégico: convierte de search tool a monitoring tool, LTV 3-5x. |
| CCE-TAXONOMY-001 | Ingestión de taxonomías oficiales CCE: UNSPSC, Acuerdos Marco TVEC, Pliegos Tipo, modalidades + marco legal. Hoy el sistema ignora códigos UNSPSC en SECOP y solo hace LIKE sobre texto. | 0 (feature) | **HIGH — Sprint 7-8**. ~2 semanas: UNSPSC ingestión + integración SoQL (1 sem) + Acuerdos Marco cruzados con resultados (1 sem). Complementa gazetteer/ROOT_TO_VARIANTS sin reemplazar. ROI alto, riesgo bajo (datos públicos oficiales). Resuelve POLYSEMIC con autoridad taxonómica. Habilita detector sobreprecio y queries por familia UNSPSC. |
| RAG-FT-STRATEGY-001 | Estrategia de inteligencia contractual: RAG sobre corpus CCE → fine-tune clasificador → fine-tune generador. Sin FT prematuro sin dataset de cohorte. | 0 (strategy) | **MEDIUM — Sprint 9+ / Trimestre 3-4 / Año 2**. RAG primero (Qdrant local, $0.01/query, ~1 día setup). FT clasificador cuando >1K ejemplos. FT generador solo si métricas: >50K queries/mes, <85% accuracy, >15% MRR en LLM. RAG da 80% del valor con 20% del esfuerzo. |
| SECOP1-COVERAGE-001 | SECOP I (plataforma legacy pre-2019) no está cubierta. Sus datos no están en Socrata/datos.gov.co. | 0 (idea) | **LOW — SIN PLAN**. Requiere fuente de datos distinta, schema diferente, pipeline de ingestión separado. Valor: cobertura histórica anterior a SECOP II. Riesgo: fuente incierta, formato legacy, sin API estándar. Solo registrado como idea — requiere estudio de factibilidad antes de cualquier estimación. |
| FOLLOWUP-VERB-OBJECT-001 | Verbos de refinamiento en follow-up ("filtra", "aplica", "ponelo", "limitalo", "acota") no están en STOPWORDS → se tratan como objeto contractual → LIKE '%filtra%' mata la query. **Confirmado:** "ahora filtra por entre 100-500 millones" devolvió 0 resultados. | 1 confirmado | **CLOSED** — ~25 verbos agregados a STOPWORDS. Commit `eeb9df4`. Suite 830/0. |
| HEAVY-QUERY-PROBE-001 | Queries amplias sin scope geográfico + ORDER BY valor + multi-OR por expansión morfológica → timeout Socrata. Caso: "contratos adulto mayor 2025 ≥500M" → 0 results pero realmente había volumen. Usuario ve trace crudo. | 1 verificado | **MEDIUM — PARCIAL pre-cohorte (Sprint 6.5)**. V1 mínima ~4h: (1) fix underscore en LIKE [30min], (2) count-first probe con is_heavy_query() [3h], (3) mensaje timeout con 2 opciones existentes [30min]. V2 completa post-cohorte cuando REGION-MACRO-001 y EXPORT-XLSX-001 estén listos: 4 opciones + job async + cursor Socrata. |
| VARIANT-EXPLOSION-001 | Expansión morfológica genera LIKEs redundantes por contención de substring. "mantenimiento" ya matchea "mantenimiento correctivo", "mantenimiento preventivo y correctivo", etc. → 12 LIKEs para cubrir lo que 2 LIKEs cubren. **Confirmado:** "contratos de mantenimiento 500-600M en Cundinamarca" → timeout autoinfligido. | 1 confirmado | **CLOSED 2026-05-24** — `dedup_like_variants()` con threshold MIN_VARIANT_LENGTH=5. Variantes <5 chars (PTAR, STAR, PAE) se conservan pero no suprimen largas. WHERE clause: 1500→268 chars. Commit `b1a7385`. Suite 838/0. |
| ALIAS-GENERIC-CLEANUP-001 | `aliases_db.json` contiene 16 aliases que son tokens genéricos puros o artículo+genérico (\"alcaldía\", \"municipio\", \"gobernación\", \"la alcaldía\", \"el ministerio\", \"la corporación\"). El code fix (`da94a96`, `_is_unqualified_generic`) los neutraliza en runtime, pero la data sigue sucia y dos casos edge (\"secretaría de educación\" → SECRETARÍA DE EDUCACIÓN DEL DISTRITO vía alias incompleto, \"la corporación autónoma\" → CORPONOR vía alias de 2 content words) no están cubiertos por el filtro actual. | 0 confirmados (higiene) | **LOW — HIGIENE POST-COHORTE**. ~30min: script que remueve 16 aliases peligrosos de aliases_db.json + rebuild gazetteer. No bloqueante porque el code fix ya protege. |
| ENTITY-COVERAGE-BOGOTA-LOCAL-001 | Las 20 alcaldías locales de Bogotá D.C. (Usaquén, Chapinero, Santa Fe, etc.) no estaban en el gazetteer porque el top-500 por volumen las excluye. Caso confirmado en prod: \"alcaldia local de teusaquillo\" → clarification porque no existía en aliases_db.json. | 1 confirmado | **CLOSED 2026-05-24** — 9 nuevas entradas + 11 existentes actualizadas. NITs verificados vía SECOP II API (18/20 comparten 899999061 del Distrito Capital). Script one-shot: `scripts/add_bogota_local_alcaldias.py`. +12 tests de regresión scan_exact+high. Commit `6365d71`. |
| STATE-PRIORITY-001 | `estado_family=oferta_abierta` no se proyecta a SoQL en algunas rutas | 3 | **CLOSED Sprint 2A-fix (B3)** |
| OPP-INTENT-001 | "algún proceso para X" / "alguna oferta del Y" / "algún proceso de Z en W" no activa intent_type. Confirmado en Putumayo aseo, Antioquia residuos, Huila transporte | 5 | **CLOSED Sprint 2A (P4)** |
| CITY-TO-ENTITY-AUTOPROMOTION-001 | `city_to_entity` convierte ciudad en entidad, ocultando procesos de otras entidades en esa ciudad | 2 (Barranquilla) | **CLOSED Sprint 2A (P9)** |
| TEMPORAL-MONTH-PARSING-001 | "en mayo", "el mes pasado", "este mes" no se resuelven como filtros de fecha. | 1 (Putumayo aseo "en mayo") | **CLOSED Sprint 2A (P5)** |
| AUTO-FALLBACK-WITHOUT-QUALITY-001 | Cuando 0 resultados, fallback automático sin filtro de fecha trae procesos viejos (2023) cuando user pidió 2026. | 1 (Antioquia residuos → Caracolí 2023) | **MEDIUM — PARCIAL**. degraded_hint existe pero no avisa año real de resultados. |
| NUMBER-CONTEXT-PHRASE-001 | Verbos comparativos ("supere", "exceda", "alcance") y conectores temporales ("en") permanecen en objeto. | 1 | **CLOSED Sprint 2A (P6)** |
| ENTITY-MULTI-REGIONAL-001 | Entidades con regionales (SENA, ICBF, DNP, ministerios) no consideran modificadores territoriales. Caso: "del sena bolivar" → "SENA SECRETARIA GENERAL". | 1 | **CLOSED Sprint 5** — regional_entities.json (SENA 32 deptos, ICBF 16). Smoke 3/5. Commit `afb1e17`. Smoke Fase 5 reveló que el cierre fue parcial: faltaba unaccent() en LIKE nombre_entidad (ver ENTITY-MULTI-REGIONAL-002). |
| ENTITY-MULTI-REGIONAL-002 | LIKE nombre_entidad sin unaccent() → tildes no matchean ("BOLÍVAR" vs "BOLIVAR"). Además entidad_resolved sintética ("SENA REGIONAL BOLÍVAR") no existe en SECOP. **Descubierto en smoke Fase 5 D-bucket** (SENA Bolivar y SENA Atlantico 0 results en 2.8s, no timeout). | 2 (SENA Bolivar, SENA Atlantico) | **CLOSED Sprint 6 Fase 5** — Fix: _accent_fold_like en todas las cláusulas entidad + entidad_like con nombre canónico corto ("SENA", "ICBF"). resolve_regional() ahora retorna tuple (regional, canonical_short). Smoke D-bucket 3/5→5/5. Commit `b39a138`. ADR-013. |
| NIT-AMOUNT-CONFUSION-001 | Montos en COP (8-9 dígitos) confundidos con NITs/contratistas sin discriminar contexto léxico | 1 | **CLOSED Sprint 2A** — `_extract_nit()` + test_monto_no_confundido_con_nit |
| RELEVANCE-PHRASE-001 | Frases técnicas compuestas (ej. "control de calidad de agua para consumo humano") destruidas por AND de tokens. Sin boost de frase exacta | 1 | **HIGH — VIVO**. 0% implementado. Prioridad #1 para cohorte según plan maestro. |
| DEDUP-PROCESS-001 | Mismo proceso aparece varias veces con distinto `noticeUID` (cambios de fase/estado generan registros separados). Sin dedup en UI | 4 (Guateque, La Estrella, Doncello, Yaguara ×3) | **MEDIUM — PARCIAL**. `_dedupe_results()` implementado pero sin test unitario. |
| COURTESY-FILLER-001 | Tokens de cortesía, queja, bidder y verbos interrogativos entraban como objeto contractual | 4 | **CLOSED Sprint 2A (P7+P3)** |
| BIDDER-CATALOG-AND-001 | `vendo X, Y y Z` tratado como AND obligatorio → 0 resultados | 1 (electrobombas) | **CLOSED Sprint 1A** — intent_vocabulary OR groups + _extract_object_terms "o" detection |
| POLYSEMIC-TOPIC-001 | `alojamiento` con 5 sentidos no desambiguados (hospedaje/hosting/logístico/albergue/militar) | 1 | **HIGH — VIVO**. 0% implementado. Requiere catálogo curado. |
| FOLLOWUP-DATASET-PURGE-001 | Follow-up que cambia dataset hereda `intent_type`/`estado_family` incompatibles | 1 | **CLOSED Sprint 1A (B6)** — `_merge_change_dataset` no hereda intent_type/estado |
| PROCUREMENT-ACTION-001 | `adquisición`/`compra`/`contratar` entran como AND obligatorio | 1 | **CLOSED Sprint 5** — ACTION_VERBS scrub en _extract_object_terms. Commit `6226234` |
| MORPHO-UNEVEN-001 | Expansión morfológica desigual (`cultura` completo, `turismo` nada) | 1 | **CLOSED Sprint 5** — 120 variantes en 15 rubros. ROOT_TO_VARIANTS 35→50. Commit `3007088` |
| HEADER-TOPIC-OMISSION-001 | Header del response omite tokens del objeto buscado | 1 | **CLOSED** — `_build_interpretation` renderiza objeto correctamente |
| MOJIBAKE-DISPLAY-001 | Caracteres mal decodificados de SECOP (`Ã³`, `Ã±`) renderizados en UI | 1 | **LOW — VIVO**. 0% implementado. `unicodedata.normalize('NFKC')` pendiente. |

---

**Consolidado 2026-05-23 (post-Sprint 5):** Tabla única. Las secciones duplicadas "Pendientes fuera del corpus" y "Categorías nuevas para corpus de diálogo" fueron eliminadas. Su contenido relevante se fusionó aquí. La tabla "Pendientes fuera del corpus" listaba bugs como vivos que ya estaban cerrados desde Sprint 1A — era desactualización por doble mantenimiento.

**Bugs realmente vivos (8):** SOURCE-COVERAGE-001, HEAVY-QUERY-PROBE-001, REGION-MACRO-001, EXPORT-XLSX-001, MARKET-NODES-001, RADAR-NODES-001, CCE-TAXONOMY-001, RAG-FT-STRATEGY-001. 2 bugs + 6 features.

---

## Categorías nuevas para corpus de diálogo (Sprint 0A)

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

## Deuda arquitectónica Sprint 2A

D1. Plan guard vive en resolve_entities (early return), no en
    transición Burr condicional. La transición with expr() no
    respetó el should_execute_decision escrito por plan_query.
    Investigar Burr write/read ordering en Sprint 2B.

D2. build_advisor_response pisa la respuesta de clarificación.
    Parche en run_query detecta should_execute=False y restaura.
    Limpiar acoplamiento en Sprint 2B.

D3. P2 (non_query_followup) sin smoke E2E real (solo unit + corpus
    con prev_params inyectado). P8 (invalid_value_range) es código
    muerto E2E porque query_router auto-swap previene rangos
    inválidos aguas arriba.

D4. Corpus runner es unit-level con params curados, no E2E.
    Los tests E2E reales viven en test_sprint2a_canonical_smoke.py
    y test_plan_query_*.py.

D5. dialogue_policy.py recibió expansión léxica ("favor"/"por favor")
    en B5 fuera de scope. Registrar para auditoría arquitectónica.

---

## Mejoras UX Sprint 3 fase 2 (2026-05-23)

Estas NO son fixes de bugs del corpus — son mejoras de UX incremental que no afectan el comportamiento core de parsing/SoQL.

| Mejora | Archivos | Estado |
|--------|----------|--------|
| Observer 2.0 — 4 signals cuantitativos (entity_concentration, modality_dominance, temporal_skew, value_outlier_ratio) | `app/core/observer.py`, `app/core/formatter.py` | ✅ Cerrado |
| Suggester contextual por intent_type | `app/core/suggester.py`, `app/core/orchestrator.py` | ✅ Cerrado |
| Onboarding /start en Telegram + Streamlit | `app/main.py`, `streamlit_app.py` | ✅ Cerrado |
| Demo scripts multi-turno A/B/C | `scripts/demo_*.py` | ✅ Cerrado |

**Métricas al cierre:**
- pytest: 736 passed, 3 xfailed
- corpus v1: 88/88 PASS
- corpus dialogue: 60/60 PASS
- lint-core: verde

---

## Bugs dogfooding 2026-05-25

Bugs descubiertos en sesión de dogfooding con query real: "muéstrame procesos para ofertar en cundinamarca entre 100 y 500 millones en reparaciones o mantenimientos".

| Bug | Severidad | Estado | Commit |
|-----|-----------|--------|--------|
| MULTI-AND-OVER-RESTRICTIVE-002 — "o" explícito ignorado, SoQL usaba AND | ALTA | ✅ Cerrado | `f9aec55` |
| ESTADO-DUPLICADO-001 — `params["estado"]` y `params["estado_de_apertura_del_proceso"]` ambos emitían WHERE | MEDIA | ✅ Cerrado | `b5a9f0f` |
| OPP-SECONDARY-SORT-001 — opportunity_search solo ordenaba por fecha, sin precio_base | MEDIA | ✅ Cerrado | `b5a9f0f` |
| HEADER-PLURAL-001 — "1 resultados" en vez de "1 resultado" | BAJA | ✅ Cerrado | `ad74d69` |

### MULTI-AND-OVER-RESTRICTIVE-002

**Causa:** `_extract_object_terms()` en query_router.py detectaba "o" explícito y producía `[["reparaciones", "mantenimientos"]]` (OR group). Pero `llm_handler.py` luego lo pisaba con lista plana `["reparaciones", "mantenimientos"]`. El SoQL builder interpretaba lista plana como AND.

**Fix:** `llm_handler.py` ahora verifica si regex ya produjo OR groups (`isinstance(item, list)`). Si sí, preserva el grupo y salta el merge de LLM para objeto.

**Deuda:** ARCH-OBJETO-MERGE-001 — source of truth ambigua entre regex y LLM. Caso edge: si LLM tiene término nuevo que regex no capturó y regex emitió flat list, el nuevo término no se incorpora.

### ESTADO-DUPLICADO-001

**Causa:** `query_router.py:545` ponía `params["estado"] = "Abierto"` (legacy) Y `params["estado_de_apertura_del_proceso"] = ["Abierto", "Publicado"]` (estado_family resuelto). SoQL builder procesaba ambos.

**Fix:** SoQL builder ahora salta el legacy `estado` si algún campo específico (`estado_de_apertura_del_proceso`, `estado_del_procedimiento`, `estado_contrato`, `estado_contrato_adicional`) ya está presente en params.

### OPP-SECONDARY-SORT-001

**Causa:** Rama `opportunity_search` en soql_builder.build() solo emitía `ORDER BY fecha DESC` sin secondary sort por valor.

**Fix:** Agregado `precio_base DESC` como secondary sort en la rama opportunity_search. Actualizado test `test_opportunity_ordering_fecha_desc` con comentario ADR.

### HEADER-PLURAL-001

**Causa:** Todos los headers usaban "resultados" hardcodeado. Con total_count=1 decía "1 resultados".

**Fix:** Helper `_resultado(n)` que retorna singular/plural. Aplicado a WhatsApp, Telegram y Streamlit. Caso `total_count_approximate` preserva el `+` ("1+ resultados").

---

## Fixes dogfooding UX 2026-05-25 (sesión Claude)

Fixes aplicados en sesión de trabajo posterior al dogfooding inicial. Documentados en ADR-017.

| Fix | Archivos | Estado |
|-----|----------|--------|
| **DOGFOOD-URL-005** — URLs por tipo de proceso | `formatter.py:_safe_url()`, `secop_client.py:_normalize_row()` | ✅ Cerrado |
| **DOGFOOD-DATE-002** — Fecha fallback cascada | `formatter.py:_best_date_for_row()` | ✅ Cerrado |
| **DOGFOOD-REFINE-004** — Refinement buttons funcionales | `orchestrator.py:_run_refinement()`, `streamlit_app.py` | ✅ Cerrado |
| **DOGFOOD-RANGE-005** — Rango "entre X y Y" herencia de unidad | `query_router.py:_extract_amounts()` | ✅ Cerrado |
| **DOGFOOD-ACTION-005B** — ACTION_VERBS += "celebrar" | `intent_vocabulary.py` | ✅ Cerrado |
| **DOGFOOD-SOQL-006** — Eliminar CASE WHEN en ORDER BY | `soql_builder.py`, `formatter.py:_reorder_active_first()` | ✅ Cerrado |

**Nota técnica DOGFOOD-SOQL-006:** El CASE WHEN `estado_contrato IN ('En ejecución', ...)` en ORDER BY de jbjy-vk9h causaba 400 Bad Request "Unexpected character". El parser interno de Socrata transformaba a `#CASE(x IN (...), 0, TRUE, 1)` y fallaba con strings acentuados. Fix: eliminar CASE WHEN del ordering SoQL, mover boost "activos primero" a post-procesamiento Python. Impacto colateral: timeouts sistemáticos en contratos atribuidos a "API lenta" se resolvieron.

**Tests:** 945 passed, 10 xfailed.
**Documentación:** `docs/operacional/DOGFOOD_2026-05-25.md`, `docs/planes/funnel-full-ux-fixes-2026-05-25.md`, `docs/decisions_log.md#ADR-017`.
# SECOPPAL — Informe de robustecimiento sistémico

**Fecha:** 2026-05-18
**Autor:** Claude (consultor)
**Alcance:** auditoría de viabilidad de la propuesta del asesor (5 capas) + plan ejecutable de robustecimiento.
**Horizonte acordado:** "el tiempo que sea requerido"; foco en robustez, acierto y comprensión, no en demo rápida.

---

## 1. Contexto y motivación

SECOPPAL pasó de MVP a etapa de bugs visibles. El backlog actual (DEMO-BLOCKERS-001 + CORPUS-BACKLOG + 3 xfailed) suma 12+ items que tienen tres firmas distintas pero, al mirarlos juntos, revelan un patrón único: **el sistema decide qué buscar de forma reactiva y por capas determinísticas inconexas**. No hay una capa que diga "antes de buscar, ¿esto tiene sentido?". La consecuencia es que cada bug obliga a un parche regex en `query_router.py` o `intent_vocabulary.py`, y la deuda crece linealmente con la cantidad de queries que ven los usuarios.

La propuesta del asesor (5 capas: QueryPlanner, DialoguePolicy, SearchSafety, Relevance, ResponsePolicy) es **técnicamente correcta y compatible con la arquitectura actual**. La pregunta no es "si hacerlo" sino "en qué orden, con qué dependencias, y dónde meter LLM vs determinístico". El resto del informe responde eso.

---

## 2. Qué tiene SECOPPAL hoy (auditoría)

Mapeo crudo entre lo que el asesor propone y lo que ya existe. Esto es importante porque varias "capas nuevas" son en realidad **consolidaciones** de piezas dispersas, no construcciones desde cero.

| Pieza propuesta | Estado actual | Dónde vive |
|---|---|---|
| QueryPlanner explícito (`should_execute`, `missing_slots`, `specificity_score`) | **No existe como tal.** Decisión repartida en 3 puntos. | `orchestrator.py:resolve_entities` (clarif si entidad no resuelve) + `orchestrator.py:execute_query` líneas 196-211 (guard `SCOPE_TOPIC_KEYS`) + `response_policy.py:_build_ambiguous_response` |
| Representación intermedia tipo plan | **Existe parcial.** `QueryFrame` es proto-plan con `scope/topic/modifiers/intent_type`. | [query_frame.py:20](app/core/query_frame.py:20) — dataclass con `frame_from_params` / `params_from_frame` |
| Dialogue policy (comandos incompletos, missing slots) | **Solo parches sueltos.** STOPWORDS extensas en parser, pero "filtra por departamento" no activa missing_slot. | [query_router.py:97-206](app/core/query_router.py:97), líneas 581-587 (ordering sin objeto → `needs_llm=True`, no `needs_clarification`) |
| SAFE-SEARCH (búsqueda demasiado amplia) | **Solo SAFE-SOQL técnico** (anti `WHERE 1=1`). No hay scoring de especificidad. | [soql_builder.py:40-46, 149-176](app/core/soql_builder.py:40) (`allow_global`) + `_followup_constants.SCOPE_TOPIC_KEYS` |
| Relevance layer (direct / probable / weak) | **No existe.** Solo `ordering_signal` (fecha vs valor). | — |
| ResponsePolicy asesora con tres caminos | **Existe el esqueleto** (ambiguo / cero / claro), pero sin etiquetar calidad ni distinguir timeout vs cero. | [response_policy.py:29-140](app/core/response_policy.py:29) |
| LLM Query Understanding | **LLM ya devuelve JSON estructurado** via tool calling (DeepSeek). Solo entra si `needs_llm=True`. No emite `missing_slots` ni `should_execute`. | [llm_handler.py:17-252](app/core/llm_handler.py:17), schema `buscar_procesos` |
| Opportunity policy post-LLM | **Existe.** Mini-enforcement determinístico. | [opportunity_policy.py:31-59](app/core/opportunity_policy.py:31) |
| Burr graph (insertar nodos) | **Extensible.** 11 nodos actuales, transiciones declarativas `expr()`. | [orchestrator.py:797-860](app/core/orchestrator.py:797) |

**Hallazgo crítico:** el sistema ya tiene **70% de las piezas** que el asesor propone, pero distribuidas y sin nombre. La intervención correcta es **consolidar y nombrar**, no construir capas paralelas. Cada parche futuro debe caer en una capa nombrada o convertirse en bug arquitectónico.

---

## 3. Base empírica (la verdad incómoda)

| Recurso | Volumen | Calidad |
|---|---:|---|
| `data/feedback.jsonl` | 2009 entradas | Solo **28 con rating=0** (1.4%). 1969 sin rating. Suficiente como semilla, insuficiente como evidencia. |
| `data/conversations/st_default.jsonl` | **2 turnos** | No hay sesiones multi-turno reales explotables. Imposible diseñar dialogue policy "guiado por datos" sin sintetizar. |
| Corpus v1 (`query_corpus_v1.yaml`) | 88 entradas | 88/88 PASS hoy. Cubre parser + workflow básico. No cubre `should_execute=false` ni `needs_clarification`. |
| Tests | 579 passed, 3 xfailed | Ningún archivo `test_dialogue_policy*` ni `test_clarification*`. |
| Queries únicas en feedback | 130 | Diversidad pobre; muchas son la misma con variaciones léxicas. |

**Diagnóstico honesto:** la base empírica **no alcanza** para diseñar `specificity_score`, umbrales y "relevance débil/probable/directa" sin caer en ingeniería de intuición. La decisión acordada (50 reales + 150 sintéticos) es realista, pero hay que ejecutarla **antes** de implementar la capa correspondiente, no después. Diseñar primero y luego buscar evidencia es la receta del parche infinito.

**Implicación de orden:** corpus → capa, no capa → corpus.

---

## 4. Veredicto sobre la pregunta central

> ¿Intervención sistemática o caso-a-caso?

**Respuesta:** ambas, pero **sistemática primero**, caso-a-caso como prueba de fuego después. Razones:

1. **Caso-a-caso solo escala lineal.** Hoy hay 12 items; mañana 30. Cada uno requiere regex/stopword adicional. El parser ya tiene STOPWORDS de 109 líneas (líneas 97-206). Más entropía garantizada.

2. **Sistemático sin casos reales = arquitectura desacoplada.** Es lo que evitamos con el corpus híbrido (50+150).

3. **El stack actual soporta la intervención.** Burr permite insertar nodos sin refactor. `QueryFrame` ya es proto-plan. LLM ya devuelve JSON. No hay deuda técnica de stack bloqueando esto.

**Plan de orden correcto:**
1. **Consolidar** las decisiones dispersas en una capa nombrada (`QueryPlan`).
2. **Cubrir** las regularidades del backlog DEMO-BLOCKERS con reglas que vivan en esa capa, no en parches.
3. **Construir corpus** dialogue/relevance antes de las capas que dependen de él.
4. **Medir** con el corpus antes y después de cada cambio (pre-existe `scripts/run_query_corpus.py`).

---

## 5. Análisis crítico de las 5 capas propuestas

No todas pagan su costo. Sincero:

### 5.1. QueryPlanner — **SÍ, prioridad máxima**

**Por qué paga.** Resuelve simultáneamente: COMMAND-SLOT-001, OPP-SCOPE-001, VALUE-TYPO-001, OPP-TIMEOUT-001 y el bug de "filtra" como objeto. Es la capa que más unifica el backlog.

**Cómo implementarlo (concreto, no abstracto):**

Crear `app/core/query_plan.py`:

```python
@dataclass
class QueryPlan:
    should_execute: bool
    dataset: str | None
    intent_type: str | None
    missing_slots: list[str]            # ["departamento", "rubro", ...]
    clarification_question: str | None
    specificity_score: int               # 0-15
    risk_flags: list[str]                # ["timeout_likely", "no_topic", ...]
    params: dict                          # params normalizados
    reason: str                           # por qué se decidió ejecutar / aclarar
```

**Determinístico, no LLM.** Las reglas son booleanas: si `query in COMMAND_FRAGMENTS` y `value_slot is None` → `should_execute=False`. No necesita semántica profunda.

**Inserción Burr:** entre `apply_context` y `resolve_entities`. Si `should_execute=False`, va directo a `clarify_query` (ya existe como nodo) y omite el resto.

**Costo:** 2-3 días de implementación + 100 tests unitarios.

### 5.2. SlotPolicy + comandos incompletos — **SÍ, parte de QueryPlanner**

No es capa separada; es una regla dentro de `QueryPlan`. Catálogo finito y enumerable:

```python
COMMAND_SLOTS = {
    "filtra por departamento": "departamento",
    "filtra por ciudad": "ciudad",
    "filtra por entidad": "entidad",
    "filtra por valor": "valor_range",
    "ordena por": "ordering_signal",
    "muéstrame por": "ordering_signal",
    "busca por modalidad": "modalidad",
    # ...
}
```

Detección por regex sobre la query completa (con tolerancia a "filtra por la ciudad" → mismo slot). El catálogo crece con feedback real. No es deep learning, es disciplina.

**Casos cubiertos del backlog:** COMMAND-SLOT-001 directo.

### 5.3. SearchSafety / specificity_score — **SÍ, pero con cuidado**

**Por qué paga.** OPP-SCOPE-001 ("Bolívar" sin rubro → 228K resultados) es el caso canónico. Cualquier opportunity_search territorio-only debe pedir rubro.

**Cómo NO hacerlo.** No proponer un score "objeto=+3, ciudad=+2..." con umbrales aleatorios. Eso es teatro estadístico: los pesos son inventados.

**Cómo SÍ hacerlo.** Reglas por intent_type:
- `opportunity_search` requiere al menos uno de `{rubro, entidad, valor_range≥1B, modalidad}` además de territorio.
- `analytical_intent=aggregate_sum` requiere `scope+topic` (ya lo exige hoy el guard).
- `bidder_intent` mismo que opportunity_search.
- Dataset contratos sin scope ni topic → bloqueado por SAFE-SOQL (ya existe).

Estas reglas son auditables y verificables con el corpus. El "score" lo dejamos para una fase 2 si las reglas resultan insuficientes (no creo que lo sean).

**Inserción:** dentro de QueryPlan.compute(), después de slot detection.

### 5.4. Relevance layer (direct / probable / weak) — **SÍ, pero después y con embeddings, no LLM por resultado**

**Por qué paga.** OPP-RELEVANCE-001 ("campaña institucional" vendida como "construcción de hospitales") es el bug que hace que el usuario pierda confianza más rápido que cualquier otro. Devolver basura confidente es peor que devolver 0 resultados.

**Por qué NO con LLM por resultado.** SECOP devuelve 10-50 rows por query. LLM-call por row = ~500ms × 30 = 15s extra. Suicidio de UX.

**Alternativa técnica honesta:** embeddings (e.g., `multilingual-e5-small` ~118MB local, o cohere/openai embed) + similitud coseno entre `topic.parsed` y `objeto_del_contrato`.

- Embedding del topic: 1 llamada por query.
- Embedding por resultado: ya está pre-computable (offline), pero como SECOP es API externa, hay que hacerlo on-the-fly. Costo: 30 rows × ~50ms embed → 1.5s aceptable, o se mete en paralelo al fetch.
- Umbrales aprendidos del corpus (50 reales): `cos>0.75 → direct`, `0.5-0.75 → probable`, `<0.5 → weak`. Los umbrales nacen del corpus, no de intuición.

**Alternativa más barata si embedding pesa:** BM25 + reglas léxicas (token overlap, bigrams, sinónimos de `morphological_variants.py`). Menos preciso pero gratuito. Recomiendo BM25 para v1 y embeddings para v2.

**Inserción Burr:** nuevo nodo `score_relevance` después de `execute_query`, antes de `format_response`.

### 5.5. ResponsePolicy asesora — **SÍ, sin LLM en el hot path**

**Por qué paga.** OPP-UX-001, OPP-STATE-001, OPP-SUGGEST-001 son todos respuesta, no búsqueda. Hoy `response_policy.py` ya tiene 3 caminos; falta:

- **Caminos nuevos:** `timeout` (distinto de cero), `weak_results_only`, `command_incomplete`, `oversized_search`.
- **Templates parametrizados, no LLM.** Cada camino tiene plantilla. Costo: 0 latencia, 0 alucinación.
- **LLM solo opcional para la "lectura rápida"** del narrator (ya existe con grounding validation en [narrator.py:169](app/core/narrator.py:169)). Mantenerlo como hoy.

**Lo que NO debe pasar:** "La mayoría son de 2026" cuando ya filtraste 2026. Eso se arregla en `observer.py:UniverseInsights` excluyendo dimensiones que el usuario fijó explícitamente. Es regla determinística, no prompt LLM.

### 5.6. LLM como Query Understanding draft — **NO en esta fase**

El asesor propone que LLM emita `{goal, topic, geo, missing_slots, should_execute, confidence}` y luego validadores determinísticos confirmen.

**Mi posición técnica:** no aporta lo suficiente para justificar el costo, dado que:

1. El regex actual + `opportunity_policy.py` ya cubre ~85% de queries del corpus (88/88 hoy).
2. Lo que el LLM aporta es robustez ante variantes lingüísticas no anticipadas. Pero eso ya lo cubre `llm_handler` como fallback cuando `needs_llm=True`.
3. Si `QueryPlan` es determinístico, su salida es auditable. Si el LLM emite el plan, hay que validar cada campo y rehacer si discrepa. Doble trabajo.
4. Latencia: cada query pasa por LLM (~500ms-1s). Hoy solo ~30% pasa. Triplicar latencia sin ganancia clara.

**Cuándo sí lo metería:**
- En **clarification questions**: cuando `missing_slots` no es trivial ("¿de qué rubro?"). LLM genera la pregunta natural, no la plantilla.
- En **paraphrasing del topic** ambiguo: "mantenimiento de cosas" → LLM normaliza a `{mantenimiento_correctivo, mantenimiento_preventivo}` con confidence.
- En **relevance v2** si embeddings no alcanzan (raro).

Decisión: **LLM se mantiene en su rol actual** (fallback de extracción de params). No se promueve a Query Understanding draft en este sprint.

---

## 6. Stack — qué cambia y qué no

**No cambia:**
- Python, FastAPI, Burr, Streamlit, Socrata, RapidFuzz, DeepSeek (LLM actual). Todo maduro y suficiente.
- Pytest como suite de regresión.
- `feedback.jsonl` como signal source.

**Se agrega:**
- **Embedding lib para Relevance v2** (no v1): `sentence-transformers` con modelo `multilingual-e5-small` local (~118MB). Alternativa cloud: Cohere embed multilingual. Recomendación: local para no añadir dependencia de red.
- **Corpus extendido**: `tests/fixtures/dialogue_policy_corpus.yaml` (200 casos) + extensión del runner para validar `should_execute`, `missing_slots`, `clarification_contains`.
- **Métricas de calidad**: script `scripts/quality_report.py` que corre el corpus y publica delta pre/post-cambio.

**Se retira o deprecia:**
- Las stopwords ad-hoc de `query_router.py:97-206` se migran a `app/core/dialogue_policy.py` (consolidación). El archivo viejo solo conserva las stopwords ortogonales a comandos (cortesía, conectores).
- `opportunity_policy.py:enforce_bidder_opportunity_policy` se convierte en regla dentro de `QueryPlan.compute()`, no en post-procesador suelto del LLM.

---

## 7. Plan de sprints (versión reducida, post-revisión del asesor)

> **Nota de ejecución.** El asesor revisó este plan y aprobó el norte arquitectónico con cuatro ajustes pragmáticos: (1) no esperar 1-2 semanas a un corpus de 200; arrancar con 40-60 casos mínimos. (2) no migrar stopwords de `query_router.py` en el primer sprint. (3) no absorber `opportunity_policy.py` todavía — QueryPlan lo respeta, no lo absorbe. (4) Relevance v1 con BM25, embeddings solo si no alcanza. La versión expandida del Sprint 0 (200 casos, migración total) sigue siendo el norte; los sprints abajo son el camino aterrizado.

### Sprint 0A — Dialogue corpus mínimo (50-60 casos)

**Antes de tocar código de core, pero sin esperar 2 semanas.**

Composición del corpus:
- **20-30 queries humanas reales** producidas por el usuario (Rodrigo) — base no negociable. Incluye los DEMO-BLOCKERS reales que ya viste fallar.
- **30 casos sintéticos** derivados de los bugs del backlog y de las 28 entradas `rating=0` del feedback existente.

Categorías:

- `command_slot` (8-10 casos): "filtra por departamento", "filtra por ciudad", "ordena por", "filtra por valor".
- `oversized_search` (6-8 casos): "procesos para presentarme en Bolívar", "contratos en 2026", opportunity_search solo territorio.
- `timeout_ux` (4-6 casos): timeout simulado, verificar que la respuesta no diga "0 resultados".
- `state_opportunity` (6-8 casos): "adulto mayor abiertos", "Cerrado" mostrado en opportunity, prioridad estado_del_procedimiento.
- `value_typo` (4-6 casos): "milloones", rangos imposibles (1B..5M).
- `weak_relevance` (6-8 casos): "construcción de hospitales" que matchea campaña institucional.
- `clarification` (4-6 casos): respuestas esperadas a comandos incompletos.

**Entregable:**
- [tests/fixtures/dialogue_policy_corpus.yaml](tests/fixtures/dialogue_policy_corpus.yaml)
- Extensión de [scripts/run_query_corpus.py](scripts/run_query_corpus.py) con validadores: `should_execute`, `missing_slots`, `clarification_contains`, `response_contains_class`.
- Baseline `before.json`.

**Tiempo estimado:** 2-3 días.

### Sprint 1A — QueryPlan mínimo

**Reglas iniciales (no más):**

1. `filtra por departamento` / `filtra por ciudad` / `filtra por entidad` → `should_execute=False`, `missing_slot=<slot>`.
2. `ordena por` (sin objeto/criterio) → `should_execute=False`, `missing_slot=ordering_criterion`.
3. `opportunity_search` + solo territorio (sin rubro, sin entidad, sin modalidad, sin valor_range≥1B) → `should_execute=False`, `missing_slot=rubro`.
4. `valor_min > valor_max` → `should_execute=False`, `risk_flag=invalid_range`, `clarification_question` parametrizada.
5. Normalización determinística de typos monetarios comunes (`milloones`→`millones`, `bilones`→`billones`) — diccionario corto.
6. Marcar `risk_flag=timeout_likely` cuando la query tenga `intent_type=opportunity_search` y scope muy amplio, para que ResponsePolicy lo use en caso de timeout real.

**Archivos:**
- Crear [app/core/query_plan.py](app/core/query_plan.py) — dataclass + `compute(params, context) -> QueryPlan`.
- Crear [app/core/dialogue_policy.py](app/core/dialogue_policy.py) — catálogo `COMMAND_SLOTS` y detección de patrones nuevos. **No migrar stopwords de `query_router.py`** en este sprint; el módulo solo *agrega* detección, no reemplaza.
- Modificar [app/core/orchestrator.py:797-860](app/core/orchestrator.py:797) — insertar nodo `plan_query` entre `apply_context` y `resolve_entities`. Si `should_execute=False`, transición a `clarify_query` (ya existe).

**Lo que NO se toca en este sprint:**
- `app/core/query_router.py` stopwords — siguen como están.
- `app/core/opportunity_policy.py` — sigue como enforcement post-LLM independiente. `QueryPlan` lo *consulta* (lee `intent_type` y `estado_family` que aquel haya sembrado), no lo absorbe.
- `app/core/llm_handler.py` — sin cambios.

**Cubre del backlog:** COMMAND-SLOT-001, OPP-SCOPE-001, VALUE-TYPO-001, OPP-TIMEOUT-001 (parcial: marca el flag, el cierre completo va en Sprint 2 vía ResponsePolicy).

**Validación:** corpus 0A en verde, suite pytest verde, smoke manual de 10 queries.

**Tiempo estimado:** 3-4 días.

### Sprint 0B — Ampliar corpus a 200 casos (en paralelo a Sprint 2)

Mientras se implementa el Sprint 2, capturar 50 casos reales (3-5 sesiones de usuario) y sintetizar otros 100-150 para llegar al corpus completo. Los umbrales de Sprint 3 (relevance) ya esperarán este corpus expandido.

### Sprint 2 — ResponsePolicy asesora

**Cubre del backlog:** OPP-UX-001, OPP-STATE-001, OPP-SUGGEST-001, OPP-TIMEOUT-001 (cierre).

**Archivo:** [app/core/response_policy.py](app/core/response_policy.py) — añadir caminos:
- `timeout_response()` — respeta `risk_flag=timeout_likely`, NUNCA dice "0 resultados".
- `command_incomplete_response()` — usa `clarification_question` del plan.
- `oversized_search_response()` — sugiere rubros, no busca.
- `weak_only_response()` — placeholder hasta Sprint 3.

**Ajustes específicos del backlog:**
- `observer.py:UniverseInsights` — excluir dimensiones explícitamente fijadas (no decir "la mayoría son de 2026" si el usuario filtró 2026).
- `suggester.py` — sugerencias contextuales por `intent_type`. No proponer "Buscar en contratos" cuando `intent_type=opportunity_search`.
- `formatter.py` — en opportunity, normalizar la presentación del estado: no mostrar "Cerrado" bruto sin contexto.

**Validación:** corpus categorías `timeout_ux`, `state_opportunity`, `oversized_search` en verde.

**Tiempo estimado:** 4-5 días.

### Sprint 3 — Relevance layer v1 (BM25 + reglas léxicas)

**Archivos:**
- Crear [app/core/relevance.py](app/core/relevance.py) — `score_row(topic, row) -> {class: direct|probable|weak, score: float, reasons: [...]}`. Usa BM25 sobre `objeto_del_contrato/proceso` + checks de bigrams del topic + sinónimos de `morphological_variants.py`.
- Insertar nodo Burr `score_relevance` después de `execute_query`.
- Modificar `formatter.py` y `response_policy.py` para etiquetar visualmente weak/probable.

**Cubre:** OPP-RELEVANCE-001, OPP-STATE-001 (parcial), OPP-UX-001 (parcial vía respuesta etiquetada).

**Validación:** corpus categoría `relevance` debe etiquetar correctamente al menos 85% de casos. Si <70%, escalar a v2 con embeddings.

### Sprint 4 — ResponsePolicy pulido UX post-relevance

Solo cosas que dependen de que Sprint 3 (Relevance) esté en su sitio. No repite Sprint 2.

- `weak_only_response()` y `weak_mixed_response()` — etiquetar visualmente weak/probable/direct en `formatter.py` y respuesta agregada en `response_policy.py` ("Encontré 1 coincidencia débil, pero no parece ser una obra hospitalaria directa.").
- Reordenamiento por relevance class antes de aplicar `ordering_signal` (un direct con menor valor va antes que un weak con mayor valor).
- Cierre de OPP-RELEVANCE-001.

**Cubre:** OPP-RELEVANCE-001 (cierre), refinamiento de OPP-SUGGEST-001.

### Sprint 5 — Relevance v2 (embeddings) si v1 no alcanzó

Condicional. Solo si Sprint 3 mostró que BM25 + reglas no llega al 85%. Añadir `sentence-transformers` con `multilingual-e5-small`, precomputar embeddings de los 30 rows en paralelo al fetch, similitud coseno + umbrales del corpus.

### Sprint 6 — LLM en clarification questions (opcional)

Si las clarification questions plantilladas se sienten robóticas, meter LLM solo para generar la pregunta natural a partir de `QueryPlan.missing_slots`. Sin cambio en should_execute (sigue determinístico).

---

## 8. Riesgos y mitigaciones

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Corpus sintético sesgado por mis intuiciones | Las capas pasan corpus pero fallan en producción | Los 50 reales son no-negociables. Sesión de captura antes del Sprint 1. |
| QueryPlan se vuelve dios-objeto | Toda lógica colapsa en un módulo de 2000 líneas | Reglas en `dialogue_policy.py`; QueryPlan solo orquesta. Lint por LoC al final del sprint. |
| Relevance v1 (BM25) no alcanza el 85% | Sprint 3 se alarga | Umbral de decisión claro al final del sprint: si <70% escalar a v2 sin debate. |
| Embeddings local pesan en latencia | UX se siente lenta | Modelo small (~118MB), embedding del topic cacheado por session, embedding de rows en paralelo al fetch SECOP. |
| Burr graph se vuelve laberinto al insertar 3 nodos nuevos | Mantenibilidad cae | Diagrama del grafo en `docs/burr_graph.md` actualizado por sprint. |
| Stopwords se duplican entre `query_router` y `dialogue_policy` | Bugs intermitentes | Migración total en Sprint 1, no incremental. Test que verifica que las stopwords de comando solo viven en un sitio. |
| LLM cost spike si en Sprint 6 se mete LLM por turno | Costo operativo | LLM solo se invoca cuando `should_execute=False AND missing_slots≠[]`. Frecuencia esperada: <15% de turnos. |

---

## 9. Cómo medir: avance, no-regresión y umbrales de éxito

La pregunta "¿cómo sabemos si sirvió y que no hubo retrocesos?" se responde con tres tipos de métricas, todas automáticas excepto la última.

### 9.1. Métricas defensivas (no-regresión) — **línea roja**

Estas son no-negociables. Si **cualquiera** falla después de un sprint, el sprint no se mergea. Cero excepciones.

| Métrica | Baseline actual | Tolerancia |
|---|---|---|
| Suite pytest | 579 passed, 3 xfailed | **0 transiciones passed→failed.** Los 3 xfail pueden bajar a 0 (avance), nunca subir. |
| Corpus existente `query_corpus_v1.yaml` | 88/88 PASS | **0 transiciones PASS→FAIL.** |
| `make lint-core` | verde | sigue verde |
| Torture matrix | 12/12 invariantes | 12/12 |
| `data/feedback.jsonl` | intacto en commits | intacto en commits (excepto cuando se agrega trazas legítimas) |

**Cómo:**
```bash
# Antes de empezar el sprint:
pytest -q | tee baseline/sprint_X_pytest_before.txt
python scripts/run_query_corpus.py --mode all --json > baseline/sprint_X_corpus_before.json
make lint-core

# Al final del sprint:
pytest -q | tee baseline/sprint_X_pytest_after.txt
python scripts/run_query_corpus.py --mode all --json > baseline/sprint_X_corpus_after.json

# Diff automatizado:
python scripts/quality_report.py --before baseline/sprint_X_corpus_before.json \
                                 --after baseline/sprint_X_corpus_after.json
```

`scripts/quality_report.py` (entregable del Sprint 0A) imprime:
- transiciones FAIL→PASS (avances)
- transiciones PASS→FAIL (regresiones — línea roja)
- queries nuevas en el corpus y su tasa de PASS

### 9.2. Métricas ofensivas (avance) — qué se cierra y qué se mejora

Cada sprint declara explícitamente **qué casos del corpus pasa de FAIL→PASS**. Ningún sprint se cierra si no produce avance medible.

| Sprint | Casos del corpus que deben pasar a PASS | Umbral mínimo |
|---|---|---|
| 1A | command_slot (8-10), oversized_search (6-8), value_typo (4-6) | ≥85% de cada categoría |
| 2 | timeout_ux (4-6), state_opportunity (6-8), clarification (4-6) | ≥85% de cada categoría |
| 3 | weak_relevance (6-8) | ≥80% (relevance es más difícil) |

**Cómo se calcula el "85%":** la categoría tiene N casos. Si N=8 y umbral=85%, al menos 7 deben PASS al final del sprint. Si no se llega: el sprint queda *parcial* y se documenta qué falta.

### 9.3. Métricas conductuales end-to-end por DEMO-BLOCKER

Por cada uno de los 8 bugs visibles del backlog, un test E2E con assert **conductual** (no técnico). Estos viven en `tests/test_demo_blockers.py` (nuevo):

```python
def test_command_slot_001_filtra_por_departamento():
    result = workflow.run_query("filtra por departamento", chat_id="...")
    assert "¿A qué departamento" in result["response"]
    assert "Resultados:" not in result["response"]
    assert result["should_execute"] is False
    assert result["soql_query"] == ""  # no se ejecutó SECOP

def test_opp_scope_001_solo_territorio_sin_rubro():
    result = workflow.run_query("procesos para presentarme en Bolívar")
    assert result["should_execute"] is False
    assert "rubro" in result["response"].lower() or "tema" in result["response"].lower()
    assert result["total_count"] is None or result["total_count"] < 1000

# ... uno por cada DEMO-BLOCKER
```

Estos tests son la **evidencia ejecutable** de que el bug está cerrado. Si después de Sprint 4 los 8 pasan, los demo-blockers están cerrados.

### 9.4. Métrica longitudinal (post-deploy)

Solo medible cuando hay usuarios reales usando la versión nueva:

- `data/feedback.jsonl` rating=0 / total con ventana móvil de 200 turnos. Baseline 1.4%; objetivo <0.5% post-Sprint 4.
- Tasa de `should_execute=False` por turno. Si es <5% probablemente el plan es demasiado permisivo; si es >25% probablemente demasiado restrictivo. Rango sano esperado: 8-15%.
- Tasa de timeout que se presenta como "0 resultados" (debe ser 0).

### 9.5. Métrica subjetiva ("se siente más asesor")

No automatizable. Después del Sprint 2 y del Sprint 4: el mismo usuario (Rodrigo) repite las 20-30 queries humanas que produjo en Sprint 0A y registra **una de tres** etiquetas por query: `mejor`, `igual`, `peor`. Si la proporción `peor` > 10%, hay regresión cualitativa aunque las métricas técnicas estén verdes.

### 9.6. Snapshot del estado actual (medir hoy, antes de tocar nada)

Antes del Sprint 1A:

```bash
mkdir -p baseline
pytest -q > baseline/M0_pytest.txt 2>&1
python scripts/run_query_corpus.py --mode all --json > baseline/M0_corpus.json
make lint-core > baseline/M0_lint.txt 2>&1
git log --oneline -1 > baseline/M0_commit.txt
```

Este "M0" es el punto de comparación de TODO el resto. Sin M0 no hay forma honesta de saber si hubo retroceso. Por eso es el primer entregable, antes incluso de empezar el corpus de diálogo.

---

## 10. Archivos críticos a modificar (referencia rápida)

**Nuevos:**
- [app/core/query_plan.py](app/core/query_plan.py) — Sprint 1
- [app/core/dialogue_policy.py](app/core/dialogue_policy.py) — Sprint 1
- [app/core/relevance.py](app/core/relevance.py) — Sprint 3
- [tests/fixtures/dialogue_policy_corpus.yaml](tests/fixtures/dialogue_policy_corpus.yaml) — Sprint 0
- [scripts/quality_report.py](scripts/quality_report.py) — Sprint 0

**Modificados:**
- [app/core/orchestrator.py:797-860](app/core/orchestrator.py:797) — inserción de nodos Burr (`plan_query`, `score_relevance`)
- [app/core/query_router.py:97-206](app/core/query_router.py:97) — **no migrar stopwords en Sprint 1A**; solo revisar interacciones. Migración deferida hasta que dialogue_policy esté estabilizado (>3 sprints en verde).
- [app/core/opportunity_policy.py](app/core/opportunity_policy.py) — **no absorber en Sprint 1A**. QueryPlan lo consulta/respeta (lee `intent_type` y `estado_family` que sembró). Absorción es decisión separada post-Sprint 4.
- [app/core/response_policy.py:29-140](app/core/response_policy.py:29) — caminos nuevos en Sprint 2; pulido weak/probable en Sprint 4.
- [app/core/observer.py](app/core/observer.py) — exclusión de dimensiones fijadas (Sprint 2)
- [app/core/formatter.py](app/core/formatter.py) — etiquetas weak/probable/direct (Sprint 3-4)
- [scripts/run_query_corpus.py](scripts/run_query_corpus.py) — validadores nuevos (Sprint 0A)

**Reutilizar (no reescribir):**
- [app/core/query_frame.py](app/core/query_frame.py) — `frame_from_params` / `params_from_frame`, sigue como representación entre params y FollowupEngine.
- [app/core/followup_engine.py](app/core/followup_engine.py) — clasificación de 9 tipos sigue siendo válida; solo se le agrega `command_incomplete` y `oversized_search` como tipos cuando vienen del plan.
- [app/core/morphological_variants.py](app/core/morphological_variants.py) — semilla léxica de Relevance v1.
- [app/core/llm_handler.py](app/core/llm_handler.py) — sin cambios estructurales en Sprints 1-5; eventual punto de entrada en Sprint 6.

---

## 11. Respuesta directa a las preguntas planteadas

**¿Factible intervención sistémica?** Sí, sin reservas. El stack lo soporta y el 70% de las piezas existen dispersas.

**¿Toca ir caso a caso 200 cases?** Solo como **corpus de validación**, no como modo de implementación. La intervención es sistémica; los 200 casos son la prueba.

**¿Alternativas de diseño o stack?** El stack se mantiene. Las alternativas que sí cambian el diseño:
- **Relevance con embeddings vs BM25:** BM25 primero, embeddings solo si v1 no alcanza.
- **LLM Query Understanding vs determinístico:** determinístico primero. LLM solo en clarification questions opcional.
- **QueryPlan como dataclass vs JSON schema:** dataclass (más auditable, menos dependencias).

**¿Sincero y aterrizado?** El proyecto está en buen punto técnico (579 tests verdes, corpus 88/88, arquitectura coherente). La fricción no es técnica, es **conceptual**: faltó nombrar la capa que decide qué buscar. Una vez nombrada, el resto cae en su sitio. El riesgo más alto no es ninguno de los sprints — es **no construir el corpus mínimo (20-30 queries humanas + 30 sintéticas) antes del Sprint 1A**, y no expandirlo a 50+ reales durante Sprint 0B. Si saltamos eso, las capas serán teatro estadístico.

---

## 12. Acuerdos cerrados con el usuario

1. **Captura de casos reales:** el usuario (Rodrigo) produce 20-30 queries humanas. Estas se combinan con 30 sintéticas en Sprint 0A para llegar a 50-60.
2. **Orden:** M0 baseline → corpus mínimo → QueryPlan mínimo → ResponsePolicy → Relevance v1 → (corpus expandido en paralelo) → Relevance v2 condicional → LLM opcional.
3. **LLM:** se queda como fallback de extracción de params. Solo se promueve si Sprint 6 lo justifica.
4. **Medición:** M0 antes de tocar nada; diff automatizado por sprint vía `scripts/quality_report.py`; línea roja = cero PASS→FAIL en suite y corpus; métricas conductuales E2E en `tests/test_demo_blockers.py`.

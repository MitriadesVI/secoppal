# SECOPPAL — Documento Técnico de Arquitectura, Stack y Bitácora de Cambios

**Fecha de creación:** 2026-05-15  
**Estado:** Borrador técnico V0.1  
**Sistema:** SECOPPAL — asistente conversacional para consulta y análisis de contratación pública SECOP II  
**Propósito del documento:** dejar una descripción comprensible para un tercero técnico o funcional sobre qué hace el sistema, cómo está construido, qué archivos lo componen, qué decisiones de arquitectura se han tomado, qué bugs relevantes se han detectado y cuál es el estado actual del proyecto.

## Estado canónico actual — 2026-06-26

- **Main:** `f683c2b` — mergeada la **capa de retrieval phrase-aware** (ADR-036, tag
  `semantic-retrieval-v1-2026-06`). Arco de 12 commits.
- **Suite:** 1714 passed, 23 xfailed.
- **Qué cambió:** el significado de la consulta sobrevive de punta a punta. Pipeline nuevo:
  `parse → PhraseSpec/objeto_phrases → WHERE recall-first → candidate_limit 200 → phrase_validator
  → rank_results (nivel + BM25) → display_limit 25 → formatter`.
- **Resultado (manual de contratación):** candidate recall 5/100→100/100, validación 96/100,
  Precision@25 52%→100%. Reutilizable: un concepto = un `PhraseSpec` (probado con "aire
  acondicionado"/SIP-049).
- **Bug histórico resuelto:** `RELEVANCE-RANKING-AFTER-FORMAT-001` — el BM25 reordenaba después del
  formateo y nunca afectaba el display; ahora hay etapa formal `rank_results` antes del render.
- **Docs:** ADR-036 en `decisions_log.md`; informe `docs/SEMANTIC_RETRIEVAL_SPRINT_2026-06.md`;
  runbook `docs/PHRASE_RETRIEVAL_RUNBOOK.md`.
- **Próximo foco:** día de dogfooding (40-50 consultas no diseñadas), corpus v2 (4 falsos negativos),
  feature "por qué apareció este resultado".

> El bloque siguiente (2026-06-13) queda como histórico.

## Estado canónico — 2026-06-13 (histórico)

- **Branch:** `feature/analytics-modular-registry` (main tiene Radar v1.0 mergeado)
- **Suite:** 1452 collected (~1425 passed, 23 xfailed)
- **Corpus:** 89/89 PASS, 0 FAIL
- **Torture matrix:** 12/12 invariantes
- **Eval harness:** `scripts/eval_rated.py` sobre 70 queries calificadas reales (SQLite). Parser-tier 38/38 = 100% (5 bugs silenciosamente resueltos por sprints mayo 2026). Pipeline-tier 26 entradas etiquetadas para `--live`. 1 bug remanente: RATED-069 (`El Banco` municipio Magdalena) — reclasificado pipeline-tier, requiere desambiguación LLM.
- **feedback:** SQLite 70 calificadas (18👍/52👎), JSONL 42 (divergencia documentada en `eval_rated.py`). Dual-store: SQLite primario, JSONL append-only.
- **Orchestrator:** 196 LOC (desde 1549 LOC) — refactor a workflow/ completado (ADR-023)
- **Decisions log:** 31 ADRs (ADR-001 a ADR-031) en `docs/decisions_log.md`
- **Features nuevas (junio 2026):** búsqueda por regiones (ADR-028), Radar v1.0 — búsquedas guardadas con feed de movimientos (ADR-029), paquete modular de analytics con registry (ADR-030), adaptador OpenWA + robustez Telegram + paridad de links (ADR-027, ADR-031)
- **Parser fixes (junio 2026):** unaccent en ciudad (BUG 1), polaridad "no sea por más de" (BUG 2), "barras" como jerga monetaria (BUG 3), "no menos de" como valor_min (fix simétrico a BUG 2)
- **Próximo foco:** sellar frame leak del follow-up (raw_params merge ciego), shadow LLM para follow-up, RATED-069 (desambiguación banco=municipio vs banco=tema), merge main → feature branch (Radar + costura regiones)
- **Conteos históricos (424/466/534/596/1212):** superados; referencia, no estado actual

---

## 1. Resumen ejecutivo

SECOPPAL es un asistente conversacional orientado a consultar, filtrar, interpretar y presentar información de contratación pública colombiana proveniente de SECOP II / datos.gov.co.

El objetivo no es replicar un portal de filtros, sino construir un **asesor inteligente de contratación pública** capaz de:

1. Entender consultas naturales del usuario.
2. Traducirlas a parámetros estructurados.
3. Resolver entidades, lugares, departamentos, contratistas, temas, fechas, valores y estados.
4. Construir consultas SoQL contra datasets de SECOP.
5. Ejecutar búsquedas con guardrails para evitar consultas globales peligrosas.
6. Mantener contexto conversacional entre turnos.
7. Permitir follow-ups como “muéstrame más”, “los de mayor valor”, “ahora en Bolívar”, “solo 2025”, “en ejecución”.
8. Presentar resultados en un formato asesor, no meramente tabular.
9. Generar sugerencias accionables para explorar el universo contractual.
10. Mantener trazabilidad de bugs, decisiones y cambios mediante tests y bitácoras.

El sistema está actualmente en una etapa de **MVP funcional con endurecimiento semántico y conversacional + primera ruta analítica**. Ya existen parser determinístico, resolución de entidades, constructor SoQL, política de respuesta asesora, seguimiento conversacional, sugerencias, paginación, feedback y suite de tests.

En mayo 2026 se agregó la primera ruta analítica determinística (AQ-001A aggregate_sum) que permite responder consultas del tipo “cuánto se contrató en X” con SUM + COUNT en vez de listar contratos.

---

## 2. Problema que resuelve

SECOP II contiene gran cantidad de información pública, pero su consulta directa suele ser difícil para usuarios no técnicos o para funcionarios que requieren interpretación rápida.

Problemas habituales:

- Los nombres oficiales de entidades no coinciden con la forma natural en que el usuario las menciona.
- Los estados de los contratos/procesos tienen semántica técnica y pueden inducir errores si se interpretan literalmente.
- Las consultas por objeto contractual requieren buscar en texto libre, con sinónimos y variantes morfológicas.
- El usuario conversa por refinamientos, no formula cada consulta completa.
- El portal puede demorar o fallar con consultas pesadas.
- Una búsqueda demasiado amplia puede devolver miles de resultados irrelevantes.
- Los filtros estrictos pueden devolver cero resultados, pero relajarlos automáticamente puede introducir resultados fuera de alcance.

SECOPPAL aborda estos problemas mediante una arquitectura por capas: parser, normalizador semántico, resolver de entidades, constructor de consultas, cliente SECOP, observer estadístico, política de respuesta y memoria conversacional de corto plazo.

---

## 3. Stack técnico

### 3.1 Lenguaje y runtime

- **Python** como lenguaje principal.
- **Burr** para definir el flujo de acciones del pipeline conversacional.
- **Socrata / sodapy** para consultar datasets de datos.gov.co.
- **RapidFuzz** para fuzzy matching de entidades y departamentos.
- **pytest** para pruebas unitarias y E2E.
- **Streamlit** como interfaz frontend local de pruebas.
- **FastAPI / Uvicorn** como backend local de servicio.

### 3.2 Datasets SECOP usados

El sistema distingue dos datasets principales:

| Dataset lógico | Dataset ID | Uso |
|---|---:|---|
| Procesos | `p6dx-8zbt` | Procesos, convocatorias, oportunidades, publicaciones, apertura, estado de procedimiento. |
| Contratos | `jbjy-vk9h` | Contratos registrados, proveedor adjudicado, valor del contrato, fecha de firma, estado contractual. |

### 3.3 Interfaces previstas

- Streamlit para pruebas visuales.
- Backend local con Uvicorn.
- Posible integración futura con Telegram / WhatsApp / Hermes.

---

## 4. Arquitectura general

```text
Usuario
  ↓
SECOPPAL Workflow / Orchestrator
  ↓
QueryRouter
  ↓
FollowupEngine, si hay contexto conversacional
  ↓
EntityResolver
  ↓
SoQLBuilder
  ↓
SecopClient
  ↓
Observer / Suggester
  ↓
ResponsePolicy + Formatter
  ↓
Respuesta asesora al usuario
```

### 4.1 Capas del sistema

| Capa | Componente | Responsabilidad |
|---|---|---|
| Entrada | `orchestrator.py` | Fachada delgada: chain-of-responsibility de handlers. |
| Workflow | `workflow/` | Acciones Burr, handlers de casos especiales, grafo, helpers, persistencia. |
| Parsing | `query_router.py` | Extrae dataset, tema, fecha, valor, estado, ciudad, NIT, modalidad. |
| Follow-up | `followup_engine.py` | Fusiona consulta actual con contexto anterior. |
| Memoria corta | `conversation_store.py` | Guarda historial por `chat_id` y sugerencias. |
| Resolución | `entity_resolver.py` | Resuelve entidades y departamentos mediante gazetteer + fuzzy + LIKE fallback. |
| SQL | `soql_builder.py` | Construye consultas SoQL y agregaciones. |
| Ejecución | `secop_client.py` | Ejecuta consultas contra Socrata/SECOP con retries. |
| Observación | `observer.py` | Calcula distribución, top entidades, valores, modalidades. |
| Sugerencias | `suggester.py` | Genera acciones como ordenar por valor, ver activos, cambiar dataset. |
| Respuesta | `response_policy.py` | Define tono asesor para ambigüedad, cero resultados y resultados claros. |
| Narrativa | `narrator.py` | Genera resumen con LLM, validando cifras monetarias. |
| Formato | `formatter.py` | Convierte resultados en tarjetas/listas para canal. |
| Feedback | `feedback.py` | Guarda trazas y calificaciones del usuario. |

---

## 5. Estructura de archivos principales

```text
app/core/
├── conversation_store.py
├── direct_responses.py
├── entity_resolver.py
├── entity_types.py
├── estado_families.py
├── feedback.py
├── followup_engine.py
├── formatter.py
├── intent_vocabulary.py
├── llm_handler.py
├── narrator.py
├── observer.py
├── orchestrator.py
├── query_frame.py
├── query_router.py
├── response_policy.py
├── secop_client.py
├── soql_builder.py
├── suggester.py
└── workflow/
    ├── actions/
    │   ├── parsing.py
    │   ├── planning.py
    │   ├── execution.py
    │   └── rendering.py
    ├── handlers/
    │   ├── reset.py
    │   ├── pagination.py
    │   ├── suggestion.py
    │   ├── direct_lookup.py
    │   ├── refinement.py
    │   └── normal_flow.py
    ├── graph.py
    ├── helpers.py
    ├── deps.py
    ├── context.py
    ├── persistence.py
    └── results.py
```

### 5.1 `orchestrator.py`

Es la **fachada delgada** del flujo. Antes era un god module de 1549 líneas; ahora es una chain-of-responsibility de 196 líneas.

Responsabilidades actuales:

- Inicializar `WorkflowDeps` (inyección de dependencias).
- Construir `HandlerContext` para cada turno.
- Ejecutar `run_query()` como chain-of-responsibility:
  1. `handle_refinement` (seed_resolved_params)
  2. `handle_reset` (comandos /reset, nueva, limpiar)
  3. `handle_pagination` ("más", "siguiente", números de página)
  4. `handle_suggestion_selection` (selección 1-5 de sugerencias)
  5. `handle_direct_lookup` (ID CO1.XXX.YYY, URL, referencia de entidad)
  6. `handle_normal_flow` (pipeline Burr completo: parse → plan → execute → observe → **rank** → render)
- Mantener shim de backward-compat para tests legacy (re-exports de actions/helpers).

Puntos relevantes:

- La paginación pura no pasa por todo el pipeline — la captura `handle_pagination` antes del Burr graph.
- El follow-up se gestiona dentro de `workflow/actions/parsing.py` vía `apply_context`.
- El guard anti `WHERE 1=1` vive en `workflow/actions/execution.py` (`execute_query`).
- Cada handler vive en `workflow/handlers/` y recibe `(WorkflowDeps, HandlerContext)` — no accede a `self` de `SecopalWorkflow`.

### 5.2 `query_router.py`

Parser determinístico de lenguaje natural.

Extrae:

- Dataset lógico: procesos / contratos.
- Tema contractual (`objeto`).
- Fechas: años, rangos, meses.
- Valores: mínimos, máximos, rangos.
- Modalidad.
- Estado semántico.
- NIT / contratista.
- Ciudad / departamento.
- Entidades mediante rewrite + gazetteer.

Decisión clave: es preferible un parser determinístico auditable antes que depender completamente del LLM.

### 5.3 `followup_engine.py`

Cerebro conversacional del seguimiento, no de todo el sistema.

Responsabilidades:

- Identificar si una frase es refinamiento, cambio de scope, cambio de año, cambio de orden, cambio de dataset, selección de sugerencia o nueva búsqueda.
- Fusionar los parámetros actuales con los anteriores.
- Evitar perder contexto en frases como:
  - “los de mayor valor”
  - “solo 2025”
  - “ahora en Bolívar”
  - “en ejecución”
  - “muéstrame más”

### 5.4 `conversation_store.py`

Memoria transaccional por chat.

Responsabilidades:

- Guardar historial en JSONL.
- Recuperar últimos turnos.
- Guardar sugerencias del último turno.
- Detectar comandos de reset.
- Detectar frases de paginación pura.

No debe ser el cerebro semántico del follow-up. Esa responsabilidad queda en `followup_engine.py`.

### 5.5 `entity_resolver.py`

Resuelve entidades y departamentos.

Estrategia:

1. Gazetteer-first: buscar alias conocidos por substring.
2. Fuzzy matching si no hay match exacto.
3. Fallback LIKE si no puede resolver.

Mejora relevante V3:

- Evita que nombres de departamentos desnudos se capturen como entidades.
- Ejemplo: “en Atlántico” debe ser departamento, no entidad.
- Solo se acepta como entidad si hay calificador como “gobernación de”, “alcaldía de”, “municipio de”, etc.

### 5.6 `soql_builder.py`

Construye consultas SoQL para procesos y contratos.

Responsabilidades:

- Seleccionar campos por dataset.
- Construir `WHERE` con departamento, ciudad, entidad, objeto, valor, estado, modalidad, fecha y contratista.
- Aplicar variantes morfológicas al objeto.
- Construir agregaciones para observer:
  - top entidades
  - estadísticas de valor
  - top modalidades
  - rango de fechas
  - distribución temporal

Guard importante:

- `_build_where()` puede devolver `1=1`, pero `orchestrator.execute_query()` bloquea consultas sin scope/topic/contratista antes de ejecutar.

### 5.7 `estado_families.py`

Capa semántica de estados.

Decisión crítica reciente:

- En SECOPPAL, `Cerrado` **no significa firmado**.
- `Cerrado` se interpreta como gestión/expediente contractual cerrado.
- “Firmado” fuerza dataset contratos, pero no aplica filtro `estado_contrato = Cerrado`.
- “En ejecución / activo / vigente” se interpreta como:

```python
["En ejecución", "Modificado", "Prorrogado"]
```

### 5.8 `suggester.py`

Genera sugerencias accionables según la consulta actual.

Cambio relevante:

- Se reemplazó “Ver solo contratos firmados” por “Ver contratos activos (en ejecución)”.
- Se agregó “Ver contratos cerrados” con semántica separada.

### 5.9 `response_policy.py`

Construye respuestas asesoras en tres caminos:

1. Consulta ambigua.
2. Cero resultados.
3. Consulta clara con resultados.

Principio de producto:

- Para consultas claras: mostrar resultados + interpretación + universo + sugerencias.
- Para consultas ambiguas: pedir aclaración con caminos posibles.
- Para cero resultados: no relajar filtros automáticamente sin avisar.

---

## 6. Flujo principal de consulta

### 6.1 Consulta nueva

Ejemplo:

```text
hola muéstrame contratos de mantenimiento en Bogotá
```

Flujo:

1. `QueryRouter.parse()` detecta:
   - dataset: contratos
   - objeto: mantenimiento
   - departamento: Distrito Capital de Bogotá
2. `EntityResolver` confirma departamento/entidad.
3. `SoQLBuilder` genera SoQL.
4. `SecopClient` ejecuta count + query.
5. `Observer` calcula universo si aplica.
6. `Suggester` propone acciones.
7. `ResponsePolicy` construye respuesta final.
8. `ConversationStore` guarda turno.

### 6.2 Follow-up

Consulta base:

```text
contratos de mantenimiento en Bogotá
```

Follow-up:

```text
los de mayor valor
```

Resultado esperado:

```json
{
  "dataset": "contratos",
  "objeto": ["mantenimiento"],
  "departamento_resolved": "Distrito Capital de Bogotá",
  "ordering_signal": "valor_desc"
}
```

### 6.3 Paginación

Consulta:

```text
muéstrame más
```

Si hay historial:

- Recupera `parsed_params` del último turno.
- Incrementa `offset`.
- Conserva orden anterior.
- Ejecuta directamente.

Si no hay historial:

- Responde pidiendo contexto.

---

## 7. Política semántica de estados SECOP

### 7.1 Principio general

Los estados de SECOP no se deben traducir de forma coloquial sin control jurídico-funcional.

### 7.2 Reglas vigentes

| Frase usuario | Interpretación SECOPPAL | Filtro |
|---|---|---|
| contratos firmados | Dataset contratos, sin filtro de estado | `dataset='contratos'` |
| contratos en ejecución | Contratos activos | `estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')` |
| contratos activos | Contratos activos | `estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')` |
| contratos vigentes | Contratos activos | `estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')` |
| contratos cerrados | Gestión/expediente contractual cerrado | `estado_contrato = 'Cerrado'` |
| contratos terminados | Contratos terminados | `estado_contrato = 'terminado'` |
| contratos cedidos | Contratos cedidos | `estado_contrato = 'cedido'` |
| suspendidos/cancelados | No procede / suspendidos | `estado_contrato IN ('Suspendido', 'Cancelado')` |

### 7.3 Invariante INV-005 — Firmado no es estado

`Firmado/firmados` no es un estado contractual filtrable. Es una propiedad universal del dataset contratos (`jbjy-vk9h`). Todo registro en ese dataset representa un contrato con existencia contractual.

Por tanto:

- `firmados` selecciona `dataset='contratos'`.
- Nunca emite valores en `estado`, `estado_contrato` ni `estado_field`.
- `en ejecución` es un subconjunto operativo de firmados, no su sinónimo.

Verificación empírica (2026-05-15): los valores reales del campo `estado_contrato` en `jbjy-vk9h` son:

```text
En ejecución, Cerrado, Modificado, terminado, Borrador, Aprobado,
Cancelado, enviado Proveedor, cedido, En aprobación, Suspendido,
Prorrogado.
```

No existen `Firmado` ni `Celebrado`.

---

## 8. Guardrails principales

### 8.1 Anti `WHERE 1=1`

El sistema no ejecuta consultas sin al menos un filtro base.

Filtros base válidos:

- Objeto / tema contractual.
- Entidad resuelta o LIKE de entidad.
- Departamento.
- Ciudad.
- Contratista.

No son filtros base suficientes por sí solos:

- Fecha.
- Valor.
- Ordenamiento.

Mensaje recomendado:

```text
Necesito al menos un filtro de entidad, lugar, tema o contratista para buscar. ¿Sobre qué quieres saber?
```

### 8.2 Cero resultados

En follow-ups conversacionales, el sistema no debe relajar automáticamente filtros.

Ejemplo correcto:

```text
No encontré resultados con esos filtros. Puedo intentar:
1. quitar el filtro de fecha
2. ampliar el tema
3. buscar en procesos
```

### 8.3 Timeout por consultas pesadas

Si una consulta con orden por valor sobre universo histórico demora demasiado:

- Reintenta sin `ordering_signal`.
- Cambia a orden por fecha.
- Explica al usuario que cambió la estrategia.

---

## 9. Versionado y bitácora de cambios

> Esta sección debe mantenerse como changelog técnico. Cada cambio importante debe registrar fecha, archivos modificados, bug/causa, solución y validación.

### 2026-05-15 — Auditoría externa: H1 vivo en camino LLM, ADR-006 erosionada

Una auditoría externa identificó que:

- El bug `firmado=Celebrado`, declarado cerrado en el camino determinístico, seguía vivo en `app/core/llm_handler.py` (`SYSTEM_PROMPT` y enum del tool). El camino determinístico estaba correcto; el camino LLM no.
- ADR-006 (no versiones paralelas en `app/core/`) se había erosionado: reaparecieron `.bak`, `.bak2`, `backup_20260514_173654/`, y binarios de ofimática dentro del módulo.

Acciones tomadas:

- Sincronizado `llm_handler.py` con INV-005 reformulada.
- Limpiado `app/core/`; archivos movidos a `archive/cleanup_2026-05-15-audit/`.
- Añadido target `make lint-core` que bloquea reincidencia.
- Tests nuevos: `test_llm_handler_state_policy.py`, `test_estado_families_integrity.py`, `test_soql_count_select_consistency.py`.
- Verificación empírica de estados reales en `jbjy-vk9h` registrada en sección 7.3.

Lección: las invariantes deben tener tests que las defiendan. ADR-006 sin `lint-core` es aspiracional; con `lint-core` es defensiva.

### 2026-05-15 — Limpieza de duplicados core

**Problema:** existían archivos paralelos tipo `v2.py`, `v3.py` dentro de `app/core`, lo cual podía confundir a agentes y humanos.

**Archivos afectados:**

- `soql_builder.py`
- `soql_builder v2.py`
- `query_router.py`
- `query_router v2.py`
- `entity_resolver v2.py`
- `entity_resolver v3.py`
- `orchestrator.py`
- `orchestrator v2.py`

**Decisiones:**

- `soql_builder.py` queda como canónico.
- `query_router.py` queda como canónico.
- `entity_resolver.py` debe contener la lógica de `entity_resolver v3.py`.
- `orchestrator.py` debe contener integración con `followup_engine.detect_and_merge()`.
- Versiones paralelas se archivan fuera de `app/core`.

**Validación esperada:**

```bash
find app/core -name "* v*.py" -print
python3 -m py_compile app/core/*.py
pytest -q
```

### 2026-05-15 — Follow-up engine como política principal de seguimiento

**Problema:** la lógica conversacional estaba repartida entre `conversation_store.py`, `query_frame.py` y `orchestrator.py`.

**Solución:** `followup_engine.py` queda como clasificador/merger canónico para follow-ups.

**Decisión:**

- `QueryFrame` es la representación intermedia oficial entre `params` y `FollowupEngine`. No es legacy: vive en el hot path de `apply_context` como transporte entre `frame_from_params` y `detect_and_merge`. Su eventual eliminación es un refactor futuro, no urgente.
- `ConversationStore` queda como memoria transaccional.
- `FollowupEngine` decide intención conversacional.

### 2026-05-15 — Bug `esten + estado_contrato = Cerrado`

**Consulta base:**

```text
hola muestrame contratos de mantenimiento en bogota
```

**Follow-up:**

```text
muestrame mas contratos que esten firmados o en ejecucion
```

**Bug original:**

- El objeto anterior `mantenimiento` se perdía.
- `esten` se convertía en objeto contractual.
- “firmados o en ejecución” terminaba como `estado_contrato = 'Cerrado'`.
- El sistema confundía `firmado` con `Cerrado`.

**Fixes aplicados:**

1. `query_router.py`: se agregaron stopwords:

```python
"este", "esten", "sea", "sean", "encuentre", "encuentren"
```

2. `estado_families.py`: se separó `con_contrato` en familias más precisas:

```text
contrato_activo
contrato_cerrado
contrato_terminado
contrato_cedido
```

3. `intent_vocabulary.py`: “firmados” fuerza dataset contratos, pero no aplica estado.
4. `soql_builder.py`: si existe lista `estado_contrato`, tiene prioridad sobre `estado` scalar legacy.
5. `suggester.py`: se reemplazó “Ver solo contratos firmados” por “Ver contratos activos (en ejecución)”.
6. Tests actualizados.

**Test E2E nuevo:**

```text
tests/test_e2e_estado_followup_bug.py
```

**Resultado esperado del escenario:**

```json
{
  "dataset": "contratos",
  "departamento_resolved": "Distrito Capital de Bogotá",
  "objeto": ["mantenimiento"],
  "estado_contrato": ["En ejecución", "Modificado", "Prorrogado"]
}
```

**Prohibido:**

```text
LIKE '%esten%'
estado_contrato = 'Cerrado'
Contratos de esten
```

**Validación reportada:**

```text
424/424 tests passed
```

---

## 10. Tests y evaluación de calidad

### 10.1 Suite general

Comando:

```bash
pytest -q
```

Estado actual:

```text
1212 passed, 23 xfailed
```

### 10.2 Eval harness sobre queries reales calificadas

**Problema que resuelve:** La métrica histórica "26% de precisión" mezclaba parser, orden, follow-up y timeouts de red en una sola cifra no accionable. No había forma de saber si un fix mejoraba o empeoraba el sistema sobre las queries que el usuario realmente calificó.

**Solución:** `scripts/eval_rated.py` — harness que corre las 70 queries calificadas reales (desde SQLite `data/secoppal.db`) contra el pipeline y compara contra comportamiento esperado etiquetado en `tests/fixtures/rated_eval_v1.yaml`. Dos tiers:

| Tier | Descripción | Modo | Entradas | Precisión |
|---|---|---|---|---|
| **Parser** | Extracción de params sin follow-up ni red | `--mode parser` (default) | 38 | 38/38 = 100% |
| **Pipeline** | Follow-up + ejecución real contra SECOP | `--mode pipeline --live` | 26 | Pendiente medición |

**Hallazgo clave — re-encuadre de precisión:**

El 26% era end-to-end histórico. Separando tiers:
- **Parser-tier 85%:** De 25 bugs calificados 👎, 19 ya estaban arreglados (rangos compuestos, `filtra`, saludos, `esten`). Nadie lo sabía por falta de medición.
- **Pipeline-tier:** El desplome real está en follow-up poisoning y recencia ("priorizó procesos viejos"). Ahí se concentran los 👎 restantes.

**6 bugs de parser — 5 resueltos, 1 reclasificado (verificación 2026-06-03):**

| ID | Bug | Tipo | Estado |
|---|---|---|---|
| RATED-016 | `oferta` se cuela en objeto | scrub | ✅ Resuelto (stopwords) |
| RATED-040 | `vias` se pierde con "...teusaquillo sobre vias" | scrub/entity | ✅ Resuelto (word boundary) |
| RATED-041 | `oficial` del `(oficial)` de UI se cuela | scrub | ✅ Resuelto (stopwords) |
| RATED-059 | `software` expande a `tecnología` | morfología | ✅ Resuelto (morpho fix) |
| RATED-067 | `o similares` inyecta `valor_max=1.000.000` | parser rango | ✅ Resuelto (regex \b + stopwords) |
| RATED-069 | `El Banco` (municipio) → tema `banco` | gazetteer/LLM | 🔄 Reclasificado pipeline-tier |

RATED-069 no se puede resolver con regex (rompería "Banco Agrario", "Banco de Bogotá", "Banco Mundial"). Es caso canónico para la capa de desambiguación LLM.

**Piso de regresión:** `tests/test_rated_eval.py` — el piso parser-tier no puede bajar de 38/38 ni las 👍 pueden regresar.

**Divergencia SQLite/JSONL:** El dashboard lee de SQLite (70 calificadas). El JSONL (`data/feedback.jsonl`) tiene solo 42. El harness usa SQLite como fuente de verdad. Ver `scripts/eval_rated.py` para detalles.

### 10.3 Corpus de regresión del parser

- `tests/fixtures/query_corpus_v1.yaml` — 88 entradas.
- `scripts/run_query_corpus.py` — runner con modos `parser|workflow|all` y filtros por severidad.
- Convenciones en `docs/QUERY_CORPUS.md`.
- Estado: 89/89 PASS (incluye corpus dialogue 61/61).

### 10.4 Matriz mínima de follow-up

| # | Consulta base | Follow-up | Esperado |
|---:|---|---|---|
| 1 | procesos abiertos de alimentación en Atlántico 2026 | muéstrame más | paginación, conserva filtros |
| 2 | procesos abiertos de alimentación en Atlántico 2026 | los de mayor valor | conserva filtros + orden valor desc |
| 3 | procesos abiertos de alimentación en Atlántico 2026 | solo 2025 | conserva filtros + cambia año |
| 4 | procesos abiertos de alimentación en Atlántico 2026 | ahora en Bolívar | conserva tema + cambia departamento |
| 5 | procesos abiertos de alimentación en Atlántico 2026 | ahora en Medellín | conserva tema + cambia ciudad/scope |
| 6 | contratos de adulto mayor en Barranquilla | ahora procesos abiertos | cambia dataset + conserva tema/scope |
| 7 | procesos de mantenimiento en Cali | contratos en ejecución | cambia dataset + estado activo |
| 8 | consulta con sugerencias | 1 | aplica sugerencia guardada |
| 9 | sin historial | muéstrame más | pide contexto |
| 10 | cualquier búsqueda | reset | limpia historial |

### 10.5 Test crítico de estados

```text
Base:
contratos de mantenimiento en Bogotá

Follow-up:
muéstrame más contratos que estén firmados o en ejecución

Debe:
- heredar mantenimiento
- heredar Bogotá
- excluir esten
- excluir Cerrado
- usar estados activos
```

---

## 11. Comandos operativos útiles

### 11.1 Levantar backend

```bash
uvicorn app.main:app --reload
```

### 11.2 Levantar Streamlit

```bash
streamlit run streamlit_app.py
```

### 11.3 Compilar core

```bash
python3 -m py_compile app/core/*.py
```

### 11.4 Ejecutar tests

```bash
pytest -q
```

### 11.5 Buscar archivos duplicados accidentales

```bash
find app/core -name "* v*.py" -print
find app/core -name "*copy*.py" -print
find app/core -name "*backup*.py" -print
```

### 11.6 Verificar integración follow-up

```bash
grep -R "detect_and_merge" -n app/core/orchestrator.py app/core/followup_engine.py
grep -R "classify_turn\|merge_query_frames" -n app/core/orchestrator.py
```

---

## 12. Riesgos conocidos y causas raíz estructurales

### 12.1 Riesgos operativos

| Riesgo | Impacto | Mitigación |
|---|---|---|---|
| Confundir estados SECOP | Respuestas jurídicamente equivocadas | Política de estados documentada + tests E2E |
| Tokens basura en objeto | Consultas irrelevantes con `LIKE '%basura%'` | Stopwords + dialogue_policy (reglas P6/P7) |
| Follow-up hereda parcialmente | Resultados fuera de intención | `followup_engine` + matriz regresión |
| Versiones paralelas en `app/core` | Agente edita archivo equivocado | `make lint-core` + archive/ |
| Consulta global accidental | Miles de resultados irrelevantes | Guard anti `WHERE 1=1` en builder + orchestrator |
| Relajación automática excesiva | Resultados fuera de alcance | En follow-up, sugerir opciones antes de relajar |
| Timeout SECOP | Mala UX | Retry controlado + count-lazy (ADR-008) |
| LLM alucina narrativa | Lectura rápida inventa cifras | Validación de grounding monetario en `narrator.py` |

### 12.2 Causas raíz estructurales (revisión integral junio 2026)

La revisión integral de arquitectura identificó 5 causas raíz que explican por qué los bugs del Bucket B (semánticos) siguen apareciendo a pesar de 1212 tests en verde:

**Causa 1 — El gate `_needs_llm` apaga el LLM de facto.** `query_router.py` solo dispara el LLM en 2 casos: (a) lugar no resuelto que choca con objeto, y (b) palabra tipo-entidad en objeto. Resultado: 17 de 2437 queries usaron LLM (0.7%). El resto va por heurística pura — incluyendo `banco magdalena`, `aires acondicionados`, `o similares`, `celebraron`. El LLM (DeepSeek, JSON estructurado) está construido y conectado, pero gateado detrás de un `if` que casi nunca es verdadero.

**Causa 2 — `QueryFrame.raw_params` con merge ciego (frame leak).** Las políticas de merge en `followup_engine.py` hacen `raw_params={**prev.raw_params, **curr.raw_params}`. El QueryFrame tiene campos tipados (`scope`, `topic`, `dataset`) que las políticas cuidan, pero `raw_params` es un canal paralelo no tipado que arrastra TODO (`entidad_ecosystem`, `intent_type`, `estado_family`) por la puerta de atrás. Resultado: `WHERE departamento='Cesar' AND nombre_entidad LIKE '%CUNDINAMARCA%'` — lógicamente imposible. Dos representaciones del estado sincronizadas a mano = el bug.

**Causa 3 — El objeto es bag-of-tokens con AND por defecto.** `_extract_object_terms` parte el texto en tokens, filtra stopwords y los une con AND. `aires acondicionados` → `%aires% AND %acondicionados%` → 0 resultados. No hay noción de frase ni relevancia. `test_multi_and_demotion.py` congela este comportamiento como correcto (ver ADR-014 para el refinamiento parcial con OR merge).

**Causa 4 — Sin capa de relevancia/sanity semántico.** `QueryPlan` valida estructura (¿falta slot?, ¿WHERE 1=1?), no semántica. Nada pregunta "¿`banco` aquí es lugar o tema?", "¿`valor_max=1.000.000` para 'parques' tiene sentido?". El propio `SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md` (mayo 18) ya lo diagnosticó: "Relevance layer: No existe."

**Causa 5 — Los tests prueban la implementación, no el producto.** 1212 tests verdes con 26% de precisión real. Eso solo es posible si los tests no miden lo que importa. Ejemplos: `test_module_size_limits.py` (disciplina de proceso, no corrección), `test_multi_and_demotion.py` (congela comportamiento defectuoso), decenas de tests que validan rutas internas con params sintéticos. El eval harness (`scripts/eval_rated.py`) cierra esta brecha al medir sobre queries reales calificadas.

---

## 13. Decisiones de diseño vigentes

El registro completo de decisiones está en [docs/decisions_log.md](../decisions_log.md) (formato ADR, numeración continua ADR-001 a ADR-023).

Esta sección antes contenía un mirror (D1–D21) que se eliminó para evitar drift entre las dos numeraciones. El log de decisiones es ahora la fuente única de verdad.

---

## 14. Roadmap

### Fase A — P0 hardening post-auditoría

**Estado:** cerrado en 2026-05-15.

- INV-005 defendida en código y tests.
- `app/core/` limpio y protegido por `make lint-core`.
- Tests de regresión para política de estados y coherencia `COUNT`/`SELECT`.
- Documento técnico actualizado con verificación empírica de `jbjy-vk9h`.

### Fase B — OBS-001 logger semántico / telemetría operacional

**Estado:** pendiente.

- Añadir `trace_id` a logs y JSONL.
- Registrar tiempos por etapa: parse, resolve, SoQL build, count, query, observe, narrate, total.
- Medir tasa LLM, tasa de degradación, timeout/retry y distribución de `intent_type` / `followup_intent_type`.
- Crear `scripts/analyze_traces.py` para rollups semanales.

### Fase C — Frame leak: sellar la fuga del follow-up (Causa 2)

**Estado:** próximo foco. Derivado de la revisión integral junio 2026.

- Eliminar el merge ciego `{**prev.raw_params, **curr.raw_params}` en `followup_engine.py`.
- Derivar `raw_params` de campos tipados o whitelistear claves que sobreviven a cada intent.
- Extender eval harness a tier pipeline (`--live`, multi-turno) para medir el fix.
- Bugs objetivo: DOGFOOD-FOLLOWUP-007 (Cundinamarca→Cesar poisoning), RATED-043/050/056/058/062.

### Fase D — Shadow LLM para follow-up

**Estado:** diseño, no implementado.

- Ejecutar LLM en paralelo (async, no bloquea) sobre follow-ups.
- Comparar output LLM vs determinístico → medir divergencia.
- Acumular evidencia antes de flipear el default.
- Columna `shadow_llm_params` en trazas para almacenar.

### Fase E — RATED-069: desambiguación banco=municipio (requiere LLM)

**Estado:** único bug parser-tier remanente, reclasificado pipeline-tier.

- `El Banco` es municipio de Magdalena, pero "banco" es polisémico (Banco Agrario, Banco de Bogotá, Banco Mundial, banco de alimentos).
- Un fix regex `del banco`→ciudad rompe toda esa clase.
- Caso canónico para shadow LLM: desambiguar entidad geográfica vs tema contractual.
- Los otros 5 bugs parser (RATED-016/040/041/059/067) fueron resueltos silenciosamente por sprints mayo 2026.

### Fase F — Relevancia y ranking

Después del follow-up:

- Mejorar ranking por relevancia textual.
- Separar coincidencia directa vs coincidencia probable.
- Evitar que resultados de baja pertinencia aparezcan arriba solo por fecha/valor.

### Fase G — Limpieza imports legacy

- Revisar imports no usados en `orchestrator.py`.
- Migrar 3 tests legacy del shim.
- Marcar `query_frame.classify_turn()` como legacy si ya no opera en producción.

### Completado recientemente

- **ADR-023 Orchestrator Split:** 1549 → 196 LOC, workflow/ extraído (2026-05-27).
- **ADR-022 Chips contextuales adaptativos:** thresholds por percentil, sidebar sticky (2026-05-26).
- **ADR-020 Entity Ecosystem Bridge:** ICBF + SENA fuera del gazetteer (2026-05-26).
- **ADR-017 Dogfooding UX fixes:** URLs, fechas, refinamiento, rangos, CASE WHEN (2026-05-25).

---

## 15. Glosario

| Término | Definición |
|---|---|
| SECOPPAL | Asistente conversacional para consulta y análisis SECOP. |
| Dataset procesos | Dataset de procesos/convocatorias SECOP II. |
| Dataset contratos | Dataset de contratos registrados SECOP II. |
| Follow-up | Consulta que depende del turno anterior. |
| Scope | Alcance territorial o entidad: ciudad, departamento, entidad. |
| Topic / objeto | Tema contractual buscado en objeto/descripción. |
| Modifier | Fecha, valor, estado, orden, modalidad, contratista. |
| Estado family | Agrupación semántica de estados SECOP. |
| Guard anti WHERE 1=1 | Protección contra consultas sin filtros reales. |
| Advisor response | Política de respuesta que interpreta y recomienda, no solo lista. |

---

## 16. Apéndice A: Hitos históricos (2026-05-15 a 2026-06-04)

### 2026-06-04 — Fechas relativas semana/día + Entidad robusta + Firmado fix + Paginación A-buffer

**Fechas relativas (ADR-025, commit `4ac717c`):** Extendida la capa temporal de DialoguePolicy + QueryPlan para soportar expresiones de semana/día: "hoy", "ayer", "esta semana", "la semana pasada", "últimos N días". Nueva señal `temporal_range` + regla P5b (espejo de P5). 15 tests. +7 tokens a STOPWORDS. Sin tocar P5/temporal_month.

**Entidad robusta a trailing tokens (commit `91a7e61`):** Shrink-fallback en `rewrite_entity()` de `entity_types.py`. Cuando la captura completa no resuelve en el gazetteer, se reintenta con spans progresivamente más cortos (prefijo válido más largo). Robusto a CUALQUIER token trailing (temporal, tema, basura). 10 tests. + STOPWORDS: `mes, meses, hecho, hizo, hace, hacen, haciendo, gastado`.

**Firmado fix (ADR-026, commit `a745ac8`):** Corregida la semántica de "firmado" que mapeaba erróneamente a `estado=Aprobado` (tiraba ~97% del recall). Validado contra SECOP real: 8 estados tienen ~100% de fecha_de_firma, 4 tienen <0.5%. "Firmado" = `fecha_de_firma IS NOT NULL`, no un valor de estado. Quitado el synonym falso de `estados.py`. 15 tests. + `firmada/firmadas` a STOPWORDS.

**Paginación A-buffer (commit `994337b`):** Eliminados duplicados entre páginas de resultados con buffer de IDs ya vistos.

### 2026-06-03 — Revisión integral de arquitectura + eval harness + verificación de bugs

**Revisión integral:** Se identificaron 5 causas raíz estructurales que explican el "parche infinito" (documentadas en §12.2). Hallazgo principal: el parser está en 100% de precisión (38/38 sobre queries reales calificadas).

**Eval harness:** Creado `scripts/eval_rated.py` + `tests/fixtures/rated_eval_v1.yaml` (65 entradas etiquetadas) + `tests/test_rated_eval.py` (piso de regresión). Corre las 70 queries calificadas desde SQLite. Dos tiers: parser (100%) y pipeline (pendiente --live).

**Verificación de bugs:** 5 de 6 bugs parser-tier ya estaban resueltos por sprints mayo 2026 (RATED-016/040/041/059/067). RATED-069 (`El Banco` municipio) reclasificado a pipeline-tier — requiere desambiguación LLM. Parser-tier: 38/38 = 100%.

**Documento técnico actualizado:** Consolidación de secciones duplicadas, header con métricas reales, §12 ampliado con causas raíz, §14 roadmap actualizado con foco en frame leak + shadow LLM.

### 2026-05-27 — Orchestrator Split (ADR-023)

`orchestrator.py`: 1549 → 196 LOC (−87%). Refactor en 8 pasos atómicos con golden tests. Workflow extraído a `workflow/` (actions, handlers, graph, helpers, deps, context, persistence, results). Suite: 1212 passed, 23 xfailed. Corpus: 89/89 PASS. Ver ADR-023 y §5.1 para estructura resultante.

### 2026-05-27 — Dogfooding: 3 bugs documentados

DOGFOOD-FOLLOWUP-007 (follow-up poisoning Cundinamarca→Cesar), DOGFOOD-RANGE-007 (rangos "más de X y menos de Y"), DOGFOOD-MORPHO-007 (software→tecnología). RANGE-007 cerrado en `ecf94de`. Los otros dos vivos como Causa 2 y RATED-059 respectivamente.

### 2026-05-26 — Entity Ecosystems: ICBF + SENA (ADR-018/019/020)

ICBF (32 regionales, 1:1 por depto) y SENA (89 entidades, 7 tipos, 616K volumen) como ecosistemas institucionales fuera del gazetteer. `entity_ecosystem_router.py` como capa independiente post-parser, pre-resolver. Patrón `contains_all` para SENA multi-entidad. Corpus: 89/89 PASS. Suite ecosystem: 205 passed, 13 xfailed.

### 2026-05-25 — Dogfooding UX fixes (ADR-017)

6 fixes en sesión de trabajo: URLs CO1.REQ vs CO1.NTC (URL-005), fechas con cascada de fallback (DATE-002), refinement buttons funcionales (REFINE-004), rangos "entre X y Y" con herencia de unidad (RANGE-005), "celebrar" como action verb (ACTION-005B), eliminación de CASE WHEN en ORDER BY (SOQL-006). 945 passed, 10 xfailed.

### 2026-05-24 — Sprint 6 Fase 5: smoke gate (ADR-013/014)

ENTITY-MULTI-REGIONAL-002: `unaccent()` obligatorio en todas las cláusulas LIKE de entidad. MULTI-AND-DEMOTION: OR merge para 3+ rare terms en objeto. Suite: 830 passed. Ambos validados contra API real.

### 2026-05-18 — Robustecimiento sistémico (en pausa)

Plan aprobado en `docs/SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md`. Baseline M0: 579 passed, corpus 88/88. Bloqueado por falta de queries humanas para `dialogue_policy_corpus.yaml`. Ver ADR-006.

### 2026-05-18 — Reconciliación main + cierre CORPUS-002-HIGH

Main reconciliado desde worktree vía cherry-picks quirúrgicos. Corpus: 88/88 PASS. Suite: 579 passed, 3 xfailed. Torture matrix: 12/12. Pendiente: DEMO-BLOCKERS-001.

### 2026-05-17 — Query Corpus V1 + analytical queries (AQ-001A, ADR-005)

Corpus de 88 queries vivas. Ruta analítica determinística (`aggregate_sum`). `detect_value_anomaly()` para valores atípicos en fuente (Luruaco $250MM vs $250B). Bidder intent: "para presentarme", "quiero vender X". Suite: 596 passed, 1 xfailed.

### 2026-05-16 — Auditoría P0 cerrada

4 bugs críticos: `/chat` sin chat_id, `degrade_query` sin recalcular count, regex narrator con falsos positivos, `_is_signed_query` borraba estados reales. Suite: 466 passed. Regla semántica: "firmados" solo fuerza dataset contratos, nunca borra filtros de estado (ADR-008).

### 2026-05-15 — Documento técnico creado

Primera versión de este documento. Auditoría H1: bug `firmado=Celebrado` vivo en camino LLM. ADR-006 erosionada (archivos `.bak` en `app/core/`). Limpieza + `make lint-core` + tests de integridad.

### 2026-05-17 — Oportunidades SECOP: estado, dedupe y presentación

Se ajustó la política de oportunidades para evitar doble filtrado de estados y mejorar la salida visible de procesos. El caso guía fue una oportunidad de mantenimiento correctivo/preventivo de parque automotor en Norte de Santander que no aparecía por combinación restrictiva de estados.

Validación:
- tests Pamplonita: 4/4
- tests B1-B7: 19/19
- torture matrix verde
- feedback.jsonl intacto

### 2026-05-17 — OPP-003: intent_type explícito + G5 timeout específico

Se elevó `intent_type=opportunity_search` a primera clase:
- Emisión desde intent_vocabulary (bidder intent) y query_router.
- Round-trip en QueryFrame.
- Tests E2E con chat_id.
- Timeout UX específico para oportunidades (mensaje + sugerencias de lugar/ventana).

Validación: 567 passed, 0 fallos, torture matrix verde.

### 2026-05-17 — Bidder intent y rangos monetarios reales

Se corrigieron patrones detectados en pruebas manuales:
- rangos monetarios compactos (VALUE-RANGE-001);
- “para poder presentarme” (BIDDER-INTENT-002);
- “quiero vender/ofrezco/soy proveedor de X” (BIDDER-INTENT-003).

Estos casos fortalecen `opportunity_search` y el smoke test de producto.


### 2026-05-17 — Query Corpus V1 + LLM-OPP-001 + PTAR-VARIANTS-001

- Se agrego corpus vivo de 99 queries para depuracion sistematica (docs/QUERY_CORPUS.md).
- Se implemento `enforce_bidder_opportunity_policy` como modulo independiente.
- Variantes PTAR/STAR/aguas residuales colapsadas a raiz unica.
- 22 corpus FAIL conocidos pasan a backlog en docs/CORPUS_BACKLOG.md.
- Pendiente CORPUS-002 para cerrar los 5 critical FAIL.

### 2026-05-17 — VALUE-SANITY-002A: valores atípicos en datos abiertos

Se detectó que algunos registros del dataset Contratos (`jbjy-vk9h`) traen `valor_del_contrato` inflado frente a `valor_facturado`. Caso guía: Luruaco/CD-1028-2025, donde `valor_del_contrato` aparece como $250.000.000.000 y `valor_facturado` como $250.000.000.

Decisión:
- No modificar silenciosamente el valor fuente.
- Marcar el resultado como valor atípico con `detect_value_anomaly()`.
- Mostrar referencia de `valor_facturado` cuando exista.
- Advertir si el mayor valor mostrado tiene inconsistencia de fuente.

Validación:
- 12 tests nuevos.
- Suite: 596 passed, 1 xfailed.
- Torture matrix verde.

### 2026-05-18 — CORPUS-001-B + CORPUS-002 — corpus de regresión sobre el parser

Se agregó un corpus de queries reales (`tests/fixtures/query_corpus_v1.yaml`,
88 entradas) y un runner (`scripts/run_query_corpus.py`) con tres modos
(`--mode parser|workflow|all`) y filtros por `--severity`, `--id` y
`--limit`. Convenciones en `docs/QUERY_CORPUS.md`.

Sobre ese corpus se cerraron los 4 críticos iniciales:
- CORPUS-R005: `convocadas` ahora activa `oferta_abierta` vía
  `estado_families.NATURAL_TO_FAMILY`.
- CORPUS-F006 / CORPUS-OC001: `estado_family=oferta_abierta` sobre
  `dataset=procesos` se promueve a `intent_type=opportunity_search`.
- CORPUS-R018: el heurístico "más + filtro nuevo → refine_filter" ya no
  degrada follow-ups que introducen un topic completamente nuevo
  (helper `_topics_overlap` en `followup_engine.py`).

Pase tras los fixes: 82/88; corpus crítico 5/5.

### 2026-05-18 — SAFE-SOQL-001 — Defensa interna anti consultas globales

SoQLBuilder ya no emite `WHERE 1=1` salvo `allow_global=True` explícito.
El guard del orchestrator sigue siendo la primera defensa; el builder
queda como segunda defensa.

Un filtro "substancial" es scope, topic, familia de estado o modalidad.
fecha/valor/orden por sí solos no califican. Tests en
`tests/test_safe_soql.py` cubren los 6 escenarios pedidos más opt-in y
filtros sustanciales individuales. `observer.py` opta por
`allow_global=True` porque opera sobre un universo ya validado.

Validación: 534 passed, corpus crítico 5/5, feedback.jsonl intacto.

### 2026-05-18 — Reconciliación main + cierre CORPUS-002-HIGH

Se reconcilió main desde el worktree `claude/zealous-yalow-296fb6` vía
rama de rescate `rescue/dirty-secoppal-2026-05-18` y cherry-picks
quirúrgicos. Resultado final:

- **VALUE-SANITY-002A:** `detect_value_anomaly()` marca valores atípicos
  en dataset Contratos sin alterar el dato fuente. ADR-005 registrada.
- **LLM-OPP-VERIFY-001:** `opportunity_policy.py` reintroducido como
  enforcement post-LLM para `intent_type=opportunity_search` en queries
  de proponente.
- **PTAR-VARIANTS-001:** 7 variantes PTAR/STAR/aguas residuales colapsadas
  a raíz `tratamiento_aguas_residuales`.
- **TEST-HYGIENE-001:** 1 xpass eliminado, xfail actualizados.
- **CORPUS-003A:** V005 ("o más") y V007 ("mil millones") cerrados.
- **CORPUS-003B:** OC004/OC005 (bidder intent indirecto) cerrados.
- **CORPUS-003C:** R003 (`limpiar→limpieza`) cerrado.
- **AN003:** cherry-pick para analytical followup "cuánto suma".

**Validación final:**
- Suite: 579 passed, 3 xfailed.
- Corpus: 88/88 PASS, 0 FAIL.
- Torture matrix: 12/12 invariantes.
- feedback.jsonl intacto.

**Pendiente registrado:** DEMO-BLOCKERS-001 — 8 tareas UX/estado/scope (COMMAND-SLOT,
OPP-UX, OPP-STATE, OPP-SUGGEST, OPP-RELEVANCE, OPP-TIMEOUT-001,
STATE-PRIORITY-001, OPP-SCOPE-001). Prioridad HIGH.

### 2026-05-18 — VALUE-TYPO-001 registrado en backlog

Typo "milloones" → "millones" debe normalizarse. Si valor_min > valor_max
(1B > 5M) no ejecutar consulta, pedir aclaración. "milloones" no debe
entrar como objeto contractual ni generar SoQL con LIKE '%milloones%'.

### 2026-05-24 — Sprint 6 Fase 5 smoke gate: 3 bugs fixados

**MORPHO-VERIFY (29ac24b):** MORPHO-EXPANSION-INCONSISTENT-001 cerrado.
Verificado que ambas rutas (heuristic_only y heuristic_plus_llm) convergen
en `_object_term_conditions()` que usa ROOT_TO_VARIANTS. Falso positivo.
3 tests nuevos en test_morpho_expansion_consistency.py.

**ENTITY-MULTI-REGIONAL-002 (b39a138):** Descubierto en smoke D-bucket.
LIKE nombre_entidad sin unaccent() + entidad_resolved sintética con tildes
inexistente en SECOP. Fix: `_accent_fold_like` en todas las cláusulas
entidad + `entidad_like` con nombre canónico corto. resolve_regional()
ahora retorna tuple (regional, canonical). ADR-013.

**MULTI-AND-DEMOTION (dd0b2bf):** Descubierto en smoke B-bucket (0/10).
4+ rare terms en AND colapsaban queries de frases técnicas. Fix: OR merge
cuando rare_terms ≥3. BM25 reordena post-query. ADR-014.

**Smoke gate Fase 5:** 6 intentos diurnos fallidos por API saturada
(10-30s/query en horario 2-4pm Colombia). Cronjob cdb0c2867f32 programado
lunes 5am (10:00 UTC) con no_agent=true y scripts/smoke_burst.py.
Target ≥45/50. API solo responde <2s en madrugada.

**Validación:** Suite 830/0. Ambos fixes validados con queries individuales
contra API real (0→50 results). CORPUS_BACKLOG actualizado (1 bug vivo).
decisions_log: ADR-013, ADR-014.

### 2026-05-25 — Dogfooding UX fixes (A-E) + CASE WHEN BadRequest

Dogfooding con query real "procesos abiertos hoy en Cundinamarca por mas de
1000 millones" reveló 5 bugs críticos de UX. Fixes aplicados en sesión de
trabajo con tests 945 passed.

**Fix A — Refinement buttons (DOGFOOD-REFINE-004):**
- `orchestrator.py:_run_refinement()` ejecuta query refinada vía bypass del
  pipeline SoQL (no re-parsea query natural).
- `streamlit_app.py` consume `_pending_refinement` del session_state y pasa
  `seed_resolved_params` a `run_query()`.

**Fix B — URLs por tipo de proceso (DOGFOOD-URL-005):**
- `formatter.py:_safe_url()` + `secop_client.py:_normalize_row()` detectan
  prefijo CO1.REQ.X vs CO1.NTC.X.
- REQ → "" (pre-publicación, SECOP devuelve "argument out of range").
- NTC → reconstruir `OpportunityDetail/Index?noticeUID=`.
- UI: "Vista pública no disponible (proceso pre-publicación)".

**Fix C — Fecha fallback cascada (DOGFOOD-DATE-002):**
- `formatter.py:_best_date_for_row()` busca: publicacion_del → publicacion_fase_3
  → recepcion_respuestas → apertura_efectiva.
- Cada fecha trae etiqueta indicando fuente.
- Todas null → "Pendiente de publicación".

**Fix D — Rango "entre X y Y" (DOGFOOD-RANGE-005):**
- `query_router.py:_extract_amounts()` hereda unidad atómica vs compuesta según
  si el lado con unidad tiene dígito.
- Ej: "entre 500 y mil millones" → min=500M, max=1000M.

**Fix E — Header desactualizado (colateral):**
- Con refinement bypass, el header se reconstruye desde params nuevos en cada
  turn. Auto-resuelto.

**Fix estructural — CASE WHEN en ORDER BY (DOGFOOD-SOQL-006):**
- `CASE WHEN estado_contrato IN (...) THEN 0 ELSE 1 END` en jbjy-vk9h causaba
  400 Bad Request "Unexpected character" (parser interno Socrata no soporta
  strings acentuados en CASE).
- Fix: eliminar CASE WHEN del ordering SoQL. Boost "activos primero" movido a
  post-procesamiento Python (`formatter._reorder_active_first`).
- Impacto colateral: timeouts sistemáticos en contratos atribuidos a "API lenta"
  desaparecieron (el CASE WHEN era la causa real, no la red).

**Tests actualizados:** test_url_repair.py reescrito para nueva política URL.
**Validación:** 945 passed, 10 xfailed.
**Documentación:** ADR-017, DOGFOOD_2026-05-25.md, funnel-full-ux-fixes-2026-05-25.md.
**Pendientes post-dogfooding:** F (trace vacío follow-up), G (gramática "5 resultados").

### 2026-05-27 — Orchestrator Split: de god module (1549 LOC) a fachada delgada (196 LOC)

**Problema:** `app/core/orchestrator.py` concentraba 1549 líneas con acciones Burr, grafo, handlers de casos especiales, flujo normal, persistencia y helpers. Cada feature nueva tocaba orchestrator.

**Solución:** Refactor en 8 pasos atómicos con golden tests (9 contratos de comportamiento observable) como red de seguridad:

1. **Paso 0:** Golden safety net (9 tests de contrato para `run_query()`).
2. **Paso 1:** Extraer helpers puros a `workflow/helpers.py`.
3. **Paso 2:** Introducir `WorkflowDeps` + `HandlerContext` (inyección + contexto inmutable).
4. **Paso 3:** Staging de `workflow/results.py` + `workflow/persistence.py`.
5. **Paso 4:** Mover grafo Burr a `workflow/graph.py`.
6. **Paso 5:** Extraer 12 acciones Burr a `workflow/actions/` (parsing, planning, execution, rendering).
7. **Paso 6:** Extraer handler de refinamiento a `workflow/handlers/refinement.py`.
8. **Paso 7:** Extraer 5 handlers (reset, pagination, suggestion, direct_lookup, normal_flow) y colapsar `run_query()` a chain-of-responsibility.

**Estructura resultante:**
- `orchestrator.py`: 196 LOC (fachada — init + run_query + shim backward-compat).
- `workflow/actions/`: 656 LOC en 4 módulos (parsing, planning, execution, rendering).
- `workflow/handlers/`: 945 LOC en 6 módulos (reset, pagination, suggestion, direct_lookup, refinement, normal_flow).
- `workflow/graph.py`: 148 LOC (ApplicationBuilder Burr).
- `workflow/helpers.py`: 177 LOC (helpers puros).
- `workflow/deps.py`: 56 LOC (WorkflowDeps).
- `workflow/context.py`: 42 LOC (HandlerContext).
- `workflow/persistence.py`: 87 LOC (persist_trace_and_turn, extract_result_ids).
- `workflow/results.py`: 211 LOC (empty_result, tabular_result, CANONICAL_KEYS).

**Invariantes arquitectónicas (4 tests de lock-in):**
- `graph.py` NO importa de `orchestrator.py`.
- `workflow/actions/*.py` NO importan de `orchestrator.py`.
- `workflow/handlers/*.py` NO importan de `orchestrator.py`.
- Los handlers solo reciben `(WorkflowDeps, HandlerContext)` — no `self` de `SecopalWorkflow`.

**Deuda consciente:**
- 3 tests legacy importan actions/helpers desde orchestrator (shim temporal con `TODO(refactor)`).
- `persist_trace_and_turn` no se adoptó en `normal_flow` ni `suggestion` (requería extender firma con `suggestions=`).
- `tabular_result` no se adoptó en el camino exitoso de `suggestion` (quirk legacy Burr-internal vs canónico).

**Validación:**
- Golden: 9/9 PASS
- Architectural lock-in: 4/4 PASS
- Suite: 1212 passed, 23 xfailed
- Corpus: 89/89 PASS
- lint-core: limpio

**Documentación:** ADR-023.

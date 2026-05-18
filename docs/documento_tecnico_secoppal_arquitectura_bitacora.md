# SECOPPAL — Documento Técnico de Arquitectura, Stack y Bitácora de Cambios

**Fecha de creación:** 2026-05-15  
**Estado:** Borrador técnico V0.1  
**Sistema:** SECOPPAL — asistente conversacional para consulta y análisis de contratación pública SECOP II  
**Propósito del documento:** dejar una descripción comprensible para un tercero técnico o funcional sobre qué hace el sistema, cómo está construido, qué archivos lo componen, qué decisiones de arquitectura se han tomado, qué bugs relevantes se han detectado y cuál es el estado actual del proyecto.

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
| Entrada | `orchestrator.py` | Dirige el flujo completo con Burr. |
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
└── suggester.py
```

### 5.1 `orchestrator.py`

Es el director general del flujo.

Responsabilidades:

- Detecta comandos de reset.
- Detecta paginación directa.
- Ejecuta sugerencias numéricas `1`, `2`, `3`.
- Construye pipeline Burr.
- Coordina parseo, resolución, query, ejecución, observación y formato.
- Persiste el turno conversacional.
- Llama a `build_advisor_response()` para construir la respuesta final.

Puntos relevantes:

- La paginación pura no pasa por todo el pipeline.
- El follow-up se gestiona mediante `followup_engine.detect_and_merge()`.
- El guard anti `WHERE 1=1` evita consultas sin filtro base.

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

## 10. Tests y matriz de regresión

### 10.1 Suite general

Comando:

```bash
pytest -q
```

Estado reportado al cierre de la corrección de estados:

```text
424/424 tests passed
```

### 10.2 Matriz mínima de follow-up

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

### 10.3 Test crítico de estados

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

## 12. Riesgos conocidos

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Confundir estados SECOP | Respuestas jurídicamente equivocadas | Política de estados documentada + tests E2E |
| Tokens basura en objeto | Consultas irrelevantes con `LIKE '%basura%'` | Stopwords + tests de encabezado |
| Follow-up hereda parcialmente | Resultados fuera de intención | `followup_engine` + matriz regresión |
| Versiones paralelas en `app/core` | Agente edita archivo equivocado | Archivar versiones fuera de core |
| Consulta global accidental | Miles de resultados irrelevantes | Guard anti `WHERE 1=1` |
| Relajación automática excesiva | Resultados fuera de alcance | En follow-up, sugerir opciones antes de relajar |
| Timeout SECOP | Mala UX | Retry controlado + aviso de estrategia |
| LLM alucina narrativa | Lectura rápida inventa cifras | Validación de grounding monetario en `narrator.py` |

---

## 13. Decisiones de diseño vigentes

Esta sección consolida el `decisions_log.md` existente dentro del documento técnico principal. La idea no es perder el detalle histórico, sino darle una ubicación clara para que tanto el usuario como el agente puedan recuperar el porqué de cada decisión.

### D1 — El LLM nunca genera SQL/SoQL

El LLM no debe construir consultas SoQL directamente. El LLM, cuando se use, solo puede ayudar a extraer parámetros: ciudad, entidad, objeto, rango de valor, fecha o intención.

**Razón:** evita inyección, reduce alucinación, mantiene trazabilidad y permite testear la construcción de consultas de forma determinística.

**Archivo principal:** `soql_builder.py`

### D2 — QueryRouter heurístico primero, LLM como fallback

El sistema intenta resolver la consulta con regex, diccionarios, gazetteer y reglas locales. El LLM se reserva para ambigüedad o señales insuficientes.

**Razón:** latencia, costo y auditabilidad. La mayoría de consultas deben resolverse localmente en milisegundos.

**Archivo principal:** `query_router.py`

### D3 — EntityResolver V3: scan + fuzzy + LIKE con protección de departamentos

La versión V3 rechaza nombres de departamento sin calificador cuando aparecen como posible entidad.

Ejemplos:

```text
"en Atlántico"             → departamento
"gobernación del Atlántico" → entidad
"alcaldía de Barranquilla"  → entidad
```

**Razón:** evitar colisiones entre entidad y departamento.

**Archivo principal:** `entity_resolver.py`

### D4 — OR semántico en objeto: `X o Y`

El parser reconoce `X o Y` como grupo OR dentro de `objeto`.

Nuevo shape:

```python
objeto: list[str | list[str]]
```

Donde:

```text
str       → término obligatorio, AND con otros términos
group list → alternativas, OR entre términos
```

Ejemplo:

```text
"canchas o parques en Atlántico"
```

Debe interpretarse como contratos/procesos de canchas **o** parques, no como documentos que contengan ambos términos.

**Archivos principales:** `query_router.py`, `soql_builder.py`

### D5 — Acrónimos de programas como vocabulario contractual

Acrónimos como `PAE`, `ICBF`, `PAEF`, `SGP`, `OCAD`, `PDET` y `SAT` se tratan como vocabulario semántico relevante.

Decisión específica:

```text
PAE → alimentacion_escolar
```

Pero `pae` no se emite como variante LIKE para evitar falsos positivos con apellidos/lugares como Páez.

**Archivo principal:** `morphological_variants.py`

### D6 — Verbos de acción del usuario no son objeto contractual

Términos como `licitar`, `presentarse`, `postular`, `ofertar`, `participar`, `competir`, `aplicar` y `concursar` son intención del usuario, no objeto contractual.

**Razón:** el usuario describe qué quiere hacer, no necesariamente el contenido del contrato.

**Archivo principal:** `query_router.py`

### D7 — Cultura y variantes normalizadas

`cultura`, `cultural`, `culturales`, `artístico`, `artística` y `patrimonio cultural` normalizan a raíz común `cultura`.

**Razón:** reducir ruido semántico y mejorar recall sin introducir ranking LLM.

### D8 — En Streamlit no duplicar resultados en texto

Cuando Streamlit ya renderiza filas como tarjetas/tabla, la respuesta textual no debe repetir una lista compacta de los mismos resultados.

**Razón:** evita ruido visual y duplicación.

**Archivo principal:** `response_policy.py` / `formatter.py`

### D9 — `objeto` en merge legacy: replace en MVP

En el merge conversacional legacy, `objeto` usa `replace`, no aditivo.

**Razón:** simplicidad. El caso “y también X” requiere una intención específica de adición.

**Defer:** versión futura: aditivo si la query empieza por “y también”, “además”, “incluye también”.

### D10 — Dataset se conserva salvo mención explícita

Si el usuario venía consultando `procesos` y el follow-up no menciona explícitamente `contratos`, se conserva el dataset anterior.

Ejemplo:

```text
Base: "procesos de pavimentación en Atlántico"
Follow-up: "y en Bolívar"
```

Debe seguir en procesos, cambiando solo el scope.

**Archivo principal:** `conversation_store.py` / `followup_engine.py`

### D11 — Ventana temporal de follow-up: 30 minutos

Si el último turno tiene más de 30 minutos, una consulta ambigua se trata como nueva conversación.

**Razón:** evitar reanudación accidental de contexto viejo.

**Constante:** `FOLLOWUP_WINDOW_MINUTES = 30`

### D12 — Sanitización de `chat_id` por canal

Se usan prefijos por canal:

```text
tg_ → Telegram
wa_ → WhatsApp
st_ → Streamlit
```

**Razón:** evitar colisiones entre IDs de canales distintos.

**Archivo principal:** `conversation_store.py`

### D13 — Guardar `result_ids` por turno

Cada turno guarda IDs de contratos/procesos mostrados.

**Razón:** prerrequisito para futuros callbacks tipo “dame detalles del primero”, “compara el segundo con el tercero”, etc.

**Campo:** `Turn.result_ids`

### D14 — `apply_context` como acción Burr separada

La inyección de contexto conversacional es un action Burr propio, no parte de `parse_query`.

**Razón:** `parse_query` debe seguir siendo puro: texto → parámetros. El contexto es otra capa.

**Grafo:**

```text
parse_query → apply_context → resolve_entities
```

### D15 — `/reset` como comando explícito

Comandos reconocidos:

```text
/reset
reset
nueva
limpiar
nuevo
```

**Efecto:** limpia historial del `chat_id` y permite empezar de cero.

### D16 — COUNT antes del SELECT

El pipeline ejecuta `count(*)` antes del `SELECT` con límite.

**Razón:** el usuario debe saber si ve todos los resultados o solo una muestra.

Ejemplo:

```text
Encontré 72.321 resultados. Te muestro los 10 más recientes.
```

### D17 — `Firmado` no equivale a `Cerrado`

En SECOPPAL, “firmado” no se interpreta como `estado_contrato = 'Cerrado'`.

```text
Firmado ≠ Cerrado
```

**Razón:** `Cerrado` corresponde al cierre de gestión/expediente contractual, no a la firma del contrato.

**Política vigente:**

```text
"firmados" → dataset contratos, sin filtro de estado
"en ejecución" → En ejecución + Modificado + Prorrogado
"cerrados" → Cerrado
```

### D18 — Advisor first, evidence after, next actions at the end

La respuesta de SECOPPAL debe seguir este orden:

1. Qué entendió el sistema.
2. Qué universo encontró.
3. Qué resultados muestra.
4. Qué puede hacer después el usuario.

**Razón:** el producto debe comportarse como asesor, no como simple buscador.

### D19 — No relajar filtros exactos automáticamente

Si no hay resultados exactos, el sistema no debe relajar filtros sin avisar.

**Razón:** relajar automáticamente puede mostrar resultados fuera de alcance como si fueran válidos.

**Política:** ofrecer opciones numeradas.

### D20 — Archivos canónicos sin sufijos de versión dentro de `app/core`

No deben quedar archivos como:

```text
orchestrator v2.py
query_router final.py
entity_resolver copia.py
```

**Razón:** humanos y agentes pueden editar el archivo equivocado.

**Política:** versiones viejas van a `archive/cleanup_YYYY-MM-DD/`.

---

## 13.1 Relación entre este documento y `decisions_log.md`

Para evitar duplicación, la política documental recomendada es:

```text
docs/SECOPPAL_DOCUMENTO_TECNICO.md   → mapa principal del sistema
docs/SECOPPAL_DECISIONS_LOG.md       → decisiones históricas tipo ADR
docs/SECOPPAL_CHANGELOG.md           → cambios por fecha/versión
docs/SECOPPAL_TEST_MATRIX.md         → matriz viva de regresión
```

Este documento puede contener una versión consolidada de las decisiones principales. El `decisions_log.md` puede mantenerse como fuente histórica detallada o migrarse a formato ADR.

---

## 14. Roadmap inmediato

### Fase A — P0 hardening post-auditoría

**Estado:** cerrado en 2026-05-15.

- INV-005 defendida en código y tests.
- `app/core/` limpio y protegido por `make lint-core`.
- Tests de regresión para política de estados y coherencia `COUNT`/`SELECT`.
- Documento técnico actualizado con verificación empírica de `jbjy-vk9h`.

### Fase B — OBS-001 logger semántico / telemetría operacional

**Dependencia:** solo ejecutar después de P0 verde.

- Añadir `trace_id` a logs y JSONL.
- Registrar tiempos por etapa: parse, resolve, SoQL build, count, query, observe, narrate, total.
- Medir tasa LLM, tasa de degradación, timeout/retry y distribución de `intent_type` / `followup_intent_type`.
- Crear `scripts/analyze_traces.py` para rollups semanales.

### Fase C — Matriz de regresión follow-up

- Asegurar tests para los 10 escenarios mínimos.
- Agregar fixtures de historial conversacional.
- Verificar `intent_type` / `followup_intent_type`.

### Fase D — Limpieza imports legacy

- Revisar imports no usados en `orchestrator.py`.
- No borrar helpers legacy mientras los tests los usen.
- Marcar `query_frame.classify_turn()` como legacy si ya no opera en producción.

### Fase E — Arranque local de desarrollo

**Estado:** resuelto mediante `make dev`.

El proyecto ya cuenta con un comando único para levantar el entorno local de desarrollo.

Comando esperado:

```bash
make dev
```

Este comando debe encargarse de iniciar backend y frontend de desarrollo, evitando tener que abrir manualmente dos terminales para `uvicorn` y `streamlit`.

**Decisión:** no crear `scripts/dev.sh` mientras `make dev` sea suficiente. Mantener una sola entrada de arranque reduce confusión.

### Fase F — Relevancia y ranking

Después del follow-up:

- Mejorar ranking por relevancia textual.
- Separar coincidencia directa vs coincidencia probable.
- Evitar que resultados de baja pertinencia aparezcan arriba solo por fecha/valor.

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

## 16. Apéndice: estado actual resumido

```text
Sistema: SECOPPAL
Fecha: 2026-05-15
Estado: MVP funcional en hardening semántico/conversacional
Tests reportados: 424/424 passed
Último bug crítico cerrado: INV-005 en camino LLM + limpieza app/core post-auditoría
Core limpio: confirmado con make lint-core
Follow-up engine: integrado como política principal de seguimiento
Política de estados: corregida; firmado no es estado, es selección de dataset contratos
Próximo bloque recomendado: OBS-001 logger semántico / telemetría operacional
```


---

## 10. Estado actual y cierre de auditoría (Mayo 2026)

**Última actualización:** 2026-05-16

### Auditoría P0 cerrada

Se completó el micro-sprint de corrección de los 4 bugs de alta prioridad identificados en la segunda auditoría holística:

| Bug | Descripción | Estado |
|-----|-------------|--------|
| **B1** | `/chat` no propagaba `chat_id` (memoria conversacional muerta vía HTTP) | ✅ Cerrado |
| **B2** | `degrade_query` no recalculaba `total_count` | ✅ Cerrado |
| **B3** | Regex de entidades del narrator con `re.IGNORECASE` generaba falsos positivos | ✅ Cerrado |
| **B9** | `_is_signed_query` actuaba como kill-switch y borraba estados reales ("firmados o en ejecución") | ✅ Cerrado |

### Regla semántica vigente (ADR-008)

- `"firmados"`, `"suscritos"`, `"celebrados"` → solo fuerzan `dataset = "contratos"`.
- Nunca borran filtros de estado válidos.
- Si el usuario combina “firmados” + un estado real (“en ejecución”, “activos”, “vigentes”), se aplica la familia correspondiente.
- Si no hay estado real explícito, queda universo completo de contratos.

**Test principal que defiende esta regla:**
```python
"contratos firmados que estén en ejecución de mantenimiento en Bogotá"
→ dataset == "contratos"
→ estado_contrato contiene ["En ejecución", "Modificado", "Prorrogado"]
```

### Métricas actuales

- **Tests:** 466 pasando
- **feedback.jsonl:** sin modificaciones desde el cierre
- **Cobertura de invariantes semánticas:** alta (estados, firmados, follow-up, degradación)

El sistema se encuentra en estado **Verde-Amarillo** y listo para exposición controlada a usuarios internos.


---

## 11. Decisiones arquitectónicas relevantes (D21)

### D21: Analytical queries determinísticas antes de LLM classifier (2026-05-17)

**Contexto:** Antes de implementar comparativas, distribuciones o top-N, se priorizó una ruta analítica simple y determinística para responder preguntas de agregación básica.

**Decisión:**
- El LLM nunca genera SoQL.
- `analytics.py` detecta intent analítico de forma determinística.
- `aggregate_sum` exige al menos un scope/topic (guard anti-global).
- `analytical_response` no puede ser sobrescrita por `format_response` ni `build_advisor_response`.
- `top_entities`, `top_contractors` y distribuciones quedan diferidos para fases posteriores (AQ-001B).

**Reglas vigentes:**
- Ruta analítica → antes que clasificador LLM.
- Solo `aggregate_sum` en esta fase.
- Guard + short-circuit en múltiples puntos del grafo Burr.

---

## 12. Roadmap próximo bloque recomendado (actualizado 2026-05-17)

Después del cierre de AQ-001A, el orden recomendado es:

1. **OPP-001** — Opportunity Hunting Audit (revisión de oportunidades de mejora de producto)
2. **AQ-001B** — `top_entities` / `top_contractors` (agregaciones de ranking)
3. **OBS-001** — Observabilidad operativa (queda pendiente si el foco es análisis de negocio)

No se recomienda activar H10 ni LLM classifier para queries analíticas mientras la ruta determinística funcione bien.

### 2026-05-17 — Oportunidades SECOP: estado, dedupe y presentación

Se ajustó la política de oportunidades para evitar doble filtrado de estados y mejorar la salida visible de procesos. El caso guía fue una oportunidad de mantenimiento correctivo/preventivo de parque automotor en Norte de Santander que no aparecía por combinación restrictiva de estados.

Validación:
- tests Pamplonita: 4/4
- tests B1-B7: 19/19
- torture matrix verde
- feedback.jsonl intacto

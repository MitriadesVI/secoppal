# SECOPPAL — Buscador Conversacional de Contratación Pública Colombiana

> **Búsqueda de procesos en SECOP II usando lenguaje natural, sin apps que descargar.**

Un usuario escribe: *"licitaciones de mantenimiento vial en Atlántico por más de 500 millones abiertas"* y recibe resultados formateados con links directos a SECOP II — a través de WhatsApp, Telegram o una interfaz web.

---

## Tabla de Contenidos

1. [Qué es SECOPPAL](#qué-es-secoppal)
2. [Stack tecnológico](#stack-tecnológico)
3. [Arquitectura](#arquitectura)
4. [Flujo de procesamiento](#flujo-de-procesamiento)
5. [Componentes principales](#componentes-principales)
6. [Canales y despliegue](#canales-y-despliegue)
7. [Estructura del proyecto](#estructura-del-proyecto)
8. [Instalación y configuración](#instalación-y-configuración)
9. [Ejecución](#ejecución)
10. [Tests](#tests)
11. [Scripts de utilidad](#scripts-de-utilidad)
12. [Variables de entorno](#variables-de-entorno)
13. [Registro de Decisiones Técnicas (ADR)](#registro-de-decisiones-técnicas-adr)

---

## Qué es SECOPPAL

SECOPPAL es un chatbot conversacional que permite buscar **procesos de contratación pública colombiana** en SECOP II usando lenguaje natural. El usuario no necesita conocer la plataforma datos.gov.co ni saber escribir queries técnicas — simplemente describe lo que busca.

**Usuarios objetivo:**
- Alcaldes y secretarios de contratación de municipios colombianos
- Proponentes (empresas y personas naturales que participan en licitaciones)
- Veedurías ciudadanas

**Diferenciador clave:** interfaz conversacional *zero-friction* — sin registro, sin app, sin curva de aprendizaje. El canal inicial es WhatsApp, que ya tienen todos.

**Datasets que consulta (SECOP II / Socrata):**

| Dataset | ID Socrata | Descripción |
|---------|-----------|-------------|
| Procesos | `p6dx-8zbt` | Procesos de contratación abiertos y cerrados |
| Contratos | `jbjy-vk9h` | Contratos firmados y en ejecución |

---

## Stack tecnológico

| Capa | Tecnología | Rol |
|------|-----------|-----|
| **API / Backend** | FastAPI + Uvicorn | Servidor HTTP, webhooks de Twilio y Telegram |
| **UI de pruebas** | Streamlit | Interfaz web para desarrollo y demos |
| **Orquestación** | Apache Burr | Máquina de estados del pipeline conversacional |
| **Spell correction** | RapidFuzz | Corrección de typos en vocabulario de contratación (36 términos) |
| **Fuzzy matching** | RapidFuzz | Resolución de nombres de entidades con variaciones |
| **Datos SECOP** | sodapy + Socrata | Consultas SoQL contra datos.gov.co |
| **Config** | pydantic-settings | Variables de entorno tipadas y validadas |
| **Canal WhatsApp** | Twilio | Webhook entrante/saliente de mensajes |
| **Canal Telegram** | Bot API (nativo) | Webhook de mensajes |
| **Testing** | pytest | Suite de pruebas unitarias y end-to-end |

---

## Arquitectura

**Principio central:** el pipeline es 100% determinístico. No usa LLM. El código extrae parámetros con regex + gazetteer y construye queries SoQL. Costo por query: **$0**.

```
Usuario (WhatsApp / Telegram / Streamlit)
        │
        ▼
┌───────────────────────────┐
│  PASO 1: Spell Correction │  ← RapidFuzz, 36 términos  (gratis, <1 ms)
│  "pretacion" → "prestacion│
│  "alcladia"  → "alcaldia" │
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  PASO 2: Query Router     │  ← Regex + gazetteer 10K+ aliases  (gratis, <1 ms)
│  Extrae: entidad, depto,  │     Cubre 100% de queries
│  objeto, fechas, montos,  │
│  estado, modalidad, orden │
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  PASO 3: Entity Resolver  │  ← Alias exacto → RapidFuzz → LIKE  (gratis)
│  "gobernación atlántico"  │
│  → "DEPARTAMENTO DE       │
│     ATLANTICO"            │
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  PASO 4: SoQL Builder     │  ← Template determinístico  (gratis)
│  Construye query para     │
│  la API Socrata           │
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  PASO 5: SECOP Client     │  ← sodapy + reintentos  (~2–5 s)
│  Ejecuta contra           │
│  datos.gov.co             │
└──────────┬────────────────┘
           ▼
┌───────────────────────────┐
│  PASO 6: Formatter        │  ← Template por canal  (gratis)
│  Emojis + links para      │
│  WhatsApp / Telegram /    │
│  Streamlit                │
└──────────┬────────────────┘
           ▼
        Respuesta al usuario
```

---

## Flujo de procesamiento

El flujo está orquestado por **Apache Burr** como máquina de estados. Cada acción recibe y produce un `State` inmutable.

```
parse_query ──► resolve_entities ──► build_query ──► execute_query
                                                         │
                                              [needs_clarification?]
                                               ┌────┘      └────┐
                                        clarify_query    format_response
```

> **Nota:** El paso `llm_parse` se ejecuta condicionalmente cuando `_needs_llm()` detecta municipios o entidades sin resolver en el objeto (ADR-009). ~90% de queries usan solo heurística.

**Transiciones clave:**

| Condición | Siguiente paso |
|-----------|---------------|
| `needs_llm = False` (mayoría) | `resolve_entities` (salta el LLM) |
| `needs_llm = True` (municipio/entidad en objeto) | `llm_parse` → `resolve_entities` |
| `needs_clarification = True` | `clarify_query` (pide más info al usuario) |
| Error en API Socrata | Reintento con backoff exponencial |

---

## Componentes principales

### `app/core/query_router.py` — Parser heurístico (~90% de queries)
El pipeline de parseo. Usa regex, gazetteer y spell correction para extraer parámetros **sin LLM**.

Extrae:
- **Dataset**: `procesos` vs `contratos` (detectado por palabras clave como "contrato", "firmado", "ejecutando")
- **Departamento**: 34 departamentos colombianos con aliases y variaciones
- **Entidad**: gobernación, alcaldía, SENA, ICBF, etc. con hints por regex. Los tokens de año (`20xx`) se eliminan del texto antes de la extracción para evitar contaminación; además, un año en el texto actúa como límite que detiene la captura de entidad.
- **Ciudad/Municipio**: nuevo parámetro extraído por LLM cuando se detecta "en [lugar]" no resuelto como departamento
- **Estado del proceso**: abierto, cerrado, adjudicado, desierto, convocatoria (→ Publicado), etc.
- **Modalidad**: licitación pública, mínima cuantía, selección abreviada, etc.
- **Montos**: rango `valor_min` / `valor_max` ("más de 500 millones", "entre 200 y 800 millones")
- **Fechas**: ISO o año solo ("en 2024", "desde enero"), o mes + año ("abril de 2026" → rango fecha_desde/fecha_hasta del mes completo)
- **Señal de ordenamiento** (`ordering_signal`): detecta frases como "más caros", "más costosos", "más grandes", etc. (`_ORDERING_SIGNAL_RE`). Estas frases son instrucciones de ordenamiento, **no** términos de objeto, y se eliminan del texto antes de la extracción de objeto.
- **Contratista**: solo para dataset contratos
- **Términos de objeto**: palabras clave residuales para búsqueda fulltext. Adjetivos de valor/tamaño (`caro`, `costoso`, `grande`, `alto`, `nuevo`, `mejor`, etc.) están en `STOPWORDS` y no contaminan el objeto.

Retorna `ParsedQuery` con `params`, `needs_llm: bool` y `route_reason: str`.

| `route_reason` | Condición |
|----------------|-----------|
| `heuristic_only` | Parámetros suficientes extraídos sin LLM |
| `heuristic_plus_llm` | Señales avanzadas detectadas (top, ranking, comparar…) |
| `insufficient_signals` | Solo se extrajo el dataset, sin otros parámetros |
| `ordering_without_object` | Señal de ordenamiento presente pero sin objeto real — el LLM interpreta la intent |

**`query_router v2.py`** — Variante con detección gazetteer-first: en lugar de regex, escanea el texto contra el índice de 10K+ aliases del `EntityResolver` antes de cualquier otra extracción. Elimina ambigüedades como "gobernación de santander" donde la heurística clásica podría dividir entidad y departamento incorrectamente.

**Stopwords y limpieza de objeto:** Los tokens residuales se filtran con un set de stopwords que incluye verbos conversacionales (`busca`, `muéstrame`…), artículos, adjetivos de valor/tamaño (`caro`, `costoso`…), **adverbios temporales** (`actualmente`, `ahora`, `hoy`, `recientemente`) y **pronombres indefinidos** (`algún`, `ningún`…). Además, signos de puntuación (`?`, `!`, `,`, etc.) se limpian antes de tokenizar para evitar que queden pegados a tokens válidos.

---

### `app/utils/spell_correction.py` — Corrección de typos
Corrige errores de escritura comunes en vocabulario de contratación pública antes del parseo.

- Vocabulario de 36 términos frecuentes (prestación, alcaldía, gobernación, licitación, etc.)
- Usa RapidFuzz con umbral de 80% de similitud
- Limpia caracteres especiales pegados a palabras ("servid=cios" → "servicios")
- Preserva palabras cortas (≤2 chars), números y palabras desconocidas (nombres propios)
- Se ejecuta después de `normalize_text()` pero antes de cualquier extracción de parámetros

---

### `app/core/entity_resolver.py` — Resolución de entidades
Convierte referencias en lenguaje natural al nombre oficial exacto en SECOP.

Tres niveles en cascada:

| Nivel | Método | Ejemplo |
|-------|--------|---------|
| **Tier 1** | Exact match en `aliases_db.json` | "sena" → "SERVICIO NACIONAL DE APRENDIZAJE -SENA-" |
| **Tier 2** | Fuzzy matching con RapidFuzz (WRatio) | "gobernacion del atlantico" → "DEPARTAMENTO DE ATLANTICO" |
| **Tier 3** | Fallback `LIKE` en la query SoQL | Entidad desconocida → búsqueda parcial en API |

La base de aliases (`aliases_db.json`) se genera semi-automáticamente con los scripts de utilidad.

**Versiones disponibles:**

| Versión | Archivo | Descripción |
|---------|---------|-------------|
| **V1** | `entity_resolver.py` | Solo `resolve_*()` — fuzzy matching clásico |
| **V2** | `entity_resolver v2.py` | Añade `scan_entity()` / `scan_departamento()` — detección por substring con índice first-word. Problema: el scan puede capturar nombres de departamentos como entidades. |
| **V3** | `entity_resolver v3.py` | Corrige V2: `scan_entity()` rechaza nombres de departamento sin calificador (`"gobernacion"`, `"alcaldia"`, etc.). **Versión recomendada con `query_router v2.py`.** |

La combinación `query_router v2 + entity_resolver v3` obtuvo **95.5% de accuracy** en el benchmark de 53 queries (vs 94.8% de V1 y 82.8% de V2). Ver `scripts/accuracy_test.py`.

---

### `app/core/soql_builder.py` — Constructor de queries SoQL
Recibe parámetros resueltos y construye la query para la API Socrata. **Completamente determinístico.**

- WHERE dinámico según parámetros presentes
- ORDER BY: `precio_base DESC, fecha DESC` por defecto (valor primario, fecha como desempate). Cuando el usuario pide explícitamente ordenar por precio (`ordering_signal=valor_desc`), usa solo `precio_base DESC`
- Escaping de inputs para prevenir inyección
- LIMIT configurable (default: 50 resultados)
- Soporte LIKE case-insensitive para búsqueda fulltext

---

### `app/core/secop_client.py` — Cliente Socrata
Wrapper de `sodapy` con resiliencia para producción:

- Reintentos automáticos con backoff exponencial
- Normalización de URLs rotas (Socrata a veces retorna URLs de login)
- Reconstrucción de URL pública desde `referencia_del_proceso`
- Conversión robusta de campos monetarios a `float`

---

### `app/core/formatter.py` — Formateador por canal
Transforma los resultados crudos en mensajes listos para cada canal:

| Canal | Formato |
|-------|---------|
| **WhatsApp** | Emojis, texto compacto, links a SECOP |
| **Telegram** | Texto limpio con Markdown |
| **Streamlit** | Resumen (la tabla la renderiza la UI) |

---

### `app/core/orchestrator.py` — Orquestador (Apache Burr)
Define la máquina de estados del workflow. Cada acción es una función pura que recibe `State` y retorna `State`. El grafo de transiciones es explícito e inspeccionable.

**`orchestrator v2.py`** — Variante que inicializa el `QueryRouter` con el `EntityResolver` (gazetteer-first), añade manejo de errores en `execute_query` (respuesta amigable en timeout), y evita resolver entidades que el scan ya resolvió en el paso de parseo.

---

### `app/core/feedback.py` — Sistema de feedback
Almacenamiento append-only en JSONL para observabilidad y mejora continua:

- `log_trace()`: guarda la ejecución completa con UUID (params, SoQL, resultados, canal)
- `rate()`: califica un resultado (1 = útil, 0 = no útil) con **comentario opcional** para notas específicas ("entidad bien pero faltó filtrar por año")
- `get_stats()`: precisión, breakdown por ruta (heurística vs LLM), % sin resultados
- `get_low_rated()`: casos para analizar y mejorar

La UI de Streamlit muestra un campo de comentario opcional debajo de cada resultado, antes de los botones 👍/👎. El comentario se persiste en `rating_comment` del JSONL.

---

### `app/data/aliases_db.json` — Gazetteer de entidades
Base de datos de aliases: mapea formas naturales de referirse a entidades al nombre oficial en SECOP.

```json
{
  "official_name": "DEPARTAMENTO DE ATLANTICO",
  "department": "ATLANTICO",
  "aliases": ["gobernacion del atlantico", "gobernacion atlantico", "..."]
}
```

Se construye combinando `fetch_top_entities.py` + `generate_aliases.py`.

---

## Canales y despliegue

### Streamlit (pruebas y demos)
Interfaz web con historial de conversación, tabla de resultados y panel de debug con `route_reason`, parámetros extraídos y SoQL generado.

```bash
streamlit run app/streamlit_app.py
```

### FastAPI + Twilio (WhatsApp)
El webhook `/webhooks/twilio/whatsapp` recibe mensajes de Twilio, los procesa y responde en el mismo hilo de WhatsApp.

### FastAPI + Telegram Bot
El webhook `/webhooks/telegram` recibe updates del Bot API de Telegram.

### Endpoints disponibles

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/` | Info del servidor |
| `GET` | `/health` | Estado de salud |
| `POST` | `/chat` | Chat directo (JSON) |
| `POST` | `/rate` | Calificar resultado |
| `GET` | `/feedback/stats` | Estadísticas de feedback |
| `POST` | `/webhooks/twilio/whatsapp` | Webhook WhatsApp |
| `POST` | `/webhooks/telegram` | Webhook Telegram |
| `GET` | `/examples` | Ejemplos de queries |

---

## Estructura del proyecto

```
secoppal/
├── app/
│   ├── config.py              # Variables de entorno (pydantic-settings)
│   ├── main.py                # Aplicación FastAPI y webhooks
│   ├── service.py             # Facade / singleton del workflow
│   ├── streamlit_app.py       # UI de pruebas
│   │
│   ├── core/
│   │   ├── orchestrator.py       # Máquina de estados (Apache Burr)
│   │   ├── orchestrator v2.py    # + manejo de errores y gazetteer-first
│   │   ├── query_router.py       # Parser heurístico (100% queries)
│   │   ├── query_router v2.py    # + gazetteer-first entity detection
│   │   ├── entity_resolver.py    # Resolución de entidades (V3 activo)
│   │   ├── entity_resolver v2.py # + scan_entity() / scan_departamento()
│   │   ├── entity_resolver v3.py # + disambiguación depts (recomendado)
│   │   ├── llm_handler.py        # DeepSeek fallback para ciudad/entidad (ADR-009)
│   │   ├── soql_builder.py       # Constructor de queries SoQL
│   │   ├── secop_client.py       # Cliente Socrata con reintentos
│   │   ├── formatter.py          # Formateador por canal
│   │   └── feedback.py           # Trazas y sistema de rating
│   │
│   ├── data/
│   │   ├── aliases_db.json    # Gazetteer de entidades (semilla)
│   │   ├── departamentos.py   # 34 departamentos con aliases
│   │   ├── estados.py         # Estados de procesos y contratos
│   │   └── colombia_geography.py  # Clasificación regional
│   │
│   └── utils/
│       ├── spell_correction.py # Corrección de typos (36 términos, RapidFuzz)
│       ├── logging.py         # Configuración de logging
│       └── money.py           # Parseo y formato de moneda COP
│
├── tests/
│   ├── conftest.py
│   ├── test_query_router.py     # 16 tests — producción stack (V2+V3)
│   ├── test_soql_builder.py     # 4 tests — ordenamiento y filtros
│   ├── test_entity_resolver.py  # 16 tests — exact, fuzzy, LIKE, scan V3
│   ├── test_spell_correction.py # 17 tests — typos, preservación, limpieza
│   └── test_orchestrator.py     # 1 test — e2e con Socrata mock
│
├── scripts/
│   ├── accuracy_test.py       # Benchmark V1/V2/V3 con 53 queries anotadas
│   ├── test_queries.py        # Prueba queries de ejemplo localmente
│   ├── fetch_top_entities.py  # Extrae top entidades de SECOP II
│   ├── generate_aliases.py    # Genera aliases con DeepSeek
│   └── build_gazetteer.py     # CLI completo para construir gazetteer
│
├── .env.example
├── requirements.txt
└── pytest.ini
```

---

## Instalación y configuración

```bash
# 1. Clonar y crear entorno virtual
git clone <repo>
cd secoppal
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales
```

---

## Ejecución

```bash
# API (FastAPI)
uvicorn app.main:app --reload

# UI de pruebas (Streamlit)
streamlit run app/streamlit_app.py

# Probar queries de ejemplo en consola
python scripts/test_queries.py
```

---

## Tests

```bash
# Ejecutar toda la suite
pytest

# Con output detallado
pytest -v

# Solo un módulo
pytest tests/test_query_router.py
```

La suite incluye (54 tests):
- **test_query_router** (16 tests): Usa stack de producción (V2 router + V3 entity resolver). Cubre dataset selection, entity/department extraction, stopwords (temporales, indefinidos), limpieza de puntuación, ordering signals, fechas por año y mes+año.
- **test_soql_builder** (4 tests): Ordenamiento por precio+fecha (default) vs solo precio (con ordering_signal), filtros WHERE
- **test_entity_resolver** (16 tests): Exact match, fuzzy, LIKE fallback (V1) + tests de `scan_entity` / `scan_departamento` de V3 (disambiguación de departamentos)
- **test_spell_correction** (17 tests): Corrección de typos comunes, preservación de palabras correctas/cortas/desconocidas, limpieza de caracteres especiales
- **test_orchestrator** (1 test): End-to-end con Socrata mockeado

---

## Scripts de utilidad

### Benchmark de accuracy entre versiones

Compara la accuracy de parseo/resolución de los tres stacks (V1, V2, V3) contra 53 queries anotadas con valores esperados:

```bash
# Reporte completo V1 vs V2 vs V3
python scripts/accuracy_test.py

# Con detalle por query (campos fallidos y valores extraídos)
python scripts/accuracy_test.py --verbose

# Filtrar por categoría: dataset | departamento | entidad | estado | modalidad | montos | fechas | combinado | edge_case
python scripts/accuracy_test.py --category entidad

# Exportar resultados a JSON
python scripts/accuracy_test.py --output accuracy_results.json
```

---

### Construir / actualizar el gazetteer de entidades

El gazetteer (`aliases_db.json`) se construye en dos pasos:

```bash
# Paso 1: Extraer top N entidades de SECOP II
python scripts/fetch_top_entities.py
# → genera scripts/top_entities.json

# Paso 2: Generar aliases con DeepSeek
python scripts/generate_aliases.py
# → genera app/data/aliases_db.generated.json

# O en un solo comando (CLI completo)
python scripts/build_gazetteer.py full --top 500
```

---

## Variables de entorno

| Variable | Descripción | Requerida |
|----------|-------------|-----------|
| `SECOP_APP_TOKEN` | Token de app para la API Socrata | Sí |
| `SECOP_API_KEY_ID` | Key ID para Socrata | Sí |
| `SECOP_APP_SECRET` | Secret de app Socrata | Sí |
| `DEEPSEEK_API_KEY` | API key de DeepSeek | Sí (para fallback LLM — ADR-009) |
| `DEEPSEEK_MODEL` | Modelo a usar (default: `deepseek-chat`) | No (legacy) |
| `DATOS_GOV_DOMAIN` | Dominio Socrata (default: `www.datos.gov.co`) | No |
| `SECOP_TIMEOUT_SECONDS` | Timeout para API SECOP (default: `30`) | No |
| `SECOP_RESULTS_LIMIT` | Máximo resultados por query (default: `25`) | No |
| `SECOP_ALIAS_DB_PATH` | Ruta al gazetteer de aliases | No |
| `TELEGRAM_BOT_TOKEN` | Token del bot de Telegram | Para canal Telegram |
| `TELEGRAM_WEBHOOK_SECRET` | Secret para verificar webhook | Para canal Telegram |
| `TWILIO_WHATSAPP_NUMBER` | Número Twilio WhatsApp | Para canal WhatsApp |
| `LOG_LEVEL` | Nivel de logging (default: `INFO`) | No |

---

## Registro de Decisiones Técnicas (ADR)

> Esta sección documenta las decisiones de diseño significativas, el contexto que las motivó y las alternativas descartadas. Se actualiza cuando se toma una decisión que no es obvia desde el código.

---

### ADR-001: Arquitectura determinística (sin LLM)

**Fecha:** 2025-04 (actualizado 2026-04)
**Estado:** Activo

**Decisión:** El pipeline es 100% determinístico. No usa LLM. Regex + gazetteer + spell correction extraen parámetros; código determinístico construye SoQL. Costo por query: $0.

**Contexto:** Los usuarios objetivo hacen queries relativamente predecibles (departamento + tipo + estado + monto). Originalmente se diseñó como arquitectura híbrida (heurística + DeepSeek fallback), pero el análisis de producción demostró que el LLM nunca se activaba (0/26 queries). La heurística cubre el 100% de los casos.

**Consecuencias:** Costo cero por query. Sin dependencia de APIs externas de LLM. Sin latencia adicional. El LLM nunca generó SoQL — y ahora tampoco extrae parámetros.

---

### ADR-002: DeepSeek V3 como LLM principal

**Fecha:** 2025-04
**Estado:** Supersedido por ADR-008

**Decisión:** Usar DeepSeek V3 via API compatible con OpenAI SDK como fallback para queries complejas.

**Contexto:** Se diseñó como fallback para ~30% de queries. En la práctica, el heurístico cubrió el 100% — DeepSeek nunca se activó en producción (0/26 queries). Eliminado en ADR-008.

---

### ADR-003: Apache Burr para orquestación

**Fecha:** 2025-04
**Estado:** Activo

**Decisión:** Usar Apache Burr como máquina de estados del pipeline, en lugar de código secuencial o LangChain/LangGraph.

**Contexto:** El flujo tiene transiciones condicionales (needs_llm, needs_clarification) que se benefician de un grafo explícito. Burr ofrece trazabilidad, estado inmutable y UI de debug sin la complejidad de LangGraph.

**Alternativas descartadas:** Código secuencial (difícil de extender), LangChain/LangGraph (overhead y abstracción innecesaria para este caso).

---

### ADR-004: Gazetteer de entidades precomputado

**Fecha:** 2025-04
**Estado:** Activo

**Decisión:** Construir y mantener offline una base de aliases (`aliases_db.json`) en lugar de resolver entidades en tiempo real contra la API.

**Contexto:** La API de Socrata es lenta para operaciones de discovery. El fuzzy matching en tiempo real contra miles de nombres oficiales es costoso. Un gazetteer precomputado con RapidFuzz da latencia sub-milisegundo.

**Consecuencias:** Requiere proceso periódico de actualización cuando se incorporan nuevas entidades a SECOP. Los scripts de utilidad automatizan este proceso.

---

### ADR-005: Feedback store en JSONL append-only

**Fecha:** 2025-04
**Estado:** Activo

**Decisión:** Almacenar trazas y ratings en archivos JSONL locales, sin base de datos.

**Contexto:** En fase MVP, la simplicidad de despliegue es prioritaria. No hay necesidad de queries complejas sobre los datos de feedback en este momento.

**Evolución esperada:** Migrar a PostgreSQL cuando el volumen de queries justifique consultas más complejas o múltiples instancias.

---

### ADR-006: Twilio como gateway de WhatsApp

**Fecha:** 2025-04
**Estado:** Activo

**Decisión:** Usar Twilio WhatsApp Business API en lugar de la API oficial de Meta directamente.

**Contexto:** La API oficial de Meta requiere aprobación de plantillas y proceso de verificación complejo. Twilio provee sandbox inmediato para desarrollo y simplifica la gestión de webhooks.

**Consecuencias:** Costo adicional por mensaje vía Twilio. Para escala significativa, evaluar migración a Meta API directa.

---

### ADR-007: Gazetteer-first entity detection y corrección V3

**Fecha:** 2026-04
**Estado:** Activo

**Decisión:** Adoptar `entity_resolver v3.py` + `query_router v2.py` como stack de producción recomendado, en lugar del parser heurístico clásico (V1).

**Contexto:** El benchmark de 53 queries reveló que la versión V2 (gazetteer-first sin guardianes) tenía 82.8% de accuracy — peor que V1 (94.8%) — porque `scan_entity` capturaba nombres de departamento como "antioquia" o "atlantico" cuando aparecían como aliases de entidades como "DEPARTAMENTO DE ANTIOQUIA", robándolos antes de que `scan_departamento` pudiera encontrarlos.

**Solución (V3):** `scan_entity` aplica una función `_is_unqualified_dept` que rechaza cualquier alias que sea un nombre de departamento a menos que esté precedido de un calificador explícito (`"gobernacion"`, `"alcaldia"`, `"municipio"`, etc.). Esto resuelve el problema de colisión sin sacrificar la detección de entidades reales.

**Resultado:** V3 alcanza **95.5% de accuracy** (+0.7pp sobre V1, +12.7pp sobre V2). Gana en 7 de 9 categorías y empata en las 2 restantes.

**Alternativas descartadas:** Invertir el orden (scan_departamento antes que scan_entity) — no funciona porque entidades compuestas como "gobernación de santander" contienen nombres de departamento dentro de ellas.

---

### ADR-008: Eliminación del LLM (DeepSeek) del pipeline

**Fecha:** 2026-04
**Estado:** Supersedido por ADR-009

**Decisión:** Eliminar completamente el uso de DeepSeek V3 del pipeline de producción. `_needs_llm()` retorna `False` siempre. El archivo `llm_handler.py` se conserva como legacy pero no se ejecuta.

**Contexto:** Análisis de 26 queries de producción (feedback.jsonl) mostró que el LLM nunca se activó (0%). Las 5 queries con rating negativo fallaron por bugs en la heurística (stopwords, entity resolution, typos) — no por ausencia de LLM. Tras corregir esos bugs (spell correction, V3 entity resolver, stopwords mejorados), la heurística cubre el 100% de los casos con 95.5% de accuracy en benchmark de 53 queries.

**Consecuencias:** Costo $0/query. Sin dependencia de API externa. Latencia reducida (~1ms vs ~2-5s con LLM). La firma `_needs_llm()` se mantiene para no romper la interfaz del orchestrator (Burr).

**Alternativas descartadas:** Mantener DeepSeek como "safety net" — innecesario dado el 0% de uso y el costo de mantener la integración.

---

*Para agregar una nueva decisión, copiar la plantilla con: Fecha, Estado (Activo / Supersedido / Descartado), Decisión, Contexto, Alternativas descartadas, Consecuencias.*

---

### ADR-009: Hybrid LLM fallback para ciudad/entidad disambiguation

**Fecha:** 2026-04
**Estado:** Activo

**Decisión:** Reintroducir DeepSeek V3 como fallback condicional para queries donde el heurístico no puede distinguir entre municipio, entidad y objeto.

**Contexto:** Feedback de producción (traces 3aa726cc, f202ab0c, 78ad68c9) mostró que el heurístico falla cuando:
- El usuario menciona un municipio ("en Puerto Salgar") — no hay gazetteer de municipios
- El usuario menciona una entidad por nombre libre ("Secretaría de Integración Social") — no está en las 500 entidades del gazetteer

Estos tokens terminan en `objeto`, generando queries LIKE sobre nombre_del_procedimiento en vez de filtrar por ciudad_entidad o nombre_entidad.

**Triggers para LLM (`_needs_llm()`):**
1. Patrón "en [lugar]" donde el lugar no se resolvió como departamento y sus tokens quedaron en objeto
2. Palabras tipo entidad (secretaría, ministerio, instituto, etc.) quedaron en objeto

**Nuevo parámetro:** `ciudad` — mapea a `ciudad_entidad` (procesos) o `ciudad` (contratos) con LIKE (casing inconsistente en SECOP).

**Protecciones:** El LLM no puede sobrescribir: departamento, estado, montos, fechas (REGEX_PRIORITY_KEYS), ni entidades ya resueltas por gazetteer (`entidad_resolved`).

**Consecuencias:** ~5-10% de queries usan LLM (~$0.001/query). 90%+ siguen siendo gratis. Latencia +2-5s solo para queries que antes daban resultados incorrectos. Accuracy benchmark: 96.3% (vs 95.5% pre-ADR-009).

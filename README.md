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
| **LLM** | DeepSeek V3 (API) | Interpretación de intención y extracción de parámetros (fallback ~30% queries) |
| **SDK LLM** | openai (compatible) | Cliente para la API de DeepSeek |
| **Fuzzy matching** | RapidFuzz | Resolución de nombres de entidades con variaciones |
| **Datos SECOP** | sodapy + Socrata | Consultas SoQL contra datos.gov.co |
| **Config** | pydantic-settings | Variables de entorno tipadas y validadas |
| **Canal WhatsApp** | Twilio | Webhook entrante/saliente de mensajes |
| **Canal Telegram** | Bot API (nativo) | Webhook de mensajes |
| **Testing** | pytest | Suite de pruebas unitarias y end-to-end |
| **HTTP async** | httpx | Llamadas asíncronas (LLM, utilidades) |

---

## Arquitectura

**Principio central:** el LLM no genera SQL ni queries. Solo interpreta intención y extrae parámetros. El código determinístico hace todo lo demás.

```
Usuario (WhatsApp / Telegram / Streamlit)
        │
        ▼
┌───────────────────────────┐
│  PASO 1: Query Router     │  ← Regex + diccionarios  (gratis, <1 ms)
│  ¿Se puede resolver       │     Cubre ~70% de queries
│  sin LLM?                 │
└──────────┬────────────────┘
           │
     ┌─────┴──────┐
     │ SÍ         │ NO (~30%)
     ▼            ▼
  Params       ┌────────────────────────┐
  extraídos    │  PASO 2: DeepSeek V3   │  ← Tool calling  (~$0.001/query)
               │  Interpreta intención  │
               │  Extrae parámetros     │
               └──────────┬─────────────┘
                          │
                     Params merged
                          │
     ┌────────────────────┘
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
parse_query ──► [needs_llm?] ──► llm_parse
     │                               │
     └──────────────────────────────►│
                                     ▼
                           resolve_entities
                                     │
                                build_query
                                     │
                             execute_query
                                     │
                          [needs_clarification?]
                           ┌────┘      └────┐
                    clarify_query    format_response
```

**Transiciones clave:**

| Condición | Siguiente paso |
|-----------|---------------|
| `needs_llm = False` | `resolve_entities` (salta el LLM) |
| `needs_llm = True` | `llm_parse` → `resolve_entities` |
| `needs_clarification = True` | `clarify_query` (pide más info al usuario) |
| Error en API Socrata | Reintento con backoff exponencial |

---

## Componentes principales

### `app/core/query_router.py` — Parser heurístico
El primer filtro del pipeline. Usa regex y diccionarios para extraer parámetros **sin tocar el LLM**.

Extrae:
- **Dataset**: `procesos` vs `contratos` (detectado por palabras clave como "contrato", "firmado", "ejecutando")
- **Departamento**: 34 departamentos colombianos con aliases y variaciones
- **Entidad**: gobernación, alcaldía, SENA, ICBF, etc. con hints por regex. Los tokens de año (`20xx`) se eliminan del texto antes de la extracción para evitar contaminación; además, un año en el texto actúa como límite que detiene la captura de entidad.
- **Estado del proceso**: abierto, cerrado, adjudicado, desierto, etc.
- **Modalidad**: licitación pública, mínima cuantía, selección abreviada, etc.
- **Montos**: rango `valor_min` / `valor_max` ("más de 500 millones", "entre 200 y 800 millones")
- **Fechas**: ISO o año solo ("en 2024", "desde enero")
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

### `app/core/llm_handler.py` — Fallback LLM (DeepSeek)
Se activa únicamente cuando el parser heurístico no pudo extraer parámetros suficientes.

- Usa la API de DeepSeek con **tool calling** estructurado (función `buscar_procesos`)
- El LLM devuelve JSON con parámetros — nunca genera SoQL
- Merge inteligente: no sobrescribe params que la heurística ya extrajo con confianza
- Timeout de 15 s con fallback silencioso si falla

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
- ORDER BY `precio_base DESC` (procesos) o `valor_del_contrato DESC` (contratos)
- Escaping de inputs para prevenir inyección
- LIMIT configurable (default: 25 resultados)
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
- `rate()`: califica un resultado (1 = útil, 0 = no útil)
- `get_stats()`: precisión, breakdown por ruta (heurística vs LLM), % sin resultados
- `get_low_rated()`: casos para analizar y mejorar

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
│   │   ├── orchestrator.py       # Máquina de estados (Apache Burr) — V1
│   │   ├── orchestrator v2.py    # + manejo de errores y gazetteer-first
│   │   ├── query_router.py       # Parser heurístico (~70% queries) — V1
│   │   ├── query_router v2.py    # + gazetteer-first entity detection
│   │   ├── entity_resolver.py    # Resolución de entidades — V1 (fuzzy)
│   │   ├── entity_resolver v2.py # + scan_entity() / scan_departamento()
│   │   ├── entity_resolver v3.py # + disambiguación depts (recomendado)
│   │   ├── llm_handler.py        # Fallback DeepSeek (~30% queries)
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
│       ├── logging.py         # Configuración de logging
│       └── money.py           # Parseo y formato de moneda COP
│
├── tests/
│   ├── conftest.py
│   ├── test_query_router.py
│   ├── test_soql_builder.py
│   ├── test_entity_resolver.py
│   └── test_orchestrator.py   # End-to-end con mock de Socrata
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

La suite incluye (30 tests):
- **test_query_router** (11 tests): Usa stack de producción (V2 router + V3 entity resolver). Cubre dataset selection, entity/department extraction, stopwords (temporales, indefinidos), limpieza de puntuación, ordering signals y fechas.
- **test_soql_builder** (2 tests): Construcción correcta de queries SoQL y ORDER BY
- **test_entity_resolver** (16 tests): Exact match, fuzzy, LIKE fallback (V1) + tests de `scan_entity` / `scan_departamento` de V3 (disambiguación de departamentos)
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
| `DEEPSEEK_API_KEY` | API key de DeepSeek | Para queries complejas |
| `DEEPSEEK_MODEL` | Modelo a usar (default: `deepseek-chat`) | No |
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

### ADR-001: Arquitectura híbrida determinística + LLM

**Fecha:** 2025-04
**Estado:** Activo

**Decisión:** El LLM (DeepSeek) solo se invoca cuando el parser heurístico no puede extraer parámetros suficientes (~30% de las queries). Para el ~70% restante, todo el procesamiento es determinístico.

**Contexto:** Los usuarios objetivo hacen queries relativamente predecibles (departamento + tipo + estado + monto). Una arquitectura 100% LLM sería más cara, más lenta y menos confiable para producción.

**Consecuencias:** Costo marginal muy bajo por query. El LLM nunca genera SoQL — si lo hiciera, sería imposible auditar o garantizar la seguridad de las queries.

---

### ADR-002: DeepSeek V3 como LLM principal

**Fecha:** 2025-04
**Estado:** Activo

**Decisión:** Usar DeepSeek V3 via API compatible con OpenAI SDK, en lugar de GPT-4o u otros modelos.

**Contexto:** Relación costo/desempeño superior para extracción de parámetros estructurados en español. Compatible con el SDK de OpenAI (sin cambios de código para migrar).

**Alternativas descartadas:** GPT-4o (10x más caro por token), Llama local (latencia, complejidad de infra).

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

*Para agregar una nueva decisión, copiar la plantilla con: Fecha, Estado (Activo / Supersedido / Descartado), Decisión, Contexto, Alternativas descartadas, Consecuencias.*

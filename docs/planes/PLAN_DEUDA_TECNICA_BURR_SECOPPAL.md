# SECOPPAL — Plan de deuda técnica Burr y observabilidad del workflow

**Estado:** borrador operativo  
**Fecha de referencia:** 2026-07-13  
**Prioridad:** media-alta  
**Tipo de iniciativa:** mantenimiento de plataforma + observabilidad + reducción del costo de depuración  
**Dependencia actual:** `burr==0.40.2`  
**Dependencia objetivo a evaluar:** `apache-burr==0.42.0`

---

## 1. Resumen ejecutivo

SECOPPAL usa Burr como motor de estados para coordinar una parte central del flujo conversacional. La versión instalada es `burr==0.40.2`. El núcleo y el cliente de tracking están disponibles, pero el entorno actual no incluye el extra `[start]`, por lo que no están instalados el servidor ni la interfaz visual de Burr.

El proyecto Apache Burr publica actualmente `apache-burr==0.42.0`. La migración no debe tratarse como un simple cambio de número de versión: cambia el nombre del paquete instalado, puede introducir dependencias nuevas y debe probarse contra los contratos de comportamiento de SECOPPAL.

La iniciativa se divide en tres objetivos independientes pero conectados:

1. **Observar el workflow actual**, para ver acciones, transiciones y estado paso a paso.
2. **Construir una bitácora persistente de dogfood**, enlazando cada consulta con su ejecución Burr, el feedback humano y una exportación legible para Claude/Codex.
3. **Migrar de forma controlada a Apache Burr 0.42**, sin cambiar el comportamiento funcional de SECOPPAL.

El valor principal no es mejorar directamente la calidad de los resultados. El valor es reducir el tiempo necesario para comprender fallos de estado, herencia conversacional, rutas alternativas y mutaciones inesperadas durante la implementación de nuevas capacidades. La bitácora debe, además, convertir consultas reales problemáticas en evidencia reproducible, diagnósticos auditables y futuros casos de regresión.

---

## 2. Estado confirmado del entorno

```text
Python:                  3.14.5
Burr instalado:          burr 0.40.2
Apache Burr instalado:   no
burr.core:               disponible
burr.tracking:           disponible
Burr CLI/UI:             no disponible
Causa:                   extra [start] no instalado
Pydantic:                2.12.5
FastAPI:                 0.135.3
Streamlit:               1.56.0
Uvicorn:                 0.44.0
```

La metadata del paquete declara comandos CLI, pero los ejecutables no están presentes en `.venv/bin`. El intento de importar `burr.cli.__main__` falla porque no están instaladas las dependencias del plugin `start`.

Esto no es un bug funcional de SECOPPAL. El runtime usa el core de Burr mediante imports Python y puede continuar funcionando sin la UI.

---

## 3. Problema que se quiere resolver

SECOPPAL tiene múltiples rutas y capas que transforman el estado de una consulta:

```text
consulta
→ parsing
→ aplicación de contexto
→ planificación
→ resolución
→ construcción de consulta
→ ejecución
→ observación
→ validación
→ ranking
→ rendering
```

Además, hay rutas especiales o parcialmente externas al grafo:

- paginación;
- refinamientos;
- selección de sugerencias;
- búsquedas por referencia;
- dual dataset;
- analytics;
- radar;
- futuros filtros de modalidad;
- degradaciones por timeout.

Cuando una variable se pierde, se hereda indebidamente o es reemplazada por otra capa, reconstruir la causa exige revisar trazas, estado, código y tests dispersos. Esto incrementa el costo de depuración y dificulta que el propietario funcional pueda comprender visualmente qué hizo el sistema.

La UI de Burr puede aportar una vista del grafo y una traza paso a paso con el estado antes y después de cada acción.

---

## 4. Objetivos

### 4.1 Objetivo principal

Disponer de observabilidad visual y reproducible sobre el workflow de SECOPPAL sin alterar sus respuestas ni su política de ejecución.

### 4.2 Objetivos específicos

- Visualizar el grafo actual de acciones y transiciones.
- Inspeccionar el estado antes y después de cada acción.
- Identificar dónde se introduce, modifica o elimina un parámetro.
- Comparar rutas de consultas simples, follow-ups y degradaciones.
- Reducir tiempo de diagnóstico de bugs de frame y de estado.
- Preparar la implementación de modalidad de contratación con mejor observabilidad.
- Asignar un identificador estable a cada consulta de dogfood y enlazarlo con su ejecución Burr.
- Persistir un índice consultable de ejecuciones, feedback, commit, resultado y diagnóstico.
- Exportar trazas en JSON y Markdown para revisión por Claude, Codex y auditorías humanas.
- Comparar ejecuciones antes/después de una corrección.
- Convertir casos reales fallidos en fixtures y regresiones permanentes.
- Migrar a `apache-burr==0.42.0` solo después de demostrar equivalencia funcional.
- Convertir la configuración de dependencias en una instalación reproducible.

---

## 5. No objetivos

Esta iniciativa no debe:

- cambiar el parser;
- modificar la política de oportunidades;
- alterar ranking o resultados;
- reestructurar el workflow;
- incorporar nuevas acciones de producto;
- integrar modalidad de contratación;
- modificar el warehouse;
- exponer la UI de Burr públicamente;
- instalar la UI directamente en producción;
- usar el tracking como fuente de verdad del negocio;
- sustituir la telemetría propia de SECOPPAL;
- duplicar indiscriminadamente todo el estado de Burr en otra base;
- activar tracking sobre los miles de tests unitarios;
- conservar indefinidamente trazas sin valor o sin feedback;
- modificar simultáneamente Burr y SODA.

---

## 6. Principios e invariantes

### INV-BURR-01 — Migración sin cambio funcional

La actualización debe conservar:

- nodos;
- transiciones;
- entrypoint;
- rutas;
- inputs;
- outputs;
- respuesta visible;
- efectos persistentes;
- comportamiento de error.

### INV-BURR-02 — Tracking apagado por defecto

La observabilidad debe estar controlada por una variable de entorno:

```text
SECOPPAL_BURR_TRACKING_ENABLED=false
```

Solo se habilita explícitamente en desarrollo o auditoría.

### INV-BURR-03 — Cero secretos y mínima información personal

No deben registrarse:

- tokens;
- contraseñas;
- cookies;
- credenciales;
- cabeceras de autenticación;
- contenidos sensibles innecesarios.

Debe revisarse si el estado incluye correo, identificadores de usuario, consultas privadas o filas completas.

### INV-BURR-04 — UI local y no pública

La UI se usa inicialmente en `localhost`. No debe exponerse en el VPS ni detrás del frontend sin:

- autenticación;
- autorización administrativa;
- política de retención;
- redacción de información;
- separación por entorno.

### INV-BURR-05 — Un solo paquete proveedor del namespace

No deben coexistir en el mismo entorno:

```text
burr
apache-burr
```

Ambos proporcionan el namespace Python `burr` y su coexistencia puede producir un entorno ambiguo.

### INV-BURR-06 — Trabajo aislado

La migración debe realizarse en:

- worktree dedicado;
- entorno virtual nuevo;
- rama propia;
- sin modificar el `.venv` principal;
- sin mezclarla con otra feature.

---

## 7. Arquitectura propuesta

### 7.1 Configuración inicial de tracking

La aplicación debe construir el tracker únicamente cuando el flag esté activo.

Esquema esperado:

```python
builder = ApplicationBuilder()

if settings.burr_tracking_enabled:
    builder = builder.with_tracker(
        "local",
        project=settings.burr_tracking_project,
    )
```

Variables sugeridas:

```text
SECOPPAL_BURR_TRACKING_ENABLED=false
SECOPPAL_BURR_TRACKING_PROJECT=secoppal-dev
SECOPPAL_BURR_TRACKING_DIR=
```

La ruta concreta del tracker debe confirmarse con la API real de la versión instalada. No debe asumirse una opción no soportada.

### 7.2 Frontera de privacidad

Antes de activar tracking se debe inventariar el estado Burr:

| Categoría | Ejemplo | Política |
|---|---|---|
| Consulta del usuario | `raw_query` | Permitida solo en local; revisar retención |
| Parámetros resueltos | objeto, entidad, fecha | Permitidos |
| SoQL | consulta generada | Permitido, sin tokens |
| Resultados SECOP | filas | Minimizar; preferir IDs y conteos |
| Identidad | email/user_id | Redactar o hashear |
| Credenciales | app token | Prohibidas |
| Excepciones | stack/error | Permitidas sin secretos |



### 7.3 Arquitectura de persistencia para dogfood

No se debe escoger entre JSON o base de datos como si fueran alternativas excluyentes. Cada capa cumple una función distinta:

```text
Burr tracking nativo
→ evidencia cruda de acciones, estados y tiempos

SQLite SECOPPAL
→ índice, etiquetas, feedback, commit y consultas operativas

JSON/Markdown exportado
→ paquete portátil para Claude, Codex y auditorías
```

Burr debe conservar la traza técnica. SECOPPAL debe añadir una capa delgada de contexto de producto y juicio humano.

### 7.4 Identificadores y proyectos

Cada consulta de dogfood debe recibir un identificador estable, por ejemplo:

```text
DOG-20260713-0001
```

Ese identificador debe enlazar como mínimo:

```text
dogfood_run_id
burr_project
burr_app_id
burr_partition_key
trace_location
feedback_id
git_sha
```

Separación inicial de proyectos:

```text
secoppal-dogfood
secoppal-tests
secoppal-dev
secoppal-production   # diferido
```

Producción no debe activarse durante el piloto.

### 7.5 Índice SQLite propuesto

La tabla inicial puede denominarse `dogfood_runs` e incluir:

```text
run_id
burr_project
burr_app_id
burr_partition_key
query_text
query_hash
session_hash
started_at
finished_at
git_sha
branch
burr_version
python_version
entrypoint
final_status
result_count
user_rating
expected_behavior
observed_problem
severity
notes
trace_location
```

No se deben duplicar snapshots completos de estado si Burr ya los conserva. El índice debe guardar referencias, metadatos y campos útiles para búsqueda.

### 7.6 Exportación para agentes

Debe existir una salida canónica en JSON y una versión compacta en Markdown. El contenido mínimo es:

```text
metadata de ejecución
consulta y contexto relevante
respuesta visible
acciones recorridas
duración por acción
delta de estado por paso
SoQL final
IDs candidatos, verificados y mostrados
errores y fallbacks
feedback humano
commit y versión de dependencias
```

Para agentes, el delta de estado debe priorizarse sobre la repetición de snapshots completos. Ejemplo conceptual:

```json
{
  "sequence": 4,
  "action": "apply_context",
  "changed": {
    "departamento": {"from": "Cundinamarca", "to": "Cesar"}
  },
  "unexpected_retained": {
    "entidad_contains_all": ["ICBF", "CUNDINAMARCA"]
  }
}
```

### 7.7 Integración selectiva con tests

No se debe capturar automáticamente toda la suite. El tracking se limita inicialmente a:

- golden tests del workflow;
- corpus E2E seleccionado;
- regresiones conversacionales;
- modalidad de contratación;
- dual dataset;
- timeout y degradación;
- casos reales promovidos desde dogfood.

Marcador sugerido:

```python
@pytest.mark.burr_trace
```

Ejecución sugerida:

```bash
pytest -m burr_trace
```

---

## 8. Hipótesis a validar

Estas hipótesis no se consideran demostradas. El piloto debe intentar confirmarlas o falsarlas.

### H-BURR-01 — El tracking nativo captura evidencia suficiente

**Hipótesis:** Burr registra con suficiente fidelidad la secuencia de acciones, estados y tiempos para evitar construir un segundo motor de trazas.

**Evidencia mínima:** diez consultas representativas reconstruibles sin revisar logs externos.

**Falsación:** faltan transiciones críticas, estados relevantes o correlación estable con la respuesta visible.

### H-BURR-02 — Un `app_id` estable permite unir ejecución y feedback

**Hipótesis:** una clave común permite navegar de una consulta calificada a su traza Burr sin heurísticas.

**Evidencia mínima:** 100% de los casos piloto enlazados mediante `dogfood_run_id`/`burr_app_id`.

**Falsación:** colisiones, trazas huérfanas o imposibilidad de recuperar la ejecución correspondiente.

### H-BURR-03 — SQLite debe ser índice, no duplicado del tracker

**Hipótesis:** guardar metadatos, etiquetas y referencias en SQLite es suficiente y evita duplicar grandes estados.

**Evidencia mínima:** consultas operativas útiles por estado, feature, commit, severidad y rating.

**Falsación:** las consultas de diagnóstico exigen copiar persistentemente todos los snapshots de Burr.

### H-BURR-04 — Los deltas de estado son más útiles para agentes

**Hipótesis:** Claude/Codex diagnostican con mayor precisión y menor ruido usando cambios por paso que leyendo snapshots completos repetidos.

**Evidencia mínima:** auditoría comparativa de al menos cinco fallos conocidos.

**Falsación:** el delta pierde contexto causal necesario o produce diagnósticos peores.

### H-BURR-05 — La UI y los exports cumplen funciones distintas

**Hipótesis:** la UI es mejor para exploración humana y el JSON/Markdown es mejor para agentes, archivo y comparación.

**Evidencia mínima:** ambos formatos resuelven al menos un caso donde el otro resulta menos práctico.

**Falsación:** uno de los dos formatos no añade utilidad material.

### H-BURR-06 — El tracking selectivo de tests evita ruido

**Hipótesis:** rastrear solo pruebas E2E y regresiones seleccionadas produce suficiente cobertura diagnóstica sin generar miles de trazas inútiles.

**Evidencia mínima:** suite marcada pequeña, estable y consultable.

**Falsación:** los defectos relevantes aparecen principalmente en tests no trazados y no pueden reproducirse.

### H-BURR-07 — Dogfood puede convertirse en regresión casi mecánicamente

**Hipótesis:** una consulta marcada como incorrecta puede transformarse en fixture con contexto, expected behavior y evidencia de flujo.

**Evidencia mínima:** al menos tres casos reales convertidos en tests y comparados antes/después.

**Falsación:** la traza carece de datos suficientes o el fixture requiere reconstrucción manual extensa.

### H-BURR-08 — El costo operativo es aceptable

**Hipótesis:** tracking activo en dogfood añade menos de 5% de overhead p50 y un volumen de almacenamiento manejable.

**Evidencia mínima:** medición sobre al menos 30 ejecuciones.

**Falsación:** latencia, tamaño o complejidad de limpieza superan el valor obtenido.

### H-BURR-09 — La privacidad puede controlarse

**Hipótesis:** redacción, hashing y minimización impiden almacenar secretos o PII innecesaria.

**Evidencia mínima:** auditoría de muestras con cero secretos y política explícita de retención.

**Falsación:** el tracker persiste campos sensibles imposibles de excluir de forma razonable.

### H-BURR-10 — La observabilidad reduce tiempo de diagnóstico

**Hipótesis:** una ejecución indexada y exportable reduce materialmente el tiempo para localizar el paso defectuoso.

**Evidencia mínima:** comparación con al menos tres bugs o regresiones históricas.

**Falsación:** el diagnóstico sigue dependiendo principalmente de lectura manual del código y logs dispersos.

---

## 9. Plan por fases

# Fase B0 — Baseline y reproducibilidad

## Objetivo

Congelar el estado previo a cualquier cambio.

## Tareas

1. Registrar versión Python, `pip freeze` y dependencias directas.
2. Capturar suite completa, corpus y golden tests.
3. Exportar la representación de nodos y transiciones.
4. Verificar árbol limpio.
5. No modificar el `.venv` principal.

## Entregables

```text
artifacts/burr_migration/baseline_environment.txt
artifacts/burr_migration/baseline_tests.json
artifacts/burr_migration/baseline_graph.json
docs/technical/BURR_BASELINE.md
```

## Gate

```text
B0_READY
B0_BLOCKED_UNREPRODUCIBLE_ENVIRONMENT
```

## Modelo recomendado

**GPT-5.6 Codex, razonamiento alto.**

---

# Fase B1 — Piloto de tracking con Burr 0.40.2

## Objetivo

Comprobar si la observabilidad aporta valor antes de migrar.

## Alcance

- Mantener `burr==0.40.2`.
- Añadir tracking local detrás de flag.
- No instalar UI en el entorno principal.
- No cambiar acciones ni transiciones.

## Consultas de prueba

1. búsqueda simple;
2. follow-up de año;
3. cambio de departamento;
4. cambio de dataset;
5. búsqueda de oportunidad;
6. phrase-aware;
7. dual dataset;
8. refinamiento por chip;
9. timeout con degradación;
10. consulta analítica.

## Evidencia requerida

Para cada caso:

- secuencia de acciones;
- estado inicial y final;
- claves modificadas;
- duración;
- error o degradación;
- correspondencia con la respuesta visible;
- utilidad diagnóstica.

## Gate

```text
B1_GO_UI_AND_MIGRATION
B1_GO_TRACKING_ONLY
B1_NO_GO_LOW_VALUE
B1_BLOCKED_PRIVACY
```

## Modelo recomendado

**GPT-5.6 Codex, razonamiento alto.**

---

# Fase B1A — Bitácora persistente de dogfood

## Objetivo

Enlazar cada consulta de dogfood con una ejecución Burr y un registro indexado en SQLite.

## Tareas

- generar `dogfood_run_id` estable;
- usarlo como `burr_app_id` o correlación equivalente;
- crear tabla `dogfood_runs`;
- enlazar feedback y traces existentes;
- guardar commit, rama, versiones y flags;
- permitir calificación `correcto/parcial/incorrecto`;
- permitir expected behavior, severidad y notas;
- definir retención y limpieza.

## Gate

```text
B1A_DOGFOOD_INDEX_WORKING
B1A_PARTIAL_TRACE_LINKAGE
B1A_BLOCKED_DATA_MODEL
```

## Modelo recomendado

**GPT-5.6 Codex, razonamiento alto.**

---

# Fase B1B — Exportación para Claude/Codex

## Objetivo

Producir paquetes portátiles y auditables sin exigir acceso directo a la UI o al repositorio completo.

## Comandos objetivo

```bash
python scripts/dogfood_runs.py list --failed
python scripts/dogfood_runs.py show DOG-20260713-0001
python scripts/dogfood_runs.py export DOG-20260713-0001 --format markdown
python scripts/dogfood_runs.py export DOG-20260713-0001 --format json
python scripts/dogfood_runs.py compare DOG-ANTES DOG-DESPUES
```

## Gate

```text
B1B_AGENT_EXPORT_WORKING
B1B_COMPARE_WORKING
B1B_NO_GO_LOW_SIGNAL
```

## Modelo recomendado

**GPT-5.6 Codex, razonamiento alto.**

---

# Fase B1C — Conversión de dogfood en regresiones

## Objetivo

Convertir casos reales problemáticos en fixtures reproducibles y tests permanentes.

## Flujo esperado

```text
consulta real
→ rating negativo o parcial
→ traza Burr
→ diagnóstico
→ fixture
→ fix
→ comparación antes/después
→ test de regresión
```

## Gate

```text
B1C_THREE_REAL_CASES_PROMOTED
B1C_PARTIAL_AUTOMATION
B1C_NO_GO_INSUFFICIENT_EVIDENCE
```

## Modelo recomendado

**GPT-5.6 Codex, Extra High**, con revisión de **Opus alto** sobre causalidad.

---

# Fase B2 — UI temporal en entorno descartable

## Objetivo

Visualizar las trazas sin alterar el entorno de SECOPPAL.

## Estrategia

Crear un entorno temporal e instalar:

```text
burr[start]==0.40.2
```

o, si B1 ya justifica la migración:

```text
apache-burr[start]==0.42.0
```

## Reglas

- no instalar `loguru` individualmente;
- no añadir extras al `.venv` principal;
- no exponer el puerto fuera de localhost;
- no conservar indefinidamente las trazas;
- documentar cómo iniciar y detener la UI.

## Gate

```text
B2_UI_WORKING
B2_UI_INCOMPATIBLE_WITH_TRACKING_DATA
B2_BLOCKED_DEPENDENCY_CONFLICT
```

## Modelo recomendado

**GLM-5.2** para instalación mecánica, con revisión de **GPT-5.6 Codex**.

---

# Fase B3 — Migración a Apache Burr 0.42

## Objetivo

Sustituir el paquete legado por la distribución actual de Apache Burr.

## Cambio de dependencia

```text
burr==0.40.2
→
apache-burr==0.42.0
```

Los imports deben mantenerse bajo el namespace:

```python
from burr.core import ...
```

salvo evidencia contraria.

## Procedimiento

1. Crear worktree y `.venv` nuevos.
2. Instalar dependencias desde el archivo canónico.
3. Desinstalar `burr`.
4. Instalar `apache-burr==0.42.0`.
5. Ejecutar import smoke.
6. Ejecutar compilación.
7. Ejecutar tests Burr/workflow.
8. Ejecutar golden tests.
9. Ejecutar corpus.
10. Ejecutar suite completa.
11. Comparar grafo antes/después.
12. Comparar trazas y respuestas.
13. Probar tracking/UI.
14. Revisar Pydantic.
15. Fijar versión exacta.

## Prohibiciones

- no adoptar APIs nuevas durante la migración;
- no reorganizar acciones;
- no cambiar transiciones;
- no corregir bugs funcionales en el mismo commit;
- no combinar la migración con modalidad;
- no instalar simultáneamente el paquete antiguo y el nuevo.

## Gates

```text
B3_MIGRATION_EQUIVALENT
B3_PARTIAL_COMPATIBILITY
B3_NO_GO_REGRESSION
B3_BLOCKED_PYTHON_COMPATIBILITY
```

## Modelo recomendado

**GPT-5.6 Codex, Extra High.**

Auditoría posterior: **Opus, razonamiento alto.**

---

# Fase B4 — Observabilidad operativa de desarrollo

## Objetivo

Convertir el tracking en una herramienta estable de ingeniería.

## Tareas

- documentar activación y desactivación;
- definir retención;
- redactar PII;
- excluir secretos;
- crear runbook;
- definir escenarios canónicos;
- medir overhead;
- decidir si la UI puede montarse bajo acceso administrativo.

## Restricción de producción

No integrar la UI al VPS sin:

```text
autenticación
autorización admin
HTTPS
redacción
retención
aislamiento de entorno
```

## Gate

```text
B4_DEV_OBSERVABILITY_CLOSED
B4_PRODUCTION_UI_DEFERRED
```

---

# Fase B5 — Cierre y ADR

## Entregables

```text
docs/technical/BURR_TRACKING_RUNBOOK.md
docs/technical/BURR_MIGRATION_CLOSEOUT.md
docs/decisions/YYYY-MM-DD_ADR-XXX_apache-burr-observability.md
```

## Dictamen final permitido

```text
BURR_GO_042_WITH_TRACKING
BURR_GO_042_NO_UI
BURR_KEEP_040_TRACKING_ONLY
BURR_NO_GO
```

---

## 10. Matriz mínima de regresión

### Workflow

- mismos nodos;
- mismas transiciones;
- mismo entrypoint;
- mismas acciones terminales;
- rutas especiales sin regresión.

### Conversación

- follow-up mantiene filtros compatibles;
- pivote de estado no hereda estado anterior;
- cambio de dataset purga parámetros incompatibles;
- dual dataset conserva grupos;
- paginación sigue la ruta esperada.

### Ejecución

- query simple;
- cero resultados;
- timeout;
- fallback sin orden;
- analytics;
- direct lookup;
- refinement.

### Presentación

- mismo texto o equivalencia aprobada;
- mismos IDs visibles;
- mismos conteos;
- mismas sugerencias;
- mismo orden.

### Persistencia

- trazas propias intactas;
- conversación intacta;
- radar intacto;
- tracking Burr no sustituye bases existentes;
- cada dogfood run enlaza una única ejecución Burr;
- no hay trazas huérfanas ni colisiones de `app_id`;
- export JSON/Markdown reproduce acciones y deltas;
- comparación antes/después identifica cambios;
- limpieza por retención no elimina casos promovidos a regresión.

---

## 11. Métricas de éxito

| Métrica | Objetivo |
|---|---:|
| Regresiones funcionales | 0 |
| Diferencias de nodos/transiciones | 0 no justificadas |
| Overhead con tracking apagado | 0 o despreciable |
| Overhead con tracking activo | medir; objetivo inicial <5% |
| Secretos registrados | 0 |
| Casos visibles en UI | ≥10 |
| Dogfood runs enlazados con traza | 100% del piloto |
| Exportaciones JSON/Markdown válidas | ≥10 |
| Casos reales convertidos en regresión | ≥3 |
| Trazas con secretos | 0 |
| Trazas huérfanas/colisiones | 0 |
| Tiempo de diagnóstico | mejora demostrada en ≥3 casos |
| Instalación reproducible | sí |
| `burr` y `apache-burr` coexistentes | no |

---

## 12. Riesgos y mitigaciones

### Riesgo: proyecto Beta/incubating

**Mitigación:** versión fija, tests exhaustivos y migración reversible.

### Riesgo: PII en tracking

**Mitigación:** redacción, flag local y retención limitada.

### Riesgo: cambio invisible de semántica

**Mitigación:** golden tests, comparación de grafo y consultas reales.

### Riesgo: sobreinvertir en observabilidad

**Mitigación:** B1 es un piloto con gate de utilidad.

### Riesgo: crear otra base de telemetría redundante

**Mitigación:** SQLite funciona como índice y capa de feedback; Burr conserva la evidencia cruda.

### Riesgo: generar miles de trazas de tests sin valor

**Mitigación:** marcador selectivo `burr_trace` y proyectos separados.

### Riesgo: pérdida de trazabilidad entre feedback y ejecución

**Mitigación:** `dogfood_run_id` estable y validación de integridad referencial.

### Riesgo: exports demasiado grandes para agentes

**Mitigación:** formato compacto basado en deltas, IDs y campos esenciales; snapshot completo solo bajo demanda.

---

## 13. Prioridad dentro del roadmap

```text
1. cerrar warehouse y cutover mínimo
2. fijar entorno reproducible
3. piloto Burr tracking 0.40
4. bitácora mínima de dogfood + enlace feedback/traza
5. export JSON/Markdown y comparación antes/después
6. decidir GO/NO-GO
7. migrar a Apache Burr 0.42
8. usar tracking durante modalidad y radar
```

Burr no debe bloquear el discovery SODA3.

---

## 14. Recomendación de modelos

| Trabajo | Modelo |
|---|---|
| Baseline y scripts mecánicos | GLM-5.2 o Kimi K2.7 Code |
| Integración de tracking | GPT-5.6 Codex alto |
| Índice dogfood y exportador | GPT-5.6 Codex alto |
| Comparador y promoción a regresión | GPT-5.6 Codex Extra High |
| Migración 0.40→0.42 | GPT-5.6 Codex Extra High |
| Auditoría causal | Opus alto |
| Documentación final | GPT-5.6 Codex medio/alto |

---

## 15. Dictamen preliminar

```text
DEUDA:                         REAL
URGENCIA:                      MEDIA
VALOR DIRECTO PARA USUARIO:    MEDIO EN DOGFOOD
VALOR PARA CALIDAD:            MUY ALTO
VALOR PARA AGENTES:            MUY ALTO
VALOR PARA MODALIDAD:          ALTO
RIESGO DE MIGRACIÓN:           MEDIO
ESTRATEGIA:                    PILOTO → MIGRACIÓN AISLADA
```

La actualización de Burr es recomendable, pero el primer paso correcto es demostrar que el tracking visual y la bitácora persistente reducen el costo de comprender, auditar y convertir dogfood en regresiones de SECOPPAL.

---

## 16. Registro en backlog técnico

Estas iniciativas quedan registradas como deudas independientes y no deben mezclarse con el cierre actual del warehouse:

### BACKLOG-PLATFORM-01 — SODA3 discovery y transporte moderno

**Documento rector:** `PLAN_DEUDA_TECNICA_SODA3_SECOPPAL.md`  
**Prioridad:** alta para discovery; migración condicionada a evidencia.  
**Inicio recomendado:** después del cierre y congelamiento mínimo del warehouse.

### BACKLOG-PLATFORM-02 — Burr 0.42 + bitácora de dogfood

**Documento rector:** `PLAN_DEUDA_TECNICA_BURR_SECOPPAL.md`  
**Prioridad:** media-alta.  
**Alcance:** tracking, SQLite index, exports para agentes, comparación de ejecuciones, promoción a regresiones, UI local y migración posterior a Apache Burr 0.42.

### Regla de secuenciación

```text
warehouse cerrado/congelado
→ baseline reproducible
→ SODA3 discovery y Burr dogfood en bloques aislados
→ ninguna migración simultánea
```

---

## 17. Fuentes verificadas

- Apache Burr UI: instalación con `apache-burr[start]`, servidor local en puerto 7241 y conexión mediante `with_tracker`.
- PyPI: `apache-burr 0.42.0`, publicado el 10 de mayo de 2026.
- Evidencia local SECOPPAL: `burr==0.40.2`, core y tracking disponibles, plugin `start` ausente.

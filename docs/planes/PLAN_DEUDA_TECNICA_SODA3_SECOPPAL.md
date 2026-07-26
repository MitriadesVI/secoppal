# SECOPPAL — Plan de deuda técnica y discovery SODA3

**Estado:** borrador operativo  
**Fecha de referencia:** 2026-07-13  
**Prioridad:** **Platform P1** para discovery; migración condicionada a evidencia  
**Tipo de iniciativa:** modernización estratégica del transporte de datos  
**Cliente actual:** `sodapy==2.2.0`  
**API actual:** SODA clásica mediante `/resource/`  
**API objetivo a evaluar:** SODA3 mediante `/api/v3/views/{dataset}/query` y `/export`

---

## 1. Resumen ejecutivo

SECOPPAL consulta actualmente `datos.gov.co` mediante `sodapy==2.2.0`. El cliente construye peticiones contra el endpoint clásico:

```text
GET /resource/{dataset}.json
```

y envía SoQL mediante parámetros como `$query`.

La versión instalada de `sodapy` es la última disponible, pero el proyecto está sin mantenimiento desde el 31 de agosto de 2022 y declara compatibilidad únicamente hasta Python 3.10. SECOPPAL lo ejecuta sobre Python 3.14.5.

SODA3 no es una nueva versión de `sodapy`. Es un contrato HTTP diferente que usa principalmente `POST`, separa consultas y exportaciones y expone opciones explícitas de paginación, timeout y ordenamiento.

La oportunidad potencial es mayor que una simple actualización de dependencia. SODA3 podría mejorar:

- transporte de consultas largas;
- control de timeout;
- paginación;
- clasificación de errores;
- exportaciones;
- ingestión de warehouse;
- independencia frente a una biblioteca abandonada.

Sin embargo, SODA3 puede mejorar **cómo se accede al dato**, pero no necesariamente:

- la frescura del dataset;
- su cobertura;
- los campos disponibles;
- la presencia de procesos ausentes de datos abiertos;
- los errores de calidad de SECOP.

Por eso la estrategia correcta es un discovery y A/B controlado, no un reemplazo inmediato.


La tesis estratégica revisada es:

> **SODA3 puede convertirse en el transporte remoto preferido de SECOPPAL, pero no sustituye su inteligencia semántica, su gobernanza ni el warehouse.**

La evaluación debe distinguir tres capas que no pueden confundirse:

```text
1. transporte del OR
   POST, longitud, encoding, timeouts y protocolo

2. ejecución remota del OR
   rendimiento, caching, orden, errores y estabilidad del backend

3. semántica del OR
   términos, anchors, agrupación, deduplicación, ruido y ranking
```

SODA3 podría mejorar las dos primeras. La tercera continúa siendo responsabilidad del parser, el resolver, el plan canónico, `SoQLBuilder`, la validación phrase-aware y el ranking de SECOPPAL.

El discovery se eleva a **Platform P1** porque las búsquedas complejas con ecosistemas institucionales, variantes, frases, exclusiones y grandes cláusulas `OR` representan una capacidad central del producto. Esta prioridad autoriza investigación y benchmark; no autoriza todavía un cutover.

---

## 2. Estado confirmado del entorno

```text
Python:                    3.14.5
sodapy:                    2.2.0
sodapy mantenido:          no
compatibilidad declarada:  Python 3.5–3.10
endpoint actual:           /resource/
método actual:             GET
SODA3 activo:              no
requests:                  2.33.1
httpx:                     0.28.1
```

Uso observado:

```text
app/core/secop_client.py
→ from sodapy import Socrata
→ Socrata(...)
→ client.get(dataset_id, query=soql)
```

También existen scripts con URLs explícitas `/resource/`.

---

## 3. Problema que se quiere resolver

### 3.1 Dependencia abandonada

`sodapy` no recibirá nuevas funciones ni correcciones. Esto crea riesgo futuro frente a:

- Python 3.14+;
- cambios de Requests;
- cambios de Socrata;
- manejo de errores;
- timeouts;
- compatibilidad con SODA3.

### 3.2 Control de transporte limitado

La capa actual hereda decisiones internas de `sodapy` sobre:

- construcción de URL;
- sesión;
- timeouts;
- serialización;
- clasificación de errores;
- reintentos;
- paginación.

SECOPPAL ya tuvo que diseñar clientes derivados para evitar mutaciones concurrentes del timeout.

### 3.3 Potencial de SODA3

Socrata recomienda `POST` para consultas porque permite consultas más largas y opciones más claras. SODA3 separa:

```text
/query   → consultas paginadas y configurables
/export  → exportación completa
```

También ofrece:

- `page`;
- `timeout`;
- `includeSystem`;
- `includeSynthetic`;
- `orderingSpecifier`;
- `serializationOptions`.

El valor real en `datos.gov.co` debe demostrarse empíricamente.


### 3.4 Hallazgo independiente: semántica ambigua de reintentos

La revisión externa identificó un posible desajuste contractual en el cliente actual:

```python
max_retries = 1
for attempt in range(max_retries):
    ...
```

Con esa forma, el valor predeterminado representa **un intento total**, no "un intento inicial más un reintento". Esto debe auditarse de forma quirúrgica e independiente de la migración SODA3.

No debe corregirse aumentando llamadas a ciegas. El contrato correcto debe definir explícitamente:

```text
max_attempts
errores retryable
presupuesto restante del deadline
backoff máximo
timeout por intento
telemetría de cada intento
```

Un retry solo es admisible cuando queda presupuesto suficiente y el error es transitorio. Un retry automático que consuma el deadline conversacional puede empeorar el producto.

---

## 4. Objetivos

### 4.1 Objetivo principal

Determinar si SODA3 ofrece una mejora material y segura para búsqueda, radar, verificación y warehouse.

### 4.2 Objetivos específicos

- Verificar soporte real de SODA3 en datasets SECOP.
- Medir paridad contra `/resource/`.
- Comparar latencia, timeouts y errores.
- Probar consultas largas y funciones SoQL.
- Medir específicamente grandes cláusulas `OR`, ecosistemas institucionales, variantes y exclusiones.
- Separar la ganancia atribuible al transporte de la atribuible a simplificaciones del builder.
- Evaluar `/export` para warehouse.
- Evaluar paginación formal.
- Diseñar un transporte reemplazable.
- Ejecutar SODA3 en shadow antes del cutover.
- Reducir dependencia de `sodapy`.
- Preservar fallback hasta cerrar la migración.

---

## 5. No objetivos

La iniciativa no debe asumir:

- mayor frescura;
- cobertura de procesos ausentes;
- acceso al portal transaccional;
- actualización sub-hora;
- corrección de errores del dataset;
- eliminación inmediata de `sodapy`;
- reescritura del SoQL builder;
- cambio simultáneo de ranking, parser o modalidad;
- sustitución de Socrata como verificador;
- convertir SECOPPAL en cliente genérico de datasets Socrata;
- construir capacidades geoespaciales sin datasets SECOP con geometrías útiles;
- exponer SoQL arbitrario al usuario;
- presentar `pageNumber` como solución a la paginación profunda;
- asumir que `orderingSpecifier=discard` es seguro en resultados visibles.

---

## 6. Principios e invariantes

### INV-SODA-01 — Mismo dataset, misma limitación de cobertura

SODA3 no debe presentarse como solución a procesos que no existen en datos abiertos.

### INV-SODA-02 — Transporte separado de la semántica

El SoQL y los parámetros canónicos pertenecen a SECOPPAL. El transporte solo envía y normaliza.

### INV-SODA-03 — Shadow antes de cutover

SODA3 no se convierte en transporte visible hasta demostrar paridad.

### INV-SODA-04 — Fallback explícito y observable

El fallback clásico debe tener razón estable y no ocultar divergencias.

### INV-SODA-05 — Errores tipados

El transporte debe distinguir:

```text
timeout
rate_limit
bad_query
authentication
not_found
transient_server
permanent_server
network
decode
contract_violation
```

### INV-SODA-06 — Sin mutación global de timeout

Cada request recibe timeout explícito.

### INV-SODA-07 — Verificación Socrata se conserva

El warehouse sigue siendo candidate pool.

### INV-SODA-08 — Migración reversible

Debe poder volverse al transporte clásico mediante configuración.


### INV-SODA-09 — Transporte nuevo no implica semántica mejor

SODA3 puede transportar o ejecutar mejor una consulta. No corrige automáticamente:

- agrupaciones incorrectas de `AND`/`OR`;
- aliases redundantes;
- anchors débiles;
- términos ruidosos;
- expansiones excesivas;
- errores de ranking.

Toda mejora debe atribuirse a la capa correcta.

### INV-SODA-10 — Timeout en capas

El timeout declarado a SODA3 no sustituye el deadline de producto. El adapter debe respetar:

```text
server_timeout <= tiempo restante del deadline
client_connect_timeout
client_read_timeout
hard application deadline
```

No se debe heredar el timeout remoto predeterminado cuando exceda el presupuesto conversacional.

### INV-SODA-11 — Orden descartable solo cuando no sea semántico

`orderingSpecifier=discard` solo puede utilizarse cuando:

- el orden no forma parte de la respuesta;
- el conjunto será procesado completamente o validado por claves;
- no se usa para paginación visible;
- no se requiere reproducibilidad de posiciones.

Debe prohibirse en top por valor, top por fecha y páginas visibles ordenadas.

---

## 7. Arquitectura objetivo

### 7.1 Interfaz de transporte

```python
class SocrataTransport(Protocol):
    def query(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float,
        page_number: int | None = None,
        page_size: int | None = None,
        ordering: str = "total",
    ) -> list[dict]:
        ...

    def export(
        self,
        dataset_id: str,
        *,
        query: str | None,
        format: str,
        timeout_seconds: float,
    ) -> ExportResponse:
        ...
```

### 7.2 Implementaciones

```text
SodapyClassicTransport
→ GET /resource/{dataset}.json

Soda3HttpTransport
→ POST /api/v3/views/{dataset}/query.json
→ POST /api/v3/views/{dataset}/export.csv
```

### 7.3 Fachada

```text
SecopClient
    ↓
SocrataTransport
    ├── classic
    └── soda3
```


### 7.4 Contratos ricos de resultado y error

El adapter no debe devolver únicamente `list[dict]`. Debe conservar información útil para gobernanza, comparación y fallback:

```python
@dataclass(frozen=True)
class QueryResult:
    rows: list[dict]
    transport: str
    elapsed_ms: float
    order_guarantee: str
    page_number: int | None
    page_size: int | None
    request_id: str | None
    partial: bool
    attempt_count: int
```

Los errores deben normalizarse sin perder la causa original:

```python
@dataclass(frozen=True)
class TransportError(Exception):
    category: str
    http_status: int | None
    remote_code: str | None
    retryable: bool
    request_id: str | None
    attempt_count: int
```

Esto permite que workflow, radar y warehouse distingan un timeout, un 429, una consulta inválida y una violación del contrato remoto.

### 7.5 Modos de operación

```text
classic_only
shadow_soda3
soda3_preferred
soda3_only
```

El modo inicial es `classic_only`.

---

## 8. Plan por fases

# Fase S0 — Baseline y reproducibilidad

## Objetivo

Congelar el comportamiento clásico.

## Tareas

- capturar versiones y dependencias;
- declarar `sodapy==2.2.0`;
- inventariar usos directos de `Socrata`;
- inventariar URLs `/resource/`;
- capturar consultas reales, resultados y latencias;
- identificar scripts fuera de `SecopClient`;
- auditar `max_retries` versus `max_attempts`;
- capturar política real de retry, deadline y backoff;
- congelar el comportamiento de `include_order=False`.

## Entregables

```text
docs/technical/SODA_CLASSIC_BASELINE.md
artifacts/soda3/classic_query_corpus.json
artifacts/soda3/classic_latency_baseline.json
artifacts/soda3/direct_sodapy_usage_inventory.csv
```

## Gate

```text
S0_READY
S0_BLOCKED_INCOMPLETE_INVENTORY
```

## Modelo recomendado

**GPT-5.6 Codex, razonamiento alto.**

---

# Fase S1 — Probe de compatibilidad SODA3

## Objetivo

Confirmar soporte en datasets principales.

## Datasets mínimos

```text
p6dx-8zbt  procesos SECOP II
jbjy-vk9h  contratos
```

## Pruebas

1. `SELECT * LIMIT 1`;
2. selección de columnas;
3. `WHERE`;
4. `LIKE`;
5. `unaccent`;
6. `IN`;
7. fechas;
8. valores;
9. `COUNT(*)`;
10. `GROUP BY`;
11. `ORDER BY`;
12. página SODA3;
13. consulta larga;
14. nulos;
15. app token;
16. timeout;
17. query inválida;
18. dataset inválido;
19. `orderingSpecifier=total`;
20. `orderingSpecifier=discard`.

## Resultado por prueba

```text
SUPPORTED
SUPPORTED_WITH_DIFFERENCE
UNSUPPORTED
INCONCLUSIVE
```

## Gate

```text
S1_SUPPORTED
S1_PARTIAL
S1_NO_GO
```

## Modelo recomendado

**GPT-5.6 Codex, Extra High.**

---


# Fase S2A — Benchmark específico de OR complejos

## Objetivo

Determinar si SODA3 aporta una mejora material en las consultas que más presionan el transporte y el backend remoto, y separar esa ganancia de posibles defectos o redundancias del compilador actual.

## Familias mínimas

```text
OR-05       5 términos
OR-20       20 términos
OR-50       50 términos
OR-100      100 términos
ENTITY      ecosistema institucional y aliases oficiales
PHRASE      variantes morfológicas + anchors
NEGATION    OR positivo + exclusiones
COMBINED    entidad + tema + territorio + fecha + estado
```

## Variantes por caso

```text
A. /resource/ clásico con el SoQL actual
B. SODA3 POST con exactamente el mismo SoQL y orden total
C. SODA3 POST con discard, solo cuando el orden no sea semántico
D. SODA3 POST con OR canónico deduplicado
```

La variante D no se usa para atribuir toda la mejora a SODA3. Sirve para medir cuánto costo proviene de ramas redundantes, aliases repetidos o agrupaciones que podrían simplificarse en `SoQLBuilder`.

## Evidencia por ejecución

- SoQL exacto y longitud;
- bytes de request;
- cantidad de ramas `OR`;
- cantidad de términos únicos;
- IDs y conteos;
- p50, p90 y p95;
- timeouts;
- código y categoría de error;
- orden y repetibilidad;
- precisión posterior del candidate pool;
- filas validadas y rankeadas por SECOPPAL.

## Clasificación causal

Toda diferencia debe marcarse como:

```text
TRANSPORT_LENGTH_OR_ENCODING
REMOTE_EXECUTION
ORDERING
BUILDER_REDUNDANCY
BUILDER_GROUPING
SEMANTIC_EXPANSION
RANKING_OR_VALIDATION
REMOTE_DATASET_VARIANCE
UNKNOWN
```

## Gates

```text
S2A_GO_COMPLEX_QUERY_TRANSPORT
S2A_GO_ONLY_WITH_DISCARD
S2A_BUILDER_SURGERY_NEEDED
S2A_NO_MATERIAL_GAIN
S2A_NO_GO_DIVERGENCE
```

## Modelo recomendado

**GPT-5.6 Codex, Extra High** para el harness y la instrumentación.  
**Opus, razonamiento alto** para atribución causal y decisión de arquitectura.

---

# Fase S2B — A/B de consultas reales

## Objetivo

Comparar clásico y SODA3 con el mismo SoQL.

## Corpus

Mínimo 30 consultas:

- oportunidad simple;
- contrato simple;
- phrase-aware;
- múltiples entidades;
- región;
- cuantía;
- fechas;
- estados;
- dual dataset;
- referencia;
- count;
- sum;
- top entidad;
- top modalidad;
- distribución temporal;
- SoQL largo;
- `ORDER BY` costoso;
- consulta amplia;
- caracteres acentuados;
- timeouts conocidos.

## Comparación

- status;
- IDs;
- filas;
- tipos;
- columnas;
- orden;
- latencia;
- tamaño;
- error;
- timeout;
- hash canónico.

## Divergencias

```text
ordering
pagination
serialization
type_conversion
missing_rows
extra_rows
backend_timing
query_semantics
unknown
```

## Gates

```text
S2_GO_PARITY
S2_GO_PARTIAL_USE_CASES
S2_NO_GO_DIVERGENCE
```

## Modelo recomendado

**GPT-5.6 Codex, Extra High.**

Auditoría: **Opus alto.**

---

# Fase S3 — Probe de warehouse mediante `/export`

## Objetivo

Determinar si SODA3 mejora extracción e ingestión.

## Experimentos

1. slice pequeño;
2. slice mediano;
3. ventana anual;
4. extracción sin orden;
5. filtro por fecha;
6. reintento tras interrupción;
7. descarga CSV;
8. hash y conteo;
9. conversión Parquet;
10. comparación con snapshot clásico.

## Métricas

```text
filas por segundo
bytes por segundo
memoria pico
tiempo total
reintentos
integridad CSV
conteo
duplicados
fecha máxima
hash
reanudación
```

## Gate

```text
S3_GO_WAREHOUSE_EXPORT
S3_GO_QUERY_PAGINATION
S3_KEEP_CLASSIC_WAREHOUSE
S3_NO_GO
```

## Modelo recomendado

**GPT-5.6 Codex, Extra High.**

---

# Fase S4 — Radar y verificación por clave

## Objetivo

Evaluar operaciones pequeñas y repetidas.

## Casos

- refresh de 1 ID;
- refresh de lote;
- lookup por referencia;
- resultado fuera de ventana;
- timeout;
- 429;
- error transitorio;
- batch de claves.

## Gate

```text
S4_GO_RADAR
S4_KEEP_CLASSIC_RADAR
S4_HYBRID
```

---

# Fase S5 — Transporte shadow

## Objetivo

Ejecutar SODA3 en paralelo sin afectar respuestas.

```text
visible → clásico
shadow  → SODA3
```

## Reglas

- no bloquear el turno;
- respetar presupuesto;
- usar muestreo;
- no persistir filas completas;
- registrar hashes, conteos y divergencias.

## Configuración sugerida

```text
SECOPPAL_SOCRATA_TRANSPORT=classic
SECOPPAL_SODA3_SHADOW_ENABLED=false
SECOPPAL_SODA3_SHADOW_SAMPLE_RATE=0.05
```

## Gate mínimo

```text
≥500 comparaciones
0 divergencias críticas no explicadas
timeouts no peores materialmente
errores clasificados
```

## Veredictos

```text
S5_READY_FOR_PARTIAL_CUTOVER
S5_NEEDS_MORE_EVIDENCE
S5_NO_GO
```

---

# Fase S6 — Cutover parcial o total

## Estrategias

### Híbrida

```text
SODA3 /query   → búsqueda y verificación
SODA3 /export  → warehouse
/resource/     → fallback
```

### Solo warehouse

```text
búsqueda/radar → clásico
warehouse      → SODA3
```

### Solo queries largas

```text
consultas normales → clásico
consultas largas   → SODA3
```

### Principal

```text
SODA3 → principal
clásico → fallback temporal
```

## Gates

```text
S6_GO_PRIMARY
S6_GO_PARTIAL
S6_KEEP_CLASSIC
S6_ROLLBACK
```

---

# Fase S7 — Retiro de sodapy

Solo procede cuando:

- SODA3 cubre los casos necesarios;
- no quedan scripts directos;
- el fallback ya no es necesario;
- existe evidencia prolongada;
- la suite no depende de clases de `sodapy`.

## Veredicto

```text
SODAPY_REMOVED
SODAPY_RETAINED_AS_FALLBACK
```

---


## 9. Límites interpretativos y capacidades descartadas

### 9.1 Geoespacial

Socrata ofrece funciones de puntos, polígonos y predicados espaciales. Esto no constituye una deuda actual de SECOPPAL mientras los datasets SECOP principales no aporten geometrías útiles para el producto.

```text
capacidad de plataforma: sí
oportunidad actual: no
prioridad: parking lot
```

### 9.2 Datasets arbitrarios

Que SODA3 sea horizontal no implica que SECOPPAL deba volverse genérico. Cada dataset incorporado debe seguir teniendo:

- rol semántico;
- esquema conocido;
- campos canónicos;
- política de fechas, valores y estados;
- normalización;
- política de verificación.

Un registry puede reducir hardcoding, pero no debe eliminar la gobernanza vertical.

### 9.3 Paginación profunda

La paginación formal de SODA3 mejora el contrato, pero no elimina el costo de páginas profundas. Para grandes recorridos deben preferirse:

```text
filtros que reduzcan el universo
particiones por fecha o clave
keyset cuando sea viable
/export para extracción masiva
```

No debe declararse resuelto el problema sustituyendo `OFFSET` por un número de página alto.

### 9.4 Analytics generales

SODA3 puede ejecutar SoQL analítico amplio. Exponer preguntas analíticas complejas de forma segura requiere todavía un planner, contratos semánticos, validación y narrativa de SECOPPAL. No es un beneficio automático del transporte.

---

## 10. Matriz de evaluación por producto

| Área | Hipótesis | Evidencia |
|---|---|---|
| Búsqueda | POST reduce fallos de queries largas | A/B real |
| OR complejos | transporte/ejecución mejora sin cambiar semántica | S2A por familias |
| Builder | deduplicación reduce costo sin perder recall | variante D y paridad |
| Radar | batch/paginación mejora refresh | pruebas por clave |
| Excel | `/export` facilita extracción | CSV + XLSX |
| Warehouse | mayor throughput o control | slice comparable |
| Analytics | mejor manejo de agregaciones | count/group/sum |
| Latencia | p90 menor | repeticiones |
| Estabilidad | menos timeouts | corridas repetidas |
| Cobertura | igual a clásico | IDs y fecha máxima |
| Frescura | no asumir mejora | medir |

---

## 11. Métricas y gates cuantitativos

### Paridad

| Métrica | Objetivo |
|---|---:|
| Consultas simples con mismos IDs | 100% |
| Conteos iguales | 100% o diferencia explicada |
| Valores normalizados iguales | 100% |
| Divergencias críticas | 0 |
| SoQL soportado | ≥95% para GO amplio |

### Rendimiento

| Métrica | Criterio |
|---|---|
| p50 | no peor >10% salvo beneficio |
| p90 | mejora o estabilidad superior |
| timeouts | no aumentar |
| throughput warehouse | mejora demostrable |
| queries largas fallidas | reducción demostrada |
| OR-50 y OR-100 | ejecutar con paridad o clasificar límite |
| p90 OR complejo | mejora material o estabilidad superior |
| ramas OR redundantes | cuantificadas y atribuibles |

### Operación

| Métrica | Objetivo |
|---|---:|
| Errores tipados | 100% conocidos |
| Fallback con reason code | 100% |
| Rollback por flag | sí |
| Mutación global de timeout | 0 |
| Dependencia sodapy fuera del adapter | 0 al final |

---

## 12. Riesgos y mitigaciones

### API nueva no significa datos mejores

**Mitigación:** separar acceso, cobertura y frescura.

### Diferencias de paginación

**Mitigación:** hashes, claves estables y pruebas de continuidad.

### Orden descartado

`orderingSpecifier=discard` puede ser no determinista.

**Mitigación:** usar solo cuando el orden no sea semántico.

### Exportación sin reanudación

**Mitigación:** slices, checksums y retry antes de producción.

### Doble carga shadow

**Mitigación:** muestreo bajo y presupuesto independiente.

### Diferencias de tipos

**Mitigación:** esquema canónico y tests.

---


## 13. Criterio para parche local versus cirugía sistémica

El benchmark no debe terminar únicamente en "SODA3 fue más rápido" o "falló". Debe alimentar una decisión causal.

Se abre una **cirugía sistémica** del builder/resolver cuando ocurra al menos una de estas condiciones:

- el mismo patrón falla en varias familias de consulta;
- existen ramas `OR` redundantes en múltiples features;
- una solución local requiere excepciones repetidas;
- el problema atraviesa resolver, builder y ranking;
- el parche desplaza el fallo a otra capa;
- se viola un invariante de seguridad, paridad o gobernanza;
- el corpus no representa la dimensión problemática.

Clasificaciones de cierre:

```text
TRANSPORT_FIX
BUILDER_LOCAL_FIX
BUILDER_SYSTEMIC_SURGERY
RANKING_OR_VALIDATION_FIX
REMOTE_DATA_LIMITATION
EXPECTED_BEHAVIOR
NEEDS_MORE_EVIDENCE
```

---

## 14. Seguridad

- usar `X-App-Token` sin imprimirlo;
- no persistir headers;
- usar HTTPS;
- limitar logs;
- definir tamaño máximo;
- conservar guard anti consulta global;
- no aceptar dataset IDs arbitrarios;
- validar content type;
- controlar redirects;
- usar timeouts de conexión y lectura;
- limitar exportaciones concurrentes.

---

## 15. Prioridad dentro del roadmap

```text
1. cerrar gobernanza y cutover mínimo del warehouse
2. congelar dependencias, corpus y baseline clásico
3. SODA3 S1 compatibility probe
4. SODA3 S2A benchmark de OR complejos
5. SODA3 S2B A/B general
6. SODA3 S3 probe de /export para warehouse
7. dictamen GO_PARTIAL / GO_SHADOW / NO_GO
8. Burr/dogfood en bloque separado
9. modalidad de contratación
```

El discovery debe tener presupuesto limitado para no retrasar indefinidamente features visibles.

---

## 16. Recomendación de modelos

| Trabajo | Modelo |
|---|---|
| Inventario y baseline | GPT-5.6 Codex alto |
| Probe HTTP SODA3 | GPT-5.6 Codex Extra High |
| Harness A/B | GPT-5.6 Codex Extra High |
| Extracción warehouse | GPT-5.6 Codex Extra High |
| Análisis causal | Opus alto |
| Scripts mecánicos | GLM-5.2 o Kimi K2.7 Code |
| Auditoría final | Opus alto + revisión humana |

---

## 17. Dictamen preliminar

```text
DEUDA:                         REAL
OPORTUNIDAD ESTRATÉGICA:       ALTA
URGENCIA DE DISCOVERY:         ALTA — PLATFORM P1
URGENCIA DE MIGRACIÓN:         BAJA HASTA MEDIR
POTENCIAL EN BÚSQUEDA:         ALTO, ESPECIALMENTE OR COMPLEJOS
POTENCIAL EN RADAR:            MEDIO-ALTO
POTENCIAL EN WAREHOUSE:        ALTO
POTENCIAL EN COBERTURA:        BAJO
POTENCIAL EN FRESCURA:         DESCONOCIDO
ESTRATEGIA:                    PROBE → OR BENCHMARK → A/B → SHADOW → CUTOVER
```

SODA3 debe tratarse como una iniciativa de plataforma con potencial de producto, pero su adopción depende de evidencia contra los datasets y consultas reales de SECOPPAL.


El discovery puede recomendar una adopción parcial. Son resultados válidos:

```text
SODA3 para consultas complejas y OR largos
SODA3 /export para warehouse
SODA3 para verificaciones y clásico para otros casos
SODA3 principal con clásico como fallback temporal
KEEP_CLASSIC si no existe ganancia material
```

La conclusión no debe forzarse a un reemplazo total.

---

## 18. Fuentes verificadas

- Documentación oficial Socrata SODA3: recomienda POST, separa `/query` y `/export`, y documenta `page`, `timeout` y `orderingSpecifier`.
- PyPI `sodapy`: versión 2.2.0, publicada el 31 de agosto de 2022, declarada sin mantenimiento y compatible oficialmente hasta Python 3.10.
- Evidencia local SECOPPAL: `sodapy==2.2.0`, endpoint `/resource/`, Python 3.14.5.
- Revisión externa de capacidades SODA3 versus SECOPPAL, utilizada como insumo crítico y no como autoridad de adopción.

# Entity Profile — Semantic Plan V1

**Fecha:** 2026-09-07. **Base:** `main` 7868664.
**Rama:** `fix/entity-profile-semantic-plan-v1` (sin merge, sin push).
**Decisión:** [ADR-095](../decisions/2026-09-07_ADR-095_entity-profile-semantic-plan-v1.md).

> **Este cierre fue REFUTADO por revisión independiente (2026-09-07).** Siete
> hallazgos P1: entre ellos, la «autoridad única de ejecución» y
> `RenderedScope == ExecutedScope` que este informe afirmaba eran ambas falsas
> —la primera porque `dual_dataset` seguía ejecutando después del plan, y la
> segunda porque el alcance omitía estado, exclusiones, modalidad, tipo y valor.
> El incidente Baranoa sí estaba corregido. La remediación está en
> [INDEPENDENT_REVIEW_REMEDIATION_V1_1](entity_profile_semantic_v1_1/INDEPENDENT_REVIEW_REMEDIATION_V1_1.md);
> lo que sigue se conserva como historia, no como estado vigente.

---

## 1. El incidente

```text
"analizame que contratos firmo alcaldia de baranoa en agosto"
  ->  0 procesos · 0 contratos · $0
```

Contraste en vivo el mismo día, sobre la misma entidad y el mismo periodo:

```text
SELECT count(*) WHERE nombre_entidad = 'ALCALDIA MUNICIPAL DE BARANOA'
  AND fecha_de_firma >= '2026-08-01' AND fecha_de_firma <= '2026-08-31'
  ->  47
```

El cero no venía de la fuente. Venía de tres defectos independientes que se
sumaban, y ninguno de los tres es específico de esta consulta.

### 1.1 La acción contractual sobrevivía como tema

`firmar` faltaba en `PROCUREMENT_ACTION_VERB_FORMS` mientras sus hermanos
semánticos —`celebrar`, `suscribir`, `comprar`, `adquirir`, `gastar`— ya
estaban. El criterio del módulo es morfológico y está escrito en él: *un verbo
conjugado no es un objeto contractual en ninguna lectura del español*. La
familia simplemente nació incompleta.

```text
objeto = ['analizame', 'firmo']
  ->  WHERE ... AND (unaccent(UPPER(objeto_del_contrato))
                     LIKE unaccent(UPPER('%firmo%')) OR ...)
```

Ningún contrato colombiano contiene la cadena `firmo` en su objeto. El filtro
garantizaba cero.

### 1.2 El overview consultaba fuentes que nadie pidió

`jobs_for_focus("overview")` devolvía seis llamadas fijas —dos a procesos,
cuatro a contratos— sin mirar qué fuente había nombrado la persona. Una
pregunta explícita por contratos consultaba procesos y reportaba su cero.

### 1.3 El evento temporal se confundía con el estado actual

```text
"analiza procesos de vias publicados por X en agosto"
  ->  AND estado_del_procedimiento = 'Publicado'
      AND estado_de_apertura_del_proceso = 'Abierto'
```

Medido en vivo sobre Baranoa, agosto de 2026:

| Universo | Filas |
|---|---:|
| procesos publicados en agosto | **48** |
| de ésos, hoy en estado `Publicado` | **2** |

El filtro de estado destruía el **96 %** del universo que la pregunta
describía. "Publicados en agosto" pregunta por lo que se publicó en agosto, no
por lo que sigue publicado hoy.

### 1.4 Hallazgo no previsto: la fuga entre esquemas es bidireccional

El perfil pasaba el MISMO diccionario de params a los dos constructores:

```text
[jbjy-vk9h]  ... AND estado_del_procedimiento = 'Publicado' ...
[p6dx-8zbt]  ... AND estado_contrato = 'terminado' ...
```

Un campo exclusivo de procesos viajando al esquema de contratos y viceversa.
`workflow/dual_dataset.params_for_dataset` ya resolvía esto para la ruta
tabular; Entity Profile nunca lo aplicó.

---

## 2. Mapa de autoridades

`CURRENT` es lo que había en `main` 7868664. `TARGET` es lo que hay ahora.

| Dimensión | Autoridad actual | Representación | Conflicto | Autoridad objetivo |
|---|---|---|---|---|
| comprador | `entity_resolver` / `buyer_groups` | `entidad_resolved`, `entidad_resolution` | — | **sin cambio**, consumida por `scope.resolve_profile_scope` |
| fuente solicitada | *ninguna* — se leía `params["dataset"]` | escalar | `dataset_explicit=True` afirmando una fuente que el usuario no dijo | **`source_request`** (nueva) |
| dataset de ejecución tabular | `QueryRouter._select_dataset` | escalar | — | **sin cambio** (pregunta distinta) |
| rango temporal | `temporal_semantics` | `fecha_desde`/`fecha_hasta` | — | **sin cambio** |
| evento temporal | *ninguna* | — | el rango se anclaba por defecto sin decirlo | **`temporal_event`** (nueva) |
| estado actual | `estado_families` | `estado_*` | se activaba desde el tramo del evento | **sin cambio**, releída con el tramo enmascarado |
| tema | extractor de objeto | `objeto*` | vertedero del residual | **sin cambio**, con purga de tramos ya consumidos |
| modalidad | `ModalityResolver` | `modalidad_include_procedure_ids` | — | **sin cambio** |
| valor | router | `valor_min`/`valor_max` | proyectado como una sola métrica sobre dos campos | **sin cambio**, con las magnitudes declaradas |
| foco del perfil | `turn._focus` | str | decidía jobs Y secciones Y render por separado | **sin cambio** como detector; deja de decidir ejecución |
| fuente efectiva | *ninguna* | — | — | **`execution_plan`** (nueva) |
| jobs | `query_runner.jobs_for_focus` | tupla | autoridad de ejecución | pasa a ser **catálogo**; ejecuta `execution_plan` |
| filtros por fuente | *ninguna* | un dict compartido | fuga bidireccional | **`source_plan`** (nueva) |
| hecho acreditado | `evidence` + parsers | por llamada | una métrica ilegible invalidaba a su hermana | **`evidence` + parsers**, ahora por métrica |
| alcance renderizado | `render` desde scope y foco | str | describía lo pedido, no lo hecho | **`effective_scope`** (nueva) |

### Puerta anti-fragmentación

| Pregunta | Respuesta canónica única |
|---|---|
| ¿Quién decide `requested_sources`? | `source_request.resolve_requested_sources` |
| ¿Quién decide `event`? | `temporal_event.resolve_temporal_event` |
| ¿Quién decide `current_state`? | `estado_families` (sin cambio) |
| ¿Quién decide el tema? | el extractor de objeto, filtrado por `turn.profile_query_params` |
| ¿Quién decide `effective_sources`? | `execution_plan.build_execution_plan` |
| ¿Quién decide los jobs? | `execution_plan.build_execution_plan` |
| ¿Quién decide los filtros de cada fuente? | `source_plan.project_source` |
| ¿Quién decide si un hecho está acreditado? | `evidence` + los parsers de `aspects/` |
| ¿Quién decide qué alcance se renderiza? | `effective_scope.build_effective_scope` |

**Ninguna dimensión tiene dos dueños.** Dos aclaraciones que la puerta exige:

- `source_request` **no** duplica `_select_dataset`. Responden preguntas
  distintas —"qué dijo la persona" vs. "qué dataset escalar ejecuta la ruta
  tabular"— con tipos distintos (conjunto + procedencia vs. escalar). La
  evidencia de que son distintas es el caso G: `_select_dataset` devuelve
  `contratos` para una consulta que dice `procesos`.
- La herencia de TEMA en un follow-up sigue siendo de `followup_mutations`
  (ADR-061). El perfil no la reimplementa; por eso P12 se verifica arrastrando
  los campos de tema como haría el motor.

**Proyecciones legacy** (compatibilidad, no co-autoridad): el fallback de
`build_entity_profile` cuando no recibe plan lee la petición PERSISTIDA antes
que re-derivarla del texto, precisamente para que no existan dos formas de
decidir lo mismo.

---

## 3. Fugas globales: qué se corrigió fuera y qué no

### 3.1 Corregida (commit separado)

`firmar` entra en `PROCUREMENT_ACTION_VERB_FORMS`. La constante se consume en
UN punto (`query_router.py:1850`, scrub de tokens temáticos) y no participa en
la selección de dataset ni en el estado.

Fuera, deliberadamente: `firma` y los participios `firmados/firmadas`. La
primera colisiona con el sustantivo por la misma razón ya documentada en el
módulo para `contrató`; los participios ya los reclama `INTENT_PATTERNS`.

**Corpus diferencial**, 16 consultas, antes/después byte a byte:

| Consulta | Cambio |
|---|---|
| `que contratos firmo alcaldia de baranoa en agosto` | `objeto: ['firmo']` → `—` |
| `que contratos firmaron en 2026` | `objeto: ['firmaron']` → `—` |
| las otras 14 | **idénticas** |

Intactos los cinco pares adversariales del sustantivo: `contratos de firma
digital`, `servicio de firma electronica`, `quien firma los contratos`,
`contratos de la firma constructora abc`, `firma de convenios
interadministrativos`.

### 3.2 NO corregida globalmente, y por qué

**El estado actual que "publicados" activa en `estado_families`.** Es real y es
global: `"procesos publicados en agosto"` añade
`estado_del_procedimiento = 'Publicado'` en cualquier ruta, no sólo en el
perfil. Corregirlo globalmente cambiaría la semántica de `opportunity_search`
para las consultas de oferente —el caso de uso central del producto— y entra de
lleno en la condición de STOP *"requiere reescribir QueryRouter de forma
amplia"*.

Se corrige **dentro de la proyección del perfil**, donde el evento temporal es
una autoridad declarada, y se deja documentado como deuda:
`PUBLICATION_EVENT_VS_STATE_GLOBAL_DEBT`.

**La selección de dataset por sopa de señales.** `_select_dataset` usa
`any(señal in texto)` sin orden ni jerarquía, de modo que `proveedores` gana a
un `procesos` explícito. Cambiarlo afecta a toda la búsqueda tabular. El perfil
lo resuelve leyendo las palabras del usuario; la ruta tabular queda como está.
Deuda: `SCALAR_DATASET_SIGNAL_SOUP_DEBT`.

---

## 4. Cobertura del último día (2E)

La hipótesis del plan era que `fecha <= 'YYYY-MM-DD'` podía excluir parte del
último día si el campo llevara hora. **Se auditó experimentalmente y NO ocurre.**

Departamento del Atlántico, julio de 2026:

| Dataset | Campo | `<= '2026-07-31'` | `< '2026-08-01'` | Δ |
|---|---|---:|---:|---:|
| `jbjy-vk9h` | `fecha_de_firma` | 2180 | 2180 | **0** |
| `p6dx-8zbt` | `fecha_de_publicacion_del` | 3535 | 3535 | **0** |

Muestras del día frontera: `2026-07-31T00:00:00.000` en ambos datasets. Las dos
fechas se almacenan a medianoche, sin componente horario.

**No se cambia la semántica global de fechas.** La preferencia hipotética
`[start, next_period_start)` no tiene evidencia que la sostenga.

---

## 5. Contratos nuevos

### Fuente solicitada ≠ fuente efectiva

`requested_sources` es de la conversación; `effective_sources` la decide el plan
como `solicitada ∩ capacidad(foco)`. El foco **nunca** reescribe la solicitada.
Si la intersección es vacía se pregunta y se envían **cero** llamadas.

| foco | capacidad | por qué |
|---|---|---|
| overview, recent, temporal | ambas | — |
| suppliers | contratos | los proveedores viven en contratos |
| modalities | procesos | la modalidad del procedimiento vive en procesos |
| values | contratos | el contrato de producto prohíbe presentar el presupuesto de un proceso como valor contratado |

`values + PROCESSES` y `modalities + CONTRACTS` se adjudican como **CLARIFY**.
Soportarlos exige capacidades nuevas y acreditadas, no reinterpretar campos.

### Evento ≠ estado

| Lenguaje | evento | estado |
|---|---|---|
| `publicados en agosto` | `PROCESS_PUBLICATION` | ninguno |
| `actualmente publicados` | ninguno | `Publicado` |
| `publicados en agosto y todavía abiertos` | `PROCESS_PUBLICATION` | `oferta_abierta` |
| `firmados en agosto` | `CONTRACT_SIGNATURE` | ninguno |
| `firmados en agosto y actualmente terminados` | `CONTRACT_SIGNATURE` | `contrato_terminado` |
| `terminados en agosto` | **no soportado** → aclaración, 0 llamadas | — |

El evento sólo existe cuando hay un periodo que anclar. Reclamar un tramo como
evento impide que ese MISMO tramo filtre el estado, por
`INV-STRUCTURED-DIMENSION-NOT-TOPIC` extendida al estado; la comprobación relee
`estado_families` con el tramo enmascarado, así que un estado nombrado con otra
palabra sobrevive.

### Proyección segura por fuente

| | procesos | contratos |
|---|---|---|
| ancla temporal | `fecha_de_publicacion_del` | `fecha_de_firma` |
| campo de valor | `precio_base` | `valor_del_contrato` |
| estado prohibido | `estado_contrato*` | `estado_del_procedimiento`, `estado_de_apertura_del_proceso` |

`precio_base` y `valor_del_contrato` **no** se declaran equivalentes: un filtro
de valor sobre ambas fuentes queda marcado como dos magnitudes.

### Verdad por métrica

`count(*)` no depende de la columna de valor. Un universo cuyos contratos tienen
valor nulo tiene un conteo cierto y una suma ausente, y son independientes.

Se conservan intactos los agregados genuinamente inservibles: forma inesperada,
conteo no numérico, negativo o fraccionario, y el cero desmentido por su propia
suma.

Ceros que eran del parser y ya no existen: `as_int` devolviendo 0 ante un valor
ilegible, y `as_float(...) or 0.0` en series temporales y proveedores.

### Alcance renderizado == alcance ejecutado

```text
ALCALDIA MUNICIPAL DE BARANOA — SECOP II Contratos
Firmados del 1 al 31 de agosto de 2026.
```

Un mes no se reduce a su año. Un perfil de contratos no menciona procesos ni
ofrece profundizar en ellos. Un acotamiento por foco se dice, no se oculta.

---

## 6. Adjudicaciones sobre expectativas existentes

Dos, ambas deliberadas y documentadas en el propio test.

1. `test_malformed_contract_aggregate_cannot_be_zero[rows3]` afirmaba
   `activity.fact("contract_count") is None` para
   `{"contract_count": "1", "total_contract_value": None}`. Trataba "una métrica
   ausente" como "el agregado entero es inservible" y perdía un hecho
   acreditado. Sale de esa lista y tiene contrato propio.
2. `test_overview_reaches_product_response_without_advisor_rewrite` afirmaba el
   encabezado `"comprador — periodo"`. El periodo solo no dice sobre QUÉ se
   midió, y con dos fuentes son dos fechas distintas.

---

## 7. Defectos que encontró el corpus

Tres, todos de la misma clase que `firmo`: un tramo consumido como dimensión
sobrevivía como tema.

| Consulta | `objeto` | Consecuencia |
|---|---|---|
| `procesos publicados en agosto y todavia abiertos` | `['todavia']` | `LIKE '%todavia%'` sobre el objeto del proceso |
| `procesos iniciados en agosto` | `['iniciados']` | tema espurio guardado tras una aclaración |
| `contratos finalizados en 2026` | `['finalizados']` | ídem |

El primero es otro generador de falsos ceros. Se cierran con
`is_consumed_temporal_term`, que aplica el invariante usando EXACTAMENTE el
vocabulario con el que el módulo ya decide — por construcción no toca los
sustantivos homónimos.

---

## 8. Dogfood en vivo (2026-09-07)

```text
"analizame que contratos firmo alcaldia de baranoa en agosto"

  requested = CONTRACTS / EXPLICIT      effective = CONTRACTS
  event     = CONTRACT_SIGNATURE        topic     = ()
  llamadas  = 4, todas jbjy-vk9h        p6dx      = 0

  ALCALDIA MUNICIPAL DE BARANOA — SECOP II Contratos
  Firmados del 1 al 31 de agosto de 2026.
  • Se observan 47 contratos por $911.5 millones; promedio $19.4 millones.
```

**47** coincide exactamente con el conteo live independiente. `$0` → `$911.5
millones`.

```text
"analiza procesos de vias publicados por alcaldia de baranoa en agosto"

  requested = PROCESSES / EXPLICIT      effective = PROCESSES
  event     = PROCESS_PUBLICATION       topic     = ('vias',)
  llamadas  = 2, todas p6dx-8zbt        sin filtro de estado actual

  • Publicó 0 procesos en el periodo.
```

Ese cero **sí** está acreditado: Baranoa publicó 48 procesos en agosto y
**ninguno** menciona vías. Antes del sprint la misma consulta habría partido de
2 procesos, no de 48.

---

## 9. Validación

| Puerta | Resultado |
|---|---|
| Freeze del incidente | 22/22, sin xfail pendientes |
| Corpus sistémico | 74 casos × 4 ejes = **276** |
| Propiedades P1–P12 | **12/12** (Hypothesis) |
| Focales del sprint | 23 + 18 + 28 + 21 + 20 = **110** |
| Corpus Entity Profile existente | **82/82** |
| Corpus parser / intención / dimensiones / HCA | **878** |
| Suite completa | **12310 passed, 11 skipped, 16 xfailed, 0 failed** (626 s) |
| Ruff | verde |
| `make lint-core` | `app/core/ limpio` |
| Semgrep | 10 en `app/` (37 global) → 10 (37). **Delta 0** |
| Topes de LoC | 2/2 |
| `git diff --check` | limpio |

---

## 10. Deuda declarada

| Id | Qué |
|---|---|
| `PUBLICATION_EVENT_VS_STATE_GLOBAL_DEBT` | fuera del perfil, `publicados` sigue activando estado actual |
| `SCALAR_DATASET_SIGNAL_SOUP_DEBT` | `_select_dataset` sigue eligiendo por sopa de señales sin orden |
| `PROFILE_PLURAL_AGREEMENT_DEBT` | el render dice "1 contratos"; cosmético y preexistente |
| `PROCESS_BUDGET_AGGREGATE_DEFERRED` | `values` sobre procesos exigiría una agregación acreditada de `precio_base` |
| `CONTRACT_MODALITY_ASPECT_DEFERRED` | `modalities` sobre contratos, sin capacidad en V1 |

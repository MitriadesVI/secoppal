# O1B — CURRENT OPPORTUNITY RETRIEVAL

Fecha de ejecución: 2026-08-17

Rama: `feature/opportunity-retrieval-v1`

Head inicial: `81993dd7a13c3016d90c24357d3d2eb12593067a`

Estado: listo para revisión independiente

---

## Promesa de producto

SECOPPAL **prioriza procesos con mejores señales disponibles de
vigencia/accionabilidad**, sin ocultar oportunidades por exigir una certeza que
SECOP no puede proporcionar.

Search ofrece descubrimiento actual razonable. Radar cubre detección temprana,
pero no es una dependencia runtime de Search ni participa en esta política.

O1B no certifica que una oferta pueda presentarse hoy. Tampoco reabre el Truth
Layer, cambia el ranking general, introduce un reranker ni intenta alcanzar
precisión perfecta.

## Gate previo e integración

La ruta productiva de `opportunity_search` usa `SecopClient` contra Socrata
(`datos.gov.co`). El repositorio no contiene una ruta runtime que pueda servir
esa búsqueda desde un warehouse local; por eso no hay divergencia local/remota
que bloqueara la integración.

Los cuatro campos necesarios están disponibles en el dataset remoto
`p6dx-8zbt`:

- `estado_del_procedimiento`;
- `estado_de_apertura_del_proceso`;
- `fecha_de_publicacion_del`;
- `fecha_de_recepcion_de`.

La ausencia histórica de transporte de `fecha_de_recepcion_de` en el warehouse
estudiado por O1A.1 no afecta el runtime actual. Si en el futuro se habilita un
backend local para Search, transportar y validar ese campo queda como
`WAREHOUSE_RECEPTION_DATE_TRANSPORT_DEFERRED`; O1B no modifica warehouse.

Las restricciones temporales explícitas ya llegan estructuradas como
`fecha_desde` / `fecha_hasta`. El parser marca el estado explícito mediante
`estado` / `estado_field`; además, la política protege familias y bundles
estructurados que no sean los dos bundles automáticos conocidos de oportunidad.
No se inspecciona texto libre dentro de `SoQLBuilder`.

## Evidencia O1A / O1A.1 y decisión

O1A demostró que filtrar un universo amplio después de `LIMIT 50` puede perder
candidatos válidos. Por eso O1B aplica el filtro grueso en el `WHERE`, antes de
`ORDER BY`, `LIMIT` y `OFFSET`; el post-retrieval solo presenta el resultado.

O1A también mostró que `estado_de_apertura_del_proceso = 'Abierto'` elimina
casos muertos dentro del segmento útil sin pérdida verificable de plazos vivos.
Se conserva intacta la regla `STATE-PRIORITY-001`.

O1A.1 mostró que la cobertura de deadline cambia mucho por modalidad y que
`fase = Presentación de oferta` no prueba por sí sola recepción vigente. Por
eso O1B no crea filtros de modalidad y endurece únicamente lo que la interfaz
puede afirmar.

## Contrato de recuperación

La política aplica exclusivamente a:

```text
dataset = procesos
AND intent_type = opportunity_search
AND sin tiempo explícito
AND sin estado explícito incompatible
```

El conjunto automático de estados se sustituye por:

```text
estado_del_procedimiento IN ('Publicado', 'Abierto')
AND estado_de_apertura_del_proceso = 'Abierto'
```

`Borrador`, `En aprobación`, `Aprobado`, `Evaluación`, `Seleccionado`,
`Cancelado` y `Suspendido` quedan fuera del default actual. Una solicitud de
estado explícita siempre conserva la semántica baseline.

La regla temporal conceptual es:

```text
current_colombia_date = reference_instant UTC → America/Bogota → date

fecha_de_recepcion_de >= current_colombia_date 00:00:00
OR (
  fecha_de_recepcion_de IS NULL
  AND fecha_de_publicacion_del >= (current_colombia_date - 90 días) 00:00:00
)
```

Interpretación:

- deadline de hoy o futuro conocido: candidato, incluso si fue publicado hace
  más de 90 días;
- deadline pasado conocido: fuera del default actual;
- deadline nulo: no significa cerrado; se conserva si la publicación es
  reciente;
- deadline nulo y publicación antigua: fuera de la búsqueda rápida default.

Los 90 días son
`DEFAULT_DISCOVERY_WINDOW_WHEN_DEADLINE_UNKNOWN`, no una vida máxima de la
oportunidad. No se impone freshness a una fila con deadline futuro.

En Socrata la disyunción se expresa como un único predicado `case(...)`. Es
lógicamente equivalente para las tres ramas anteriores y evita el coste severo
observado con un `OR` entre dos columnas de fecha. Los literales siguen el
formato temporal existente del repositorio y no contienen texto del usuario.

## Overrides y referencia temporal

`USER_EXPLICIT_TIME > O1B_DEFAULT_TIME_POLICY`: año, mes o rango explícito
mantiene el `WHERE` baseline; no recibe ni la ventana de 90 días ni el corte por
deadline.

`USER_EXPLICIT_STATE > O1B_DEFAULT_STATE_POLICY`: una consulta como “procesos
en evaluación” no se convierte en `Publicado / Abierto`.

Planning captura una única fecha UTC consciente de zona horaria, con precisión
de segundo, y la transporta como metadata JSON-safe del plan. La política deriva
de ella una sola fecha civil `America/Bogota`; nunca depende del host ni del
usuario. Query, count y fallback técnico reutilizan el valor. La paginación
conserva la referencia original de la consulta, mientras un follow-up semántico
descarta la metadata heredada y captura una referencia nueva. La política es
pura, determinista, sin I/O, backend, LLM, warehouse ni reloj propio.

## Fan-out y timeout

Una búsqueda O1B ejecuta una sola consulta productiva normal:

- sin Tier 2 automático;
- sin segunda consulta semántica oculta cuando hay cero filas;
- sin oversampling masivo;
- sin filtro post-`LIMIT` para rescatar recall.

El fallback técnico H4 preexistente, que retira `ORDER BY` ante timeout, conserva
su contrato y todos los filtros sustantivos. Un timeout no se interpreta como
cero resultados.

La arquitectura queda compatible con una acción futura explícita “Ampliar a
publicaciones anteriores”, pero esa función no se implementa en O1B.

## Presentación visible

No cambia `process_lifecycle` ni su stage canónico
`offer_phase_observed`. Solo cambia la certeza de la etiqueta:

| Evidencia | Presentación |
|---|---|
| deadline futuro conocido | `🟢 Recepción de ofertas · límite YYYY-MM-DD` |
| deadline nulo/no observado | `🔵 Publicado · Vigencia por verificar` |
| deadline pasado conocido | `🟠 Plazo de recepción reportado vencido` |

Las etapas canónicas `selected`, `evaluation`, `cancelled` y `suspended`
mantienen su autoridad existente. No se exponen nombres internos.

## Paridad fuera de alcance

Se compararon pre-O1B y post-O1B sobre 14 consultas deterministas fuera del
scope, con `PYTHONHASHSEED=0` para neutralizar el orden no determinista ya
existente de términos morfológicos. Intent, dataset, params, SoQL completo,
`WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, ranking/paginación y respuesta de
contratos quedaron idénticos.

```text
UNAUTHORIZED_QUERY_DIVERGENCES = 0
```

El corpus incluye procesos con fecha explícita, contratos, estados Seleccionado
y Evaluación, entidad, modalidad, direct lookup e históricos.

## Validación live

Corpus definitivo: 25 temas (`alimentación`, PAE, mantenimiento, obra,
tecnología, salud, vivienda, aseo, infraestructura, servicios, dotación,
construcción, cafetería, residuos, limpieza, medicamentos, computadores,
interventoría y capacitación), 50 filas máximas por tema, mismo instante de
referencia.

| Métrica | Baseline | O1B |
|---|---:|---:|
| queries | 25 | 25 |
| filas observadas | 1.250 | 1.250 |
| timeouts | 0 | 0 |
| errores | 0 | 0 |
| sin resultados | 0 | 0 |
| mediana | 1,415 s | 0,723 s |
| p90 | 3,745 s | 1,201 s |
| prepublicación | 882 (70,56%) | 0 (0%) |
| deadline vencido conocido | 125 (10,00%) | 0 (0%) |
| deadline futuro | 51 | 299 |
| deadline nulo + publicación reciente | 0 | 951 |
| leakage Seleccionado | 0 | 0 |
| leakage Evaluación | 0 | 0 |

`NEW_TIMEOUTS = 0`; mediana y p90 quedan por debajo del baseline y de los gates
`baseline × 1,20`. No aumentaron las consultas sin resultados.

El primer ensayo con el `OR` literal conservó resultados pero produjo mediana
2,030 s y p90 8,412 s, por encima del gate. Antes del cierre se sustituyó por
`case(...)`; en una comparación de cinco temas produjo los mismos 50 IDs y el
mismo orden, y el corpus definitivo de 25 temas confirmó el resultado de
latencia anterior.

La muestra top-50 no expuso una publicación mayor a 90 días con deadline futuro
(`FUTURE_DEADLINE_OLDER_90D_FOUND = 0` observado). Esto no se interpreta como
imposibilidad: el predicado no contiene un hard freshness gate en esa rama y el
caso está cubierto de forma determinista por tests. El dato live solo significa
que no apareció en esas 1.250 posiciones.

## Tests RED y mutaciones

Antes de producción fallaron `RED_1` a `RED_5`, `RED_8` y `RED_9`.
`RED_6`, `RED_7` y `RED_10` eran guardianes de preservación y ya pasaban en el
baseline. Tras la implementación, los diez pasan.

Cada mutación obligatoria fue introducida temporalmente, detectada y restaurada:

| Mutación | Guardián que falló |
|---|---|
| MUT_A — reintroducir Borrador | RED_1 |
| MUT_B — freshness de 90 días también con deadline futuro | RED_4 |
| MUT_C — tratar NULL como cerrado | RED_3 |
| MUT_D — aplicar O1B a tiempo histórico explícito | RED_6 |
| MUT_E — afirmar recepción con deadline NULL | RED_8 |

## O1B.1 — frontera calendar_date y reloj de follow-up

La revisión independiente detectó que `fecha_de_recepcion_de` y
`fecha_de_publicacion_del` son columnas Socrata `calendar_date`, materializadas
a `00:00:00`. Compararlas contra la hora UTC exacta excluía un deadline reportado
para el día de hoy tan pronto pasaba medianoche UTC, aun cuando en Colombia el
día seguía vigente o acababa de empezar.

O1B.1 conserva la política sustantiva y corrige solo la frontera:

```text
CALENDAR_TIMEZONE = America/Bogota
DEADLINE_COMPARISON = fecha_de_recepcion_de >= Colombia hoy 00:00:00
FRESHNESS_COMPARISON = fecha_de_publicacion_del >= Colombia hoy - 90 días 00:00:00
```

Esto no afirma que el proceso permanezca abierto hasta las 23:59. Solo evita
descartarlo cuando SECOP reporta como fecha límite el día de hoy y no ofrece
precisión horaria suficiente.

### Bordes deterministas

Con fecha Colombia `2026-08-18`:

| Caso | Resultado |
|---|---|
| deadline `2026-08-18` | include |
| deadline `2026-08-19` | include |
| deadline `2026-08-17` | exclude |
| NULL + publicación `2026-08-18` | include |
| NULL + publicación `2026-05-20` | include (`>=`, día 90) |
| NULL + publicación `2026-05-19` | exclude |

`2026-08-18T04:59:59Z` deriva `2026-08-17` en Colombia;
`2026-08-18T05:00:01Z` deriva `2026-08-18`. El test no depende del timezone del
host.

### Follow-up y paginación

La clave interna `_opportunity_reference_instant` se elimina de los params
heredados justo antes de planificar un follow-up semántico; si O1B sigue siendo
aplicable, planning captura un instante nuevo exactamente una vez.

La paginación no crea un nuevo plan: reconstruye la misma consulta desde
`PaginationBuffer.query_params`. Por eso una página 2 consultada en D+1 conserva
la referencia D y no mueve silenciosamente el universo entre páginas.

`Suggestion.modified_params` todavía puede transportar la clave interna. Quitarla
exigiría cambiar el handler de selección de sugerencias, fuera del scope
productivo autorizado de O1B.1; la revisión indicó que no era blocker. El
formatter solo presenta labels y un test comprueba que ni el nombre de la clave
ni su valor aparecen en la respuesta pública.

### RED, mutaciones y smoke live

Antes de producción fallaron los tres defectos:

- `RED_CALENDAR_TODAY`: el deadline de hoy se comparaba contra la hora UTC;
- `RED_TIMEZONE_BOUNDARY`: el corte cambiaba con el día UTC, no a las 05:00 UTC;
- `RED_FOLLOWUP_REFERENCE`: planning no consultaba el reloj al recibir una
  referencia heredada.

Las mutaciones obligatorias fueron detectadas y restauradas:

- volver a `deadline > full UTC instant` hizo fallar el boundary test;
- volver a heredar la referencia hizo fallar el test de follow-up.

Smoke Socrata del 2026-08-17 Colombia, sobre `p6dx-8zbt`:

| Categoría | Evidencia |
|---|---|
| deadline hoy | 3 filas naturales; `CO1.REQ.10796476`, `10832914`, `10855362` |
| deadline mañana | 3 filas naturales; `CO1.REQ.10782890`, `10847913`, `10752822` |
| deadline ayer | 0 en el segmento default; 3 filas conocidas fuera del segmento usadas como control de exclusión |
| NULL + reciente | 3 filas; las tres publicadas exactamente en la frontera `2026-05-19` |

No se añadió ninguna consulta productiva, retry o fan-out; el smoke fue una
verificación de auditoría fuera del runtime.

### Regresión O1B.1

```text
Opportunity O1B/O1B.1       22 passed
Regresión focal ampliada    288 passed
Query corpus                89/89 PASS
Dialogue corpus             60/61 PASS
                            GOB-DEPTO-001, único fallo basal
Suite baseline O1B          7460 passed, 2 skipped, 24 xfailed
Suite post-O1B.1            7466 passed, 2 skipped, 24 xfailed

NEW_FAILURES = 0
NEW_SKIPS    = 0
NEW_XFAILS   = 0
```

## Límites y deuda explícita

- `STATE_DATE_CONFLICT_DEFERRED`: Evaluación + deadline futuro permanece fuera
  del default; O1B no intenta reconciliar campos contradictorios.
- `OPPORTUNITY_MODALITY_SEMANTICS_DEFERRED`: no se excluye contratación directa
  ni se cambia el catálogo de modalidades; ausencia de deadline no es evidencia
  negativa por sí sola.
- `WAREHOUSE_RECEPTION_DATE_TRANSPORT_DEFERRED`: solo aplicará si Search gana un
  backend local futuro.
- Un candidato con deadline nulo puede ser un falso positivo plausible y exige
  verificación humana.
- La posibilidad de presentar una oferta depende de condiciones que estos
  cuatro campos no certifican.
- Radar sigue siendo el mecanismo de detección temprana y no una dependencia
  runtime de esta búsqueda.

## Alcance preservado

No cambiaron Truth Layer, Radar, warehouse, contratos, catálogo de modalidades,
ranking general, parser semántico general, typo correction, synonyms, market
graph, grouping, LLM, reranker, `ORDER BY`, `LIMIT`, `OFFSET` ni filtros de
objeto, entidad, valor, departamento o modalidad.

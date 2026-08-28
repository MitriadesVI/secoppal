# SECOPPAL — SODA3 Runtime Canary V1

**Fecha local:** 2026-08-27 21:25–21:33 COT
**Rama:** `experiment/soda3-discovery-v1`
**Base:** `main@dd8381802cecef3f69220508ee641bcd87b5eb78`
**Tipo:** capture-and-replay experimental; no es migración ni shadow productivo
**Veredicto obligatorio:** `NO_GO_DIVERGENCE`
**Gate interno:** `SODA3_QUERY_DISCOVERY_OPEN`

## Resumen ejecutivo

La mejora de rendimiento de SODA3 sobrevivió al runtime real y se hizo todavía más visible en la
cola. No obstante, el canary estricto **no autoriza integración ni cutover** porque no conservó
paridad completa y SODA3 introdujo fallos asimétricos reproducibles en dos llamadas.

- 26 consultas humanas reales ejecutadas una vez por el runtime para captura;
- 83 operaciones Socrata congeladas: 34 `query`, 12 `count` y 37 `aggregate`;
- 100% de operaciones capturadas y clasificadas;
- 249 pares A/B, 498 ejecuciones HTTP y SoQL idéntico en 249/249;
- 194 pares con éxito en ambos brazos;
- 176/194 pares exitosos con paridad estricta (90,72%);
- 9 pares adicionales con contenido igual pero solo cambio de orden;
- 9 pares exitosos con divergencia de contenido, concentrados en tres SoQL subordenados;
- 33 fallos asimétricos: 27 `classic timeout → SODA3 ok` y 6 `classic ok → SODA3 timeout`;
- 49 timeouts clásicos frente a 28 SODA3, una reducción neta de 42,86%;
- p90 elegible global: **2.172,739 ms → 269,323 ms** (87,60% menor);
- p90 de contratos: **1.802,407 ms → 299,699 ms** (83,37% menor);
- p90 de procesos: **2.911,847 ms → 233,871 ms** (91,97% menor);
- los 26 casos pasaron el gate de no vacuidad, incluidos dos controles `expected_empty`.

El resultado matizado es claro: **SODA3 sigue siendo el transporte candidato con mayor señal de
rendimiento, pero el runtime actual no puede cambiarse sin alterar resultados observables en
consultas con `LIMIT` subordenado, y dos fallbacks de tres segundos fueron peores de forma
sistemática.** Esto descarta `NO_MATERIAL_GAIN`, pero obliga a `NO_GO_DIVERGENCE` bajo los gates
acordados.

## Alcance y método

### Captura en el borde real

El harness envolvió experimentalmente el `SecopClient` ya creado por `SecopalWorkflow`. Registró
cada llamada `query`, `count` o `aggregate`, incluidas las hechas por clientes derivados con timeout
propio. El wrapper conserva `isinstance(..., SecopClient)` porque el runtime usa esa condición antes
de derivar presupuestos para analytics y Actor Profile.

Cada consulta humana final se ejecutó una sola vez por parser, resolvers, QueryPlan, builder,
analytics y Actor Profile. Esa ejecución clásica solo construyó el manifiesto y quedó excluida de
las métricas A/B. Los reemplazos de admisión se hicieron antes de congelar el manifiesto: los casos
que no llegaron a la frontera remota o usaban un ID ya ausente no forman parte del corpus final.

El manifiesto final cubre:

| Dataset | `query` | `count` | `aggregate` | Total |
|---|---:|---:|---:|---:|
| `jbjy-vk9h` | 22 | 8 | 27 | 57 |
| `p6dx-8zbt` | 12 | 4 | 10 | 26 |
| **Total** | **34** | **12** | **37** | **83** |

Las 83/83 operaciones quedaron clasificadas. Hubo 66 éxitos y 17 timeouts durante captura; esos
resultados no entraron en métricas. El corpus incluye búsquedas tabulares, oportunidades,
phrase-aware, negaciones, OR, valor/fecha/estado/entidad, dos Actor Profile, tres analytics, dos
lookups no vacíos y dos controles vacíos. Un caso phrase-aware produjo realmente `LIMIT 200`.

### Replay congelado

Durante replay no se ejecutó ninguna parte del runtime. Cada brazo recibió únicamente el
`operation + dataset_id + exact_soql` congelado:

| Brazo | Endpoint | Transporte |
|---|---|---|
| A — clásico | `GET /resource/{dataset_id}.json` | parámetro `$query` |
| B — SODA3 | `POST /api/v3/views/{dataset_id}/query.json` | JSON `query` |

SODA3 usó `orderingSpecifier=total` e `includeSynthetic=false`. Cada par recibió el mismo timeout
explícito, redondeado hacia arriba desde el presupuesto capturado por el runtime (2–11 segundos).
Hubo tres rondas; el orden de operaciones se aleatorizó y el primer brazo se alternó entre rondas.
Se usó `requests==2.33.1`, ya instalado en el proyecto, sin añadir dependencias.

La latencia solo se calculó sobre pares con SoQL idéntico, ambos brazos exitosos y sin divergencia
de contenido. Los pares `ordering_only` sí fueron elegibles porque conservaron exactamente el mismo
conjunto y hash semántico; el gate de paridad estricta los contabiliza por separado.

### Evidencia y reproducibilidad

```text
corpus:          tests/fixtures/soda3_runtime_canary_queries.json
corpus sha256:   3785b1695ba4c6ee4b4e11b249077a42ac9cc3740c5a1082814dc3ed66488fb7
manifest:        /tmp/soda3_runtime_canary_manifest_frozen_v3.json
manifest sha256: 880e63ecaf2672e217238c234db09d2847c89d95da6648c8c8490d9952c5076a
manifest size:   140.093 bytes
replay:          /tmp/soda3_runtime_canary_replay_v3.jsonl
replay sha256:   0e35f51afb5483d98352ab6420e809266283546afddb103fbdc3e3735feb7bb7
replay size:     1.154.642 bytes
```

El JSONL contiene 775 eventos: una metadata, 498 ejecuciones, 249 comparaciones, 26 admisiones y
un resumen. Registra SoQL, hashes, IDs, bytes, filas, HTTP status, error class y latencia, pero no
headers. Se verificó que el literal del App Token no aparece ni en el manifiesto ni en el JSONL.

Comandos para una campaña nueva, siempre con rutas de salida nuevas:

```bash
python scripts/audits/soda3_runtime_canary.py capture \
  --env-file .env \
  --actor-index data/actor_alias_index.sqlite \
  --output /tmp/soda3-runtime-manifest.json

python scripts/audits/soda3_runtime_canary.py replay \
  --env-file .env \
  --manifest /tmp/soda3-runtime-manifest.json \
  --output /tmp/soda3-runtime-replay.jsonl
```

## Gates

| Gate | Exigencia | Resultado | Pasa |
|---|---|---:|:---:|
| SoQL A/B | 100% idéntico | 249/249 | Sí |
| Operaciones capturadas | 100% clasificadas | 83/83 | Sí |
| No vacuidad | todos los casos admitidos | 26/26 | Sí |
| Paridad de respuestas exitosas | 100% estricta | 176/194 | **No** |
| Divergencias críticas | 0 | 18 pares exitosos no estrictos | **No** |
| p90 contratos | ≥25% mejor | 83,37% mejor | Sí |
| p90 procesos | ≥25% mejor | 91,97% mejor | Sí |
| Timeouts SODA3 | ≤ clásico | 28 ≤ 49 | Sí |
| Fallos asimétricos | 0 | 33 | **No** |

## 1. Paridad

| Clasificación | Pares/etiquetas |
|---|---:|
| Pares totales | 249 |
| Éxito en ambos brazos | 194 |
| Paridad estricta | 176 |
| `ordering_only` | 9 |
| pares con `missing_rows` + `extra_rows` | 3 |
| `unknown` | 6 |
| `serialization` | 0 |
| `type_conversion` | 0 |
| `remote_variance` | 55 |

Los 18 pares exitosos no estrictos se concentraron de forma estable en seis operaciones, siempre
en las tres rondas:

- tres operaciones produjeron el mismo contenido y distinto orden (`ordering_only`);
- una consulta `LIMIT 50` sin `ORDER BY` devolvió 12 IDs distintos por transporte en el borde del
  conjunto (`missing_rows` + `extra_rows`);
- dos agregados `GROUP BY ... ORDER BY cnt DESC LIMIT 5` eligieron un quinto grupo diferente ante
  empates (`unknown` en el clasificador, confirmado por diagnóstico fuera de métricas).

Los hashes de cada transporte fueron estables entre las tres rondas. Por tanto, estas nueve
divergencias de contenido no encajan con una mutación momentánea del dataset entre brazos; son una
diferencia reproducible de selección ante un orden total insuficiente.

Los dos controles `expected_empty` fueron admitidos: todas sus respuestas exitosas permanecieron
vacías. Los 24 casos principales observaron al menos una respuesta no vacía.

## 2. Latencia

La muestra elegible contiene 185 pares. Excluye fallos y las nueve divergencias de contenido.

| Corte | Clásico | SODA3 | Mejora de SODA3 |
|---|---:|---:|---:|
| p50 global | 336,660 ms | 174,305 ms | 48,23% |
| p90 global | 2.172,739 ms | 269,323 ms | 87,60% |
| p95 global | 3.074,556 ms | 426,886 ms | 86,12% |
| p90 `jbjy-vk9h` | 1.802,407 ms | 299,699 ms | 83,37% |
| p90 `p6dx-8zbt` | 2.911,847 ms | 233,871 ms | 91,97% |

La mejora pareada mediana fue 42,459%. SODA3 fue más rápido en 147/185 pares elegibles (79,46%).
Por operación:

| Operación | Pares elegibles | p90 clásico | p90 SODA3 | Mejora pareada mediana |
|---|---:|---:|---:|---:|
| `query` | 70 | 2.999,082 ms | 265,873 ms | 36,697% |
| `count` | 12 | 2.045,433 ms | 187,308 ms | 84,064% |
| `aggregate` | 103 | 2.097,282 ms | 278,779 ms | 41,139% |

La señal de latencia es material, transversal a ambos datasets y coherente con V0. No compensa el
fallo de paridad: rendimiento y compatibilidad son gates independientes.

## 3. Estabilidad y timeouts

| Resultado pareado | Pares |
|---|---:|
| ambos OK | 194 |
| clásico timeout, SODA3 OK | 27 |
| ambos timeout | 22 |
| clásico OK, SODA3 timeout | 6 |

SODA3 redujo el total de timeouts de 49 a 28. Esta es evidencia nueva de reducción de riesgo neto,
pero no permite afirmar que SODA3 “elimine” timeouts.

Los seis fallos desfavorables a SODA3 no fueron ruido disperso: ocurrieron 3/3 veces en dos
fallbacks de contratos con presupuesto de tres segundos:

- `RT-NEG-002__call_002`: negación de alimentación, 12 ramas OR;
- `RT-TAB-002__call_002`: interventoría técnica de obra, 4 ramas OR.

En las seis ejecuciones el clásico respondió y SODA3 agotó el presupuesto. Esto viola el gate
defensivo aun cuando el balance global favorece ampliamente a SODA3. No hubo HTTP 4xx/5xx,
errores de decoding ni formas de respuesta inválidas: todos los fallos fueron timeouts.

## 4. Consultas OR complejas

El máximo SoQL producido por el runtime tuvo 2.412 caracteres y 24 ramas OR. La cobertura compleja
no fue uniformemente favorable:

| Segmento | Pares | ambos OK | clásico timeout/SODA3 OK | ambos timeout | clásico OK/SODA3 timeout |
|---|---:|---:|---:|---:|---:|
| OR ≥ 5 | 75 | 39 | 17 | 16 | 3 |
| OR ≥ 10 | 39 | 15 | 8 | 13 | 3 |
| OR ≥ 20 | 9 | 3 | 3 | 3 | 0 |

En el caso OR-24 principal, SODA3 respondió 3/3 donde el clásico agotó 3/3; el fallback devolvió
paridad estricta 3/3 y el `count` agotó ambos transportes 3/3. Esto confirma que SODA3 vuelve más
viable un OR ancho, pero no resuelve por sí solo todas las operaciones derivadas.

El caso phrase-aware con `LIMIT 200` tuvo un par exitoso y estricto alrededor de seis segundos. En
las otras dos rondas el clásico agotó ocho segundos mientras SODA3 respondió en 491 ms y 230 ms.
Su fallback `LIMIT 50` agotó ambos brazos en las tres rondas. La ruta de candidate pool grande está
cubierta, pero no autoriza afirmar estabilidad universal.

## 5. Soporte por dataset

| Dataset | Operaciones capturadas | Pares | Ambos OK | Paridad estricta | Timeouts clásico | Timeouts SODA3 | p90 mejora |
|---|---:|---:|---:|---:|---:|---:|---:|
| `jbjy-vk9h` | 57 | 171 | 129 | 117 | 36 | 24 | 83,37% |
| `p6dx-8zbt` | 26 | 78 | 65 | 59 | 13 | 4 | 91,97% |

SODA3 ejecutó `query`, `count` y `aggregate` en ambos datasets. Procesos no tuvo ningún timeout
exclusivo de SODA3; los seis casos desfavorables ocurrieron en contratos. Ambos datasets mostraron
divergencias reproducibles en consultas subordenadas: dos operaciones en procesos y cuatro en
contratos si se incluye `ordering_only`.

## 6. Hallazgos de transporte

1. SODA3 conserva una ventaja grande y repetible de cola sobre el transporte clásico.
2. El beneficio cubre `query`, `count` y `aggregate`; no depende solo de SoQL largos.
3. SODA3 redujo los timeouts totales y recuperó 27 pares donde clásico falló.
4. SODA3 también perdió seis pares que clásico resolvió, concentrados en dos consultas y un
   presupuesto de tres segundos. Ese patrón requiere explicación antes de integración.
5. No aparecieron diferencias de serialización o conversión de tipos.
6. Con SoQL subordenado, `orderingSpecifier=total` no preserva necesariamente la selección u orden
   observable del endpoint clásico. Es una incompatibilidad de cutover aunque ambos resultados
   sean respuestas válidas para la consulta incompleta.
7. No se probaron `/export`, `orderingSpecifier=discard`, retries, paginación SODA3, shadow ni
   fallback productivo.

## 7. Hallazgos que pertenecen al builder

Tres familias de divergencia nacen de SoQL que no define un orden total:

1. `RT-TAB-004__call_002` usa `LIMIT 50` sin `ORDER BY`. Clásico y SODA3 eligieron conjuntos
   distintos, estables por transporte, con 12 IDs exclusivos de cada brazo.
2. `RT-TAB-007__call_003` y `RT-OPP-003__call_003` usan
   `GROUP BY ... ORDER BY cnt DESC LIMIT 5` sin desempate por nombre. Un diagnóstico fuera de las
   métricas confirmó empates exactamente en el quinto lugar (`cnt=3` y `cnt=8`) y una entidad final
   distinta por transporte.
3. Las tres operaciones `ordering_only` conservan el contenido, pero dejan empates sin resolver:
   dos agregados de Actor Profile ordenan solo por `cnt`; la consulta tabular de procesos contiene
   IDs duplicados y no desambigua filas con la misma clave de orden.

Esto no exonera al transporte: un cutover hoy cambiaría resultados reales. Sí evita diagnosticarlo
como pérdida semántica de SODA3. El problema de fondo es que el builder/runtime delega decisiones
de selección y desempate al backend. Corregirlo pertenece a una rama separada y cambiaría de forma
controlada el comportamiento productivo; esta rama no modifica `SoQLBuilder`.

También se observó que algunas consultas candidatas de analytics/actor no llegaban a Socrata o
pedían aclaración. Se reemplazaron antes de congelar el corpus. Ese hallazgo pertenece a routing y
parser, no a transporte, y no se contó como fallo A/B.

## 8. Recomendación del siguiente paso

No crear todavía un transporte productivo, shadow, feature flag ni router por complejidad. El
siguiente paso debe ser un cierre pequeño de los bloqueadores, no otra campaña general:

1. corregir en una rama separada el orden total de las consultas con `LIMIT`, incluidos desempates
   estables para agregados y duplicados;
2. investigar las dos consultas de fallback donde SODA3 agotó 3/3 y clásico respondió 3/3, sin
   ampliar el timeout ni cambiar el SoQL entre brazos;
3. recapturar únicamente los casos afectados a través del runtime y repetir sus tres rondas;
4. exigir cero diferencias de contenido, cero `ordering_only` y cero timeout exclusivo de SODA3;
5. solo si esos gates pasan, cerrar `/query` y abrir otra rama para una integración mínima,
   reversible y observable.

La evidencia ya justifica seguir invirtiendo: el beneficio p90 es demasiado grande para ignorarlo.
No justifica migración productiva: hoy existe una alteración observable de resultados y una
regresión reproducible de timeout en dos fallbacks.

## Veredicto final

```text
NO_GO_DIVERGENCE
```

Interpretación: SODA3 continúa como candidato preferido por rendimiento, pero el discovery de
`/query` no está cerrado y no debe pasar a integración hasta resolver los bloqueadores acotados.

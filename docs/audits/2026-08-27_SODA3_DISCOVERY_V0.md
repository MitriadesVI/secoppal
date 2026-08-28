# SECOPPAL — SODA3 Discovery V0

**Fecha local:** 2026-08-27 20:05–20:10 COT
**Rama:** `experiment/soda3-discovery-v1`
**Base:** `main@dd8381802cecef3f69220508ee641bcd87b5eb78`
**Tipo:** spike de medición; no es una migración
**Veredicto:** `GO_SODA3_QUERY`

## Resumen ejecutivo

SODA3 produjo una mejora material de latencia sin cambiar el SoQL y sin divergencias de
resultado en los pares comparables:

- 16 casos, 5 rondas y 160 ejecuciones HTTP;
- 80 pares A/B, con orden alternado y casos aleatorizados por ronda;
- 78 pares comparables con paridad estricta;
- 2 pares no comparables por timeout simétrico de ambos transportes;
- 0 pares con SoQL diferente;
- 0 `missing_rows`, `extra_rows`, `serialization`, `type_conversion`, `ordering_only` o
  `unknown` entre respuestas exitosas;
- mejora pareada mediana de SODA3: **59,543%**;
- p50 global: **600,675 ms → 189,343 ms** (**68,478%** menor);
- p90 global: **2.333,012 ms → 478,053 ms** (**79,509%** menor);
- p95 global: **6.881,970 ms → 939,177 ms** (**86,353%** menor);
- consultas complejas: mejora pareada mediana de **54,670%**;
- OR-100: mejora pareada mediana de **60,395%**.

El veredicto significa que existe evidencia suficiente para avanzar a una validación más amplia
del endpoint SODA3 `/query`. **No autoriza cutover, shadow, feature flags ni cambios en
`SecopClient`.** La estabilidad no mejoró en esta campaña: ambos brazos tuvieron exactamente dos
timeouts. La ganancia demostrada es de latencia, especialmente de cola y consultas complejas, no
una reducción demostrada de fallos.

## Alcance y método

### Contratos comparados

| Brazo | Método y endpoint | Transporte de SoQL |
|---|---|---|
| A — clásico | `GET /resource/{dataset_id}.json` | parámetro `$query` |
| B — SODA3 | `POST /api/v3/views/{dataset_id}/query.json` | JSON `query` |

Ambos brazos recibieron la misma cadena materializada de SoQL. El brazo SODA3 usó en todas las
ejecuciones primarias:

```json
{
  "orderingSpecifier": "total",
  "includeSynthetic": false,
  "timeout": 30
}
```

El timeout del cliente HTTP también fue 30 segundos para ambos brazos. No hubo retries. Se usó
`requests==2.33.1`, dependencia ya instalada en el proyecto. El App Token estuvo presente en los
headers, pero el harness no persiste headers, valores de entorno ni mensajes de excepción. Se
verificó además que el literal del token no aparece en el artefacto de campaña.

La forma de POST y sus opciones se contrastaron con la documentación oficial de Socrata:
[Queries using SODA3](https://dev.socrata.com/docs/queries/index.html) y
[orderingSpecifier](https://dev.socrata.com/docs/queries/orderingspecifier).

### Corpus

El corpus contiene 16 casos, ocho por dataset:

- `p6dx-8zbt`: simple, fecha, count, unaccent, OR-5, OR-50, OR-100 y combinada;
- `jbjy-vk9h`: simple, fecha, count, unaccent, OR-5, OR-20, OR-100 y combinada.

Las consultas OR se materializaron desde un catálogo ordenado de 100 términos. El harness preserva
el orden y cualquier repetición: no deduplica, reagrupa ni simplifica ramas. La consulta más larga
midió 8.636 caracteres. Cada caso se materializó una vez y esa misma cadena se reutilizó en A y B.

En cada ronda se aleatorizó el orden de casos. El primer brazo de cada caso se eligió con semilla
`20260827` y se alternó en rondas sucesivas. SODA3 fue primero en 37 pares comparables y segundo en
41. La mejora mediana fue 44,298% cuando SODA3 fue primero y 62,607% cuando fue segundo. La señal,
por tanto, no depende únicamente de recibir una caché calentada por el GET previo, aunque el orden
sí amplifica parte de la diferencia.

### Registro y reproducibilidad

El JSONL de campaña contiene 258 eventos: una metadata, 160 ejecuciones, 80 comparaciones, 16
resúmenes de caso y un resumen de campaña. Por ejecución registra dataset, caso, transporte, SoQL
exacto, longitud, ramas OR, latencia, status, HTTP status, clase de error, bytes, filas, IDs
canónicos y hashes raw/canónico.

```text
artefacto temporal: /tmp/secoppal_soda3_discovery_v1_20260827.jsonl
tamaño:             666.647 bytes
sha256:             509515d7cde3f960cd936ee19f002280a71c155e1ad10f6e06174fcab5977247
fixture sha256:     f7585823d8be0ee653b6e293c75c9cd48404582deb878f7e37deb40cecfa1d74
```

Comando de reproducción desde el worktree, con un archivo nuevo de salida:

```bash
.venv/bin/python scripts/audits/soda3_probe.py \
  --env-file .env \
  --output /tmp/soda3-discovery.jsonl
```

### Validación local

- `ruff check scripts/audits/soda3_probe.py`: pasa;
- `python -m py_compile scripts/audits/soda3_probe.py`: pasa;
- validación interna del corpus: pasa con 16 casos y máximo de 100 ramas OR;
- `pytest -q tests/test_socrata_retry.py`: 4 pruebas pasan;
- `pytest -q`: 9.384 pruebas pasan, 11 skips y 24 xfails esperados;
- `git diff --check`: pasa;
- los cuatro archivos protegidos por el experimento no tienen diff contra `main`.

## 1. Paridad

### Resultado

| Métrica | Resultado |
|---|---:|
| Pares totales | 80 |
| Pares comparables | 78 |
| Paridad estricta en pares comparables | 78/78 (100%) |
| SoQL idéntico | 80/80 (100%) |
| `missing_rows` | 0 |
| `extra_rows` | 0 |
| `serialization` | 0 |
| `type_conversion` | 0 |
| `ordering_only` | 0 |
| `unknown` | 0 |
| `remote_variance` | 2 pares |

Los dos `remote_variance` no fueron divergencias de contenido: ambos transportes agotaron el
presupuesto en los mismos dos casos y ronda. No hubo un resultado exitoso que pudiera compararse.

Hubo 63 pares con conjuntos de IDs no vacíos y comparables. Los 15 pares restantes corresponden a
diez agregaciones `count` sin ID y cinco ejecuciones de la consulta combinada de contratos que
devolvió cero filas en ambos brazos. En los 15 casos se comparó la forma y el hash canónico de la
respuesta; todos coincidieron.

La paridad se decidió antes de habilitar la métrica de rendimiento. Un par solo fue elegible para
latencia si conservó el SoQL, ambos brazos respondieron correctamente y no apareció una divergencia
de contenido. No se atribuyó ganancia a los dos pares con timeout.

## 2. Latencia

| Corte | A clásico p50 | B SODA3 p50 | A p90 | B p90 | A p95 | B p95 | Mejora pareada mediana |
|---|---:|---:|---:|---:|---:|---:|---:|
| Global | 600,675 ms | 189,343 ms | 2.333,012 ms | 478,053 ms | 6.881,970 ms | 939,177 ms | 59,543% |
| Complejas | 656,931 ms | 202,483 ms | 6.437,534 ms | 596,333 ms | 15.657,389 ms | 709,224 ms | 54,670% |
| `p6dx-8zbt` | 600,675 ms | 189,671 ms | 5.250,384 ms | 563,158 ms | 15.125,004 ms | 846,156 ms | 62,708% |
| `jbjy-vk9h` | 611,374 ms | 188,026 ms | 1.567,957 ms | 353,744 ms | 1.959,037 ms | 793,723 ms | 54,028% |

La mejora no fue uniforme. Las consultas de fecha quedaron esencialmente neutras: mediana pareada
de -9,593% en procesos y +13,683% en contratos. La consulta combinada de contratos, que fue vacía,
tuvo -6,842%. Doce de los dieciséis casos superaron 15% de mejora mediana; los beneficios mayores
aparecieron en `count`, `unaccent`, OR largos y la combinada no vacía de procesos.

El p50 del transporte por posición fue estable:

| Posición del brazo | Clásico | SODA3 |
|---|---:|---:|
| Primero | 584,290 ms | 190,230 ms |
| Segundo | 617,060 ms | 189,112 ms |

Esto reduce, pero no elimina, el riesgo de atribuir caché temporal a un transporte.

## 3. Estabilidad y timeouts

| Dataset | Clásico OK/timeout | SODA3 OK/timeout |
|---|---:|---:|
| `p6dx-8zbt` | 40 / 0 | 40 / 0 |
| `jbjy-vk9h` | 38 / 2 | 38 / 2 |
| Total | 78 / 2 | 78 / 2 |

Los cuatro timeouts fueron `ReadTimeout` y ocurrieron en la primera ronda:

- `contract_simple`: clásico 30.003,744 ms; SODA3 31.892,746 ms;
- `contract_unaccent`: clásico 30.003,466 ms; SODA3 30.267,070 ms.

No hubo HTTP 4xx/5xx, errores de decoding ni errores de contrato durante la campaña primaria. Un
smoke anterior registró un `ConnectionError` de SODA3; tres POST mínimos inmediatamente posteriores
respondieron HTTP 200 y la campaña no repitió ese error. Se conserva como señal de varianza de red,
no como fallo reproducible.

Conclusión de estabilidad: SODA3 **no fue peor**, pero tampoco redujo timeouts. La mejora observada
está en la distribución de latencia exitosa, especialmente p90/p95.

## 4. Consultas OR complejas

| Familia | Pares | Longitud máxima | Clásico p50 | SODA3 p50 | Clásico p90 | SODA3 p90 | Mejora pareada mediana |
|---|---:|---:|---:|---:|---:|---:|---:|
| OR-5 | 10 | 689 | 217,944 ms | 188,354 ms | 362,728 ms | 319,356 ms | 16,269% |
| OR-20 | 5 | 1.810 | 335,444 ms | 171,454 ms | 673,100 ms | 263,124 ms | 49,324% |
| OR-50 | 5 | 4.465 | 649,718 ms | 197,076 ms | 723,692 ms | 443,619 ms | 68,251% |
| OR-100 | 10 | 8.636 | 778,576 ms | 240,005 ms | 1.017,886 ms | 618,802 ms | 60,395% |

Los 30 pares OR terminaron correctamente y con paridad estricta. No hubo 414, errores de encoding,
errores SoQL ni timeouts. El clásico también pudo transportar los 8.636 caracteres; por tanto, esta
campaña demuestra una mejora de latencia en OR largos, no la eliminación de un límite binario de
longitud.

La señal crece a partir de OR-20. OR-5 solo produjo una mejora moderada, por lo que no existe
evidencia para enrutar exclusivamente por el mero hecho de encontrar un `OR`.

## 5. Soporte por dataset

### `p6dx-8zbt`

- soporte SODA3: 40/40 respuestas;
- paridad estricta: 40/40;
- mediana pareada: 62,708% de mejora;
- p90: 5.250,384 ms clásico frente a 563,158 ms SODA3;
- soportó `count`, `unaccent`, OR-100 y consulta combinada no vacía.

### `jbjy-vk9h`

- soporte SODA3: 38/40 respuestas;
- el clásico tuvo exactamente el mismo resultado: 38/40;
- paridad estricta: 38/38 pares exitosos;
- mediana pareada: 54,028% de mejora;
- p90: 1.567,957 ms clásico frente a 353,744 ms SODA3;
- los dos transportes agotaron el presupuesto en los mismos casos de la primera ronda;
- la consulta combinada entidad/tema/fecha/estado fue vacía en ambos brazos.

SODA3 está soportado en ambos datasets para las formas probadas. La evidencia de la consulta
combinada de contratos solo prueba compatibilidad sintáctica y paridad del conjunto vacío, no recall
no vacuo.

## 6. Hallazgos de transporte

1. POST SODA3 transportó sin errores las consultas de hasta 8.636 caracteres.
2. La cola p90/p95 mejoró materialmente y mucho más que el p50.
3. La mejora persiste cuando SODA3 se ejecuta primero, aunque es mayor cuando se ejecuta segundo.
4. No se observó reducción de timeouts: el conteo fue 2 contra 2.
5. No hubo diferencia semántica ni de orden en respuestas exitosas.
6. El tamaño de respuesta fue equivalente: mediana de 14.169 bytes clásico y 14.171 bytes SODA3.
7. Todos los éxitos devolvieron HTTP 200 y `application/json`.
8. No se probó `/export`, paginación SODA3, `orderingSpecifier=discard`, shadow ni fallback.

La campaña no permite separar con certeza transporte HTTP de decisiones del backend remoto SODA3.
Sí permite afirmar que el contrato completo POST + ejecución remota produjo menor latencia con el
mismo SoQL y resultado.

## 7. Hallazgos que pertenecen al builder o a la semántica

1. El SoQL no se deduplicó ni simplificó. La ganancia medida no puede atribuirse a cirugía del
   builder.
2. OR-100 llegó a 8.636 caracteres. Es evidencia de costo y bloat potencial, pero no demuestra que
   las ramas sean semánticamente redundantes. Este spike no autoriza deduplicarlas.
3. La consulta combinada de contratos devolvió cero filas en las cinco rondas. La selección de
   entidad, tema, fecha y estado necesita un caso no vacuo en el siguiente corpus. Cambiar esos
   predicados sería trabajo de corpus/builder, no transporte.
4. Las consultas de fecha simples no mostraron ganancia material consistente. La selección de
   transporte no debe basarse únicamente en la presencia de fecha.
5. El corpus usa SoQL estático con forma compatible con SECOPPAL para aislar transporte; no prueba
   todas las proyecciones actuales de `SoQLBuilder`, variantes morfológicas, exclusiones ni frases.

No se encontró evidencia suficiente para modificar `SoQLBuilder` en esta rama.

## 8. Recomendación del siguiente paso

Mantener el código productivo intacto y avanzar a una segunda fase de evidencia, no a migración:

1. repetir esta campaña en al menos tres ventanas temporales independientes;
2. ampliar a consultas reales capturadas de SECOPPAL, incluyendo frases, negaciones, ecosistemas
   institucionales y casos históricamente asociados a timeout;
3. reemplazar la combinada vacía de contratos por un caso no vacuo sin alterar los brazos A/B;
4. conservar 5+ rondas, alternancia, mismo timeout y paridad previa al rendimiento;
5. exigir nuevamente cero `missing_rows`/`extra_rows` y una mejora p90 material por dataset;
6. solo después diseñar un shadow acotado en otra rama y con autorización separada.

No se recomienda migración productiva con una sola campaña. La evidencia actual sí supera el umbral
para continuar el discovery de `/query`: mejora pareada global de 59,543%, mejora p90 de 79,509%,
paridad estricta en todas las respuestas comparables y soporte en ambos datasets.

## Veredicto final

```text
GO_SODA3_QUERY
```

Interpretación: continuar la validación de SODA3 `/query` como candidato de transporte. No implica
cutover productivo, retiro de `sodapy`, creación de una abstracción, feature flag o shadow.

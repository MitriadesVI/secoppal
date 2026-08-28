# SECOPPAL — SODA3: microprobe causal del único bloqueador V1.1

**Fecha local:** 2026-08-27
**Rama:** `experiment/soda3-discovery-v1`
**HEAD de la adjudicación congelada:** `fa287f50ab0ca4f0ed3b1bf9b485786a3d042011`
**Blocker bajo estudio:** `RT-TAB-002__call_002`
**Tipo:** microprobe causal de una sola variable sobre un SoQL congelado. **No** modifica
producción, no implementa la decisión y no abre campaña general.
**Veredicto:** `ORDERING_SPECIFIER_CAUSAL_SIGNAL`

## 1. Pregunta

La adjudicación V1.1 dejó **un** bloqueador real: `RT-TAB-002__call_002`, un fallback H4 que el
clásico resuelve en ~1 s y que SODA3 agotaba 3/3 contra un presupuesto de 3 s. La pregunta que este
microprobe responde no es si la regresión existe —está medida— sino **qué la causa**:

> ¿Falla porque SODA3 no puede ejecutar ese SoQL, o porque el Runtime Canary V1 le impuso
> `orderingSpecifier=total` a una consulta que deliberadamente no lleva `ORDER BY`?

El fallback productivo elimina el orden remoto justamente para abaratar la consulta
(`soql_builder.build(..., include_order=False)`), de modo que exigir un **orden total** al backend
contradice el propósito de la ruta. La documentación de Socrata distingue `total` (impone orden
total), `no_preserve` (ignora `ORDER BY` heredados), y `discard` (ignora heredados y los del
view/query, y puede mejorar rendimiento).

**Hipótesis falsable.** `RT-TAB-002__call_002` falla en SODA3 bajo 3 s por el uso de
`orderingSpecifier=total`, no por una incapacidad intrínseca de SODA3 frente a ese SoQL.

## 2. Procedencia del SoQL — congelado, no reconstruido

| Artefacto | SHA-256 | Verificado |
|---|---|:---:|
| `/tmp/soda3_runtime_canary_manifest_frozen_v3.json` | `880e63ec…5076a` | Sí |
| `RT-TAB-002__call_002` — `exact_soql` | `3fea8b47e5bc3aa844c54ec05668cd95357c6a6459355a6e759e3c96af8df7cf` | Sí |
| `/tmp/soda3_fallback_ordering_probe.jsonl` (salida) | `aaa8dfab3c9cdcb1af380222f716c78267e6e4ee15ce3eee586e8cf49dcb2de6` | — |

El SoQL se lee **exclusivamente** del manifiesto V1, cuyo hash se verifica antes de emitir cualquier
request; el script aborta con `EVIDENCE_GAP` si el manifiesto falta, el hash no coincide, el
`call_id` no es único, el SoQL no cuadra con su propio hash congelado, o si el SoQL contuviera
`ORDER BY`. No se reconstruye por runtime ni se modifica un carácter.

Perfil de la consulta: dataset `jbjy-vk9h`, `SELECT` de 15 columnas, tres bloques `AND` de dos ramas
`OR` cada uno con `unaccent(UPPER(...)) LIKE`, `LIMIT 50`, **sin `ORDER BY`** — es decir,
`result_order_guarantee = unordered_sample`.

## 3. Diseño

Cinco brazos que difieren en **una sola variable** (el `orderingSpecifier` del payload SODA3);
mismo SoQL, mismo dominio, mismo App Token, mismo timeout de cliente (3 s), misma sesión, sin
retries.

| brazo | transporte | endpoint | `orderingSpecifier` |
|---|---|---|---|
| A `classic` | clásico | `GET /resource/jbjy-vk9h.json` | n/a |
| B1 `soda3_total` | SODA3 | `POST /api/v3/views/jbjy-vk9h/query.json` | `total` (control: configuración V1) |
| B2 `soda3_default` | SODA3 | ídem | **omitido** del payload |
| B3 `soda3_no_preserve` | SODA3 | ídem | `no_preserve` |
| B4 `soda3_discard` | SODA3 | ídem | `discard` |

Los cuatro brazos SODA3 mantienen `includeSynthetic: false` y `timeout: 3`. 5 rondas × 5 brazos =
**25 requests**, con el orden de brazos aleatorizado por ronda bajo semilla fija `20260827`:

```text
r1  no_preserve | classic | default | total | discard
r2  discard | total | default | classic | no_preserve
r3  discard | total | default | no_preserve | classic
r4  no_preserve | default | classic | discard | total
r5  discard | no_preserve | default | classic | total
```

El entrelazado importa: `soda3_total` ocupó la 2.ª posición en las rondas 2 y 3, rodeado de brazos
que respondieron con normalidad. Su fallo no puede atribuirse a una ventana temporal degradada.

### Criterio de comparación

El SoQL **no lleva `ORDER BY`** y la ruta se declara `unordered_sample`. Por tanto **no se exige
paridad de ids** entre respuestas exitosas. El objeto del microprobe es
`SUCCESS WITHIN 3 SECONDS`: respuesta válida, filas que satisfacen el mismo SoQL, ausencia de
errores de serialización, y tiempo dentro del presupuesto.

## 4. Resultados

```text
ronda  classic          soda3_total       soda3_default    soda3_no_preserve  soda3_discard
1      ok      811 ms   timeout 3004 ms   ok      776 ms   ok     1892 ms     ok     1052 ms
2      ok      781 ms   timeout 3003 ms   ok      450 ms   ok      140 ms     ok      201 ms
3      ok      763 ms   timeout 3002 ms   ok      441 ms   ok      224 ms     ok      252 ms
4      ok      803 ms   timeout 3003 ms   ok      157 ms   ok      147 ms     ok      220 ms
5      ok      807 ms   timeout 3002 ms   ok      298 ms   ok      192 ms     ok      540 ms
```

| brazo | ok/intentos | HTTP | filas | rango latencia | hashes de conjunto distintos |
|---|:-:|:-:|:-:|---|:-:|
| `classic` | **5/5** | 200 | 50 | 763–811 ms | 1 |
| `soda3_total` | **0/5** | — (timeout) | — | 3002–3004 ms | — |
| `soda3_default` | **5/5** | 200 | 50 | 157–776 ms | 1 |
| `soda3_no_preserve` | **5/5** | 200 | 50 | 140–1892 ms | 1 |
| `soda3_discard` | **5/5** | 200 | 50 | 201–1052 ms | 1 |

Separación total, sin solapamiento: el peor éxito SODA3 (1892 ms) queda **1,1 s por debajo** del
mejor fallo de `total`, y los cinco timeouts de `total` están clavados en el presupuesto
(3002–3004 ms), no rozándolo.

**Resultado excedente, no exigido.** Los cuatro brazos exitosos devolvieron las **mismas 50 filas**:
un único `canonical_hash` de conjunto (`c044be23…`) compartido por classic, `default`,
`no_preserve` y `discard`, en las 20 ejecuciones exitosas, con intersección de ids 50/50 contra el
clásico. El criterio solo pedía éxito dentro del presupuesto; se obtuvo además identidad de
conjunto. Los `response_bytes` difieren en 2 bytes entre clásico (65 295) y SODA3 (65 297), lo que
es serialización, no contenido.

## 5. Gates

### G1 — reproducción de V1: **SÍ**

`soda3_total` reprodujo el patrón V1 exactamente: 5/5 timeout duro contra un clásico que responde
5/5. V1 había medido 3/3 timeout a 3003,7 / 3002,5 / 3003,0 ms; aquí 5/5 a 3002–3004 ms, con el
clásico en 763–811 ms contra los 788–1073 ms de V1. La regresión es estable a un día de distancia y
bajo un scheduler distinto. No es varianza temporal.

### G2 — hipótesis `orderingSpecifier`: **APOYADA**

Las tres condiciones se cumplen sin matices:

- el clásico sigue respondiendo (5/5);
- `total` sigue fallando (0/5), con agotamiento duro;
- `no_preserve`, `discard` **y** el default eliminan el timeout de forma consistente (5/5 cada uno),
  la mayoría de las veces más rápido que el propio clásico.

Como los cinco brazos comparten SoQL, endpoint, token, sesión y presupuesto, y los brazos SODA3
difieren **únicamente** en esa clave del payload, la atribución es directa: la variable manipulada
es la única candidata que queda en pie.

### G3 — candidato conservador: `no_preserve`

`no_preserve` y `discard` funcionan de forma equivalente (5/5 ambos; 140–1892 ms contra
201–1052 ms — la única muestra alta de `no_preserve` es su primera ejecución de la campaña, en
posición 1 de la ronda 1). Ante equivalencia, se recomienda **`no_preserve` como primera opción a
validar**, porque ignora únicamente los `ORDER BY` heredados y preserva cualquier orden declarado
por la propia consulta; `discard` descarta también el del view/query, lo que es más agresivo de lo
que este fallback necesita.

**Esta decisión no se implementa aquí.** Nada de `app/`, `tests/`, `requirements.txt`, SoQLBuilder,
SecopClient, workflow, fallbacks ni configuración productiva fue tocado.

## 6. Alcance e interpretación

Lo que este microprobe establece:

> El único bloqueador V1.1 **no demuestra una regresión intrínseca de SODA3**. Es un artefacto de la
> configuración uniforme del Runtime Canary V1, que aplicó `orderingSpecifier=total` a todas las
> llamadas —incluidas las que el runtime emite deliberadamente sin `ORDER BY`.

Lo que **no** establece, y conviene no sobreleer:

- No mide el resto del corpus. V1 usó `total` en las 249 llamadas; este microprobe solo prueba una.
  Un cambio de política de ordenamiento afectaría a operaciones con `ORDER BY` real, cuya semántica
  bajo `no_preserve` no se ha medido aquí.
- No revalida los gates históricos: V0 `GO_SODA3_QUERY` y V1 `NO_GO_DIVERGENCE` siguen intactos.
- No convierte la adjudicación V1.1 en `GO_INTEGRATION`. `TRUE_MIGRATION_BLOCKERS = 1` sigue siendo
  el recuento correcto **bajo la configuración V1**; lo que cambia es que ese bloqueador ahora tiene
  una causa identificada y reversible fuera de producción.
- La deuda de TOP-N determinístico sigue siendo deuda post-migración, sin cambios.

Reproducción: `scripts/audits/soda3_fallback_ordering_probe.py --env-file .env`. Ejecuciones crudas
en `/tmp/soda3_fallback_ordering_probe.jsonl` (SHA-256 arriba); no persiste token ni cabeceras.

## 7. Dictamen

```text
V1_PATTERN_REPRODUCED:            SÍ  (soda3_total 0/5, timeout duro 3002-3004 ms)
ORDERING_SPECIFIER_CAUSAL:        SÍ
BEST_NON_PRODUCTIVE_CANDIDATE:    no_preserve  (equivalente a discard; más conservador)
VERDICT:                          ORDERING_SPECIFIER_CAUSAL_SIGNAL
PRODUCTION_DIFF:                  CLEAN
```

## 8. Siguiente paso recomendado

`SODA3_QUERY_FINAL_ADJUDICATION`: repetir la campaña de paridad V1 sobre el corpus congelado con una
política de ordenamiento **consciente de la ruta** —`total` donde el SoQL declara `ORDER BY`,
`no_preserve` donde el runtime lo elimina a propósito— y readjudicar los bloqueadores bajo esa
configuración, antes de cualquier decisión de integración.

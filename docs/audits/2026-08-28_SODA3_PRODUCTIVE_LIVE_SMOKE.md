# SODA3 — Smoke LIVE de la implementación productiva (V1)

**Fecha:** 2026-08-28
**Rama:** `feature/soda3-transport-v1`
**HEAD:** `12687c89d5d4bb8b178e158eb78f5ba4cafe3af5`
**Dominio:** `www.datos.gov.co` (live, App Token presente)
**Runner:** `scripts/audits/soda3_productive_live_smoke.py`
**Evidencia:** `/tmp/soda3_productive_live_smoke.jsonl` (49 eventos)
**Veredicto:** `PRODUCTIVE_LIVE_SMOKE_GO`

---

## 1. Qué pregunta responde este smoke — y cuál NO

No vuelve a decidir si SODA3 es mejor transporte. Eso quedó adjudicado en
`docs/audits/2026-08-27_SODA3_QUERY_FINAL_ADJUDICATION.md` → `GO_INTEGRATION`.

La única pregunta aquí es de **costura**:

> ¿La cadena productiva `Settings → SecopalWorkflow → SecopClient →
> Soda3HttpTransport` emite contra `www.datos.gov.co` requests compatibles con
> los contratos que los probes experimentales validaron?

Por eso son cuatro casos y no una campaña: pocos, tomados del manifiesto
congelado del canario V1, ejecutados por el camino real. Sin nueva campaña, sin
estadística, sin tocar builders ni SoQL.

## 2. Preflight

| Comprobación | Resultado |
|---|---|
| Rama | `feature/soda3-transport-v1` |
| HEAD | `12687c8` |
| Working tree trackeado | limpio (`git status --short -uno` vacío) |
| `DEFAULT_TRANSPORT_KIND` | `classic` |
| `SECOPPAL_SOCRATA_TRANSPORT` en el entorno al arrancar | ausente |
| `SECOPPAL_SOCRATA_TRANSPORT` en `.env` | **no está** — el default se aplica por código |
| `.env` modificado | no |
| Flag persistido | no (se pone en el proceso y se restaura al salir) |

Fuente de SoQL congelado: `/tmp/soda3_runtime_canary_manifest_frozen_v3.json`
(`corpus_sha256 = 3785b169…88fb7`, capturado en `dd83818`).

## 3. Metodología: qué se observa y dónde

Dos puntos de observación, ninguno dentro de `app/`:

1. **`RecordingSecopClient(SecopClient)`** — subclase, no proxy. `_client_for` y
   `_query_with_ordering` deciden por `isinstance`, y `derived_for_timeout` mira
   `self.__dict__`: cualquier otra forma cambiaría el camino que se quiere
   observar. Reconstruye el cliente con los mismos parámetros del delegado
   —incluido `transport`— para no degradar SODA3 a clásico ni hornear un timeout
   ajeno en el brazo clásico. Registra operación, dataset, `transport_name`,
   timeout del cliente, `ordering_specifier`, SoQL, status, latencia, filas, IDs
   y una muestra de la fila **ya normalizada**.

2. **`RecordingHttpClient`** — envuelve el `httpx.Client` que el propio
   transporte productivo construyó (vía su propiedad pública `http_client`, con
   sus headers). Es el único punto donde se lee el `orderingSpecifier`
   **efectivo**: el que viaja en el body del POST, no el que creemos haber
   pasado. **No registra headers** — ahí vive el App Token.

Persistencia aislada con `isolated_runtime_stores` (`Settings(**overrides)`):
`find data -newermt "-30 minutes"` quedó vacío tras la corrida.

## 4. Selección del transporte por el mecanismo productivo

Los tres brazos construyen un `SecopalWorkflow` completo desde `Settings`, que
lee la variable de entorno por alias. Ningún brazo instancia
`Soda3HttpTransport` a mano.

| Brazo | env `SECOPPAL_SOCRATA_TRANSPORT` | `settings.secoppal_socrata_transport` | `workflow.secop_client.transport_name` |
|---|---|---|---|
| classic | *(ausente)* | `classic` | **`classic`** |
| soda3 | `soda3` | `soda3` | **`soda3`** |
| rollback | *(ausente)* | `classic` | **`classic`** |

`PRODUCT_WIRING = PASS`. SODA3 exigió el flag explícito; el default siguió siendo
clásico sin ninguna intervención.

## 5. Caso A — query normal con orden

**Consulta:** `licitaciones publicas de obra en atlantico` (`RT-TAB-006`, ruta
`heuristic_only`, dataset `p6dx-8zbt`), atravesando `workflow.run_query` en ambos
brazos con `today = 2026-08-27`.

### SoQL materializado

| | sha256 (12) | longitud |
|---|---|---|
| classic | `3e9fa8a15b58` | 727 |
| soda3 | `3e9fa8a15b58` | 727 |
| rollback | `3e9fa8a15b58` | 727 |

Idéntico byte a byte, y coincide con `RT-TAB-006__call_001` del manifiesto
congelado. El transporte no reescribió el SoQL.

### Resultados

| Métrica | classic | soda3 |
|---|---|---|
| `orderingSpecifier` en el alambre | n/a | **`total`** |
| status | ok | ok |
| filas | 50 | 50 |
| `total_count` | 214 | 214 |
| `result_order_guarantee` | `exact_order` | `exact_order` |
| latencia query | 730,1 ms | **432,7 ms** |
| latencia turno completo | 1.823,9 ms | **1.184,3 ms** |

**Paridad de IDs:** los 50 `id_del_proceso` coinciden en **conjunto y en orden**
(`ids_classic == ids_soda3` exacto). El mismo turno emitió además 1 `count` y 5
`aggregate`: los 5 sha coinciden uno a uno y devuelven el mismo número de filas
(1, 1, 2, 5, 7).

`NORMAL_QUERY = PASS`, `NORMAL_QUERY_PARITY = PASS`.

## 6. Caso B — H4

**Consulta:** `contratos de interventoria tecnica de obra` (`RT-TAB-002`), el
bloqueador causal histórico, por el workflow productivo SODA3.

**El fallback se disparó solo.** No hizo falta la comprobación de costura
sintética: la ruta real se recorrió entera.

| Llamada | timeout | `orderingSpecifier` | status | latencia | filas |
|---|---|---|---|---|---|
| primary `query` | 8 s | `total` | **timeout** | 8.003 ms | — |
| fallback `query` | 3 s | **`no_preserve`** | **ok** | **523,4 ms** | 50 |
| fallback `count` | 3 s | **`total`** | timeout | 3.003 ms | — |

El SoQL del fallback tiene sha
`3fea8b47e5bc3aa844c54ec05668cd95357c6a6459355a6e759e3c96af8df7cf` — **idéntico
byte a byte** al congelado `RT-TAB-002__call_002` de
`docs/audits/2026-08-27_SODA3_FALLBACK_ORDERING_PROBE.md`. El runtime productivo
reprodujo exactamente el SoQL del blocker histórico y lo resolvió en 523 ms.

Contrato de runtime observado en el estado del turno:

```
primary_query_status      = timed_out
fallback_attempted        = True
fallback_used             = True
fallback_strategy         = removed_order_by
fallback_query_status     = completed
result_order_guarantee    = unordered_sample
transformations_applied   = ['removed_order_by']
substantive_filters_preserved = True
```

Se cumple la separación de ejes que la adjudicación exigía: **la query del
fallback va con `no_preserve` y su `count` conserva `total`**. El `count` agotó
—igual que en la captura congelada (`RT-TAB-002__call_003` error)— y el turno
entregó 50 filas con `total_count` aproximado, que es el comportamiento diseñado:
el conteo es un eje independiente del orden.

`H4_PRIMARY = TIMEOUT`, `H4_FALLBACK_TRIGGERED = YES`,
`H4_NO_PRESERVE_PRODUCT_PATH = PASS`.

El timeout del primary **no es una regresión**: la adjudicación V1.1 midió ese
mismo primary agotando 3/3 en **ambos** transportes (8.254 / 8.256 / 8.308 ms,
clásico timeout 3/3). Lo que era regresión SODA3-only —el fallback— dejó de serlo.

## 7. Caso C — count

SoQL congelado `RT-TAB-006__call_002` (sha `fc19773ba124`, dataset `p6dx-8zbt`),
por el `SecopClient` que cada workflow construyó.

| | valor | `orderingSpecifier` | latencia |
|---|---|---|---|
| classic | **214** | n/a | 482,1 ms |
| soda3 | **214** | **`total`** | 482,6 ms |

`COUNT = PASS`, `COUNT_PARITY = PASS`.

## 8. Caso D — aggregate

SoQL congelado `RT-ACTOR-001__call_001` (sha `2d3f98e4b644`, dataset
`jbjy-vk9h`): `SUM(valor_del_contrato)` + `COUNT(*)` de Actor Profile. Se eligió
**a propósito un aggregate escalar**, fuera de la deuda
`TOPN_BOUNDARY_TIE_DEBT_POST_MIGRATION`, para que el smoke fuera inequívoco.

| | `total_value` | `total_count` | `orderingSpecifier` | latencia |
|---|---|---|---|---|
| classic | `1400470611395.71` | `4250` | n/a | 1.068,8 ms |
| soda3 | `1400470611395.71` | `4250` | **`total`** | **145,3 ms** |

Equivalencia semántica exacta. `AGGREGATE = PASS`, `AGGREGATE_PARITY = PASS`.
No se tocó TOP-N; esa deuda sigue post-migración, sin cambios.

## 9. Normalización

Las filas SODA3 pasan por `SecopClient._normalize_row` igual que las clásicas.
Evidencia dura, comparando la fila **cruda del alambre** con la fila que el
cliente devolvió:

**Caso A (`p6dx-8zbt`), primera fila:**

| Campo | crudo SODA3 | normalizado SODA3 | normalizado classic |
|---|---|---|---|
| `id_del_proceso` | `"CO1.REQ.8370443"` (str) | `"CO1.REQ.8370443"` (str) | idem |
| `precio_base` | `"80116617537"` (**str**) | `80116617537.0` (**float**) | `80116617537.0` (float) |
| `urlproceso` | `{'url': '…/STS/Users/Login/Index'}` (**dict**) | `""` (**str**) | `""` (str) |

La URL de `Login` se descartó y, por ser `CO1.REQ.` (requisición
pre-publicación), la jerarquía de reparación decidió correctamente **no** fabricar
link. `_safe_money` convirtió el string a float.

**Caso B, fila del fallback H4 (`jbjy-vk9h`):**

| Campo | crudo SODA3 | normalizado |
|---|---|---|
| `id_contrato` | `"CO1.PCCNTR.8489358"` | `"CO1.PCCNTR.8489358"` |
| `valor_del_contrato` | `"8452876.46"` (str) | `8452876.46` (float) |
| `urlproceso` | `{'url': '…/OpportunityDetail/Index…'}` (dict) | `"https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=…"` (str) |

No se añadió ningún normalizador.

## 10. Errores

Sobre 12 requests SODA3 live:

```
HTTP_4XX_5XX             = 0
DECODE_ERRORS            = 0
CONTRACT_SHAPE_ERRORS    = 0
UNEXPECTED_NETWORK_ERRORS = 0
```

Los únicos dos fallos fueron `Soda3TimeoutError` en el Caso B (primary 8 s y
`count` del fallback 3 s), y **ambos activaron correctamente la política
productiva existente**: el primary abrió H4 porque `Soda3TimeoutError` hereda de
`TimeoutError` y `_is_timeout_exception` lo reconoce por la cadena
`RuntimeError ← Soda3TimeoutError ← ReadTimeout`; el count agotado produjo un
total aproximado sin bloquear la entrega de filas.

No se amplió ningún timeout. `includeSynthetic` fue `False` en los 12 requests.
Los `orderingSpecifier` del alambre: 11 `total`, 1 `no_preserve` (el fallback H4).

## 11. Rollback

Tras el brazo SODA3 se restauró el entorno y se construyó un **workflow nuevo**
con `transport = classic`, repitiendo el Caso A.

- `transport_name = classic`;
- SoQL sha `3e9fa8a15b58` — idéntico;
- 50 filas, `total_count = 214`, `result_order_guarantee = exact_order`;
- los 50 IDs coinciden en **conjunto y orden** con el brazo clásico inicial;
- `SECOPPAL_SOCRATA_TRANSPORT` ausente del entorno al terminar.

Sin cambios de código, sin reinicio especial, sin migración de datos, sin estado
residual, sin fallback automático. `ROLLBACK_TO_CLASSIC = PASS`.

## 12. Producción intacta

```
git diff -- app/ tests/ requirements.txt   → vacío
git diff --check                            → vacío
git status --short --untracked-files=no     → vacío
find data -newermt "-30 minutes" -type f    → vacío
```

`PRODUCTION_DIFF = CLEAN`.

## 13. Veredicto

**`PRODUCTIVE_LIVE_SMOKE_GO`**

La implementación productiva real —seleccionada por configuración, construida por
`SecopalWorkflow`, ejecutada por `SecopClient`, normalizada por el código
productivo y gobernada por la política H4 real— se comporta live de acuerdo con
la evidencia congelada del discovery. Queda autorizada para dogfood/canary más
amplio **con SODA3 todavía opt-in**.

No se declara `GO_CUTOVER`. El default sigue siendo `classic`.

### Lo que este smoke NO demuestra

- No cubre `/export`, que nunca estuvo en la interfaz V1.
- No mide estabilidad bajo concurrencia ni bajo carga sostenida.
- No cierra la deuda `TOPN_BOUNDARY_TIE_DEBT_POST_MIGRATION`: se eligió
  deliberadamente un aggregate fuera de ella.
- No reevalúa el timeout del primary de `RT-TAB-002`, que agota en ambos
  transportes y es un problema de la consulta, no del transporte.

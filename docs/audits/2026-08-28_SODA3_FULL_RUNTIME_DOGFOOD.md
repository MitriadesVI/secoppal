# SODA3 full runtime dogfood V1

**Fecha:** 2026-08-28

**Rama:** `feature/soda3-transport-v1`

**HEAD ejecutado:** `be83deaf99c49d45b9c3e9f74bc1004686cdbbcd`

**Transporte:** `soda3`

**Veredicto:** `SODA3_FULL_RUNTIME_DOGFOOD_GO`

## 1. Conclusión

SECOPPAL recorrió su maquinaria productiva completa con SODA3 como única
fuente remota: búsquedas en ambos datasets, conversación, paginación, lookup,
Actor Profile, analytics, observer, H4 y Radar. Las 41 operaciones del dogfood
salieron por el mismo `workflow.deps.secop_client`; no se construyó un cliente
paralelo ni se instanció `Soda3HttpTransport` a mano.

No hubo abortos de turno, HTTP 4xx/5xx, errores de contrato o decode, fallos
exclusivos y reproducibles de SODA3, ni regresiones semánticas confirmadas. Los
timeouts individuales fueron absorbidos por las políticas existentes. Los
casos que produjeron una respuesta vacía o analítica degradada se reprodujeron
solo de manera focal con classic y quedaron adjudicados como variación de
fuente/runtime, no como regresión del transporte.

```text
SODA3_FULL_RUNTIME_DOGFOOD = GO
NEXT_RECOMMENDED_STEP = MAKE_SODA3_DEFAULT
```

## 2. Preflight y wiring

La Fase 0 ya estaba congelada en el HEAD inicial:

```text
be83dea docs(audit): validate SODA3 productive live transport
```

El proceso del dogfood arrancó con la variable presente antes de construir
`Settings` o el singleton:

```bash
SECOPPAL_SOCRATA_TRANSPORT=soda3 \
SECOPPAL_ACTOR_PROFILE_ENABLED=true \
.venv/bin/python scripts/audits/soda3_full_runtime_dogfood.py ...
```

El runner comprobó al inicio:

```text
get_workflow().secop_client.transport_name = soda3
get_workflow().deps.secop_client is get_workflow().secop_client = true
```

La instrumentación envolvió el `SecopClient` y el `httpx.Client` que el
workflow productivo ya había construido. Las conversaciones y el feedback se
aislaron en un temporal. La pasada Radar utilizó el `RadarStore` productivo
existente y no invocó ningún notifier.

## 3. Evidencia

| Artefacto | Propósito |
|---|---|
| `scripts/audits/soda3_full_runtime_dogfood.py` | runner reproducible, SODA3-only |
| `/tmp/soda3_full_runtime_dogfood_v1.jsonl` | campaña principal: 18 turnos, Radar y 98 eventos; SHA-256 `ecd0bf46f5462ac5e86e2071896897bb6b38aff8241c52a6e869350a50bbbc99` |
| `/tmp/soda3_full_runtime_observer_completion_v1.jsonl` | turno focal 19: confirma observer en modo muestra; SHA-256 `33ebe58f520da520880753103f54999af0ab61bc567467038fdcd641b592de45` |

El último evento de la campaña principal dice `NEEDS_ADJUDICATION`: era un
resumen mecánico previo a las reproducciones focales y contaba los timeouts
manejados como errores. No es el veredicto final. El runner quedó corregido
para separar `fatal_transport_errors` de `handled_operation_errors` y aceptar
`pagination_exhausted` como resultado válido.

### Totales SODA3

| Métrica | Resultado |
|---|---:|
| Turnos humanos | 19 |
| Llamadas remotas | 41 |
| `query` | 26 |
| `count` | 6 |
| `aggregate` | 9 |
| HTTP 200 | 25 |
| Timeout antes de status HTTP | 16 |
| HTTP 4xx/5xx | 0 |
| Errores de contrato/decode | 0 |
| Datasets | `jbjy-vk9h`, `p6dx-8zbt` |

Los 16 timeouts son operaciones individuales, no 16 turnos fallidos. Incluyen
primaries que activaron H4, counts best-effort y un aggregate analítico. Ninguno
escapó como excepción fatal del workflow.

## 4. Ledger por turno

`calls` usa `q/c/a = query/count/aggregate`. `rows/total` conserva el valor que
el producto presentó; `—` significa total no confirmado. Los timeouts fueron
registrados sin cuerpos, headers ni secretos.

| ID | Human query | Route | Dataset | calls | rows/total | timeouts | fallback | ms |
|---|---|---|---|---:|---:|---:|---|---:|
| FU-01 | contratos de aseo en cali de junio de 2026 | `heuristic_only` | `jbjy-vk9h` | 2/0/0 | 0/— | 2 | timed_out | 11,738 |
| FU-02 | y solo los mayores de 300 millones | `heuristic_only` | `jbjy-vk9h` | 1/0/0 | 2/2 | 0 | no | 752 |
| FU-03 | ahora en 2025 | `heuristic_only` | `jbjy-vk9h` | 1/0/0 | 17/17 | 0 | no | 8,089 |
| FU-04 | ordenalos por mayor valor | `heuristic_only` | `jbjy-vk9h` | 1/0/0 | 17/17 | 0 | no | 889 |
| FU-05 | muestrame mas | `pagination_exhausted` | `jbjy-vk9h` | 0/0/0 | 0/17 | 0 | n/a | 3 |
| FU-06 | busca contratos de software en bogota | `heuristic_only` | `jbjy-vk9h` | 2/1/0 | 50/50 | 2 | completed | 14,367 |
| TAB-PROC | procesos de salud en valle del cauca | `heuristic_only` | `p6dx-8zbt` | 1/1/0 | 50/50 | 1 | no | 9,112 |
| TAB-ENTITY-VALUE-DATE | contratos de alimentacion de la gobernacion de sucre por mas de 200 millones en 2025 | `heuristic_only` | `jbjy-vk9h` | 1/0/0 | 4/4 | 0 | no | 3,020 |
| OPP-OPEN | licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas | `heuristic_only` | `p6dx-8zbt` | 1/0/0 | 1/1 | 0 | no | 520 |
| OBJ-OR | que oportunidades hay para construccion de canchas o parques? | `heuristic_only` | `p6dx-8zbt` | 2/0/0 | 0/— | 2 | timed_out | 11,266 |
| NEGATION | contratos de alimentacion sin transporte | `heuristic_only` | `jbjy-vk9h` | 2/1/0 | 50/50 | 2 | completed | 13,774 |
| PHRASE | contratos para elaboracion o actualizacion de manuales de contratacion en 2024 | `heuristic_only` | `jbjy-vk9h` | 2/0/0 | 0/— | 2 | timed_out | 11,915 |
| DIRECT-CONTRACT | CO1.PCCNTR.7754467 | `direct_lookup_id` | `jbjy-vk9h` | 2/0/0 | 1/1 | 0 | n/a | 677 |
| DIRECT-PROCESS | CO1.REQ.9445488 | `direct_lookup_id` | `p6dx-8zbt` | 2/0/0 | 1/1 | 0 | n/a | 334 |
| ACTOR | contratos del NIT 860002400 | `actor_profile` | `jbjy-vk9h` | 1/0/7 | profile/4,250 | 0 | no | 2,693 |
| ANALYTICS-SUM | cuanto suman los contratos de primera infancia en barranquilla 2026 | `analytics_aggregate_sum_error` | `jbjy-vk9h` | 0/0/1 | 0/0 | 1 | no | 10,018 |
| ANALYTICS-STATS | valor promedio de los contratos de obra en Antioquia | `analytics_value_stats` | `jbjy-vk9h` | 0/0/1 | analytic/0 | 0 | no | 1,142 |
| H4-CANDIDATE | contratos de interventoria tecnica de obra | `heuristic_only` | `jbjy-vk9h` | 1/1/0 | 50/50 | 1 | no | 5,870 |
| OBSERVER-COMPLETION | contratos activos de la gobernacion del atlantico | `heuristic_only` | `jbjy-vk9h` | 2/1/0 | 50/50 | 2 | completed | 14,586 |

`pagination_exhausted` es el resultado correcto: el universo refinado contenía
17 filas, todas entregadas en la página anterior. No hubo cursor perdido ni una
consulta silenciosamente omitida.

## 5. Adjudicación focal de anomalías

No se hizo A/B sistemático. Solo se reprodujeron los cuatro resultados
anómalos observados, deteniéndose en cada caso tan pronto hubo clasificación.

| Caso | SODA3 inicial | Classic focal | Repetición SODA3 | Clasificación |
|---|---|---|---|---|
| FU-01 | primary 8 s + H4 3 s agotados | 3 filas, 1,411 ms | 3 filas, 773 ms | `SOURCE/RUNTIME_VARIANCE` |
| OBJ-OR | primary 8 s + H4 3 s agotados | 34 filas, 8,471 ms | 34 filas, 7,838 ms | `SOURCE/RUNTIME_VARIANCE` |
| PHRASE | primary 8 s + H4 3 s agotados | mismos timeouts 8 s + 3 s | no necesaria | `SOURCE/RUNTIME_VARIANCE` |
| ANALYTICS-SUM | aggregate agotó 10 s; respuesta fail-safe | mismo `analytics_aggregate_sum_error`, 3,061 ms | no necesaria | `SOURCE/RUNTIME_VARIANCE` |

En FU-01 y OBJ-OR el SoQL fue idéntico entre la reproducción classic y SODA3.
La repetición SODA3 eliminó el fallo. PHRASE falló igual con classic bajo los
mismos presupuestos. ANALYTICS-SUM alcanzó el mismo resultado de producto con
classic, aunque sin timeout de transporte. Por tanto:

```text
SODA3_ONLY_REPRODUCIBLE_FAILURES = 0
CONFIRMED_SEMANTIC_REGRESSIONS = 0
```

## 6. Rutas especiales y concurrencia

### Actor Profile

El perfil del NIT `860002400` emitió siete aggregates y una query contra
`jbjy-vk9h`. Los siete aggregates devolvieron HTTP 200, el perfil quedó
`partial=false` y reportó 4.250 contratos. Los intervalos registrados contienen
18 pares solapados: hubo trabajo concurrente real sobre el mismo
`Soda3HttpTransport` y su pool `httpx.Client`.

### Analytics y observer

`analytics_value_stats` respondió normalmente por aggregate SODA3. El aggregate
sum degradado quedó clasificado contra classic y no es exclusivo de SODA3. El
observer estuvo presente, `computed_from=sample`, `partial=false`; la rama
concurrente remota ya quedó cubierta por Actor Profile, sin serialización
artificial.

### H4

H4 se activó naturalmente en seis turnos. En FU-06, NEGATION y
OBSERVER-COMPLETION el fallback sin
orden remoto (`orderingSpecifier=no_preserve`) terminó con 50 filas y conservó
los filtros sustantivos. En los otros tres casos agotó su presupuesto y el
producto respondió de forma transparente. No hubo fallback automático entre
transportes.

### Radar

Se ejecutó una sola pasada supervisada del radar piloto existente
`alimentacion costa`:

```text
Radar
→ workflow.deps.secop_client
→ soda3

run_id      = 2e7ff3f22a264422beb4084b7998e756
dataset     = p6dx-8zbt
status      = ok
outcome     = radar_ok
total       = 100
new         = 1
changed     = 0
execution   = 9060 ms
notification_sent = false
```

El radar emitió dos queries y un count. El count agotó su presupuesto best-effort;
la recuperación de candidatos, el diff y la materialización terminaron
coherentemente. La fila del radar quedó sin error, `failure_count=0`.

## 7. Gates finales

```text
FATAL_TRANSPORT_ERRORS = 0
HTTP_CONTRACT_ERRORS = 0
SEMANTIC_REGRESSIONS_CONFIRMED = 0
SODA3_ONLY_REPRODUCIBLE_FAILURES = 0

NORMAL_SEARCH = PASS
FOLLOWUPS = PASS
PAGINATION = PASS
DIRECT_LOOKUP = PASS
ACTOR_PROFILE = PASS
ANALYTICS_OBSERVER = PASS
H4 = PASS
RADAR = PASS
CONCURRENT_RUNTIME_PATH = PASS
```

## 8. Salida requerida

```text
SODA3_FULL_RUNTIME_DOGFOOD: PASS

BRANCH:
feature/soda3-transport-v1

HEAD:
be83deaf99c49d45b9c3e9f74bc1004686cdbbcd

TRANSPORT:
soda3

HUMAN_TURNS:
19

REMOTE_CALLS:
41

DATASETS:
jbjy-vk9h, p6dx-8zbt

NORMAL_SEARCH:
PASS

FOLLOWUPS:
PASS

PAGINATION:
PASS

DIRECT_LOOKUP:
PASS

ACTOR_PROFILE:
PASS

ANALYTICS_OBSERVER:
PASS

H4:
PASS

RADAR:
PASS

CONCURRENT_RUNTIME_PATH:
PASS

HTTP_CONTRACT_ERRORS:
0

SODA3_ONLY_REPRODUCIBLE_FAILURES:
0

CONFIRMED_SEMANTIC_REGRESSIONS:
0

VERDICT:
SODA3_FULL_RUNTIME_DOGFOOD_GO

NEXT_RECOMMENDED_STEP:
MAKE_SODA3_DEFAULT
```

No se cambió el default productivo, no se retiró sodapy y no se añadió fallback
automático. Classic queda disponible como rollback explícito.

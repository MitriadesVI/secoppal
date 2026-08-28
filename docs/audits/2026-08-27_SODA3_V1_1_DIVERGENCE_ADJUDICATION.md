# SECOPPAL — SODA3 V1.1: adjudicación causal de divergencias

**Fecha local:** 2026-08-27
**Rama:** `experiment/soda3-discovery-v1`
**HEAD:** `7f082f2c9f756365159b23c8c44b1f2302507b16`
**Tipo:** adjudicación posterior sobre evidencia congelada. **No** es una campaña nueva, no ejecuta
consultas live, no modifica producción y no implementa fixes.
**Veredicto V1.1:** `V1_1_BLOCKERS_CONFIRMED`

## Gates históricos — no se mueven

| Fase | Gate | Estado |
|---|---|---|
| V0 | `GO_SODA3_QUERY` | intacto |
| V1 | `NO_GO_DIVERGENCE` | intacto |

V1.1 **no reescribe** V1. V1 fue correcto bajo su propio criterio (paridad estricta byte/posición y
cero fallos asimétricos). Esta fase responde una pregunta distinta y posterior:

> ¿cambiar de transporte modificaría una garantía que SECOPPAL realmente promete al usuario?

La distinción operativa que gobierna todo el documento:

```text
diferencia HTTP  ≠  diferencia observable  ≠  diferencia semántica
```

## Resumen ejecutivo

De los 18 pares exitosos no estrictos, **ninguno es un bloqueador de migración**: los seis
operativos causales se explican por tres mecanismos, todos ellos empates reales sobre un SoQL que el
propio runtime emite sin orden total, y en dos de los tres casos la diferencia ni siquiera alcanza
la superficie visible del producto.

El bloqueador real está en otro sitio: de los seis fallos asimétricos desfavorables a SODA3,
**uno de los dos fallbacks sí sería alcanzado por un runtime SODA3** y allí SODA3 no responde
mientras el clásico sí. Ese caso cambia la respuesta al usuario de *«muestra de coincidencias»* a
*«no pude responder»*.

```text
18 pares no estrictos  → 0 bloqueadores
 6 pares SODA3-timeout → 1 operación bloqueante (3 pares), 1 operación inalcanzable (3 pares)
```

## Evidencia y verificación

| Artefacto | SHA-256 | Verificado |
|---|---|:---:|
| `/tmp/soda3_runtime_canary_manifest_frozen_v3.json` | `880e63ec…5076a` | Sí |
| `/tmp/soda3_runtime_canary_replay_v3.jsonl` | `0e35f51a…bb7` | Sí |

Ambos hashes coinciden con los esperados. No se reconstruyó evidencia ni se ejecutó ninguna consulta
live. Matriz estructurada reproducible:
`docs/audits/evidence/2026-08-27_SODA3_V1_1_DIVERGENCE_ADJUDICATION.json`.

### Semántica del clasificador (necesaria para leer las etiquetas)

Del código congelado (`scripts/audits/soda3_probe.py`, reutilizado por el canary):

- `canonical_hash` y `raw_hash` son hashes **de conjunto** (insensibles al orden);
- `ordering_only` ⟺ conjunto idéntico (raw **y** canónico) **y** hash ordenado distinto;
- `unknown` en un par con ambos éxitos ⟺ `canonical_hash` distinto **y** `_shape_signature`
  idéntico **y** `_type_signature` idéntico — es decir, contenido realmente distinto, no
  serialización ni conversión de tipos;
- `canonical_ids` solo existe cuando la proyección incluye `id_contrato` / `id_del_proceso`; los
  agregados no tienen identidad de fila, por eso su divergencia cae en `unknown` y no en
  `missing_rows`/`extra_rows`.

## 1. Universo adjudicado

```text
194 pares exitosos
176 paridad estricta
 18 pares exitosos no estrictos  = 6 operaciones × 3 rondas
  6 pares SODA3-timeout / clásico-ok = 2 operaciones × 3 rondas
 27 pares clásico-timeout / SODA3-ok
```

Las 6 operaciones causales de los 18 pares se reprodujeron **3/3 rondas cada una**, con hashes
estables por transporte. Eso descarta mutación del dataset entre brazos como explicación.

## 2. Tabla causal obligatoria

| operación | dataset | rondas | tipo SoQL | rol runtime | observación A/B | causa | contrato productivo | clasificación | bloquea migración |
|---|---|--:|---|---|---|---|---|---|---|
| `RT-ACTOR-001__call_003` | `jbjy-vk9h` | 3/3 | `GROUP BY nombre_entidad ORDER BY cnt DESC LIMIT 5` | Actor Profile, primary (5 s) | conjunto idéntico (raw y canónico), 5 filas, solo orden distinto | empate en `cnt`: ambos resultados satisfacen el `ORDER BY`, luego toda diferencia posicional está entre filas con `cnt` igual | Actor Profile publica el top con su `cnt`; los valores mostrados son idénticos | `ORDER_TIE_NON_SEMANTIC` | **NO** |
| `RT-ACTOR-001__call_007` | `jbjy-vk9h` | 3/3 | `GROUP BY proveedor… ORDER BY cnt DESC LIMIT 10` | Actor Profile, primary (5 s) | conjunto idéntico, **9 filas < LIMIT 10** → sin truncamiento posible | ídem; además no hay frontera que disputar | ídem | `ORDER_TIE_NON_SEMANTIC` | **NO** |
| `RT-TAB-006__call_001` | `p6dx-8zbt` | 3/3 | `SELECT … ORDER BY fecha DESC, precio_base DESC, id DESC LIMIT 50` | búsqueda tabular, **primary exitoso** (8 s) | conjunto idéntico **y secuencia de `ordered_ids` idéntica**; 50 filas / 40 ids únicos | el swap solo puede ocurrir entre filas que comparten `id_del_proceso` ⟹ empatadas en las **tres** claves del `ORDER BY` | orden presentacional (`ordering_signal` ausente); `result_order_guarantee = exact_order` describe que el `ORDER BY` se honró, y ambos lo honran | `ORDER_TIE_NON_SEMANTIC` | **NO** |
| `RT-TAB-004__call_002` | `jbjy-vk9h` | 3/3 | `SELECT … LIMIT 50` **sin `ORDER BY`** | **fallback H4** degradado (3 s) | 12 ids exclusivos por brazo; misma cardinalidad (50) | `LIMIT` sin `ORDER BY`: ningún transporte tiene selección prometida | la ruta se declara muestra: `result_order_guarantee = unordered_sample`, `degraded=True`, cabecera «*N coincidencias (muestra sin orden garantizado)*» y aviso «*recuperé una muestra de coincidencias*»; filtros sustantivos intactos y `total_count` por eje independiente | `UNORDERED_SAMPLE_EXPECTED_VARIANCE` | **NO** |
| `RT-TAB-007__call_003` | `jbjy-vk9h` | 3/3 | `GROUP BY nombre_entidad ORDER BY cnt DESC LIMIT 5` | insight de universo, primary (4 s) | 5 filas ambos; conjunto distinto, forma y tipos idénticos | si `cnt[5] ≠ cnt[6]` el top-5 es único ⟹ conjunto distinto **implica** empate exacto en la frontera 5/6 | superficie visible = `top_entities[0]` (`entity_concentration_name/_pct`); la frontera 5/6 **nunca llega al usuario** en esta ruta | `TOPN_BOUNDARY_TIE_DEBT_POST_MIGRATION` | **NO** |
| `RT-OPP-003__call_003` | `p6dx-8zbt` | 3/3 | `GROUP BY entidad ORDER BY cnt DESC LIMIT 5` | insight de universo, primary (4 s) | ídem | ídem | ídem | `TOPN_BOUNDARY_TIE_DEBT_POST_MIGRATION` | **NO** |

### A. Mismo contenido, distinto orden — las tres operaciones `ordering_only`

Identificadas: `RT-ACTOR-001__call_003`, `RT-ACTOR-001__call_007`, `RT-TAB-006__call_001`.

`ordering_only` **no** se dio por benigno de oficio. El argumento es de unicidad, no de intuición:
ambos brazos ejecutan el **mismo** SoQL y ambos devuelven un resultado que lo satisface. Por tanto
toda diferencia de posición tiene que ocurrir entre filas **empatadas en todas las claves del
`ORDER BY`**; si no lo estuvieran, uno de los dos brazos estaría violando su propio `ORDER BY`.

- Las dos operaciones de Actor Profile ordenan solo por `cnt`. El conjunto es idéntico, luego la
  frontera del `LIMIT` coincidió; y `call_007` devolvió 9 filas contra un `LIMIT 10`, de modo que
  ni siquiera hubo truncamiento. Cada `cnt` mostrado es idéntico en ambos brazos.
- En `RT-TAB-006__call_001` la evidencia es aún más específica: la **secuencia de `ordered_ids` es
  idéntica** entre transportes (verificado par a par). Con ids iguales en cada posición, el único
  swap posible es entre las 10 filas duplicadas (50 filas / 40 ids únicos), que comparten
  `fecha_de_publicacion_del`, `precio_base` e `id_del_proceso`.

Ninguna viola una garantía de `exact_order`: `exact_order` afirma que el `ORDER BY` se honró, y
ambos lo honran. Ninguna corresponde a un orden semántico: los tres casos tienen `ordering_signal`
ausente (`order_is_presentational` ⟹ orden por defecto del sistema).

**Residual documentado (no bloqueante, no adjudicable con evidencia congelada).**
`_dedupe_results` (`app/core/phrase_ranking.py:57`) deduplica por id conservando *el primero* del
orden canónico. En `RT-TAB-006__call_001` el empate no *desaparece* por deduplicación: puede
propagarse a **cuál** de dos registros empatados del mismo proceso queda visible. Los 40 procesos
mostrados son los mismos y sus claves de orden son idénticas; lo que no puede determinarse es si los
campos no ordenados de esos registros difieren, porque el replay **no persiste contenido de fila**.
Se registra como `EVIDENCE_GAP` acotado, a medir cuando el policy-aware replay capture filas.

### B. `LIMIT 50` sin `ORDER BY` — `RT-TAB-004__call_002`

**Rol en el runtime: fallback degradado H4, no primary.** Determinado desde el código y el
manifiesto, no por intuición:

1. `app/core/workflow/actions/execution.py:145-167` — el primary con `ORDER BY` retorna de
   inmediato si tiene éxito. El fallback vive en el `except` y solo se arma con
   `is_timeout and order_is_presentational(params)` (líneas 183-199), con
   `soql_builder.build(did, fb_params, include_order=False)` y `limit = 50`.
2. El manifiesto lo confirma literalmente: `RT-TAB-004__call_001` (primary, 8 s) y
   `RT-TAB-004__call_002` (3 s) difieren en **exactamente 72 caracteres**, y el diff es
   `ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC`. El `WHERE` es
   idéntico. El mismo patrón aparece en `RT-NEG-002` y `RT-TAB-002`.
3. En captura, `call_001` terminó en `capture_status=error` (timeout de 8 s), que es precisamente
   la condición que abre el fallback.

**El contrato productivo declara esta ruta como muestra sin garantía global**, y lo hace visible:

- `execution.py:220` fija `result_order_guarantee = ORDER_SAMPLE` (`"unordered_sample"`),
  `degraded=True`, `substantive_filters_preserved=True`, `transformations_applied=["removed_order_by"]`;
- `formatter.py:709` sustituye la cabecera por «`N coincidencias (muestra sin orden garantizado)`»
  en vez de «los N más recientes»;
- `rendering.py:107` y `response_policy.py:268` anteponen «⚠️ La búsqueda se demoró; recuperé una
  **muestra** de coincidencias, pero no pude garantizar el orden por fecha ni por valor»;
- `normal_flow.py:250` suprime la cabecera de validación de frases porque sobre una muestra no es
  significativa.

Los 12 ids exclusivos por brazo caen dentro de esa varianza declarada: el `WHERE` es idéntico (SoQL
100% igual, gate V1), los filtros sustantivos están intactos, la cardinalidad es la misma y
`total_count` viaja por un eje independiente. Ningún transporte —tampoco el clásico— tiene prometida
*cuál* subserie de 50 devuelve; la estabilidad observada del clásico es un hecho, no una promesa.

→ `UNORDERED_SAMPLE_EXPECTED_VARIANCE`, `MIGRATION_BLOCKING = NO`.

*Matiz honesto para el equipo de producto (no bloqueante):* el texto visible enfatiza el **orden**;
la advertencia sobre la **selección** la carga únicamente el sustantivo «muestra». Es una nota de
redacción, no un gate.

### C. TOP-N con empate de frontera — `RT-TAB-007__call_003` y `RT-OPP-003__call_003`

Decisión de producto ya tomada y respetada aquí: **no** se implementa TOP-N determinístico antes de
la migración; esta fase solo registra la deuda.

Las cuatro condiciones exigidas se sostienen, pero conviene ser explícito sobre **cómo** se
sostienen, porque una de ellas es deductiva y no observacional:

1. *Mismo `cnt` en los grupos en conflicto* — **deducido**, no leído. Fijado el universo agrupado,
   si el 5.º y el 6.º `cnt` fueran distintos el top-5 sería **único** y ambos brazos tendrían que
   coincidir. Como los conjuntos difieren y ambos resultados satisfacen `ORDER BY cnt DESC LIMIT 5`,
   necesariamente `cnt[5] == cnt[6]`. Premisas: mismo universo subyacente —respaldado por la
   estabilidad de hashes 3/3 por transporte y por los otros 176 pares estrictos— y que ambos backends
   honran el `ORDER BY`, algo que V0 verificó sobre 80 pares sin una sola diferencia de orden.
2. *La única diferencia es qué miembro empatado ocupa la frontera* — se sigue de (1): los rangos con
   `cnt > cnt[5]` son idénticos en ambos brazos.
3. *No cambia ningún valor agregado ni aparece/desaparece un grupo con mejor score* — se sigue de (2).
   `_shape_signature` y `_type_signature` idénticos (deducible de la etiqueta `unknown`) confirman
   que no hay cambio de forma ni de tipos.
4. *Ambos resultados satisfacen el SoQL emitido* — sí; el SoQL es idéntico (gate V1: 249/249).

**Adicionalmente, la frontera no es observable en esta ruta.** `build_top_entities`
(`soql_builder.py:603`) alimenta `observer.py`, y el flujo de búsqueda consume el resultado
**solo por `top_entities[0]`** (`observer.py:346` y `observer.py:388`), que produce
`entity_concentration_name` / `entity_concentration_pct`. El formatter publica una sola línea:
«*X concentra el N% del universo*» (`formatter.py`, `_insights_summary`). Los puestos 2 a 5 —y por
tanto la frontera 5/6— no llegan al usuario en esta ruta.

*Límite declarado:* un empate hipotético en el puesto 1 sí cambiaría el nombre mostrado, aunque no el
porcentaje (mismo `cnt`, mismo total). La evidencia congelada no permite descartarlo porque no
persiste contenido de fila. Es la misma deuda, no una nueva: ambas lecturas satisfacen el SoQL
emitido y son igualmente verdaderas.

→ `TOPN_BOUNDARY_TIE_DEBT_POST_MIGRATION`, `MIGRATION_BLOCKING = NO`. Deuda registrada, a resolver
**después** de la migración.

> **Nota sobre V1.** V1 §7 cita un diagnóstico fuera de métricas con los valores concretos del
> empate (`cnt=3` y `cnt=8`). Ese diagnóstico **no está en los artefactos congelados**; su salida
> cruda no es verificable desde `/tmp`. La adjudicación de arriba no depende de él: se sostiene por
> el argumento de unicidad. Se declara como `EVIDENCE_GAP` acotado para no apoyarse en un dato que
> esta fase no puede reproducir.

## 3. Adjudicación de los fallos asimétricos

### El problema del capture-and-replay

El manifiesto se capturó ejecutando el **runtime clásico**. Por eso el replay ejecuta ramas que un
runtime SODA3 quizá nunca habría alcanzado:

```text
captura clásica:   primary → timeout ⟹ fallback ejecutado ⟹ congelado en el manifiesto
replay:            primary clásico | primary SODA3 | fallback clásico | fallback SODA3
runtime SODA3 real:  primary SODA3 → éxito ⟹ FIN (el fallback nunca ocurre)
```

El control flow es determinista y está en una sola función
(`execution.py`, `_execute_query`): el `return` del camino feliz (línea 152) impide cualquier
fallback; el fallback solo se arma dentro del `except` y solo si `is_timeout`. Reconstruir la
alcanzabilidad es, por tanto, leer el estado del **primary del mismo turno**.

| fallback | primary asociado | primary SODA3 3 rondas | fallback alcanzable | clasificación | bloquea |
|---|---|---|---|---|---|
| `RT-NEG-002__call_002` | `RT-NEG-002__call_001` (query, 8 s) | **ok 3/3** — 233,4 / 511,2 / 253,2 ms (clásico timeout 3/3 a ~8 s) | **inalcanzable** | `FALLBACK_UNREACHABLE_WITH_SODA3_PRIMARY` | **NO** |
| `RT-TAB-002__call_002` | `RT-TAB-002__call_001` (query, 8 s) | **timeout 3/3** — 8254 / 8256 / 8308 ms (clásico timeout 3/3) | **siempre alcanzable** | `SODA3_REACHABLE_REGRESSION` | **SÍ** |

#### `RT-NEG-002__call_002` — inalcanzable, y además una mejora

El primary de ese turno (12 ramas OR, negación de alimentación) **agota 3/3 en clásico y responde
3/3 en SODA3 en 233–511 ms**. Bajo un runtime SODA3 el camino feliz retorna en la línea 152 y el
fallback nunca se construye. El timeout SODA3 3/3 observado en `call_002` es un artefacto del replay
sobre una rama congelada por la captura clásica.

El efecto neto para el usuario es lo contrario de una regresión: hoy recibe una *muestra sin orden
garantizado*; con SODA3 recibiría el resultado ordenado del primary (`result_order_guarantee =
exact_order`).

*Observación adyacente, no bloqueante:* `RT-NEG-002__call_003` (el `count`) agota en ambos
transportes 3/3. Bajo un runtime SODA3 ese `count` se emitiría después del query exitoso (el
fallback también lo emite), de modo que el conteo seguiría degradado. Es el mismo comportamiento que
hoy y es un eje independiente del orden.

#### `RT-TAB-002__call_002` — el bloqueador real

**Ambos** transportes agotan el primary 3/3 a ~8,3 s. Un runtime SODA3 entra por el `except`,
`order_is_presentational` es cierto y arma el fallback con el presupuesto fijo
`FALLBACK_FETCH_SECONDS = 3.0` (`app/core/deadline.py:32`) — el mismo que usó el replay. Y allí:

```text
clásico  ok 3/3      1019,1 / 788,5 / 1072,7 ms
SODA3    timeout 3/3 3003,7 / 3002,5 / 3003,0 ms
```

Consecuencia en el runtime, leída del código: `fallback_query_status = "timed_out"` →
`execution.py:242-252` retorna `results=[]`, `risk_flag_timeout=True`,
`result_order_guarantee = unavailable`. `RT-TAB-002__call_003` (el `count`) también agota en ambos
brazos, así que no hay conteo que rescate la respuesta.

La diferencia observable es exactamente la que importa: hoy el usuario recibe **una muestra de 50
coincidencias**; con SODA3 recibiría **nada**. No es una diferencia de bytes ni de posiciones: es un
turno que deja de responderse. Reproducible 3/3 en el primary y 3/3 en el fallback, con agotamiento
duro del presupuesto (3003 ms clavados), no marginal.

→ `SODA3_REACHABLE_REGRESSION`, `MIGRATION_BLOCKING = SÍ`.

#### Banda observada, sin atribuir mecanismo

El corpus contiene **seis** fallbacks H4 `LIMIT 50` sin `ORDER BY` con presupuesto de 3 s. Tres son
paridad estricta y bastante más rápidos en SODA3 (`RT-TAB-001__call_002`, `RT-OR-001__call_002`,
`RT-NEG-001__call_002`: 155–222 ms contra 173–1136 ms). Los otros tres son exactamente las tres
anomalías de este informe (`RT-TAB-004__call_002` divergencia de contenido,
`RT-NEG-002__call_002` y `RT-TAB-002__call_002` timeouts). La correlación existe pero **no es
suficiente**: la mitad de la banda se comporta perfectamente. Esta fase no atribuye mecanismo; el
diagnóstico de causa pertenece al siguiente bloque.

### Distribución de los 27 pares favorables a SODA3 (resumen, sin adjudicar uno a uno)

27 pares en los que el clásico agotó y SODA3 respondió, repartidos en 14 operaciones de 11 casos
humanos: `RT-PHRASE-001` (6: query y count, 3/3 cada uno), `RT-NEG-002__call_001` (3),
`RT-OR-001__call_001` (3), `RT-TAB-001` count (3), `RT-TAB-004` (3: count 2 + query 1),
`RT-PHRASE-002` (2), `RT-TAB-005` count (2), `RT-OPP-003` (2: aggregate 1 + count 1),
`RT-OPP-002` (1), `RT-OPP-004` (1), `RT-AN-003` (1). Cubre `query`, `count` y `aggregate` en ambos
datasets. Ninguno bloquea: son recuperaciones, no regresiones.

## 4. Apéndice verificable — 18 pair-level records + 6 fallbacks

Los 24 registros completos (hashes de conjunto crudo y canónico por brazo y ronda, ids exclusivos,
latencias, HTTP status, elegibilidad de rendimiento) viven en
`docs/audits/evidence/2026-08-27_SODA3_V1_1_DIVERGENCE_ADJUDICATION.json`, generados directamente
desde el JSONL congelado. Resumen:

| operación | rondas | clásico (filas) | SODA3 (filas) | conjunto idéntico | `ordered_ids` idénticos | ids solo clásico / solo SODA3 | etiqueta harness |
|---|--:|--:|--:|:-:|:-:|--:|---|
| `RT-ACTOR-001__call_003` | 1,2,3 | 5 | 5 | Sí | n/a (agregado) | 0 / 0 | `ordering_only` |
| `RT-ACTOR-001__call_007` | 1,2,3 | 9 | 9 | Sí | n/a (agregado) | 0 / 0 | `ordering_only` |
| `RT-TAB-006__call_001` | 1,2,3 | 50 | 50 | Sí | **Sí** | 0 / 0 | `ordering_only` |
| `RT-TAB-004__call_002` | 1,2,3 | 50 | 50 | No | No | **12 / 12** | `missing_rows`+`extra_rows` |
| `RT-TAB-007__call_003` | 1,2,3 | 5 | 5 | No | n/a (agregado) | 0 / 0 | `unknown` |
| `RT-OPP-003__call_003` | 1,2,3 | 5 | 5 | No | n/a (agregado) | 0 / 0 | `unknown` |
| `RT-NEG-002__call_002` | 1,2,3 | ok | timeout | — | — | — | `remote_variance` |
| `RT-TAB-002__call_002` | 1,2,3 | ok | timeout | — | — | — | `remote_variance` |

Los hashes por transporte son estables en las tres rondas en las seis operaciones exitosas, lo que
excluye variación remota momentánea.

## 5. Dictamen

```text
SUCCESSFUL_NON_STRICT_PAIRS:        18
TRUE_MIGRATION_BLOCKERS:             1   (RT-TAB-002__call_002 — 3 pares)
NON_BLOCKING_TOPN_TIES:              6   (2 operaciones × 3 rondas)
NON_BLOCKING_ORDER_VARIANCE:         9   (3 operaciones × 3 rondas)
NON_BLOCKING_UNORDERED_SAMPLE:       3   (1 operación × 3 rondas)
NEEDS_POLICY_AWARE_REPLAY:           0
EVIDENCE_GAPS:                       2
SODA3_ONLY_TIMEOUT_PAIRS:            6
REACHABLE_SODA3_REGRESSIONS:         3   (1 operación × 3 rondas)
UNREACHABLE_FALLBACK_REGRESSIONS:    3   (1 operación × 3 rondas)
UNRESOLVED_FALLBACK_REGRESSIONS:     0
```

`EVIDENCE_GAPS` declarados, ambos por la misma causa —el replay no persiste contenido de fila— y
ninguno altera una clasificación:

1. los valores `cnt` del empate TOP-N no son observables en los artefactos congelados (el
   diagnóstico citado por V1 §7 no está en `/tmp`); la adjudicación se sostiene por unicidad;
2. el residual de `_dedupe_results` en `RT-TAB-006__call_001` —si los campos no ordenados de dos
   registros empatados del mismo proceso difieren— no es medible aquí.

### Veredicto

```text
V1_1_BLOCKERS_CONFIRMED
```

Queda **una** regresión SODA3 realmente alcanzable por la política del runtime
(`RT-TAB-002__call_002`). Las 18 divergencias de contenido/orden **no** justifican fixes productivos
previos a la migración: son empates reales sobre un SoQL sin orden total, y en dos de los tres
mecanismos ni siquiera alcanzan la superficie visible del producto.

Lectura conjunta, sin reescribir nada:

```text
V1 histórico: NO_GO_DIVERGENCE
V1.1:         1 bloqueador real (fuera de los 18) / 18 pares no estrictos, todos permitidos
```

No se declara `GO_INTEGRATION`: ese dictamen pertenece al siguiente bloque.

## 6. Alcance de esta fase

No se implementó TOP-N determinístico, ni desempates, ni cambios de `ORDER BY`, fallbacks, timeouts,
transporte, feature flags, shadow, cutover ni `/export`. No se ejecutó campaña live. No se retiró
`sodapy`. Archivos creados: este documento y su matriz JSON. `app/`, `tests/` y `requirements.txt`
sin diff.

## 7. Siguiente paso recomendado

Diagnosticar por qué SODA3 agota el presupuesto de 3 s en `RT-TAB-002__call_002` —el único
bloqueador— sin cambiar SoQL, presupuesto ni transporte, y sin abrir otra campaña general.

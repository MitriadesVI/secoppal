# ACTOR_EXACT_PERSONAL_PREFIX_COLLISION_V1

**Implementado. PRIMARY_PERFORMANCE_GATE = NO_GO (conservado);
PERFORMANCE_ADJUDICATION = GO; FINAL_PROMOTION_VERDICT = GO.** El texto que sigue
es el informe de la entrega original y **no se reescribe**: describe el estado en
que la puerta primaria falló. La re-adjudicación posterior, con su propia regla
escrita antes de medir, está al final en
[PERFORMANCE_ADJUDICATION_V1](#performance_adjudication_v1). El nombre personal exacto único
CC ahora requiere comprobar extensiones exactas N2 de otras CC. Una colisión pide
selección; un chequeo inconcluso bloquea perfil, fuzzy y fallback. No cambian N2,
esquema del índice, NIT ni ambigüedades exactas anteriores. No hubo merge ni push.

La decisión funcional está implementada y cubierta, pero el primer benchmark
AFTER incumplió un umbral fijado antes del runtime. Se conserva ese resultado:
una sonda posterior que no reproduce el pico no demuestra su causa ni reemplaza
la puerta fallida. El índice productivo quedó intacto.

- BASE: `345804cf55becaf728d3ec9c20dc9e2f76a3a5b9`.
- BRANCH: `feature/actor-exact-personal-prefix-collision-v1`.
- HEAD de runtime validado: `ddcc6b8` (SHA completo en `source_snapshot.json`).
- WORKTREE: `/Users/rodrigoortiz/Documents/secoppal-actor-exact-personal-prefix-collision-v1`.
- ADR: [094](../decisions/2026-09-06_ADR-094_actor-exact-personal-prefix-collision.md),
  reservado por el coordinador tras máximo global 093 en todos los worktrees.
- INDEX_SCHEMA_CHANGED: **NO**; `actor_alias_index.py` y N2 sin cambios.

## Puertas ejecutadas

Salidas literales completas, incluidos stdout/stderr, comandos y códigos, en
[`artifacts/audits/actor_exact_personal_prefix_collision_v1/`](../../artifacts/audits/actor_exact_personal_prefix_collision_v1/).
Los strings JSON conservan espacios emitidos por herramientas sin introducir
trailing whitespace en archivos versionados.

Ruff, `ruff check app/ tests/ scripts/`:

```text
All checks passed!
```

Semgrep, `semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off`, resumen
literal; salida completa en `semgrep.json` y estructurada en `semgrep_machine.json`:

```text
Ran 3 rules on 269 files: 10 findings.
```

**Baseline global rojo: 10; delta: 0.** Comparación estructurada del rule id,
archivo y líneas inicial/final idéntica al baseline del coordinador. Ninguna
ubicación afectada pertenece a los archivos runtime modificados. El exit code 0
del ejecutable no vuelve verde el baseline Semgrep.

Corrí los 14 archivos enumerados en `focal.json`, no la suite en esa tanda:
resolver, invariantes, candidatos, routing, orquestación, conversación/corpus,
completitud, precedencia geográfica, proyección, propiedades y topes de módulos.

```text
........................................................................ [ 18%]
........................................................................ [ 36%]
........................................................................ [ 54%]
........................................................................ [ 72%]
........................................................................ [ 90%]
.....................................                                    [100%]
397 passed in 74.93s (0:01:14)
```

Después del guard de arbitraje actor/tema, corrí los cuatro archivos afectados
consignados en `topic_conflict_green.json`, no la suite en esa tanda:

```text
........................................................................ [ 39%]
........................................................................ [ 78%]
.......................................                                  [100%]
183 passed in 20.60s
```

Corpus de actor y corpus de consulta (`--mode all` del harness determinista):

```text
Summary: 41/41 PASS, 0 FAIL (mode=all, severity=any, limit=none)
Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
```

`make lint-core` (no sustituye Ruff):

```text
app/core/ limpio
```

`git diff --check` y `git diff BASE..HEAD --check`: stdout vacío, exit code 0.

FULL_SUITE: **PASS**, ejecutada una sola vez por el coordinador en el HEAD de runtime
`ddcc6b8ac4312e63ff34d3e9d187045300412268`; exit code 0.

```text
11738 passed, 11 skipped, 14 xfailed in 505.61s (0:08:25)
```

La suite completa la ejecutó únicamente el coordinador, en serie, después de
congelar fuente/tests/scripts y de terminar focales, auditoría, benchmarks y
dogfood. La entrega no denomina a los focales validación global.

## Fuera del carril y trabajo pendiente

No se tocaron los diez hallazgos Semgrep existentes, Actor Profile Scope
Visibility, Money, Entity Profile, Procurement Foundation/Relationships, Offers,
Competition, Radar, Gazetteer, Supplier Placeholder TOPN ni otros frentes.

Se acreditó otro comportamiento previo: `exact_ambiguous_alias` → documento
explícito se trata como búsqueda nueva y pierde el año anterior, aunque llega a
perfil con identidad explícita. Se reprodujo cargando el resolver/código de
`main @ 345804c`, fixture sintético y persistencia temporal; recibo en
`existing_document_followup_baseline.json`. No se amplió esa política de filtros.
El nuevo camino INCONCLUSIVE → CC/NIT declarado sí usa el reemplazo de actor
existente, purga la incertidumbre y preserva filtros. Una cifra suelta no abre ese
nuevo enganche.

**Pendiente: resolver el incumplimiento de rendimiento antes de promoción.**
No se ajustaron umbrales, no se repitió la puerta primaria para buscar un pase y
no se ocultó el timeout observado en la auditoría. No se verificaron conteos
contractuales actuales: el dogfood usa un doble vacío sin red. La distribución
de aliases no demuestra precisión humana ni adjudica cuál persona quiso el
usuario. No se hace merge/push ni se arranca la app.

## Universo e impacto

El denominador contiene exclusivamente aliases N2 con una única IdentityKey
considerando **todos** los tipos, cuya identidad es CC. Exactos ambiguos previos
y NIT quedan fuera. El oráculo independiente recorre el índice y sus prefijos de
tokens; no reutiliza SQL de rango ni el helper del runtime para decidir colisiones.

| Clasificación | Oráculo completo | Comprobación runtime con presupuesto |
|---|---:|---:|
| eligible_exact_unique_cc_aliases | 1.066.953 | 1.066.953 |
| affected_found | 20.788 | 20.787 |
| unaffected_absent_confirmed | 1.046.165 | 1.046.165 |
| inconclusive_checks | 0 | 1 |

La comprobación inconclusa fue `TimeoutError` en el grupo de cuatro o más tokens.
No se contabiliza como ausencia. Cero discrepancias entre comprobaciones
completas y oráculo. Enumeraciones runtime incompletas en ese recorrido: **0**.
Duración de la auditoría runtime, incluyendo reconstrucción del oráculo: 14,08 s.

| Tokens | Elegibles | FOUND runtime | ABSENT runtime | INCONCLUSIVE | FOUND oráculo |
|---|---:|---:|---:|---:|---:|
| 1 | 10.552 | 1.614 | 8.938 | 0 | 1.614 |
| 2 | 43.090 | 11.210 | 31.880 | 0 | 11.210 |
| 3 | 231.621 | 7.765 | 223.856 | 0 | 7.765 |
| 4+ | 781.690 | 198 | 781.491 | 1 | 199 |

Dentro de FOUND, el **oráculo** clasifica las identidades challenger así:

| Tokens | 1 challenger | 2–9 challengers | ≥10 challengers |
|---|---:|---:|---:|
| 1 | 301 | 568 | 745 |
| 2 | 4.653 | 4.898 | 1.659 |
| 3 | 2.837 | 4.024 | 904 |
| 4+ | 105 | 87 | 7 |

El runtime confirma 7.896 casos de un challenger, 9.576 de 2–9 y 3.315 de ≥10;
el caso inconcluso no entra en esos buckets. El impacto global del oráculo es
1,948 %, concentrado en nombres cortos (15,30 % de los elegibles de un token y
26,02 % de dos). Se reporta sin convertirlo en precisión ni threshold semántico.

## Acceso nominal y rendimiento

SQL definitiva compartida:

```sql
SELECT normalized_alias,raw_alias,raw_aliases_json,document_type,
       base_number,occurrences,first_seen,last_seen
FROM actor_aliases INDEXED BY sqlite_autoindex_actor_aliases_1
WHERE normalized_alias >= ? AND normalized_alias < ?
  AND document_type = 'CC' AND base_number != ?
ORDER BY normalized_alias,document_type,base_number
```

Existencia agrega `LIMIT 1` después de excluir al exacto. Enumeración no limita
aliases: deduplica en streaming y se detiene al probar la décima identidad.
Ambos EXPLAIN contienen literalmente:

```text
SEARCH actor_aliases USING INDEX sqlite_autoindex_actor_aliases_1 (normalized_alias>? AND normalized_alias<?)
USE TEMP B-TREE FOR LAST TERM OF ORDER BY
```

PREFIX_ACCESS / RANGE_ON_NORMALIZED_ALIAS: **YES**, sin SCAN ni migración. El orden
parcial temporal se incluye en la medición y en el presupuesto. El helper de
sucesor, BINARY/UTF-8 real y los extremos Unicode tienen pruebas SQLite focales.

BASELINE_MEASURED: **YES**, antes del runtime, 160 consultas × 50 repeticiones,
5 warmups, después de recorrer el índice. `budget_contract.json` fijó 5 ms de
existencia y 20 ms de enumeración. Como referencia, el p95 máximo de SQL candidata
productiva fue 0,009167/0,0235 ms; casos densos de 10.000 filas alcanzaron p95 de
1,927/2,035 ms o 9,796 ms al deduplicar aliases de la misma CC challenger.
El margen de 5/20 ms se eligió desde esas mediciones, no después del AFTER.

INTERRUPTION_MECHANISM: reloj monotónico, SQLite progress_handler cada 100
instrucciones y comprobaciones Python antes/después de leer/materializar. Un
agotamiento en existencia produce INCONCLUSIVE; después de FOUND conserva
AMBIGUOUS, testigo y exhaustividad desconocida. Tests hacen avanzar un reloj
controlado **durante** VM/streaming y verifican limpieza del handler, además del
rechazo previo por presupuesto cero. No aumenta el timeout SQLite. Es control
cooperativo, no una garantía de preempción del SO o disco.

ACCEPTANCE_THRESHOLDS, por consulta productiva caliente, congelados:

- p95 resolver AFTER ≤ p95 BEFORE propio + 0,5 ms.
- p95 existencia ≤ 0,5 ms.
- p95 enumeración completa, dedup/representante/orden ≤ 1 ms.
- Incumplimiento: NO_GO.

Los JSON conservan p50/p95/max de cada consulta. La siguiente tabla muestra rangos
de p50 y máximo p95 **entre consultas**, no inventa un percentil global a partir
de percentiles individuales:

| Fase | Rango p50 por consulta (ms) | Máximo p95 por consulta (ms) | Máximo absoluto (ms) |
|---|---:|---:|---:|
| Resolver BEFORE | 0,088583–0,108458 | 0,131792 | 0,193125 |
| Resolver AFTER | 0,104667–0,234208 | 1,662333 | 7,594750 |
| Existencia AFTER | 0,003958–0,011833 | 0,112208 | 0,281500 |
| Enumeración AFTER | 0,007500–0,055125 | 0,109250 | 0,378542 |

Un caso de un token/≥10 challengers incumple resolver p95: BEFORE 0,099750 ms,
AFTER 1,662333 ms, límite 0,599750 ms. Su hash es
`00d89a30f637437a54e698a3840c5a527b95f58e9afae93ba7c84a370d8dd0e0`.
Los límites de las dos fases SQL pasan en las 160 consultas. No hubo estado
INCONCLUSIVE dentro del benchmark productivo.

El diagnóstico independiente posterior alternó 2.000 pares de BEFORE, cargado
del blob congelado, y AFTER sobre esa query. Obtuvo p95 de 0,143292/0,272250 ms y
0,128083 ms en prefix. **No reprodujo el pico; no identificó su causa.** Se
conserva separado en `performance_diagnosis.json`, sin reemplazar el NO_GO.
`dense_after.json` añade diez casos sintéticos con comparaciones de resolver y
fases, incluyendo 10.000 aliases/identidades; tampoco sustituye la puerta primaria.

## Autoridad, candidatos y conversación

- CHECK_SEMANTICS: ABSENT_CONFIRMED mantiene el exacto; FOUND prueba ambigüedad;
  INCONCLUSIVE no acredita presencia ni ausencia.
- INCONCLUSIVE: identidad null, candidates vacío, razón explícita;
  FUZZY_ATTEMPTED=NO y EXECUTION_ALLOWED=NO. Strong prefix, bare y conversación
  terminan en política sin otra sonda ni fallback.
- CANDIDATE_CONTRACT: MAX_VISIBLE=10; EXACT_FIRST=YES; mínimo alias representante
  por IdentityKey; orden nominal/tipo/base; no alias_id/inserción/frecuencia;
  `total_candidates_calculated=NO`.
- FOUND_ENUMERATION_FAILURE: exacto y testigo preservados; HAS_MORE=null;
  ENUMERATION_INCOMPLETE=true. La frase «No pude completar la lista» no se
  transforma en «Hay más opciones compatibles».
- RESOLVE_PARITY: `resolve_exact()` y `resolve()` iguales cuando había exacto;
  FTS spy confirma cero intentos en ambigüedad e inconcluso.
- SAME_IDENTITY=UNCHANGED; EXISTING_EXACT_AMBIGUITY=UNCHANGED; NIT_BEHAVIOR=UNCHANGED.
- Selección por rank llega al perfil sin loop ni nuevo lookup, y elimina metadata
  pendiente. Documento explícito CC/NIT responde a INCONCLUSIVE con reemplazo
  acreditado por parser; una cifra desnuda no recibe esa autoridad.

El dogfood encontró una regresión inicial de presentación para TRANSPORTE:
la política uniforme hallaba una extensión CC y creaba botones mientras todavía
preguntaba actor-vs-tema. La regresión roja y la comparación independiente con
BEFORE están conservadas. El correctivo `ddcc6b8` elimina únicamente el nuevo
pendiente prefix durante ese conflicto. El texto previo y cero botones se
preservan; `proveedor transporte` explícito sí presenta candidatos. Las
ambigüedades exactas previas conservan sus controles. La traza subyacente puede
registrar la nueva evidencia estructural, sin abrir selección antes del arbitraje.

## Dogfood local y privacidad

HEAD cargado: `ddcc6b8`, runtime limpio. Flag efectivo: true. Índice:
`/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite`, SHA-256
`464add1dbb479e616ee80a079b11c256db0d67dc6b979e4f285612233bdb8690`.
Esquema 1, N2, build `2026-08-20T18:10:58+00:00`; mismo SHA que BEFORE.

Canal: workflow local/Burr con índice real solo lectura y fuente SECOP doble
EMPTY. Feedback JSON/SQLite, store conversacional y Settings globales redirigidos
a un directorio temporal; caché de settings limpiada antes/después. Chat nuevo
por consulta acredita reset. Sockets bloqueados, LLM sin llamadas, app no
arrancada. Nombres públicos conservados; documentos personales enmascarados.
La fuente doble no acredita conteos actuales.

| Consulta | AFTER | Controles | Llamadas al doble |
|---|---|---:|---:|
| jose david cantillo | AMBIGUOUS / exact_prefix_ambiguous | 3 | 0 |
| jose david cantillo arcon | RESOLVED / exact_unique_alias → perfil | 0 | 8 |
| jose david cantillo solano | RESOLVED / exact_unique_alias → perfil | 0 | 8 |
| proveedor jose cantillo | AMBIGUOUS / exact_ambiguous_alias | 3 | 0 |
| transporte | aclaración actor_or_topic_ambiguous; sin pending | 0 | 0 |

Cantillo corto BEFORE=RESOLVED/exact_unique_alias. AFTER muestra exacto original,
ARCON y SOLANO; `candidates_count=3`, `candidates_has_more=false`,
`enumeration_incomplete=false`. Sus p50/p95 de resolver pasaron de
0,095750/0,115000 ms a 0,151708/0,185750 ms. Los seis temas mantenimiento,
alimentación, adulto mayor, software, vigilancia y aseo tienen regresión
sintética que conserva el baseline.

## Instrumentos, archivos y reproducción

Preflight registrado: Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10;
Codegraph 0.9.6, `codegraph status`: `Index is up to date` en main/base.
Codegraph MCP cargado, status/context/impact consultados. El grafo de la constante
no enumera todos los consumidores: se complementó con `rg` y lectura directa de
resolver, modelo, proyección, policy, trace, candidatos, workflow y selección.
El índice de main no se presenta como actualizado con el runtime del worktree.

Ocho propiedades Hypothesis nuevas, 35 ejemplos máximos cada una,
`derandomize=True`, sin I/O externo: misma identidad, extensión CC distinta,
fuzzy-only, permutaciones, equivalencia N2, evidencia adicional/truncamiento,
inconcluso sin fuzzy y bypass de identidad explícita. Las propiedades semánticas
usan reloj fijo; el agotamiento de tiempo se verifica por separado durante VM y
streaming. Las fixtures densas verifican filas insertadas realmente, sin dejar
que las políticas de suciedad del builder oculten el caso adversarial.

Archivos runtime: helper nuevo `app/data/actor_exact_prefix.py`; enganche mínimo
en resolver; modelo/metadata en actor_name y actor_resolution; presentation/trace
con actor_scope_policy, actor_candidate_ui y actor_resolution_trace; admisión y
arbitraje en workflow/actions/actor_resolution y hook de reemplazo de autoridad
en workflow/actions/parsing. No se modificó `query_plan.py` ni se elevaron topes.
Pruebas propias en `test_actor_exact_prefix*.py` y fixture helper sintético.

Scripts reproducibles en `scripts/audits/actor_prefix_{baseline,validation,dense,dogfood}.py`:

```bash
PY=/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python
INDEX=/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite
# Usar directorio de salida NUEVO; no repinear la evidencia de esta entrega.
$PY scripts/audits/actor_prefix_validation.py --index-path "$INDEX" \
  --evidence-dir /ruta/evidencia-nueva --phase audit
$PY scripts/audits/actor_prefix_validation.py --index-path "$INDEX" \
  --evidence-dir /ruta/evidencia-nueva --phase benchmark
$PY scripts/audits/actor_prefix_dense.py --output /ruta/dense-nuevo.json
$PY scripts/audits/actor_prefix_dogfood.py --index-path "$INDEX" \
  --output /ruta/dogfood-nuevo.json
```

El directorio nuevo de validación necesita copias de `budget_contract.json`,
`baseline_benchmark.json` y `benchmark_queries.json`, sin cambiar su contenido.
El script baseline exige `--baseline-root` con HEAD exactamente BASE, `--index-path`
y `--output-dir` nuevo. No reinterpreta el código AFTER como baseline.

Commits, sin squash:

1. `b7d7126` — test(actor): freeze exact personal prefix authority.
2. `0e6e2c3` — feat(actor): clarify exact personal prefix collisions.
3. `ddcc6b8` — fix(actor): preserve topic arbitration before prefix candidate selection.
4. Cierre documental y evidencia de este informe, posterior a suite serial.

La corrección de arbitraje justifica un cuarto commit frente a la preferencia
inicial de tres. No hay merge ni push. **VERDICT de esta entrega: NO_GO** hasta
resolver la puerta primaria de rendimiento; los pases funcionales no la sustituyen.
Esa puerta se resolvió después, por adjudicación predefinida y no por
reinterpretación: ver PERFORMANCE_ADJUDICATION_V1 al final.

Inventario exacto de cambios: `files_changed.json` en el directorio de evidencia.

## PERFORMANCE_ADJUDICATION_V1

Adjudicación posterior, sólo auditoría. **No sustituye ni borra el resultado
original**: `after_benchmark.json`, `performance_diagnosis.json` y
`budget_contract.json` quedan intactos y la puerta primaria sigue siendo la que
fue. Responde una única pregunta, definida antes de volver a medir: *el
incumplimiento observado, ¿es una degradación reproducible del resolver nuevo o
una observación transitoria no reproducida?*

- PRIMARY_PERFORMANCE_GATE: **NO_GO** (conservado, no reinterpretado).
- PERFORMANCE_ADJUDICATION: **GO** — 0 campañas fallidas de 5.
- ORIGINAL_GATE_FAILURE_RETAINED: **YES**.
- ORIGINAL_SPIKE_REPRODUCED: **NO**.
- FINAL_PROMOTION_VERDICT: **GO**, si no aparece otro blocker.

### Por qué la regla se escribió antes

El diagnóstico de 2.000 repeticiones ya existía y no reprodujo el pico, pero no
anuló el NO_GO porque **no había una regla previa de re-adjudicación**: decidir
después de ver el número es escoger el resultado. Por eso
`performance_adjudication_contract.json` —commits, hash de la consulta, cinco
campañas, 2.000 mediciones por versión, umbrales y regla de decisión— se escribió
y se hasheó **antes** de la primera medición, y el ejecutor vuelve a verificar ese
SHA-256 al arrancar.

CONTRACT_SHA256: `1d77b02ee29520977cb978539b72425b3325e450fd5c9c64d6f97dc2e3ac3270`.

### Condiciones congeladas

- RUNTIME_CHANGED: **NO**. `git status --porcelain -- app` vacío antes y después;
  HEAD `030def55fdf7a013acac8ca4d10419f0d0a47102`. El runtime medido es el de
  `ddcc6b8`; `030def5` sólo añade documentación y evidencia.
- BEFORE real: blob Git de
  `345804cf55becaf728d3ec9c20dc9e2f76a3a5b9:app/data/actor_alias_resolver.py`,
  SHA-256 `82f20e11a1a430f7418113cbc4e7dd52380366508e272ca4252fa060b96ed0a7`,
  cargado con el mismo mecanismo `importlib` de `actor_prefix_dense.py`. No se
  reimplementó el algoritmo anterior. El blob importa `app.core.actor_name` de la
  rama, que sólo añade un estado extra, dos campos opcionales y un validador al
  que sólo llega `method='exact_prefix_ambiguous'`: el camino RESOLVED de BEFORE
  no cambia. Los estados observados lo confirman —BEFORE devolvió `RESOLVED`
  2.000/2.000 y AFTER `AMBIGUOUS` 2.000/2.000 en cada campaña—, así que las dos
  versiones ejecutaron de verdad algoritmos distintos.
- INDEX_CHANGED: **NO**. `data/actor_alias_index.sqlite`, SHA-256
  `464add1dbb479e616ee80a079b11c256db0d67dc6b979e4f285612233bdb8690`,
  662.409.216 bytes, mtime_ns 1787249492985828920, schema 1, N2,
  `built_at` 2026-08-20T18:10:58+00:00. Idéntico al de `manifest.json` y al SHA-256
  recalculado tras las campañas. Abierto `mode=ro&immutable=1` como en producción.
- Sin concurrencia: ninguna suite, dogfood, build de índice, auditoría masiva ni
  otro benchmark SECOPPAL corriendo; las campañas se ejecutaron en serie. La carga
  del sistema (`load average` 4,7–5,0, de la interfaz gráfica del equipo, no de
  procesos SECOPPAL) queda registrada por campaña como metadata diagnóstica, no
  como excusa: **ninguna campaña se descartó por el entorno**.
- Consulta: exactamente la que falló,
  `00d89a30f637437a54e698a3840c5a527b95f58e9afae93ba7c84a370d8dd0e0`, resuelta
  desde `benchmark_queries.json`. Un token, ≥10 challengers. Nunca escrita en claro.

### Diseño

Cinco campañas independientes. Cada una: 20 warmups por versión y 2.000
mediciones por versión, **pareadas e intercaladas**, con el orden alternando por
par (BEFORE primero en los pares, AFTER primero en los impares), de modo que cada
versión pasa la mitad de sus muestras en cada posición y la deriva temporal no
puede favorecer a ninguna. Autoridad del gate: reloj de pared `perf_counter_ns`.
El tiempo de CPU se recogió en una pasada aparte, sólo como diagnóstico, para no
contaminar la medición que decide. Los percentiles son **por campaña**; no se
mezclan campañas en un percentil global. Umbrales sin tocar: resolver Δp95 ≤ 0,5
ms, existencia p95 ≤ 0,5 ms, enumeración p95 ≤ 1,0 ms; presupuestos operativos de
5 ms y 20 ms sin cambios.

### Resultado por campaña

| Campaña | BEFORE p95 (ms) | AFTER p95 (ms) | Δ (ms) | Existencia p95 (ms) | Enumeración p95 (ms) | PASS |
|---|---:|---:|---:|---:|---:|:--:|
| 1 | 0,173708 | 0,346709 | 0,173001 | 0,018458 | 0,068125 | sí |
| 2 | 0,201458 | 0,365541 | 0,164083 | 0,018000 | 0,064209 | sí |
| 3 | 0,195500 | 0,360500 | 0,165000 | 0,019916 | 0,069166 | sí |
| 4 | 0,137750 | 0,271000 | 0,133250 | 0,014792 | 0,063667 | sí |
| 5 | 0,115458 | 0,221875 | 0,106417 | 0,012458 | 0,054459 | sí |

FAIL_CAMPAIGNS: **ninguna**. El peor Δ observado, 0,173 ms, es el 35 % del umbral
de 0,5 ms; el pico primario era 1,563 ms. Ni una campaña se repitió, ni se
descartó, ni se añadió una sexta.

### Qué se reproduce y qué no

El pico **no** desaparece del mundo: los máximos absolutos por campaña llegan a
4,78 / 4,00 / 9,23 / 1,26 / 0,46 ms en AFTER. Pero también en BEFORE —5,50 / 3,08 /
2,91 / 0,45 / 0,26 ms—, con el algoritmo antiguo, sobre el mismo índice y en el
mismo proceso. La cola larga es del entorno, no del resolver nuevo. Lo que no se
reproduce es que esa cola contamine el p95: con n=50 un solo outlier lo arrastra,
con n=2.000 no. Ésa es la lectura honesta del GO —**el pico primario no se
reprodujo bajo una campaña predefinida y suficientemente repetida**—, y no una
afirmación de que el resolver nuevo no tenga cola.

El coste real y sostenido del resolver nuevo en esta consulta es de unos 0,10 ms
de p50 (0,106 → 0,203 ms en la campaña 1), coherente con el tiempo de CPU medido aparte (p95
BEFORE 0,111 ms, AFTER 0,217 ms): el trabajo añadido es cómputo, no espera.

### SANITY_COHORT

Doce consultas del corpus ya congelado —tres exactas CC sin challenger, tres con
uno, tres con 2–9 y tres con ≥10—, elegidas de forma determinista por SHA-256
dentro de cada bucket, 500 pares alternados cada una. Sólo diagnóstico: no entran
en la regla 0/1/≥2.

| Bucket | Δ p95 resolver (ms) | Existencia p95 (ms) | Enumeración p95 (ms) |
|---|---:|---:|---:|
| absent (3) | 0,0150–0,0165 | 0,0053–0,0054 | — |
| one (3) | 0,0397–0,0783 | 0,0115–0,0140 | 0,0087–0,0102 |
| two_to_nine (3) | 0,0820–0,1415 | 0,0173–0,0202 | 0,0176–0,0530 |
| ten_plus (3) | 0,1102–0,1805 | 0,0129–0,0158 | 0,0597–0,0607 |

SYSTEMATIC_REGRESSION_FOUND: **NO**. Ninguna de las doce roza un umbral; el peor
Δ es 0,1805 ms. El coste crece monótonamente con el número de challengers, que es
exactamente lo que el algoritmo predice: sin challenger casi no cuesta nada
(0,015 ms) y sólo el caso denso paga. No hay blocker nuevo que reportar.

### Verificación

`ruff check` limpio sobre los archivos nuevos; los 17 tests del adjudicador pasan;
`git diff --check` limpio. **No se volvió a correr la suite completa**: ya pasó
—11.738 passed, 11 skipped, 14 xfailed— y el runtime no cambió en esta
adjudicación, así que repetirla no aportaría evidencia.

### Reproducción

```bash
PY=/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python
INDEX=/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite
EV=/ruta/evidencia-nueva   # copiar benchmark_queries.json, baseline_benchmark.json
                           # y after_benchmark.json, sin cambiar su contenido
$PY scripts/audits/actor_prefix_adjudication.py --index-path "$INDEX" \
  --evidence-dir "$EV" --phase contract
$PY scripts/audits/actor_prefix_adjudication.py --index-path "$INDEX" \
  --evidence-dir "$EV" --phase run --contract-sha256 <sha impreso arriba>
```

La regla de decisión vive aparte, en `scripts/audits/actor_prefix_adjudicator.py`,
sin reloj ni I/O, y `tests/test_actor_prefix_adjudicator.py` la fija: 0/5 → GO,
1/5 → INCONCLUSIVE, 2+/5 → NO_GO, umbrales exactos, fallo por cada una de las tres
puertas, y rechazo de cualquier número de campañas distinto de cinco —que es la
forma de impedir que una sexta campaña rompa un empate o que una campaña fallida
se caiga de la cuenta.

Mediciones crudas completas —cada valor individual en nanosegundos, no sólo el
p95— en `performance_adjudication_raw.jsonl.gz`. Resumen y veredicto en
`performance_adjudication.json`.

Commit adicional, sin squash ni modificación de los commits de runtime:

5. `test(actor): adjudicate exact-prefix performance gate`.

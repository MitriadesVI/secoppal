# Municipal M1A.1 — fundamentos de lenguaje y territorio

Checkpoint sobre `bc40153`, rama `main`. **Producción municipal: 0 promociones. PUSH: NO.**

Este trabajo conserva byte por byte `aliases_db.json`, `buyer_source_names.json` y las propuestas M0. Las 498 BuyerUnits nuevas y cinco ampliaciones sólo existen en el catálogo temporal de auditoría. La geografía DIVIPOLA sí incorpora una autoridad runtime independiente; no acredita alcaldías, no usa NIT y no deriva BuyerGroups.

## Autoridades y cambios

Se mantienen ADR-040 (territorio/comprador), ADR-069 (sin degradación semántica silenciosa), ADR-072 (ownership exacto no implica elegibilidad de scan) y ADR-074 (BuyerUnit acreditada, distinta del territorio).

**Captura.** `CAPTURE_BOUNDARY` exige whitespace antes y frontera de palabra después de los seis conectores de cláusula. `de/del` siguen siendo conectores internos. Se congelaron 34 fallos predictivos antes del cambio, incluidos los diez ejemplos municipales solicitados y la ESE Carmen Emilia Ospina. Los controles positivos siguen cortando cláusulas reales y R2A.1 conserva su escalera de candidatos.

**Ordenación.** El control se interpreta antes de la reescritura institucional. `ExactNameIndex` acredita offsets de nombres existentes, incluidos aliases con scan deshabilitado: proteger texto no resuelve un owner. Los spans se sustituyen temporalmente por marcadores opacos que impiden formar un comando a través de un nombre, y se restauran después del consumo. No se reordena globalmente el parser ni se retira `nuevo` del vocabulario. La matriz automática DIVIPOLA descubre seis nombres: Nuevo Colón, Nuevo Belén de Bajirá, Pueblo Nuevo, Talaigua Nuevo, Nueva Granada y Castilla La Nueva. Los adjetivos de orden fuera del nombre siguen funcionando.

**Geografía.** `scripts/audits/build_municipal_territories.py` verifica el SHA-256 de la captura DANE congelada por M0 y genera `app/data/municipal_territories_v1.json`: 1.103 municipios/distritos, 120.509 bytes. Se excluyen las 18 áreas no municipalizadas y la isla según la misma clasificación de M0. Cada registro conserva código DIVIPOLA, municipio, departamento y alias normalizado; la cabecera conserva versión, clasificación, captura y procedencia. El loader rechaza versiones, cardinalidad, códigos o aliases inválidos.

La gramática `en X[, departamento Y]` consulta ese registro antes del scan legacy de compradores. Sólo un candidato acreditado produce ciudad y departamento; el SoQL usa igualdad normalizada de ciudad junto al departamento. Los nombres homónimos exigen aclaración. Un nombre que también puede designar un departamento conserva esa lectura como candidato legacy, pero el plan y el builder bloquean su ejecución hasta desambiguar: no se elige una localidad por popularidad. La forma explícita `en el departamento de X` conserva la autoridad departamental. La resolución municipal viaja con el scope en los followups y se elimina cuando se cambia de scope.

El barrido completo detectó una interferencia anterior adicional: el corrector convertía `Ferrer` en `ferrea`. La misma protección por spans DIVIPOLA evita corregir nombres acreditados, sin una excepción por municipio.

**Interacción entre compradores.** Los 1.888 aliases candidatos solos no bastaban. El cruce con aliases existentes encontró que el nuevo comprador de Ciénaga podía capturar el prefijo de un alias del Hospital San Cristóbal. La evidencia previa se conserva. La corrección impide que una reescritura corta parta un nombre acreditado más largo que comienza en el mismo offset; no resuelve un owner desde un alias incompatible ni cambia la política de scan. Se valida además con las familias hospital, universidad y ESE.

## Gates y reproducción

Las cifras finales y sus decisiones se registran en `artifacts/audits/municipal_m1a_1/`. La ejecución no usa LLM ni Socrata. No representa una prueba del proveedor LLM vivo ni garantiza cobertura física adicional de compradores en producción.

```bash
.venv/bin/python3 scripts/audits/build_municipal_territories.py --check
.venv/bin/python3 scripts/audits/replay_municipal_m1a1.py
.venv/bin/python3 scripts/audits/promote_municipal_m1a.py
.venv/bin/python3 scripts/audits/benchmark_municipal_m1a1.py
.venv/bin/python3 scripts/run_query_corpus.py --mode all
.venv/bin/python3 -m pytest tests/ -q
.venv/bin/ruff check app/ tests/ scripts/
make lint-core
```

El promotor sin `--apply` actualiza exclusivamente el preview. El mismo promotor conserva `--apply` para una tarea M1A RETRY separada; M1A.1 no lo ejecuta.

## Resultado semántico

| Gate | Resultado |
|---|---:|
| Propuestas cargadas juntas | 503: 498 nuevas + 5 ampliaciones |
| Aliases candidatos por parser → plan → SoQL | 1.888; 0 fallos, 0 wrong owners |
| Cross-interaction | 3.071 consultas; 0 nuevos wrong owners y 0 owners correctos perdidos |
| Scan nuevo sobre fuentes congeladas | 0 cruces; 42.790 superficies auditadas |
| Nuevas claves de aliases multi-owner | 0 (141 históricas permanecen) |
| Datos e idempotencia | PASS |
| Territorios DIVIPOLA | 1.103 |
| Nombres normalizados distintos | 1.021: 957 únicos + 64 homónimos |
| `en X` sobre todo DIVIPOLA | 953 exactos + 150 aclaraciones + 0 fallos |
| `en X, departamento Y` sobre todo DIVIPOLA | 1.103 exactos |
| `en X` sobre los 501 adicionales M0 | 436 exactos + 65 aclaraciones + 0 fallos |
| Los 501 adicionales con departamento | 501 exactos |
| Producción / propuestas M0 | Bytes originales conservados |

Las 150 aclaraciones completas incluyen 146 municipios con nombres homónimos y cuatro nombres únicos que también pueden designar un departamento. Ninguna elige una localidad por popularidad. Los aliases de comprador conservan su owner incluso cuando el nombre territorial desnudo requiere precisión.

Los tests anteriores que exigían autopromoción de Barranquilla a comprador se sustituyen por el contrato territorial solicitado. Los escenarios de diálogo cuyo scope declarado era un departamento ahora lo dicen explícitamente cuando existen homónimos municipales. Se actualizan únicamente cuatro fingerprints de Barranquilla por el nuevo filtro de ciudad/departamento y el pin del builder, con pruebas semánticas de alcance. El corpus canónico de 89 consultas y `proposals.json` permanecen byte-idénticos. El tipo `QueryPlan` se extrae a su módulo de valores para respetar el límite arquitectónico de tamaño, sin elevarlo ni reordenar políticas.

## Rendimiento

**PERFORMANCE: ACCEPTABLE.** Medición emparejada sin procesos de tests concurrentes, mismas listas y repeticiones que M1A. Ambos catálogos usan el runtime corregido. El baseline sigue sin compradores promovidos; el candidato contiene el lote completo.

| Operación | Baseline p50 / p95 (ms) | Candidato p50 / p95 (ms) |
|---|---:|---:|
| Carga EntityResolver | 84,817 / 85,089 | 106,246 / 109,321 |
| Lookup exacto | 0,0055 / 0,1123 | 0,0055 / 0,1117 |
| Scan | 5,000 / 22,660 | 5,412 / 36,896 |
| Fuzzy | 11,191 / 12,901 | 13,410 / 15,587 |
| Pipeline de canarios | 11,237 / 91,486 | 0,621 / 0,663 |

Registro geográfico: carga p50/p95 **5,732/5,803 ms**; lookup exacto cualificado sobre los 1.103 municipios **0,0024/0,0037 ms**. El candidato evita el fuzzy fallido de las alcaldías aún ausentes en producción; eso explica la mejora del pipeline de estos canarios, no una mejora demostrada en todo el tráfico.

El incremento absoluto del scan p95 es **14,236 ms** y el del fuzzy **2,686 ms**. La carga adicional es **21,429 ms**. No aparece un SLA propio del resolver; el presupuesto de turno existente en `app/core/deadline.py` es 15.000 ms. Estos costes no justifican bloquear la promoción por porcentaje relativo. No se verifica latencia de LLM o Socrata en vivo.

El perfil separa construcción del índice, scan y elección fuzzy. Bajo instrumentación, construir el índice de scan cuesta 5,910 → 6,986 ms. El scan de las 12 consultas compila 6.676 → 11.421 regexes: la recompilación domina ese coste. Copiar las opciones fuzzy cuesta 0,017 → 0,021 ms; domina el scoring, no esa copia. Los tiempos instrumentados sirven para atribuir coste, no sustituyen el benchmark de pared. Se deja identificada la compilación de regex como mejora futura separada; este checkpoint no rediseña EntityResolver.

## Validación y checkpoint

**MUNICIPAL_M1A_1: PASS. M1A_RETRY_READY: YES. PRODUCTION_PROMOTED: 0. PUSH: NO.**

| Runtime | Focales | Suite completa |
|---|---:|---:|
| A — `4912e55` | 403 passed, 1 xfailed | 10.915 passed, 11 skipped, 14 xfailed; complemento de índice: 14 passed |
| B — `5cc7d2f` | 451 passed, 1 xfailed | 10.934 passed, 11 skipped, 14 xfailed; complemento de índice: 14 passed |
| C — `dd38f22` | 484 passed, 1 xfailed | 10.976 passed, 2 skipped, 14 xfailed |
| Interacción — `d81c0f0` | 489 passed, 1 xfailed | 10.981 passed, 2 skipped, 14 xfailed |

A/B se ejecutaron en copias aisladas de cada commit. Nueve pruebas requieren el índice local de actores y se ejecutaron después de enlazarlo; el complemento contiene esas nueve y cinco que ya habían pasado. La cobertura combinada es 10.924 / 10.943 tests distintos aprobados, con sólo los dos skips originales. C y el ajuste final ejecutaron la suite con ese índice disponible. Los logs conservan esta distinción; no se añadieron skips ni xfails.

Cada runtime pasó corpus **89/89**, Ruff y `make lint-core`. El validador del gazetteer registra 599 entidades, 10.802 aliases, **0 errores**, 67 warnings legacy y 141 claves multi-owner frente al tope histórico de 173. La regeneración del registro DIVIPOLA es byte-idéntica. La corrección de interacción pasó además los tests de trailing tokens y el rated eval.

El [checkpoint verificable](../../artifacts/audits/municipal_m1a_1/checkpoint.json) reúne hashes de producción/M0, commits, conteos y resultados. Los detalles están en `pipeline_gate.json`, `geography_gate.json.gz`, `cross_interaction.json.gz`, `data_gates.json`, `performance.json` y `validation/`. La medición de rendimiento es reproducible en método; una nueva ejecución produce tiempos nuevos y requiere revisar de nuevo su veredicto.

El siguiente paso autorizado por este resultado es preparar una **tarea M1A RETRY separada** con el mismo promotor `--apply`. Este checkpoint no lo ejecutó. La cobertura física de producción continúa siendo la de M0/M1A sin promoción.

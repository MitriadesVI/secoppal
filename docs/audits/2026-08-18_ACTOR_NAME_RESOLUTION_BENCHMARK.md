# ACTOR PROFILE AP3-A — Actor Name Resolution Benchmark

**Fecha:** 2026-08-18
**Tipo:** experimento técnico + benchmark + documentación
**Modelo usado:** OpenAI Codex (GPT-5)
**Rama:** `feature/actor-profile-v1`
**Base HEAD:** `9f6464090334739c9697211cf784aa53f03fcedb`
**Main HEAD:** `2403ebed12eeec243ff195e87496885fc6bdc8bd`

## 1. Veredicto

**AP3A_ACTOR_RESOLUTION_BENCHMARK: PASS**

La estrategia precision-first recomendada para AP3-B es:

```text
RECOMMENDED_NORMALIZATION: N2
RECOMMENDED_EXACT_INDEX: normalized alias -> set[IdentityKey]
RECOMMENDED_CANDIDATE_GENERATOR: SQLite FTS5 trigram
RECOMMENDED_RANKING: native FTS5/BM25
RECOMMENDED_RERANKER: NONE
AUTO_RESOLVE_POLICY: exact N2 alias associated with one stable IdentityKey only
EXACT_AMBIGUOUS: AMBIGUOUS; never choose silently
FUZZY: CANDIDATES ONLY
FUZZY_AUTO_RESOLVE: NO
```

La conclusión contraintuitiva es importante: **RapidFuzz no mejoró el ranking**. La mejor configuración rerankeada fue unión FTS5 → `token_set_ratio`, pero redujo Recall@10 de 79,83% a 64,78% y Recall@1 de 67,13% a 49,20%. AP3-B no debe añadir un reranker solo porque la dependencia ya exista.

La política base P0/P1 sí superó el gate de seguridad en el corpus evaluado:

- 5.353 aliases exactos únicos auto-resueltos;
- 0 falsas auto-resoluciones;
- 5 aliases exactos ambiguos, los 5 detectados;
- fuzzy no auto-resuelve ningún caso.

Ninguna política P3 ensayada alcanzó 0 falsas auto-resoluciones. Incluso el corte más restrictivo probado (`score >= 99`, margen `>= 20`) produjo 16 errores entre 217 decisiones, una tasa de 7,37%. **P3 queda rechazado.**

## 2. Alcance y fronteras respetadas

AP3-A no implementó resolución en runtime. No creó índice, tabla ni `ActorId` de producción. No modificó routing, `QueryPlan`, `ActorProfile`, Radar, Truth Layer, warehouse, feature flags ni ningún archivo bajo `app/`.

Los únicos artefactos son:

- script reproducible no-runtime;
- fixture sanitizado;
- review pack humano;
- metadata completa de la corrida;
- tests focales del benchmark;
- este informe.

No se descargó el dataset completo, no se hizo scan local de ~6M filas y no se incorporó ninguna dependencia.

## 3. Preflight

```text
BRANCH: feature/actor-profile-v1
HEAD: 9f6464090334739c9697211cf784aa53f03fcedb
MAIN_HEAD: 2403ebed12eeec243ff195e87496885fc6bdc8bd
WORKTREE_BEFORE: 87 entradas (2 documentos modificados y 85 artefactos untracked ajenos)
```

Los cambios preexistentes fueron preservados. No se usó `stash`, `reset`, `clean`, `restore`, `git add .` ni ninguna operación global sobre el árbol.

## 4. Fuente y adquisición

### 4.1 Contrato descubierto desde HEAD

| Campo | Valor |
|---|---|
| Dominio | `www.datos.gov.co` |
| Dataset | `jbjy-vk9h` |
| Nombre | SECOP II - Contratos Electrónicos |
| Documento | `documento_proveedor` |
| Tipo | `tipodocproveedor` |
| Nombre observado | `proveedor_adjudicado` |
| Contrato | `id_contrato` |
| Fecha | `fecha_de_firma` |

El dataset y los campos se tomaron del `DatasetSpec` y del source contract verificado por AP2/AP2.1; no se hardcodeó un slug histórico alternativo.

### 4.2 Probes previos

Se intentó primero la agregación server-side global requerida. El resultado empírico fue:

| Probe | Resultado | Latencia |
|---|---:|---:|
| `GROUP BY tipo, documento, nombre`, `LIMIT 5`, universo global | abortado, sin resultado | >30 s |
| mismo `GROUP BY`, enero de 2025, `LIMIT 5` | `ReadTimeout` | 25,273 s |
| muestra cruda, `LIMIT 1000`, `ORDER BY :id`, NIT | éxito | 0,800 s |
| muestra cruda, `LIMIT 1000`, `ORDER BY :id`, CC | éxito | 0,451 s |
| agregado dirigido sobre 20 documentos | éxito, 21 grupos | 0,317 s |

La agregación global y la mensual se descartaron; insistir habría violado la regla de adquisición del encargo.

### 4.3 Política final de muestreo

Semilla fija: `20260818`.

1. Muestra ancla ordenada por `:id` y limitada a 50.000 filas NIT + 50.000 filas CC.
2. Normalización documental con la política AP1/AP2.1.
3. Selección de 2.500 identidades lógicas frecuentes por tipo, con desempate pseudoaleatorio fijo.
4. Inclusión dirigida de los documentos contaminados ya verificados `13848603` y `860002400`.
5. Construcción de variantes base/base+DV/base-DV para no perder escrituras reales del NIT.
6. Agregación server-side en lotes de 50 documentos, nunca global.
7. Pseudonimización de IdentityKey CC y enmascaramiento del documento antes de escribir artefactos.

Resultado de adquisición:

| Medida | Valor |
|---|---:|
| Filas ancla | 100.000 |
| Identidades seleccionadas | 2.501 CC + 2.501 NIT |
| Variantes documentales consultadas | 10.011 |
| Lotes agregados | 201 |
| Grupos `(tipo, documento, nombre)` recibidos | 5.949 |
| Tiempo acumulado de llamadas | 49,32 s |

El fixture no conserva la muestra cruda ni las listas de documentos de cada `IN`. Conserva la plantilla de consulta, la semilla, la política, los conteos, el hash y solo los aliases sanitizados necesarios para repetir el benchmark offline.

## 5. Stable identity buckets

La unidad conceptual fue `(document_type, base_number, observed_name)`. Un bucket se consideró estable únicamente si cumplió todo lo siguiente:

- tipo conocido `NIT` o `CC`;
- documento utilizable según AP1/AP2.1;
- al menos 3 ocurrencias totales;
- `dominant_name_share >= 0,50`;
- máximo 25 nombres observados;
- ausencia de colisión del mismo `base_number` entre NIT y CC;
- nombre no vacío ni sentinel;
- al menos un alias limpio.

Una agrupación legítima cuyo nombre dominante contiene `UNIÓN TEMPORAL`, `CONSORCIO` o `UT` se conserva. Cuando el nombre dominante no es una agrupación, los nombres secundarios con esas marcas se excluyen como posibles contratos de una agrupación publicados bajo el documento de un integrante. Esta regla evita convertir el caso documentado de La Previsora en aliases registrales inventados.

### 5.1 Resultado de buckets

| Medida | Valor |
|---|---:|
| Identidades estables | 4.857 |
| Aliases estables | 5.455 |
| Identidades estables multi-alias | 470 |
| Buckets dirty excluidos | 174 |
| Aliases secundarios de grupo excluidos | 153 |

Razones de exclusión —un bucket puede tener más de una—:

| Razón | Buckets |
|---|---:|
| ocurrencias insuficientes | 150 |
| colisión NIT/CC | 32 |
| baja participación del nombre dominante | 3 |
| cardinalidad extrema de aliases | 2 |

Los buckets con `base_number = 13848603` quedaron excluidos por colisión cross-type. El caso no se convirtió en gold limpio.

## 6. Corpus de evaluación

| Estrato | Construcción | Casos |
|---|---|---:|
| S1 exact unique | alias N2 asociado a una sola IdentityKey estable | 5.353 |
| S2 real held-out | leave-one-alias-out; se retira la forma N2 de esa identidad | 937 |
| S3 exact ambiguous | mismo alias N2 asociado a 2+ IdentityKeys estables | 5, todos usados |
| S4 hard negatives | par de aliases de identidades distintas con `WRatio >= 82` | 500 |

El corpus de política contiene 6.295 queries. Los 937 held-outs provienen de 470 identidades multi-alias; el objetivo global `>=1.000 queries` se supera ampliamente, aunque el subestrato S2 por sí solo queda en 937. No se inventaron aliases para llegar a una cifra redonda.

### 6.1 Review pack

El review pack contiene 105 casos:

- 25 held-outs fáciles;
- 25 held-outs difíciles;
- 50 hard negatives;
- las 5 ambigüedades exactas disponibles.

Estados:

- `AUTO_GOLD`: 50;
- `REQUIRES_REVIEW`: 55;
- `EXCLUDED_DIRTY`: se conserva en el fixture de buckets, no se presenta como review gold.

Para CC, el fixture usa un identificador opaco de muestra `CC:sample:<ordinal>` que no se deriva del documento, y los artefactos humanos muestran como máximo los últimos cuatro dígitos. No se persiste la cédula completa. Se descartó un hash simple porque el espacio numérico de cédulas permitiría atacarlo por fuerza bruta.

## 7. Normalizaciones

| Normalización | Exact unique | Ambiguos detectados | Exact unique coverage | Mejor Recall@1 fuzzy | Mejor Recall@10 fuzzy |
|---|---:|---:|---:|---:|---:|
| N0 casefold | 5.421 | 3 | 99,9447% | 69,90% | 78,66% |
| N1 + accent/punctuation | 5.394 | 4 | 99,9259% | 69,37% | 79,83% |
| N2 + whitespace collapse | 5.353 | 5 | 99,9067% | 69,37% | **79,83%** |
| N3 + legal suffix folding | 5.284 | 7 | 99,8677% | 69,26% | 79,83% |

**Recomendación: N2.**

N1 y N2 empatan en recall máximo; N2 produce una representación canónica más estable para exact match y FTS. N3 mejora algunas posiciones bajo reranking, pero crea dos ambigüedades exactas adicionales y no mejora Recall@10 sobre N2. Quitar formas societarias no aporta suficiente beneficio para la llave de autoridad; puede estudiarse como señal secundaria, no como normalización canónica de AP3-B.

`FUNDACIÓN`, `UNIÓN TEMPORAL` y `CONSORCIO` nunca se eliminan.

## 8. SQLite FTS5

```text
sqlite_version(): 3.53.1
ENABLE_FTS5: YES
unicode61: AVAILABLE
trigram: AVAILABLE
```

El tokenizer trigram se creó y consultó realmente; no se infirió desde compile options.

Índice efímero N2 usado para la estrategia ganadora:

| Medida | Valor |
|---|---:|
| Aliases indexados | 5.455 |
| Tamaño | 1.159.168 bytes (1,11 MiB) |
| Build time | 17,23 ms |

El tamaño corresponde al índice experimental con las tablas FTS del benchmark. No es una proyección de producción sobre todo SECOP.

## 9. Estrategias comparadas

Se ejecutaron, para N0–N3:

- normalized exact;
- FTS5 `unicode61` con ranking BM25;
- FTS5 `trigram` con ranking BM25;
- `unicode61` → RapidFuzz `ratio`, `token_set_ratio`, `WRatio`;
- `trigram` → los mismos tres scorers;
- unión `unicode61 ∪ trigram` → los mismos tres scorers;
- RapidFuzz global sobre 500 queries como techo acotado, nunca como propuesta de producción.

### 9.1 Resultado principal sobre S2

Todas las cifras siguientes usan N2 y 937 held-outs, salvo donde se indica N3.

| Estrategia | P@1 / R@1 | R@3 | R@5 | R@10 | MRR |
|---|---:|---:|---:|---:|---:|
| FTS5 unicode61 | **69,37%** | 75,56% | 76,73% | 78,44% | **72,81%** |
| FTS5 trigram | 67,13% | 74,92% | 76,73% | **79,83%** | 71,81% |
| unicode61 → WRatio | 46,21% | 49,63% | 50,59% | 51,44% | 48,49% |
| trigram → WRatio | 46,32% | 50,48% | 52,19% | 54,86% | 49,69% |
| unión → ratio | 44,18% | 51,87% | 54,86% | 58,48% | 49,49% |
| unión → token_set_ratio | 44,82% | 59,23% | 60,94% | 64,46% | 53,00% |
| unión → WRatio | 46,00% | 49,84% | 50,91% | 51,76% | 48,55% |
| mejor RapidFuzz: unión N3 → token_set_ratio | 49,20% | 59,34% | 61,05% | 64,78% | 55,24% |

FTS5 trigram gana por Recall@10. `unicode61` tiene mejor top1/MRR y puede servir como señal secundaria o fallback, pero la unión rerankeada con RapidFuzz destruye parte sustancial del ranking útil de BM25.

### 9.2 Candidate set y latencia ganadora

| Medida | Valor |
|---|---:|
| Candidate set p50 | 50 |
| Candidate set p95 | 50 |
| Candidate set max | 50 |
| Latencia p50 | 0,299 ms |
| Latencia p95 | 0,779 ms |
| Latencia max | 1,129 ms |

El cap experimental fue 50; por eso p50 y p95 llegan al techo. AP3-B debe medir top-K operativamente útil por separado y no interpretar 50 como tamaño óptimo.

### 9.3 RapidFuzz global

Sobre 500 queries, `token_set_ratio` global con N3 obtuvo:

- P@1 / Recall@1: 47,00%;
- Recall@3: 60,00%;
- Recall@5: 62,80%;
- Recall@10: 65,80%;
- MRR: 54,66%;
- latencia p50/p95: 3,77/4,86 ms sobre solo 5.455 aliases.

No mejoró FTS5 y su escalabilidad al universo completo no se evaluó. Sigue siendo únicamente un techo acotado, no arquitectura.

## 10. Políticas de auto-resolution

### P0 — exact normalized unique

```text
auto_resolutions: 5.353
false_auto_resolutions: 0
FALSE_AUTO_RESOLUTION_RATE: 0 / 5.353 = 0,0000%
exact_unique_coverage: 99,9067% de los aliases exactos observados
```

**Aceptada**, siempre que el índice conserve `alias -> set[IdentityKey]` y no aplaste el conjunto a un único actor durante el build.

### P1 — exact normalized ambiguous

```text
ambiguous_aliases: 5
detected_as_ambiguous: 5
AMBIGUITY_DETECTION_RATE: 100%
```

**Aceptada**: estado `AMBIGUOUS`, nunca elección silenciosa.

### P2 — fuzzy candidates only

**Aceptada**. Devuelve candidatos ordenados; no constituye identidad.

### P3 — fuzzy auto-resolve

**Rechazada**. Se barrieron scores 85/90/92/95/97/99 y márgenes 5/10/15/20. Ninguna combinación produjo cero errores. La menor tasa observada fue 6,81% (`score >= 85`, margen `>= 20`, 16 errores/235 decisiones); subir el score a 99 no la eliminó.

Esto muestra por qué un score alto de similitud no puede convertirse en autoridad documental.

## 11. Topic collision set

Ninguno de los diez términos fue un alias exacto N2 en la muestra, pero todos produjeron candidatos fuzzy fuertes:

| Término | Exact actors | Mejor score fuzzy |
|---|---:|---:|
| alimentación | 0 | 90 |
| mantenimiento | 0 | 90 |
| software | 0 | 90 |
| vigilancia | 0 | 90 |
| nutrición | 0 | 90 |
| adulto mayor | 0 | 90 |
| transporte | 0 | 90 |
| obras | 0 | 90 |
| aseo | 0 | 90 |
| salud | 0 | 90 |

Ejemplos observados incluyen organizaciones reales con `software`, `salud`, `aseo`, `nutrición` y `adulto mayor` en el nombre. El hallazgo para AP3-C es vinculante: **la mera existencia de candidatos fuzzy —incluso score 90— no puede activar el intent de actor ni consumir el tópico**. Routing necesita evidencia independiente de intención de actor.

## 12. Gates

| Gate | Estado | Evidencia |
|---|---|---|
| INV-NAME-EXACT-UNIQUE | PASS | 5.353/5.353 correctos |
| INV-NAME-AMBIGUOUS | PASS | 5/5 detectados; 0 elegidos |
| INV-NAME-FUZZY-NOT-AUTHORITY | PASS | P2 candidates only; P3 rechazado |
| INV-NAME-HARD-NEGATIVE | PASS | 500 pares; 50 en review humano |
| INV-TOPIC-PRESERVATION | PASS | set de 10 términos medido; cero cambio de routing |
| INV-ID-TYPE | PASS | 32 buckets cross-type excluidos; tipo en la llave |
| INV-DIRTY-SOURCE | PASS | 174 buckets excluidos; `13848603` no es gold |

## 13. Limitaciones

1. La muestra no es un censo. Favorece documentos frecuentes observados en 100.000 filas ancla ordenadas por el id interno de Socrata.
2. Solo hay 5 ambigüedades exactas estables en esta muestra. La tasa de detección es completa, pero el intervalo empírico es pequeño.
3. S2 contiene 937 queries, ligeramente por debajo de la meta sugerida de 1.000 para ese subestrato; el corpus de política total contiene 6.295.
4. Los thresholds de bucket son conservadores y reducen recall del gold para proteger precisión. No deben trasladarse a producción sin otra medición.
5. `dominant_name_share` detecta contaminación gruesa, no prueba razón social ni identidad registral.
6. Las fechas son las observadas en contratos con `fecha_de_firma`; la fuente omite el campo cuando no existe.
7. Las latencias son locales, sobre 5.455 aliases y SQLite 3.53.1; no predicen el índice completo.
8. La evaluación de política exacta usa el índice completo de aliases conocidos. El held-out evalúa candidate recall, no simula toda forma posible de staleness del índice.
9. El review pack sigue sin etiquetas humanas adjudicadas. `REQUIRES_REVIEW` significa exactamente eso.

## 14. Recomendación AP3-B

AP3-B puede avanzar con estas condiciones:

1. Persistir dos estructuras conceptuales separadas:
   - tabla exacta N2 `normalized_alias -> IdentityKey` sin unique constraint sobre el alias;
   - FTS5 trigram para candidate generation.
2. Auto-resolver solo si la búsqueda exacta devuelve exactamente una IdentityKey estable.
3. Devolver `AMBIGUOUS` si devuelve dos o más, incluyendo sus evidencias; nunca usar BM25 para desempatar una colisión exacta.
4. Para no-match exacto, devolver como máximo top-10 candidatos FTS5. No auto-resolver.
5. No incorporar RapidFuzz en AP3-B v1. Conservar el benchmark para reensayarlo si cambia el corpus o el candidate generator.
6. Mantener `document_type` en toda clave y consulta. Nunca unir NIT/CC por número desnudo.
7. Guardar con cada alias: ocurrencias, primera/última observación y señales de bucket; no llamar razón social al nombre SECOP.
8. Mantener CC completa fuera de artefactos humanos; la persistencia de producción deberá justificar cifrado/retención por separado.
9. AP3-C debe gobernar topic collision antes de activar nombre desnudo como actor intent.
10. Ejecutar revisión humana del pack antes de usar aliases dudosos para cualquier decisión de autoridad.

**AP3B_READY: YES**, para candidate generation y exact-unique only; **NO** para fuzzy auto-resolution.

## 15. Reproducibilidad

```bash
# Repetir offline, sin red
.venv/bin/python scripts/actor_resolution_benchmark.py --quiet

# Regenerar muestra desde SECOP con la política registrada
.venv/bin/python scripts/actor_resolution_benchmark.py --acquire

# Gate focal
.venv/bin/python -m pytest tests/test_actor_resolution_benchmark.py -q
```

El fixture registra SHA-256, semilla, query templates, conteos, versiones y política de exclusión. La metadata conserva las métricas de todas las combinaciones, no solo la ganadora.

## 16. Arquitectura recomendada — cierre explícito

```text
RECOMMENDED_CANDIDATE_GENERATOR: SQLite FTS5 trigram, native BM25 rank
RECOMMENDED_RERANKER: NONE
BEST_RAPIDFUZZ_VARIANT_TESTED: union(unicode61,trigram) -> token_set_ratio
RECOMMENDED_NORMALIZATION: N2
AUTO_RESOLVE_POLICY: exact normalized unique only
FUZZY_AUTO_RESOLVE: NO

EXACT UNIQUE -> AUTO_RESOLVE
EXACT MULTIPLE -> AMBIGUOUS
NO EXACT -> FTS5 CANDIDATES ONLY
FUZZY SCORE/MARGIN -> NEVER IDENTITY AUTHORITY IN V1
```

La recomendación no es que trigram “resuelva” nombres. Trigram recupera candidatos; únicamente el alias exacto N2 asociado a una sola IdentityKey estable puede resolver automáticamente.

## 17. Validaciones de cierre

```text
make lint-core
  app/core/ limpio

pytest -q
  7961 passed, 2 skipped, 24 xfailed

pytest tests/test_actor_resolution_benchmark.py -q
  8 passed

python scripts/run_query_corpus.py --mode all
  89/89 PASS

python scripts/run_dialogue_corpus.py --mode all
  60/61 PASS
  unico fallo: GOB-DEPTO-001, basal/preexistente

git diff -- app/
  vacio

PRODUCTION_CODE_CHANGED: NO
```

`git diff --check` global sigue señalando whitespace en `docs/decisions_log.md`, archivo modificado antes de AP3-A y preservado sin cambios por este bloque. Ningún artefacto AP3-A introduce ese hallazgo.

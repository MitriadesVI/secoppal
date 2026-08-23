# MP1.1 / MP2-PREP — reparación metodológica del experimento Market Pack

**Fecha:** 2026-08-22

**Rama:** `exp/market-pack-adulto-mayor-methodology-v2`

**Padre congelado:** `910cf9d098acb61edf01269b489133b1f06454b8`

**Alcance:** instrumento experimental; sin cambios de runtime, Registry ni conocimiento

```text
METHODOLOGY_REPAIR PASS
VISIBLE_RUNTIME_CHANGE NO
REGISTRY_CHANGED NO
RANKING_SYMMETRY PASS
B1_INDEPENDENCE PASS
LEGACY_COMPARABILITY PASS
SAFETY_METRICS_FALSIFIABLE PASS
TRANSPORT_RANDOMIZATION PASS
OLD_COLD_DEMOTED PASS
OLD_BLIND_PACKET_COMPARATIVE_USE PROHIBITED
TRANSPORT_DIAGNOSIS TIME_WINDOW_EFFECT
```

## 1. Decisión

```text
EXAM_REPAIRED_READY_FOR_EVIDENCE
```

Esta decisión solo declara que el nuevo instrumento puede producir evidencia
comparable en una corrida futura con gold humano. No decide si los Market Packs
sirven, no rehabilita el P@K de MP1 y no autoriza integración productiva.

## 2. WHAT WAS WRONG

MP1 queda preservado como evidencia histórica, pero su comparación no era
causalmente válida por cinco razones:

1. D aplicaba `assessment.score` y A/B conservaban el orden Socrata. El P@K
   comparativo habría mezclado recuperación y un reranker disponible solo para D.
2. B incluía 33 términos/qualifiers informados por el dominio ya observado. No
   era un baseline lexical independiente y el holdout ya había sido consumido.
3. C se trataba como brazo competitivo sobre 204 proyecciones truncadas y
   repetidas. Esas proyecciones contienen 112 `result_key`; la ausencia de una
   señal en el texto proyectado no prueba una pérdida semántica.
4. Cuatro métricas de seguridad se devolvían como cero constante. Una propiedad
   estructural no sustituye un contador que pueda detectar una violación real.
5. El probe live ejecutaba siempre A→B→D una sola vez. Un timeout de una consulta
   OR no separaba complejidad, orden ni variación temporal de Socrata.

El paquete ciego MP1 también heredó la asimetría de ranking. Sus filas aún
pueden recibir etiquetas diagnósticas, pero el paquete es
`NOT_VALID_FOR_COMPARATIVE_P_AT_K`.

## 3. WHAT CHANGED

### 3.1 Tres capas de medición independientes

- **R0 — Retrieval set:** estado de transporte, candidatos, IDs estables,
  intersecciones y exclusivos. Un timeout/erro/abstención conserva
  `candidate_count=null` e `ids=null`; nunca se transforma en cero resultados.
- **R1 — Common rank:** A, B1 y D usan exactamente
  `SOURCE_ORDER_STABLE_DEDUP_V1`, con orden fuente y deduplicación estable. Los
  tests aplican los tres brazos a los mismos IDs y exigen resultados idénticos.
- **R2 — D semantic rank ablation:** `D_ROW_EVIDENCE_SCORE_ABLATION_V1` solo
  compara `D_COMMON_RANK` contra `D_SEMANTIC_RANK`. No participa en P@K
  comparativo ni puede sustentar superioridad de recuperación.

P@10/P@25 permanecen `null` si falta gold humano independiente. Conteo de
candidatos, recall, precisión y timeout se mantienen como magnitudes distintas.

### 3.2 Baselines B

`B_LEGACY_INFORMED_OR_MP1` se conserva únicamente como diagnóstico histórico.
No es gate.

`B1_PRODUCTION_LEXICAL` recibe el `QueryPlan` ya resuelto por el producto y hace
solo una copia defensiva. Por ello A y B1 generan el mismo SoQL: el valor de B1
en este reparo es servir de control de repetición/orden. El módulo B1:

- no importa Legacy;
- no importa Registry;
- no contiene vocabulario de Adulto Mayor, facets, blockers ni Market Pack;
- no altera `QueryRouter`, `QueryPlan`, `SoQLBuilder`, morfología o PhraseSpec.

`B2_FROZEN_CURATED` queda como protocolo y no fue construido ni evaluado. Solo
podrá derivarse de evidencia humana design/train, congelarse antes del nuevo
cold y evaluarse una vez sobre una fuente independiente. Legacy, Registry, D,
el holdout MP1 y el futuro cold son entradas prohibidas.

### 3.3 C como referencia histórica comparable

C ahora se llama `C_HISTORICAL_KNOWLEDGE_REFERENCE`. No tiene precisión ni wins
competitivos. La reanálisis produjo:

| Unidad | Conteo |
|---|---:|
| Proyecciones registradas | 204 |
| `result_key` deduplicados | 112 |
| Ocurrencias duplicadas | 92 |
| `COMPARABLE_TEXT_PRESENT` | 96 |
| `PROJECTION_TEXT_INSUFFICIENT` | 16 |

Los 46 `legacy_only` agregados por MP1 no sobreviven como pérdida. Cada una de
las 16 claves con proyección insuficiente lleva explícitamente
`NO_SEMANTIC_LOSS_INFERENCE_ALLOWED`; los duplicados llevan
`DUPLICATE_RESULT_KEY`.

La proyección sí conserva evidencia histórica deduplicada y observable:

| Evidencia histórica | `result_key` distintos |
|---|---:|
| setting `centro de vida` | 8 |
| setting `centro de bienestar` | 1 |
| setting `centro día` | 8 |
| lexical `tercera edad` | 1 |

Esto documenta cobertura histórica, no relevancia humana, falso negativo ni
victoria frente a D.

### 3.4 Seguridad falsable

Las métricas nuevas se calculan desde trazas inspeccionables:

- pérdida de término explícito compara señales antes/después solo en planes
  realmente ejecutados;
- hijack requiere una operación `REPLACE_EXPLICIT` no autorizada;
- `REVIEW_REQUIRED_USED` inspecciona relaciones realmente consumidas;
- cada expansión consumida debe trazar `concept_id`, relación, estado de review,
  regla de retrieval y permiso efectivo;
- la evidencia de fila declara su campo y una fuente permitida.

Fuentes de fila permitidas:

```text
ROW_OBJECT
ROW_DESCRIPTION
ROW_FIELD
APPROVED_ROW_DERIVATION
```

`QUERY_SIGNAL` no pertenece a ese conjunto. En 54 trazas reales del corpus
diagnóstico/sintético los cinco contadores fueron cero. Cinco controles
negativos independientes provocaron, respectivamente, un conteo mayor que cero:

| Control | Contador esperado | Observado |
|---|---|---:|
| eliminación de término | `EXPLICIT_TERM_LOSSES` | 1 |
| reemplazo no autorizado | `SEMANTIC_HIJACKS` | 1 |
| relación review-required consumida | `REVIEW_REQUIRED_USED` | 1 |
| expansión sin soporte/política | `UNSUPPORTED_EXPANSIONS` | 1 |
| query usada como evidencia de fila | `QUERY_SIGNAL_AS_ROW_EVIDENCE` | 1 |

### 3.5 Probe de transporte repetido y aleatorizado

El probe se ejecutó sin SODA3, con un `SecopClient`, timeout de 15 s, un intento,
misma fecha de referencia, seis consultas MP1 y semilla `20260822`.

| Medida | Resultado |
|---|---:|
| Rondas completas | 3/3 |
| Slots observados | 54/54 |
| Órdenes distintos A/B1/D | 6/6 permutaciones |
| `ok` | 18 |
| `timeout` | 21 |
| D abstained | 15 |
| Pares A/B1 con hash SoQL idéntico | 18 |
| Pares A/B1 exitosos con conjuntos comparables | 9 |
| Diferencias de conjunto A/B1 | 0 |

A y B1 coincidieron en estado en los 18 pares. Sin embargo, dos pares exitosos
con SoQL idéntico tuvieron deltas de latencia materiales (umbral predeclarado en
el analizador: 5 s):

| Query | Ronda | A | B1 | Delta |
|---|---:|---:|---:|---:|
| `alimentación escolar` | 1 | 13,417 s | 2,440 s | 10,977 s |
| `alimentación escolar` | 3 | 2,314 s | 8,999 s | 6,685 s |

D solo fue ejecutable para `alimentación para adulto mayor`: 3/3 timeouts. Su
SoQL tuvo 18 ramas `LIKE`, frente a 20 en A/B1; por tanto no existe evidencia
para atribuir esos timeouts a mayor complejidad de D. El diagnóstico es:

```text
TIME_WINDOW_EFFECT
```

No hubo ninguna terna A/B1/D con tres sets observados; por eso el probe tampoco
produce una conclusión de utilidad de retrieval.

### 3.6 Holdout y paquete ciego

El antiguo cold se renombra conceptualmente a `MP1_DIAGNOSTIC_HOLDOUT`. El YAML
no se modificó y no puede presentarse como `NEW_COLD_V2`.

`NEW_COLD_V2` es únicamente un protocolo vacío. No contiene queries y solo se
podrá llenar después de etiquetas humanas, eventual revisión limitada del
Registry, B2 congelado y selección de una fuente independiente. No podrá
incluir `discovery_gap` ni expectativas de confirmación.

Los hashes de los cuatro artefactos MP1, incluido CSV/map ciegos, se verifican
antes de generar cualquier artefacto MP2-PREP. Permanecen byte por byte iguales.

## 4. REMAINS UNMEASURED

- P@10/P@25, recall y positivos exclusivos: falta gold humano independiente.
- Utilidad comparativa A/B1/D: D no devolvió un set en el probe.
- Efecto del reranking semántico: no hubo candidatos D para la ablación R2.
- B2 curado: protocolo no construido.
- `NEW_COLD_V2`: deliberadamente no creado.
- Generalidad entre mercados y costo de construir un segundo Market Pack.
- Cualquier mejora derivada de revisar `alimentaria`, `centro de vida`, `centro
  día`, `centro de bienestar`, `tercera edad` u otros gaps. Esa revisión queda
  fuera de esta rama.

## 5. Archivos

### Creados

```text
app/experiments/market_packs/methodology.py
app/experiments/market_packs/production_lexical.py
scripts/experiments/market_pack_methodology_repair.py
artifacts/experiments/market_pack_adulto_mayor/mp2_prep/methodology_manifest.json
artifacts/experiments/market_pack_adulto_mayor/mp2_prep/transport_randomized_probe.json
artifacts/experiments/market_pack_adulto_mayor/mp2_prep/legacy_comparability_reanalysis.json
artifacts/experiments/market_pack_adulto_mayor/mp2_prep/safety_negative_controls.json
docs/audits/2026-08-22_MARKET_PACK_METHODOLOGY_REPAIR.md
```

### Modificados — solo experimento/tests

```text
app/experiments/market_packs/models.py
app/experiments/market_packs/evaluator.py
tests/test_market_pack_adulto_mayor.py
```

No se modificaron `semantic_registry_v0.json`, conocimiento/facets/relaciones,
`QueryRouter`, `SoQLBuilder`, ranking productivo, morfología, PhraseSpec,
runtime, SODA3 ni Legacy.

## 6. Validación

```text
make lint-core: PASS
pytest focal tests/test_market_pack_adulto_mayor.py: 28 passed
query corpus: 89/89 PASS
dialogue corpus: 60/61; mismo fallo basal GOB-DEPTO-001
pytest completo: 8295 passed, 2 skipped, 24 xfailed
git diff --check: PASS
```

El único fallo de corpus de diálogo es el basal ya documentado:
`GOB-DEPTO-001`, esperaba `GOBERNACIÓN DE BOYACÁ` y obtuvo `None`.

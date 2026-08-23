# Cierre del spike Semantic Registry + Market Pack

**Fecha:** 2026-08-23
**Rama de cierre:** `exp/semantic-radar-adulto-mayor-final` (HEAD `118123f` antes de este commit)
**ADR asociado:** ADR-062 en [`docs/decisions_log.md`](../decisions_log.md)
**Naturaleza:** cierre documental. No se ejecutaron pruebas nuevas, no se modificó
runtime, no se reparó el resultado y no se probó Infraestructura Vial.

```text
SEMANTIC_MARKET_PACK_SPIKE:
CLOSED

FINAL_RUNTIME_VERDICT:
NO_GO

SEARCH_MARKET_PACK:
NO_GO

RADAR_MARKET_PACK:
NO_GO

SEMANTIC_REGISTRY:
KEEP_ADVISORY

INFRAESTRUCTURA_VIAL_TRANSFER_TEST:
CANCELLED

RECOMMENDED_RADAR_BASELINE:
CURATED_MARKET_DEFINITION

FUTURE_SEMANTIC_DIRECTION:
POST_RETRIEVAL_RERANK_OR_CLASSIFICATION
```

## 1. Contexto

El spike preguntaba si un **Market Pack** — una definición de mercado ejecutada
sobre el **Semantic Registry v0** (conceptos, relaciones tipadas, qualifiers y
política de abstención) — podía sustituir o superar a la recuperación actual en
dos frentes:

1. **Search**: reemplazar el retrieval clásico por expansión semántica de consulta.
2. **Radar**: clasificar pertenencia a mercado con mejor utilidad operativa que
   una definición curada lexical.

El protocolo preestablecido fijaba que la transferencia a un segundo mercado
(Infraestructura Vial) solo procedía si el primer mercado cerraba en `GO` o
`KEEP_WITH_LIMITS`.

## 2. Experimentos ejecutados

| Fase | Objeto | Autoridad documental |
|---|---|---|
| MP1 | Spike inicial del Market Pack Adulto Mayor | `docs/audits/2026-08-22_MARKET_PACK_ADULTO_MAYOR_SPIKE.md` |
| MP1-R | Reparación de la metodología de evaluación | `docs/audits/2026-08-22_MARKET_PACK_METHODOLOGY_REPAIR.md` |
| MP1-D | Análisis de labels diagnósticos ciegos | `docs/audits/2026-08-22_MP1_DIAGNOSTIC_LABEL_ANALYSIS.md` |
| MP2 | B2 (strong curated OR) + protocolo de cold independiente | `docs/audits/2026-08-22_MP2_B2_AND_COLD_PROTOCOL.md` |
| MP2-FINAL (Search) | Sonda de retrieval A/B2/D sobre cold v2 en vivo | `artifacts/experiments/market_pack_adulto_mayor/mp2_final/20260823T033843Z/` |
| SRADAR-2 (Radar) | Veredicto final de clasificación de mercado | `docs/audits/2026-08-23_ADULTO_MAYOR_SEMANTIC_RADAR_FINAL.md` |

Brazos comparados:

- **A_CLASSIC** — retrieval de producción vigente.
- **B2_STRONG_CURATED_OR** — definición de mercado curada: términos positivos,
  frases y exclusiones, en un OR plano.
- **D_SEMANTIC_REGISTRY_HYBRID** — Market Pack sobre el Semantic Registry con
  política conservadora de abstención y evidencia de fila requerida.

## 3. Evidencia

### 3.1 Search — `FINAL_EXPERIMENT_TRANSPORT_INCONCLUSIVE`

Run `20260823T033843Z`, 36 casos de cold v2 × 3 rondas = 108 slots por brazo,
seis permutaciones balanceadas (18 cada una), semilla `20260822`, cliente único
Socrata, `timeout=15s`, `max_attempts=1`, sin reintento oculto.

| Brazo | Slots | OK (brazo completo) | Timeout | Dependency timeout |
|---|---:|---:|---:|---:|
| A_CLASSIC | 108 | 42 | 66 | 0 |
| B2_STRONG_CURATED_OR | 108 | 24 | 18 | 66 |
| D_SEMANTIC_REGISTRY_HYBRID | 108 | 42 | 0 | 66 |

```text
paired_query_count: 9
minimum_paired_queries: 24
transport_sufficient_for_blind_adjudication: false
verdict_emitted: false
human_metrics_calculated: false
winner_analyzed: false
```

El transporte no alcanzó el mínimo de consultas pareadas, por lo que **no se
emitió veredicto comparativo de Search**. La sonda sí dejó dos hechos que no
dependen del transporte:

- el componente semántico de D registró **108/108 abstenciones**
  (`component_status_counts.abstained = 108`); D nunca aportó expansión propia;
- por eso D no tuvo timeouts propios y sus 42 rondas exitosas son **passthrough**
  del brazo clásico, con las mismas 1.606 ocurrencias de candidatos observadas
  que A_CLASSIC;
- cero violaciones de seguridad en las 108 trazas
  (`SEMANTIC_HIJACKS`, `UNSUPPORTED_EXPANSIONS`, `EXPLICIT_TERM_LOSSES`,
  `QUERY_SIGNAL_AS_ROW_EVIDENCE`, `REVIEW_REQUIRED_USED` = 0), lo que confirma que
  el diseño era seguro pero **inerte**.

Integridad del cold: `PASS` — 36 casos, hash canónico
`aa0f4c32bec5b054792d0337a6d0ded4a538ac9cd4595140e43fec98d3bbc039`, distribución
de familias validada, campos prohibidos ausentes.

**Lectura honesta:** el experimento de Search es *inconcluyente por transporte*.
No prueba que la expansión semántica sea peor que la clásica. Sí muestra que
**no hay evidencia de que la implementación actual produzca expansión capaz de
satisfacer el gate de valor único**: en cada oportunidad que tuvo, se abstuvo.

### 3.2 Radar — `ADULTO_MAYOR_RADAR_SEMANTIC_NO_GO`

Muestra ciega de 240 filas, labels congelados en `405f358` antes de abrir el MAP,
join 240/240, gold principal de 228 filas (`yes/no`), 12 `uncertain` fuera de los
denominadores.

| Métrica | B2 | D |
|---|---:|---:|
| Notificaciones | 67 | 42 |
| Precision | 0,913793 | 1,000000 |
| Recall | 0,913793 | 0,724138 |
| F1 | 0,913793 | 0,840000 |
| Abstention rate | 0,000000 | 0,808333 |
| Hard-negative notificados | 1 | 0 |
| Falsas por 100 candidatos | 2,083333 | 0,000000 |

Diferencial:

```text
D_FIXES_B2_FALSE_POSITIVES: 5      (atribución: ABSTENTION_POLICY)
D_FIXES_B2_HARD_NEGATIVES: 1       (atribución: ABSTENTION_POLICY)
D_RECOVERS_B2_MISSES: 2            (atribución: ROW_EVIDENCE)
D_LOSES_B2_POSITIVES: 13           (atribución: ABSTENTION_POLICY)
SEMANTIC_CAUSAL_WINS: 2
SEMANTIC_CAUSAL_LOSSES: 1
NET_TRUE_NOTIFICATIONS: -11
```

Gates: `R1_PRECISION PASS`, `R2_HARD_NEGATIVES PASS`, `R3_UNIQUE_VALUE PASS`,
`R4_RECALL_COST FAIL`, `R5_MAINTAINABILITY PASS`.

El hallazgo decisivo no es el F1 sino la **atribución**: los cinco falsos
positivos que D corrige y los trece positivos que pierde provienen del mismo
mecanismo — abstención por `NO_SUFFICIENT_FROZEN_SEMANTIC_EVIDENCE` — no de
razonamiento semántico. Solo dos aciertos y una pérdida son causalmente
semánticos. La complejidad del Registry no está pagando el resultado; lo que
está pagando es una política conservadora de "no avisar ante la duda", y esa
política es más barata de implementar sin Registry.

Para un Radar diario el intercambio es inaceptable: evitar ~2 falsas alertas por
cada 100 candidatos a cambio de dejar de avisar ~19 oportunidades reales por cada
100 oportunidades reales.

## 4. Decisión

```text
SEMANTIC_MARKET_PACK_RUNTIME: NO_GO
```

No se continúa el spike. **La prueba de transferencia a Infraestructura Vial
queda cancelada** porque el primer mercado no superó el gate de utilidad
operativa y el protocolo solo autorizaba transferir ante `GO` o
`KEEP_WITH_LIMITS`.

## 5. Qué NO significa este NO-GO

No se concluye:

- que la semántica de dominio carezca de valor;
- que el Semantic Registry deba eliminarse;
- que embeddings o rerankers no puedan funcionar;
- que el Radar no pueda manejar mercados;
- que la expansión semántica en Search sea peor que la clásica (ese experimento
  quedó inconcluyente por transporte, no refutado).

La conclusión es limitada y precisa: **la hipótesis e implementación actuales de
Market Pack + Semantic Registry no justifican su complejidad como autoridad
runtime para retrieval ni para clasificación de mercado.**

## 6. Qué se conserva

**Semantic Registry: KEEP, con rol reclasificado a `ADVISORY / GOVERNANCE / AUTHORING`.**

Usos permitidos:

- representar relaciones semánticas explícitas y su provenance;
- documentación y auditoría de vocabulario;
- sugerencia de vocabulario al curar una definición de mercado;
- soporte al authoring de market definitions.

Uso prohibido sin evidencia independiente nueva: convertirlo en autoridad de
runtime para retrieval o clasificación.

Se conserva además la separación de autoridades que el spike validó como
correcta y que ya estaba documentada en
`docs/audits/2026-08-22_SEMANTIC_AUTHORITY_AUDIT.md`:

```text
MORPHOLOGY
DOMAIN SEMANTICS
QUERY SEMANTICS
```

Toda la evidencia experimental se conserva: cold, labels, manifests, métricas,
auditorías y hashes. La evidencia de un NO-GO tiene valor y se archiva bajo los
namespaces experimentales existentes (`artifacts/experiments/…`,
`scripts/experiments/…`, `app/experiments/…`).

## 7. Baseline recomendado para Radar

```text
MARKET DEFINITION
  positive_terms
  phrases
  negative_terms / exclusions
  filtros estructurados: región · entidad · valor · modalidad · fechas
→ BROAD RETRIEVAL
→ CANDIDATE DEDUP / DIFF
→ NOTIFICATION
```

B2 demuestra que una definición curada lexical + filtros estructurados es
competitiva (F1 0,914 con cero abstenciones). **No se copia el B2 de Adulto Mayor
a producción**: se adopta el *patrón arquitectónico*, no el artefacto.

## 8. Search

Search se mantiene como está. No se sustituye Classic Search por Market Packs.
Prioridades futuras independientes (SODA3/transporte, recall lexical, reranking,
embeddings, clasificadores post-retrieval) **no forman parte de este cierre**.

## 9. Dirección semántica futura (hipótesis, no roadmap)

El experimento mostró que una política conservadora de abstención sacrifica
cobertura. Por eso la hipótesis registrada es:

```text
BROAD RETRIEVAL → RERANK / CLASSIFY
```

antes que

```text
SEMANTIC PRE-RETRIEVAL GATING
```

No se implementa nada ahora.

## 10. Matriz de integración de commits

Ancestry verificada con `git branch --contains` y `git merge-base --is-ancestor <c> main`.
**Ninguno de los commits del spike es ancestro de `main`.** No se hizo merge.

| commit | purpose | current_branch | already_main | recommended_action | reason |
|---|---|---|---|---|---|
| `25986d9` | Semantic Registry v0 dormido: `app/core/semantic_registry.py`, `app/data/semantic_registry_v0.json`, doc de arquitectura, audit de autoridad, tests | `feature/semantic-registry-v0` + 5 ramas | NO | **B parcial / C parcial** — extraer a main solo `docs/architecture/SEMANTIC_REGISTRY_V0.md` y `docs/audits/2026-08-22_SEMANTIC_AUTHORITY_AUDIT.md`; el código queda en rama | El audit documenta un `RUNTIME_BUG_CONFIRMED` de `morphological_variants` que es útil en main; el módulo sin consumidor sería código muerto en runtime |
| `ceebe73` | Endurecimiento de contratos del Registry v0 | `feature/semantic-registry-v0` + 5 | NO | **C** (docs ya cubiertos por la extracción anterior) | Refina el mismo módulo dormido |
| `910cf9d` | Spike MP1: `app/experiments/market_packs/`, harness, artefactos MP1, audit del spike | `exp/market-pack-adulto-mayor-v1` + 4 | NO | **B parcial / C** — preservar el audit; harness fuera de producción | El harness experimental no debe entrar a main |
| `756ca2c` | Reparación de metodología: `methodology.py`, `production_lexical.py`, sondas de transporte y controles negativos | `exp/market-pack-adulto-mayor-methodology-v2` + 3 | NO | **B parcial / C** — preservar `2026-08-22_MARKET_PACK_METHODOLOGY_REPAIR.md` | El repair metodológico es la razón por la que el NO-GO es creíble; el código es harness |
| `8901346` | Congelamiento de labels diagnósticos ciegos MP1 | `exp/market-pack-adulto-mayor-b2-v2` + 3 | NO | **B — DOCUMENTATION_EVIDENCE_ONLY** | Evidencia de cadena de custodia; no ejecutable |
| `dca137a` | Análisis de labels diagnósticos MP1 | `exp/market-pack-adulto-mayor-b2-v2` + 3 | NO | **B — DOCUMENTATION_EVIDENCE_ONLY** | Ídem |
| `4dab647` | Concepto `centro de vida` con evidencia | `feature/semantic-registry-centro-vida-evidence` + 2 | NO | **C — EXPERIMENT_ONLY_DO_NOT_MERGE** | Es exactamente el tipo de "añadir vocabulario" que los criterios de reapertura excluyen |
| `910cf9d`…`63f9dcc` | B2 strong curated OR + protocolo de cold pre-registrado | `exp/market-pack-adulto-mayor-b2-v2`, `exp/semantic-radar-adulto-mayor-final` | NO | **B parcial / C** — preservar `2026-08-22_MP2_B2_AND_COLD_PROTOCOL.md` | El protocolo pre-registrado es el activo reutilizable; `b2_curated_or.py` es harness |
| `c599c51` | Cold v2 independiente (36 casos) + manifiesto | `exp/market-pack-adulto-mayor-b2-v2`, `exp/semantic-radar-adulto-mayor-final` | NO | **B — DOCUMENTATION_EVIDENCE_ONLY** | Cold reutilizable como benchmark futuro; conservar en rama |
| `51af6b3` | Muestra ciega del Radar (240) + harness `semantic_radar_adulto_mayor_final.py` | `exp/semantic-radar-adulto-mayor-final` | NO | **B parcial / C** — evidencia se conserva; harness fuera de producción | Mismo criterio |
| `405f358` | Labels adjudicados del Radar congelados | `exp/semantic-radar-adulto-mayor-final` | NO | **B — DOCUMENTATION_EVIDENCE_ONLY** | Freeze atómico previo a abrir el MAP |
| `118123f` | Métricas finales + `2026-08-23_ADULTO_MAYOR_SEMANTIC_RADAR_FINAL.md` | `exp/semantic-radar-adulto-mayor-final` | NO | **B — DOCUMENTATION_EVIDENCE_ONLY; extraer a main** | Es la autoridad del veredicto NO-GO |

### Evidencia congelada después del cierre (2026-08-23)

Lo que al momento del cierre seguía sin commitear quedó preservado en tres
commits separados sobre `exp/semantic-radar-adulto-mayor-final`, sin mezclar
evidencia con ejecutables:

| commit | contenido | clasificación |
|---|---|---|
| `4a24e38` | `artifacts/…/mp2_final/` — run `20260823T033843Z` completo (manifiesto, `retrieval_evidence.json`, paquete ciego de 675 filas, MAP sin abrir) más el run abortado `20260823T033718Z`. Los tres SHA-256 del manifiesto verifican MATCH; escaneo de secretos limpio | `EXPERIMENT_EVIDENCE_ONLY` |
| `ce4e430` | `scripts/experiments/market_pack_adulto_mayor_mp2_final.py` (runner que generó la evidencia) y el delta de `tests/test_market_pack_mp2_freeze.py` que valida el cold materializado contra el hash canónico. Sin ese delta la suite de la rama estaba en rojo: la versión previa exigía que `new_cold_v2.yaml` **no** existiera | `EXPERIMENT_ONLY_DO_NOT_MERGE` |
| `d734fb6` | `docs/planes/SPIKE_REGISTRO_SEMANTICO_MARKET_PACKS_V1.md` — plan pre-registrado: marco GO / KEEP WITH LIMITS / NO-GO, invariantes I1–I7, brazos A–E, gates 1–5 y fases S0–S6, incluida la Fase S4 de Infraestructura que el NO-GO cancela | `EXPERIMENT_EVIDENCE_ONLY` |

Ninguno de los tres es candidato a producción y ninguno se extrae a `main`.

### Extracción a `main` (ejecutada en `docs/semantic-market-pack-closure`)

Rama docs-only nacida exactamente de `main`, por restauración de rutas
(`git restore --source=<commit> -- <path>`), **sin cherry-pick** de ningún
commit experimental — un cherry-pick habría arrastrado Registry dormido,
harness, Market Pack y artefactos:

| path | source commit | razón |
|---|---|---|
| `docs/decisions_log.md` (solo el delta de ADR-062) | `6bedf99` | Decisión de cierre en el índice canónico de ADR. Aplicado como parche sobre la versión vigente de `main`, preservando los ADR previos |
| `docs/audits/2026-08-23_SEMANTIC_MARKET_PACK_SPIKE_CLOSURE.md` | `6bedf99` | Documento de síntesis: acompaña a los otros cuatro, no los duplica — es lo único que carga la matriz de integración de commits y la trazabilidad de la evidencia |
| `docs/audits/2026-08-23_ADULTO_MAYOR_SEMANTIC_RADAR_FINAL.md` | `118123f` | Autoridad del `ADULTO_MAYOR_RADAR_SEMANTIC_NO_GO`: métricas, atribución causal y cadena de custodia |
| `docs/audits/2026-08-22_MARKET_PACK_METHODOLOGY_REPAIR.md` | `756ca2c` | Explica por qué la evaluación original de MP1 era inválida; sin este documento el NO-GO no es auditable |
| `docs/audits/2026-08-22_SEMANTIC_AUTHORITY_AUDIT.md` | `ceebe73` | Documenta el `RUNTIME_BUG_CONFIRMED` de `MORPHOLOGICAL_VARIANTS`, deuda **real de producción** independiente del spike |
| `docs/architecture/SEMANTIC_REGISTRY_V0.md` | `4dab647` | Contrato del artefacto que ADR-062 conserva en rol advisory; el documento se autodeclara dormido (INV-SR-04 y INV-SR-05), así que no induce a creerlo activo en `main` |

Deliberadamente **fuera** de `main`: `app/core/semantic_registry.py`,
`app/data/semantic_registry_v0.json`, `app/experiments/market_packs/`,
`scripts/experiments/`, `artifacts/experiments/`, los tests experimentales y el
plan del spike.

Las ramas `feature/semantic-registry-v0`, `exp/market-pack-adulto-mayor-*` y
`exp/semantic-radar-adulto-mayor-final` se conservan como archivo de evidencia.
No se borran.

### Deuda documental derivada

`MORPHOLOGICAL_VARIANTS` mezcla morfología, equivalencia, alias, jerarquía y
asociación en una sola autoridad; el router pierde la identidad explícita al
convertir `nutricion` en el root `alimentacion`. Es deuda de producción, no del
spike, y **no se corrige aquí**. Queda registrada como
`SEMANTIC-AUTHORITY-CLEANUP` en §12.1 de
[`documento_tecnico_secoppal_arquitectura_bitacora.md`](../documento_tecnico_secoppal_arquitectura_bitacora.md).

## 11. Validación

Cierre documental. No se modificó runtime ni ningún archivo ejecutable, por lo
que no corresponde correr la suite. Verificación aplicada: `git diff --check`
limpio sobre los archivos de este cierre.

---

Un experimento que da NO-GO y evita construir complejidad innecesaria es un
experimento exitoso.

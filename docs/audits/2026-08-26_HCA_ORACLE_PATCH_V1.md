# HCA ORACLE PATCH V1 — alinear seis oráculos con la semántica canónica

Fecha: 2026-08-26. **HCA_ORACLE_PATCH_V1: PASS**. Rama `audit/hca-oracle-patch-v1`,
base `8833ba286d5679926493efed4287cdbb425ff276`. **RUNTIME_FILES_CHANGED: 0**;
**RUNNER_CHANGED: NO**; `FIXTURE_ONLY_PATCH`. No merge, no push, no ADR.

Este bloque **no corrige un defecto de producto**. Cambia el instrumento de medición:
seis expectativas del corpus HCA que una adjudicación ciega previa ya demostró
incorrectas dejan de exigir una representación que las autoridades canónicas
descartaron. El examen pasa a medir la arquitectura vigente, no una versión
anterior de sí misma.

## 1. Por qué se descongela el corpus

`tests/fixtures/audits/conversation_holistic_v1.yaml` nació congelado y con una regla
explícita en su cabecera: *el corpus no se edita para hacer pasar casos*. Esa regla
sigue vigente y este patch no la rompe, porque ninguna expectativa se relaja por
observar el runtime. Cada cambio se apoya en una adjudicación anterior e independiente:

- [Oracle Review](2026-08-26_HCA_ORACLE_REVIEW.md) y su
  [plan de patch](evidence/2026-08-26_HCA_ORACLE_REVIEW/oracle_patch_plan.json)
  adjudicaron los 18 FAIL de entonces en fase ciega (intención sellada **antes** de
  ver runtime) y clasificaron cinco como `CONFIRMED_ORACLE_DEBT`.
- [Final P1 Runtime Fixes](2026-08-26_HCA_FINAL_P1_RUNTIME_FIXES.md) y su revisión de
  integración cerraron los seis bugs reales de runtime y, sólo entonces, habilitaron
  **HCA-FOLLOWUP-016** (`PRIMARY_RUNTIME_BUG: CLOSED`, `SECONDARY_ORACLE_DEBT: CONFIRMED`).

La cabecera del fixture lleva ahora una enmienda que deja constancia de esto: una
expectativa sólo puede cambiar sobre autoridad adjudicada previa, nunca sobre conducta
observada. El Oracle Review original **no se borra**: conserva la evidencia de por qué
el patch es legítimo.

## 2. Alcance: seis casos, ni uno más

Autorizados y modificados: `HCA-ACTOR-004`, `HCA-TOPIC-007`, `HCA-ENTITY-001`,
`HCA-FOLLOWUP-007`, `HCA-META-001B`, `HCA-FOLLOWUP-016`.

Explícitamente **no** tocados, con su clasificación intacta:

| CASE_ID | Clasificación | Por qué se queda como está |
| --- | --- | --- |
| HCA-TOPIC-012 | `SAFE_UNSUPPORTED_CAPABILITY` | Su cambio de oracle era MAYBE, no confirmado. |
| HCA-MODALITY-002 / -012 / HCA-TYPO-003 | `KNOWN_DEFERRED_PRODUCT_DEBT` | El FAIL es correcto mientras la capacidad siga diferida. |
| HCA-ENTITY-004 / HCA-STATE-003 / HCA-SOURCE-002 | `INSUFFICIENT_EVIDENCE` | No hay autoridad para declarar qué oracle sería el correcto. |

Los oráculos de negación `NEG-N15` y `NEG-ANTI-002` viven en otro fixture y otra
campaña focal: **no se mezclan aquí**. `make audit-negation-v1` conserva su basal.

El gate estructural compara caso por caso por hash del fragmento parseado, no por
líneas: [corpus_gate.py](evidence/2026-08-26_HCA_ORACLE_PATCH_V1/corpus_gate.py) →
[corpus_gate.json](evidence/2026-08-26_HCA_ORACLE_PATCH_V1/corpus_gate.json).
`CHANGED_CASE_IDS` = los seis autorizados; `UNEXPECTED_CHANGED_CASES: 0`; conjunto y
orden de los 195 IDs intactos. Fuera de los casos sólo cambia el comentario de cabecera.

## 3. Los seis parches

### 3.1 HCA-ACTOR-004 — `FUZZY_CANDIDATE_DOES_NOT_AUTHORIZE_IDENTITY`

`contratos de lmp ingieneria en 2025`

- **OLD** — `has: [actor_scope]`, `objeto_not: [lmp, ingieneria]`.
- **WHY_WRONG** — exigía un scope autoritativo antes de que exista selección. FTS/fuzzy
  produce **CANDIDATES**, y candidatos ≠ identidad; la propia nota del caso hablaba de
  candidatos mientras el oracle pedía identidad resuelta.
- **NEW** — `expect: block`; `has: [actor_pending]`; `absent: [actor_scope, contratista]`;
  `eq: {actor_resolution_status: CANDIDATES}`;
  `candidate_present: {NIT 900739049, max_rank 3}`; `objeto_not` preservado.
- **AUTHORITY** — ADR-060 §4/§5/§9–10; `ACTOR_PROFILE_V1` §7–10; INV-NAME-NO-GUESS;
  contrato R7.

La aserción **cero source calls** no es expresable: el runner registra SoQL, no llamadas
al stub. No se inventa una aserción débil que la simule — SoQL generado ≠ llamada, como
documentó el propio Oracle Review. La observación `source_calls=0` queda donde ya está,
en la evidencia de aquella revisión.

### 3.2 HCA-TOPIC-007 — `CANONICAL_TOPIC_ALIAS`

`procesos del pae en la guajira`

- **OLD** — `objeto_has: [pae]`.
- **WHY_WRONG** — la autoridad vigente representa PAE como el concepto canónico
  `alimentacion_escolar` con aliases curados en retrieval. Exigir la forma superficial
  del acrónimo mide una representación interna, no la conservación del tema; el catálogo
  evita el literal `%pae%` justamente por colisiones como Páez.
- **NEW** — `objeto_has: [alimentacion_escolar]`;
  `soql_has: ["departamento_entidad = 'La Guajira'", "%alimentacion escolar%"]`;
  `soql_not: ["%pae%"]`.
- **AUTHORITY** — ADR-006; ADR-024; ADR-036; ADR-061.

El oracle no se debilita a «simplemente ejecutó»: sigue probando el tema, en el concepto
canónico y en el predicado enviado a la fuente.

### 3.3 HCA-ENTITY-001 — valor real de fuente, no limpieza estética

`contratos de la gobernacion del atlantico en 2026`

- **OLD** — `soql_not: ["ATLANTICO**"]`.
- **WHY_WRONG** — ADR-040 documenta que `DEPARTAMENTO DEL ATLANTICO**` es el valor real
  registrado en SECOP. Los asteriscos son dato de fuente, no Markdown espurio; el oracle
  confundió una cosa con la otra y pedía ensuciar el LIKE por estética.
- **NEW** — `ds: contratos`; `eq: {entidad_resolved: "DEPARTAMENTO DEL ATLANTICO**"}`;
  `absent: [departamento_resolved]`; `soql_has` con el LIKE de comprador real y la
  ventana 2026 completa.
- **AUTHORITY** — ADR-040.

`absent: [departamento_resolved]` es la parte nueva que importa: prohíbe que el comprador
se convierta accidentalmente en territorio.

### 3.4 HCA-FOLLOWUP-007 — `STRUCTURED_DIMENSION_COUNTS_AS_PRESERVED_SEMANTICS`

T1 obra + Atlántico + modalidad; T2 `mejor quita el filtro de modalidad`.

- **OLD** (T2) — `absent: [modalidad_include_procedure_ids]`, `objeto_has: [obra]`.
- **WHY_WRONG** — SEARCH-SH1/ADR-061 decidieron que *obra* es dimensión estructurada de
  tipo contractual y **no** se duplica como tema. El oracle exigía la representación vieja.
- **NEW** (T2) — `expect: execute`; `ds: contratos`;
  `absent: [modalidad_include_procedure_ids, modalidad_exclude_procedure_ids]`;
  `eq: {contract_type_include: [obra]}`;
  `soql_has: ["departamento = 'Atlántico'", "tipo_de_contrato = 'Obra'"]`;
  `soql_not: ["modalidad_de_contratacion IN", "modalidad_de_contratacion ="]`.
- **AUTHORITY** — ADR-054 y guía de modalidad §2–3; ADR-061 INV-STRUCTURED-DIMENSION-NOT-TOPIC.

T1 no se toca. `obra` **no** vuelve a objeto.

### 3.5 HCA-META-001B — de metamórfico falso a anti-metamórfico

`contratos de adulto mayor del departamento del atlantico para 2026`

- **OLD** — `same_frame_as: HCA-META-001A`, turno con sólo `expect: execute`.
- **WHY_WRONG** — ADR-040 INV-7 distingue **comprador** de **territorio**: «gobernación del
  Atlántico» y «departamento del Atlántico» consultan universos distintos a propósito. El
  oracle metamórfico protegía una equivalencia semánticamente falsa y convertía una
  diferencia legítima en mismatch.
- **NEW** — se retira `same_frame_as` y se declara
  `differ_from: {case: HCA-META-001A, fields: [entidad_resolved, departamento_resolved]}`,
  más `eq: {departamento_resolved: "Atlántico"}`, `absent: [entidad_resolved]`,
  `objeto_has: [adulto_mayor]`, `soql_has` con territorio + tema + ventana 2026 y
  `soql_not: ["unaccent(UPPER(nombre_entidad)) LIKE"]`.
- **AUTHORITY** — ADR-040 INV-7; ADR-061.

`differ_from` ya existía en el corpus (HCA-ANTI-00x) y en los focales: no es DSL nueva.
El par queda comparado dimensionalmente — mismo tema y tiempo, universo distinto — sin
exigir igualdad del frame completo y sin añadir equivalencia falsa. **HCA-META-001A no se
modifica**: está fuera del alcance autorizado.

### 3.6 HCA-FOLLOWUP-016 — reordenar conserva el universo

T1 obra + Cali + >300M; T2 `los mas antiguos primero`.

- **OLD** (T2) — `eq: {valor_min: 300000000}`, `objeto_has: [obra]`.
- **WHY_WRONG** — el defecto **de runtime** ya se cerró en el Final P1 Sprint:
  `FILTERS(WHERE1) == FILTERS(WHERE2)` salvo ordering/paginación, `WHERE_DRIFT = 0`.
  El único fallo restante era `objeto_has: [obra]`, con obra estructuralmente presente en
  `contract_type_include` y en `tipo_de_contrato`.
- **NEW** (T2) — `expect: execute`; `ds: contratos`;
  `eq: {valor_min: 300000000, ciudad: Cali, contract_type_include: [obra], ordering_signal: fecha_asc}`;
  `absent: [entidad_ecosystem, entidad_resolved, entidad_like]`;
  `soql_has` con Cali, `valor_del_contrato >= 300000000`, `tipo_de_contrato = 'Obra'` y
  `ORDER BY fecha_de_firma ASC`; `soql_not: ["unaccent(UPPER(nombre_entidad)) LIKE"]`.
- **AUTHORITY** — addendum al Oracle Patch Plan en la
  [revisión de integración Final P1](2026-08-26_HCA_FINAL_P1_RUNTIME_INTEGRATION_REVIEW.md) §6;
  ADR-066 (reordenar conserva WHERE); ADR-061.

**Límite declarado:** el DSL del runner no puede comparar la igualdad de WHERE **entre
turnos**. No se inventó una aserción débil ni se extendió el runner para simularla. Lo que
sí es observable en T2 se afirma completo: dinero, ciudad, tipo contractual, orden
ascendente y ausencia de compradores derivados. La prueba de igualdad de WHERE sigue
siendo evidencia independiente en la revisión de integración, no una aserción del corpus.

## 4. No-vacuidad: cada oracle nuevo detecta el defecto que dice impedir

[mutation_probe.py](evidence/2026-08-26_HCA_ORACLE_PATCH_V1/mutation_probe.py) reusa
`check_turn`/`material_frame` **del runner sin editarlo**, toma el registro real de la
corrida post-patch y le inyecta el defecto conceptual. Un oracle es NONVACUOUS si el
registro real pasa y el mutado falla.

| CASE_ID | Mutación inyectada | Resultado |
| --- | --- | --- |
| HCA-ACTOR-004 | el fuzzy auto-resuelve identidad y ejecuta | FAIL (5 checks) |
| HCA-TOPIC-007 | se pierde alimentación escolar; queda el territorio | FAIL (2 checks) |
| HCA-ENTITY-001 | el comprador se reinterpreta como territorio | FAIL (3 checks) |
| HCA-FOLLOWUP-007 | al quitar modalidad se pierde también Obra | FAIL (2 checks) |
| HCA-FOLLOWUP-016 | reaparecen compradores derivados al ordenar | FAIL (2 checks) |
| HCA-META-001B | ambos frames colapsan a la misma dimensión | FAIL (2 checks) |

**`PATCH_NONVACUOUS = 6/6`** —
[mutation_probe.json](evidence/2026-08-26_HCA_ORACLE_PATCH_V1/mutation_probe.json).

Ningún oracle nuevo se apoya sólo en `needs_clarification=true`, `dataset=procesos` o
`executed=true`: cada uno comprueba además la dimensión que estaba en disputa.

## 5. Resultado y composición

| | PASS | FAIL | ERROR |
| --- | --- | --- | --- |
| Antes | 182 | 13 | 0 |
| Después | **188** | **7** | 0 |

Se compararon los **195 IDs**, no el delta neto: **`OLD_PASS_TO_NEW_FAIL = 0`**;
`FAIL→PASS` = exactamente los seis parcheados
([hca_status_diff.json](evidence/2026-08-26_HCA_ORACLE_PATCH_V1/hca_status_diff.json)).

Los 7 FAIL restantes, sin reclasificar por conveniencia:

```text
RAW_HCA:                 188 / 195
SAFE_UNSUPPORTED:        1   (HCA-TOPIC-012)
KNOWN_DEFERRED:          3   (HCA-MODALITY-002, HCA-MODALITY-012, HCA-TYPO-003)
INSUFFICIENT_EVIDENCE:   3   (HCA-ENTITY-004, HCA-STATE-003, HCA-SOURCE-002)
REAL_RUNTIME_BUG:        0
CONFIRMED_ORACLE_DEBT:   0   (entre los casos adjudicados)
```

**188/195 no es «100% correcto».** Tres casos siguen sin evidencia suficiente para saber
cuál sería su oracle correcto, tres esperan capacidades diferidas y uno documenta una
capacidad no soportada. Lo que cambia es que ya no queda deuda de oráculo confirmada
contaminando la cifra.

## 6. Validación

| Puerta | Resultado |
| --- | --- |
| HCA holístico | 188 PASS / 7 FAIL / 0 ERROR; PASS→FAIL 0 sobre 195 IDs |
| `make audit-hca-final-p1` | 40/40 |
| `make audit-actor-hygiene-v1` | 36/36 |
| `make audit-control-language-v1` | 42/42 |
| `make audit-time-v2` | 32/32 |
| `make audit-negation-v1` | 22/24 — mismos `NEG-N15` y `NEG-ANTI-002` basales |
| `ruff check app/ tests/ scripts/` | All checks passed |
| `make lint-core` / `git diff --check` | PASS |
| `git diff <base>..HEAD -- app/` | vacío |
| Aislamiento del runner | `outside_sandbox: []`, 0 escrituras de producción |

Los cinco corpus focales conservan su SHA y su cifra
([focal_corpora.json](evidence/2026-08-26_HCA_ORACLE_PATCH_V1/focal_corpora.json)).

**No se ejecutó la suite completa de pytest.** El patch es fixture + docs, el runner no
cambió y ningún test productivo se tocó: HCA completo más los cinco focales cubren lo que
este cambio puede afectar. Se declara explícitamente en vez de insinuar una validación
más amplia de la que hubo.

## 7. Hashes

```text
HCA_SHA_BEFORE: 262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba
HCA_SHA_AFTER:  a341405a79d74cdb43872cf937cd2b58f4ebb736afe4a2135ced9b6d389c352e
```

El SHA **debía** cambiar: el fixture se modificó deliberadamente. Fingir que el corpus
sigue congelado en el SHA viejo sería falso. A partir de aquí,
**`ORACLE_PATCH_V1_SHA = a341405a…`** es el baseline canónico del Final HCA.

Hashes de fragmento por caso (antes → después) en
[corpus_gate.json](evidence/2026-08-26_HCA_ORACLE_PATCH_V1/corpus_gate.json).

## 8. Evidencia reproducible

En `docs/audits/evidence/2026-08-26_HCA_ORACLE_PATCH_V1/`:
`corpus_gate.py` / `corpus_gate.json` (gate estructural),
`mutation_probe.py` / `mutation_probe.json` (no-vacuidad),
`hca_verdicts_before.json` / `hca_verdicts_after.json` (195 casos cada uno),
`hca_status_diff.json` (comparación por ID y composición de FAIL),
`focal_corpora.json` (los cinco focales), `isolation_after.json` (stores efectivos).

```bash
.venv/bin/python docs/audits/evidence/2026-08-26_HCA_ORACLE_PATCH_V1/corpus_gate.py --base <base>
.venv/bin/python scripts/audits/run_conversation_holistic_audit.py --out <dir>
.venv/bin/python docs/audits/evidence/2026-08-26_HCA_ORACLE_PATCH_V1/mutation_probe.py --results <dir>/results.jsonl
```

## 9. Cierre

```text
HCA_ORACLE_PATCH_V1: PASS
RUNTIME_FILES_CHANGED: 0
RUNNER_CHANGED: NO
CHANGED_CASE_IDS: 6 / 6 autorizados; UNEXPECTED: 0
PATCH_NONVACUOUS: 6 / 6
RAW_HCA: 182 / 195 → 188 / 195
PASS_TO_FAIL: 0
NEXT: READY_FOR_HCA_ORACLE_PATCH_INTEGRATION_REVIEW
```

No se crea ADR: este patch no cambia la arquitectura del producto, cambia el instrumento
de evaluación.

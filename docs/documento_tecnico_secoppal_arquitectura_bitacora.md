# SECOPPAL — Documento Técnico de Arquitectura, Stack y Bitácora de Cambios

**Fecha de creación:** 2026-05-15  
**Estado:** Borrador técnico V0.1  
**Sistema:** SECOPPAL — asistente conversacional para consulta y análisis de contratación pública SECOP II  
**Propósito del documento:** dejar una descripción comprensible para un tercero técnico o funcional sobre qué hace el sistema, cómo está construido, qué archivos lo componen, qué decisiones de arquitectura se han tomado, qué bugs relevantes se han detectado y cuál es el estado actual del proyecto.

## Estado canónico actual — 2026-08-28

### SODA3 `/query` — transporte productivo integrado

- **`SODA3_TRANSPORT_V1 = INTEGRATED`** (ADR-070). Integrado a `main` por
  fast-forward en `dd51ccd`. La salida a la red de SECOPPAL dejó de ser
  `GET /resource/{dataset}.json` vía `sodapy` y pasó a
  `POST /api/v3/views/{dataset}/query.json`.
- **El buscador no cambió.** Esta decisión sustituyó *por dónde sale* la consulta,
  no *qué* consulta se hace. El SoQL sigue siendo autoridad exclusiva de SECOPPAL:
  el transporte lo emite literal, sin construirlo, reescribirlo ni normalizarlo, y
  la normalización de filas sigue en `SecopClient._normalize_row`. La semántica del
  buscador, el ranking y la resolución de valores son idénticos.
- **`classic` sigue disponible como rollback**, no como red automática:
  `SECOPPAL_SOCRATA_TRANSPORT=classic` reactiva el camino `sodapy` legacy, y
  `sodapy` permanece instalado para que ese rollback sea real y no teórico. La
  selección falla cerrado: cualquier otro valor aborta la construcción del
  workflow en vez de elegir uno en silencio. Cambiar de transporte exige
  reconstruir o reiniciar el workflow.
- **No existe fallback automático SODA3 → classic.** Son dos transportes
  seleccionables, no una cascada. Un fallo HTTP, de JSON o de forma levanta
  excepción tipada y **nunca** se degrada a `[]`.
- **H4 conserva `no_preserve`.** El fallback H4 —la ruta donde el runtime retira el
  `ORDER BY` a propósito para abaratar la consulta— es la única excepción a
  `orderingSpecifier=total`. La señal se declara desde el control flow, no leyendo
  el texto del SoQL: la ausencia de `ORDER BY` también es legítima en `count`,
  `aggregate` y direct lookup, que no son H4. El `count` del propio fallback
  conserva `total`.
- **Probado sobre el producto entero, no sobre el transporte aislado.** El full
  runtime dogfood ejercitó 19 turnos humanos y 41 llamadas remotas contra ambos
  datasets, con búsqueda, follow-ups, paginación, direct lookup, Actor Profile,
  analytics/observer, H4 y **Radar** en `PASS`; `HTTP_CONTRACT_ERRORS = 0`,
  `SODA3_ONLY_REPRODUCIBLE_FAILURES = 0`, regresiones semánticas 0.
- **Suite completa verde** en el cutover: `9461 passed, 2 skipped, 24 xfailed,
  0 failed`, con default live smoke `PASS` y rollback classic explícito `PASS`.
- **`SODA3_QUERY_MIGRATION = CLOSED`.** El discovery de `/query` está cerrado y no
  se reabre; lo que siga son capacidades nuevas, no esta migración.

Decisión: `docs/decisions/2026-08-28_ADR-070_soda3-default-transport.md`.
Informes: `docs/audits/2026-08-28_SODA3_PRODUCTIVE_LIVE_SMOKE.md`,
`docs/audits/2026-08-28_SODA3_FULL_RUNTIME_DOGFOOD.md`.

### Capacidades SODA3 pendientes de evaluación

SODA3 expone capacidades que el discovery de `/query` **no** probó y que ADR-070
**no** autoriza. Se registran para no perderlas de vista:

```text
EXP-01  orderingSpecifier=discard      prioridad muy alta
EXP-02  /export para Warehouse         prioridad muy alta
EXP-03  SEARCH candidate retrieval     exploratorio
EXP-04  :updated_at incremental        Warehouse
EXP-05  page API                       diferido
```

**Pendiente de evaluación ≠ autorizado para runtime.** Ninguna de estas
capacidades tiene evidencia de aceptación, y cada una necesita su propio ciclo de
discovery y su propia decisión antes de tocar producción. La deuda
`deterministic TOP-N` también sigue abierta y es posterior a esta migración.

## Estado canónico — 2026-08-26 (histórico)

### Adjudicación causal de los hallazgos fresh — siete síntomas, un mecanismo

- **Rama:** `audit/fresh-findings-causality` sobre `4d9e5c7`. **Audit-only**:
  `RUNTIME_FILES_CHANGED: 0`, `FIXTURES_CHANGED: 0`, sin merge, sin push.
  El corpus fresh queda congelado: es el primer examen fuera de muestra de la
  campaña y su valor depende de no editarlo.
- **`FRESH_FINDINGS_CAUSALITY: SYSTEMATIC_CONFIRMED`.** Las **7 causas raíz**
  declaradas por el closeout son síntomas de **1 mecanismo**, repartido en
  **4 sitios reparables**:
  `ENUMERATED_SURFACE_FORM_VOCABULARY_WITH_FAIL_OPEN_DEFAULT`. Cada dimensión
  se reconoce con una lista de formas superficiales mantenida a mano; el token
  ausente no da error ni aclaración, cae a `objeto` o desaparece, y el sistema
  ejecuta una consulta **bien formada y semánticamente equivocada**.
- **Pares mínimos:** `compra` catalogado y `compró` no; `participar` sí y
  `licitar` no; `millones` cuenta como evidencia monetaria y `mil` no. Mismo
  lema, distinta inflexión, comportamiento opuesto.
- **Prueba predictiva: `12/12`.** Doce consultas derivadas del mecanismo, que
  **ningún corpus contiene**, fallaron como se predijo. Consecuencia estratégica:
  parchar nueve escenarios **no cierra la clase**; el lote fresh tomó una
  muestra del espacio de fallo, no lo agotó.
- **Dos hallazgos nuevos, ausentes del closeout:**
  **(1) `NEGATED_STATE_POLARITY_INVERSION`, propuesto P0** y más grave que el P0
  conocido — cuando el estado negado **sí** está en `ESTADO_SYNONYMS` la negación
  no se ignora, **se invierte**: `no estén abiertos` proyecta
  `estado_del_procedimiento IN (Publicado, Borrador, En aprobación, Aprobado,
  Abierto)` y ejecuta 2 source calls sin aclarar. Fail-open entrega de más;
  fail-inverted entrega el complemento exacto de lo pedido y parece plausible.
  **(2) `SPELL_CORRECTOR_FABRICATES_TOPIC`, propuesto P1** — `correct_query`
  convierte `otra`→`obra` y `otras`→`obras`, inyectando un tema fantasma;
  `STOPWORDS` sí protege `otra`, pero el corrector corre **antes** del scrub.
- **`STATE-002` es una clave duplicada, no un diseño:** `NATURAL_TO_FAMILY`
  declara `vigente(s)` dos veces en el mismo dict (`oferta_abierta` l.138,
  `contrato_activo` l.156); gana la última en silencio. **Ruff `F601` lo detecta
  con precisión y no está en el `select` de `ruff.toml`** — la puerta se diseñó
  para nacer en verde y esta regla quedó fuera con un bug real dentro.
- **El gate «>3 raíces → revisar arquitectura» no aplica como estaba escrito:**
  buscaba dispersión y lo que hay es **concentración**. Mejor noticia para la
  arquitectura, peor para el alcance del sprint.
- **Informe:** [adjudicación causal fresh](audits/2026-08-26_FRESH_FINDINGS_CAUSALITY.md).
- **Siguiente:** **`READY_FOR_HCA_FRESH_P1_SPRINT`**, P0 primero y con dos gates:
  **anti-vocabulario** (el fix no se acepta si es sólo añadir `publicado` a una
  lista: una autoridad única de estado y fail-closed por defecto) y
  **predictivo** (re-correr las doce consultas del §4, que no están en ningún corpus).

### Cierre de campaña HCA — bloqueado por bugs frescos — 2026-08-26


### HCA Final Audit — campaña abierta por bugs frescos

- **`HCA_FINAL_AUDIT: BLOCKED_FOR_NEW_RUNTIME_BUG` · `HCA_CAMPAIGN: OPEN`.**
  Auditoría final audit-only sobre `main=92476c5f2edba5be91a60c3592a5da6f86f3b1ab`,
  rama `audit/hca-final-closeout`. No modifica runtime, tests productivos,
  fixtures ni corpus; sin merge ni push.
- **HCA canónico reproducido: 188 PASS / 7 FAIL / 0 ERROR**, corpus
  `a341405a79d74cdb43872cf937cd2b58f4ebb736afe4a2135ced9b6d389c352e`,
  intacto. Comparación por los 195 IDs contra el baseline post Oracle Patch:
  cambios de estado **0**, PASS→FAIL **0**. `SEMANTICALLY_SUPPORTED: 189/195`
  sigue significando 188 runner PASS + TOPIC-012 safe fail-closed, no runner score.
- Los siete FAIL siguen siendo exactamente TOPIC-012 (1 SAFE),
  MODALITY-002/012 y TYPO-003 (3 DEFERRED), ENTITY-004, STATE-003 y SOURCE-002
  (3 INSUFFICIENT). `REAL_RUNTIME_BUG_IN_REMAINING_SEVEN=0` y
  `CONFIRMED_ORACLE_DEBT_IN_HCA=0`. No se decidió only-modality,
  buyer-sujeto/territorio ni liquidación.
- **Regresión canónica estable:** final P1 40/40, actor 36/36, control 42/42,
  tiempo 32/32, negación 22/24 con los dos fallos basales; query 89/89;
  dialogue 60/61 con `GOB-DEPTO-001` basal. Full suite: **8657 passed,
  2 skipped, 24 xfailed, 0 failed**. Runner y focales aislados,
  `outside_sandbox=[]`; `AUDIT_ATTRIBUTABLE_PRODUCTION_WRITES=0`.
- **Fresh challenge pre-registrado:** 52 escenarios / 58 turnos; runner crudo
  30 PASS / 22 FAIL, adjudicación semántica **41/52**. Encuentra **1 P0 + 8 P1**
  en nueve escenarios y siete causas raíz: negación de estado fail-open; pérdida
  de cota inferior en rangos monetarios pequeños; contradicción
  `oferta_abierta`→`Cerrado/Seleccionado`; verbos de compra desviados a procesos
  y tema; variantes naturales de paginación/reset; e intención `licitar` sin
  enforcement de oportunidad. Además hay 2 P2 y 12 P3 de expectativa.
- **Canarios vivos:** 6/6 respondieron, filas persistidas=0 y violaciones de los
  hard filters emitidos=0. El estado agregado es **FAIL**: el canario de
  oportunidades confirmó que el SoQL emitido contradice la familia declarada;
  es runtime, no Socrata. Ordering-only conserva WHERE en Meta e ICBF para las
  variantes cubiertas.
- **Principios finales:** intent survives normalization **PARTIAL**;
  dimensions do not collapse into topic **PARTIAL**; follow-ups preserve
  universe **PARTIAL**; unsupported fails closed **FAIL** por el P0.
  `QUERY_PLAN: AT_LIMIT` en 299/300 LoC efectivas y `NEXT_TOUCH_RULE: ACTIVE`.
- **Informe y evidencia:**
  [HCA Final Closeout](audits/2026-08-26_HCA_FINAL_CLOSEOUT.md). Próximo trabajo:
  sprint acotado de las siete causas frescas, empezando por el P0, y repetición
  del mismo plan sin cambiar sus oráculos.

### Integración Oracle Patch V1 y reparación de evidencia canónica

- **`FINAL_P1_RUNTIME: INTEGRATED` · `ORACLE_PATCH_V1: INTEGRATED`**.
  Review de integración **PASS**. Main previo
  `8833ba286d5679926493efed4287cdbb425ff276`; reparación docs/evidence-only
  `25d9eac05eb14e56ed2f8b9c9ddb6b670c0f1092`; patch fuente
  `9bd014c0d11c0acb353006eddd4421078c1bba0a` integrado por **cherry-pick** como
  `f074402c5147e98ede373d09341874df2c428aaf`, sin conflictos ni merge commit.
  La divergencia tras reparar docs es `CLEAN_DOC_DIVERGENCE`. Sin rebase ni push;
  rama fuente preservada. Este cierre posterior sólo añade documentación.
- **`CANONICAL_EVIDENCE_OMISSION` reparada:** informe Final P1 más 58 artefactos
  legítimos ahora tracked, todos byte-idénticos; 57/57 hashes del manifiesto
  original verificados. Los 191 untracked ajenos conservan su SHA y estado.
  El stage fue explícito. Los scripts `.py` archivados son evidencia previa,
  no cambios nuevos de runtime o runner. Se conservaron 34 avisos de espacios
  finales del `mutation.log` histórico; no se reescribió evidencia para limpiarlos.
- **RAW HCA: 188/195**, 7 FAIL, 0 ERROR. Baseline repetido 182/195; patch y main
  postintegración repiten 188/195. Comparados los 195 IDs: **PASS→FAIL=0**,
  **FAIL→PASS=exactamente los seis autorizados**, orden intacto, unexpected=0.
  No vacuidad reejecutada: **6/6**. El diff aplicado es byte-idéntico al fuente.
- **SEMANTICALLY_SUPPORTED: 189/195 = 188 PASS + 1 SAFE**, no score del runner.
  Los siete FAIL conservan **1 SAFE** (TOPIC-012), **3 DEFERRED**
  (MODALITY-002/012, TYPO-003) y **3 INSUFFICIENT**
  (ENTITY-004, STATE-003, SOURCE-002). REAL_RUNTIME_BUG=0 y
  CONFIRMED_ORACLE_DEBT=0 entre los fallos adjudicados restantes.
- **Baseline Final HCA:**
  `HCA_ORACLE_PATCH_V1_SHA=a341405a79d74cdb43872cf937cd2b58f4ebb736afe4a2135ced9b6d389c352e`.
  Reemplaza deliberadamente a
  `262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`;
  el SHA histórico y las adjudicaciones permanecen documentados.
- **Límites:** FOLLOWUP-016 tiene igualdad de WHERE demostrada mediante evidencia
  **EXTERNAL_AND_CANONICAL**, pues el DSL no compara turnos. Cero llamadas de
  ACTOR-004 sigue siendo evidencia externa complementaria; el fixture protege
  block, pending, CANDIDATES y ausencia de identidad. No se añadió DSL ni proxy.
- **Focales repetidos:** final P1 40/40, actor 36/36, control 42/42, tiempo 32/32,
  negación 22/24 con NEG-N15 y NEG-ANTI-002 basales. RUNTIME_CHANGED=0;
  RUNNER_CHANGED=0; Python inesperado=0; FULL_PYTEST=NOT_REQUIRED, no ejecutado.
- **Informe e inventario:**
  [Integration Review Oracle Patch V1](audits/2026-08-26_HCA_ORACLE_PATCH_V1_INTEGRATION_REVIEW.md).
  **Siguiente: `READY_FOR_HCA_FINAL_AUDIT`**. Final HCA aún no se declara cerrado.

Los dos bloques siguientes conservan los cierres históricos de preparación del
patch y de runtime; sus antiguos pasos siguientes quedan sustituidos por el
estado canónico de integración anterior.

### Oracle Patch V1 — el examen deja de medir una versión anterior de sí mismo

- **Rama:** `audit/hca-oracle-patch-v1` sobre
  `8833ba286d5679926493efed4287cdbb425ff276`. **Sin merge, sin push, sin ADR**:
  este bloque no cambia la arquitectura del producto, cambia el instrumento de
  evaluación. `RUNTIME_FILES_CHANGED: 0`; `RUNNER_CHANGED: NO`;
  `FIXTURE_ONLY_PATCH`.
- **Seis oráculos reescritos**, todos con adjudicación previa que los autoriza:
  `HCA-ACTOR-004` (candidato fuzzy no autoriza identidad), `HCA-TOPIC-007`
  (PAE es el concepto canónico `alimentacion_escolar`, no la forma superficial),
  `HCA-ENTITY-001` (`DEPARTAMENTO DEL ATLANTICO**` es valor real de fuente,
  no Markdown a limpiar), `HCA-FOLLOWUP-007` y `HCA-FOLLOWUP-016` (obra es
  dimensión estructurada de tipo contractual y no se duplica como tema) y
  `HCA-META-001B`, que pasa de metamórfico falso a **anti-metamórfico**
  (`differ_from`) porque comprador y territorio son universos distintos por diseño.
- **Nada más se tocó.** `HCA-TOPIC-012` sigue `SAFE_UNSUPPORTED_CAPABILITY`;
  MODALITY-002/012 y TYPO-003 siguen `KNOWN_DEFERRED_PRODUCT_DEBT`;
  ENTITY-004, STATE-003 y SOURCE-002 siguen `INSUFFICIENT_EVIDENCE`.
  `NEG-N15` y `NEG-ANTI-002` viven en otro fixture y no entraron aquí.
  Gate estructural por hash de fragmento: `CHANGED_CASE_IDS` = los seis,
  `UNEXPECTED_CHANGED_CASES: 0`, orden y conjunto de los 195 IDs intactos.
- **HCA: 182/195 → 188/195 PASS**, 7 FAIL, 0 ERROR, comparando los 195 IDs:
  **`PASS→FAIL = 0`**. Los 7 restantes son 1 SAFE + 3 DEFERRED + 3 INSUFFICIENT;
  `REAL_RUNTIME_BUG: 0` y `CONFIRMED_ORACLE_DEBT: 0`. **188/195 no es
  «100% correcto»**: es la cifra ya sin deuda de oráculo contaminándola.
- **El SHA del corpus cambia a propósito** — fingir que sigue congelado sería
  falso. `262c225c…` → **`a341405a79d74cdb43872cf937cd2b58f4ebb736afe4a2135ced9b6d389c352e`**,
  nuevo baseline canónico del Final HCA.
- **No-vacuidad probada: `PATCH_NONVACUOUS = 6/6`.** Cada oracle nuevo se
  enfrentó al defecto conceptual que dice impedir (fuzzy que auto-resuelve, tema
  perdido, comprador vuelto territorio, Obra perdida al quitar modalidad,
  compradores derivados reapareciendo al ordenar, ambos frames colapsando) y
  falló como debía. Ninguno se apoya sólo en «ejecutó» o «pidió aclaración».
- **Límites declarados, no disimulados:** el runner no observa llamadas a la
  fuente, así que «cero source calls» de ACTOR-004 **no** es aserción del corpus;
  y la igualdad de WHERE entre turnos de FOLLOWUP-016 sigue siendo evidencia
  externa. No se inventó una aserción débil ni se extendió el runner para
  simular ninguna de las dos.
- **Validación:** focales sin mover — final-p1 40/40, actor 36/36, control 42/42,
  tiempo 32/32, negación 22/24 basal. Ruff, lint-core y diff-check PASS.
  Aislamiento del runner `outside_sandbox: []`. **Suite completa NO ejecutada**,
  por ser patch de fixture + docs con runner intacto.
- **Informe:** [Oracle Patch V1](audits/2026-08-26_HCA_ORACLE_PATCH_V1.md).
  El [Oracle Review](audits/2026-08-26_HCA_ORACLE_REVIEW.md) original se conserva:
  guarda la evidencia de por qué el patch era legítimo.
- **Siguiente:** **`READY_FOR_HCA_ORACLE_PATCH_INTEGRATION_REVIEW`**.

### Cierre Final P1 Runtime — 2026-08-26

- **Main:** `dec589a21bf36844d595db80093e7824ea2a0189`, promovido mediante
  `git merge --ff-only feature/hca-final-p1-runtime-fixes` desde
  `eb8cfe1ed72ba5c9df0ab9c0ead5089ff5d09368`. Siete commits: cuatro runtime
  y tres de cierre documental (el primero incluye tests y Makefile).
  Sin merge commit, sin push; rama feature y untracked preexistentes preservados.
- **`HCA_FINAL_P1_RUNTIME: CLOSED / INTEGRATED`**.
  **`REAL_RUNTIME_BUGS: 6 / 6 CLOSED`**: ENTITY-005, MONEY-020/026,
  STATE-005/007 y FOLLOWUP-016. No se añadieron features ni se corrigieron
  defectos nuevos durante la revisión. Runtime, tests y fixtures del HEAD
  permanecieron intactos.
- **HCA congelado:** **177/195 → 182/195 PASS**, 13 FAIL, 0 ERROR;
  comparación de los 195 IDs, **PASS→FAIL = 0**. SHA256 intacto:
  `262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`.
- **Semánticamente sostenidos:** **183/195 → 189/195**.
  **189/195 != runner score**: 182 PASS + cinco deudas de oráculo confirmadas
  previas + un SAFE + la deuda secundaria de FOLLOWUP-016.
  Restan **3 KNOWN_DEFERRED_PRODUCT_DEBT + 3 INSUFFICIENT_EVIDENCE**;
  además, los raw failures de Oracle Debt permanecen hasta aplicar su patch.
  `FINAL_HCA` no está completo.
- **FOLLOWUP-016:** `PRIMARY_RUNTIME_BUG: CLOSED`;
  `SECONDARY_ORACLE_DEBT: CONFIRMED`. WHERE preservado entre turnos,
  dinero/Cali/Obra y orden correctos; el fallo restante exige `obra` como
  objeto en lugar de tipo contractual. Añadido al Oracle Patch Plan posterior
  del informe de integración; **no se modificó ningún oráculo**.
- **Verificación final:** suite completa **8657 passed, 2 skipped,
  24 xfailed, 0 failed**; focal P1 **40/40**, también post-merge;
  tests focales de integración **82 PASS**, también post-merge.
  Query **89/89**; dialogue **60/61**, mismo `GOB-DEPTO-001` basal.
  Negación **22/24**, mismos dos fallos basales; tiempo **32/32**;
  control **42/42**; actor **36/36**. Ruff cambiado, lint-core y diff-check PASS.
  **Semgrep: TOOL_UNAVAILABLE**, no ejecutado ni instalado; regla de
  persistencia inspeccionada manualmente. Runner HCA: **0 production writes**.
- **Tests y autoridades:** plan_query_node y modality golden **JUSTIFIED**;
  pagination **NONVACUOUS**. Modalidad tiene delta estructural **0**.
  Mutación actual: seis fallos de ordering y uno de paginación sin el fix.
  Baseline focal inicial versionado **25/40**; fixture final **26/40** en base
  y **40/40** en feature. QueryPlan intacto en **299/300 LoC**;
  `NEXT_TOUCH_RULE: PRESERVED`. Liquidación, identidad Actor, relaciones,
  Control Limit y capacidades diferidas mantienen sus contratos.
- **Informe y evidencia:**
  [revisión final P1](audits/2026-08-26_HCA_FINAL_P1_RUNTIME_INTEGRATION_REVIEW.md).
  Este cierre documental se registra en un commit docs-only sobre main,
  posterior al HEAD promovido; no altera runtime, tests ni fixtures.
- **Siguiente:** **`READY_FOR_HCA_ORACLE_PATCH`**.

## Estado histórico de integración HCA-R7 — 2026-08-26

El registro siguiente conserva el estado previo a Oracle Review y Final P1;
las cifras y clasificaciones actuales son las del cierre canónico anterior.

- **Main — integración funcional:** `23f307cf1c33cec029213d1707df91488f34f3bb`
  — **HCA-R7 Actor Hygiene V1**, integrado ff-only sobre
  `39234f873fb345b9de72ded7092afacadfdd646c`: un commit, sin commit de merge,
  **sin push**. `39234f8` es exclusivamente el cierre documental de R6 y su
  padre es `8bd5467aa7aa203d55605a758eedc0bf18ca33b5`; no hubo divergencia
  inadvertida. Este cierre documental posterior no cambia runtime ni oráculos.
  Conserva Control Language V1, Time Semantics V2 y los cierres previos.
- **Integration review R6:** `PASS`; rama `feature/hca-r6-control-language`
  conservada en `8bd5467`. Sin cambios de runtime, tests u oráculos
  durante esta revisión. Cierre documental en commit docs-only `39234f8`,
  autorizado como base canónica de HCA-R7; `8bd5467` identifica la integración
  funcional de R6.
- **Estado de cierre:** `HCA_CORE_PHASE_1: INTEGRATED` ·
  `DIRECT_LOOKUP_HIJACK: CLOSED` · `NORMALIZATION_CONTRACT: CLOSED` ·
  `DIMENSIONAL_NEGATION_V1: CLOSED` · `TIME_SEMANTICS_V2: CLOSED` ·
  `AUDIT_RUNNER_ISOLATION: CLOSED` · `CONTROL_LANGUAGE_V1: CLOSED` ·
  `ACTOR_HYGIENE_V1: CLOSED / INTEGRATED` ·
  `MAIN_INTEGRATION: 23f307cf1c33cec029213d1707df91488f34f3bb`.
- **Suite histórica (integration review R6, sobre `8bd5467`):** 8554 passed,
  2 skipped, 24 xfailed · query corpus 89/89 · dialogue corpus 60/61 con único
  FAIL `GOB-DEPTO-001` (remedido en `b661a09`, idéntico) ·
  `make audit-control-language-v1` 42/42 · `make audit-negation-v1` 22/24
  (mismos `NEG-N15` y `NEG-ANTI-002` basales) · `make audit-time-v2` 32/32 ·
  focal de orden/paginación/dinero/modalidad/follow-up: 409 passed, 1 xfailed ·
  `make lint-core` y `git diff --check` limpios. Ruff: tres F401 preexistentes
  en `formatter.py`, idénticos a `b661a09`; **0 hallazgos nuevos**.
- **HCA-1 congelado** (`conversation_holistic_v1.yaml`, sha `262c225c…`,
  intacto): **177 PASS / 18 FAIL / 0 ERROR de 195** en `main`.
  La revisión de integración R7 reprodujo **175/20/0** en el main previo
  `39234f8` y **177/18/0** en `23f307c`: solo `HCA-ACTOR-011` y
  `HCA-META-002B` corregidos, **0 rotos**.
  Base `b661a09` remedida desde una exportación aislada: **171/24/0**.
  R6 corrige `HCA-INTENT-007`, `HCA-INTENT-008`, `HCA-STATE-006` y
  `HCA-ORDER-004`; **0 rotos**, comparados caso por caso. Fase 1 llevó
  **156 → 164**; R5 llevó **164 → 171**, con la línea base remedida de forma
  independiente en un worktree limpio de `e0ba211` y el diferencial comparado
  caso por caso: 7 TIME corregidos, **0 rotos**. HCA **no** está completo.
  Los 8 casos de Fase 1 fueron
  (`HCA-INTENT-010`, `HCA-MODALITY-004`, `HCA-GEO-006`, `HCA-MONEY-021`,
  `HCA-NEG-003`, `HCA-NEG-005`, `HCA-NEG-006`, `HCA-ANTI-004`) y **0 casos
  rotos** frente a main.
- **Qué cambió, en una línea:** el sistema dejó de secuestrar consultas
  naturales como referencias, dejó de borrar la intención del usuario al
  normalizarla, y aprendió a distinguir *qué* se está negando antes de decidir
  si puede ejecutarlo.
- **Verificación R6:** una autoridad primaria de control en
  `app/core/control_language.py`; 20 secuencias de orden con **0 mutaciones
  de WHERE**, conteo o decisión de ecosistema. Re-pin de `pagination.py`:
  `JUSTIFIED`, AST idéntico salvo la delegación de etiqueta; oráculos
  estructurales de paridad intactos. Fuente viva, caso histórico: **0 → 50
  filas**, sin `LIKE '%ordenados%'`, orden correcto y **0 violaciones de
  filtros**. El desempate con fecha nula se contrastó con `NULLS FIRST/LAST`
  explícitos en Socrata; no se alteró el SoQL del runtime.
- **Integration review R7:** **PASS**; fast-forward verificado a `23f307c`.
  Rama `feature/hca-r7-actor-hygiene` conservada; **sin push**. Diff de
  integración: 19 archivos, **UNRELATED=0**, autoridad Actor preservada.
- **Validación de integración R7, remedida:** suite completa
  **8641 passed, 2 skipped, 24 xfailed**; focal ampliada **4419 passed,
  1 xfailed** (subconjunto, no sumar); índice ausente/corrupto/incompatible
  **7 passed**; fixture actor **36/36**; adversarial **242/242**;
  query **89/89**, dialogue **60/61** con el mismo `GOB-DEPTO-001`;
  control **42/42**, tiempo **32/32**, negación **22/24** con los mismos dos
  FAIL basales. Ruff cambiado, `lint-core` y diff-check limpios.
- **HCA R7 frente a main previo:** **175/20/0 → 177/18/0 de 195**, mismo arnés offline
  y corpus congelado; corregidos `HCA-ACTOR-011` y `HCA-META-002B`, **0 rotos**.
  `HCA-ACTOR-004`: **CONFIRMED_ORACLE_DEBT**, sigue FAIL; exige scope antes de selección, aunque su texto
  pide candidatos. El resultado conserva CANDIDATES y mejora LMP de rank 7 a 1;
  no se cambia el oráculo ni se concede autoridad a fuzzy para ponerlo verde.
- **Evidencia de integración:**
  [revisión R7](audits/2026-08-26_HCA_R7_INTEGRATION_REVIEW.md).
- **Siguiente:** `READY_FOR_HCA_ORACLE_REVIEW`; `FINAL_HCA` sigue abierto.

### Cierres acumulados

- **`HCA-LOOKUP-HIJACK-001`: CLOSED** (`0d194f3`). `_extract_entity_refs()`
  exigía prefijo + componente, pero no que algún componente fuera numérico: un
  prefijo corto (`SA`, `SI`, `LIC`) consumía el principio de una palabra normal
  y `contratos de salud en huila` viajaba a la ruta de lookup por referencia.
  Ahora la referencia exige estructura numérica, y un subprefijo alfabético solo
  vale separado (`SAMC-JCT-006`). El P0 estaba congelado como `xfail(strict)` en
  `test_dialogue_e2e_invariants.py`; ese xfail es hoy un test verde.
- **`NORMALIZATION_CONTRACT`: CLOSED** (ADR-063, `9ecf3be`).
  `INV-NORMALIZATION-PRESERVES-INTENT`. `más de 500 millones y menos de 100
  millones` ya no se reordena con `min()`/`max()`: P8 ve la contradicción real
  en lugar de un rango plausible — P8 nació ciego, no se rompió después.
  `desde 2025` conserva su extremo abierto porque el fallback genérico de año
  solo actúa si el extractor temporal no reconoció estructura alguna.
- **`DIMENSIONAL_NEGATION_V1`: CLOSED** (ADR-064, `7b7a79a`→`c66ebba`).
  La negación se bindea a su dimensión antes que cualquier extractor positivo
  reclame el valor gobernado. Ejecutan OBJECT, MODALITY y DEPARTMENT cuando la
  resta de conjunto es segura. CITY, TIME, BUYER, ACTOR, CONTRACT_TYPE y STATE
  **aclaran fail-closed** sin mutar el frame previo ni invalidar su buffer. El
  ciclo de vida es simétrico: lo que el usuario excluyó, lo puede retirar.
- **`TIME_SEMANTICS_V2`: CLOSED para R5** (ADR-065, rama
  `feature/hca-r5-time-v2`). `temporal_semantics` es la autoridad temporal única;
  parser y plan consumen su resolución con reloj civil inyectable. Día, mes,
  año, rangos explícitos, extremos abiertos, relativos y semanas preservan
  granularidad; fechas imposibles/rangos invertidos aclaran. Focal 32/32, HCA
  TIME 15/15 y fuente viva 80 filas sin violaciones (reproducida en el
  integration review).
- **`AUDIT_RUNNER_ISOLATION`: CLOSED** (`68f98e4`). Correr un runner de
  auditoría ya no escribe en los stores reales del proyecto sin depender de que
  el operador exporte variables antes de invocarlo:
  `scripts/audits/audit_isolation.py` redirige todo store escribible a un
  temporal efímero **antes** de importar `app`, pasa overrides explícitos a
  `Settings(...)`, deja los recursos de solo lectura (gazetteer, índice de
  actores) en su sitio, y emite `isolation.json` con las rutas efectivas de los
  objetos realmente construidos. La salida `--out` sobrevive; el sandbox de
  stores se borra siempre. Invariante:
  `INV-AUDIT-RUNNER-NO-PRODUCTION-WRITES`.

- **`CONTROL_LANGUAGE_V1`: CLOSED e integrado en main** (ADR-066,
  `8bd5467`, rama `feature/hca-r6-control-language` conservada).
  Una consulta mezcla dos lenguajes:
  CONTENIDO (lo que se busca) y CONTROL (cómo ordenarlo, y qué clase de pregunta
  es). El sistema consumía el criterio del orden pero no el comando a su
  alrededor, así que `contratos de obra en cali ordenados por mayor valor`
  viajaba a SECOP con `LIKE '%ordenados%'` y devolvía **0 filas** sobre un
  universo de 50 — verificado contra la fuente en ambas ramas el mismo día.
  `app/core/control_language.py` es ahora la autoridad: reconoce por span, no
  por lista de palabras, así que `ordenamiento territorial` y `gestión de
  órdenes de compra` quedan intactos. `ordering_signal` gana `valor_asc` y
  `fecha_asc` — antes `de menor a mayor valor` se leía descendente, invirtiendo
  la intención. Un follow-up de solo orden cambia el `ORDER BY` y nada más
  (`INV-ORDERING-FOLLOWUP-PRESERVES-UNIVERSE`, verificado comparando el `WHERE`
  generado, no solo el frame). Los verbos que fijan el modo de la pregunta
  (`está contratando`, `piensa contratar`, `cierran`) dejan de ser tema y
  conservan su complemento. HCA **171 → 175**, 0 rotos; focal 42/42;
  barrido adversarial original de R6: 516 comprobaciones, 0 fugas.

- **`ACTOR_HYGIENE_V1`: CLOSED / INTEGRATED**, main funcional `23f307c`.
  Casing deja de ser evidencia de actor; los alias exactos y las menciones
  explícitas conservan autoridad. Actor pending solo consume selecciones
  plausibles: una nueva búsqueda usa el reemplazo completo del merger existente,
  sin actor ni nonce residual; rango, ordinal y nombre exacto inequívoco siguen
  seleccionando. FTS5/BM25 recupera un pool acotado de 100 identidades y ordena
  por cobertura con ancla literal y como máximo un typo antes del top10.
  **Fuzzy nunca resuelve identidad**; exactos, ambigüedad, CC/NIT, índice no
  disponible y negación mantienen sus contratos. La desaparición total histórica
  no se reproduce con el índice actual: antes de R7 LMP estaba rank 7 y R7 lo
  lleva a rank 1. P50/P95 originales de R7, antes
  → después (ms): exacto **0,165/0,270 → 0,163/0,218**; typo
  **9,430/13,368 → 9,551/10,572**. Índice real immutable, hash intacto;
  validación sin red, stores efímeros. Revisión de integración: P50 exacto
  **0,160 → 0,164 ms**, normal **28,529 → 29,841 ms**, typo
  **9,070 → 8,986 ms**; sin regresión material. No Actor Relationships, AP v2 ni ADR
  duplicado. Autoridades, límites y evidencia en
  `docs/audits/2026-08-26_HCA_R7_ACTOR_HYGIENE.md`.

### Deuda explícita — HCA **no** está completo

- `FINAL_HCA` — pendiente: 177/195 PASS, 18 FAIL, 0 ERROR; no se declara cierre total.
- `TIME_AFTER_R5` — siguen fuera negación temporal ejecutable, NOT BETWEEN,
  huecos/resta de intervalos, quarters/semestres/años fiscales, recurrencia,
  días hábiles y deadlines. R5 no declara resuelta toda la semántica temporal.
- `CONTROL_LIMIT_SEMANTICS` (antes `DEFERRED_CONTROL_LIMIT`) — `top N`
  reconoce y consume el span y aplica el
  orden que lo acompaña, pero **no** la cardinalidad. `params["limit"]` no es
  cardinalidad de display sino tamaño del pool de candidatos que se valida y
  rankea; fijarlo en 10 cambiaría *qué* filas sobreviven, no cuántas se ven.
  Aplicarlo bien exige un `display_limit` separado atravesando formatter y
  paginación.
- `DEFERRED_FOLLOWUP_SCOPE_TOPIC` — un follow-up clasificado `change_scope` que
  trae un tema genuinamente nuevo lo pierde: la política de mutación de
  `change_scope` no admite campos de topic y el merger hereda el anterior.
  Reproducido en `b661a09` y `8bd5467` — no es regresión de R6. Congelado como
  `xfail(strict=True)` en `tests/test_control_language.py`.
- `ORACLE_REVIEW` — `NEG-N15` (`VALID_DEFERRED`) y `NEG-ANTI-002`
  (`WRONG_ORACLE_CANDIDATE`) son **deuda de oráculo**, no bugs de runtime
  abiertos. `HCA-FOLLOWUP-016`: **`POTENTIAL_ORACLE_DEBT`**, confirmado como
  candidato a revisión: exige `objeto_has: [obra]`, pero SEARCH-SH1 proyecta
  `obra` a `contract_type_include`. El runtime preserva `fecha_asc`, dinero,
  ciudad y tipo, sin contaminar `objeto`; el único fallo actual es esa
  exigencia temática. `HCA-FOLLOWUP-007` permanece en la lista previa de
  candidatos por la misma proyección. No se corrigieron los oráculos; el
  corpus HCA **no** se editó. El fixture pre-registrado `negation_focal_v1.yaml` (sha `f4f64a33…`,
  intacto) queda en 22 PASS / 2 FAIL; no se editó para fabricar 24/24.
  `HCA-ACTOR-004`: **CONFIRMED_ORACLE_DEBT** en la revisión de integración:
  pide candidatos en sus notas pero exige `actor_scope` en la aserción. Runtime
  conserva `CANDIDATES`, sin identidad, y el único fallo es el scope ausente;
  Actor Profile V1 prohíbe autorizar identidad fuzzy antes de selección humana.
  Fixture intacto. `ACTOR_RELATIONSHIPS: NOT PART OF R7`; no se abre Actor Profile V2.
- `SEMANTIC_AUTHORITY_CLEANUP` — pendiente.
- `QUERY_PLAN_AT_LIMIT` — `app/core/query_plan.py` sigue en **299/300 LoC**;
  R6 y R7 no lo tocaron ni necesitaron invocar la regla
  (`tests/test_module_size_limits.py`). Decisión: `ACCEPTED_AT_LIMIT`, sin
  refactor en este bloque. **`NEXT_TOUCH_RULE`:** cualquier nueva modificación
  funcional de `query_plan.py` debe extraer primero una responsabilidad
  suficiente para recuperar margen. El tope de 300 **no** se sube.
- `TEMPORAL_EPHEMERAL_SIGNAL_HYGIENE` — `_temporal_replace_scope` la limpia
  `temporal_gate.temporal_plan_block()`, que no se alcanza en dos salidas
  tempranas (bloqueo por negación contradictoria, frame pendiente de selección
  de actor). En esos casos la señal queda en el frame reportado. Sin ruta a
  SoQL observada (0 en 251 turnos de auditoría) y neutralizada aguas abajo por
  `_merge_modifiers` y por `_TRANSIENT` en `query_fingerprint`: higiene, no
  bug abierto.
- `GOB-DEPTO-001` — mismatch histórico del dialogue corpus, ajeno a este frente.

### Reproducibilidad del fixture focal de negación

El punto de entrada canónico es:

```bash
make audit-negation-v1
```

El target reutiliza, sin duplicar lógica, el runner offline/mock existente:

```bash
.venv/bin/python scripts/audits/run_conversation_holistic_audit.py \
  --corpus tests/fixtures/audits/negation_focal_v1.yaml
```

El reporting (`results.jsonl` y `verdicts.json`) sigue a cargo del runner. Por
defecto se escribe en su directorio temporal; para conservarlo en una ruta
elegida por el operador:

```bash
make audit-negation-v1 AUDIT_OUT=/ruta/de/salida
```

El resultado de cierre R4 es **22 PASS / 2 FAIL / 0 ERROR**. `NEG-N15` y
`NEG-ANTI-002` se preservan como **ORACLE / BENCHMARK DEBT**; por tanto, este
22/24 documenta el estado auditado y no es un gate perpetuo de producto ni dos
bugs abiertos de runtime. El fixture permanece congelado con SHA-256
`f4f64a33ea7009eb1da9fa7d3c910e67010e6dafc6b4c5b6932901c27fad9a1d`.

## Estado canónico — 2026-08-22 (histórico)

- **Main:** `8acb21a9a432f7750ac719f5921ad70776c448a2` — dos frentes cerrados y
  mergeados ff-only: **Actor Profile V1** (ADR-060) y **SEARCH-SH1** (ADR-061).
- **Suite:** 8250 passed, 2 skipped, 24 xfailed · query corpus 89/89 ·
  dialogue corpus 60/61 (`GOB-DEPTO-001` basal, histórico) · corpus de modalidad
  3.327 passed · dinero/follow-up/paginación focalizados 364 passed ·
  `make lint-core` limpio.
- **Qué cambió, en una línea:** el sistema aprendió *de quién* habla una consulta,
  y dejó de perder por el camino lo que el usuario acababa de pedir.

### Actor Profile V1 (ADR-060 · HEAD de cierre `7f24517`)

- Identidad **tipada** `IdentityKey(document_type, base_number)` — `NIT` y `CC`.
  La longitud jamás decide el tipo; el DV es evidencia de calidad, no autoridad.
- **Directorio local completo** de aliases desde SECOP II Contratos
  (`jbjy-vk9h`): 1.235.591 grupos de origen, 1.214.351 identidades indexadas,
  cobertura de identidad 98,84% y de actividad contractual 98,13%.
- Resolución **exacta autoritativa** vs. **candidatos difusos sin autoridad**: el
  trigram recupera, nunca auto-selecciona. Un candidato solo se vuelve
  autoritativo por selección explícita del usuario.
- **Huella contractual observada** en SECOP II, con fecha de consulta y límites
  declarados. No es dossier legal, ni RUP/RUES, ni informe de riesgo.
- **Conversación natural de actor:** `SET_ACTOR`, `REPLACE_ACTOR`,
  `SELECT_ACTOR_CANDIDATE`, `SET_BUYER`, `SET_TERRITORY`.
- **NIT y CC completos** en la lista de candidatos: el documento *es* la evidencia
  de desambiguación.
- **Selección clicable** en Streamlit que llega a la misma operación de backend
  que el texto, independiente del transporte.
- **Flag apagado por defecto en código** (`SECOPPAL_ACTOR_PROFILE_ENABLED=false`);
  se activa por entorno aprovisionado y exige un `actor_alias_index.sqlite`
  completo y compatible en `SECOP_ACTOR_ALIAS_INDEX_PATH`.
- Auditoría adversarial congelada de **1.150 casos**: P0 0 · P1 0 · identidades
  equivocadas 0 · secuestros temáticos 0 · pérdidas de actor 0 ·
  auto-resoluciones difusas 0.

### SEARCH-SH1 (ADR-061 · `8acb21a`)

- **Separación de dimensiones semánticas** (`semantic_dimensions.py`): no todo
  token no-stopword se convierte en `objeto`.
- **`convenios` deja de fugarse a tema.** Auditado en fuente: no existe
  `tipo_de_contrato = 'Convenio'`; es una clase **compuesta** que exige evidencia
  estructurada más texto controlado.
- **La arquitectura de modalidad se preserva intacta** — `ModalityResolver`,
  catálogo y léxico sin modificar. Naturaleza contractual ≠ modalidad.
- **Los deltas ortogonales componen:** repetir el año ya no borra un refinamiento
  monetario explícito.
- **Los refinamientos monetarios sobreviven** toda la cadena hasta el SoQL y el
  contexto almacenado.
- **La paginación se rebasa** tras cualquier mutación semántica de filtro.
- Dogfood en vivo: Gobernación de Atlántico / adulto mayor / 2026 pasa de 46 a 4
  filas con `>100M`, con **0** filas bajo el umbral y paginación correcta sobre el
  universo refinado.

### Promesa de producto vigente

Sin cambio respecto de 2026-08-17: SECOPPAL **prioriza procesos con las mejores
señales disponibles de vigencia y accionabilidad**; no pretende certificar que
todos sigan abiertos. Actor Profile añade una segunda promesa de la misma
familia: **describe la huella observada, no certifica al actor**.

### Deuda vigente

- Latencia trigram sobre el índice completo: **287–857 ms** según caché;
  pico de memoria del builder **3,02 GB**; **77** equivalencias de forma jurídica
  ambiguas que fallan cerrado. Ninguna bloquea el cierre.
- `DEPLOY-TZ-001` (ver §12.1) — sigue abierta.
- `GOB-DEPTO-001` del dialogue corpus — mismatch histórico, ajeno a ambos frentes.
- Deuda de lint del repositorio en superficies legacy; `lint-core` y todos los
  archivos nuevos de SEARCH-SH1 están limpios.

### Siguiente frente

**Actor Relationships V1 — UT / Consorcios.** No implementado. La operación
siguiente probable es una auditoría de fuente contra *Grupos de Proveedores —
SECOP II* (dataset probable `ceth-n4bn`), que debe validarse antes de fijar
arquitectura.

### Docs

- ADR-060 y ADR-061 en [`decisions_log.md`](decisions_log.md).
- Guías técnicas: [`architecture/ACTOR_PROFILE_V1.md`](architecture/ACTOR_PROFILE_V1.md) ·
  [`architecture/SEMANTIC_QUERY_DIMENSIONS.md`](architecture/SEMANTIC_QUERY_DIMENSIONS.md).
- Auditoría SEARCH-SH1:
  [`audits/2026-08-22_SEARCH_SH1_SEMANTIC_MONEY_HARDENING.md`](audits/2026-08-22_SEARCH_SH1_SEMANTIC_MONEY_HARDENING.md).
- Auditorías de Actor Profile en `docs/audits/`
  (`2026-08-18_ACTOR_PROFILE_DIAGNOSTIC.md`,
  `2026-08-19_ACTOR_PROFILE_V1_ADVERSARIAL_AUDIT.md`,
  `2026-08-20_AP3E_PRODUCTION_ACTOR_DIRECTORY.md`,
  `2026-08-20_AP3E1_FULL_INDEX_REMEDIATION.md`).
- Propuesta histórica de junio, parcialmente superada:
  [`planes/2026-06-29_perfil-contratista-dossier.md`](planes/2026-06-29_perfil-contratista-dossier.md).

> Los bloques siguientes (2026-08-17 y anteriores) quedan como histórico.

## Estado canónico — 2026-08-17 (histórico)

- **Main:** `2d4e24a` — arco **M1 → M2 → O1B** mergeado ff-only
  (ADR-056, ADR-057, ADR-058).
- **Suite:** 7466 passed, 2 skipped, 24 xfailed · query corpus 89/89 ·
  dialogue corpus 60/61 (`GOB-DEPTO-001` basal) · torture matrix 12/12 ·
  `make lint-core` limpio.
- **Qué cambió, en una línea:** el sistema dejó de afirmar disponibilidad que no
  puede sostener, y empezó a priorizar la que sí.
  - **M1 (`ad1bb28`)** — fundación del Truth Layer integrada *dormida*:
    36 archivos nuevos, 0 modificados, `VISIBLE_PRODUCT_CHANGE = NO`.
  - **M2 (`81993dd`)** — la tarjeta deja de mostrar
    `estado_de_apertura_del_proceso` como estado. Ese campo decía "Abierto" en el
    87,5% del dataset, y el 65,7% de esos no estaba en recepción de ofertas.
  - **O1B (`2d4e24a`)** — la búsqueda de oportunidades filtra en el `WHERE`, antes
    de `LIMIT`, por fecha civil `America/Bogota`. Prepublicación 68,8% → 0% y
    plazos vencidos 7,1% → 0% en muestra pareada de 22 consultas.
- **Promesa de producto vigente:** SECOPPAL **prioriza procesos con las mejores
  señales disponibles de vigencia y accionabilidad**; no pretende certificar que
  todos sigan abiertos. Eso permite tolerar falsos positivos plausibles (plazo
  nulo) sin sacrificar oportunidades ni perseguir una precisión imposible.
- **Investigación cerrada:** `TRUTH_LAYER_RESEARCH = CLOSED` ·
  `OPPORTUNITY_STATE_DATE_RESEARCH = CLOSED`.
- **Deuda nueva:** `DEPLOY-TZ-001` (ver §12.1) — garantizar `America/Bogota` en
  todos los entornos soportados. No se resuelve tocando la lógica de oportunidades.
- **Guía del Truth Layer:** `docs/architecture/TRUTH_LAYER_FOUNDATION.md`
  (dossier curado; el archivo histórico vive en `docs/archive/truth-layer-experimental/`).
- **Docs:** ADR-056/057/058 en `decisions_log.md`; informes en `docs/audits/`
  (`2026-08-14_TRUTH_LAYER_MAIN_FOUNDATION_INTEGRATION.md`,
  `2026-08-17_M2_PROCESS_OPERATIONAL_STATE.md`,
  `2026-08-17_O1B_CURRENT_OPPORTUNITY_RETRIEVAL.md`).

> Los bloques siguientes (2026-06-26 y anteriores) quedan como histórico.

## Estado canónico — 2026-06-26 (histórico)

- **Main:** `f683c2b` — mergeada la **capa de retrieval phrase-aware** (ADR-036, tag
  `semantic-retrieval-v1-2026-06`). Arco de 12 commits.
- **Suite:** 1714 passed, 23 xfailed.
- **Qué cambió:** el significado de la consulta sobrevive de punta a punta. Pipeline nuevo:
  `parse → PhraseSpec/objeto_phrases → WHERE recall-first → candidate_limit 200 → phrase_validator
  → rank_results (nivel + BM25) → display_limit 25 → formatter`.
- **Resultado (manual de contratación):** candidate recall 5/100→100/100, validación 96/100,
  Precision@25 52%→100%. Reutilizable: un concepto = un `PhraseSpec` (probado con "aire
  acondicionado"/SIP-049).
- **Bug histórico resuelto:** `RELEVANCE-RANKING-AFTER-FORMAT-001` — el BM25 reordenaba después del
  formateo y nunca afectaba el display; ahora hay etapa formal `rank_results` antes del render.
- **Docs:** ADR-036 en `decisions_log.md`; informe `docs/SEMANTIC_RETRIEVAL_SPRINT_2026-06.md`;
  runbook `docs/PHRASE_RETRIEVAL_RUNBOOK.md`.
- **Próximo foco:** día de dogfooding (40-50 consultas no diseñadas), corpus v2 (4 falsos negativos),
  feature "por qué apareció este resultado".

> El bloque siguiente (2026-06-13) queda como histórico.

## Estado canónico — 2026-06-13 (histórico)

- **Branch:** `feature/analytics-modular-registry` (main tiene Radar v1.0 mergeado)
- **Suite:** 1452 collected (~1425 passed, 23 xfailed)
- **Corpus:** 89/89 PASS, 0 FAIL
- **Torture matrix:** 12/12 invariantes
- **Eval harness:** `scripts/eval_rated.py` sobre 70 queries calificadas reales (SQLite). Parser-tier 38/38 = 100% (5 bugs silenciosamente resueltos por sprints mayo 2026). Pipeline-tier 26 entradas etiquetadas para `--live`. 1 bug remanente: RATED-069 (`El Banco` municipio Magdalena) — reclasificado pipeline-tier, requiere desambiguación LLM.
- **feedback:** SQLite 70 calificadas (18👍/52👎), JSONL 42 (divergencia documentada en `eval_rated.py`). Dual-store: SQLite primario, JSONL append-only.
- **Orchestrator:** 196 LOC (desde 1549 LOC) — refactor a workflow/ completado (ADR-023)
- **Decisions log:** 31 ADRs (ADR-001 a ADR-031) en `docs/decisions_log.md`
- **Features nuevas (junio 2026):** búsqueda por regiones (ADR-028), Radar v1.0 — búsquedas guardadas con feed de movimientos (ADR-029), paquete modular de analytics con registry (ADR-030), adaptador OpenWA + robustez Telegram + paridad de links (ADR-027, ADR-031)
- **Parser fixes (junio 2026):** unaccent en ciudad (BUG 1), polaridad "no sea por más de" (BUG 2), "barras" como jerga monetaria (BUG 3), "no menos de" como valor_min (fix simétrico a BUG 2)
- **Próximo foco:** sellar frame leak del follow-up (raw_params merge ciego), shadow LLM para follow-up, RATED-069 (desambiguación banco=municipio vs banco=tema), merge main → feature branch (Radar + costura regiones)
- **Conteos históricos (424/466/534/596/1212):** superados; referencia, no estado actual

---

## 1. Resumen ejecutivo

SECOPPAL es un asistente conversacional orientado a consultar, filtrar, interpretar y presentar información de contratación pública colombiana proveniente de SECOP II / datos.gov.co.

El objetivo no es replicar un portal de filtros, sino construir un **asesor inteligente de contratación pública** capaz de:

1. Entender consultas naturales del usuario.
2. Traducirlas a parámetros estructurados.
3. Resolver entidades, lugares, departamentos, contratistas, temas, fechas, valores y estados.
4. Construir consultas SoQL contra datasets de SECOP.
5. Ejecutar búsquedas con guardrails para evitar consultas globales peligrosas.
6. Mantener contexto conversacional entre turnos.
7. Permitir follow-ups como “muéstrame más”, “los de mayor valor”, “ahora en Bolívar”, “solo 2025”, “en ejecución”.
8. Presentar resultados en un formato asesor, no meramente tabular.
9. Generar sugerencias accionables para explorar el universo contractual.
10. Mantener trazabilidad de bugs, decisiones y cambios mediante tests y bitácoras.

El sistema está actualmente en una etapa de **MVP funcional con endurecimiento semántico y conversacional + primera ruta analítica**. Ya existen parser determinístico, resolución de entidades, constructor SoQL, política de respuesta asesora, seguimiento conversacional, sugerencias, paginación, feedback y suite de tests.

En mayo 2026 se agregó la primera ruta analítica determinística (AQ-001A aggregate_sum) que permite responder consultas del tipo “cuánto se contrató en X” con SUM + COUNT en vez de listar contratos.

---

## 2. Problema que resuelve

SECOP II contiene gran cantidad de información pública, pero su consulta directa suele ser difícil para usuarios no técnicos o para funcionarios que requieren interpretación rápida.

Problemas habituales:

- Los nombres oficiales de entidades no coinciden con la forma natural en que el usuario las menciona.
- Los estados de los contratos/procesos tienen semántica técnica y pueden inducir errores si se interpretan literalmente.
- Las consultas por objeto contractual requieren buscar en texto libre, con sinónimos y variantes morfológicas.
- El usuario conversa por refinamientos, no formula cada consulta completa.
- El portal puede demorar o fallar con consultas pesadas.
- Una búsqueda demasiado amplia puede devolver miles de resultados irrelevantes.
- Los filtros estrictos pueden devolver cero resultados, pero relajarlos automáticamente puede introducir resultados fuera de alcance.

SECOPPAL aborda estos problemas mediante una arquitectura por capas: parser, normalizador semántico, resolver de entidades, constructor de consultas, cliente SECOP, observer estadístico, política de respuesta y memoria conversacional de corto plazo.

---

## 3. Stack técnico

### 3.1 Lenguaje y runtime

- **Python** como lenguaje principal.
- **Burr** para definir el flujo de acciones del pipeline conversacional.
- **Socrata / sodapy** para consultar datasets de datos.gov.co.
- **RapidFuzz** para fuzzy matching de entidades y departamentos.
- **pytest** para pruebas unitarias y E2E.
- **Streamlit** como interfaz frontend local de pruebas.
- **FastAPI / Uvicorn** como backend local de servicio.

### 3.2 Datasets SECOP usados

El sistema distingue dos datasets principales:

| Dataset lógico | Dataset ID | Uso |
|---|---:|---|
| Procesos | `p6dx-8zbt` | Procesos, convocatorias, oportunidades, publicaciones, apertura, estado de procedimiento. |
| Contratos | `jbjy-vk9h` | Contratos registrados, proveedor adjudicado, valor del contrato, fecha de firma, estado contractual. |

### 3.3 Interfaces previstas

- Streamlit para pruebas visuales.
- Backend local con Uvicorn.
- Posible integración futura con Telegram / WhatsApp / Hermes.

---

## 4. Arquitectura general

```text
Usuario
  ↓
SECOPPAL Workflow / Orchestrator
  ↓
QueryRouter
  ↓
FollowupEngine, si hay contexto conversacional
  ↓
EntityResolver
  ↓
SoQLBuilder
  ↓
SecopClient
  ↓
Observer / Suggester
  ↓
ResponsePolicy + Formatter
  ↓
Respuesta asesora al usuario
```

### 4.1 Capas del sistema

| Capa | Componente | Responsabilidad |
|---|---|---|
| Entrada | `orchestrator.py` | Fachada delgada: chain-of-responsibility de handlers. |
| Workflow | `workflow/` | Acciones Burr, handlers de casos especiales, grafo, helpers, persistencia. |
| Parsing | `query_router.py` | Extrae dataset, tema, fecha, valor, estado, ciudad, NIT, modalidad. |
| Follow-up | `followup_engine.py` | Fusiona consulta actual con contexto anterior. |
| Memoria corta | `conversation_store.py` | Guarda historial por `chat_id` y sugerencias. |
| Resolución | `entity_resolver.py` | Resuelve entidades y departamentos mediante gazetteer + fuzzy + LIKE fallback. |
| SQL | `soql_builder.py` | Construye consultas SoQL y agregaciones. |
| Ejecución | `secop_client.py` | Ejecuta consultas contra Socrata/SECOP con retries. |
| Observación | `observer.py` | Calcula distribución, top entidades, valores, modalidades. |
| Sugerencias | `suggester.py` | Genera acciones como ordenar por valor, ver activos, cambiar dataset. |
| Respuesta | `response_policy.py` | Define tono asesor para ambigüedad, cero resultados y resultados claros. |
| Narrativa | `narrator.py` | Genera resumen con LLM, validando cifras monetarias. |
| Formato | `formatter.py` | Convierte resultados en tarjetas/listas para canal. |
| Feedback | `feedback.py` | Guarda trazas y calificaciones del usuario. |

---

## 5. Estructura de archivos principales

```text
app/core/
├── conversation_store.py
├── direct_responses.py
├── entity_resolver.py
├── entity_types.py
├── estado_families.py
├── feedback.py
├── followup_engine.py
├── formatter.py
├── intent_vocabulary.py
├── llm_handler.py
├── narrator.py
├── observer.py
├── orchestrator.py
├── query_frame.py
├── query_router.py
├── response_policy.py
├── secop_client.py
├── soql_builder.py
├── suggester.py
└── workflow/
    ├── actions/
    │   ├── parsing.py
    │   ├── planning.py
    │   ├── execution.py
    │   └── rendering.py
    ├── handlers/
    │   ├── reset.py
    │   ├── pagination.py
    │   ├── suggestion.py
    │   ├── direct_lookup.py
    │   ├── refinement.py
    │   └── normal_flow.py
    ├── graph.py
    ├── helpers.py
    ├── deps.py
    ├── context.py
    ├── persistence.py
    └── results.py
```

### 5.1 `orchestrator.py`

Es la **fachada delgada** del flujo. Antes era un god module de 1549 líneas; ahora es una chain-of-responsibility de 196 líneas.

Responsabilidades actuales:

- Inicializar `WorkflowDeps` (inyección de dependencias).
- Construir `HandlerContext` para cada turno.
- Ejecutar `run_query()` como chain-of-responsibility:
  1. `handle_refinement` (seed_resolved_params)
  2. `handle_reset` (comandos /reset, nueva, limpiar)
  3. `handle_pagination` ("más", "siguiente", números de página)
  4. `handle_suggestion_selection` (selección 1-5 de sugerencias)
  5. `handle_direct_lookup` (ID CO1.XXX.YYY, URL, referencia de entidad)
  6. `handle_normal_flow` (pipeline Burr completo: parse → plan → execute → observe → **rank** → render)
- Mantener shim de backward-compat para tests legacy (re-exports de actions/helpers).

Puntos relevantes:

- La paginación pura no pasa por todo el pipeline — la captura `handle_pagination` antes del Burr graph.
- El follow-up se gestiona dentro de `workflow/actions/parsing.py` vía `apply_context`.
- El guard anti `WHERE 1=1` vive en `workflow/actions/execution.py` (`execute_query`).
- Cada handler vive en `workflow/handlers/` y recibe `(WorkflowDeps, HandlerContext)` — no accede a `self` de `SecopalWorkflow`.

### 5.2 `query_router.py`

Parser determinístico de lenguaje natural.

Extrae:

- Dataset lógico: procesos / contratos.
- Tema contractual (`objeto`).
- Fechas: años, rangos, meses.
- Valores: mínimos, máximos, rangos.
- Modalidad.
- Estado semántico.
- NIT / contratista.
- Ciudad / departamento.
- Entidades mediante rewrite + gazetteer.

Decisión clave: es preferible un parser determinístico auditable antes que depender completamente del LLM.

### 5.3 `followup_engine.py`

Cerebro conversacional del seguimiento, no de todo el sistema.

Responsabilidades:

- Identificar si una frase es refinamiento, cambio de scope, cambio de año, cambio de orden, cambio de dataset, selección de sugerencia o nueva búsqueda.
- Fusionar los parámetros actuales con los anteriores.
- Evitar perder contexto en frases como:
  - “los de mayor valor”
  - “solo 2025”
  - “ahora en Bolívar”
  - “en ejecución”
  - “muéstrame más”

### 5.4 `conversation_store.py`

Memoria transaccional por chat.

Responsabilidades:

- Guardar historial en JSONL.
- Recuperar últimos turnos.
- Guardar sugerencias del último turno.
- Detectar comandos de reset.
- Detectar frases de paginación pura.

No debe ser el cerebro semántico del follow-up. Esa responsabilidad queda en `followup_engine.py`.

### 5.5 `entity_resolver.py`

Resuelve entidades y departamentos.

Estrategia:

1. Gazetteer-first: buscar alias conocidos por substring.
2. Fuzzy matching si no hay match exacto.
3. Fallback LIKE si no puede resolver.

Mejora relevante V3:

- Evita que nombres de departamentos desnudos se capturen como entidades.
- Ejemplo: “en Atlántico” debe ser departamento, no entidad.
- Solo se acepta como entidad si hay calificador como “gobernación de”, “alcaldía de”, “municipio de”, etc.

### 5.6 `soql_builder.py`

Construye consultas SoQL para procesos y contratos.

Responsabilidades:

- Seleccionar campos por dataset.
- Construir `WHERE` con departamento, ciudad, entidad, objeto, valor, estado, modalidad, fecha y contratista.
- Aplicar variantes morfológicas al objeto.
- Construir agregaciones para observer:
  - top entidades
  - estadísticas de valor
  - top modalidades
  - rango de fechas
  - distribución temporal

Guard importante:

- `_build_where()` puede devolver `1=1`, pero `orchestrator.execute_query()` bloquea consultas sin scope/topic/contratista antes de ejecutar.

### 5.7 `estado_families.py`

Capa semántica de estados.

Decisión crítica reciente:

- En SECOPPAL, `Cerrado` **no significa firmado**.
- `Cerrado` se interpreta como gestión/expediente contractual cerrado.
- “Firmado” fuerza dataset contratos, pero no aplica filtro `estado_contrato = Cerrado`.
- “En ejecución / activo / vigente” se interpreta como:

```python
["En ejecución", "Modificado", "Prorrogado"]
```

### 5.8 `suggester.py`

Genera sugerencias accionables según la consulta actual.

Cambio relevante:

- Se reemplazó “Ver solo contratos firmados” por “Ver contratos activos (en ejecución)”.
- Se agregó “Ver contratos cerrados” con semántica separada.

### 5.9 `response_policy.py`

Construye respuestas asesoras en tres caminos:

1. Consulta ambigua.
2. Cero resultados.
3. Consulta clara con resultados.

Principio de producto:

- Para consultas claras: mostrar resultados + interpretación + universo + sugerencias.
- Para consultas ambiguas: pedir aclaración con caminos posibles.
- Para cero resultados: no relajar filtros automáticamente sin avisar.

---

## 6. Flujo principal de consulta

### 6.1 Consulta nueva

Ejemplo:

```text
hola muéstrame contratos de mantenimiento en Bogotá
```

Flujo:

1. `QueryRouter.parse()` detecta:
   - dataset: contratos
   - objeto: mantenimiento
   - departamento: Distrito Capital de Bogotá
2. `EntityResolver` confirma departamento/entidad.
3. `SoQLBuilder` genera SoQL.
4. `SecopClient` ejecuta count + query.
5. `Observer` calcula universo si aplica.
6. `Suggester` propone acciones.
7. `ResponsePolicy` construye respuesta final.
8. `ConversationStore` guarda turno.

### 6.2 Follow-up

Consulta base:

```text
contratos de mantenimiento en Bogotá
```

Follow-up:

```text
los de mayor valor
```

Resultado esperado:

```json
{
  "dataset": "contratos",
  "objeto": ["mantenimiento"],
  "departamento_resolved": "Distrito Capital de Bogotá",
  "ordering_signal": "valor_desc"
}
```

### 6.3 Paginación

Consulta:

```text
muéstrame más
```

Si hay historial:

- Recupera `parsed_params` del último turno.
- Incrementa `offset`.
- Conserva orden anterior.
- Ejecuta directamente.

Si no hay historial:

- Responde pidiendo contexto.

---

## 7. Política semántica de estados SECOP

### 7.1 Principio general

Los estados de SECOP no se deben traducir de forma coloquial sin control jurídico-funcional.

### 7.2 Reglas vigentes

| Frase usuario | Interpretación SECOPPAL | Filtro |
|---|---|---|
| contratos firmados | Dataset contratos, sin filtro de estado | `dataset='contratos'` |
| contratos en ejecución | Contratos activos | `estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')` |
| contratos activos | Contratos activos | `estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')` |
| contratos vigentes | Contratos activos | `estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')` |
| contratos cerrados | Gestión/expediente contractual cerrado | `estado_contrato = 'Cerrado'` |
| contratos terminados | Contratos terminados | `estado_contrato = 'terminado'` |
| contratos cedidos | Contratos cedidos | `estado_contrato = 'cedido'` |
| suspendidos/cancelados | No procede / suspendidos | `estado_contrato IN ('Suspendido', 'Cancelado')` |

### 7.3 Invariante INV-005 — Firmado no es estado

`Firmado/firmados` no es un estado contractual filtrable. Es una propiedad universal del dataset contratos (`jbjy-vk9h`). Todo registro en ese dataset representa un contrato con existencia contractual.

Por tanto:

- `firmados` selecciona `dataset='contratos'`.
- Nunca emite valores en `estado`, `estado_contrato` ni `estado_field`.
- `en ejecución` es un subconjunto operativo de firmados, no su sinónimo.

Verificación empírica (2026-05-15): los valores reales del campo `estado_contrato` en `jbjy-vk9h` son:

```text
En ejecución, Cerrado, Modificado, terminado, Borrador, Aprobado,
Cancelado, enviado Proveedor, cedido, En aprobación, Suspendido,
Prorrogado.
```

No existen `Firmado` ni `Celebrado`.

---

## 8. Guardrails principales

### 8.1 Anti `WHERE 1=1`

El sistema no ejecuta consultas sin al menos un filtro base.

Filtros base válidos:

- Objeto / tema contractual.
- Entidad resuelta o LIKE de entidad.
- Departamento.
- Ciudad.
- Contratista.

No son filtros base suficientes por sí solos:

- Fecha.
- Valor.
- Ordenamiento.

Mensaje recomendado:

```text
Necesito al menos un filtro de entidad, lugar, tema o contratista para buscar. ¿Sobre qué quieres saber?
```

### 8.2 Cero resultados

En follow-ups conversacionales, el sistema no debe relajar automáticamente filtros.

Ejemplo correcto:

```text
No encontré resultados con esos filtros. Puedo intentar:
1. quitar el filtro de fecha
2. ampliar el tema
3. buscar en procesos
```

### 8.3 Timeout por consultas pesadas

Si una consulta con orden por valor sobre universo histórico demora demasiado:

- Reintenta sin `ordering_signal`.
- Cambia a orden por fecha.
- Explica al usuario que cambió la estrategia.

---

## 9. Versionado y bitácora de cambios

> Esta sección debe mantenerse como changelog técnico. Cada cambio importante debe registrar fecha, archivos modificados, bug/causa, solución y validación.

### 2026-05-15 — Auditoría externa: H1 vivo en camino LLM, ADR-006 erosionada

Una auditoría externa identificó que:

- El bug `firmado=Celebrado`, declarado cerrado en el camino determinístico, seguía vivo en `app/core/llm_handler.py` (`SYSTEM_PROMPT` y enum del tool). El camino determinístico estaba correcto; el camino LLM no.
- ADR-006 (no versiones paralelas en `app/core/`) se había erosionado: reaparecieron `.bak`, `.bak2`, `backup_20260514_173654/`, y binarios de ofimática dentro del módulo.

Acciones tomadas:

- Sincronizado `llm_handler.py` con INV-005 reformulada.
- Limpiado `app/core/`; archivos movidos a `archive/cleanup_2026-05-15-audit/`.
- Añadido target `make lint-core` que bloquea reincidencia.
- Tests nuevos: `test_llm_handler_state_policy.py`, `test_estado_families_integrity.py`, `test_soql_count_select_consistency.py`.
- Verificación empírica de estados reales en `jbjy-vk9h` registrada en sección 7.3.

Lección: las invariantes deben tener tests que las defiendan. ADR-006 sin `lint-core` es aspiracional; con `lint-core` es defensiva.

### 2026-05-15 — Limpieza de duplicados core

**Problema:** existían archivos paralelos tipo `v2.py`, `v3.py` dentro de `app/core`, lo cual podía confundir a agentes y humanos.

**Archivos afectados:**

- `soql_builder.py`
- `soql_builder v2.py`
- `query_router.py`
- `query_router v2.py`
- `entity_resolver v2.py`
- `entity_resolver v3.py`
- `orchestrator.py`
- `orchestrator v2.py`

**Decisiones:**

- `soql_builder.py` queda como canónico.
- `query_router.py` queda como canónico.
- `entity_resolver.py` debe contener la lógica de `entity_resolver v3.py`.
- `orchestrator.py` debe contener integración con `followup_engine.detect_and_merge()`.
- Versiones paralelas se archivan fuera de `app/core`.

**Validación esperada:**

```bash
find app/core -name "* v*.py" -print
python3 -m py_compile app/core/*.py
pytest -q
```

### 2026-05-15 — Follow-up engine como política principal de seguimiento

**Problema:** la lógica conversacional estaba repartida entre `conversation_store.py`, `query_frame.py` y `orchestrator.py`.

**Solución:** `followup_engine.py` queda como clasificador/merger canónico para follow-ups.

**Decisión:**

- `QueryFrame` es la representación intermedia oficial entre `params` y `FollowupEngine`. No es legacy: vive en el hot path de `apply_context` como transporte entre `frame_from_params` y `detect_and_merge`. Su eventual eliminación es un refactor futuro, no urgente.
- `ConversationStore` queda como memoria transaccional.
- `FollowupEngine` decide intención conversacional.

### 2026-05-15 — Bug `esten + estado_contrato = Cerrado`

**Consulta base:**

```text
hola muestrame contratos de mantenimiento en bogota
```

**Follow-up:**

```text
muestrame mas contratos que esten firmados o en ejecucion
```

**Bug original:**

- El objeto anterior `mantenimiento` se perdía.
- `esten` se convertía en objeto contractual.
- “firmados o en ejecución” terminaba como `estado_contrato = 'Cerrado'`.
- El sistema confundía `firmado` con `Cerrado`.

**Fixes aplicados:**

1. `query_router.py`: se agregaron stopwords:

```python
"este", "esten", "sea", "sean", "encuentre", "encuentren"
```

2. `estado_families.py`: se separó `con_contrato` en familias más precisas:

```text
contrato_activo
contrato_cerrado
contrato_terminado
contrato_cedido
```

3. `intent_vocabulary.py`: “firmados” fuerza dataset contratos, pero no aplica estado.
4. `soql_builder.py`: si existe lista `estado_contrato`, tiene prioridad sobre `estado` scalar legacy.
5. `suggester.py`: se reemplazó “Ver solo contratos firmados” por “Ver contratos activos (en ejecución)”.
6. Tests actualizados.

**Test E2E nuevo:**

```text
tests/test_e2e_estado_followup_bug.py
```

**Resultado esperado del escenario:**

```json
{
  "dataset": "contratos",
  "departamento_resolved": "Distrito Capital de Bogotá",
  "objeto": ["mantenimiento"],
  "estado_contrato": ["En ejecución", "Modificado", "Prorrogado"]
}
```

**Prohibido:**

```text
LIKE '%esten%'
estado_contrato = 'Cerrado'
Contratos de esten
```

**Validación reportada:**

```text
424/424 tests passed
```

---

### 2026-08-22 — Actor Profile V1 + SEARCH-SH1: de quién habla la consulta, y qué sobrevive al refinamiento

**Problema/causa.** Dos frentes independientes con el mismo patrón de fondo: el
sistema descartaba en silencio información que el usuario sí había dado.

1. **Actor.** El nombre de un proveedor entraba como objeto contractual. AP0 lo
   midió: *"qué ha celebrado la persona 1140835135 en 2025"* producía una
   búsqueda de la palabra `persona` en 2025, presentada como el historial de esa
   persona. En procesos, `spec.contractor is None` hacía que el filtro de
   contratista simplemente no se añadiera al `WHERE`.
2. **Búsqueda.** `convenios` caía en el extractor de objeto y se volvía búsqueda
   literal de texto. Y en un follow-up, repetir el año clasificaba el turno como
   `change_year`, cuyo conjunto permitido descartaba `valor_min`; como el campo
   desaparecía antes de serializar el frame, el fingerprint no cambiaba y la
   paginación seguía recorriendo el universo sin filtrar.

**Solución.**

- **Actor Profile V1 (ADR-060, `7f24517`)** — identidad tipada
  `IdentityKey(document_type, base_number)` como spine; directorio SQLite local
  de aliases construido desde `jbjy-vk9h`; estado de resolución separado del
  método de autoridad, con la recuperación difusa explícitamente **sin**
  autoridad; huella contractual observada con límites declarados; álgebra
  conversacional de actor; documento completo y selección clicable en la UX de
  candidatos. Flag apagado por defecto en código.
- **SEARCH-SH1 (ADR-061, `8acb21a`)** — `SemanticDimension` como vocabulario
  compartido de dimensiones; resolutor y proyección de tipo/naturaleza
  contractual respaldados por auditoría de fuente (`convenio` es **compuesto**,
  no un alias de modalidad); deltas ortogonales componibles en el follow-up; y
  toda mutación semántica de filtro invalida la paginación.

**Archivos tocados.** Actor: `app/core/actor_*.py`, `app/core/actor_profile/`,
`app/core/workflow/actions/actor_resolution.py`,
`app/core/workflow/handlers/actor_selection.py`, `app/data/actor_alias_*.py`,
`app/config.py`, `streamlit_app.py`. SEARCH-SH1:
`app/core/semantic_dimensions.py`, `app/core/contract_type_resolver.py`,
`app/core/contract_type_projection.py`, `app/core/followup_engine.py`,
`app/core/followup_mutations.py`, `app/core/_followup_constants.py`,
`app/core/query_router.py`, `app/core/query_frame.py`,
`app/core/soql_builder.py`, `app/core/conversation_store.py`,
`app/core/workflow/handlers/normal_flow.py`.

**Validación.** Suite 8250 passed, 2 skipped, 24 xfailed · query corpus 89/89 ·
dialogue corpus 60/61 (`GOB-DEPTO-001` basal) · modalidad/diálogo/E2E 3.327
passed · dinero/follow-up/paginación 364 passed · mutaciones SEARCH-SH1 15/15
detectadas con fuentes restauradas byte a byte · `make lint-core` limpio.
Cierre de Actor Profile: 8175 passed, corpus de actor 41/41, corpus natural 6/6,
mutaciones AP3-G de UX 15/15, auditoría adversarial congelada de 1.150 casos con
P0 0 · P1 0 · identidades equivocadas 0.

**Dogfood en vivo (2026-08-22).** Gobernación de Atlántico / adulto mayor / 2026:
46 filas base → 4 tras `>100M`, con 0 filas bajo el umbral y paginación correcta
sobre el universo refinado. Convenios en Barranquilla: 2 filas de procesos, sin
fuga a `objeto`. Convenio + régimen especial + `>2B`: 0 filas — intersección
válida respaldada por fuente.

**Deuda aceptada.**

- Latencia trigram del índice completo **287–857 ms** según caché, sobre el
  presupuesto de turno; pico de memoria del builder **3,02 GB**.
- **77** equivalencias de forma jurídica ambiguas: fallan cerrado.
- El índice de aliases es requisito de aprovisionamiento para activar el frente.
- `GOB-DEPTO-001` sigue siendo un mismatch histórico, ajeno a ambos frentes.

**Fuera de alcance de Actor Profile V1.** UT/consorcios, RUP, RUES,
representante legal, SIRI, responsabilidad fiscal, `ActorId` como grafo entre
fuentes, score de riesgo, Radar por actor, grafo profundo/Neo4j. Siguiente
frente: **Actor Relationships V1 — UT / Consorcios**.

**Documentación:** ADR-060 y ADR-061 en `decisions_log.md`;
`docs/architecture/ACTOR_PROFILE_V1.md`;
`docs/architecture/SEMANTIC_QUERY_DIMENSIONS.md`;
`docs/audits/2026-08-22_SEARCH_SH1_SEMANTIC_MONEY_HARDENING.md`.

---

### 2026-08-17 — Arco M1 → M2 → O1B: de afirmar disponibilidad a priorizarla

**Problema/causa.** Dos defectos con la misma raíz: el sistema afirmaba
disponibilidad que la fuente no sostiene.

1. La tarjeta mostraba `estado_de_apertura_del_proceso` como estado del proceso.
   Medido en vivo sobre 9.018.738 filas: ese campo dice "Abierto" en el 87,5%, y
   el 65,7% de esos ya no admite ofertas (60,1% `Seleccionado`, 4,1% `Evaluación`).
2. La búsqueda de oportunidades devolvía 68,8% de filas en prepublicación y 7,1%
   con plazo vencido. O1A había falsificado la salida fácil: filtrar después de
   `LIMIT 50` pierde candidatos, porque el recorte ocurre antes que el filtro.

**Solución.** Tres bloques encadenados, cada uno con su ADR:

- **M1 (ADR-057, `ad1bb28`)** — trasplante selectivo de la fundación del Truth
  Layer desde la rama experimental: 28 `FOUNDATION_KEEP` de 103 elementos
  inventariados, 36 archivos nuevos y 0 modificados. Entra dormida.
- **M2 (ADR-058, `81993dd`)** — `process_operational_state.py` traduce la salida
  del observer a lo que la tarjeta puede afirmar. El campo crudo no se oculta: se
  degrada a corroboración y se muestra cuando contradice la etapa.
- **O1B (ADR-056, `2d4e24a`)** — política pura en
  `opportunity_retrieval_policy.py`, aplicada solo a `procesos` +
  `opportunity_search` sin tiempo ni estado explícitos. Filtro en el `WHERE` con
  un único `case(...)` de Socrata y frontera de día en **fecha civil
  `America/Bogota`**, no en UTC.

**Archivos tocados.** `app/core/opportunity_retrieval_policy.py` (nuevo),
`app/core/process_operational_state.py`, `app/core/soql_builder.py`,
`app/core/workflow/actions/planning.py`, `app/core/workflow/actions/execution.py`,
más la fundación bajo `app/core/truth_layer/` (M1) y tests dedicados.

**Validación.** Suite 7466 passed, 2 skipped, 24 xfailed · query corpus 89/89 ·
dialogue corpus 60/61 (`GOB-DEPTO-001` basal, preexistente) · torture matrix
12/12 invariantes · `make lint-core` limpio · `data/feedback.jsonl` sin tocar.
Paridad fuera de alcance: **0 divergencias** sobre 22 consultas comparadas contra
`81993dd` (procesos con año, históricos, estados explícitos, contratos, entidad,
modalidad, valor, direct lookup). Verificación live contra Socrata: 6/6 casos de
frontera correctos, incluidos plazo de hoy (38 filas), de mañana (637) y plazo
futuro con publicación de más de 90 días (22).

**Corrección intermedia (O1B.1).** La primera versión comparaba contra el
instante UTC exacto. Eso excluía el proceso durante todo su último día y, entre
las 19:00 y 24:00 COT, adelantaba un día calendario. `2d4e24a` mueve la frontera
a `America/Bogota` y hace que un follow-up semántico recapture el reloj en vez de
heredar el del turno inicial; la paginación sí conserva la referencia original.

**Deuda aceptada.**

- `DEPLOY-TZ-001` — dependencia de la base de zonas horarias del sistema (§12.1).
- `STATE_DATE_CONFLICT_DEFERRED` — `Evaluación` con plazo futuro sigue fuera del
  default; O1B no reconcilia campos contradictorios.
- `OPPORTUNITY_MODALITY_SEMANTICS_DEFERRED` — no se excluye contratación directa;
  la ausencia de plazo no es evidencia negativa por sí sola.
- `WAREHOUSE_RECEPTION_DATE_TRANSPORT_DEFERRED` — solo aplica si Search gana un
  backend local.
- Un candidato con plazo nulo puede ser un falso positivo plausible: eso es
  deliberado, no un defecto.

**Documentación:** ADR-056, ADR-057, ADR-058; informes en `docs/audits/`.

---

### 2026-08-17 — Truth Layer: foundation consolidada y dossier técnico preservado

**Problema/causa.** M1 integró selectivamente el *código* probado del Truth Layer,
pero no la memoria técnica que explica por qué ese código quedó así. Main tenía
1 documento del Truth Layer; el worktree experimental
`secoppal-truth-layer` (`exp/generic-enrichment-runtime-v1`, `ba02826`) tenía 33,
unas 11.700 líneas. Copiarlos todos habría creado un cementerio documental;
no copiar nada habría perdido meses de razonamiento.

**Solución.** Curaduría con matriz `DOCUMENTO / EN_MAIN / VIGENTE_O_HISTÓRICO /
ACCIÓN` antes de mover nada, y una guía de entrada única:
`docs/architecture/TRUTH_LAYER_FOUNDATION.md` — qué problema resuelve, qué hay
realmente en main, qué usa producción hoy, qué está dormido, principios
arquitectónicos, qué NO está integrado, evidencia de cierre, cuándo reabrir y
documentos fuente.

- **Canónicos** (`docs/architecture/truth-layer/`, 6 + 2 de segundo nivel):
  reconciliación de diseño R1/R2/R3, closeout de proceso T1B-2 con su
  post-commit patch, temporal + visibility T1B-4/5; en segundo nivel el closeout
  contractual T1B-3 y E1 como case study.
- **Histórico** (`docs/archive/truth-layer-experimental/`, 5 + README): dogfood,
  D1-MIN, addendum de autorización, bloque G y opportunity composition, con una
  tabla que dice de cada uno **por qué no es vigente**.
- **Fuera de curaduría:** los 18 restantes siguen en el worktree, que se conserva
  como archivo histórico.

**Tres hallazgos de la matriz.** (1) `DESIGN_RECONCILIATION_R1` **no** está
supersedido — R3 lo dice expresamente, y el cuerpo sustantivo de decisiones vive
ahí, no en R2; rescatar solo R2/R3 habría dejado el razonamiento colgando.
(2) El post-commit patch de T1B-2 toca `process_lifecycle.py`, que **sí** está en
main: sin él el closeout describe un estado anterior al código productivo.
(3) **El cierre empírico final no existía**: el identificador
`TRUTH_LAYER_PROVEN_FOUNDATION_CLOSE_WORKSTREAM` se citaba desde M1 sin documento
que lo sustentara. Se redactó
`2026-08-17_TRUTH_LAYER_EMPIRICAL_CLOSEOUT.md` citando cada número con su fuente
y declarando explícitamente lo que **no** se midió — en particular que la revisión
humana de calidad semántica **nunca se completó** (muestra de 80 preparada,
`reviewer labels prefilled=0`), y que el benchmark de core conserva veredicto de
FALLO (`106.732 ms/1.000` contra un gate de `70 ms/1.000`).

**Archivos tocados.** Solo documentación: `docs/architecture/` (nuevo),
`docs/archive/truth-layer-experimental/` (nuevo). Ningún archivo de código.

**Resultado.** La investigación experimental fue cerrada; se integró
selectivamente la infraestructura probada y se preservó el dossier técnico que
documenta sus invariantes, límites y criterios de reactivación. Producción
consume **un solo** punto del Truth Layer (`process_lifecycle` → M2); los ejes
contractuales y las librerías de identidad/grouping están integrados pero
dormidos (`GROUPING_RUNTIME_WIRING = DEFERRED`).

**Deuda registrada.** `HUMAN_SEMANTIC_REVIEW = NEVER_COMPLETED` — es el eslabón
abierto si alguien reabre la línea. Reabrir exige un consumidor de producto
declarado, no curiosidad técnica (§8 de la guía).

**Documentación:** `docs/architecture/TRUTH_LAYER_FOUNDATION.md` y los 14
documentos que enlaza.

---

## 10. Tests y evaluación de calidad

### 10.1 Suite general

Comando:

```bash
pytest -q
```

Estado actual:

```text
1212 passed, 23 xfailed
```

### 10.2 Eval harness sobre queries reales calificadas

**Problema que resuelve:** La métrica histórica "26% de precisión" mezclaba parser, orden, follow-up y timeouts de red en una sola cifra no accionable. No había forma de saber si un fix mejoraba o empeoraba el sistema sobre las queries que el usuario realmente calificó.

**Solución:** `scripts/eval_rated.py` — harness que corre las 70 queries calificadas reales (desde SQLite `data/secoppal.db`) contra el pipeline y compara contra comportamiento esperado etiquetado en `tests/fixtures/rated_eval_v1.yaml`. Dos tiers:

| Tier | Descripción | Modo | Entradas | Precisión |
|---|---|---|---|---|
| **Parser** | Extracción de params sin follow-up ni red | `--mode parser` (default) | 38 | 38/38 = 100% |
| **Pipeline** | Follow-up + ejecución real contra SECOP | `--mode pipeline --live` | 26 | Pendiente medición |

**Hallazgo clave — re-encuadre de precisión:**

El 26% era end-to-end histórico. Separando tiers:
- **Parser-tier 85%:** De 25 bugs calificados 👎, 19 ya estaban arreglados (rangos compuestos, `filtra`, saludos, `esten`). Nadie lo sabía por falta de medición.
- **Pipeline-tier:** El desplome real está en follow-up poisoning y recencia ("priorizó procesos viejos"). Ahí se concentran los 👎 restantes.

**6 bugs de parser — 5 resueltos, 1 reclasificado (verificación 2026-06-03):**

| ID | Bug | Tipo | Estado |
|---|---|---|---|
| RATED-016 | `oferta` se cuela en objeto | scrub | ✅ Resuelto (stopwords) |
| RATED-040 | `vias` se pierde con "...teusaquillo sobre vias" | scrub/entity | ✅ Resuelto (word boundary) |
| RATED-041 | `oficial` del `(oficial)` de UI se cuela | scrub | ✅ Resuelto (stopwords) |
| RATED-059 | `software` expande a `tecnología` | morfología | ✅ Resuelto (morpho fix) |
| RATED-067 | `o similares` inyecta `valor_max=1.000.000` | parser rango | ✅ Resuelto (regex \b + stopwords) |
| RATED-069 | `El Banco` (municipio) → tema `banco` | gazetteer/LLM | 🔄 Reclasificado pipeline-tier |

RATED-069 no se puede resolver con regex (rompería "Banco Agrario", "Banco de Bogotá", "Banco Mundial"). Es caso canónico para la capa de desambiguación LLM.

**Piso de regresión:** `tests/test_rated_eval.py` — el piso parser-tier no puede bajar de 38/38 ni las 👍 pueden regresar.

**Divergencia SQLite/JSONL:** El dashboard lee de SQLite (70 calificadas). El JSONL (`data/feedback.jsonl`) tiene solo 42. El harness usa SQLite como fuente de verdad. Ver `scripts/eval_rated.py` para detalles.

### 10.3 Corpus de regresión del parser

- `tests/fixtures/query_corpus_v1.yaml` — 88 entradas.
- `scripts/run_query_corpus.py` — runner con modos `parser|workflow|all` y filtros por severidad.
- Convenciones en `docs/QUERY_CORPUS.md`.
- Estado: 89/89 PASS (incluye corpus dialogue 61/61).

### 10.4 Matriz mínima de follow-up

| # | Consulta base | Follow-up | Esperado |
|---:|---|---|---|
| 1 | procesos abiertos de alimentación en Atlántico 2026 | muéstrame más | paginación, conserva filtros |
| 2 | procesos abiertos de alimentación en Atlántico 2026 | los de mayor valor | conserva filtros + orden valor desc |
| 3 | procesos abiertos de alimentación en Atlántico 2026 | solo 2025 | conserva filtros + cambia año |
| 4 | procesos abiertos de alimentación en Atlántico 2026 | ahora en Bolívar | conserva tema + cambia departamento |
| 5 | procesos abiertos de alimentación en Atlántico 2026 | ahora en Medellín | conserva tema + cambia ciudad/scope |
| 6 | contratos de adulto mayor en Barranquilla | ahora procesos abiertos | cambia dataset + conserva tema/scope |
| 7 | procesos de mantenimiento en Cali | contratos en ejecución | cambia dataset + estado activo |
| 8 | consulta con sugerencias | 1 | aplica sugerencia guardada |
| 9 | sin historial | muéstrame más | pide contexto |
| 10 | cualquier búsqueda | reset | limpia historial |

### 10.5 Test crítico de estados

```text
Base:
contratos de mantenimiento en Bogotá

Follow-up:
muéstrame más contratos que estén firmados o en ejecución

Debe:
- heredar mantenimiento
- heredar Bogotá
- excluir esten
- excluir Cerrado
- usar estados activos
```

---

## 11. Comandos operativos útiles

### 11.1 Levantar backend

```bash
uvicorn app.main:app --reload
```

### 11.2 Levantar Streamlit

```bash
streamlit run streamlit_app.py
```

### 11.3 Compilar core

```bash
python3 -m py_compile app/core/*.py
```

### 11.4 Ejecutar tests

```bash
pytest -q
```

### 11.5 Buscar archivos duplicados accidentales

```bash
find app/core -name "* v*.py" -print
find app/core -name "*copy*.py" -print
find app/core -name "*backup*.py" -print
```

### 11.6 Verificar integración follow-up

```bash
grep -R "detect_and_merge" -n app/core/orchestrator.py app/core/followup_engine.py
grep -R "classify_turn\|merge_query_frames" -n app/core/orchestrator.py
```

---

## 12. Riesgos conocidos y causas raíz estructurales

### 12.1 Riesgos operativos

| Riesgo | Impacto | Mitigación |
|---|---|---|---|
| Confundir estados SECOP | Respuestas jurídicamente equivocadas | Política de estados documentada + tests E2E |
| Tokens basura en objeto | Consultas irrelevantes con `LIKE '%basura%'` | Stopwords + dialogue_policy (reglas P6/P7) |
| Follow-up hereda parcialmente | Resultados fuera de intención | `followup_engine` + matriz regresión |
| Versiones paralelas en `app/core` | Agente edita archivo equivocado | `make lint-core` + archive/ |
| Consulta global accidental | Miles de resultados irrelevantes | Guard anti `WHERE 1=1` en builder + orchestrator |
| Relajación automática excesiva | Resultados fuera de alcance | En follow-up, sugerir opciones antes de relajar |
| Timeout SECOP | Mala UX | Retry controlado + count-lazy (ADR-008) |
| LLM alucina narrativa | Lectura rápida inventa cifras | Validación de grounding monetario en `narrator.py` |
| `DEPLOY-TZ-001` — falta `tzdata` en el entorno | `ZoneInfo("America/Bogota")` se resuelve al importar `opportunity_retrieval_policy`: sin base de zonas horarias el arranque falla | Garantizar `America/Bogota` en todos los entornos soportados (microbloque de deployment/hardening). **No** se mitiga tocando la lógica de oportunidades |
| `SEMANTIC-AUTHORITY-CLEANUP` — `MORPHOLOGICAL_VARIANTS` mezcla autoridades | Morfología, equivalencia, alias, jerarquía y asociación viven en una sola tabla: el router convierte `nutricion` en el root `alimentacion` y pierde la identidad explícita; el builder abre un OR donde la relación asociativa adquiere autoridad de equivalencia. `RUNTIME_BUG_CONFIRMED` en `docs/audits/2026-08-22_SEMANTIC_AUTHORITY_AUDIT.md` | Separar autoridades (morfología / semántica de dominio / semántica de consulta) en un microbloque propio. Deuda **abierta**, no abordada por ADR-062: el cierre del spike semántico la documenta pero no la corrige |

### 12.2 Causas raíz estructurales (revisión integral junio 2026)

La revisión integral de arquitectura identificó 5 causas raíz que explican por qué los bugs del Bucket B (semánticos) siguen apareciendo a pesar de 1212 tests en verde:

**Causa 1 — El gate `_needs_llm` apaga el LLM de facto.** `query_router.py` solo dispara el LLM en 2 casos: (a) lugar no resuelto que choca con objeto, y (b) palabra tipo-entidad en objeto. Resultado: 17 de 2437 queries usaron LLM (0.7%). El resto va por heurística pura — incluyendo `banco magdalena`, `aires acondicionados`, `o similares`, `celebraron`. El LLM (DeepSeek, JSON estructurado) está construido y conectado, pero gateado detrás de un `if` que casi nunca es verdadero.

**Causa 2 — `QueryFrame.raw_params` con merge ciego (frame leak).** Las políticas de merge en `followup_engine.py` hacen `raw_params={**prev.raw_params, **curr.raw_params}`. El QueryFrame tiene campos tipados (`scope`, `topic`, `dataset`) que las políticas cuidan, pero `raw_params` es un canal paralelo no tipado que arrastra TODO (`entidad_ecosystem`, `intent_type`, `estado_family`) por la puerta de atrás. Resultado: `WHERE departamento='Cesar' AND nombre_entidad LIKE '%CUNDINAMARCA%'` — lógicamente imposible. Dos representaciones del estado sincronizadas a mano = el bug.

**Causa 3 — El objeto es bag-of-tokens con AND por defecto.** `_extract_object_terms` parte el texto en tokens, filtra stopwords y los une con AND. `aires acondicionados` → `%aires% AND %acondicionados%` → 0 resultados. No hay noción de frase ni relevancia. `test_multi_and_demotion.py` congela este comportamiento como correcto (ver ADR-014 para el refinamiento parcial con OR merge).

**Causa 4 — Sin capa de relevancia/sanity semántico.** `QueryPlan` valida estructura (¿falta slot?, ¿WHERE 1=1?), no semántica. Nada pregunta "¿`banco` aquí es lugar o tema?", "¿`valor_max=1.000.000` para 'parques' tiene sentido?". El propio `SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md` (mayo 18) ya lo diagnosticó: "Relevance layer: No existe."

**Causa 5 — Los tests prueban la implementación, no el producto.** 1212 tests verdes con 26% de precisión real. Eso solo es posible si los tests no miden lo que importa. Ejemplos: `test_module_size_limits.py` (disciplina de proceso, no corrección), `test_multi_and_demotion.py` (congela comportamiento defectuoso), decenas de tests que validan rutas internas con params sintéticos. El eval harness (`scripts/eval_rated.py`) cierra esta brecha al medir sobre queries reales calificadas.

---

## 13. Decisiones de diseño vigentes

El registro completo de decisiones está en [docs/decisions_log.md](decisions_log.md)
(formato ADR; el máximo vigente es **ADR-061**). La numeración tiene huecos:
algunos números quedaron reservados en worktrees paralelos y **ADR-054** y
**ADR-059** viven además como archivo propio bajo `docs/decisions/`. Al asignar
un ADR nuevo, el número sale del **máximo global** sobre todos los worktrees, no
del máximo de este archivo.

Esta sección antes contenía un mirror (D1–D21) que se eliminó para evitar drift entre las dos numeraciones. El log de decisiones es ahora la fuente única de verdad.

---

## 14. Roadmap

### Fase A — P0 hardening post-auditoría

**Estado:** cerrado en 2026-05-15.

- INV-005 defendida en código y tests.
- `app/core/` limpio y protegido por `make lint-core`.
- Tests de regresión para política de estados y coherencia `COUNT`/`SELECT`.
- Documento técnico actualizado con verificación empírica de `jbjy-vk9h`.

### Fase B — OBS-001 logger semántico / telemetría operacional

**Estado:** pendiente.

- Añadir `trace_id` a logs y JSONL.
- Registrar tiempos por etapa: parse, resolve, SoQL build, count, query, observe, narrate, total.
- Medir tasa LLM, tasa de degradación, timeout/retry y distribución de `intent_type` / `followup_intent_type`.
- Crear `scripts/analyze_traces.py` para rollups semanales.

### Fase C — Frame leak: sellar la fuga del follow-up (Causa 2)

**Estado:** próximo foco. Derivado de la revisión integral junio 2026.

- Eliminar el merge ciego `{**prev.raw_params, **curr.raw_params}` en `followup_engine.py`.
- Derivar `raw_params` de campos tipados o whitelistear claves que sobreviven a cada intent.
- Extender eval harness a tier pipeline (`--live`, multi-turno) para medir el fix.
- Bugs objetivo: DOGFOOD-FOLLOWUP-007 (Cundinamarca→Cesar poisoning), RATED-043/050/056/058/062.

### Fase D — Shadow LLM para follow-up

**Estado:** diseño, no implementado.

- Ejecutar LLM en paralelo (async, no bloquea) sobre follow-ups.
- Comparar output LLM vs determinístico → medir divergencia.
- Acumular evidencia antes de flipear el default.
- Columna `shadow_llm_params` en trazas para almacenar.

### Fase E — RATED-069: desambiguación banco=municipio (requiere LLM)

**Estado:** único bug parser-tier remanente, reclasificado pipeline-tier.

- `El Banco` es municipio de Magdalena, pero "banco" es polisémico (Banco Agrario, Banco de Bogotá, Banco Mundial, banco de alimentos).
- Un fix regex `del banco`→ciudad rompe toda esa clase.
- Caso canónico para shadow LLM: desambiguar entidad geográfica vs tema contractual.
- Los otros 5 bugs parser (RATED-016/040/041/059/067) fueron resueltos silenciosamente por sprints mayo 2026.

### Fase F — Relevancia y ranking

Después del follow-up:

- Mejorar ranking por relevancia textual.
- Separar coincidencia directa vs coincidencia probable.
- Evitar que resultados de baja pertinencia aparezcan arriba solo por fecha/valor.

### Fase G — Limpieza imports legacy

- Revisar imports no usados en `orchestrator.py`.
- Migrar 3 tests legacy del shim.
- Marcar `query_frame.classify_turn()` como legacy si ya no opera en producción.

### Completado recientemente

- **ADR-023 Orchestrator Split:** 1549 → 196 LOC, workflow/ extraído (2026-05-27).
- **ADR-022 Chips contextuales adaptativos:** thresholds por percentil, sidebar sticky (2026-05-26).
- **ADR-020 Entity Ecosystem Bridge:** ICBF + SENA fuera del gazetteer (2026-05-26).
- **ADR-017 Dogfooding UX fixes:** URLs, fechas, refinamiento, rangos, CASE WHEN (2026-05-25).

---

## 15. Glosario

| Término | Definición |
|---|---|
| SECOPPAL | Asistente conversacional para consulta y análisis SECOP. |
| Dataset procesos | Dataset de procesos/convocatorias SECOP II. |
| Dataset contratos | Dataset de contratos registrados SECOP II. |
| Follow-up | Consulta que depende del turno anterior. |
| Scope | Alcance territorial o entidad: ciudad, departamento, entidad. |
| Topic / objeto | Tema contractual buscado en objeto/descripción. |
| Modifier | Fecha, valor, estado, orden, modalidad, contratista. |
| Estado family | Agrupación semántica de estados SECOP. |
| Guard anti WHERE 1=1 | Protección contra consultas sin filtros reales. |
| Advisor response | Política de respuesta que interpreta y recomienda, no solo lista. |

---

## 16. Apéndice A: Hitos históricos (2026-05-15 a 2026-06-04)

### 2026-06-04 — Fechas relativas semana/día + Entidad robusta + Firmado fix + Paginación A-buffer

**Fechas relativas (ADR-025, commit `4ac717c`):** Extendida la capa temporal de DialoguePolicy + QueryPlan para soportar expresiones de semana/día: "hoy", "ayer", "esta semana", "la semana pasada", "últimos N días". Nueva señal `temporal_range` + regla P5b (espejo de P5). 15 tests. +7 tokens a STOPWORDS. Sin tocar P5/temporal_month.

**Entidad robusta a trailing tokens (commit `91a7e61`):** Shrink-fallback en `rewrite_entity()` de `entity_types.py`. Cuando la captura completa no resuelve en el gazetteer, se reintenta con spans progresivamente más cortos (prefijo válido más largo). Robusto a CUALQUIER token trailing (temporal, tema, basura). 10 tests. + STOPWORDS: `mes, meses, hecho, hizo, hace, hacen, haciendo, gastado`.

**Firmado fix (ADR-026, commit `a745ac8`):** Corregida la semántica de "firmado" que mapeaba erróneamente a `estado=Aprobado` (tiraba ~97% del recall). Validado contra SECOP real: 8 estados tienen ~100% de fecha_de_firma, 4 tienen <0.5%. "Firmado" = `fecha_de_firma IS NOT NULL`, no un valor de estado. Quitado el synonym falso de `estados.py`. 15 tests. + `firmada/firmadas` a STOPWORDS.

**Paginación A-buffer (commit `994337b`):** Eliminados duplicados entre páginas de resultados con buffer de IDs ya vistos.

### 2026-06-03 — Revisión integral de arquitectura + eval harness + verificación de bugs

**Revisión integral:** Se identificaron 5 causas raíz estructurales que explican el "parche infinito" (documentadas en §12.2). Hallazgo principal: el parser está en 100% de precisión (38/38 sobre queries reales calificadas).

**Eval harness:** Creado `scripts/eval_rated.py` + `tests/fixtures/rated_eval_v1.yaml` (65 entradas etiquetadas) + `tests/test_rated_eval.py` (piso de regresión). Corre las 70 queries calificadas desde SQLite. Dos tiers: parser (100%) y pipeline (pendiente --live).

**Verificación de bugs:** 5 de 6 bugs parser-tier ya estaban resueltos por sprints mayo 2026 (RATED-016/040/041/059/067). RATED-069 (`El Banco` municipio) reclasificado a pipeline-tier — requiere desambiguación LLM. Parser-tier: 38/38 = 100%.

**Documento técnico actualizado:** Consolidación de secciones duplicadas, header con métricas reales, §12 ampliado con causas raíz, §14 roadmap actualizado con foco en frame leak + shadow LLM.

### 2026-05-27 — Orchestrator Split (ADR-023)

`orchestrator.py`: 1549 → 196 LOC (−87%). Refactor en 8 pasos atómicos con golden tests. Workflow extraído a `workflow/` (actions, handlers, graph, helpers, deps, context, persistence, results). Suite: 1212 passed, 23 xfailed. Corpus: 89/89 PASS. Ver ADR-023 y §5.1 para estructura resultante.

### 2026-05-27 — Dogfooding: 3 bugs documentados

DOGFOOD-FOLLOWUP-007 (follow-up poisoning Cundinamarca→Cesar), DOGFOOD-RANGE-007 (rangos "más de X y menos de Y"), DOGFOOD-MORPHO-007 (software→tecnología). RANGE-007 cerrado en `ecf94de`. Los otros dos vivos como Causa 2 y RATED-059 respectivamente.

### 2026-05-26 — Entity Ecosystems: ICBF + SENA (ADR-018/019/020)

ICBF (32 regionales, 1:1 por depto) y SENA (89 entidades, 7 tipos, 616K volumen) como ecosistemas institucionales fuera del gazetteer. `entity_ecosystem_router.py` como capa independiente post-parser, pre-resolver. Patrón `contains_all` para SENA multi-entidad. Corpus: 89/89 PASS. Suite ecosystem: 205 passed, 13 xfailed.

### 2026-05-25 — Dogfooding UX fixes (ADR-017)

6 fixes en sesión de trabajo: URLs CO1.REQ vs CO1.NTC (URL-005), fechas con cascada de fallback (DATE-002), refinement buttons funcionales (REFINE-004), rangos "entre X y Y" con herencia de unidad (RANGE-005), "celebrar" como action verb (ACTION-005B), eliminación de CASE WHEN en ORDER BY (SOQL-006). 945 passed, 10 xfailed.

### 2026-05-24 — Sprint 6 Fase 5: smoke gate (ADR-013/014)

ENTITY-MULTI-REGIONAL-002: `unaccent()` obligatorio en todas las cláusulas LIKE de entidad. MULTI-AND-DEMOTION: OR merge para 3+ rare terms en objeto. Suite: 830 passed. Ambos validados contra API real.

### 2026-05-18 — Robustecimiento sistémico (en pausa)

Plan aprobado en `docs/SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md`. Baseline M0: 579 passed, corpus 88/88. Bloqueado por falta de queries humanas para `dialogue_policy_corpus.yaml`. Ver ADR-006.

### 2026-05-18 — Reconciliación main + cierre CORPUS-002-HIGH

Main reconciliado desde worktree vía cherry-picks quirúrgicos. Corpus: 88/88 PASS. Suite: 579 passed, 3 xfailed. Torture matrix: 12/12. Pendiente: DEMO-BLOCKERS-001.

### 2026-05-17 — Query Corpus V1 + analytical queries (AQ-001A, ADR-005)

Corpus de 88 queries vivas. Ruta analítica determinística (`aggregate_sum`). `detect_value_anomaly()` para valores atípicos en fuente (Luruaco $250MM vs $250B). Bidder intent: "para presentarme", "quiero vender X". Suite: 596 passed, 1 xfailed.

### 2026-05-16 — Auditoría P0 cerrada

4 bugs críticos: `/chat` sin chat_id, `degrade_query` sin recalcular count, regex narrator con falsos positivos, `_is_signed_query` borraba estados reales. Suite: 466 passed. Regla semántica: "firmados" solo fuerza dataset contratos, nunca borra filtros de estado (ADR-008).

### 2026-05-15 — Documento técnico creado

Primera versión de este documento. Auditoría H1: bug `firmado=Celebrado` vivo en camino LLM. ADR-006 erosionada (archivos `.bak` en `app/core/`). Limpieza + `make lint-core` + tests de integridad.

### 2026-05-17 — Oportunidades SECOP: estado, dedupe y presentación

Se ajustó la política de oportunidades para evitar doble filtrado de estados y mejorar la salida visible de procesos. El caso guía fue una oportunidad de mantenimiento correctivo/preventivo de parque automotor en Norte de Santander que no aparecía por combinación restrictiva de estados.

Validación:
- tests Pamplonita: 4/4
- tests B1-B7: 19/19
- torture matrix verde
- feedback.jsonl intacto

### 2026-05-17 — OPP-003: intent_type explícito + G5 timeout específico

Se elevó `intent_type=opportunity_search` a primera clase:
- Emisión desde intent_vocabulary (bidder intent) y query_router.
- Round-trip en QueryFrame.
- Tests E2E con chat_id.
- Timeout UX específico para oportunidades (mensaje + sugerencias de lugar/ventana).

Validación: 567 passed, 0 fallos, torture matrix verde.

### 2026-05-17 — Bidder intent y rangos monetarios reales

Se corrigieron patrones detectados en pruebas manuales:
- rangos monetarios compactos (VALUE-RANGE-001);
- “para poder presentarme” (BIDDER-INTENT-002);
- “quiero vender/ofrezco/soy proveedor de X” (BIDDER-INTENT-003).

Estos casos fortalecen `opportunity_search` y el smoke test de producto.


### 2026-05-17 — Query Corpus V1 + LLM-OPP-001 + PTAR-VARIANTS-001

- Se agrego corpus vivo de 99 queries para depuracion sistematica (docs/QUERY_CORPUS.md).
- Se implemento `enforce_bidder_opportunity_policy` como modulo independiente.
- Variantes PTAR/STAR/aguas residuales colapsadas a raiz unica.
- 22 corpus FAIL conocidos pasan a backlog en docs/CORPUS_BACKLOG.md.
- Pendiente CORPUS-002 para cerrar los 5 critical FAIL.

### 2026-05-17 — VALUE-SANITY-002A: valores atípicos en datos abiertos

Se detectó que algunos registros del dataset Contratos (`jbjy-vk9h`) traen `valor_del_contrato` inflado frente a `valor_facturado`. Caso guía: Luruaco/CD-1028-2025, donde `valor_del_contrato` aparece como $250.000.000.000 y `valor_facturado` como $250.000.000.

Decisión:
- No modificar silenciosamente el valor fuente.
- Marcar el resultado como valor atípico con `detect_value_anomaly()`.
- Mostrar referencia de `valor_facturado` cuando exista.
- Advertir si el mayor valor mostrado tiene inconsistencia de fuente.

Validación:
- 12 tests nuevos.
- Suite: 596 passed, 1 xfailed.
- Torture matrix verde.

### 2026-05-18 — CORPUS-001-B + CORPUS-002 — corpus de regresión sobre el parser

Se agregó un corpus de queries reales (`tests/fixtures/query_corpus_v1.yaml`,
88 entradas) y un runner (`scripts/run_query_corpus.py`) con tres modos
(`--mode parser|workflow|all`) y filtros por `--severity`, `--id` y
`--limit`. Convenciones en `docs/QUERY_CORPUS.md`.

Sobre ese corpus se cerraron los 4 críticos iniciales:
- CORPUS-R005: `convocadas` ahora activa `oferta_abierta` vía
  `estado_families.NATURAL_TO_FAMILY`.
- CORPUS-F006 / CORPUS-OC001: `estado_family=oferta_abierta` sobre
  `dataset=procesos` se promueve a `intent_type=opportunity_search`.
- CORPUS-R018: el heurístico "más + filtro nuevo → refine_filter" ya no
  degrada follow-ups que introducen un topic completamente nuevo
  (helper `_topics_overlap` en `followup_engine.py`).

Pase tras los fixes: 82/88; corpus crítico 5/5.

### 2026-05-18 — SAFE-SOQL-001 — Defensa interna anti consultas globales

SoQLBuilder ya no emite `WHERE 1=1` salvo `allow_global=True` explícito.
El guard del orchestrator sigue siendo la primera defensa; el builder
queda como segunda defensa.

Un filtro "substancial" es scope, topic, familia de estado o modalidad.
fecha/valor/orden por sí solos no califican. Tests en
`tests/test_safe_soql.py` cubren los 6 escenarios pedidos más opt-in y
filtros sustanciales individuales. `observer.py` opta por
`allow_global=True` porque opera sobre un universo ya validado.

Validación: 534 passed, corpus crítico 5/5, feedback.jsonl intacto.

### 2026-05-18 — Reconciliación main + cierre CORPUS-002-HIGH

Se reconcilió main desde el worktree `claude/zealous-yalow-296fb6` vía
rama de rescate `rescue/dirty-secoppal-2026-05-18` y cherry-picks
quirúrgicos. Resultado final:

- **VALUE-SANITY-002A:** `detect_value_anomaly()` marca valores atípicos
  en dataset Contratos sin alterar el dato fuente. ADR-005 registrada.
- **LLM-OPP-VERIFY-001:** `opportunity_policy.py` reintroducido como
  enforcement post-LLM para `intent_type=opportunity_search` en queries
  de proponente.
- **PTAR-VARIANTS-001:** 7 variantes PTAR/STAR/aguas residuales colapsadas
  a raíz `tratamiento_aguas_residuales`.
- **TEST-HYGIENE-001:** 1 xpass eliminado, xfail actualizados.
- **CORPUS-003A:** V005 ("o más") y V007 ("mil millones") cerrados.
- **CORPUS-003B:** OC004/OC005 (bidder intent indirecto) cerrados.
- **CORPUS-003C:** R003 (`limpiar→limpieza`) cerrado.
- **AN003:** cherry-pick para analytical followup "cuánto suma".

**Validación final:**
- Suite: 579 passed, 3 xfailed.
- Corpus: 88/88 PASS, 0 FAIL.
- Torture matrix: 12/12 invariantes.
- feedback.jsonl intacto.

**Pendiente registrado:** DEMO-BLOCKERS-001 — 8 tareas UX/estado/scope (COMMAND-SLOT,
OPP-UX, OPP-STATE, OPP-SUGGEST, OPP-RELEVANCE, OPP-TIMEOUT-001,
STATE-PRIORITY-001, OPP-SCOPE-001). Prioridad HIGH.

### 2026-05-18 — VALUE-TYPO-001 registrado en backlog

Typo "milloones" → "millones" debe normalizarse. Si valor_min > valor_max
(1B > 5M) no ejecutar consulta, pedir aclaración. "milloones" no debe
entrar como objeto contractual ni generar SoQL con LIKE '%milloones%'.

### 2026-05-24 — Sprint 6 Fase 5 smoke gate: 3 bugs fixados

**MORPHO-VERIFY (29ac24b):** MORPHO-EXPANSION-INCONSISTENT-001 cerrado.
Verificado que ambas rutas (heuristic_only y heuristic_plus_llm) convergen
en `_object_term_conditions()` que usa ROOT_TO_VARIANTS. Falso positivo.
3 tests nuevos en test_morpho_expansion_consistency.py.

**ENTITY-MULTI-REGIONAL-002 (b39a138):** Descubierto en smoke D-bucket.
LIKE nombre_entidad sin unaccent() + entidad_resolved sintética con tildes
inexistente en SECOP. Fix: `_accent_fold_like` en todas las cláusulas
entidad + `entidad_like` con nombre canónico corto. resolve_regional()
ahora retorna tuple (regional, canonical). ADR-013.

**MULTI-AND-DEMOTION (dd0b2bf):** Descubierto en smoke B-bucket (0/10).
4+ rare terms en AND colapsaban queries de frases técnicas. Fix: OR merge
cuando rare_terms ≥3. BM25 reordena post-query. ADR-014.

**Smoke gate Fase 5:** 6 intentos diurnos fallidos por API saturada
(10-30s/query en horario 2-4pm Colombia). Cronjob cdb0c2867f32 programado
lunes 5am (10:00 UTC) con no_agent=true y scripts/smoke_burst.py.
Target ≥45/50. API solo responde <2s en madrugada.

**Validación:** Suite 830/0. Ambos fixes validados con queries individuales
contra API real (0→50 results). CORPUS_BACKLOG actualizado (1 bug vivo).
decisions_log: ADR-013, ADR-014.

### 2026-05-25 — Dogfooding UX fixes (A-E) + CASE WHEN BadRequest

Dogfooding con query real "procesos abiertos hoy en Cundinamarca por mas de
1000 millones" reveló 5 bugs críticos de UX. Fixes aplicados en sesión de
trabajo con tests 945 passed.

**Fix A — Refinement buttons (DOGFOOD-REFINE-004):**
- `orchestrator.py:_run_refinement()` ejecuta query refinada vía bypass del
  pipeline SoQL (no re-parsea query natural).
- `streamlit_app.py` consume `_pending_refinement` del session_state y pasa
  `seed_resolved_params` a `run_query()`.

**Fix B — URLs por tipo de proceso (DOGFOOD-URL-005):**
- `formatter.py:_safe_url()` + `secop_client.py:_normalize_row()` detectan
  prefijo CO1.REQ.X vs CO1.NTC.X.
- REQ → "" (pre-publicación, SECOP devuelve "argument out of range").
- NTC → reconstruir `OpportunityDetail/Index?noticeUID=`.
- UI: "Vista pública no disponible (proceso pre-publicación)".

**Fix C — Fecha fallback cascada (DOGFOOD-DATE-002):**
- `formatter.py:_best_date_for_row()` busca: publicacion_del → publicacion_fase_3
  → recepcion_respuestas → apertura_efectiva.
- Cada fecha trae etiqueta indicando fuente.
- Todas null → "Pendiente de publicación".

**Fix D — Rango "entre X y Y" (DOGFOOD-RANGE-005):**
- `query_router.py:_extract_amounts()` hereda unidad atómica vs compuesta según
  si el lado con unidad tiene dígito.
- Ej: "entre 500 y mil millones" → min=500M, max=1000M.

**Fix E — Header desactualizado (colateral):**
- Con refinement bypass, el header se reconstruye desde params nuevos en cada
  turn. Auto-resuelto.

**Fix estructural — CASE WHEN en ORDER BY (DOGFOOD-SOQL-006):**
- `CASE WHEN estado_contrato IN (...) THEN 0 ELSE 1 END` en jbjy-vk9h causaba
  400 Bad Request "Unexpected character" (parser interno Socrata no soporta
  strings acentuados en CASE).
- Fix: eliminar CASE WHEN del ordering SoQL. Boost "activos primero" movido a
  post-procesamiento Python (`formatter._reorder_active_first`).
- Impacto colateral: timeouts sistemáticos en contratos atribuidos a "API lenta"
  desaparecieron (el CASE WHEN era la causa real, no la red).

**Tests actualizados:** test_url_repair.py reescrito para nueva política URL.
**Validación:** 945 passed, 10 xfailed.
**Documentación:** ADR-017, DOGFOOD_2026-05-25.md, funnel-full-ux-fixes-2026-05-25.md.
**Pendientes post-dogfooding:** F (trace vacío follow-up), G (gramática "5 resultados").

### 2026-05-27 — Orchestrator Split: de god module (1549 LOC) a fachada delgada (196 LOC)

**Problema:** `app/core/orchestrator.py` concentraba 1549 líneas con acciones Burr, grafo, handlers de casos especiales, flujo normal, persistencia y helpers. Cada feature nueva tocaba orchestrator.

**Solución:** Refactor en 8 pasos atómicos con golden tests (9 contratos de comportamiento observable) como red de seguridad:

1. **Paso 0:** Golden safety net (9 tests de contrato para `run_query()`).
2. **Paso 1:** Extraer helpers puros a `workflow/helpers.py`.
3. **Paso 2:** Introducir `WorkflowDeps` + `HandlerContext` (inyección + contexto inmutable).
4. **Paso 3:** Staging de `workflow/results.py` + `workflow/persistence.py`.
5. **Paso 4:** Mover grafo Burr a `workflow/graph.py`.
6. **Paso 5:** Extraer 12 acciones Burr a `workflow/actions/` (parsing, planning, execution, rendering).
7. **Paso 6:** Extraer handler de refinamiento a `workflow/handlers/refinement.py`.
8. **Paso 7:** Extraer 5 handlers (reset, pagination, suggestion, direct_lookup, normal_flow) y colapsar `run_query()` a chain-of-responsibility.

**Estructura resultante:**
- `orchestrator.py`: 196 LOC (fachada — init + run_query + shim backward-compat).
- `workflow/actions/`: 656 LOC en 4 módulos (parsing, planning, execution, rendering).
- `workflow/handlers/`: 945 LOC en 6 módulos (reset, pagination, suggestion, direct_lookup, refinement, normal_flow).
- `workflow/graph.py`: 148 LOC (ApplicationBuilder Burr).
- `workflow/helpers.py`: 177 LOC (helpers puros).
- `workflow/deps.py`: 56 LOC (WorkflowDeps).
- `workflow/context.py`: 42 LOC (HandlerContext).
- `workflow/persistence.py`: 87 LOC (persist_trace_and_turn, extract_result_ids).
- `workflow/results.py`: 211 LOC (empty_result, tabular_result, CANONICAL_KEYS).

**Invariantes arquitectónicas (4 tests de lock-in):**
- `graph.py` NO importa de `orchestrator.py`.
- `workflow/actions/*.py` NO importan de `orchestrator.py`.
- `workflow/handlers/*.py` NO importan de `orchestrator.py`.
- Los handlers solo reciben `(WorkflowDeps, HandlerContext)` — no `self` de `SecopalWorkflow`.

**Deuda consciente:**
- 3 tests legacy importan actions/helpers desde orchestrator (shim temporal con `TODO(refactor)`).
- `persist_trace_and_turn` no se adoptó en `normal_flow` ni `suggestion` (requería extender firma con `suggestions=`).
- `tabular_result` no se adoptó en el camino exitoso de `suggestion` (quirk legacy Burr-internal vs canónico).

**Validación:**
- Golden: 9/9 PASS
- Architectural lock-in: 4/4 PASS
- Suite: 1212 passed, 23 xfailed
- Corpus: 89/89 PASS
- lint-core: limpio

**Documentación:** ADR-023.

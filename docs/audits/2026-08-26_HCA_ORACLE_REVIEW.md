# HCA ORACLE REVIEW — adjudicación semántica de los 18 FAIL restantes

Fecha: 2026-08-26. **HCA_ORACLE_REVIEW: PASS** (proceso de revisión completado; no significa producto aprobado). **18_CASES_ADJUDICATED: 18**. Tres reciben explícitamente `INSUFFICIENT_EVIDENCE`; no se fuerza un veredicto de producto. **NEXT: READY_FOR_HCA_FINAL_FIX_SPRINT**. No se inicia ese sprint ni se aplica el plan de oracle.

## 1. Alcance, base y garantías

Base canónica/HEAD_BEFORE del worktree: `0a77070ec6be5aab82360a6b10e8b3c77a1048ac`. Rama: `codex/hca-oracle-review`. El checkout original estaba en `df3cc9625ce56ed88b0dcd62a95911b0a798743f` (`main`, dos commits de tooling posteriores); se dejó intacto, incluidos sus archivos no rastreados. Worktree de esta revisión: `/Users/rodrigoortiz/Documents/secoppal-hca-oracle-review`. HEAD_AFTER/COMMIT es el commit docs-only que contiene este informe, informado también en la entrega final; no se mezcla con la base auditada.

Corpus: `tests/fixtures/audits/conversation_holistic_v1.yaml`, SHA-256 antes/después `262c225c7939f04516ee95b3c77b4911f9f476058941a88a61d01c918499ffba`. **Byte-idéntico**. No se modifican runtime, tests, fixtures, corpus de diálogo/query ni ADRs. QueryPlan conserva **299/300 LoC G8** (371 líneas físicas; G8 excluye blanco y comentarios). No merge, no push.

Se preservan los cierres DIRECT_LOOKUP_HIJACK, NORMALIZATION_CONTRACT, DIMENSIONAL_NEGATION_V1, TIME_SEMANTICS_V2, CONTROL_LANGUAGE_V1 y ACTOR_HYGIENE_V1. Encontrar fallos ajenos o incompletos no reabre indiscriminadamente esos frentes.

## 2. Método ciego y límites de evidencia

La primera corrida se guardó sin abrir sus trazas. Un extractor leyó sólo IDs/status para seleccionar el conjunto y mostró query, nota y oracle. No mostró `fails`, `final_frame`, respuesta ni SoQL. Las columnas de fallo real de la tabla inicial se completaron **después** del sello A; de otro modo el propio texto del fallo revelaría runtime y rompería la ceguera.

Fase A sellada a `2026-08-26T11:16:59.184976+00:00`, primero `HCA-ACTOR-004`, luego los otros 17: [phase_a_blind.json](evidence/2026-08-26_HCA_ORACLE_REVIEW/phase_a_blind.json). SHA-256 `575e7916496c4c91c41fe092ee039f0ff412e8e296165a042c2353599054d66d`. No se reescribió después del contraste. La entrada TOPIC-012 contiene una errata numérica de referencia: «ADR-031 geografía» debe leerse **ADR-028**; la intención y el dictamen ciego se mantienen originales.

Orden aplicado: intención razonable → decisiones canónicas → invariantes → contratos Frame/Plan → conducta deseada → implementación → oracle. Las hipótesis del encargo no sustituyeron esta jerarquía. En fase B se leyó además el cierre R7; sus conclusiones históricas se trataron como hipótesis, especialmente FOLLOWUP-016.

El runner original recorre el workflow real, parser, follow-up, resolutores y builder; SECOP query/count/aggregate y narrador están stubbeados, LLM externo desactivado, persistencias en sandbox efímero. El índice de actores se abre read-only/immutable; hash verificado igual al cierre R7. La captura adicional delega al runner **sin editarlo**, observa `run_query` y envuelve únicamente sus stubs para registrar argumentos y número de llamadas. Repite **177/18/0**, con status idéntico caso por caso.

**Source call** en este informe significa invocación observada del stub SECOP, no acceso a Socrata vivo. SoQL generado ≠ llamada: los casos sólo-modalidad tienen plan permisivo/SoQL y cero llamadas por el guard final. No se necesitó fuente viva: la revisión resuelve predicados/autoridades, no estima precisión de filas. Las cinco filas sintéticas no respetan necesariamente filtros; no prueban retrieval, orden efectivo de datos, cardinalidad o calidad de resultados. No se reportan como contratos reales ni como defectos por estar bajo un umbral. Rows: **NOT_MEASURED**.

Fuentes de evidencia: [verdicts completos](evidence/2026-08-26_HCA_ORACLE_REVIEW/hca_verdicts.json), [turnos FAIL originales](evidence/2026-08-26_HCA_ORACLE_REVIEW/hca_failed_turns.jsonl), [plan/frame/candidates/source_calls/respuesta completa](evidence/2026-08-26_HCA_ORACLE_REVIEW/runtime_contrast.jsonl), [runs e aislamiento](evidence/2026-08-26_HCA_ORACLE_REVIEW/run_summaries.json), [probes de fronteras](evidence/2026-08-26_HCA_ORACLE_REVIEW/probe_boundaries.json), [manifest y hashes](evidence/2026-08-26_HCA_ORACLE_REVIEW/manifest.json).

## 3. Autoridades canónicas consultadas

| Autoridad | Contrato relevante |
| --- | --- |
| [ADR-001/003/004/006](../decisions_log.md) | Followup único; familia contrato_activo; prioridad de listas; aclaración preejecución. |
| [ADR-024](../decisions_log.md) | Alcance útil distinto de WHERE no tautológico; modalidad/estado/fecha no bastan; P13 pasivo. |
| [ADR-028/040](../decisions_log.md) | Amazonas es departamento; territorio distinto de comprador; valor real ATLANTICO**. |
| [ADR-054 y guía modalidad](../guides/MODALITY_CONVERSATION_GUIDE.md) | CLEAR y deltas ortogonales; no pérdida de filtros independientes. |
| [ADR-059](../decisions/2026-08-18_ADR-059_money-v2-deterministic-authority.md) | Enteros COP; literal 500 mil aplica ×1000; gramática monetaria determinística. |
| [ADR-060 y arquitectura actor](../architecture/ACTOR_PROFILE_V1.md) | Identidad exacta o selección, nunca fuzzy; ambigüedad pide aclaración. |
| [Contrato R7](2026-08-26_HCA_R7_ACTOR_HYGIENE.md) | Strong search vence heurística; alias exacto real puede coexistir con filtros; candidato no autoriza identidad. |
| [ADR-061 y dimensiones](../architecture/SEMANTIC_QUERY_DIMENSIONS.md) | Tipo contractual no duplica tema; deltas independientes conservados. |
| [ADR-064 y negación](../architecture/DIMENSIONAL_NEGATION_SEMANTICS.md) | CITY CLARIFY_ONLY, no comprador positivo desde ciudad negativa. |
| [ADR-065/066](../decisions_log.md) | Autoridad temporal; control no es contenido; reordenar conserva WHERE; top N diferido. |
| [Referencia de estados](../reference/estado_contrato_semantica.md) | Borrador-proceso es no publicado; firmado no equivale a activo/cerrado; no proyección canónica de liquidación. |

## 4. Conjunto congelado: exactamente los 18 FAIL del runner

El orden de esta tabla es el del corpus; la sección individual comienza por actor como calibración. El fallo se copia textualmente del runner y no se interpreta como clasificación.

| CASE_ID | TURN_COUNT | DOMAIN | ORACLE_FAILURE | CURRENT_STATUS |
| --- | --- | --- | --- | --- |
| HCA-TOPIC-007 | 1 | topic_object | turn 1 'procesos del pae en la guajira': objeto lacks 'pae' (objeto=['alimentacion_escolar']) | FAIL |
| HCA-TOPIC-012 | 1 | topic_object | turn 1 'contratos de salud en amazonas': expected execution, got clarification | FAIL |
| HCA-MODALITY-002 | 1 | modality | turn 1 'contratos por seleccion abreviada de menor cuantia en 2026': expected execution, got clarification | FAIL |
| HCA-MODALITY-012 | 1 | modality | turn 1 'procesos de enajenacion de bienes en 2026': expected execution, got clarification | FAIL |
| HCA-ENTITY-001 | 1 | entity_buyer | turn 1 'contratos de la gobernacion del atlantico en 2026': soql contains 'ATLANTICO**' | FAIL |
| HCA-ENTITY-004 | 1 | entity_buyer | turn 1 'que contrato el departamento del atlantico en 2025': missing param entidad_resolved | FAIL |
| HCA-ENTITY-005 | 1 | entity_buyer | turn 1 'contratos de la alcaldia': expected block, got execution | FAIL |
| HCA-MONEY-020 | 1 | money | turn 1 'contratos de papeleria por mas de 100 mil pesos': valor_min=None, expected 100000 | FAIL |
| HCA-MONEY-026 | 1 | money | turn 1 'contratos de papeleria por mas de 1 millon': valor_min=None, expected 1000000 | FAIL |
| HCA-ACTOR-004 | 1 | actor | turn 1 'contratos de lmp ingieneria en 2025': missing param actor_scope | FAIL |
| HCA-STATE-003 | 1 | state | turn 1 'contratos liquidados de obra en 2025': missing param estado_contrato | FAIL |
| HCA-STATE-005 | 1 | state | turn 1 'procesos publicados de aseo en cesar': soql contains 'Borrador' | FAIL |
| HCA-STATE-007 | 1 | state | turn 1 'contratos en ejecucion de la alcaldia de medellin': missing param estado_contrato | FAIL |
| HCA-TYPO-003 | 1 | typo | turn 1 'procesos de regimen especial en 2026': expected execution, got clarification | FAIL |
| HCA-FOLLOWUP-007 | 2 | followup | turn 2 'mejor quita el filtro de modalidad': objeto lacks 'obra' (objeto=[]) | FAIL |
| HCA-FOLLOWUP-016 | 2 | followup | turn 2 'los mas antiguos primero': objeto lacks 'obra' (objeto=[]) | FAIL |
| HCA-META-001B | 1 | metamorphic | metamorphic mismatch vs HCA-META-001A: {'departamento_resolved': (None, 'Atlántico'), 'entidad_resolved': ('DEPARTAMENTO DEL ATLANTICO**', None)} | FAIL |
| HCA-SOURCE-002 | 1 | source_capability | turn 1 'procesos liquidados de obra en cauca': dataset='contratos', expected 'procesos' | FAIL |

## 5. Adjudicación individual — fase A y contraste B

### 5.1. HCA-ACTOR-004

**QUERY / DIALOGUE**

T1: `contratos de lmp ingieneria en 2025`

**ORIGINAL_ORACLE**

```yaml
turns:
- has:
  - actor_scope
  objeto_not:
  - lmp
  - ingieneria
functional_note: sentinel typo LMP INGIENERIA — actor-shaped mention must go to candidates,
  not topic
```

**BLIND_USER_INTENT** — Contratos en 2025 del proveedor cuya mención contiene el typo lmp ingieneria.

**REQUIRED_DIMENSIONS** — ACTOR pendiente; TIME=2025; dataset contratos; no TOPIC derivado del nombre.

**CANONICAL_AUTHORITIES** — ADR-060 §4, §5, §9–10; ACTOR_PROFILE_V1 §7–10; INV-NAME-NO-GUESS.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Mostrar candidatos tipados y pedir selección; no autorizar actor_scope por similitud ni consultar fuente contractual antes de seleccionar.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_CLARIFY`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — INCORRECTO: has actor_scope contradice la propia nota de candidatos y la autoridad exact-only. La ausencia de fuga temática sí es válida.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — CANDIDATES, método fts5_trigram_bm25; LMP INGENIERIA SAS, NIT 900739049, rank 1. No actor_scope ni contratista. Plan actor_name_candidates bloqueado; pide selección y hace cero llamadas.

**SOURCE_BEHAVIOR**

T1: plan=`actor_name_candidates`, should_execute=`false`, aclaración final=`true`; source_calls=0 (query=0, count=0, aggregate=0).

No se envía consulta. SoQL vacío.

**CLASSIFICATION** — `CONFIRMED_ORACLE_DEBT`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `ACTOR_CANDIDATE_NOT_IDENTITY`.

**RATIONALE** — Confirma la calibración ciega: la expectativa de scope autoritativo contradice candidatos sin selección. El ranking no autoriza identidad. No es fallo de recall ni un bug que deba curarse auto-resolviendo fuzzy.

**SHOULD_ORACLE_CHANGE:** YES. **SHOULD_RUNTIME_CHANGE:** NO.

### 5.2. HCA-TOPIC-007

**QUERY / DIALOGUE**

T1: `procesos del pae en la guajira`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  ds: procesos
  objeto_has:
  - pae
```

**BLIND_USER_INTENT** — Buscar procesos del programa de alimentación escolar en La Guajira.

**REQUIRED_DIMENSIONS** — TOPIC=PAE o expansión curada equivalente; GEOGRAPHY=La Guajira; dataset procesos.

**CANONICAL_AUTHORITIES** — ADR-006; ADR-024; ADR-036 (PAE requiere aliases curados); ADR-061.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Preservar el tema y el departamento; no imponer otra entidad ni cambiar la intención. PAE puede expresarse por aliases equivalentes, sin obligar un campo interno particular.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — Ejecución válida. objeto_has pae es demasiado específico si existe expansión semántica equivalente; verificar fuente antes de decidir deuda.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — objeto=[alimentacion_escolar], departamento La Guajira, procesos. Ejecuta una consulta con aliases de alimentación escolar y no usa LIKE %pae%.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `p6dx-8zbt`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE departamento_entidad = 'La Guajira' AND (unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%simat%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%simat%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%comedor escolar%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%comedor escolar%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%comedores escolares%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%comedores escolares%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%restaurante escolar%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%restaurante escolar%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%alimentacion escolar%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%alimentacion escolar%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%refrigerios escolares%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%refrigerios escolares%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%restaurantes escolares%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%restaurantes escolares%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%complemento alimentario%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%complemento alimentario%'))) ORDER BY fecha_de_publicacion_del DESC, precio_base DESC, id_del_proceso DESC LIMIT 50
```

**CLASSIFICATION** — `CONFIRMED_ORACLE_DEBT`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `CANONICAL_TOPIC_ALIAS`.

**RATIONALE** — La intención se representa mediante el concepto canónico y sus aliases; exigir el substring pae en params no mide la conservación del tema. El catálogo excluye %pae% para evitar Páez. La expansión curada existente no se certifica aquí como recall/precision perfecta; la deuda confirmada es exigir la forma superficial.

**SHOULD_ORACLE_CHANGE:** YES. **SHOULD_RUNTIME_CHANGE:** NO.

### 5.3. HCA-TOPIC-012

**QUERY / DIALOGUE**

T1: `contratos de salud en amazonas`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  objeto_has:
  - salud
functional_note: very broad topic should still execute, not clarify
```

**BLIND_USER_INTENT** — Buscar contratos relacionados con salud en el departamento de Amazonas.

**REQUIRED_DIMENSIONS** — TOPIC=salud; GEOGRAPHY=Amazonas; dataset contratos.

**CANONICAL_AUTHORITIES** — ADR-024 alcance útil; ADR-028 geografía regional y precedencia Amazonas; ADR-040; ADR-061.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Tema amplio pero explícito más territorio es alcance suficiente; no reemplazar salud por una entidad sanitaria sin autoridad.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — VÁLIDO semánticamente: debe ejecutar conservando salud y Amazonas.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — El índice devuelve alias exact_unique_alias SALUD, NIT 900659292, y el parser conserva tema salud y Amazonas. P17 emite actor_or_topic_ambiguous, should_execute=false, pregunta explícita tema/proveedor; cero llamadas.

**SOURCE_BEHAVIOR**

T1: plan=`actor_or_topic_ambiguous`, should_execute=`false`, aclaración final=`true`; source_calls=0 (query=0, count=0, aggregate=0).

No se envía consulta. SoQL vacío.

**CLASSIFICATION** — `SAFE_UNSUPPORTED_CAPABILITY`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `EXACT_ACTOR_TOPIC_AMBIGUITY`.

**RATIONALE** — La fase A favorecía ejecución temática sin conocer una colisión exacta. El contraste aporta ese hecho: no es un nombre inventado ni heurística fuzzy. ADR-060 y el contrato R7 permiten que alias exacto y filtros coexistan y prohíben atribución insegura. La aclaración conserva explícitas ambas lecturas y el territorio; no ejecuta actor_scope. Se registra este cambio de supuesto, sin reescribir fase A. SAFE describe la imposibilidad de elegir automáticamente el rol, no una falta de soporte de salud.

**SHOULD_ORACLE_CHANGE:** MAYBE. **SHOULD_RUNTIME_CHANGE:** NO.

### 5.4. HCA-MODALITY-002

**QUERY / DIALOGUE**

T1: `contratos por seleccion abreviada de menor cuantia en 2026`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  has:
  - modalidad_include_procedure_ids
  objeto_not:
  - seleccion
  - abreviada
  - cuantia
```

**BLIND_USER_INTENT** — Contratos por selección abreviada de menor cuantía durante 2026, sin tema ni territorio.

**REQUIRED_DIMENSIONS** — MODALITY=seleccion_abreviada_menor_cuantia; TIME=2026; contratos.

**CANONICAL_AUTHORITIES** — ADR-024 distinción SCOPE_TOPIC_KEYS/NONEMPTY_WHERE_KEYS; ADR-054; ADR-061 §3.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Resolver modalidad y año, sin fuga temática. Bajo la política general vigente, solicitar tema/entidad/lugar/contratista antes de ejecutar una búsqueda sólo-modalidad.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_CLARIFY`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — EXECUTE es prematuro bajo el contrato vigente; demanda válida para una ampliación de scope, no un defecto del resolver de modalidad.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Inclusión seleccion_abreviada_menor_cuantia y año correctos; sin fuga temática. Plan valid_query con unsafe_global_candidate; builder produce SoQL, pero el guard de ejecución aclara y hace cero llamadas.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`true`; source_calls=0 (query=0, count=0, aggregate=0).

No se envía consulta. Existe SoQL construido, pero bloqueado aguas abajo.

**CLASSIFICATION** — `KNOWN_DEFERRED_PRODUCT_DEBT`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `ONLY_MODALITY_SCOPE_POLICY`.

**RATIONALE** — ADR-024 distingue WHERE no tautológico de alcance útil: modalidad y fecha no habilitan búsqueda sin tema/entidad/lugar/contratista. Mismo límite que NEG-N15 VALID_DEFERRED. No hay bug del resolutor; la demanda de ejecutar sólo modalidad exige decisión de producto. Runtime YES es para entregar esa demanda diferida, no para reparar la seguridad vigente.

**SHOULD_ORACLE_CHANGE:** NO. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.5. HCA-MODALITY-012

**QUERY / DIALOGUE**

T1: `procesos de enajenacion de bienes en 2026`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  ds: procesos
functional_note: enajenacion de bienes is a real SECOP modality
```

**BLIND_USER_INTENT** — Procesos por enajenación de bienes en 2026, a escala no acotada.

**REQUIRED_DIMENSIONS** — MODALITY=enajenación; TIME=2026; procesos.

**CANONICAL_AUTHORITIES** — ADR-024; ADR-054; ADR-061 §3 (enajenación reconocida).

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Reconocer la familia de modalidad; solicitar alcance útil. La fecha no sustituye tema/territorio/entidad.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_CLARIFY`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — EXECUTE excede la política de alcance vigente; mantener demanda diferenciada del soporte léxico.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Incluye enajenacion_sobre_cerrado y enajenacion_subasta, procesos, 2026. Plan valid_query/unsafe_global_candidate; aclara alcance en guard final, cero llamadas.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`true`; source_calls=0 (query=0, count=0, aggregate=0).

No se envía consulta. Existe SoQL construido, pero bloqueado aguas abajo.

**CLASSIFICATION** — `KNOWN_DEFERRED_PRODUCT_DEBT`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `ONLY_MODALITY_SCOPE_POLICY`.

**RATIONALE** — Enajenación sí se interpreta. El FAIL sólo exige ejecución de un universo sólo-modalidad que la política general no permite todavía. Se mantiene como capacidad diferida, no como falso negativo léxico ni parche de oracle en este bloque.

**SHOULD_ORACLE_CHANGE:** NO. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.6. HCA-ENTITY-001

**QUERY / DIALOGUE**

T1: `contratos de la gobernacion del atlantico en 2026`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  soql_not:
  - ATLANTICO**
functional_note: canonical value must not carry markdown/asterisks into LIKE
```

**BLIND_USER_INTENT** — Contratos de la Gobernación del Atlántico en 2026.

**REQUIRED_DIMENSIONS** — BUYER=Gobernación del Atlántico; TIME=2026; contratos.

**CANONICAL_AUTHORITIES** — ADR-040: Atlántico se registra como DEPARTAMENTO DEL ATLANTICO** y el comprador usa el valor real.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Filtrar el campo de comprador por el valor real SECOP. No eliminar caracteres del nombre fuente porque parezcan Markdown.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — INCORRECTO: prohibir ATLANTICO** contradice la autoridad de datos documentada. El oracle debe comprobar comprador, no higiene de un supuesto Markdown.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Resuelve comprador DEPARTAMENTO DEL ATLANTICO** y envía LIKE sobre nombre_entidad, con fechas 2026. No lo trata como territorio.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%DEPARTAMENTO DEL ATLANTICO**%')) AND fecha_de_firma >= '2026-01-01' AND fecha_de_firma <= '2026-12-31' ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `CONFIRMED_ORACLE_DEBT`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `SOURCE_BUYER_LITERAL`.

**RATIONALE** — ADR-040 documenta exactamente los dos asteriscos como parte del valor SECOP. El oracle confundió dato de fuente con Markdown. El P0 histórico no es una severidad de runtime confirmada. Limpiar la cadena por estética no es un arreglo autorizado; cualquier presentación escapada es otra superficie.

**SHOULD_ORACLE_CHANGE:** YES. **SHOULD_RUNTIME_CHANGE:** NO.

### 5.7. HCA-ENTITY-004

**QUERY / DIALOGUE**

T1: `que contrato el departamento del atlantico en 2025`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  has:
  - entidad_resolved
functional_note: 'department-as-buyer: gobernación del atlántico ≠ territory filter
  only'
```

**BLIND_USER_INTENT** — Qué compró el Departamento del Atlántico en 2025: el verbo favorece lectura de comprador.

**REQUIRED_DIMENSIONS** — BUYER frente a GEOGRAPHY materialmente distinguibles; TIME=2025; contratos.

**CANONICAL_AUTHORITIES** — ADR-040 INV-7; ADR-006; orden de autoridad del encargo (intención antes que implementación/oracle).

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — No fingir equivalencia territorio=comprador. La intención razonable favorece comprador, pero ADR-040 enruta departamento de X a territorio y no establece excepción del sujeto qué contrató. Requiere decidir esa precedencia; aclarar es una salida segura.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_CLARIFY`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — NO DETERMINADO: has entidad_resolved no basta como autoridad; conflicto entre sintaxis de sujeto comprador y política territorial. No declarar representación territorial plenamente correcta sin resolverlo.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Interpreta departamento como territorio y ejecuta departamento=Atlántico con fechas 2025, sin nombre_entidad. Resumen dice Contratos en Atlántico.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE departamento = 'Atlántico' AND fecha_de_firma >= '2025-01-01' AND fecha_de_firma <= '2025-12-31' ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `INSUFFICIENT_EVIDENCE`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `BUYER_SUBJECT_VS_TERRITORIAL_POLICY`.

**RATIONALE** — La intención razonable de qué contrató el departamento favorece sujeto comprador, mientras ADR-040 manda patrón territorial y no declara excepción para ese verbo. La fase A pidió aclarar esa frontera. No se toma la ejecución territorial como prueba de corrección ni se fuerza una excepción al ADR desde el oracle. Hace falta decisión explícita sobre sujeto comprador; no es el mismo caso que la frase territorial META-001B.

**SHOULD_ORACLE_CHANGE:** MAYBE. **SHOULD_RUNTIME_CHANGE:** MAYBE.

### 5.8. HCA-ENTITY-005

**QUERY / DIALOGUE**

T1: `contratos de la alcaldia`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: block
functional_note: unqualified generic must clarify, not pick arbitrary entity
```

**BLIND_USER_INTENT** — Contratos de una alcaldía no especificada.

**REQUIRED_DIMENSIONS** — BUYER pendiente; contratos; sin territorio ni tema sustancial.

**CANONICAL_AUTHORITIES** — ADR-006 P1; ADR-024 alcance útil; ADR-040 separación comprador/territorio.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Preguntar cuál alcaldía; no seleccionar comprador, usar alcaldía como tema ni ejecutar un universo inventado.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_CLARIFY`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — VÁLIDO: block. Una consulta genérica no autoriza entidad arbitraria.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — No elige entidad arbitraria, pero transforma alcaldia en objeto y envía LIKE %alcaldia% sobre objeto/descripcion. Plan valid_query; una llamada.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE (unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%alcaldia%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%alcaldia%'))) ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `REAL_RUNTIME_BUG`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `UNQUALIFIED_BUYER_AS_TOPIC`.

**RATIONALE** — El comportamiento contradice la fase A: comprador incompleto requiere aclaración. Neutralizar el alias de entidad no basta si luego el sustantivo institucional se convierte en tema y sirve para saltar el guard de alcance. Es un bug de universo consultado, no de resumen.

**SHOULD_ORACLE_CHANGE:** NO. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.9. HCA-MONEY-020

**QUERY / DIALOGUE**

T1: `contratos de papeleria por mas de 100 mil pesos`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  eq:
    valor_min: 100000
functional_note: 100 mil = 100,000 (not millions)
```

**BLIND_USER_INTENT** — Contratos de papelería por más de cien mil pesos.

**REQUIRED_DIMENSIONS** — TOPIC=papelería; VALUE mínimo=100000 COP; contratos.

**CANONICAL_AUTHORITIES** — ADR-059 §2 y §6; MONEY_V2_CLOSEOUT gramática/unidades; ADR-061.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Aplicar escala literal 100 × 1000, preservar comparador y tema, proyectar al valor contractual; no filtrar por mil como texto.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — VÁLIDO: valor_min=100000 es contrato público COP, no detalle histórico de implementación.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Elimina el mínimo; sólo queda papeleria. Una llamada sin predicado de valor. money_to_cop(100 mil pesos)=100000, pero el router descarta montos <1000000.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE (unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%papeleria%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%papeleria%'))) ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `REAL_RUNTIME_BUG`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `MONEY_ROUTER_UNIT_ACCEPTANCE`.

**RATIONALE** — El contrato público es COP, y moneda más comparador son evidencia explícita. El piso antirruido no justifica ignorar un mínimo inequívoco y ejecutar más ampliamente. Subcausa: umbral duro del router, no conversión de escala. No se atribuye a un MV2 diferido sin una correspondencia documentada entre esos IDs y este caso.

**SHOULD_ORACLE_CHANGE:** NO. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.10. HCA-MONEY-026

**QUERY / DIALOGUE**

T1: `contratos de papeleria por mas de 1 millon`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  eq:
    valor_min: 1000000
functional_note: millón singular
```

**BLIND_USER_INTENT** — Contratos de papelería por más de un millón de pesos.

**REQUIRED_DIMENSIONS** — TOPIC=papelería; VALUE mínimo=1000000 COP; contratos.

**CANONICAL_AUTHORITIES** — ADR-059 contrato numérico/COP; ADR-063 normalización preserva intención; ADR-061.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Reconocer singular sin tilde como unidad monetaria normal, preservar mínimo y tema.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — VÁLIDO: umbral COP 1000000.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Elimina el mínimo; fuente sólo filtra papelería. La primitiva da 1000000 para 1 millon. El patrón del router millones? no acepta millon; la variante gramaticalmente incorrecta 1 millones sí produce valor_min=1000000.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE (unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%papeleria%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%papeleria%'))) ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `REAL_RUNTIME_BUG`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `MONEY_ROUTER_UNIT_ACCEPTANCE`.

**RATIONALE** — La fase A exige singular monetario normal. La evidencia localiza el hueco en reconocimiento de unidad del router, no en Decimal ni en normalización de acentos. Se agrupa con MONEY-020 como un trabajo acotado de admisión de montos, conservando sus dos causas técnicas distintas.

**SHOULD_ORACLE_CHANGE:** NO. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.11. HCA-STATE-003

**QUERY / DIALOGUE**

T1: `contratos liquidados de obra en 2025`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  ds: contratos
  has:
  - estado_contrato
functional_note: liquidados is contract-side
```

**BLIND_USER_INTENT** — Contratos de obra liquidados, con filtro temporal 2025.

**REQUIRED_DIMENSIONS** — STATE=liquidación contractual; CONTRACT_TYPE=obra; TIME=2025; contratos.

**CANONICAL_AUTHORITIES** — ADR-003/004/024; docs/reference/estado_contrato_semantica.md enumera estados pero no fija proyección de liquidación.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — No inventar estado_contrato=Liquidado ni equiparar liquidación con Cerrado/terminado/firmado. Falta autoridad canónica que proyecte liquidación y aclare qué fecha rige 2025; pedir precisión o declarar limitación antes de prometer resultados liquidados.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_DEFER`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — NO DETERMINADO: dataset contratos es razonable; has estado_contrato no demuestra liquidación y podría congelar una representación falsa.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Usa estado=Cerrado con estado_field=estado_contrato; tema textual obra, fechas de firma 2025, y ejecuta. Falta la lista estado_contrato que el oracle exige.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE (unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%obra%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%obra%'))) AND estado_contrato = 'Cerrado' AND fecha_de_firma >= '2025-01-01' AND fecha_de_firma <= '2025-12-31' ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `INSUFFICIENT_EVIDENCE`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `LIQUIDATION_SOURCE_CONTRACT_UNDEFINED`.

**RATIONALE** — Un scalar correctamente proyectado bastaría para no exigir una lista interna, pero eso no acredita la equivalencia liquidado=Cerrado. La base no contiene una autoridad canónica de liquidación; el comentario de código closest no la crea. Tampoco determina firma vs fecha de liquidación para 2025. No declarar oracle debt ni inventar Liquidado como literal. Requiere contrato de estado/fecha antes de decidir el arreglo.

**SHOULD_ORACLE_CHANGE:** MAYBE. **SHOULD_RUNTIME_CHANGE:** MAYBE.

### 5.12. HCA-STATE-005

**QUERY / DIALOGUE**

T1: `procesos publicados de aseo en cesar`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  soql_not:
  - Borrador
functional_note: publicados literally says Publicado — family must not inject Borrador
```

**BLIND_USER_INTENT** — Procesos de aseo publicados en Cesar.

**REQUIRED_DIMENSIONS** — STATE=publicado; TOPIC=aseo; GEOGRAPHY=Cesar; procesos.

**CANONICAL_AUTHORITIES** — docs/reference/estado_contrato_semantica.md §3–4: Borrador-proceso es no publicado; ADR-004; ADR-058; ADR-056 limita oportunidades.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Respetar publicado; no ampliar a borradores no publicados. No equiparar literalmente publicado con todo el funnel de oportunidades abiertas.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — VÁLIDO: excluir Borrador del universo solicitado. La política oferta_abierta no autoriza expandir cualquier mención explícita de publicado.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Publicado pasa a oferta_abierta y envía IN (Publicado, Borrador, En aprobación, Aprobado, Abierto), además apertura=Abierto. Una llamada real al stub de procesos.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `p6dx-8zbt`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE departamento_entidad = 'Cesar' AND (unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%aseo%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%aseo%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%aseos%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%aseos%')) OR unaccent(UPPER(nombre_del_procedimiento)) LIKE unaccent(UPPER('%aseo urbano%')) OR unaccent(UPPER(descripci_n_del_procedimiento)) LIKE unaccent(UPPER('%aseo urbano%'))) AND estado_del_procedimiento IN ('Publicado', 'Borrador', 'En aprobación', 'Aprobado', 'Abierto') AND estado_de_apertura_del_proceso = 'Abierto' ORDER BY fecha_de_publicacion_del DESC, precio_base DESC, id_del_proceso DESC LIMIT 50
```

**CLASSIFICATION** — `REAL_RUNTIME_BUG`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `STATE_FAMILY_PROJECTION`.

**RATIONALE** — La referencia canónica distingue borrador no publicado de publicado, y sólo incluye borradores deliberadamente en oportunidades/abiertos. La consulta no pidió ese funnel. Conservarlo para oportunidades no autoriza contaminar publicado literal. La cláusula incorrecta ya está en fuente: no es sólo una etiqueta errónea ni se necesita creer las filas sintéticas para demostrarlo.

**SHOULD_ORACLE_CHANGE:** NO. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.13. HCA-STATE-007

**QUERY / DIALOGUE**

T1: `contratos en ejecucion de la alcaldia de medellin`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  has:
  - estado_contrato
functional_note: en ejecución family
```

**BLIND_USER_INTENT** — Contratos en ejecución de la Alcaldía de Medellín.

**REQUIRED_DIMENSIONS** — STATE=contrato_activo; BUYER=Alcaldía de Medellín; contratos.

**CANONICAL_AUTHORITIES** — ADR-003/004; ADR-040; ADR-024.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Proyectar familia contrato_activo={En ejecución, Modificado, Prorrogado} conforme ADR-003 y comprador. Aceptar familia canónica sin requerir lista duplicada en params.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — has estado_contrato es innecesario si la familia se proyecta correctamente; confirmar SoQL y texto antes de declarar deuda.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Frame declara contrato_activo, pero fuente envía sólo estado_contrato=En ejecución. El intent inicial consume la frase y la proyección de familia no materializa Modificado/Prorrogado; el fallback legacy recupera un scalar.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%DISTRITO ESPECIAL DE CIENCIA TECNOLOGIA E INNOVACION DE MEDELLIN%')) AND estado_contrato = 'En ejecución' ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `REAL_RUNTIME_BUG`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `STATE_FAMILY_PROJECTION`.

**RATIONALE** — Se mantiene la fase A basada en ADR-003: en ejecución mapea a contrato_activo con tres valores. La ausencia de un campo interno por sí sola no sería bug; la pérdida observable de dos miembros de la familia sí lo es. El probe directo de familia devuelve los tres, y contratos activos también; no se halló una decisión posterior que revoque esa política. Si producto quiere literal exacto, debe cambiar autoridad explícitamente, no legitimar el accidente del fallback. No se infiere que activo, firmado y adjudicado sean sinónimos.

**SHOULD_ORACLE_CHANGE:** NO. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.14. HCA-TYPO-003

**QUERY / DIALOGUE**

T1: `procesos de regimen especial en 2026`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  has:
  - modalidad_include_procedure_ids
functional_note: regimen without accent
```

**BLIND_USER_INTENT** — Procesos por régimen especial en 2026, sin alcance adicional.

**REQUIRED_DIMENSIONS** — MODALITY=régimen especial; TIME=2026; procesos.

**CANONICAL_AUTHORITIES** — ADR-024; ADR-054; ADR-061 §3.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Reconocer régimen sin tilde, sin volverlo tema. Solicitar alcance útil según la política nacional de consulta.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_CLARIFY`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — EXECUTE excede alcance vigente; no es automáticamente un typo sin soporte.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Reconoce regimen_especial y regimen_especial_con_ofertas sin tilde, procesos y 2026. Plan valid_query/unsafe_global_candidate, guard pide alcance; cero llamadas.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`true`; source_calls=0 (query=0, count=0, aggregate=0).

No se envía consulta. Existe SoQL construido, pero bloqueado aguas abajo.

**CLASSIFICATION** — `KNOWN_DEFERRED_PRODUCT_DEBT`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `ONLY_MODALITY_SCOPE_POLICY`.

**RATIONALE** — La ausencia de tilde no causa el FAIL. La familia se reconoce y el límite es el mismo que MODALITY-002/012 y NEG-N15. No crear un proyecto de typos separado ni relajar seguridad para subir el score.

**SHOULD_ORACLE_CHANGE:** NO. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.15. HCA-FOLLOWUP-007

**QUERY / DIALOGUE**

T1: `contratos de obra en atlantico por licitacion publica`

T2: `mejor quita el filtro de modalidad`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  has:
  - modalidad_include_procedure_ids
- absent:
  - modalidad_include_procedure_ids
  objeto_has:
  - obra
functional_note: 'filter removal: quita el filtro de modalidad'
```

**BLIND_USER_INTENT** — Buscar obra en Atlántico por licitación y después retirar todo el filtro de modalidad.

**REQUIRED_DIMENSIONS** — Preservar CONTRACT_TYPE=obra, GEOGRAPHY=Atlántico, dataset; limpiar inclusiones/exclusiones de MODALITY.

**CANONICAL_AUTHORITIES** — ADR-054 CLEAR; guía modalidad §2–3; ADR-061 INV-STRUCTURED-DIMENSION-NOT-TOPIC.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — CLEAR debe preservar naturaleza y territorio y eliminar modalidad. Obra estructurada no debe duplicarse como tema.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — La ausencia de modalidad es válida; objeto_has obra es incorrecto si obra permanece proyectada por contract_type_include.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — T1 incluye obra, Atlántico y licitación. T2 delega CLEAR al motor de modalidad; queda obra + Atlántico, sin inclusiones/exclusiones de modalidad, ejecuta. Ambos WHERE conservan tipo_de_contrato=Obra.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE departamento = 'Atlántico' AND modalidad_de_contratacion IN ('Licitación pública', 'Licitación pública Obra Publica') AND (tipo_de_contrato = 'Obra') ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

T2: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE departamento = 'Atlántico' AND (tipo_de_contrato = 'Obra') ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `CONFIRMED_ORACLE_DEBT`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `STRUCTURED_CONTRACT_TYPE_NOT_TOPIC`.

**RATIONALE** — El control de modalidad sí funciona. ADR-061 prohíbe duplicar el tramo obra como tema cuando ya lo consume la dimensión de tipo contractual. El oracle exige la representación vieja. El header abreviado no enumera el tipo, pero no afirma otra naturaleza; esa observación UX no convierte el fallo del oracle en retrieval bug.

**SHOULD_ORACLE_CHANGE:** YES. **SHOULD_RUNTIME_CHANGE:** NO.

### 5.16. HCA-FOLLOWUP-016

**QUERY / DIALOGUE**

T1: `contratos de obra en cali por mas de 300 millones`

T2: `los mas antiguos primero`

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
  eq:
    valor_min: 300000000
- eq:
    valor_min: 300000000
  objeto_has:
  - obra
functional_note: ' ordering change must not lose money filter (reorder keeps WHERE)'
```

**BLIND_USER_INTENT** — Buscar obra en Cali por más de 300 millones; ordenar después de antiguo a reciente.

**REQUIRED_DIMENSIONS** — CONTRACT_TYPE=obra; CITY=Cali; VALUE mínimo=300000000; ORDER fecha ASC; contratos.

**CANONICAL_AUTHORITIES** — ADR-061; ADR-066 INV-ORDERING-FOLLOWUP-PRESERVES-UNIVERSE.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Cambiar sólo ORDER BY, preservando WHERE de tipo, ciudad y monto. Obra no requiere filtro temático redundante.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — INCORRECTO exigir objeto_has obra como representación duplicada; monto y conservación observable del universo siguen siendo obligatorios.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — Dinero=300000000, ciudad=Cali y tipo=obra sobreviven; T2 change_order sólo acepta ordering_signal=fecha_asc. Sin embargo T1 consulta sin entidad_ecosystem y T2 consulta con un OR de diez compradores. Una llamada por turno.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE unaccent(UPPER(ciudad)) LIKE unaccent(UPPER('%Cali%')) AND valor_del_contrato >= 300000000 AND (tipo_de_contrato = 'Obra') ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

T2: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE unaccent(UPPER(ciudad)) LIKE unaccent(UPPER('%Cali%')) AND (unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%ALCALDÍA DE SANTIAGO DE CALI%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%SECRETARÍA DE EDUCACIÓN MUNICIPAL%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%SECRETARÍA DE SALUD PÚBLICA MUNICIPAL%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%SECRETARÍA DE INFRAESTRUCTURA Y VALORIZACIÓN%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%SECRETARÍA DE CULTURA DE CALI%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%DEPARTAMENTO ADMINISTRATIVO DE GESTIÓN DEL MEDIO AMBIENTE%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%EMPRESAS MUNICIPALES DE CALI%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%INSTITUTO MUNICIPAL DE DEPORTE Y RECREACIÓN%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%SECRETARÍA DE MOVILIDAD DE CALI%')) OR unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%SECRETARÍA DE BIENESTAR SOCIAL%'))) AND valor_del_contrato >= 300000000 AND (tipo_de_contrato = 'Obra') ORDER BY fecha_de_firma ASC, valor_del_contrato ASC, id_contrato ASC LIMIT 50
```

**CLASSIFICATION** — `REAL_RUNTIME_BUG`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `ORDERING_FOLLOWUP_ECOSYSTEM_DRIFT`.

**RATIONALE** — La hipótesis de objeto_has obra obsoleto se confirma como defecto secundario del oracle, pero la condición ciega de WHERE idéntico falla. normal_flow añade ecosistema post-query y lo persiste; al reordenar, ese estado pasa al próximo fetch. Captura de source_calls confirma el AND adicional; no es sólo SoQL decorativo. Primaria REAL_RUNTIME_BUG. No entra al plan de oracle mientras sea primaria runtime. No es CHANGE_SCOPE_TOPIC_LOSS: el intent real es change_order y no se pierde obra.

**SHOULD_ORACLE_CHANGE:** YES. **SHOULD_RUNTIME_CHANGE:** YES.

### 5.17. HCA-META-001B

**QUERY / DIALOGUE**

T1: `contratos de adulto mayor del departamento del atlantico para 2026`

Referencia `HCA-META-001A`: `contratos de adulto mayor de la gobernacion del atlantico en 2026`.

**ORIGINAL_ORACLE**

```yaml
turns:
- expect: execute
same_frame_as: HCA-META-001A
```

**BLIND_USER_INTENT** — Contratos de adulto mayor del departamento del Atlántico en 2026; comparado con Gobernación.

**REQUIRED_DIMENSIONS** — TOPIC=adulto mayor; GEOGRAPHY=Atlántico; TIME=2026; contratos. Referencia usa BUYER.

**CANONICAL_AUTHORITIES** — ADR-040 INV-7; ADR-061.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Preservar la diferencia departamento territorial vs Gobernación compradora. No igualar universos de todas las entidades y una sola entidad.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_EXECUTE`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — INCORRECTO same_frame_as: la sustitución cambia dimensión semántica, no es una variante neutra.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — B consulta departamento=Atlántico; A consulta comprador DEPARTAMENTO DEL ATLANTICO**. Ambos conservan adulto_mayor y 2026, ejecutando una llamada cada uno.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE departamento = 'Atlántico' AND (unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%adulto mayor%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%adulto mayor%')) OR unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%persona mayor%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%persona mayor%')) OR unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%adultos mayores%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%adultos mayores%')) OR unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%personas mayores%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%personas mayores%'))) AND fecha_de_firma >= '2026-01-01' AND fecha_de_firma <= '2026-12-31' ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `CONFIRMED_ORACLE_DEBT`. **PRIORITY** — `P2`. **ROOT_CAUSE** — `TERRITORY_BUYER_FALSE_EQUIVALENCE`.

**RATIONALE** — ADR-040 INV-7 exige universos distintos entre departamento territorial y Gobernación compradora. La transformación no es neutra. El FAIL metamórfico protege una equivalencia semánticamente falsa; uppercase/lowercase, en cambio, sí debe preservar intención (no es este par).

**SHOULD_ORACLE_CHANGE:** YES. **SHOULD_RUNTIME_CHANGE:** NO.

### 5.18. HCA-SOURCE-002

**QUERY / DIALOGUE**

T1: `procesos liquidados de obra en cauca`

**ORIGINAL_ORACLE**

```yaml
turns:
- ds: procesos
functional_note: contract state requested on processes wording — must not fabricate
```

**BLIND_USER_INTENT** — Procesos de obra liquidados en Cauca: vocabulario de procesos junto a atributo contractual.

**REQUIRED_DIMENSIONS** — CONTRACT_TYPE=obra; GEOGRAPHY=Cauca; STATE=liquidación, capacidad/dataset sin resolver.

**CANONICAL_AUTHORITIES** — ADR-003/024; referencia de estados; ADR-061 proyección explícita por dataset; ausencia de contrato canónico de liquidación en la base.

**EXPECTED_PRODUCT_BEHAVIOR (fase A)** — Aclarar procesos frente a contratos y la semántica de liquidación; no fabricar estado de proceso ni cambiar fuente en silencio. No basta comprobar dataset.

**SHOULD_EXECUTE / SHOULD_CLARIFY / SHOULD_DEFER (fase A)** — `SHOULD_CLARIFY`.

**SEMANTIC_ORACLE_VERDICT (fase A)** — NO DETERMINADO para dataset obligatorio; el oracle no verifica aclaración ni proyección válida y no autoriza semántica inventada.

**CURRENT_RUNTIME_BEHAVIOR (fase B)** — A pesar de procesos, selecciona contratos y consulta Cauca + texto obra + estado_contrato=Cerrado. No aclara. El oracle sólo reclama dataset procesos.

**SOURCE_BEHAVIOR**

T1: plan=`valid_query`, should_execute=`true`, aclaración final=`false`; source_calls=1 (query=1, count=0, aggregate=0).

Dataset invocado: `jbjy-vk9h`. Predicado y orden efectivamente enviados al stub:

```sql
WHERE departamento = 'Cauca' AND (unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%obra%')) OR unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%obra%'))) AND estado_contrato = 'Cerrado' ORDER BY fecha_de_firma DESC, valor_del_contrato DESC, id_contrato DESC LIMIT 50
```

**CLASSIFICATION** — `INSUFFICIENT_EVIDENCE`. **PRIORITY** — `P1`. **ROOT_CAUSE** — `LIQUIDATION_SOURCE_CONTRACT_UNDEFINED`.

**RATIONALE** — Conservar dataset procesos no resuelve una capacidad de liquidación que ese oracle no demuestra. Tampoco moverlo en silencio a contratos prueba corrección. Comparte falta de autoridad de liquidación con STATE-003 y agrega conflicto de dataset. La evidencia de implementación no permite declarar Liquidado=Cerrado ni estado de proceso equivalente. Se conserva INSUFFICIENT_EVIDENCE; decisión de fuente y política de aclaración antes de modificar runtime/oracle.

**SHOULD_ORACLE_CHANGE:** MAYBE. **SHOULD_RUNTIME_CHANGE:** MAYBE.

## 6. Matriz final y composición

| CASE_ID | CLASS | RUNTIME_CHANGE | ORACLE_CHANGE | PRIORITY | ROOT_CAUSE | NOTES |
| --- | --- | --- | --- | --- | --- | --- |
| HCA-ACTOR-004 | CONFIRMED_ORACLE_DEBT | NO | YES | P2 | ACTOR_CANDIDATE_NOT_IDENTITY | Ver evidencia individual |
| HCA-TOPIC-007 | CONFIRMED_ORACLE_DEBT | NO | YES | P2 | CANONICAL_TOPIC_ALIAS | Ver evidencia individual |
| HCA-TOPIC-012 | SAFE_UNSUPPORTED_CAPABILITY | NO | MAYBE | P2 | EXACT_ACTOR_TOPIC_AMBIGUITY | Ver evidencia individual |
| HCA-MODALITY-002 | KNOWN_DEFERRED_PRODUCT_DEBT | YES | NO | P2 | ONLY_MODALITY_SCOPE_POLICY | Entrega diferida, no fix de guard |
| HCA-MODALITY-012 | KNOWN_DEFERRED_PRODUCT_DEBT | YES | NO | P2 | ONLY_MODALITY_SCOPE_POLICY | Entrega diferida, no fix de guard |
| HCA-ENTITY-001 | CONFIRMED_ORACLE_DEBT | NO | YES | P2 | SOURCE_BUYER_LITERAL | Ver evidencia individual |
| HCA-ENTITY-004 | INSUFFICIENT_EVIDENCE | MAYBE | MAYBE | P1 | BUYER_SUBJECT_VS_TERRITORIAL_POLICY | Autoridad pendiente |
| HCA-ENTITY-005 | REAL_RUNTIME_BUG | YES | NO | P1 | UNQUALIFIED_BUYER_AS_TOPIC | Ver evidencia individual |
| HCA-MONEY-020 | REAL_RUNTIME_BUG | YES | NO | P1 | MONEY_ROUTER_UNIT_ACCEPTANCE | Ver evidencia individual |
| HCA-MONEY-026 | REAL_RUNTIME_BUG | YES | NO | P1 | MONEY_ROUTER_UNIT_ACCEPTANCE | Ver evidencia individual |
| HCA-STATE-003 | INSUFFICIENT_EVIDENCE | MAYBE | MAYBE | P1 | LIQUIDATION_SOURCE_CONTRACT_UNDEFINED | Autoridad pendiente |
| HCA-STATE-005 | REAL_RUNTIME_BUG | YES | NO | P1 | STATE_FAMILY_PROJECTION | Ver evidencia individual |
| HCA-STATE-007 | REAL_RUNTIME_BUG | YES | NO | P1 | STATE_FAMILY_PROJECTION | Ver evidencia individual |
| HCA-TYPO-003 | KNOWN_DEFERRED_PRODUCT_DEBT | YES | NO | P2 | ONLY_MODALITY_SCOPE_POLICY | Entrega diferida, no fix de guard |
| HCA-FOLLOWUP-007 | CONFIRMED_ORACLE_DEBT | NO | YES | P2 | STRUCTURED_CONTRACT_TYPE_NOT_TOPIC | Ver evidencia individual |
| HCA-FOLLOWUP-016 | REAL_RUNTIME_BUG | YES | YES | P1 | ORDERING_FOLLOWUP_ECOSYSTEM_DRIFT | Deuda oracle secundaria; sin patch ahora |
| HCA-META-001B | CONFIRMED_ORACLE_DEBT | NO | YES | P2 | TERRITORY_BUYER_FALSE_EQUIVALENCE | Ver evidencia individual |
| HCA-SOURCE-002 | INSUFFICIENT_EVIDENCE | MAYBE | MAYBE | P1 | LIQUIDATION_SOURCE_CONTRACT_UNDEFINED | Autoridad pendiente |

`YES` runtime en deuda diferida significa necesario **para entregar la capacidad solicitada**, no que hoy haya que quitar el guard. `MAYBE` no es una aprobación de la conducta observada. Las severidades se adjudican semánticamente: ENTITY-001 deja de ser un supuesto P0; MONEY-026 sube a P1 por pérdida silenciosa de restricción.

| Clasificación primaria | Casos |
| --- | --- |
| REAL_RUNTIME_BUG | 6 |
| SAFE_UNSUPPORTED_CAPABILITY | 1 |
| CONFIRMED_ORACLE_DEBT | 5 |
| PRESENTATION_ONLY_DEFECT | 0 |
| KNOWN_DEFERRED_PRODUCT_DEBT | 3 |
| INSUFFICIENT_EVIDENCE | 3 |

**Suma: 18.** Casos con defectos secundarios se cuentan una sola vez.

## 7. Clusters de runtime y prioridades

| CLUSTER | CASE_COUNT | SEVERITY | SHARED_ROOT_CAUSE | RECOMMENDED_NEXT_ACTION |
| --- | --- | --- | --- | --- |
| MONEY_ROUTER_UNIT_ACCEPTANCE | 2 | P1 | Frontera router→conversor descarta montos válidos: piso <1M y singular ausente (dos subcausas, una superficie). | Sprint acotado de admisión monetaria: preservar umbral explícito hasta fuente y conservar rechazo de cardinales no monetarios. |
| STATE_FAMILY_PROJECTION | 2 | P1 | Desalineación entre lenguaje literal, familia declarada y predicado proyectado. STATE-005 amplía; STATE-007 trunca. No se afirma una sola línea causal. | Restablecer la autoridad canónica: publicado no incorpora borrador; contrato_activo conserva tres valores. No mezclar liquidación sin decisión de producto. |
| UNQUALIFIED_BUYER_AS_TOPIC | 1 | P1 | Comprador genérico no resuelto cae al residual temático y habilita guard. | Aclarar comprador antes de ejecutar; no promover alcaldía a tema ni a entidad arbitraria. |
| ORDERING_FOLLOWUP_ECOSYSTEM_DRIFT | 1 | P1 | Ecosistema inyectado post-fetch persiste y se vuelve filtro del siguiente turno. | Verificar igualdad de WHERE en source_calls T1/T2 y separar metadata presentacional de filtros persistidos; preservar orden, monto y tipo. |

Son **cuatro paquetes de trabajo para seis casos**, no seis proyectos independientes ni un refactor genérico de QueryPlan. STATE_FAMILY agrupa una frontera semántica compartida con dos mecanismos diferentes; MONEY conserva dos causas técnicas explícitas. El detalle de líneas observado no es autorización de implementación en este bloque.

Rutas causales verificadas: `query_router.py:1095,1388,1446` (unidad y piso), `intent_vocabulary.py:81` → `query_router.py:402,783,831` (familia consumida y fallback), `estado_families.py:122` → `query_router.py:823` → `soql_builder.py:493,513` (publicado/funnel), `normal_flow.py:186–205,447` → frame heredado → `soql_builder.py:370` (ecosistema). La traza de FOLLOWUP-016 acepta sólo ordering_signal y aun así el segundo fetch trae diez compradores; el material_frame del HCA **no incluye entidad_ecosystem**, por lo que una comparación de ese frame no detecta el cambio de universo.

`CHANGE_SCOPE_TOPIC_LOSS`: **0/18**. Los únicos diálogos FAIL son FOLLOWUP-007 (`refine_filter` + CLEAR) y FOLLOWUP-016 (`change_order`); ninguno solicita tema nuevo ni clasifica change_scope. La deuda preexistente permanece fuera del conjunto. `CONTROL_LIMIT_SEMANTICS`: **0/18**; ningún caso FAIL pide top N/primeros N. Sigue diferido globalmente; `params[limit]` no prueba display cardinality.

## 8. Estado, retrieval y presentación: fronteras explícitas

| Caso | Intención/familia esperada | Política/source actual | Decisión |
| --- | --- | --- | --- |
| STATE-003 | Liquidación contractual; fecha sin proyección canónica definida | Scalar Cerrado + fecha_de_firma; obra textual | INSUFFICIENT_EVIDENCE; no declarar liquidación igual a cierre. |
| STATE-005 | Publicado; no borrador no publicado | oferta_abierta expandida + apertura Abierto | Bug de predicado fuente, no de tarjeta. |
| STATE-007 | contrato_activo de ADR-003: En ejecución/Modificado/Prorrogado | Etiqueta familia, predicado scalar sólo En ejecución | Bug de proyección de familia, no sólo campo interno ausente. |
| SOURCE-002 | Liquidación + vocabulario procesos; aclarar dataset/capacidad | Cambia a contratos y Cerrado sin aclarar | INSUFFICIENT_EVIDENCE; el cambio de dataset no prueba corrección. |

La revisión no define una nueva ontología: publicado ≠ borrador; apertura Abierto no demuestra ofertas disponibles (ADR-058); firmado no equivale a activo/cerrado (ADR-026); adjudicado no se equipara a ejecutando; liquidación queda sin decisión. Para STATE-007 se conserva el contrato **ya aceptado** de ADR-003, no una inferencia nueva sobre datos de fuente.

**PRESENTATION_ONLY_DEFECT: 0** como primaria entre estos 18. Las cabeceras no enumeran siempre tipo/estado y algunos slugs se muestran sin humanizar; son observaciones de UX, no prueba de retrieval incorrecto ni oracles de resumen en estos casos. FOLLOWUP-016 no es presentation-only porque el AND adicional llega a la llamada fuente. En STATE-005 el problema también está en WHERE. El render de filas sintéticas queda fuera de valoración semántica de resultados.

Inspección de oracles internos: `actor_scope` sólo tras autoridad, nunca por rank; `objeto_has` no sustituye el contrato de dimensiones; `entidad_resolved` no confunde comprador/territorio; `estado_contrato` lista no es imprescindible si un scalar proyecta la misma semántica; `same_frame_as` sólo sirve en transformaciones realmente neutras. Ninguno de los 18 falla por una etiqueta de intent aislada; no se añade una exigencia nueva sobre ella.

## 9. Controles de negación y deuda global — fuera del denominador HCA

NEG-N15 y NEG-ANTI-002 pertenecen a `negation_focal_v1.yaml`, no a los 18 FAIL holísticos. Se ejecutó el focal completo: **22 PASS / 2 FAIL / 0 ERROR**. [Captura de los controles](evidence/2026-08-26_HCA_ORACLE_REVIEW/negation_controls.jsonl).

| Caso externo | Dictamen | Evidencia y autoridad |
| --- | --- | --- |
| NEG-N15 | KNOWN_DEFERRED_PRODUCT_DEBT / VALID_DEFERRED | contratacion directa sin ofertas incluye contratacion_directa; no exclusiones. P13 pasivo marca unsafe_global_candidate; guard final aclara, 0 llamadas. ADR-024 no autoriza sólo-modalidad. No es fallo de negación; mismo cluster de deuda que tres HCA. |
| NEG-ANTI-002 | CONFIRMED_ORACLE_DEBT (externo) | Positivo NEG-N12: ciudad Barranquilla, entidad_resolved ausente, 1 llamada. Negativo: ciudad ausente, entidad_resolved ausente, unsupported_negation, 0 llamadas. La diferencia de ciudad y de ejecución es válida; exigir además diferencia de entidad inventaría una promoción positiva prohibida por ADR-006/P9 y ADR-064/S3. |
| GOB-DEPTO-001 | Deuda dialogue corpus; no re-adjudicada aquí | Existe en tests/fixtures/dialogue_policy_corpus.yaml:498; consulta Gobernación de Boyacá. No pertenece a HCA, no se suma a 195/18. No se ejecutó de nuevo ni se declara cerrado. |
| CHANGE_SCOPE_TOPIC_LOSS | Deuda global preexistente, no manifestada entre los 18 | Bitácora DEFERRED_FOLLOWUP_SCOPE_TOPIC; política change_scope excluye TOPIC_FIELDS. No se implementa fix. |
| CONTROL_LIMIT_SEMANTICS | Diferido global, 0 casos entre los 18 | ADR-066 exige separar pool y display; no se usa limit=10 como cierre. |
| SEMANTIC_AUTHORITY_CLEANUP / QUERY_PLAN_AT_LIMIT | Pendientes globales | No se abre refactor; QueryPlan idéntico, G8=299. |

No se incluye NEG-ANTI-002 en el plan de patch **HCA** ni en sus cinco deudas confirmadas; la corrección del focal de negación requerirá su propio alcance autorizado. El presente review confirma la hipótesis sin editar ese fixture.

## 10. Plan exacto de oracle — propuesto, no aplicado

Sólo las **cinco clasificaciones primarias CONFIRMED_ORACLE_DEBT**. [Plan JSON](evidence/2026-08-26_HCA_ORACLE_REVIEW/oracle_patch_plan.json). Se conservan query, IDs, orden, notas históricas relevantes y los turns no mencionados; se actualizan las notas que describen una expectativa refutada. No se modifica runtime para satisfacer este plan. No se cambia el corpus durante esta revisión.

### HCA-ACTOR-004 — turno 1

**OLD_ORACLE**

```yaml
has:
- actor_scope
objeto_not:
- lmp
- ingieneria
```

**PROPOSED_ORACLE** (reemplazo exacto de las aserciones del turno; `q` se conserva)

```yaml
expect: block
absent:
- actor_scope
- contratista
has:
- actor_pending
candidate_present:
  document_type: NIT
  base_number: '900739049'
  max_rank: 3
objeto_not:
- lmp
- ingieneria
```

**Cambio exacto de nota:** `sentinel typo LMP INGIENERIA — actor-shaped mention must go to candidates, not topic` → `Typo actor-shaped: present typed candidates without authorizing identity before user selection; no topic leakage.`.

**WHY** — Confirma la calibración ciega: la expectativa de scope autoritativo contradice candidatos sin selección. El ranking no autoriza identidad. No es fallo de recall ni un bug que deba curarse auto-resolviendo fuzzy.

**AUTHORITY** — ADR-060 §4, §5, §9–10; ACTOR_PROFILE_V1 §7–10; INV-NAME-NO-GUESS.

**Aceptación observable adicional:** Antes de selección: cero source_calls; candidato tipado visible, ninguna identidad autorizada. La DSL expect:block no prueba por sí sola cero llamadas; comprobar con el observador adjunto.

### HCA-TOPIC-007 — turno 1

**OLD_ORACLE**

```yaml
expect: execute
ds: procesos
objeto_has:
- pae
```

**PROPOSED_ORACLE** (reemplazo exacto de las aserciones del turno; `q` se conserva)

```yaml
expect: execute
ds: procesos
soql_has:
- departamento_entidad = 'La Guajira'
- '%alimentacion escolar%'
soql_not:
- '%pae%'
```

**Cambio exacto de nota:** `None` → `PAE denotes the canonical school-feeding topic; validate territory and source aliases, not the input acronym in an internal field.`.

**WHY** — La intención se representa mediante el concepto canónico y sus aliases; exigir el substring pae en params no mide la conservación del tema. El catálogo excluye %pae% para evitar Páez. La expansión curada existente no se certifica aquí como recall/precision perfecta; la deuda confirmada es exigir la forma superficial.

**AUTHORITY** — ADR-006; ADR-024; ADR-036 (PAE requiere aliases curados); ADR-061.

### HCA-ENTITY-001 — turno 1

**OLD_ORACLE**

```yaml
expect: execute
soql_not:
- ATLANTICO**
```

**PROPOSED_ORACLE** (reemplazo exacto de las aserciones del turno; `q` se conserva)

```yaml
expect: execute
ds: contratos
soql_has:
- unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%DEPARTAMENTO DEL ATLANTICO**%'))
- fecha_de_firma >= '2026-01-01'
- fecha_de_firma <= '2026-12-31'
```

**Cambio exacto de nota:** `canonical value must not carry markdown/asterisks into LIKE` → `Buyer must use the real SECOP source name, including literal asterisks where present; preserve the 2026 scope.`.

**WHY** — ADR-040 documenta exactamente los dos asteriscos como parte del valor SECOP. El oracle confundió dato de fuente con Markdown. El P0 histórico no es una severidad de runtime confirmada. Limpiar la cadena por estética no es un arreglo autorizado; cualquier presentación escapada es otra superficie.

**AUTHORITY** — ADR-040: Atlántico se registra como DEPARTAMENTO DEL ATLANTICO** y el comprador usa el valor real.

### HCA-FOLLOWUP-007 — turno 2

**OLD_ORACLE**

```yaml
absent:
- modalidad_include_procedure_ids
objeto_has:
- obra
```

**PROPOSED_ORACLE** (reemplazo exacto de las aserciones del turno; `q` se conserva)

```yaml
expect: execute
ds: contratos
absent:
- modalidad_include_procedure_ids
- modalidad_exclude_procedure_ids
soql_has:
- departamento = 'Atlántico'
- tipo_de_contrato = 'Obra'
soql_not:
- modalidad_de_contratacion IN
- modalidad_de_contratacion =
```

**Cambio exacto de nota:** `filter removal: quita el filtro de modalidad` → `CLEAR removes modality and preserves territory plus structured contract type Obra; do not require duplicate topic text.`.

**WHY** — El control de modalidad sí funciona. ADR-061 prohíbe duplicar el tramo obra como tema cuando ya lo consume la dimensión de tipo contractual. El oracle exige la representación vieja. El header abreviado no enumera el tipo, pero no afirma otra naturaleza; esa observación UX no convierte el fallo del oracle en retrieval bug.

**AUTHORITY** — ADR-054 CLEAR; guía modalidad §2–3; ADR-061 INV-STRUCTURED-DIMENSION-NOT-TOPIC.

### HCA-META-001B — turno 1

**OLD_ORACLE**

```yaml
expect: execute
```

Eliminar además `same_frame_as: HCA-META-001A` del nivel caso.

**PROPOSED_ORACLE** (reemplazo exacto de las aserciones del turno; `q` se conserva)

```yaml
expect: execute
ds: contratos
soql_has:
- departamento = 'Atlántico'
- '%adulto mayor%'
- fecha_de_firma >= '2026-01-01'
- fecha_de_firma <= '2026-12-31'
soql_not:
- unaccent(UPPER(nombre_entidad)) LIKE
```

**Cambio exacto de nota:** `None` → `Territorial department is not equivalent to Gobernacion buyer (ADR-040); preserve topic/time while querying the territorial universe.`.

**WHY** — ADR-040 INV-7 exige universos distintos entre departamento territorial y Gobernación compradora. La transformación no es neutra. El FAIL metamórfico protege una equivalencia semánticamente falsa; uppercase/lowercase, en cambio, sí debe preservar intención (no es este par).

**AUTHORITY** — ADR-040 INV-7; ADR-061.

**Relación observable:** A: comprador; B: territorio; mismo tema y tiempo. No exigir igualdad del frame completo.

No se genera patch para REAL_RUNTIME_BUG ni KNOWN_DEFERRED_PRODUCT_DEBT. FOLLOWUP-016 tiene `ORACLE_CHANGE=YES` por su aserción duplicada, pero queda expresamente **fuera** de este plan hasta resolver el defecto de universo. SAFE TOPIC-012 queda MAYBE para fijar una precondición reproducible del índice/rol; no se amplía el conjunto de deuda confirmada por conveniencia. Los tres INSUFFICIENT requieren autoridad antes de cambiar examen o producto.

## 11. Métricas — composición explícita, sin score encubierto

**RAW_HCA_SCORE / raw_supported = 177/195 = 90,77%**, con 18 FAIL y 0 ERROR. No se modificó ninguna aserción ni se convirtió FAIL a PASS.

**SEMANTICALLY_SUPPORTED = 183/195 = 93,85%**, definido exclusivamente para esta revisión como `177 PASS observados + 5 CONFIRMED_ORACLE_DEBT + 1 SAFE_UNSUPPORTED correctamente fail-closed`. Es una cobertura de comportamiento según la política actual, **no** un nuevo resultado del runner ni una auditoría semántica de los 177 PASS. El SAFE exige ambigüedad material, pregunta específica, conservación explícita de las dimensiones y cero source_calls; TOPIC-012 lo cumple. No significa ejecución exitosa de la capacidad solicitada.

**ACTIONABLE_RUNTIME_FAILURES = 6 ahora** (seis REAL_RUNTIME_BUG + cero presentation-only). Hay además **3 casos de producto deliberadamente diferidos**, un único frente ONLY_MODALITY_SCOPE_POLICY: si se decide entregar la demanda, el backlog de cambios suma **9 casos**. Esa extensión no se cuenta como bug inmediato ni se incluye en semantically_supported pese a su bloqueo seguro. **3 casos INSUFFICIENT** quedan fuera de ambos numeradores; no son aprobaciones. No se suma dos veces el defecto de oracle secundario de FOLLOWUP-016.

Comprobación del universo: `177 + 5 + 1 + 6 + 0 + 3 + 3 = 195`; adjudicación: `6 + 1 + 5 + 0 + 3 + 3 = 18`.

## 12. Ejecuciones, verificación y reproducción

| Corrida | PASS | FAIL | ERROR | Observación |
| --- | --- | --- | --- | --- |
| hca | 177 | 18 | 0 | Runner original |
| captured | 177 | 18 | 0 | Status idéntico al basal |
| negation | 22 | 2 | 0 | Mismos NEG-N15/NEG-ANTI-002; observador de llamadas |
| actor | 36 | 0 | 0 | Runner original |
| control | 42 | 0 | 0 | Runner original |
| time | 32 | 0 | 0 | Runner original |

Todos los `isolation.json` registran `outside_sandbox=[]`. Los focales actor/control/time se usaron como controles de cierre, no para sustituir las autoridades ni para afirmar cardinalidad de top N. No full pytest: no hubo cambios de runtime/tests. Los probes son observaciones de frontera, no tests nuevos ni modificaciones a los existentes.

Comandos ejecutados desde el worktree (Python del entorno existente):

```bash
PY=/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python3
$PY scripts/audits/run_conversation_holistic_audit.py --out /tmp/secoppal-hca-oracle-review-20260826/hca
$PY docs/audits/evidence/2026-08-26_HCA_ORACLE_REVIEW/capture_runtime.py --out /tmp/secoppal-hca-oracle-review-20260826/captured
$PY docs/audits/evidence/2026-08-26_HCA_ORACLE_REVIEW/capture_runtime.py --corpus tests/fixtures/audits/negation_focal_v1.yaml --out /tmp/secoppal-hca-oracle-review-20260826/negation
$PY scripts/audits/run_conversation_holistic_audit.py --corpus tests/fixtures/audits/actor_hygiene_v1.yaml --out /tmp/secoppal-hca-oracle-review-20260826/actor
$PY scripts/audits/run_conversation_holistic_audit.py --corpus tests/fixtures/audits/control_language_v1.yaml --out /tmp/secoppal-hca-oracle-review-20260826/control
$PY scripts/audits/run_conversation_holistic_audit.py --corpus tests/fixtures/audits/time_v2_focal.yaml --out /tmp/secoppal-hca-oracle-review-20260826/time
$PY docs/audits/evidence/2026-08-26_HCA_ORACLE_REVIEW/probe_boundaries.py
git diff --exit-code 0a77070 -- app tests scripts
git diff --check
git diff --cached --check
```

Verificación de alcance: **RUNTIME_FILES_CHANGED=0; FIXTURES_CHANGED=NO; HCA_FIXTURE_CHANGED=NO**. Los archivos existentes de app/tests/scripts y ADRs conservan sus blobs canónicos; únicamente se agregan este informe y su carpeta de evidencia. Stage explícito; commit previsto `docs(audit): adjudicate remaining HCA oracle failures`. El manifest enumera hashes de artefactos y el chequeo final comprueba la suma, IDs, composición, paridad del runner, ausencia de cambios prohibidos y hash ciego.

## 13. Decisión y siguiente acción única

```yaml
HCA_ORACLE_REVIEW: PASS
18_CASES_ADJUDICATED: 18
REAL_RUNTIME_BUG: 6
SAFE_UNSUPPORTED: 1
CONFIRMED_ORACLE_DEBT: 5
PRESENTATION_ONLY: 0
KNOWN_DEFERRED: 3
INSUFFICIENT_EVIDENCE: 3
NEXT: READY_FOR_HCA_FINAL_FIX_SPRINT
RUNTIME_FILES_CHANGED: 0
FIXTURES_CHANGED: NO
MERGE: NO
PUSH: NO
```

Existen seis fallos P1 confirmados y coherentes en cuatro frentes; por eso no corresponde READY_FOR_ORACLE_PATCH todavía, aunque haya cinco oracles confirmados. Tampoco corresponde READY_FOR_FINAL_HCA. Antes o durante la planificación del sprint debe resolverse la autoridad pendiente del sujeto comprador y de liquidación; este documento no la inventa. **No se ejecuta ninguna siguiente acción.**

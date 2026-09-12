# Olympus Policy

Autoridad canónica para trabajo multiagente en SECOPPAL. `AGENTS.md` conserva
las leyes duras del producto; los bootstraps de cada runtime apuntan aquí. Esta
política gobierna decisiones futuras, no reescribe auditorías ni evidencia
histórica.

## 1. Autoridades operativas

Cada trabajo se delimita con cinco autoridades concretas:

1. `MISSION`: decisión buscada, invariantes, criterios, gates y stops.
2. `WORKTREE`: checkout aislado, branch y `BASE_SHA` acreditados.
3. `OWNERSHIP`: un writer por paths solapados; readers pueden paralelizarse.
4. `SCOPE`: paths permitidos y protegidos; un finding no lo amplía.
5. `CANDIDATE`: SHA congelado, estado Git y evidencia que se auditan.

Dentro de las leyes duras de seguridad del producto y del repo, la petición
actual manda sobre la política del repo; ésta manda sobre defaults. Ninguna
petición puede relajar esas leyes duras.
La historia es evidencia, no autoridad operativa presente. HERA coordina el
estado; CÁSTOR inventaría; PÓLUX falsifica; METIS cierra arquitectura y contrato;
ARES implementa; HERMES diseña la verificación; ZEUS audita en contexto fresco;
HADES rediagnostica loops; CLÍO conserva conocimiento durable.

HERA opera con la skill `hera`; HADES con la skill `hades`. Esta policy define el
qué (triggers, outputs, gates); las skills definen el cómo (pasos, método).

Antes de escribir, la cápsula de misión debe contener:

```text
MISSION / CURRENT_BEHAVIOR / EXPECTED_BEHAVIOR / AUTHORITY / INVARIANTS
ALLOWED_SCOPE / PROTECTED_SCOPE / APPROVED_DECISION / ACCEPTANCE_CASES
GATES / STOP_CONDITIONS / EVIDENCE_REFS / OUTPUT_CONTRACT
```

## 2. Toolchain y navegación

HERA registra una vez al inicio:

```text
TOOLCHAIN_READINESS
repo_access, git, graft, tgrep, rg, ruff, semgrep, pytest, hypothesis,
network_allowed, worktree_writable, external_provider_access
```

Cada valor es `READY`, `DEGRADED`, `UNAVAILABLE` o `FORBIDDEN`, acompañado de
versión y operaciones soportadas cuando se puedan observar. Una herramienta
opcional ausente degrada el plan; no bloquea la misión ni justifica una larga
reparación. No se inventan capacidades ni acceso externo.

Usa Graft para topología, callers, dependencias y blast radius. Usa tgrep para
literales, regex, ocurrencias y discovery. Para verdad fresca o probar ausencia
tras una mutación, usa `rg` o `tgrep --fresh`. Estas preferencias sólo aplican
cuando el preflight acredita la herramienta y la operación; los fallbacks
obligatorios son `rg`, `git grep`, lectura focal y `git log`/`git blame`.
Semgrep busca patrones prohibidos, Hypothesis prueba invariantes combinatorias y
pytest comportamiento ejecutado.

## 3. Preflight cognitivo

Antes de METIS y ARES en toda misión `COMPLEX` o `HIGH_RISK`, HERA conserva un
`COGNITIVE_PREFLIGHT`:

### Pregunta y realidad

`QUESTION` expresa la decisión exacta, no sólo “implementar X”.
`CURRENT_REALITY` clasifica cada proposición como `FACT`, `INFERENCE`,
`HYPOTHESIS` o `UNKNOWN`; ausencia de evidencia no confirma nada.

### Authority Surface Map y count

Enumera cada superficie desde intención hasta render/persistencia. Para cada una
registra `owner`, `input_authority`, `output_authority`, `may_mutate`,
`may_only_suggest`, `may_reject`, `may_delete` y `provenance`.

`AUTHORITY_SURFACE_COUNT` cuenta las superficies que pueden afectar la decisión:
0–2 orienta `LOW`, 3–4 `MEDIUM`, 5+ `HIGH`. Sube un nivel si una superficie
puede mutar autoridad creada por otra. Es una señal de routing, no una métrica
de tamaño de código.

### Mutation Matrix

Para cada dimensión registra:

```text
AUTHORITY_BASE / PROPOSER / CAN_ADD / CAN_REPLACE / CAN_DELETE
CAN_CANONICALIZE / CAN_EXECUTE / PROVENANCE_REQUIRED
```

Ninguna mutación semántica nace de autorización implícita.

### Omisión, conflicto e invariantes

Toda omisión se decide explícitamente como `NO_CHANGE`, `DELETE`, `UNKNOWN`,
`NOT_APPLICABLE` o `ERROR`. `None`, claim ausente y lista vacía nunca adquieren
significado destructivo por accidente.

Todo conflicto entre autoridades resuelve explícitamente a `BASE_WINS`,
`DELTA_WINS`, `BLOCK`, `CLARIFY` o `DIAGNOSTIC_ONLY`. METIS expresa invariantes
falsables, no adjetivos como “robusto”.

### Counterexample Pack

PÓLUX/METIS preparan antes de ARES casos aplicables de: válido, ausente,
malformado, stale, foreign, contradictorio, autorreferencial, ambiguo, polaridad
negativa, autoridad parcial y replay/persistencia. Toda categoría omitida debe
ser una decisión deliberada.

### Contract Completeness Gate

Antes de reclamar ownership de escritura:

```text
MATERIAL_UNKNOWNS = N
AUTHORITY_GAPS = N
OMISSION_DEFINED = YES|NO
CONFLICT_DEFINED = YES|NO
INVARIANTS_DEFINED = YES|NO
COUNTEREXAMPLES_READY = YES|NO
CONTRACT_INCOMPLETE = YES|NO
ARES_AUTHORIZED = YES|NO
```

Si hay un material unknown, un authority gap o cualquier respuesta `NO`,
el gate emite literalmente `CONTRACT_INCOMPLETE=YES` y después
`ARES_AUTHORIZED=NO`. Sólo cuando todos están cerrados emite
`CONTRACT_INCOMPLETE=NO`. ARES no descubre arquitectura: una premisa falsa
exige `STOP + ESCALATE`.

### Framing Gate

Antes de METIS/ARES en `COMPLEX`/`HIGH_RISK`, HERA evalúa cinco preguntas sin
resolverlas en profundidad — sólo determina si el framing está acreditado:

```text
A. CAUSALITY          ¿mecanismo causal reproducido o aún hipótesis?
B. CAPACITY           ¿la palanca tiene headroom para cerrar el hueco?
C. PRODUCT ORACLE     ¿el gate mide producto o sólo su propio test?
D. CONTRARY EVIDENCE  ¿hay evidencia que contradiga la tesis?
E. CHEAP DISCRIMINATOR ¿hay una medición barata que invalide el arco?
```

Outputs: `FRAMING_CONFIDENT`, `CHEAP_MEASUREMENT_FIRST`,
`HADES_CHALLENGE_REQUIRED`, `THESIS_REFUTED`, `HUMAN_DECISION_REQUIRED`.

## 4. Router dinámico y provenance

El modelo solicitado se deriva de:

```text
ROLE DEFAULT + RISK + AUTHORITY_SURFACE_COUNT + DESIGN_REOPEN
+ EVIDENCE_CONFLICT + TASK_TYPE -> DOWNGRADE | KEEP | UPGRADE
```

No es una asignación fija por nombre. El modelo caro se coloca donde pueda
evitar ramas completas de trabajo; se optimiza `TOTAL_REASONING_COST_TO_DECISION`,
no tokens por llamada.

Defaults y cambios permitidos:

- HERA: Astra Light; escala a Astra Medium o Sol High para adjudicar una
  contradicción conceptual entre agentes.
- CÁSTOR: Terra High; Sol Medium sólo ante topología/autoridad ambigua.
- PÓLUX: Sol Light; Sol Medium al cruzar autoridades y Sol High ante P1/P2
  conceptual o disputa no trivial.
- METIS: Sol Medium en `NORMAL`; Sol High en `COMPLEX/HIGH_RISK`; Astra Medium
  es alternativa para síntesis multiperspectiva.
- ARES: Sol Medium; puede bajar a Terra High si el contrato cerrado vuelve la
  implementación mecánica; Sol High sólo para dificultad técnica no local con
  contrato estable.
- HERMES: inicia Sol High para diseñar evidencia; baja a Terra High/Sol Medium
  al ejecutar gates cerrados; sube a Sol Extra High si evidencia acreditada
  discrepa o un gate contradice una invariante.
- ZEUS: contexto fresco, Sol Medium en `NORMAL`, Sol High en `HIGH_RISK` o tras
  un reopen; Sol Extra High para arquitectura que sobrevivió/falló varias veces.
- HADES: Sol Extra High y contexto fresco. Nunca hereda modelo. Astra Light sólo
  puede orquestar investigadores explícitos Sol High o superiores.
- CLÍO: Terra High o Sol Light para distinguir decisión vigente, evidencia,
  historia, hipótesis descartada y resultado superseded.

Cada actor registra:

```text
ROLE / REQUESTED_MODEL / REQUESTED_REASONING / RESOLVED_MODEL
RESOLVED_REASONING / FRESH_CONTEXT / ESCALATION_REASON
```

Si el runtime no expone el modelo real, `RESOLVED_MODEL=UNKNOWN`. Un fallback se
registra con razón; nunca se afirma una resolución no observada.

## 5. Findings y control de loops

HERA adjudica cada finding antes de enviarlo a implementación:

- `LOCAL_DEFECT`: contrato conocido violado → reparación acotada de ARES.
- `DESIGN_GAP`: falta autoridad/arquitectura → METIS y `DESIGN_REOPEN += 1`.
- `NEW_REQUIREMENT`: requiere decisión; no se implementa automáticamente.
- `OUT_OF_SCOPE_RISK`: se registra sin ampliar el paquete.
- `ORACLE_PROBLEM`: detiene la decisión.
- `TOOLING_PROBLEM`: corrige o degrada la evidencia, no el producto.
- `REFUTED`: no produce cambio.

ZEUS busca ángulos y bordes plausibles e intenta falsificar el candidate, pero
es falible. Todo finding debe aportar evidencia y un contraejemplo concretos;
ni su severidad ni el esfuerzo o modelo usado lo convierten en verdad. HERA lo
adjudica como `LOCAL_DEFECT`, `DESIGN_GAP` u otra clase, y puede declararlo
`REFUTED`.
La especulación implausible o sin evidencia no otorga autoridad para cambiar
código.

`DESIGN_REOPEN` es monotónico durante una misión. Incrementa por cambios de
autoridad, omisión, composición, invariante, arquitectura o definición de la
feature; no por typos, errores Git, tooling mishaps, worktree accidental, oracle
maintenance ni tests faltantes de una regla decidida. No se reinicia con un
commit, candidate o agente nuevo.

Con `DESIGN_REOPEN >= 2`:

```text
STOP_PATCH_LOOP
ARES_AUTHORIZED = NO
THIRD_INCREMENTAL_PATCH = FORBIDDEN
HADES_REQUIRED = YES
```

HADES busca el supuesto común y clasifica `BAD_IMPLEMENTATION`, `BAD_CONTRACT`,
`BAD_ORACLE`, `MISSING_PRODUCT_DECISION`, `AUTHORITY_GAP` o
`WRONG_FEATURE_BOUNDARY`. Sus outcomes válidos son `REPLAN`,
`HUMAN_DECISION_REQUIRED`, `ABANDON` y `SIMPLIFY`; no produce el tercer parche.

HADES opera en dos modos: `HADES_CHALLENGE` (antes de implementar, cuando el
framing es dudoso y existe al menos una señal — `CAUSALITY_UNPROVEN`,
`LEVER_HEADROOM_UNKNOWN`, `PRODUCT_ORACLE_WEAK`, `CONTRARY_EVIDENCE_EXISTS`,
`CHEAP_DISCRIMINATOR_AVAILABLE`, `MULTIPLE_SYMPTOMS_MAY_SHARE_CAUSE`,
`HIGH_DOWNSTREAM_COST` o `PREVIOUS_LOOP_HISTORY`) y `HADES_REFRAME` (tras
`DESIGN_REOPEN >= 2` o colapso de la explicación causal). HADES es read-only:
no implementa, no modifica candidate, no arregla bugs. Tras
`CONFIRM`/`MATIZE`/`REFUTE`, HERA adjudica y ARES implementa; sólo una
autorización humana explícita permite a HADES escribir.

## 6. Candidate, passport y ADR

HERA congela `CANDIDATE_HEAD` antes de auditoría. Cualquier cambio posterior
produce `CERTIFICATION_INVALID` y obliga a verificar el nuevo SHA. Un SHA no es
por sí solo autoridad ni permiso de merge.

El passport conceptual contiene `mission_id`, `base_sha`, `candidate_sha`,
branch, worktree, dirty state, scopes, owned/protected paths, risk,
authority-surface count, design reopens, audit round, modelos solicitados y
resueltos, candidate status, decisión, próximas acciones autorizadas y evidence
manifest.

La infraestructura actual `olympus-state` registra run/base/branch/worktree,
ownership, blockers, repairs, gates y candidate freeze/check. Los campos del
passport que no soporte son `GAP_REPORT`; no se duplican en un ledger Markdown
ni se inventan comandos. La autorización de merge permanece separada.

Para ADR, se calcula `MAX_GLOBAL_ADR` entre worktrees. Los workers no reservan
números por iniciativa propia: HERA serializa la asignación final, salvo que una
reserva mecánica real exista y haya sido verificada.

## 7. Evidencia y gates por destino

- `EXPERIMENT_STOPPED/INCONCLUSIVE`: integridad, reproducibilidad relevante,
  ausencia de daño y razón del stop.
- `PROCEED/IMPLEMENTATION_CANDIDATE`: focales, afectados, invariantes y tooling
  aplicable.
- `MERGE_CANDIDATE`: gates completos definidos por repo y blast radius.
- `PRODUCTION_PROMOTION`: máximo nivel requerido y autorización independiente.

No se corren suites por ceremonia. Todo reporte enumera comandos, scope y salida
literal. Warnings, lint, Semgrep y findings separan `BASELINE`, `CANDIDATE` y
`DELTA`; un baseline conocido no equivale a nuevos findings ni se oculta como
ruido.

El autor de un plan no convierte su expected en ground truth al escribirlo.
Toda evidencia distingue `EXPECTED`, `PROVISIONAL_ORACLE`,
`EXPLORATORY_FEEDBACK` y `ADJUDICATED_GOLD`, con provenance suficiente.

La falta de autoridad para `PROCEED` no prueba la conclusión opuesta. Outcomes
válidos: `PROCEED`, `STOP`, `INCONCLUSIVE`, `HUMAN_DECISION_REQUIRED`.

### Integración, failure attribution y audit inheritance

La integración sigue `TOPOLOGY → GATES → FAILURE_ATTRIBUTION →
[ONE_LOCAL_REPAIR] → FREEZE → [GLOBAL_RETRY] → FINAL_PREFLIGHT → PROMOTION`.
`TOPOLOGY` clasifica `FF_READY`, `MAIN_DRIFT_DISJOINT`, `MAIN_DRIFT_OVERLAP`,
`SEMANTIC_CONFLICT` o `UNKNOWN_TOPOLOGY`. El drift puramente
documental/tooling/disjoint se resuelve sin human gate cuando la intención de
integrar es explícita, no cambia semántica productiva, no reescribe historia y
preserva el candidate original; el human gate se reserva para conflicto
semántico/authority/scope/historia.

Los fallos se atribuyen por `nodeid` a `BASELINE_EXISTING`, `CANDIDATE_DELTA` o
`UNKNOWN`, reproduciendo subsets exactos contra baseline/main; no se repiten
suites globales por conteos. La reparación acotada es `LOCAL_REPAIR_BUDGET=1`
con una segunda reparación global sólo si focales/estáticos pasan; no hay
tercera automática. La auditoría se hereda: si un feature ya fue auditado y la
nueva delta es sólo topología/docs/fixture, se audita sólo la nueva delta.

## 8. Métricas y detector de repetición

Por feature, HERA registra sin crear una plataforma paralela:

```text
FEATURE / risk_class / authority_surface_count / models_by_role
implementation_candidates / audit_rounds / design_reopens / bounded_repairs
hades_invocations / human_gates / full_suite_runs / findings_by_severity
new_information_per_round / final_decision
```

Antes de repetir una ronda declara si espera `NEW_HYPOTHESIS`, `NEW_EVIDENCE`,
`NEW_COUNTEREXAMPLE` o `NEW_AUTHORITY_DECISION`. Si todas son `NO`, HERA emite
`REJECT_REDUNDANT_ROUND`. Las métricas explican costo a decisión; no premian más
actividad.

## 9. Benchmark Semantic Bridge

Este hardening no autoriza otro candidate. La próxima misión separada puede usar
como baseline: 2 implementation candidates, 2 rondas ZEUS frescas, 2 design
reopens, 1 HADES, 1 human gate y decisión `NO_GO`.

La comparación pregunta si el mapa de autoridad, omisión y conflicto se cerró
antes de ARES; si METIS redujo reopens; si ZEUS encontró defectos locales en vez
de decisiones ausentes; si hubo menos código descartado, candidates y gates
repetidos; y si los modelos caros evitaron trabajo downstream. Un stop temprano
bien acreditado puede ser mejora. El benchmark nunca se manipula para obtener
`GO` y `NEXT_RUN_AUTHORIZED=NO` hasta una misión humana explícita.

## 10. Cierre

HERA mantiene estado y serializa promoción. ARES entrega diff y gates, pero no
congela ni se autoaudita. HERMES define evidencia proporcional. ZEUS recibe
contrato, SHA, invariantes, scope, evidencia y limitaciones sin narrativa
persuasiva del writer. Sólo un candidate congelado, con evidencia aplicable y
adjudicación independiente, puede avanzar a la siguiente decisión autorizada.

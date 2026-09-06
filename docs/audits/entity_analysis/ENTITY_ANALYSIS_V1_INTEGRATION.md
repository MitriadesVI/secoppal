# Entity Analysis V1 — integración sobre Procurement Relationship Foundation

2026-09-06. Rama `integration/entity-analysis-v1`, worktree propio. Un agente,
sin merge ni push. Este documento preserva el informe histórico de `b4425ac`:
**NO_GO** por la decisión pendiente sobre Foundation y la suite completa.

Actualización autorizada de Foundation: se fija exclusivamente el diff histórico
a `c79f8460c84ab26631c2534af106405cb16803f2..d608336a32553ac11a69924d663491ce852e1446`;
el aislamiento runtime sigue inspeccionando el árbol actual. Las nuevas
regresiones prueban el rango literal, exclusión de cambios posteriores de Entity
Analysis y fallo cerrado ante un consumidor actual. No se modifica ni repinea
ningún artefacto Foundation ni territorial. Verificación previa al commit:
`191 passed in 12.86s`; Ruff PASS, Semgrep baseline 10/delta 0, module-size,
lint-core y diff-check PASS; ambos replays byte-exactos y Baranoa 1037/1376.
La única suite completa se ejecuta después del commit acotado de esta corrección;
su resultado y el veredicto final se entregan en el reporte de cierre de la tarea.

## Cambio y frontera

Se conservaron los seis commits fuente mediante cherry-pick; sus patch IDs son
idénticos ([mapa](integration/source-commits.json)). `9443125` no se cherry-pickeó.
Su intención se recreó manteniendo intacto el detector AST de `fac51c0`.

`_runtime_imports()` enumera todos los consumidores reales. La autorización
exacta contiene sólo `app/core/entity_profile/scope.py`; el gate histórico
`RUNTIME_IMPORTS` cuenta los no autorizados. Mutaciones con un segundo consumidor
en Procurement Relationships, Entity Profile y otro paquete fallan cerrado.
Hypothesis comprueba que la autorización nunca oculta imports adicionales.

Bogotá conserva `buyer_group.id`, versión, los 38 miembros y el scope. Sólo
`entity_profile` omite la declaración textual completa. El grupo mantiene su
`group_id` interno sin cambiar el contrato público de referencia `id`.
Las rutas analítica puntual, Actor Profile y tabular conservan precedencia.
Actor Profile se prueba con índice acreditado de fixture y flag habilitado
sólo por tests; su default productivo sigue apagado.

ADR-081 mantiene metadata posterior a BuyerUnit/BuyerGroup productivo y sólo
`PRODUCT_RECOGNIZED`. El barrido de 40 worktrees encontró únicamente la misma
decisión en fuente/integración ([evidencia](integration/adr-scan.json)). La
bitácora añade ADR-081 después de ADR-092, sin reordenar ni eliminar historia.
Foundation, identidades, edges, QueryRouter, QueryPlan y configuración no cambian.

## Verificación y salidas literales

Preflight: Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10; Codegraph 0.9.6.
Codegraph MCP está expuesto en la sesión; se usó CLI con índice propio inicializado
y sincronizado en el worktree. `query`, `callers`, `callees` e `impact`
identificaron el radio de workflow/Actor/BuyerGroup. `affected` devolvió una lista
vacía; se complementó con el impacto y selección manual de regresiones.

```text
$ .venv/bin/ruff check app/ tests/ scripts/
All checks passed!

$ .venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
Ran 3 rules on 257 files: 10 findings.

$ .venv/bin/python -m pytest <25 archivos focales enumerados abajo> -q
2 failed, 505 passed in 48.64s

$ .venv/bin/python -m pytest tests/test_entity_profile_workflow.py tests/test_actor_flag_off.py -q
32 passed in 6.66s

$ .venv/bin/python -m pytest tests/test_entity_profile_scope.py tests/test_entity_profile_queries.py tests/test_entity_profile_workflow.py tests/test_territorial_knowledge.py tests/test_territorial_import_guard.py tests/test_module_size_limits.py -q
113 passed in 10.02s

$ make lint-core
app/core/ limpio

$ git diff --check fac51c0
```

Corrí los tests focales indicados, no la suite. La salida completa, incluido el
primer fallo corregido de la aserción `group_id` frente a `id`, está preservada
sin alterar whitespace en [literal-logs.json](integration/literal-logs.json).
Semgrep sigue rojo con diez hallazgos históricos; delta global y propio cero,
comparando path, regla y fragmento literal con el baseline de `fac51c0`.
El hallazgo existente de `execution.py` sólo se desplaza de línea. Se conserva
el límite de Semgrep: escanea `app/` y omite un archivo mayor de 1 MB.

Primera selección focal: Entity Profile scope/queries/workflow; Territorial
Knowledge/import guard; Procurement Relationships/layers model/layers sources;
analytics execution/analytical queries; Actor Profile/render/resolution
orchestration/conversation; declared BuyerGroups/group analytics truth;
workflow architecture/results/persistence; orchestrator; parallel SECOP
execution/dual dataset execution; timeout handling/transparency; module size.

Los 122 archivos protegidos de Foundation, Procurement Layers, autoridades y
artefactos son byte-idénticos a `fac51c0`. El replay territorial es byte-exacto
y la reconstrucción offline de Baranoa conserva 1.037 nodos y 1.376 edges
([invariantes y digests](integration/invariants.json)).

## Dogfood

Se ejecutaron tres overviews live en serie y cuatro controles puntuales: 22
requests HTTP. Cada overview usó seis llamadas. Baranoa y Bogotá presentan siete
hechos; sus conteos de procesos/contratos y valores coinciden exactamente con
los controles del mismo `WHERE`. Barranquilla conserva procesos cuando las
tres agregaciones contractuales agotan timeout; no inventa ceros. Estados,
latencias y cifras constan en el [dogfood](ENTITY_ANALYSIS_V1_DOGFOOD.md).

Reproducción opcional de esa muestra live, con un archivo de salida nuevo:

```bash
.venv/bin/python docs/audits/entity_analysis/integration/reproduce_dogfood.py --output /tmp/entity-integration-new-observation.json
```

## Bloqueo histórico fuera del carril en b4425ac

`tests/test_procurement_relationships.py::test_complete_offline_replay_canaries_and_runtime_isolation`
falla con `ValueError: invalid procurement relationship contract: closed gates`.
`scripts/audits/relationship_foundation/gates.py::isolation` usa
`git diff c79f846 --name-only` contra todo el checkout y atribuye 18 archivos de
Entity Analysis a efectos de Foundation. Sus referencias runtime actuales son
cero. No es pérdida de identidades, edges ni recibos
([diagnóstico](integration/foundation-blocker.json)).

El [parche propuesto](integration/foundation-gate-proposal.json), **no aplicado**,
fija únicamente esa comparación histórica a los blobs de
`c79f8460c84ab26631c2534af106405cb16803f2..d608336a32553ac11a69924d663491ce852e1446`.
Los imports y dependencias se siguen comprobando sobre el árbol actual. Se
ejecutó la propuesta sólo en memoria, en un proceso aislado: registro y once
artefactos del replay fueron byte-exactos. No se repinó evidencia.

AGENTS.md, sección 3, prohíbe editar archivos de otro carril incluso para un
arreglo obvio. Se solicitó autorización explícita para ese archivo y una
regresión focal; está pendiente. Se conserva el fallo. No se ejecutó la suite
completa antes de resolver la decisión para respetar su única ejecución final.

Contradicción documental observada: `.claude/CLAUDE.md` prescribe
`ruff → pytest → semgrep` y contiene disponibilidad antigua; se siguió el orden
vigente de AGENTS.md: `ruff → semgrep → pytest`, con preflight real. No se editó
esa documentación compartida ni los hallazgos históricos.

## Veredicto histórico de b4425ac

```text
ENTITY_ANALYSIS_V1_INTEGRATION: NO_GO
MAIN_BASE: fac51c09b79674fde00cebe4597eeafa43f04234
SOURCE_HEAD: 94431254efbc65267d13ce7694e2bf6cdb0565b0
INTEGRATION_HEAD: ver HEAD de integration/entity-analysis-v1
SOURCE_COMMITS_APPLIED: 0e3c4ed 4b23e35 fbdd99d 7dd274a b93c244 e8ba54e
9443125: NOT_CHERRY_PICKED
INTENT_REIMPLEMENTED: YES
ACTUAL_RUNTIME_CONSUMERS: [app/core/entity_profile/scope.py]
AUTHORIZED_RUNTIME_CONSUMERS: [app/core/entity_profile/scope.py]
UNAUTHORIZED_RUNTIME_CONSUMERS: []
PAYLOAD_SHA: c0d6933182a7ccbdecaf465b6380ecca9507c816719607d3d64fe630ddbc9c63
BYTE_EXACT: PASS
PROCUREMENT_LAYERS_RUNTIME_DEPENDENCY: NONE
GRAPH_DATABASE: NONE
PROCUREMENT_BARANOA_REGRESSION: PASS
NODES: 1037
EDGES: 1376
BUYER_UNIT: PASS
BUYER_GROUP: PASS, 38 miembros estructurados
TERRITORY_DIRECT: CLARIFICATION, 0 llamadas
ACTIVITY: PASS
VALUES: PASS, median UNAVAILABLE_V1
MODALITIES: PASS
SUPPLIERS: PASS
TEMPORAL: PASS
RECENT: PASS, máximo 3 por dataset
QUERY_BUDGET: PASS
CALLS_MAX: 6; focos 1-2; sin presupuesto BUDGET_EXHAUSTED
BARANOA: PASS, 6 calls, 1119.4 ms, exact-WHERE consistency PASS
BOGOTA: PASS, 6 calls, 3370.8 ms, exact-WHERE consistency PASS
BARRANQUILLA: PARTIAL honesto, 6 calls, 8344.4 ms
PRECEDENCE_ANALYTICS: PASS
PRECEDENCE_ENTITY: PASS
PRECEDENCE_ACTOR: PASS, fixture acreditada; default productivo OFF
PRECEDENCE_NORMAL: PASS
CORPUS_SINGLE: 40/40 PASS
CORPUS_MULTITURN: 15/15 PASS
RUFF: PASS
SEMGREP_BASELINE: 10
SEMGREP_DELTA: 0
MODULE_SIZE: PASS
LINT_CORE: PASS
DIFF_CHECK: PASS
FULL_SUITE: NOT_RUN, pendiente decisión de gate Foundation
PASSED: NOT_MEASURED
SKIPPED: NOT_MEASURED
XFAILED: NOT_MEASURED
FAILED: NOT_MEASURED
NEW_XFAILS: NOT_MEASURED; no se añadieron marcas xfail
ADR: 081
MERGE_TO_MAIN: NO
PUSH: NO
```

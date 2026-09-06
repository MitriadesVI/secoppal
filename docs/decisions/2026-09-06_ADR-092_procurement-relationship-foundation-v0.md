# ADR-092 — Procurement Relationship Foundation V0

Fecha: 2026-09-06. Estado: Accepted — SHADOW_FOUNDATION, sin runtime.

## Problema

Cuenta SECOP, grupo de proveedores y Actor tipado son identidades distintas.
Una sola etiqueta PARTIAL confundía soporte de una afirmación con cobertura del
universo, y permitía interpretar coocurrencias derivadas como hechos primarios.

## Decisión

Se acepta el contrato versionado app/data/procurement_relationship_registry_v0.json
con exactamente nueve tipos de nodo, ocho relaciones primarias y dos diferidas.
ASSERTION_STATUS y COVERAGE_STATUS son dimensiones independientes. RUNTIME_STATUS
conserva PRODUCTIVE/SHADOW/DEFERRED; los ocho edges V0 son SHADOW. El estado de una
fuente productiva existente no promueve este esquema. ADR-080 continúa vigente.

SECOP_SUPPLIER usa código de plataforma, SUPPLIER_GROUP conserva su código y tipo
propios y sólo referencia una cuenta por testigos exactos. Actor mantiene
IdentityKey(NIT|CC|UNKNOWN, base_number). REPRESENTS_ACTOR sigue DEFERRED.
PAA_ITEM no tiene identidad materializable; PLANNED_AS sigue DEFERRED.

El join directo BDOS→REQ fue refutado en 1.250 contratos. Se admite únicamente
jbjy.proceso_de_compra = p6dx.id_del_portafolio, seguido de result_identity,
cuando hay un único proceso estable. Las 19 filas ambiguas y 23 huérfanas no
producen edges. HAS_RECORDED_CONTRACT expresa asociación registrada y
PUBLISHED_CONTRACT atribuye publicación, sin presumir firma o adjudicación.

COMPETES_WITH, PARTNERED_WITH y otras métricas de coocurrencia son derivadas,
con ventana, cobertura y definición explícitas; nunca autoridad primaria.
No se inventan instituciones para canonicals sin logical_id. No se usa NIT para
territorio, BuyerUnit, cuenta de plataforma o membresía. Se preservan las autoridades
result_identity, Actor Profile, BuyerGroups, bindings y Territorial Knowledge.

## Evidencia y validación

[Arquitectura](../architecture/PROCUREMENT_RELATIONSHIP_FOUNDATION_V0.md),
[recibos propios](../../artifacts/audits/relationship_foundation_v0/observation_manifest.json),
[gates](../../artifacts/audits/relationship_foundation_v0/gates.json),
[validación](../../artifacts/audits/relationship_foundation_v0/validation.json).

Auditoría histórica b936d0f fue hipótesis, no autoridad heredada ni cherry-pick.
Se preserva evidencia negativa y se exige replay byte-exacto offline, focales con
Hypothesis, Ruff, Semgrep con cero delta propio, module-size y diff-check.

Numeración: barrido de todos los worktrees registrados y sus docs, incluidos
ocultos/ignorados; máximo global ADR-091. Evidencia: adr_scan.json. Los rangos
081–090 permanecen reservados para los otros carriles.

## Alcance y no decisiones

Sólo contrato y loader shadow. Sin warehouse, graph database, red desde el loader,
imports de runtime, cambios de query, Entity Analysis, Actor Profile, Radar o SoQL.
No autoriza integración, merge ni push. NEXT: ENTITY_ANALYSIS_V1_INTEGRATION,
sujeto a una decisión posterior. La cobertura parcial y las relaciones diferidas
son límites explícitos del contrato aceptado, no capacidades entregadas.

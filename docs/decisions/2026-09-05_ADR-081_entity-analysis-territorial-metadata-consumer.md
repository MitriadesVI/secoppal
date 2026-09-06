# ADR-081 — Territorial Knowledge como metadata posterior a BuyerUnit

**Fecha:** 2026-09-05
**Estado:** Accepted — consumidor runtime acotado de Entity Analysis V1.
**Predecesora:** [ADR-080](2026-09-04_ADR-080_territorial-knowledge-shadow-registry.md).
**Evidencia:** [arquitectura](../architecture/ENTITY_ANALYSIS_V1.md) y
[dogfood](../audits/entity_analysis/ENTITY_ANALYSIS_V1_DOGFOOD.md).

## Contexto

ADR-080 aceptó Territorial Knowledge V0 como registro shadow sin consumidores
runtime para impedir que territorio, candidatos de conocimiento o evidencia de
afiliación se convirtieran en autoridad de buyer. Entity Analysis V1 necesita
mostrar contexto territorial e IDs institucionales estables, pero su scope ya
se resuelve mediante las autoridades productivas existentes.

El riesgo no está en leer metadata: está en permitir que el registro resuelva
texto, cree scope o altere el predicado SECOP. La integración debe demostrar
que el orden de autoridad no se invierte.

## Decisión

Se autoriza a `app/core/entity_profile/scope.py` como primer consumidor runtime
acotado de Territorial Knowledge V0 bajo esta secuencia obligatoria:

1. el resolver productivo entrega una BuyerUnit canónica de alta confianza, o
   `buyer_groups.json` entrega una referencia versionada;
2. Entity Analysis fija su scope exclusivamente con esa autoridad;
3. sólo después hace lookup exacto de `product_buyer_unit` en Territorial
   Knowledge;
4. sólo una institución `PRODUCT_RECOGNIZED` puede aportar metadata.

La metadata autorizada es: `institution_id`, DIVIPOLA, municipio,
departamento, macro-región, familia institucional, estado de conocimiento y
referencias de procedencia.

Territorial Knowledge no puede:

- resolver texto, BuyerUnit o BuyerGroup;
- crear buyer, alias o binding;
- promover candidatos o resolver perfiles identity-pending;
- inferir/recalcular membresía institucional;
- añadir, quitar o cambiar un filtro SoQL.

`VERIFIED_LOGICAL_UNRECOGNIZED` y `ADJUDICATED_OFFLINE_CANDIDATE` siguen siendo
contexto shadow y jamás scope productivo. Un lookup sin coincidencia no degrada
ni amplía el scope: simplemente no añade metadata.

## Consecuencias

Entity Analysis puede conservar referencias graph-ready sin base de grafos y
sin confundir `Territory` con `BuyerUnit`. Baranoa mantiene simultáneamente el
territorio DIVIPOLA 08078 y la BuyerUnit
`ALCALDIA MUNICIPAL DE BARANOA`; ninguno sustituye al otro.

La premisa `RUNTIME_IMPORTS = 0` de ADR-080 queda sustituida sólo para este
consumidor por:

```text
ENTITY_PROFILE_RUNTIME_CONSUMERS = 1
AUTHORIZED_CONSUMERS = 1
UNAUTHORIZED_CONSUMERS = 0
TEXT_RESOLUTION_FROM_KNOWLEDGE = 0
QUERY_SCOPE_FROM_KNOWLEDGE = 0
CANDIDATE_PRODUCTIVE_USE = 0
BUYER_GROUP_MEMBERSHIP_REDEFINED = 0
```

El inventario `_runtime_imports()` conserva todos los imports reales detectados
por el AST compartido de `fac51c0`, incluido `scope.py`. La autorización es
exactamente `frozenset({"app/core/entity_profile/scope.py"})`.
`_unauthorized_runtime_imports()` devuelve actual menos autorizado; el gate
histórico `RUNTIME_IMPORTS = 0` cuenta esos consumidores no autorizados.
La provenance textual no constituye import runtime. Procurement Relationships
no está autorizado: cualquier import real suyo hace fallar el gate.

El replay permanece byte-exacto, sin repin del JSON V0 ni cambio a su estado
histórico. SHA-256:
`c0d6933182a7ccbdecaf465b6380ecca9507c816719607d3d64fe630ddbc9c63`.
Esta ADR no autoriza ningún segundo consumidor; cada ampliación requiere una
decisión nueva. Entity Analysis no depende del runtime de Procurement Layers,
no carga snapshots ni hace traversal (`PROCUREMENT_LAYERS_RUNTIME_DEPENDENCY =
NONE`, `GRAPH_DATABASE = NONE`). ADR-092 permanece intacta.

## Numeración

Antes de reservar se recorrieron los `docs/` de todos los worktrees registrados
por `git worktree list --porcelain`. El máximo global observado fue ADR-080 y no
se encontró ninguna reserva 081–090. Carril A usa ADR-081 de su rango 081–085.

Reconfirmación de integración, 2026-09-06: barrido global de los worktrees
registrados; el único ADR-081 previo corresponde a esta misma decisión en
`feature/entity-analysis-v1-core`. ADR-092 conserva expresamente la reserva
081–090 de los otros carriles. No se renumera por el orden de integración.

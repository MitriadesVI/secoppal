# ADR-080 — Territorial Knowledge V0 como autoridad shadow estable

**Fecha:** 2026-09-04
**Estado:** Accepted — `SHADOW_NO_RUNTIME`; sin promoción ni integración runtime.
**Evidencia:** [Territorial Knowledge V0](../architecture/TERRITORIAL_KNOWLEDGE_V0.md),
[`territorial_knowledge_v0.json`](../../app/data/territorial_knowledge_v0.json).

## Contexto

M2A acredita 994 unidades lógicas territoriales: 625 reconocidas por producto y
369 verificadas todavía no reconocidas. M2B añade 55 candidatas offline. M2B.1
añade 43 candidatas y enlaza siete perfiles a canonicals productivos existentes,
sin crear siete instituciones nuevas. Quedan 826 perfiles con afiliación
verificada e identidad pendiente.

Mantener estos resultados sólo en artefactos de fase dispersa dificulta su
consumo reproducible, pero promoverlos al gazetteer confundiría conocimiento
acreditado con BuyerUnits productivas. La compilación requiere una autoridad
separada y explícitamente shadow.

## Decisión

Se acepta `app/data/territorial_knowledge_v0.json` como registro shadow estable
del conocimiento territorial M1A→M2B.1. Contiene exactamente 1.103 territorios y
1.099 instituciones: 632 `PRODUCT_RECOGNIZED`, 369
`VERIFIED_LOGICAL_UNRECOGNIZED` y 98 `ADJUDICATED_OFFLINE_CANDIDATE`.

El registro conserva estas fronteras:

1. `Territory` y `Institution` son dimensiones distintas; DIVIPOLA nunca se
   convierte en BuyerUnit.
2. Un NIT es evidencia y jamás `institution_id`, autoridad de afiliación o regla
   de fusión.
3. Los 826 perfiles identity-pending son referencias de deuda y no instituciones.
4. Los siete enlaces M2B.1 enriquecen una institución productiva existente cada
   uno; no se cuentan como candidatas ni se duplican.
5. `product_buyer_unit` sólo existe para `PRODUCT_RECOGNIZED`. Una candidata de
   conocimiento no es productiva.
6. Bogotá referencia sus tres BuyerGroups declarados sin copiar ni recalcular
   membresía. El registro no define miembros de BuyerGroup.
7. Baranoa `08078` referencia `ALCALDIA MUNICIPAL DE BARANOA` como institución
   reconocida sin sustituir al territorio.

`app/core/territorial_knowledge.py` se limita a cargar, validar y construir
índices exactos read-only. Ningún módulo runtime lo importa. Quedan fuera
QueryRouter, SoQL, Radar, Analytics, Formatter, follow-ups, aliases, bindings,
BuyerGroups y `result_identity`.

## Reproducción y gates

`scripts/build_territorial_knowledge_v0.py` consume únicamente autoridades
congeladas, ordena toda colección, omite timestamps y produce bytes idénticos en
cada replay. El registro conserva SHA-256 de sus inputs y falla ante deriva de
conteos, IDs, territorios, estado productivo o referencias.

Los gates aceptados son:

```text
TERRITORIES = 1103
DUPLICATE_INSTITUTION_IDS = 0
MULTI_TERRITORY_INSTITUTION = 0
IDENTITY_PENDING_AS_INSTITUTION = 0
UNRESOLVED_AFFILIATION_AS_MEMBERSHIP = 0
M2B1_EXISTING_LINKS_DUPLICATED = 0
RUNTIME_IMPORTS = 0
BUYER_GROUP_MEMBERSHIP_REDEFINED = 0
```

La aceptación fue validada con focales territoriales, corpus 89/89, Ruff,
`lint-core`, module-size, `git diff --check`, replay byte-exacto y suite completa
de 11.145 pruebas verdes, dos skips y catorce xfails esperados.

## Numeración y siguiente paso

Antes de reservar esta decisión se inspeccionaron con `rg --hidden --no-ignore`
los directorios `docs/` de los 29 worktrees registrados. El máximo global era
ADR-079. Se reserva ADR-080 para esta autoridad shadow.

No se autoriza promoción, import runtime, alias, binding, cambio de BuyerGroup ni
mutación de evidencia histórica. El siguiente bloque es `ENTITY_ANALYSIS_V1`.

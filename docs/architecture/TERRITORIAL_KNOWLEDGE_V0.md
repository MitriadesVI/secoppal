# Territorial Knowledge V0

**Fecha:** 2026-09-04. **Estado:** Accepted — `SHADOW_NO_RUNTIME`.
**Runtime:** sin imports ni integración. **Promociones:** ninguna. **PUSH:** NO.
**NEXT:** `ENTITY_ANALYSIS_V1`.

**Decisión:**
[ADR-080](../decisions/2026-09-04_ADR-080_territorial-knowledge-shadow-registry.md).

## Decisión

`app/data/territorial_knowledge_v0.json` es la autoridad shadow estable que
compila el conocimiento acreditado entre M1A y M2B.1. No sustituye ninguna
autoridad productiva, no ejecuta consultas y no convierte candidatos de
conocimiento en BuyerUnits. Su única superficie de código es un loader que
valida el archivo y construye índices exactos read-only.

El barrido previo a reservar esta decisión usó `git worktree list --porcelain`
y `rg --hidden --no-ignore` sobre `docs/` en los 29 worktrees registrados. El
máximo global fue ADR-079 en este worktree; por tanto, la nueva autoridad se
reserva como ADR-080.

## Modelo y cardinalidades congeladas

Un `Territory` conserva DIVIPOLA, municipio, departamento, macro-región y tres
listas de referencias: instituciones, perfiles cuya identidad sigue pendiente
y BuyerGroups declarados. Una `Institution` conserva ID lógico, territorio,
familia, estado de conocimiento, display, BuyerUnit productiva opcional,
perfiles, autoridades de identidad y procedencia.

| Componente | Conteo | Interpretación |
|---|---:|---|
| Territorios DIVIPOLA | 1.103 | Geografía; nunca BuyerUnit |
| Unidades lógicas M2A | 994 | 625 productivas + 369 verificadas no reconocidas |
| Candidatas offline M2B | 55 | Conocimiento; cero aplicación productiva |
| Instituciones nuevas M2B.1 | 43 | Candidatas offline |
| Enlaces M2B.1 | 7 | Enriquecen canonicals productivos existentes |
| Instituciones totales | 1.099 | 632 productivas + 369 no reconocidas + 98 candidatas |
| Perfiles identity-pending | 826 | Deuda referenciada; no instituciones |

Los siete enlaces M2B.1 no se materializan como candidatas adicionales. Cada
uno usa el ID lógico estable emitido por M2B.1, conserva el canonical ya
declarado en `aliases_db.json` como `product_buyer_unit` y anexa exactamente un
perfil acreditado. Así se representa una sola institución por enlace sin
confundir el canonical productivo con el perfil físico.

## Estados de conocimiento

- `PRODUCT_RECOGNIZED`: existe una BuyerUnit productiva declarada. El campo
  `product_buyer_unit` contiene su canonical exacto.
- `VERIFIED_LOGICAL_UNRECOGNIZED`: M2A acreditó identidad lógica y territorio,
  pero no existe BuyerUnit productiva reconocida.
- `ADJUDICATED_OFFLINE_CANDIDATE`: M2B o M2B.1 acreditó una candidata de
  conocimiento. `product_buyer_unit` es `null` y la cobertura productiva sigue
  sin cambios.

El `institution_id` siempre proviene de IDs lógicos M2A/M2B/M2B.1. Un NIT puede
aparecer en la evidencia congelada, pero jamás se copia como ID ni decide una
afiliación, una fusión o una membresía.

## Límites de autoridad

`Territory != BuyerUnit`. Un territorio puede referenciar cero o muchas
instituciones; esa relación no transforma el territorio en comprador.
`Registry != BuyerGroup membership`: el registro sólo guarda IDs de grupos ya
declarados. Bogotá (`11001`) referencia `bogota_central`, `bogota_executive` y
`bogota_local_mayoralties`; los miembros permanecen exclusivamente en
`app/data/buyer_groups.json` y no se copian ni recalculan.

Baranoa (`08078`) conserva su registro territorial propio y referencia como
institución `ALCALDIA MUNICIPAL DE BARANOA`, con estado
`PRODUCT_RECOGNIZED`. La Alcaldía no reemplaza el territorio y la mención
territorial no crea un filtro de comprador.

Los 826 perfiles pendientes provienen del remanente exacto M2B.1. Sólo entran
en `identity_pending_profile_refs`; no reciben `institution_id`. Perfiles con
afiliación territorial no resuelta no pertenecen a territorio alguno y quedan
fuera del registro.

Quedan fuera por diseño `QueryRouter`, SoQL, Radar, Analytics, Formatter,
follow-ups, aliases, bindings, BuyerGroups y `result_identity`. La exclusión es
estructural: ningún módulo de runtime importa el loader y el JSON no contiene
membresía de grupos.

## Construcción y reproducción

La generación es offline y ordena territorios por DIVIPOLA, instituciones por
ID y todas las referencias como conjuntos estables. No incorpora timestamps ni
estado de entorno. Registra SHA-256 de cada input congelado y serializa JSON con
claves ordenadas, UTF-8 y newline final.

```bash
.venv/bin/python scripts/build_territorial_knowledge_v0.py
.venv/bin/python scripts/build_territorial_knowledge_v0.py --check
```

Un replay a otra ruta debe ser byte-idéntico:

```bash
.venv/bin/python scripts/build_territorial_knowledge_v0.py \
  --output /tmp/territorial_knowledge_v0.json
cmp app/data/territorial_knowledge_v0.json /tmp/territorial_knowledge_v0.json
```

## Loader read-only

`app/core/territorial_knowledge.py` expone exclusivamente carga, validación e
índices exactos e inmutables:

```python
get_territory(divipola)
get_institution(institution_id)
institutions_for_territory(divipola)
institutions_for_family(family)
```

No normaliza entradas, no resuelve lenguaje, no infiere territorio, no proyecta
nombres físicos y no ejecuta consultas. Un schema, estado, gate, referencia o
cardinalidad divergente falla durante la carga.

## Gates de aceptación

El builder y el loader cierran los siguientes gates:

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

La aceptación exige pruebas focales, corpus 89/89, Ruff, `make lint-core`,
module-size, `git diff --check`, replay byte-exacto y suite completa. Este ADR
acepta sólo la autoridad shadow; cualquier import de runtime, promoción,
alias, binding o redefinición de grupo requiere una decisión posterior.

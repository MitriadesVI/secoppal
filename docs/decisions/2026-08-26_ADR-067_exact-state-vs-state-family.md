# ADR-067 — Estado léxico exacto frente a familia semántica

> **Registro canónico.** Esta decisión está registrada en
> [`docs/decisions_log.md`](../decisions_log.md), índice canónico de ADR del
> repositorio. La autoridad *as-built* es
> `docs/audits/2026-08-26_HCA_FINAL_P1_RUNTIME_FIXES.md`.

**Estado:** Accepted
**Fecha:** 2026-08-26
**Rama:** `feature/hca-final-p1-runtime-fixes`

```text
AS_BUILT_AUTHORITY:
  docs/audits/2026-08-26_HCA_FINAL_P1_RUNTIME_FIXES.md
```

## Contexto

`app/core/estado_families.py` traduce vocabulario natural de estado a valores
reales de SECOP agrupados por familia de ciclo de vida. El mapa era único:
todo token conocido resolvía a una familia y proyectaba **todos** sus valores.

Esa uniformidad producía dos errores opuestos que el HCA Oracle Review confirmó
como defectos de runtime:

- **HCA-STATE-005** — "procesos publicados de aseo en cesar" devolvía
  `estado_del_procedimiento IN ('Publicado', 'Borrador', 'En aprobación',
  'Aprobado', 'Abierto')`. El usuario nombró un valor literal del dataset y
  recibió borradores.
- **HCA-STATE-007** — "contratos en ejecución de la alcaldía de Medellín"
  declaraba `estado_family = contrato_activo` en el frame y llegaba a la fuente
  como `estado_contrato = 'En ejecución'`, perdiendo `Modificado` y
  `Prorrogado`, que ADR-003 declara parte de la misma familia.

La causa común no es el catálogo sino la ausencia de una distinción: **nada
garantizaba que la forma de resolver un estado y la forma de proyectarlo
coincidieran**. Una familia podía declararse sin materializarse (la ruta de
`intent_vocabulary` no conoce el dataset y nunca escribió filtros), y un literal
podía materializarse como familia.

## Decisión

Un token de estado se resuelve por **uno de dos contratos**, y la proyección a
la fuente corresponde siempre al contrato con que se resolvió.

1. **Léxico exacto** — el token nombra un valor literal del dataset. Proyecta
   ese valor y solo ese. `LEXICAL_EXACT_STATE` tiene precedencia sobre
   `NATURAL_TO_FAMILY` y deriva la ubicación (familia, campo) de
   `ESTADO_FAMILIES` en vez de duplicarla: un literal que no exista en el
   catálogo no puede resolverse como exacto.

   **INV-EXACT-STATE-DOES-NOT-WIDEN-TO-FAMILY.**

2. **Familia semántica** — el token describe una etapa del ciclo de vida sin
   nombrar un valor. Proyecta la familia completa autorizada para el dataset.

   **INV-STATE-FAMILY-PROJECTS-COMPLETE.** El invariante se aplica en
   `app/core/estado_projection.py` sobre los params ya resueltos, no en cada
   ruta que declara familia, de modo que cualquier camino queda cubierto. El
   proyector **rellena y nunca ensancha**: un campo de estado que ya trae
   valores es una decisión previa y se respeta intacta.

Los dos invariantes se necesitan mutuamente. Sin el segundo, una familia
declarada se proyecta a medias; sin el primero, el segundo ensancharía
"publicados" al funnel completo.

### Criterio de admisión al catálogo exacto

Deliberadamente restrictivo. Un token entra solo si cumple **las tres**:

1. es el nombre literal de un valor real del dataset;
2. su familia tiene más de un valor para ese dataset — si no, exacto y familia
   coinciden y no hay nada que decidir;
3. no es a la vez vocabulario que describe la familia entera.

Por (3) quedan **fuera**:

- `abiertos`, `vigentes`, `disponibles` — son el lenguaje natural de la oferta
  abierta. `Abierto`, además, es un valor residual de versiones antiguas del
  dataset (≈31K filas): tratarlo como petición literal vaciaría la búsqueda.
- `en ejecución` — ADR-003 le asigna los tres estados de contrato activo. Pide
  la familia, no un literal.

Entran `publicado/publicados/publicadas` → `Publicado` y
`borrador/borradores/en borrador` → `Borrador`.

## Consecuencias y límites

La policy de oportunidad vigente (O1B: `Publicado` + `Abierto` más ventana de
descubrimiento) **no cambia**. "Oportunidades de aseo en Cesar" sigue
resolviendo por familia; "procesos publicados" ya no.

El invariante de proyección corrige, además de HCA-STATE-007, un caso de la
misma naturaleza que nadie había reportado: "procesos desiertos" declaraba
`estado_family = no_procede` y consultaba la fuente **sin ningún filtro de
estado**. Ahora proyecta `IN ('Cancelado', 'Suspendido')`.

Esta decisión **no** toca el alcance de liquidación. "Contratos liquidados" y
"procesos liquidados" (HCA-STATE-003, HCA-SOURCE-002) siguen exactamente como
estaban: qué significa *liquidado* en SECOP sigue sin autoridad suficiente y es
trabajo aparte.

`app/core/query_plan.py` no se tocó: sigue en 299 LoC y la `NEXT_TOUCH_RULE`
no se invocó.

## Evidencia de aceptación

- HCA congelado, corpus sha `262c225c…` sin editar: `HCA-STATE-005` y
  `HCA-STATE-007` FAIL → PASS, **0 casos rotos**;
- corpus focal `hca_final_p1_fixes.yaml`: 7 casos de estado en verde, incluidos
  los anti-sentinels de policy de oportunidad y de liquidación;
- fuente Socrata: `Publicado` y `Borrador` son valores reales y disjuntos (0
  filas en la conjunción); los tres estados de `contrato_activo` existen por
  separado — el truncamiento perdía filas legítimas y el ensanchamiento traía
  filas no pedidas;
- diferencial de SoQL sobre 50 consultas adversariales contra `eb8cfe1`: los
  únicos cambios de filtro de estado son los cuatro previstos.

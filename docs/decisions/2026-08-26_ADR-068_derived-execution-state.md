# ADR-068 — El estado derivado de ejecución no persiste como estado del usuario

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

`resolved_params` mezcla dos cosas de naturaleza distinta:

- **estado semántico del usuario** — tema, territorio, comprador, dinero,
  tiempo, estado, modalidad. Es lo que la persona pidió y debe sobrevivir a un
  follow-up;
- **estado derivado de ejecución** — lo que el motor calcula *después* de
  consultar la fuente, para enriquecer la respuesta.

`entidad_ecosystem` es del segundo tipo. `normal_flow` lo inyecta tras ejecutar,
cuando la consulta tiene territorio pero no comprador: reconstruye la SoQL para
la respuesta asesora pero **no vuelve a consultar**, así que las filas que el
usuario vio nunca lo aplicaron.

Como se persistía en el turno, el siguiente follow-up lo heredaba como si fuera
un filtro solicitado. En **HCA-FOLLOWUP-016**, pedir "los más antiguos primero"
tras una búsqueda en Cali añadía al `WHERE` un `OR` con diez compradores del
ecosistema municipal: el usuario pidió reordenar y recibió otro universo.

Tres capas ya se defendían por su cuenta, cada una con su propia lista — el
buffer de paginación, la repesca de página 2 y el guardado de radares. Ninguna
formalizaba la categoría, y faltaba justamente la cuarta: la persistencia
conversacional, que es por donde se propaga.

## Decisión

1. Se declara la categoría **estado derivado de ejecución** en
   `app/core/derived_execution_state.py`, autoridad única de qué claves la
   componen. `entidad_ecosystem` es su primer miembro.

2. **El historial conversacional guarda estado semántico del usuario, nunca
   estado derivado de ejecución.** El filtro se aplica en
   `ConversationStore.append_turn` y no en los cuatro handlers que escriben
   turnos: el invariante es del historial — lo que entra ahí es exactamente lo
   que el próximo follow-up hereda.

3. **INV-PURE-ORDERING-PRESERVES-SOURCE-UNIVERSE.** Para un follow-up que solo
   cambia el orden, `FILTERS(WHERE(T2)) == FILTERS(WHERE(T1))`. Las únicas
   diferencias admisibles son `ORDER BY` y el cursor de paginación. No pueden
   aparecer compradores, ecosistemas, tema, geografía, dinero, estado ni
   modalidad nuevos.

El gate del invariante se pone sobre las **source calls reales**, no sobre el
`QueryFrame`: el defecto vivía justo en el hueco entre ambos, así que comparar
frames no lo habría detectado.

## Consecuencias y límites

Purgar la clave derivada **no** puede llevarse un universo legítimo. Un
ecosistema que el usuario declara no viaja por `entidad_ecosystem`: "contratos
del ICBF en Antioquia" llega como `entidad_resolved` / `entidad_like`, y
`should_expand_ecosystem` ni siquiera se activa cuando hay entidad. La
expansión post-query solo ocurre en ausencia de comprador, que es precisamente
el caso en que nadie la pidió.

La categoría queda abierta: si aparece otro enriquecimiento post-query que hoy
se persiste, su lugar es esta lista, no una cuarta deny-list local.

No se introduce event sourcing ni un modelo de procedencia por campo. La
distinción se resuelve con una lista corta porque la clave que la necesitaba es
una sola.

## Evidencia de aceptación

- `tests/test_ordering_followup_source_universe.py` — 10 casos que comparan las
  source calls de T1 y T2 (territorio municipal y departamental, comprador
  explícito, dinero, tiempo, estado, negación, modalidad). **Verificado por
  mutación**: sin el fix, 4 de ellos fallan;
- HCA-FOLLOWUP-016 re-adjudicado: `WHERE` idéntico entre turnos, orden correcto
  (`fecha_asc`), `obra` preservada como `contract_type_include`. El caso sigue
  rojo en el HCA bruto **solo** por `objeto_has:[obra]`, que SEARCH-SH1
  representa estructuralmente como tipo contractual — deuda de oráculo, no de
  runtime;
- HCA congelado: **0 casos rotos**.

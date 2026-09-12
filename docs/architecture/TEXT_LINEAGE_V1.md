# TEXT_LINEAGE_V1.1

## Problema causal

Dos fragmentos con la misma superficie no comparten identidad. Buscar un texto
del `raw_query` en el residual de `QueryRouter` puede reasignar una ocurrencia
consumida a otra que sobrevivió. Los hashes de ambos extremos autentican los
textos, pero no prueban la relación entre sus intervalos.

## Representación

V1.1 usa `SEGMENT_LEDGER_WITH_CAUSAL_RECEIPTS`, sólo en memoria y por request. El
ledger conserva el texto y SHA-256 raw, el texto y SHA-256 actual, segmentos
`ORIGINAL`, `DERIVED_FROM_ORIGINAL` e `INSERTED`, y tombstones `CONSUMED`.
Cada mutación real añade un evento inmutable enlazado al hash del evento previo.
Los proofs son `exact`, `whole-segment-only` o `unproven`.

Las mutaciones de texto que alcanzan el seam pasan por primitivas que producen
el string y actualizan el ledger con los mismos offsets. El corrector emite
receipts por token desde su propio loop. El resolver de tipo contractual emite
receipts desde el match que realiza cada consumo. No existe replay de parser ni
matching posterior para reconstruir provenance.

## Inventario hasta CONTROL

| Etapa | Operación | Estrategia de lineage |
|---|---|---|
| `normalize_text` | NORMALIZE/DELETE | NFKD/lower por unidad cuando acredita el output; whole-unit `unproven` si el lower es contextual |
| `correct_query` | CORRECT/COMPACT | receipts de tokens y whitespace del mismo loop |
| modalidad | MASK | offsets del resolver, placeholder no semántico |
| negación | MASK | `masked_spans`, tombstone exacto |
| universo contractual | MASK | la ocurrencia elegida por la mutación legacy se consume en ese instante |
| temporal | MASK | span de la autoridad temporal aplicado sin corregir el comportamiento legacy |
| relación de actor | MASK/COMPACT | match de la mutación y compactación causal |
| tipo contractual | MASK/COMPACT | receipts de matches reales del resolver |
| documento/NIT | MASK | matches reales de identificador y marcador |
| año residual | MASK | matches reales del loop legacy |
| whitespace | MERGE/DELETE | espacio nuevo derivado del run completo; vecinos conservan identidad |

El ledger se sella inmediatamente antes de `interpret_control_language`. Las
mutaciones posteriores no forman parte de este seam.

## Invariantes y API

- `descendants_of_raw_span()` devuelve sólo `EXACT_DESCENDANT`,
  `TRANSFORMED_DESCENDANT`, `MULTIPLE_DESCENDANTS`,
  `NO_SURVIVING_DESCENDANT` o `UNPROVEN`.
- `origin_of_seam_span()` ofrece la consulta inversa seam → raw.
- Un consumo crea tombstone; sus placeholders nunca cuentan como descendiente.
- Gaps, splits parciales de unidades indivisibles y many-to-many no acreditado
  producen `UNPROVEN`.
- `request_id`, hash raw y hash del seam deben coincidir o la consulta falla
  cerrada.
- El mismo surface en otra ocurrencia nunca acredita identidad.

Una partition se valida completa antes de mutar. Si todos sus receipts son
`KEEP`, conserva los segmentos y no emite evento. Cualquier receipt causal
produce exactamente un evento global, incluso cuando los hashes de entrada y
salida coinciden. `MASK` y `DELETE` retiran el origen; `INSERT` nunca lo hereda;
`REPLACE` retira el origen y crea texto insertado; `CORRECT`, `MERGE` y
`NORMALIZE` conservan origen derivado. Un `REPLACE` con la misma superficie se
rechaza atómicamente: debe expresarse con la causa real.

`QueryRouter.parse(..., text_lineage_request_id=...)` activa observación de
forma keyword-only. Sin ese argumento no crea trace. El trace vuelve en
`ParsedQuery.text_lineage`; nunca entra en `params`, `QueryPlan`, SoQL,
persistencia ni estado global.

## Ejemplo

En `con vial y sin con vial`, el binding de negación enmascara sólo la segunda
ocurrencia y crea su tombstone. Consultar su intervalo raw devuelve
`NO_SURVIVING_DESCENDANT`; consultar la primera devuelve su propio intervalo del
seam. La igualdad literal entre ambas no participa en la respuesta.

## Autoridad y límites

Lineage acredita procedencia textual, no significado. El parser, sus resolvers,
`QueryPlan` y SoQL conservan toda autoridad ejecutable. V1 no repara el bug
legacy de offsets universo → temporal: OFF y ON mantienen el mismo output o la
misma excepción. Una normalización contextual que no permita acreditar
fronteras internas queda `unproven`; no se inventa alineamiento por caracteres.

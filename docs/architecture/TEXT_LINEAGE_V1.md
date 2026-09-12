# TEXT_LINEAGE_V1.2

## Problema causal

Dos fragmentos con la misma superficie no comparten identidad. Buscar un texto
del `raw_query` en el residual de `QueryRouter` puede reasignar una ocurrencia
consumida a otra que sobrevivió. Los hashes de ambos extremos autentican los
textos, pero no prueban la relación entre sus intervalos.

## Representación

V1.2 usa `SEGMENT_LEDGER_WITH_CAUSAL_RECEIPTS`, sólo en memoria y por request. El
ledger conserva el texto y SHA-256 raw, el texto y SHA-256 actual, segmentos
`ORIGINAL`, `DERIVED_FROM_ORIGINAL` e `INSERTED`, y tombstones `CONSUMED`.
Cada mutación real añade un evento inmutable enlazado al hash del evento previo.
El evento compromete su schema, el manifiesto canónico de receipts y su digest,
el digest del estado posterior y el evento previo. Los proofs son `exact`,
`whole-segment-only` o `unproven`.

Las mutaciones de texto que alcanzan el seam pasan por un único reducer puro en
`text_lineage_reducer.py`. `replace_span`, `replace_text_span` y
`apply_partition` son adapters hacia ese reducer; no contienen otra matriz ni
otra implementación de estado. El corrector emite
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

Una partition se valida y canonicaliza completa antes de reducir sobre estado
temporal; sólo después se asigna el nuevo ledger. Un error deja piezas,
tombstones e historia byte-idénticos. Receipts `KEEP` positivos preservan texto;
padding `KEEP` vacío es neutral y no entra al manifiesto. Cualquier receipt
causal produce exactamente un evento global, incluso cuando los hashes de
entrada y salida coinciden.

| Operación | Forma y efecto |
|---|---|
| `KEEP` | input/output positivos e iguales; conserva identidad |
| `MASK` | input/output positivos; consume origen y emite placeholder `INSERTED` |
| `DELETE` | input positivo, output vacío; consume origen |
| `INSERT` | input vacío, output positivo; emite `INSERTED` sin origen |
| `REPLACE` | input/output positivos y distintos; consume y emite `INSERTED` |
| `CORRECT`, `MERGE`, `NORMALIZE` | input/output positivos; conserva origen derivado |

El envelope acepta sólo su operación homónima, salvo `REPLACE` como receipts
`REPLACE` o como conjunto `DELETE` + `INSERT`, `CORRECT` como subconjunto de
`CORRECT`/`MERGE`/`DELETE`, `MERGE` como `MERGE`/`DELETE`, y `NORMALIZE` como
`NORMALIZE`/`DELETE`. `KEEP` puede acompañar zonas intactas. Nombres desconocidos
o con casing distinto fallan antes del fast-path all-KEEP. `MASK` canonicaliza
`placeholder=True`; todas las demás operaciones usan `False`, y un valor
explícito contradictorio o no booleano se rechaza.

`TextLineageView.verify_integrity()` reproduce cada evento desde el raw mediante
el mismo reducer puro y compara manifiesto, hashes, cadena, digest de estado,
segmentos y tombstones. `seal()` ejecuta esa verificación y se niega a devolver
una vista inconsistente. El replay usa receipts ya acreditados: no vuelve a
ejecutar el parser ni busca superficies.

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

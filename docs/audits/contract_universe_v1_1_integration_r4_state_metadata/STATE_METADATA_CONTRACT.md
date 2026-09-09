# Contrato R4 de metadata de estado

`PRESERVE SEMANTIC PROVENANCE WITHOUT WIDENING EXECUTION`

El universo (`celebrated` / `source_records`) delimita elegibilidad. No expresa
un estado solicitado. `contract_universe_version` es metadata del sistema,
siempre. El predicado de elegibilidad puede mencionar `estado_contrato` sin
convertirse en «Estado actual» en el perfil.

Un estado contractual explícito tiene su restricción física en
`estado_contrato`. Cuando el adaptador de composición transporta un estado
escalar reconocido, conserva `estado` y `estado_field=estado_contrato`.
Solo conserva o recupera `estado_family` si la proyección completa del catálogo
es exactamente el filtro elegido. No busca familias por nombres parecidos,
no añade estados, no modifica el predicado ni la versión de universo.

`apply_request` primero retira la metadata del estado anterior y los campos
pertenecientes a procesos; fija el filtro explícito; recupera provenance a
través del resolver existente; comprueba compatibilidad antes de devolver los
params. Una familia presente pero más amplia/incompatible se elimina. El
helper trabaja sobre la copia que posee `apply_request`, sin mutar al llamador.
La comprobación conservadora exige la lista canónica completa, incluido su
orden; no acredita representaciones alternativas por inferencia.

| Estado reconocido | Filtro físico | Provenance compatible |
|---|---|---|
| Cerrado | `[Cerrado]` | estado Cerrado; campo estado_contrato; contrato_cerrado |
| Suspendido | `[Suspendido]` | estado Suspendido; campo estado_contrato; contrato_suspendido |
| terminado | `[terminado]` | estado terminado; campo estado_contrato; contrato_terminado |
| contrato_activo, ruta ya soportada | `[En ejecución, Modificado, Prorrogado]` | familia completa conservada; representación legacy existente intacta |

La igualdad de SQL al retirar metadata legacy se ejecuta, no se infiere:
`SQL_PARITY.json` compara nueve observaciones base/candidato, y los controles
comprueban también el recorrido por QueryFrame/reload. La recuperación de
refinamientos usa `estado`/`estado_family` para describir «estado»; borrar ambos
produce «ese filtro». Ese consumidor acredita que no son solo un oráculo
histórico. No se afirma que todos los consumidores necesiten las tres claves:
SoQLBuilder, sugerencias y material_filters también entienden el filtro físico.

## Traza de followup

`accepted_delta` conserva los valores admitidos para el merge. Su uso de la
palabra accepted no cambia. Las listas de la traza tienen un contrato distinto:

- `requested_fields`: campos de petición del turno, excluida metadata de universo derivada.
- `accepted_fields`: campos de petición aceptados, excluida metadata derivada.
- `derived_fields`: metadata aceptada para el merge que el sistema calculó.

El clasificador acotado reutiliza `contract_universe_intent.request(text)`:
una decisión explícita cuyo universo coincide con el valor aceptado conserva
`contract_universe` en accepted; la versión siempre queda en derived. Esta
clasificación solo anota la traza: no autoriza ni amplía ejecución. No se crea
un sistema general de provenance ni se cambia el contrato de otras trazas.

«regresa a los contratos»: accepted = dataset, dataset_explicit; derived =
contract_universe, contract_universe_version. El resultado final conserva
celebrated y contract-universe-1.1.

«todos los registros de la fuente» / «registros cancelados»: accepted contiene
contract_universe=source_records; la versión queda en derived; el SQL no lleva
el predicado celebrated.

## Fronteras heredadas, verificadas y sin remediar

La frase contigua exacta «contratos firmados en ejecución de mantenimiento»
**ya requiere aclaración en e23753c**. El adaptador de composición es escalar.
Frame OFF/ON emiten cero SQL en base y candidato. La variante existente
«contratos firmados que estén en ejecución de mantenimiento» conserva los tres
estados. R4 no convierte la familia en un escalar para cerrar Cerrado ni añade
soporte nuevo de composiciones multivalor. Se preserva el límite acreditado por
F2.2; no se declara que el ejemplo contiguo ejecute la familia.

El helper directo de followup para «y los suspendidos» ya proyecta
`[Cancelado, Suspendido]` en base y candidato al heredar contratos desde una
familia parseada sin contexto. La consulta explícita «y los contratos
suspendidos» reemplaza exactamente Cerrado por Suspendido. No se modifica ese
normalizador heredado para ampliar estos diez nodeids; la observación se
conserva en `base_additional_boundaries.jsonl`. No se acredita ejecución
productiva de ese filtro amplio a partir de un helper de merge.

Un `compute_query_plan` o `SoQLBuilder.build` aislado no ejecuta todas las
puertas de workflow: los SQL diagnósticos de `base_controls.jsonl` no son SQL
ejecutado. Las afirmaciones de cero ejecución usan el ledger del workflow.

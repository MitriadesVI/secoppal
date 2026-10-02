# Clasificación de los cuatro registros sin código (2026-10-01)

Pedido del usuario: antes del sello, R0075, SF017, SF063 y SF132 se asignan a un `gap_id`, a `M2A_GOLD` o fuera de
alcance; ninguno queda sin clasificar. Adjudicación del orquestador de esta sesión, por regla. No se modifica ninguna
etiqueta: se conservan las dos y se añade este registro de adjudicación.

| Registro | Etiquetas | Desacuerdo | Regla que lo resuelve | Destino |
|---|---|---|---|---|
| R0075 | A y B: DELTA, `SET GEOGRAPHY` sobre ranura ocupada, EXECUTE; misma IR resultante. A marca AMBIGUOUS por una lectura de UNIÓN de lugares | Sólo la marca de ambigüedad | F1: el vocabulario de M2a es cerrado (`SET/CLEAR/KEEP/SET_ORDER/SET_UNIVERSE`) y `SET` sobre ranura ocupada reemplaza. La unión de valores en una ranura no es expresable en M2a: pertenece al álgebra de conjuntos por ranura (G-FU-09, M2b). | **M2A_GOLD** con la lectura de reemplazo. La lectura de unión se anota como caso de ejemplo de G-FU-09. |
| SF017 | A y B: DELTA, `SET_ORDER` + `SET CARDINALITY 1`, EXECUTE; misma IR. A marca AMBIGUOUS (pregunta sobre lo mostrado) | Sólo la marca de ambigüedad | F4 (adjudicada): «los de mayor valor» y similares re-consultan, no filtran lo mostrado. Superlativo singular = ORDER + CARDINALITY 1 (ADR-103 enmienda 1). | **M2A_GOLD**. |
| SF063 | A y B: NEW_SEARCH, misma IR. A marca AMBIGUOUS (lectura de fragmento sobre el marco pendiente). `ops`: A `[]`, B `[CANCEL_PENDING]` | Marca de ambigüedad y convención de `ops` | A7: una consulta autosuficiente cancela la pendiente; A9: el turno forma por sí solo una búsqueda admisible; A9.1 no aplica porque introduce TOPIC y BUYER nuevos. La diferencia de `ops` es la tensión de esquema O12/O13 (con rechazo explícito, ¿`CANCEL_PENDING` o `[]`?), no semántica. | **M2A_GOLD**. La convención O12/O13 se fija en el esquema antes del sello (recomendación: `CANCEL_PENDING` cuando hay pendiente). |
| SF132 | A: `SET_UNIVERSE` al mismo universo, CLEAR. B: `KEEP UNIVERSE`, AMBIGUOUS con alternativa «licitaciones = oportunidades» | Ambigüedad viva sobre el significado del sustantivo coloquial; la alternativa cambia la vista | Ninguna regla adjudicada decide si «licitaciones» coloquial repetido sobre un ancla de procesos pide la vista de oportunidades. Es interacción vista/universo, no conversación. | **FOLLOWUP_FRONTIER**, `G-FU-19` (vista y universo; dueño: ola F, Product View). Provisional: conservar el universo vigente y anunciarlo. |

## Corrección posterior: R0205 pasa a M2A_GOLD (2026-10-01)

R0205 había quedado en la frontera (G-FU-18) sólo por la duda de si «en adelante» es `>=` o `>`. Esa duda ya la decide el
contrato vigente de D-MONEY: `app/core/money_comparators.py:99` mapea «en adelante» a `GTE` (verificado en `main` @
`45457425` y en `secoppal-m1-int-par` @ `2dde6489`). Con A8.1 la restricción se sustituye completa, y la lectura
principal de los dos etiquetadores ya es `VALUE >= 500 millones`, EXECUTE, con anuncio. Aplicación mecánica de un
contrato existente: no hubo re-etiquetado. El código `ENADELANTE` se da por cerrado y sale del hueco G-FU-18.

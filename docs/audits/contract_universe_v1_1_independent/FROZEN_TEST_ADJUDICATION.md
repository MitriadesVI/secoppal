# Los diez rojos congelados: adjudicación

Los 43 controles independientes originales se ejecutaron intactos, con sus seis
fuentes Python preservadas byte por byte ([PRESERVATION.json](PRESERVATION.json)).

```text
10 failed, 33 passed in 5.25s
```

La lista coincide exactamente con la declarada en REMEDIATION.md.

## Prueba de causalidad, antes que la tabla

La tabla por test sólo describe. La pregunta —¿los diez rojos son *únicamente*
la adjudicación de Aprobado?— se responde por experimento: sobre el runtime
V1.1 completo se restauró **sólo** la regla V1 de Aprobado (`eligible` y
`predicate` sin la exigencia de fecha), mediante
[`tests/cu_v1_policy_shim.py`](../../../tests/cu_v1_policy_shim.py), un plugin
que no toca ninguna otra superficie.

```text
43 passed in 5.05s
```

**Los 43 quedan verdes.** Ninguna otra parte de la remediación —el cambio
léxico de F1, la aclaración de F2, la eliminación de `record_terms`, la
admisión de F4, la composición dual de F5, las versiones de política y
catálogo— contribuye a ninguno de los diez fallos. La causa es única.

## Tabla por test

| Test original | Valor V1 | Valor V1.1 | Primera aserción que falla | Aserciones anteriores | Resto del contrato en el port | Adjudicación |
|---|---:|---:|---|---|---|---|
| `test_independent_sql_matrix[False-seed0]` | 24 | 22 | `independent.py:34` `total_count == 24` | `:33` (conjunto de IDs, insensible a Aprobado) **pasa** | suma, comprador, monto, agregado SUM/COUNT y `errors` vacío: **pasan con 22** | expectativa obsoleta |
| `test_independent_sql_matrix[False-seed1]` | 24 | 22 | `:34` | idem | idem | expectativa obsoleta |
| `test_independent_sql_matrix[True-seed0]` | 24 | 22 | `:34` | idem | idem | expectativa obsoleta |
| `test_independent_sql_matrix[True-seed1]` | 24 | 22 | `:34` | idem | idem | expectativa obsoleta |
| `test_direct_record_does_not_leak_to_search` | 24 | 22 | `:168` `total_count == 24` | `:165` el lookup directo devuelve la fila `Cancelado` **pasa** | `:169` universo `celebrated` **pasa** | expectativa obsoleta |
| `test_radar_cancellation_event_and_digest` | 24 → 23 | 22 → 21 | `:184` `first.total == 24 and second.total == 23` | construcción del radar y ambas corridas **pasan** | digest con `Cancelado`, «salieron del universo», un único `changed_state` con clave `A-0-dated`, `prev_status='En ejecución'`, `status='Cancelado'`: **todas pasan** | expectativa obsoleta |
| `test_actor_name_state_word_has_no_universe_authority[False]` | 26 | 24 | `consumers.py:43` `total_count == 26` | `:41` identidad `71000001` **pasa**; `:42` universo `celebrated` **pasa** | composición de filas del SELECT emitido = 22 admitidos + `OTHER-BUYER` + `LOW-VALUE` | expectativa obsoleta (**el contrato F1 del propio test ya pasa**) |
| `test_actor_name_state_word_has_no_universe_authority[True]` | 26 | 24 | `:43` | idem | idem | expectativa obsoleta |
| `test_materialized_dual_children_and_source_names[celebrated-24]` | 24 | 22 | `:58` `counts['total_count'] == 24` | construcción de ambos hijos **pasa** | procesos `ok`, sin `estado_contrato` en su SoQL, respuesta con ambos títulos, ningún SQL de `p6dx-8zbt` contaminado: **pasan**. El caso hermano `[source_records-94]` **pasa intacto** | expectativa obsoleta |
| `test_real_base_conversation_requires_review` | 24 | 22 | `legacy.py:30` `fresh['total_count'] == 24` | `:26` aclaración, `:27` cero SQL, `:28` bytes de la conversación preservados **pasan** | `:31` los bytes siguen preservados tras la búsqueda nueva **pasa** | expectativa obsoleta |

## Ports independientes

[`tests/test_cu_v11_audit_ports.py`](../../../tests/test_cu_v11_audit_ports.py)
reescribe los diez con oráculos propios: conserva **todas** las aserciones
originales y añade la composición que el conteo no podía expresar —
`A-7-dated` dentro, `A-7-absent` y `A-7-invalid` fuera— derivada de
POLICY_DELTA.md, no leída del runtime. Para el Actor, que devuelve agregados y
no filas, la composición se comprueba re-evaluando el SELECT que el runtime
emitió realmente.

```text
11 passed
```

## Adjudicación

Los diez rojos son **exclusivamente expectativas contractuales obsoletas** por
el cambio 24→22 / 26→24 / Radar 24→23 → 22→21. Se autoriza actualizar esos
oráculos en el gate de coordinación, con esta tabla y la prueba del shim como
justificación. No se editó ningún test congelado durante esta revisión, y no se
usó xfail, skip ni deselect en ningún momento.

# Manifiesto para el coordinador — propuesta, sin efectuar reconciliación

La propuesta B.2 se conserva en lo relativo a los tres ports y las dos expectativas reemplazadas. **Se ajusta:** los once residuales originales siguen verdes, pero hay cuatro node IDs nuevos rojos que no pueden archivarse como montajes históricos; además reapareció un test histórico dependiente del día, que falla igual en la base B.1.

El mapa exhaustivo está en `reconciliation_manifest.json`: original, reemplazo acreditado, controles que deben conservarse, hash de cada archivo histórico, resultados de cada node ID, 11 residuales, 45 controles anteriores, 53 node IDs B.2 y los 51 propios. Ningún archivo de colección activo se modificó; no hay deselect ni xfail nuevos.

## Sustituciones ya adjudicadas

| Original histórico | Reemplazo acreditado | Preservación requerida |
|---|---|---|
| `test_v12b_independent_publication.py::test_reservation_serializes_execution_not_only_append` (4 parámetros) | `test_v12b1_remediation_acceptance.py::test_serial_execution_reserves_the_next_revision`, `test_other_chat_executes_while_first_chat_is_paused`; `test_v12b1_process_ports.py::test_real_workflows_serialize_across_processes_and_reload` | Los dos `[0-False]`/`[1-False]` originales son controles verdes de chats distintos y deben seguir activos. Sólo los `[0-True]`/`[1-True]` tienen el montaje obsoleto. |
| `test_v12b_independent_publication.py::test_two_processes_both_enter_when_fcntl_is_missing` | `test_v12b1_process_ports.py::test_without_fcntl_every_process_refuses_before_entry` | Rechazo desde la entrada en ambos procesos, con control positivo de flock. No exigir HELD para el primero. |
| `test_v12b_independent_boundaries.py::test_current_actor_api_token_reaches_actor_handler` (4 parámetros) | `test_v12b1_actor_port.py::test_actor_direct_control_with_own_unconsumed_token` y `test_actor_api_publishes_and_rejects_consumed_token` | Conservar los `[False-false]`/`[True-false]` originales verdes OFF. API/directo necesitan tokens propios; ON consumido no es un control válido reutilizable. |
| `test_entity_profile_frame_b_publication.py::test_a_record_from_another_catalog_does_not_govern_the_next_turn` | `test_v12b_independent_continuity.py::test_cross_references_are_validated_on_real_reload[catalog_version]` y `test_v12b1_independent_records.py::test_individual_record_damage_rejects_before_io[catalog]` | El original exige ejecutar tras corrupción. F5 exige rechazo previo; no se debilita F5. |
| `test_entity_profile_frame_b_semantics.py::test_a_forged_execution_record_does_not_change_the_next_turn` | `test_v12b1_remediation_acceptance.py::test_current_record_damage_blocks_before_calls[focus]` y `[expected_sections]`, con `test_v12b1_independent_records.py::test_valid_publications_remain_reloadable` | Conservar control positivo de registro válido y daños individuales. |

La colección histórica intacta da **exactamente cinco rojos** correspondientes a esos montajes. Los cuatro controles verdes también se ejecutan, no se infieren. Los dos rojos del autor vuelven a fallar por exigir ejecución, con `admission_invalid_current_publication` y cero consultas. La revisión no los vuelve a adjudicar como bloqueos.

## Qué debe permanecer activo

- Los 70 originales directamente vigentes y los cuatro controles históricos verdes, sin pérdida de parametrizaciones.
- Los tres ports con sus aserciones de ejecución/publicación, exclusión o Actor y sus controles.
- Los once residuales originales de B.1, todos comprobados PASSED en `acceptance.xml.gz`; los 45 controles B.1 también PASSED. No son montajes sustituidos.
- Los tres archivos B.2 del autor, con 53 node IDs reales.
- Las 49 pruebas propias que afirman un contrato de producto (45 verdes y cuatro rojas), incluidas las dos secuencias de publicación y dos topologías de ranking. Su ubicación actual es el directorio autorizado de auditoría; el coordinador debe incluirlas explícitamente en la colección/gate o trasladar una copia acreditada preservando esta fuente. No puede perderlas por asumir que el glob `tests/` ya las incluye.
- El nodo temporal histórico dependiente del día, pendiente de corrección del montaje por el coordinador. No se usa como deselect adicional ni se mezcla con los tres ports adjudicados.

Las cinco observaciones históricas `test_v12b_independent_base_comparison` y las dos observaciones propias de `PublicationRecoveryError` conservan valor diagnóstico, pero no se cuentan como garantías verdes. Sus node IDs exactos están separados en el JSON. La adjudicación histórica de mutaciones permanece 25 conductuales + 2 de etiqueta; las nueve nuevas están en su propio manifiesto.

## Secuencia de reconciliación propuesta

1. Resolver los dos hallazgos nuevos y acreditar sus cuatro casos rojos, manteniendo los controles positivos. Esta auditoría no hace esa remediación.
2. Congelar copias byte a byte fuera de la colección pytest de los cuatro archivos que contienen los montajes y expectativas sustituidos. Los hashes se entregan en `preserve_historical_files`; apuntan a los blobs intactos del candidato.
3. Extraer el resto de casos vigentes y los cuatro controles verdes a colección activa conservando aserciones y parámetros. Incorporar reemplazos ya acreditados y todos los residuales/controles nuevos descritos. No mantener un deselect por nombre completo que excluya los cuatro controles verdes.
4. Corregir por separado el montaje del reloj: `tests/entity_profile_frame_harness.py::observe` omite la fecha al adaptar claims aunque QueryRouter sí recibió TODAY. No interpretar el rojo del 8 de septiembre como una regresión B.2; el original falla igual en la base. El coordinador debe acreditar el reemplazo antes de cambiar colección; aquí no se propone una retirada sin cobertura.
5. Comparar colección activa por node ID y aserciones contra este manifiesto, preservando originales, logs y hashes. Después ejecutar el gate global en serie sobre el nuevo SHA que corresponda. Esta auditoría no acredita ese gate ni autoriza merge/push/activación.

La unión dirigida de 2.654 node IDs describe inventario, no un total de protecciones independientes. Los targets se superponen; el JSON conserva cada resultado sin sumar éxitos repetidos.

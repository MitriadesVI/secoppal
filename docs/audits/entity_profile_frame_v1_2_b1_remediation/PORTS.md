# Originales, ports autorizados y expectativas incompatibles

Todos los originales se preservan; `original_integrity.json` contiene hashes.
La reproducción previa fue **53 failed, 31 passed** sobre runtime intacto.
La ejecución íntegra posterior fue **5 failed, 79 passed**. Sus cinco rojos son
las instancias de los tres montajes siguientes, sin xfail ni skip añadidos.

| Original | Precondición incompatible tras reparación | Sustituto de aceptación |
|---|---|---|
| `test_reservation_serializes_execution_not_only_append` | Espera que ambas reservas del mismo chat regresen antes de ejecutar. Eso contradice exclusión anterior a reserva. Los dos casos de chats distintos siguen pasando | `test_serial_execution_reserves_the_next_revision`, `test_other_chat_executes_while_first_chat_is_paused`, `test_real_workflows_serialize_across_processes_and_reload` |
| `test_two_processes_both_enter_when_fcntl_is_missing` | Exige HELD del primer proceso sin garantía; ahora se rechaza desde el primero | `test_without_fcntl_every_process_refuses_before_entry`; flock positivo histórico y prueba real de workflows/procesos |
| `test_current_actor_api_token_reaches_actor_handler` | Después de API exitosa, consume otra vez el mismo token en su control directo; la selección publicada ya eliminó el pending vigente. Fallan sólo los dos parámetros ON | `test_actor_direct_control_with_own_unconsumed_token` y `test_actor_api_publishes_and_rejects_consumed_token`, cada una con confirmed/pending, persistencia temporal y token propios |

El tercer port fue autorizado expresamente por el usuario durante la sesión.
No intercambia llamadas sobre el mismo estado: control directo y API usan casos
pytest independientes y archivos temporales distintos. La API se comprueba
antes de consumir su token, publica owner ACTOR, conserva CC 70000012, se recarga
con otra instancia y rechaza reutilización sin consultas nuevas. El handler real
no se modificó ni se reactivó ningún token. La mutación
`B1-F3-actor-operation` vuelve a omitir la operación en admisión y mata las dos
variantes positivas de API mediante aserción de estado RESOLVED, acreditando F3.

El runner `acceptance.py` enumera exactamente estos tres nombres y separa sus
nueve parametrizaciones; ejecuta los restantes originales más los nuevos ports.
Las nueve incluyen cuatro controles originales verdes, que sí se ejecutaron en
la corrida original íntegra; no se ocultan ni se afirman nueve defectos.
Los cinco probes `base_comparison` son observacionales, no pruebas de corrección
por el simple hecho de pasar. No se suman targets superpuestos.

## Dos expectativas del autor reemplazadas por la orden F5

- `test_entity_profile_frame_b_publication.py::test_a_record_from_another_catalog_does_not_govern_the_next_turn`
  exige ejecutar con catálogo archivado 999. F5 exige rechazar catálogo incompatible.
- `test_entity_profile_frame_b_semantics.py::test_a_forged_execution_record_does_not_change_the_next_turn`
  exige continuar después de alterar foco/secciones. F5 exige validar coherencia.

Se ejecutaron completas y quedaron rojas, sin editarlas ni omitirlas del target
amplio del autor: **2 failed, 1903 passed**. No son ports adicionales de la
auditoría ni refutaciones de F5. Su expectativa anterior queda reemplazada por
la instrucción explícita B.1; las pruebas independientes de corrupción y
`test_current_record_damage_blocks_before_calls` acreditan el rechazo correcto.
Los controles intactos, ejecución parcial/total, seis focos, CLEAR, reloj,
A2/R3d y R1/R2/R4 permanecen en ese target y los conjuntos complementarios.

## Mutaciones y disciplina del oráculo

Las doce mutaciones originales se repitieron. M7 sólo cambia de ubicación con
la extracción de coordinación; conserva la rama que devuelve el perfil a legacy.
Se añadieron quince mutaciones causales. El primer clasificador no reconocía el
formato `E assert` de pytest; se conservó ese intento. Las pruebas nuevas Actor
y migración se ajustaron para afirmar ausencia de estado en vez de causar
KeyError/AttributeError. No se contabilizan esos errores como detección.

El primer bypass de staging alteraba también la creación de la tabla de control,
causando IndexError de setup. Se reemplazó por bypass sólo cuando ya existe un
sobre publicado. Así conserva control inicial verde y demuestra que una consulta
cuya publicación falla altera el historial heredable si se retira la protección.

M9 original llega a una excepción de ExecutionRecord al retirar la validación de
finitud. Su prueba de discriminación adicional convierte esa regresión de producto
en aserción explícita: un agregado contractual inválido debe generar respuesta
honesta, no abortar publicación. Se preserva el original y se corren ambos.

`mutation_final_summary.json` enlaza cada target, resultado y restauración.
No hubo mutaciones en otros worktrees ni en la rama entregada: sólo exportaciones
Git temporales. La evidencia histórica y todos los originales siguen intactos.

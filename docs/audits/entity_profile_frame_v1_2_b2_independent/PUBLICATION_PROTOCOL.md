# Adjudicación de publicación, recuperación y límites

**F4 no cierra.** La barrera protege las fronteras originales y el rollback funciona con un prefijo confirmado. No conserva la no publicación del prefijo cuando hay dos fallos consecutivos. El resultado no depende de una caída física.

La primitiva real hace `_arm` (write/flush/fsync de barrera y fsync del directorio), append/flush/fsync del sobre, fsync del directorio y unlink de la barrera. El lock existente excluye al lector/escritor del mismo chat durante ese tramo. La barrera es por archivo y época, sin guardar semántica. La recarga comprueba su existencia antes de admitir el sobre; historial y buffer pasan por `continuity_path`, que deriva a `.unavailable` con publicación corrupta. El export tabular exige contrato y el turno del historial que referencia.

## Fronteras acreditadas

Los nombres abreviados de esta tabla se expanden en los node IDs de `acceptance.xml.gz`, `own_verified.xml.gz` y `reconciliation_manifest.json`.

| Frontera real / escenario | Resultado observado | Evidencia |
|---|---|---|
| Preparación: fsync de historial y buffer privados, tras escritura del handler | False; el contexto A sigue gobernando. La generación incompleta no reemplaza A. | B.1 `failed_io_never_authorizes…[history_fsync|buffer_fsync]` |
| Preparación falla mientras otro chat publica | Huérfano físico inerte; otro chat y A conservados. | B.1 `preparation_failure_keeps_other_chat_and_orphan_inert` |
| Creación de barrera falla antes de visibilidad | False; A sigue legible por otro proceso. | Propio `explicit_valid_retry…[before_barrier_visible-*]` |
| Escritura de barrera completa seguida de error | Barrera visible; sin nuevo append admitido. | B.2 `barrier_failures…[arm_write-*]` |
| fsync de barrera / directorio de barrera | False; continuidad bloqueada, sin promover el nuevo turno. | B.2 `[arm_fsync-*]`, `[arm_directory-*]` |
| Append parcial y completo seguido de error | False; la cola queda cercada. | B.1 `append_boundary_is_fail_closed_after_write[True|False]` |
| fsync de sobre / su directorio después del append | False; JSON completo permanece en disco, pero no tiene autoridad mientras sigue la barrera. | B.1 `failed_io_never_authorizes…[envelope_fsync|directory_fsync]` |
| Unlink falla antes de retirar la barrera | Rearme real; False conserva el bloqueo. | B.2 `[confirm_before-*]` |
| Unlink retira la barrera y después informa error | Rearme real antes de False; otro proceso no admite el fallo. | B.2 `[confirm_after-*]` |
| Unlink visible + falla rearmar; prefijo previo válido | Truncate real al offset previo y fsync; A sigue válido. Bytes del prefijo iguales; otro chat publica y no se revierte. | B.2 `[confirm_rearm_failure-*]`; propios `rollback_of_retry…[False-*]` |
| Mismo mecanismo, pero el prefijo contiene un B ya declarado no publicado | **B vuelve a gobernar después de truncar C**, aunque B y C devolvieron False. | Propios `rollback_of_retry…[True-False]`, `[True-True]` |
| Nueva publicación después de cola parcial / completa fallida | API de ConversationStore admite el nuevo sobre; separador evita mezclarlo con el fragmento. | B.2 `explicit_publication_after_partial_append_is_reloadable`, `success_after_unconfirmed_complete_append_without_reset` |
| Perfil explícito nuevo tras fallo de sobre, sin reset | Publica nuevo perfil y continúa; no incorpora el historial del fallo. | Propio `explicit_valid_retry…[complete_envelope-True]` |
| Tabla explícita tras sobre corrupto | La admisión conservadora aclara sin consultas. Con reset, tabla y paginación válidas. No se exige debilitar la adjudicación F5. | Propio `[complete_envelope-False]` |
| Reset con barrera de época anterior físicamente presente | Barrera antigua inerte; publicación y continuación válidas. | B.2 `barrier_failures…`; propios `explicit_valid_retry…` |
| Lectura de otro proceso | Repite la decisión desde archivos reales y expone historial; no usa blacklist en memoria. | Subprocesses en B.2 y `process_read` propio |

Las inyecciones llaman a las operaciones reales y fallan después o antes del punto precisado. Se comprueban inode, visibilidad, lista de fronteras alcanzadas y bytes. No se reemplazan `publish`, `publish_turn`, `append_confirmed`, validadores ni loaders por respuestas simuladas. La frontera sintética es el cliente SECOP y la narración; toda persistencia es temporal.

## Por qué el rollback propio no es suficiente

Tras A válido y B fallido, el archivo tiene dos líneas completas y la barrera de época 0. C toma como offset el final físico de B. Cuando la confirmación de C deja de ser visible y no puede rearmar, restaurar ese offset deja las dos líneas, sin barrera. B tiene revisión 2 y el lector ve dos líneas: lo admite.

Los controles verifican el rollback byte a byte y la publicación de un chat diferente dentro de la frontera del fallo. No se objeta que la recuperación sea local; el defecto es admitir que todo prefijo físico anterior a C estaba confirmado. Esto viola el False ya comunicado sobre B dentro de la rama de recuperación declarada soportada, con truncate y ambos fsync de recuperación exitosos.

En la variante tabular se acreditan cuatro rutas: `load_envelope`, `get_history`, `export_params` y la paginación integrada. En la variante de perfil se acreditan Frame/plan/evidencia recargados y las dos consultas posteriores de proveedores de Galapa/julio. No se creó un Radar usando el export: la frontera API devuelve sus params y el flujo de Radar tiene regresiones sintéticas separadas.

## PublicationRecoveryError

`PublicationRecoveryError` deriva de RuntimeError; el publicador no lo captura en su grupo de OSError/ValueError/errores de reserva. La llamada integrada queda interrumpida sin devolver un diccionario de resultado, sin éxito y sin un False acreditado.

`test_recovery_error_observes_physical_state_and_next_turn[open_failure]` alcanza: append y fsync del nuevo perfil completos; unlink visible seguido de error; reapertura de barrera fallida antes de crearla; reapertura `r+b` para truncar también fallida. **Físicamente queda el sobre nuevo íntegro, sin barrera. Otro proceso lo acepta y «proveedores» ejecuta Galapa/julio.** El control `[ok]` permite truncar, devuelve False y conserva Baranoa. Ambos casos verdes son observaciones que distinguen estados, no dos garantías de seguridad.

Por tanto, la excepción no prueba bloqueo de lectores ni recuperación segura. En esta rama hay autoridad admitida con un resultado de publicación indeterminado para el llamador. El autor excluye la inmunidad a fallos simultáneos que impidan cercar y truncar; no se convierte ese límite en una promesa nueva ni se exige resistir cualquier fallo físico. Sí se corrige la posible lectura del reporte: “interrumpió” no significa “la recarga rechaza”. La apertura F4 principal usa la rama donde truncar y sincronizar **sí** funcionan y ambas respuestas devuelven explícitamente False; no necesita esa exclusión extrema.

## Reinicio de proceso frente a caída física

Las pruebas con subprocess acreditan visibilidad para un intérprete nuevo que lee el mismo filesystem. No simulan pérdida eléctrica, pérdida de caché del dispositivo ni remonte tras caída del sistema.

La retirada final de barrera no tiene fsync posterior. Lectura estática: después del `unlink` exitoso la primitiva retorna. Dentro del modelo declarado, una caída física puede hacer reaparecer la barrera previamente sincronizada y bloquear conservadoramente un éxito. Ese límite de disponibilidad se acepta como tal: éxito comunicado y continuidad posterior bloqueada pueden obligar a repetir/resetear. No se presenta como promoción de un False ni como durabilidad física probada.

No se exige inmunidad absoluta a fallos físicos arbitrarios. El hallazgo principal es una discrepancia determinista entre False y autoridad admitida sin caída física. Retención de generaciones y barreras antiguas sigue siendo deuda operativa ajena; no es la causa de este veredicto.

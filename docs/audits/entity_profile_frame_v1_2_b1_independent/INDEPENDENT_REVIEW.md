**Veredicto: REMEDIATION_REQUIRED.** B.1 cierra los casos históricos reutilizables de F1–F10 y los tres ports son válidos. No cierra completamente F4, F5 ni F9: esta revisión reproduce tres brechas adicionales dentro de esos requisitos. El OverflowError heredado tiene impacto de producto concreto y tampoco se acepta para activar las capacidades afectadas. No se concede `V1_2_B1_GO_FOR_COORDINATOR_GATE`.

| Referencia | SHA |
|---|---|
| Candidato auditado | `af8f0a3407102dcb807089e87e470d754b835d61` |
| HEAD probado declarado por el autor | `64f9836edf96a89051ffc4eb21b0b3397091228c` |
| Auditoría B / base B.1 | `708ea3b3f609af4d9d3abe471f6963ee23513d7a` |
| Runtime B | `1b2a85f89e5d556f1de392e11b9da437da6540d1` |
| Base anterior a B, control de OverflowError | `4c86d5bafaa3296b73c6523465b33efa4311910a` |

Rama: `audit/entity-profile-frame-v1-2-b1-independent`. Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b1-independent`. HEAD de auditoría: el commit que contiene esta entrega, obtenido con `git rev-parse HEAD` y consignado íntegro en la respuesta final; no se fabrica una autorreferencia dentro del commit.

Se añadieron cinco archivos de pruebas y scripts/evidencia propios. Un único auditor/escritor; sin subagentes ni cambios de runtime, configuración, pruebas originales o sustitutas, documentos compartidos, main o worktrees anteriores. Sin merge, push ni activación.

**Autoridades, integridad y método.** Se leyeron AGENTS, CARRILES, el diseño congelado, la revisión y matriz B y los documentos REMEDIATION, PORTS, REPRODUCE, REPAIR_DESIGN y RESULTS de B.1. `artifact_inventory.json` inventaría todos los archivos del directorio B.1, lee/descomprime sus contenidos y conserva los resultados relevantes de logs y campañas, incluidos intentos anteriores. `integrity.json` acredita ascendencia y preservación. Los 485 originales declarados tienen los mismos bytes que BASE y candidato; los 35 archivos de salida comprimida cumplen el manifiesto de bytes descomprimidos. Los 2.410 archivos del candidato se cotejaron contra sus blobs Git y están en `candidate_sha256.json`.

El delta probado→final no cambia app, Streamlit ni tests. Es evidencia/documentación y `verify_evidence.py`; la eliminación de `.log` se corresponde con archivos gzip cuyos bytes descomprimidos están verificados. BASE B→auditoría B no cambia runtime. Arquitectura, AGENTS, CARRILES, configuración, ADR, bitácora, Makefile, Ruff, Semgrep y data permanecen idénticos. La revisión volvió a ejecutar los targets sobre el candidato final; sus resultados no se atribuyen al autor.

Las conversaciones usan el workflow real, parser, compilador, handlers y ConversationStore, con instancias nuevas al recargar. Settings y persistencia quedan en los temporales de pytest; SECOP y narración se sustituyen en sus fronteras. El plugin `offline_guard` prohíbe sockets IP; los workers de concurrencia usan clientes sintéticos y persistencia explícitamente temporal. Radar usa store SQLite temporal para probar permisos y un runner sintético para efectos; no se crearon radares de usuario ni hubo mensajes. Los ensayos de API se distinguen de rutas integradas. No se ejecutaron app, SECOP/LLM live ni pytest global.

**Primero, adjudicación de la partición.** Se reprodujeron los comandos completos antes de usar su partición. Los rojos históricos son exactamente los documentados y se aceptan las sustituciones; no fundamentan este veredicto. Véanse [reconciliación](RECONCILIATION.md) y `test_reconciliation.json` para cada node ID y los cuatro controles verdes apartados.

| Conjunto ejecutado | Salida literal |
|---|---|
| Principal del autor, sin exclusiones | `2 failed, 1903 passed in 141.88s (0:02:21)` |
| Adyacentes | `472 passed, 2 xfailed in 14.54s` |
| Corpus | `276 passed in 1.09s` |
| Utilidad | `15 passed in 3.66s` |
| Núcleo, A2, ports, propiedades y corpus | `408 passed in 1.98s` |
| Auditoría B completa intacta | `5 failed, 79 passed in 34.87s` |
| Partición congelada B.1, sin incorporar nuestras pruebas al glob | `106 passed, 9 deselected in 32.91s` |
| Nuevas pruebas independientes, juntas | `11 failed, 45 passed in 16.80s` |

Los conjuntos se superponen. La colección dirigida tiene 2.550 node IDs únicos; es inventario, no 2.550 garantías verdes. Cinco casos `base_comparison` son probes observacionales y se separan de las garantías de producto. Los dos xfails Actor son preexistentes y sus archivos siguen idénticos; no se añadieron marcas. No se suma ninguna cifra de esta tabla.

**F4 residual, P1: el turno declarado no publicado sí adquiere autoridad.**

Secuencia integrada: tabla contractual A → nueva tabla de procesos B → error inyectado en `os.fsync` del archivo del sobre o de su directorio, cuando el handler ya escribió historial y buffer privados → recarga → «muéstrame más». La respuesta B devuelve `continuation_published=False` y «No pude guardar el contexto… la próxima pregunta no podrá continuarlo». Sin embargo, el lector carga B como revisión vigente y la continuación usa procesos de B. Otro caso equivalente escribe una línea completa de perfil y lanza OSError desde la frontera de escritura: el Frame suppliers declarado no publicado gobierna el siguiente turno.

Evidencia: `test_v12b1_independent_storage.py::test_failed_io_never_authorizes_the_unpublished_generation[envelope_fsync|directory_fsync]` y `test_append_boundary_is_fail_closed_after_write[False]`; salida completa en `storage.log.gz` y `own_final.log.gz`.

La inyección discrimina el descriptor real por inode, comprueba que ya existe historial privado con la consulta B y ocurre después de la preparación, no al entrar en `publish_turn`. `conversation_store.py:417–425` hace append/flush antes de completar ambos fsync. `turn_publication.py:138–145` convierte el error en respuesta no publicada, pero `_read_envelope` acepta la línea completa que quedó. La discrepancia es entre resultado devuelto y autoridad recargada, sin necesidad de suponer pérdida física de datos.

Controles favorables: fallos durante fsync de historial y buffer privados conservan A; una escritura truncada invalida continuidad sin llamadas; el huérfano permanece físicamente y es inerte; el fallo de preparación no destruye un commit de otro chat. Una publicación válida referencia exactamente la generación preparada, por ID y hashes. Pasan los ports de serialización entre procesos, chats distintos y reset con reserva previa. Por tanto, no se niega el cierre de serialización/época ni se afirma corrupción en toda publicación: queda abierta la frontera de error después de append completo. No se propone restauración global ni rediseño en esta auditoría.

**F5 residual, P2: duplicar una llamada conservando su job ID evita la validación de membresía.**

Se publica un overview contractual válido; se añade una copia exacta de `execution_record.calls[0]` al mismo arreglo, sin cambiar plan, digest, Frame, SQL, IDs ni contrato; otra instancia pide proveedores. Ejecuta dos consultas en lugar de rechazar el registro incompatible. La lista pasa de cuatro observaciones a cinco para cuatro jobs, así que sus contadores de intentos/métricas ya no acreditan la ejecución real.

Evidencia integrada: `test_v12b1_independent_records.py::test_individual_record_damage_rejects_before_io[call_duplicate]`. Propiedad determinista: `test_record_call_membership_preserves_multiplicity`, contraejemplo mínimo `job_ids=['a']`; primero valida el registro sin duplicación. `record_validation.py:66–68` exige unicidad de jobs del plan, pero transforma las llamadas a un set y pierde multiplicidad.

Los otros 19 daños individuales nuevos sí bloquean antes de consultar: chat, revisión, turn/result IDs, referencias Frame/Plan/Evidence, esquema, catálogo, snapshot, quitar/añadir jobs, SQL de job, quitar/añadir llamada con ID nuevo, SQL/dataset de llamada, comprador del snapshot y base del pending. También pasan los nueve daños históricos y cinco B.1. Las publicaciones ejecutadas, parcial/totalmente fallidas, pending inicial y pending que conserva ejecución se recargan correctamente. No se trata un fallo de fuente como corrupción. No se demostró que la duplicación cambie el SQL de la nueva consulta ni que ocurra en una publicación normal; la brecha es la admisión de evidencia incompatible, exigida por F5.

**F9 residual, P2: reutilizar ID/revisión permite mezclar comprador y plan.**

Se obtienen Frames reales Baranoa y Galapa con el mismo periodo. Se compila/ejecuta Baranoa y se reemplaza únicamente `buyer_scope` del Frame suministrado al assembler/renderer por el scope acreditado de Galapa, manteniendo ID, revisión y predicados de Baranoa. Ambos consumidores aceptan el objeto. El renderer encabeza Galapa y conserva hechos/SQL de Baranoa.

Evidencia: `test_v12b1_independent_records.py::test_foreign_buyer_with_reused_ids_is_rejected[assembler|renderer]`; el control válido devuelve Baranoa y cuatro contratos con cuatro llamadas. `validate_frame_plan` en `record_validation.py:30–35` comprueba referencia y predicados; buyer_scope no forma parte de esos predicados y no se compara. Los tests históricos cambian también ID/revisión, por lo que pasan sin cubrir esta sustitución.

Es una frontera de API exigida expresamente por F9. No se demostró una petición ordinaria que produzca espontáneamente ese Frame. No se atribuye una mezcla cotidiana ni una falla del resolver de compradores.

**Matriz F1–F10, con límites.** `I`: workflow/recarga reales; `A`: API; `P`: procesos; `R`: reproducción del candidato. Los nombres abreviados se expanden en `test_reconciliation.json` y los logs.

| F | Evidencia que sí pasa | Adjudicación |
|---|---|---|
| F1 | I: ocho combinaciones perfil/pending→más, ordinal sin opciones, repetición y chip rechazado; conserva confirmed/pending/owner; `clarification_retains_execution_without_new_calls` conserva registro y permite continuar; propiedad de referencias | CERRADO en el alcance probado |
| F2 | I: seis reformulaciones completas sin alternativas, con/sin confirmed, mismo/otro comprador; cancelación positiva; R5d, «sí» y cambios incompletos no ejecutan | CERRADO |
| F3 | I: LIST/RUN, lookup, documento Actor, nueva tabla y reset frente a pending; API/directo Actor independientes, recarga, token consumido rechazado; controles nuevos token inválido, TTL físico, versión, identidad inválida y OFF; Radar owner autorizado/ajeno usa lookup real y efecto sintético | CERRADO; permisos de las fronteras existentes, sin certificación externa live |
| F4 | P/I: ejecución serial por chat; otro chat trabaja; reset invalida reserva; preparación privada/huérfanos, generación exacta y trabajo ajeno preservados; escritura truncada rechazada | PARCIAL: tres fallos posteriores a escritura completa arriba |
| F5 | I: referencias/esquemas/catálogo/snapshot/jobs/SQL dañados rechazados; controles válidos ejecutados y pending; errores de fuente honestos | PARCIAL: llamada duplicada admitida; una prueba integrada y una propiedad roja |
| F6 | I: seis dimensiones del cursor, TTL físico, página siguiente, agotamiento, dual ambiguo→grupo, ambos «más»; referencia UI ausente/incompleta/vieja/otro chat rechazada y válida ejecuta; dueño explícito de lookup/Actor/Radar | CERRADO en estos casos; no se convierte rows en capacidad |
| F7 | I: OFF→ON migra equivalente con provenance y bloquea tres incompatibles; tabla→perfil→OFF no revive cursor; LIST/RUN siguen disponibles con OFF | CERRADO para este binario con guarda. Binario antiguo no certificado ni propuesto como rollback |
| F8 | P/A: cada proceso sin fcntl rechaza antes de entrar; flock disponible tiene control positivo y mutación discriminante | CERRADO para procesos con almacén compartido; sin promesa distribuida |
| F9 | A: rechaza ID/revisión y Frame ajenos históricos; conserva métricas/respuesta válida | PARCIAL: comprador ajeno con IDs reutilizados aceptado |
| F10 | I: SAVE después de perfil tiene cero efectos; tabla exportable vigente guarda filtros exactos/owner; LIST/RUN positivos y owner ajeno negado | CERRADO |

**Utilidad conservada y mutaciones.** Se reprodujeron A2/R3d, los trece contraejemplos, R1/R2/R4, los ocho escenarios B y seis focos. Las corridas conservan presupuestos, exclusiones temáticas/de modalidad, CLEAR durable, reloj inyectado, conteos independientes de montos y respuesta honesta ante fuente parcial/total fallida. `expected_sections` y `not_requested_sections` tienen consumidor en el assembler. Los controles positivos impiden adjudicar seguridad por bloquearlo todo; no se extrapola su cobertura a todo analytics.

La campaña congelada B.1 se ejecutó sobre una exportación de `af8f0a3`: 27 baselines verdes, 27 resultados automáticos DETECTED y restauración de app verificada después de la campaña y de cada mutación. No hubo timeouts, import errors ni errores de montaje contados. `mutations.json` conserva salidas completas. M9 tiene una aserción adicional de respuesta que convierte el aborto en fallo conductual; su excepción por sí sola no se cuenta como garantía.

**Adjudicación independiente de los oráculos: 25 detecciones conductuales y dos diferencias de etiqueta, no 27 protecciones de producto retiradas.** En M2 y M3 siguen pasando aclaración y cero llamadas; sólo falla `route_reason`. Se repitieron ambos en otra exportación con `test_continuation_rejection_prevents_product_effects`: baseline y mutante pasan, sin filas/consultas; M3 conserva Frame. `mutation_product_oracles.json` registra los resultados y hashes restaurados. Las defensas restantes vuelven redundantes esas retiradas particulares. Esto limita la afirmación de la campaña, no demuestra una vulneración productiva de M2/M3. Los tres ports autorizados sí detectan quitar su protección mediante los mutantes B1-F4, B1-F8 y B1-F3-actor.

**OverflowError heredado: bloqueo acotado de producto para activación.**

Entrada sintética mínima: una fila de `GROUP BY documento_proveedor` con documento/tipo válidos, conteo 1 y `total_contract_value='Infinity'`; también falla `'1e999'`, valor textual finito que desborda al convertir a float. Se conserva válido el agregado contractual principal. Ruta B.1: `run_query → coordinate_turn → run_profile_route → run_profile_turn → render_frame_profile → profile_fact_lines → _overview` o `_supplier_lines → _money → analytics.base.format_currency`; `int(val)` en `analytics/base.py:36` lanza `OverflowError: cannot convert float infinity to integer`.

`parse_suppliers` usa `as_float`; el renderer reutiliza ese monto sin la acreditación estricta del agregado contractual. Overview aborta tras cuatro llamadas, suppliers tras dos; `run_query` no devuelve respuesta ni publica el nuevo resultado. Valores/montos responde como control porque no usa ese ranking. No es sólo una cifra omitida: se pierde la respuesta completa, incluidos hechos válidos.

La misma prueba, sin editarla, da cuatro rojos/dos verdes en `af8f0a3`, `1b2a85f` y `4c86d5b`. Son exportaciones temporales offline con hashes app iguales antes/después. `git blame` atribuye la línea del formatter a `3150cbf4b9474c9608d25a06ce220461520ff25f`; no se atribuye el defecto a B.1. Evidencia: `supplier*.log.gz`, `historical_controls.json` y prueba propia `test_v12b1_independent_supplier_overflow.py`.

Decisión: **no aceptable para activar overview/proveedores bajo la garantía de respuesta honesta ante métricas inválidas**. Requiere adjudicación/remediación acotada por el coordinador antes de activar esas capacidades; queda abierto, sin waiver implícito por preexistencia. No se corrige aquí, no se añade trabajo general sobre analytics ni se afirma frecuencia real de esas entradas en SECOP.

**Puertas y alcance pendiente.** Salidas literales:

```text
All checks passed!
Ran 3 rules on 323 files: 10 findings.
11 failed, 45 passed in 16.80s
app/core/ limpio
2 passed in 0.14s
```

Ruff 0.16.4 sobre app/tests/scripts. Semgrep 1.176.0: baseline global rojo diez, candidato diez, delta cero por identidad y fragmento. Nueve fragmentos idénticos; Radar 109→110. Paginación 177→179 cambia el cuerpo del try para el buffer admitido, con AST de except idéntico. La primera exportación sin Git produjo un finding de Settings en config al enraizar las exclusiones de ruta de otra forma: se conserva `semgrep_base_no_git.*`; la repetición con raíz Git aislada obtiene el baseline comparable de diez. No se alteró ni suprimió ninguna regla.

Hypothesis 6.165.10 ejecutó propiedades de núcleo/B/B.1 y la propiedad independiente de multiplicidad; la última falla por la invariante real descrita. Codegraph 0.9.6 con Node 22: `Not initialized`, MCP ausente; fallback explícito a rg/manual, consumidores en `consumers.txt`. No se utilizó un índice ajeno como autoridad. Se ejecutaron lint-core, tamaño y diff-check; comandos y reproducción exacta en [REPRODUCE.md](REPRODUCE.md).

**Corrí esos targets, no la suite.** No se certifica un gate global. Fuera de carril quedan intactos los diez hallazgos Semgrep, los dos xfails Actor, todos los originales y documentos compartidos. No se abrió trabajo de retención/optimización; las generaciones antiguas inertes constituyen una deuda operativa separada, no la razón del rechazo. Quedan por resolver F4/F5/F9 y el bloqueo de proveedores, reconciliar explícitamente la colección activa preservando la historia y después ejecutar el gate global del coordinador. Esta auditoría no efectúa ninguna de esas remediaciones, integración o activación.

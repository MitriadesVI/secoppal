# Revisión independiente V1.2-B

**Veredicto: REMEDIATION_REQUIRED.** Ocho hallazgos P1 y dos P2, agrupados por causa. No se recomienda pasar al gate del coordinador mientras permanezcan estos incumplimientos. No se emite PRODUCTION_GO.

- Candidato: `1b2a85f89e5d556f1de392e11b9da437da6540d1`.
- BASE B: `4c86d5bafaa3296b73c6523465b33efa4311910a`; ascendencia comprobada.
- Rama: `audit/entity-profile-frame-v1-2-b-independent`.
- Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b-independent`.
- HEAD final de auditoría: el commit que contiene este informe, consultable con `git rev-parse HEAD` en esta rama y consignado completo en la entrega. No se inserta una autorreferencia ficticia.

## Autoridades, método y límites

Se leyeron AGENTS, CARRILES, el diseño congelado completo, THESIS, INVENTORY, ACCEPTANCE_MATRIX, ADVERSARIAL_MATRIX, REPRODUCE, CLOSEOUT y mutation_campaign.json de B, y cierres/evidencias A2 y R3d. La orden B original no apareció como documento en la línea Git ni entre los prompts SECOPPAL localizados en Downloads. Las alusiones del autor a sus secciones no se reconstruyen como requisitos escritos. Los requisitos adjudicados aquí proceden del diseño congelado y de la solicitud explícita de esta auditoría.

Auditoría con un único escritor; ningún subagente, modificación de runtime, configuración, test del autor, expectativa congelada, catálogo, ADR, bitácora, main o worktree del implementador. Sólo se añadieron tests/scripts propios y este directorio. Los clientes externos son sintéticos; admisión, parser, composición, compilación, workflow y persistencia son reales. Cada turno del harness conversacional crea otra instancia. Las pruebas Actor con directorio sintético reutilizan su fixture de workflow y se identifican aparte. Las pruebas de almacenamiento alteran exclusivamente archivos temporales de su chat. La campaña de mutaciones usa una exportación Git desechable del candidato, no este runtime.

El socket tripwire de pytest impide conexiones IP. No se ejecutaron app, SECOP/LLM live, radares reales ni mensajes. Los efectos SAVE/RUN observados se interceptaron exclusivamente en las fronteras externas. Las observaciones completas de respuesta, SQL enviado, Frame/pending y contrato por turno están en `independent_final.log`.

## Hallazgos confirmados

### F1 — P1: una aclaración que ofrece continuar el perfil lo borra

**Requisito:** §§7, 10.1 y 11: aclarar no altera confirmed; la continuidad ofrecida sigue siendo del perfil.

**Secuencia mínima:** `Analiza contratos de la Alcaldía de Baranoa en agosto` → `Más` → `Proveedores`. Después de T1 hay Frame overview/contratos/agosto y contrato ENTITY_PROFILE. T2 realiza cero llamadas y dice «Dime en qué quieres profundizar», pero publica `confirmed_frame=null`, `pending=null`, owner NONE. T3 termina en `heuristic_only`, sin los dos jobs suppliers ofrecidos. Se reproduce también con `1`, un chip viejo y «más» repetido, con/sin pendiente anterior: ocho rojos.

**Causa:** `orchestrator._admit_and_dispatch`: una decisión CLARIFY no produce `outcome`; cae a `ProfileConversationState()` y `delegated_contract`, igual que una consulta nueva. La matriz del autor afirma «perfil intacto», pero sus R7a/R7c sólo comprueban la respuesta y las llamadas, no ese estado durable.

**Prueba:** `test_clarification_preserves_the_profile_it_offers_to_continue`.
**Impacto:** rompe la interacción que la propia respuesta propone; también pierde cambios pendientes al rechazar entradas.
**Temporalidad:** el síntoma final de pérdida de continuidad ya aparece en BASE (allí «mas» ejecuta una búsqueda legacy). B introduce el borrado del nuevo Frame y corrige las llamadas de T2, pero deja incumplida la continuidad prometida. No se atribuye todo el síntoma histórico a B.

### F2 — P1: reformulación completa bloqueada por pendientes sin alternativas

**Requisito:** §7 y §10.1 permiten abandonar un pendiente con un pedido nuevo inequívoco; la entrega dice que preguntas sin alternativas se resuelven reformulando.

**Secuencia:** `Analiza contratos publicados en agosto de la Alcaldía de Baranoa` → `Analiza contratos firmados en agosto de la Alcaldía de Baranoa`. La primera pregunta publica pending sin alternativas; la segunda vuelve a `entity_profile_pending_unresolved`, cero llamadas, mismo pendiente. Esperado: ejecutar la petición soportada después de su admisión completa. Sin confirmed inicial falla incluso cambiando a Galapa. Con confirmed anterior, la salida depende de si el comprador difiere de **confirmed**, no del pedido pendiente. Cuatro de seis combinaciones fallan; dos cambios de comprador y la cancelación explícita son controles verdes.

**Causa:** `resolve_pending` sólo permite superseder por `_is_other_buyer`, que devuelve False sin confirmed; `_select` devuelve None incondicionalmente si no hay alternativas.
**Prueba:** `test_full_reformulation_exits_a_pending_without_alternatives` y control `test_explicit_cancel_is_a_control_not_a_required_workaround`.
**Impacto:** la pregunta no tiene salida por la reformulación anunciada. Exigir «cancelar primero» sería cambiar el producto, no resolver el hallazgo.
**Temporalidad:** regresión introducida por B, reproducida con el mismo probe en BASE: la reformulación allí ejecuta cuatro llamadas.

### F3 — P1: el pendiente captura comandos ajenos; la API Actor pierde precedencia

**Requisito:** §§2, 10 y 10.1: Radar LIST/RUN, lookup, documento Actor, nueva búsqueda y selección estructurada conservan sus autoridades y permisos.

**Reproducción:** con un pendiente real, `mis radares`, `ejecutar radar Sintetico`, `CO1.PCCNTR.12345`, `contratos del proveedor CC 70000011` y `procesos de pavimentacion en bogota` vuelven al pendiente del perfil. Con confirmed sin pendiente, estos controles conservan el handler; reset funciona en ambos estados. RUN con store sintético acredita el owner `synthetic-owner` y el efecto simulado en el control confirmado; el pendiente produce **cero invocaciones** al store/runner.

Además, se crea una lista Actor sintética con token vigente, se publica un perfil (que no reemplaza esa lista legacy) y se llama `select_actor_candidate(2, token)`: B devuelve `admission_profile_has_no_options`. El mismo token aún es aceptado por `handle_actor_selection`, que resuelve el documento sintético `70000012`. Es una comprobación de la autoridad real, no sólo de classify.

**Causa:** `is_profile_turn` reclama todo TEXT cuando `state.has_pending`; la admisión no consulta antes las autoridades de comandos. `admit` ni recibe `actor_candidate_selection`, y su forma ordinal decide sobre el dueño del perfil.
**Pruebas:** `test_explicit_external_commands_keep_their_handler`, `test_radar_run_preserves_owner_and_reaches_only_synthetic_effect`, `test_current_actor_api_token_reaches_actor_handler`.
**Impacto:** bloquea operaciones explícitas ajenas y una API vigente. No se simulan efectos para forzar la admisión.
**Temporalidad:** LIST y lookup conservan su ruta en BASE y quedan capturados en B (probes idénticos). El bloqueo del token estructurado está en la nueva admisión B.

### F4 — P1: publicación optimista no serializa el turno ni sella sus efectos de continuidad

**Requisito:** §11, reserva serial anterior a ejecución, invalidación por reset y autoridad exclusiva del registro completo publicado.

**Tres reproducciones complementarias:**

1. Dos instancias reservan revisión 0, ambas antes de publicar. Una barrera en la salida real de `reserve_turn` lo garantiza. Eventos en el cliente ordenan las cuatro llamadas de A antes de las cuatro de B. Se fuerza publicación A→B y B→A: ambos ejecutan; sólo uno publica revisión 1, el otro devuelve respuesta con aviso de contexto no guardado. Chats distintos publican ambos. No hay doble revisión confirmada: **sí funciona el CAS de append**; no hay serialización de ejecución.
2. Se pausa la primera ejecución tras reservar revisión 0, se ejecuta `/reset` en otra instancia y se libera la llamada. El turno anterior al reset publica revisión 1 y vuelve a crear el perfil que se acababa de invalidar. El reset no cambia una generación durable que invalide la reserva.
3. Tabla A publicada → consulta B con fallo OSError inyectado sólo en `publish_turn`. El sobre sigue en A y la respuesta avisa del fallo, pero legacy ya contiene B. El siguiente `muestrame mas` devuelve `pagination_exhausted` sobre **procesos/alimentación/Bogotá de B**, en vez del referente A. El buffer/historial no es inerte hasta publicación.

**Causa:** el lock sólo rodea lectura de reserva y append, en intervalos separados; no vive durante admisión/ejecución. Las rutas delegadas persisten antes del sobre; paginación sigue leyendo `history[-1]`. `clear` elimina el sobre sin tombstone/generación protegida contra reservas previas.
**Pruebas:** `test_reservation_serializes_execution_not_only_append`, `test_reset_invalidates_an_already_reserved_initial_turn`, `test_failed_delegated_publication_cannot_replace_legacy_authority`.
**Impacto:** respuestas no publicables, resurrección después de reset y continuación de un turno que el sistema declaró no guardado.
**Temporalidad:** incumplimientos de la integración nueva de reserva/sobre B. Los escritores legacy son previos; B debía hacerlos compatibles con la nueva autoridad. No se afirma corrupción de revisiones publicada ni exactly-once externo.

### F5 — P1: recarga acepta referencias cruzadas rotas y evidencia ausente

**Requisito:** §§8, 11 y 15.2.5/8: referencias exactas Frame–Plan–Evidence–resultado/chat, versiones y catálogo compatibles.

**Reproducción:** se publica un perfil íntegro, se modifica una sola dimensión de su sobre temporal y otra instancia pide `proveedores`. Se aceptan plan_ref ajeno, evidence_ref ajeno, frame_ref ajeno, result_id discordante, revisión de contrato 999, execution_record ausente, chat_id de otra conversación, versión de registro 99 y catálogo desconocido. Los nueve casos ejecutan en lugar de invalidar/aclarar sin llamadas.

**Causa:** `ConversationEnvelope.from_dict` valida principalmente forma exterior; `_restore` decodifica estado y contrato por separado y no consume ni valida `execution_record` contra ellos. El parser de `ExecutionRecord` aislado no acredita la ruta de recarga.
**Prueba:** `test_cross_references_are_validated_on_real_reload`.
**Impacto:** un registro inconsistente puede seguir siendo autoridad ejecutable; las etiquetas de referencias no prueban integridad.
**Temporalidad:** introducido en las nuevas primitivas y lector B. No existía ese sobre en BASE.

### F6 — P1: capacidades tabulares publicadas sin vínculo con el cursor consumido

**Requisito:** §10/10.1: cursor, generación, fingerprint, orden, grupo, expiración y referente UI del resultado mostrado; Actor/Radar/lookup no reciben capacidades por forma de payload.

**Reproducción:** después de mostrar una tabla real con 60 filas sintéticas, cambiar individualmente cursor_id, generation, fingerprint, order_contract, expiration o group del contrato no evita recibir las filas 26–50. Son seis referencias incompatibles aceptadas. Un seed sin `_continuation_ref` ejecuta refinamiento; referencias incompletas/ajenas se rechazan y una válida ejecuta como controles. Una propiedad determinista confirma que omitir la referencia evita la guarda independientemente del ID de resultado.

Al vencer físicamente el sidecar, se repite la búsqueda con dos llamadas y se entregan 25 filas, en lugar de pedir autorización para repetir. El «más» tabular corto cae a heuristic_only con siete llamadas y 25 filas, en lugar de consumir el cursor: dos pruebas adicionales. Los resultados delegados reales publican owner NONE para Actor y Radar; un lookup de contrato se publica como TABULAR_SEARCH con cursor inventado y fingerprint vacío. Esto no acredita que todos los payloads Actor adquieran cursor: el contraejemplo observado de adquisición es **lookup**.

**Causa:** `_tabular_groups` deriva cursor/generación del ID de reserva y deja expiration=None. No hay consumidor de esos IDs en el handler; éste redescubre params/buffer desde legacy. `_seed` permite la ausencia de referencia. `delegated_contract` decide por rows/dataset_groups, no por autoridad de resultado.
**Pruebas:** `test_pagination_consumes_the_published_capability`, `test_ui_seed_requires_current_result_reference`, `test_reference_free_seed_never_acquires_tabular_authority`, `test_delegated_results_publish_their_actual_owner`, `test_real_sidecar_expiry_does_not_silently_repeat_a_search`, `test_bare_more_uses_the_same_tabular_cursor_as_the_full_phrase`.
**Impacto:** las capacidades publicadas no controlan el siguiente resultado y UI tiene una vía sin acreditación.
**Temporalidad:** incumplimiento nuevo de la frontera B; mecánica legacy y ausencia histórica de «más» corto son preexistentes, reconocidas también por CLOSEOUT. No se atribuye a B la creación de esos handlers antiguos.

### F7 — P1: migración y rollback no están en la ruta real

**Requisito:** §§11 y 15.3: migrar sólo significado equivalente, nunca modo híbrido; rollback debe rechazar contextos no comprendidos.

**Reproducciones:** (a) crear perfil con B OFF, activar B, pedir `proveedores`: ejecuta dos jobs de perfil legacy, pero el sobre B publica confirmed=None/owner NONE. (b) tabla con cursor → perfil B → B OFF → `muestrame mas`: devuelve 25 filas de la tabla anterior al perfil. La prueba del autor llamada rollback sólo simula versión desconocida con el lector B aún ON; no prueba apagar B o volver al lector previo.

**Causa:** no hay consumo de `migrate_legacy_context` en admisión; sin sobre se delega al flujo que hereda params. El flag OFF retorna directamente `_dispatch` sin negar continuidad incompatible.
**Pruebas:** `test_legacy_profile_migrates_or_blocks_in_the_real_route`, `test_rollback_does_not_reactivate_a_preprofile_tabular_cursor`.
**Impacto:** activación híbrida y reactivación de otro universo después del rollback.
**Temporalidad:** fronteras creadas por B que no completan las garantías requeridas. El funcionamiento del binario V1.1 antiguo no se presentó como validado.

### F8 — P2: sin fcntl se admite una garantía inexistente

**Requisito:** §11: exclusión entre procesos del mismo almacén, o rechazo explícito si falta la garantía.

**Reproducción:** `sys.modules['fcntl']=None` sólo en los procesos de prueba. `conversation_lock` permite entrar normalmente; dos procesos sostienen simultáneamente el mismo lock, acreditado por handshake mediante pipes mientras el primero permanece dentro. No depende de sleeps.
**Causa:** `_acquire_file_lock` captura ImportError y retorna éxito; un comentario es la única advertencia.
**Pruebas:** `test_missing_fcntl_does_not_silently_admit_writers`, `test_two_processes_both_enter_when_fcntl_is_missing`.
**Impacto:** garantía ausente sin señal de ejecución. El host auditado sí tiene fcntl; la reproducción corresponde a la plataforma simulada sin él.
**Temporalidad:** nueva primitiva B. No se exige implementar soporte multiplataforma nuevo: la ausencia no puede anunciarse como protección activa.

### F9 — P2: renderer acepta un Frame distinto del autorizado

**Requisito:** §§8, 9 y 15.2.5: revisión exacta del Frame y alcance visible/SQL del mismo universo.

**Reproducción aislada:** compilar y ejecutar un Frame Baranoa/agosto; pasar al renderer ese plan/evidencia con revision+1, o con un Frame Galapa/julio. No se rechaza. En el segundo caso encabeza Galapa sobre el SQL/alcance de Baranoa. Se esperaba ValueError/invalidación antes de presentar la mezcla.
**Causa:** `build_profile_sections` y `render_frame_profile` usan identidad del Frame recibido, sin compararlo con `plan.frame_ref`; sólo verifican evidence.plan_id.
**Prueba:** `test_renderer_rejects_a_frame_other_than_the_authorized_revision`.
**Impacto:** falta una defensa obligatoria en la API de render. **No se demostró que una petición ordinaria de run_query produzca esta mezcla**; severidad P2 y evidencia aislada, no incidente integrado.
**Temporalidad:** API nueva B.

### F10 — P1: SAVE sobre un perfil exporta una búsqueda anterior

**Requisito:** tabla §10.1: perfil no exportable; guardar «esta búsqueda» debe acreditar el referente, sin buscar params hacia atrás.

**Secuencia:** `contratos de obra en baranoa` → `analiza contratos de Alcaldía de Baranoa en agosto` → `guardar esta búsqueda como Radar Sintetico`. Llega a `save_radar_from_params` con la tabla antigua, y responde `radar_saved`. El spy retorna un ID inexistente: no se creó radar ni se ejecutó su revisión.
**Causa:** admisión delega SAVE sin negar el perfil; `_last_search_params` encuentra el turno legacy antiguo porque B no escribió el perfil allí.
**Prueba:** `test_radar_save_cannot_export_an_older_tabular_search_over_profile`.
**Impacto:** guardar/automatizar otro universo que el solicitado.
**Temporalidad:** la familia de SAVE sobre perfil es preexistente: el mismo probe también alcanza el guardado simulado en BASE. B conserva el riesgo y agrava la desvinculación al omitir su turno del historial que SAVE consulta. No es un hallazgo nuevo atribuido exclusivamente a B, pero incumple un gate explícito de esta entrega.

## Confirmaciones y refutaciones favorables al candidato

- Target principal del autor reproducido: **1905 passed**; adyacentes **472 passed, 2 xfailed**. No son una suite global.
- Los 13 contraejemplos A2 y R3d/R1/R2/R4 permanecen en los targets verdes, sin modificar sus archivos. Corpus 276, conjunto core/adversariales 408 y utilidad 15 reproducidos.
- Los ocho escenarios B tienen tests reales con recarga. R5a–R6 y CLEAR ejecutan con sus invariantes. Se refuta la extensión documental «perfil intacto» de R7a/R7c, no el cero llamadas que sí demuestran.
- Instrumentación de las 196 pruebas propias del autor: **60 con B ON y 136 con B OFF** en el momento del test. Los 136 incluyen pruebas puras y controles; no equivale a 136 workflows legacy. Tampoco 196 equivale a 196 integraciones B. Ver inventario por nodeid en `author_b_flags.json`.
- SQL publicado = SQL realmente enviado en los controles independientes de los seis focos, referencias correctas en publicaciones íntegras, presupuestos conservados. CLEAR conserva una exclusión legítima y el reloj de 2024 sigue gobernando agosto al recargar en 2026.
- Fallo parcial/total, montos no finitos y conteos independientes tienen controles reproducidos del autor. No se halló una regresión de esas invariantes en estas corridas.
- Las doce mutaciones del autor se detectaron de nuevo en copia desechable; hashes restaurados. Eso acredita los oráculos concretos, no las fronteras omitidas. La prueba de flock mata eliminación del mecanismo, pero no refuta F4.
- Semgrep final conserva los diez hallazgos del baseline; delta por regla, archivo, línea y fragmento **cero**.

## Desfases documentales y SHA efectivamente probado

CLOSEOUT aún llama probado a `11399da`. Se verificó que **su runtime y streamlit_app.py son byte a byte los del candidato**: los dos commits posteriores agregan utilidad, pruebas de publicación y documentación. El incremento 1887→1905 se reconcilia como 15 casos de utilidad y tres pruebas de publicación. La auditoría volvió a ejecutar el target documentado sobre `1b2a85f`, con resultado 1905. No se interpreta el desfase como defecto funcional.

El inventario final dice «13 módulos nuevos», aunque enumera 17. El diff real contiene **17 app nuevos, 8 app modificados y streamlit_app.py**, siete archivos de tests (incluido el harness), un script y siete artefactos documentales: **41 archivos**. `diff_inventory.txt` y `candidate.diff.gz` conservan el intervalo completo. La cifra 196 de tests propios sí coincide con la colección final.

Los dos xfail son idénticos por hash de archivo en BASE/candidato. Ambas ejecuciones con `--runxfail` producen la misma comparación `CANDIDATES != RESOLVED` en ambos parámetros. No se usó su motivo escrito como prueba de preexistencia.

## Lectura estática, inconclusos y decisiones no autorizadas

- `expected_sections` sí se consume en `build_frame_sections`. `not_requested_sections` se serializa en ExecutionRecord, pero no tiene consumidor explícito en assembler/renderer; estos usan el complemento de expected. Incumplimiento literal pendiente de adjudicar: no se añade otro P1 porque no se reprodujo una respuesta incorrecta con un plan válido exclusivamente por ello.
- El registro contiene llamadas/SQL y referencias, no una serialización completa de todas las estructuras del plan. F5 prueba que la recarga ni siquiera verifica esas referencias. La suficiencia archivística de un snapshot futuro queda para remediación/diseño, sin inventar requisitos adicionales.
- No se acredita exclusión en un almacén distribuido no compartido, ACK físico de canal, exactly-once, ni una carrera aleatoria entre cuatro procesos. Los casos de concurrencia propios usan fronteras controladas y se reportan como tales.
- Se reprodujeron tests existentes sobre los 35 ports; no se volvió a portar cada API histórica V1.1 ni se ejecutó sobre el candidato el binario antiguo completo. La traza original y sus hashes permanecen congelados.
- No se pretende certificar exhaustivamente todas las combinaciones de grupo dual, catálogo y UI. La matriz muestra qué evidencia es integrada, aislada o estática. Los fallos confirmados bastan para REMEDIATION_REQUIRED; los controles verdes no se extrapolan a cobertura universal.

Decisiones de producto que esta auditoría **no autoriza**: borrar confirmed por una aclaración; exigir cancelar antes de reformular; mantener pendiente con precedencia sobre comandos externos; exportar perfiles o un referente anterior como radar; considerar CAS equivalente a serialización completa; activar sin exclusión entre procesos; aceptar rollback que reactive legacy; convertir capacidades publicadas en etiquetas informativas. Tampoco se exige memoria multitema nueva: F1 se limita al perfil cuya continuidad ofrece la respuesta.

## Puertas, preservación y trabajo restante

Véase `REPRODUCE.md` para comandos exactos y salidas literales, y `REQUIREMENTS_MATRIX.md` para cobertura. Ruff, Semgrep, Hypothesis y Codegraph se verificaron. Codegraph 0.9.6 funciona con Node 22; este worktree no tiene índice y no hay MCP cargado. `status`/`impact` lo documentan; **fallback declarado a rg + lectura manual**, sin afirmar grafo vigente por archivos ajenos.

`candidate_hashes.json` congela 765 archivos existentes de app/tests y streamlit; `integrity.json` confirma cero cambios. También conserva hashes de los dos adversariales originales del commit histórico. La copia de mutación terminó con igualdad de hashes de todo app. No hubo merge, push o activación.

Fuera de carril no se tocaron los diez hallazgos Semgrep heredados, los dos xfail Actor preexistentes, los documentos históricos ni decisiones compartidas. No se remedió ninguno de los diez hallazgos. La suite global sigue reservada al coordinador, en serie, después de la remediación y su revisión independiente.

# B.1: diseño previo a implementación

Un solo escritor, rama fix/entity-profile-frame-v1-2-b1-remediation desde
708ea3b3f609af4d9d3abe471f6963ee23513d7a. Ancestro runtime
1b2a85f89e5d556f1de392e11b9da437da6540d1 comprobado: todo el delta es A en
pruebas, scripts y evidencia; ningún original cambia. Sin merge/push/activación.

## Causas y cierre previsto

| Hallazgo | Causa | Archivos / prueba de cierre |
|---|---|---|
| F4 | Reserva no cubre ejecución; escritores legacy anteriores a publicación; reset sin época | store_lock, conversation_store, workflow/turn coordination y staging; exclusión con eventos/procesos, reset intercalado, publicación fallida y recarga |
| F5 | Decodificación sin integridad conjunta | envelope, workflow admission, execution_record; alteraciones unitarias y round-trip de confirmed/pending |
| F8 | ImportError admite escritor | store_lock; rechazo desde primer proceso + flock positivo |
| F9 | Render mezcla Frame ajeno | frame_response y validación común; revisión/comprador ajenos, válido |
| F1 | CLARIFY se publica como nueva búsqueda | turn_publication/coordinación; secuencias con recarga y estado idéntico |
| F2 | Pending exige otro buyer/alternativa | frame_turn, pending_resolution; reformulación completa desde cero/mismo/otro buyer y negativos ambiguos |
| F3 | Pending reclama comandos y pierde operación Actor | admission; detectores existentes + transporte de operación/token; LIST/RUN/lookup/Actor/reset/tabla |
| F6 | Cursor decorativo y seed opcional | continuation, admission, pagination, context, UI; buffer real validado y entregado al handler, TTL/dual/exhausted/referencias |
| F10 | SAVE busca historia hacia atrás | radar/context/admission; referente tabular vigente exportable, perfil sin efectos |
| F7 | Migrador puro no integrado, OFF bypass | admission/coordinación; OFF→ON migración o bloqueo, ON→OFF rechazo, legacy positivo |

## Autoridades, publicación y reset

El candado de ejecución por chat precede reserve_turn y permanece hasta terminar
publicación. Otro candado corto protege append/CAS y reset. Reset no espera la
consulta externa: incrementa una época durable y elimina la referencia visible;
una reserva con época anterior no publica. Chats distintos tienen candados distintos.

ConversationStore prepara las escrituras legacy y buffers en una generación
privada del turno. Los handlers existentes escriben allí dentro del contexto del
turno. fsync de esos archivos precede append del sobre completo, que referencia
esa generación. Sólo esa referencia publicada permite a los lectores usarla.
Una generación huérfana nunca adquiere autoridad; no se restaura ni borra estado
global tras fallos. Las trazas pueden registrar intentos, sin volverse contexto.

El sobre es la autoridad de resultado/chat/revisión; ProfileConversationState
posee confirmed/pending; plan posee SQL/secciones; ExecutionRecord acredita el
plan y llamadas. Una aclaración conserva referencias al confirmado y su registro
sin fabricar otra ejecución. La recarga valida conjuntamente estos componentes.

Los lectores legacy de historial, sugerencias y buffer se redirigen a la
generación publicada. Paginación recibe el buffer acreditado, no redescubre
history[-1]. SAVE recibe los filtros del resultado tabular actual, no busca un
turno anterior. Actor conserva su validador/token; Radar conserva permisos y
negocio. Una aclaración de perfil conserva su dueño y no ofrece capacidades de
tabla. El modo OFF del binario actualizado rechaza continuar sobres nuevos;
esto no acredita un binario antiguo sin esa guarda.

## Oráculos preservados

Los cinco tests independientes permanecen byte a byte. Los montajes A (barrera
tras ambas reservas) y B (primer HELD sin fcntl) se ejecutan y reportan separados;
se añaden ports que conservan la invariante conductual con precondición compatible.
No xfail/skip para defectos. Retirar cada protección debe causar fallo conductual.

## Instrumentos

Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph CLI con Node 22:
Not initialized; MCP no cargado. Fallback explícito rg + inspección manual.
No pytest global ni fronteras externas reales; plugin offline_guard en pytest.

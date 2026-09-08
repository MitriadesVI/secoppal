# V1.2-B.1 — Remediación conversacional

Veredicto: **V1_2_B1_READY_FOR_INDEPENDENT_REVIEW**. No es un GO de integración
ni de producción. Frame sigue apagado por defecto; sin merge, push, app, SECOP,
LLM, mensajes o radares reales. Toda persistencia de pruebas estuvo aislada.

La reparación hace que la publicación controle la continuidad: serializa antes
de reservar, prepara las escrituras legacy en una generación privada y sólo
permite heredar la generación referenciada por un sobre completo. Reset cambia
una época durable. La admisión valida conjuntamente las referencias y conserva
el dueño del perfil durante aclaraciones. Los handlers reciben referencias
reales de cursor/exportación, y los comandos explícitos conservan su autoridad.

## Referencias y disciplina

- Candidato auditado: `1b2a85f89e5d556f1de392e11b9da437da6540d1`.
- Auditoría/base: `708ea3b3f609af4d9d3abe471f6963ee23513d7a`.
- Base anterior a B: `4c86d5bafaa3296b73c6523465b33efa4311910a`.
- Rama: `fix/entity-profile-frame-v1-2-b1-remediation`.
- Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b1-remediation`.
- Un escritor; ningún subagente ni modificación de otro worktree. Se verificaron
  ascendencia y delta exclusivamente añadido de auditoría antes de crear la rama.
- Se leyeron AGENTS, CARRILES, arquitectura completa, revisión/matriz/reproducción
  independiente, cierre B y referencias A2/R3d. [Diseño previo](REPAIR_DESIGN.md)
  escrito antes de editar runtime. No se modificaron autoridades compartidas,
  QueryRouter/query_plan, catálogos, dinero, identidad, territorio, pins o ADR.
- [Integridad](original_integrity.json): 485 archivos originales de tests y de
  auditoría/script independiente conservados byte a byte. Incluye los cinco
  tests independientes y todos los controles preexistentes.

## Matriz causal F1–F10

`I` significa workflow real con persistencia/recarga; `A`, API real aislada.
Los clientes y efectos externos son sintéticos; no se sustituye admisión,
almacenamiento, parser, handlers ni validación por mocks.

| Hallazgo | Cierre e impacto | Evidencia posterior |
|---|---|---|
| F1 P1 | CLARIFY conserva estado, referencias y registro; no crea owner NONE ni capacidades tabulares | I: `test_clarification_preserves_the_profile_it_offers_to_continue`, `test_clarification_retains_execution_without_new_calls`; ocho secuencias originales y propiedad de referencias |
| F2 P1 | Composición independiente soportada con buyer propio puede reemplazar cualquier pending; «sí»/cambio incompleto no ejecutan | I: seis combinaciones originales `test_full_reformulation_exits_a_pending_without_alternatives`; cancelación, selección y R5d |
| F3 P1 | Detectores existentes de Radar/lookup y decisiones del router preceden al pending; operación/token Actor llegan íntegros | I: comandos externos, RUN sintético; [port Actor](PORTS.md), API/handler con estados independientes, recarga y replay rechazado; mutaciones F3 |
| F4 P1 | Lock por chat durante reserva→admisión→ejecución→publicación; CAS adicional; época reset; generación privada de historial/buffer | I/procesos: `test_real_workflows_serialize_across_processes_and_reload`; I/hilos: serial y chats distintos; reset intercalado y fallo de publicación originales; tres mutaciones causales |
| F5 P1 | Se valida el sobre completo antes de admitir: chat, revisión, IDs, Frame, pending/base, plan/evidence, catálogo, esquema, jobs, SQL y predicados del snapshot | I: nueve alteraciones originales, cinco nuevas, round-trip confirmed/pending, migración y registro preservado; mutación de validación conjunta |
| F6 P1 | Publicación referencia buffer real por hash de contenido, grupo, generación, fingerprint, contrato de orden y expiración. Handler recibe ese buffer. Referencia UI obligatoria; dueños explícitos | I: seis alteraciones originales, TTL físico, ambos «más», agotado sin llamadas, dual→aclaración→grupo, seed válido/ausente/viejo; owner Actor/Radar/lookup; propiedades y mutaciones |
| F7 P1 | Migración equivalente integrada antes del flujo legacy; incompatible bloquea sin llamadas. OFF rechaza continuidad B y permite Radar LIST/RUN explícitos | I: OFF→ON compatible, tres incompatibles; tabla→perfil→OFF→más; controles OFF históricos, Radar OFF; mutaciones activation/rollback |
| F8 P2 | ImportError de fcntl rechaza antes de admitir escritores | Dos procesos rechazan desde el primero; flock positivo y port B; mutación no-fcntl |
| F9 P2 | Assembler/renderer validan referencia exacta y predicados del Frame contra plan | A: revisión/comprador ajenos y control válido; assembler adicional; mutación específica |
| F10 P1 | SAVE sólo recibe filtros del Turn exportable actual; nunca busca una tabla anterior al perfil | I: cero efectos después de perfil; tabla vigente guarda mismos filtros/owner; LIST/RUN permanecen; mutación de exportación |

Los diez hallazgos del candidato anterior se consideran **corregidos**, no
refutados. La incompatibilidad posterior de un montaje no niega el defecto que
ese original probó. No queda un P1/P2 encargado marcado inconcluso.

## Publicadores, consumidores y lectores retirados

| Autoridad | Publicador | Consumidor / comprobación |
|---|---|---|
| Época reset | `store_epoch.advance_epoch`, bajo lock del sobre | reserva y CAS rechazan época anterior aunque revisión vuelva a cero |
| Sobre y revisión | `ConversationStore.publish_turn`, fsync de archivo/directorio | `admission._restore` + `envelope_validation`; no retroceso ante cola corrupta |
| Generación de continuidad | `stage_continuity` prepara archivos y fsync de ancestros; el sobre referencia generación | rutas del mismo ConversationStore; generaciones huérfanas no se leen |
| confirmed/pending | `frame_turn` + `profile_contract` | pending/base y confirmado coherentes; aclaración retiene ejecución anterior sin consultas inventadas |
| Programa autorizado / ejecución | `build_execution_record` conserva snapshot, digest, Frame y llamadas/métricas | `record_validation` comprueba membresía y coherencia sin recompilar SQL ni cambiar significado |
| Cursor | publicador lee el buffer preparado y emite `TabularGroup` | `validate_tabular` entrega buffer exacto a `HandlerContext`; handler no consulta history[-1] en la ruta admitida |
| Exportación | `export_ref` al Turn actual en generación publicada | `export_params` exige ese Turn exacto; Radar recibe params acreditados, sin búsqueda retrospectiva |
| Actor/Radar | handler declara dueño; contrato referencia resultado/traza existente | validadores de Actor, token, TTL, versión e identidad siguen intactos; Radar conserva owner/permisos |
| UI | Streamlit transporta result_id/revisión y chat cuando está disponible | admisión coteja referente actual; ID ajeno, referencia ausente/incompleta o chat discordante no autoriza |

Los escritores legacy conservan su API, pero escriben dentro de la generación
privada del turno B. La misma interfaz de lectura ve exclusivamente la generación
publicada. No hay otro almacén semántico ni una base de estado Burr. Un fallo
no provoca borrado/restauración global. No se promete deshacer efectos externos:
si SAVE/RUN ocurrió y después falla publicar, el resultado del handler y el aviso
de contexto no guardado se devuelven juntos; no hay reintento automático.
Las generaciones antiguas/huérfanas permanecen en disco sin autoridad: no se
implementó recolección automática de basura ni retención productiva en este lote.

`reserve_turn` aislado sigue siendo una primitiva CAS; la exclusión durante toda
la ejecución pertenece a `coordinate_turn`. Reset usa el lock corto de publicación
y puede invalidar una reserva mientras su consulta está en curso. Los procesos
comparten sistema de archivos; no se afirma exclusión distribuida.

## Migración y rollback

| Transición | Resultado acreditado |
|---|---|
| OFF legacy equivalente → ON → proveedores | Migración por checker existente; Frame con provenance legacy_migration; ejecución/recarga posterior válidas |
| OFF legacy sin request, periodo discordante o evento no soportado → ON | Aclaración, cero llamadas |
| ON tabla → perfil → OFF → más | Rechazo explícito; tabla antigua no revive |
| Perfil B → OFF → Radar LIST/RUN | Se mantiene la operación explícita y su owner; no permite «más» del contexto incompatible |
| OFF sin sobre nuevo | Paridad en targets históricos dirigidos, incluyendo Actor y paginación legacy |

Rollback seguro exige **este binario actualizado con la guarda de lectura**.
Un binario anterior que desconoce sobres/generaciones no se declara soportado.
Ante un sobre incompatible se pide reset/nueva conversación; no se interpreta
legacy como si representara ese resultado. B antiguo sin snapshot suficiente
se rechaza por integridad al cargar; no se inventa evidencia retroactivamente.

## Observaciones estáticas adjudicadas

`not_requested_sections` tiene consumidor explícito en assembler, valida su
partición con expected y produce secciones no solicitadas; el renderer recibe
esas secciones. Los seis focos y presupuestos conservan sus controles verdes.

ExecutionRecord ya no conserva sólo etiquetas/llamadas: serializa SourcePlans,
jobs/SQL, predicados, limitaciones, versión y referencia de Frame, más resumen de
Evidence y snapshot del Frame confirmado. El digest acredita consistencia del
snapshot almacenado, no autenticidad frente a alguien que reescriba coherentemente
todo el almacén. No es firma criptográfica ni una segunda autoridad ejecutora.
La nueva consulta compila desde Frame después de validar la publicación actual.

Dos tests del autor exigen ignorar un registro falsificado o catálogo desconocido
para seguir ejecutando. Esa expectativa contradice expresamente F5 de la orden
B.1 y se conserva **roja e intacta**, separada en [PORTS.md](PORTS.md). No se
presenta como control verde ni se modifica la guarda para satisfacerla.

## Fuera de carril

Al ampliar inicialmente la fixture M9 a valores no finitos del ranking de
proveedores, se reprodujo `OverflowError: cannot convert float infinity to integer`
en `analytics/base.py:36`, invocado por el renderer legacy de hechos. La misma
prueba falló sobre el candidato congelado, en exportación temporal offline:
`supplier_nonfinite_baseline.log`. No se tocó ese parser/renderer ni se añadió
un P1 al encargo. Es un hallazgo adicional para enrutar al coordinador.
La fixture amplia queda reproducible en el commit `c75297c`, y sus salidas están
conservadas; el control M9 final usa el agregado contractual original acreditado.

También permanecen fuera de alcance los diez findings Semgrep, los dos xfails
Actor, documentos históricos y datos/untracked del worktree principal.

## Validación y límites

Los comandos, salidas literales, SHAs y particiones están en
[REPRODUCE.md](REPRODUCE.md). Corrí los targets allí enumerados, **no la suite**.
Ruff global verde; Semgrep global baseline 10/delta 0 por identidad y fragmento;
lint-core, tamaño y diff-check ejecutados. Hypothesis determinista en propiedades
originales y nuevas; Codegraph CLI 0.9.6 sin índice/MCP, fallback rg/manual.
La campaña definitiva sobre el HEAD probado acredita 27/27 en `mutations.json`,
sin kills de setup, import o timeout y con hashes de todo app restaurados.
Se conservan los intentos previos, incluida la repetición focal M9.

Pendientes por alcance: revisión independiente B.1, gate global del coordinador,
evaluación del hallazgo adicional de proveedores y cualquier decisión de
integración/activación. No se certifican todos los entornos/canales ni ACK físico.

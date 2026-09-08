# SECOPPAL V1.2-B.2 — cierre residual

**V1_2_B2_READY_FOR_INDEPENDENT_REVIEW**. Resultados literales y límites en RESULTS.md.

Código congelado y probado: `aaa9168b75a5633469af507a1cf7c1c2e2c1ce00`.
Base de auditoría: `af3d0c7a26182fc26599a4b764ec60fe600b17f8`.
Candidato B.1: `af8f0a3407102dcb807089e87e470d754b835d61`.
Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b2-remediation`.
Rama: `fix/entity-profile-frame-v1-2-b2-residual-remediation`.

Un solo escritor. La ascendencia candidato→auditoría está verificada; todo el
delta de auditoría era añadido, sin modificar runtime ni tests del candidato.
El worktree y la rama eran nuevos, y el checkout inicial estaba limpio. Se
leyeron AGENTS, CARRILES, arquitectura congelada, revisión/reproducción/
reconciliación independientes, sus cinco tests y los documentos de diseño,
ports y resultados B.1. El contrato previo está en REPAIR_CONTRACT.md.

## Reparación y evidencia por bloqueo

| Bloqueo | Antes, node IDs en before.txt.gz | Después, misma aceptación intacta |
|---|---|---|
| F4 | `failed_io_never_authorizes…[envelope_fsync]`, `[directory_fsync]`; `append_boundary…[False]` | La barrera por chat/época impide que JSON completo declarado no publicado gobierne sobre, historial, buffer, exportación o continuación. Las tres pruebas pasan; también pasan los errores de preparación y escritura parcial originales. |
| F5 | `individual_record_damage…[call_duplicate]`; `record_call_membership_preserves_multiplicity` | Correspondencia uno a uno entre jobs y observaciones: cardinalidad y membresía, sin deduplicación. Dos originales verdes. Las observaciones no intentadas siguen siendo registros legítimos y se distinguen de llamadas intentadas. |
| F9 | `foreign_buyer_with_reused_ids…[assembler]`, `[renderer]` | BuyerScope completo congelado en ProfilePlan y su snapshot. Igualdad estructural antes de ensamblar/renderizar; recarga valida el mismo vínculo sin resolver ni interpretar SQL. Dos originales verdes. |
| Proveedores | `supplier_invalid_value_keeps_a_product_response[Infinity-overview]`, `[Infinity-suppliers]`, `[1e999-overview]`, `[1e999-suppliers]` | Validación monetaria previa al modelo y al registro. Monto no acreditable deja valor ausente y estado explícito; conserva proveedor/conteos y el resto de hechos. Cuatro originales verdes. |

La reproducción inicial fue `11 failed, 45 passed in 17.54s`. No se adjudica
cierre restando contadores: `test_reconciliation_proposed.json` contiene cada
node ID original y su resultado extraído del XML de aceptación final. Ninguno
se archivó, modificó, deseleccionó o marcó xfail. Los originales históricos y
toda su evidencia están preservados por hash.

## Publicación: contrato y tabla de fallos

`workflow/publication_io.py` extrae exclusivamente la escritura confirmada. La
barrera no almacena Frame, filtros ni otra autoridad semántica. Se crea y
sincroniza antes del append, bajo el lock de publicación existente. La lectura
requiere ausencia de barrera en la época actual, JSON/revisión válidos y las
validaciones conjuntas existentes. La confirmación retira la barrera después
del flush, fsync del sobre y fsync del directorio. No se eliminó ningún fsync
previo. El candado de ejecución, CAS, generaciones privadas y reset se conservan.

| Fallo alcanzado | Estado que reconoce otra instancia | Continuación/recuperación |
|---|---|---|
| fsync de historial o buffer privado | Sobre anterior; generación fallida huérfana | Continúa autoridad anterior acreditada; otro chat intacto |
| escritura de barrera completa seguida de error | Barrera visible; sobre anterior o inexistente | Continuidad inválida; no se consulta desde el fallo |
| fsync de barrera o su directorio | Barrera; ningún append nuevo | Continuidad inválida o anterior si no hubo persistencia de barrera |
| append parcial/completo seguido de error | Barrera y cola fallida | Continuidad inválida; cola fallida no autoriza Frame/cursor |
| fsync de sobre/directorio tras append | Barrera durable | Continuidad inválida; no se infiere éxito por JSON legible |
| retirada de barrera falla antes de visibilidad | Barrera | Se confirma de nuevo el bloqueo antes de devolver False |
| retirada falla después de visibilidad | Se vuelve a crear y sincronizar la barrera | False sólo después de cercar otra vez |
| retirada visible + falla rearmar barrera | Se trunca sólo el append propio al offset previo y se sincroniza | Prefijo anterior o continuidad inválida; no restaura globalmente otros chats/revisiones |
| fracaso físico simultáneo al cercar y truncar/sincronizar | `PublicationRecoveryError` | No devuelve éxito ni un False acreditado; no se presenta como una variante de éxito incierto |
| reset durante ejecución | Época anterior | La reserva previa es rechazada; las barreras antiguas quedan inertes |
| nuevo append tras una cola parcial | Separador de línea antes del nuevo sobre | El nuevo resultado completo/revisión puede recargarse; el fragmento previo sigue inerte |

Las pruebas nuevas discriminan inodes/paths reales e incluyen errores después
de visibilidad de la nueva primitiva, recarga con proceso nuevo, otro chat,
perfiles/tablas, siguiente continuación y recuperación tras reset. La
recuperación de escritura completa/parcial también se prueba directamente en
ConversationStore. No hay ports nuevos: los inyectores independientes
originales siguen alcanzando su frontera. Los tres ports históricos B.1 se
conservan con su adjudicación previa.

Visibilidad no equivale a durabilidad. No se promete inmunidad ante todos los
fallos físicos simultáneos. El borrado final de barrera no se sincroniza con una
operación posterior que pudiera convertirlo en un False ambiguo: tras una caída
física puede reaparecer la barrera y bloquear conservadoramente un éxito. El
éxito ordinario, la recarga con otra instancia y la continuación sí se prueban.
La barrera ya sincronizada protege los fallos anteriores a confirmación. No se
implementó limpieza/retención de generaciones ni de barreras de épocas pasadas.

## Comprador, registros y montos

El comprador incluye kind, display_name, canonical_buyers, authority,
authority_version y group_id. La proyección SQL queda congelada como antes.
Baranoa y el grupo soportado Bogotá se prueban con copia serializada equivalente
y daños individuales, conservando sus miembros reales. Alterar el comprador del
snapshot y recalcular sólo su digest también se rechaza en la recarga. Un digest
acredita consistencia, no autenticidad frente a quien reescriba coherentemente
todo el almacén. Un snapshot antiguo sin el nuevo vínculo no se completa a ojo:
se rechaza continuidad; no se recompila desde un Frame potencialmente ajeno.

Para observaciones, los fallos parciales/totales y `BUDGET_EXHAUSTED` no intentado
siguen siendo válidos. Hypothesis determinista comprueba duplicación con
observaciones intentadas y no intentadas; una ejecución real sin presupuesto
publica sus observaciones y se puede continuar tras recargar.

`parse_suppliers → SupplierMetric → secciones → render` usa validación Decimal
estricta existente y acredita la representación float sólo si sigue siendo
finita. Infinity, -Infinity y NaN son INVALID. `1e999` es Decimal finito pero
REPRESENTATION_OVERFLOW; `1e-999` se distingue como REPRESENTATION_UNDERFLOW,
sin inventar un cero. Ausencia/cadena vacía son UNAVAILABLE; texto malformado,
INVALID. No se impone un techo monetario de negocio. Valores normales conservan
su conversión y formato; Hypothesis comprueba paridad. `as_float` y el formatter
compartido de analytics no cambiaron.

El registro persistido marca INVALID los montos de proveedor que su presentación
no puede acreditar, en vez de seguir contándolos como métricas VALID. Los
conteos, identidad, montos de otros proveedores y agregado contractual principal
sobreviven. Una sección de proveedores incompleta se declara PARTIAL; el render
no afirma el principal por valor si el ranking tiene métricas ausentes. El
ranking por cantidad sigue disponible. La frase histórica «valor no disponible»
se conserva. Los tests de mezcla cubren overview/suppliers/values, presupuesto
4/2/1, publicación y siguiente turno; values tiene comparación textual positiva.

## Regresiones, instrumentos y límites

Los resultados finales y los comandos literales están en RESULTS.md y los
archivos de salida comprimidos. No sumar conjuntos superpuestos. Se ejecutaron
las siete familias cerradas F1/F2/F3/F6/F7/F8/F10 mediante los 70 node IDs vigentes,
los cuatro controles históricos verdes y los ports B.1; incluye serialización
entre procesos, rechazo sin fcntl, reset, Actor/token, Radar sintético SAVE/LIST/
RUN, UI, cursor/TTL y rollback. También A2/R3d/13 contraejemplos, R1/R2/R4, ocho
escenarios con recarga, seis focos/presupuestos, CLEAR, exclusiones, reloj,
publicaciones válidas y fuentes parcial/totalmente fallidas.

Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10. Codegraph 0.9.6 con Node 22:
Not initialized; query no disponible y MCP ausente. Fallback explícito rg y
lectura manual; consumidores inventariados en consumers.txt.gz. No se usó un
índice ajeno como si fuera actual. Disponibilidad no equivale a validación.
Semgrep: baseline diez, actual diez, delta cero por regla/ruta/fragmento completo,
no sólo por conteo. Las 2.457 rutas congeladas ajenas al delta runtime conservaron
bytes: originales/tests, evidencia, arquitectura, AGENTS/CARRILES, configuración,
identidades, guardas/pins, ADR/bitácora y demás archivos; ver preservation.json.

**Corrí estos targets, no la suite.** No app, SECOP/LLM live, mensajes, radares
reales, datos de usuario, merge, push ni activación. Persistencia de pytest y
mutaciones en temporales; plugin offline_guard bloquea sockets IP.

Fuera de alcance permanecen diez findings Semgrep y dos xfails Actor previos.
La reconciliación activa definitiva y el gate global pertenecen al coordinador.
La propuesta conserva los originales, ports, cinco probes observacionales y las
dos expectativas históricas reemplazadas; no introduce deselect permanente.
No se reabre su adjudicación ni se hacen pasar cambiando runtime. Frame OFF por
defecto, configuración intacta.

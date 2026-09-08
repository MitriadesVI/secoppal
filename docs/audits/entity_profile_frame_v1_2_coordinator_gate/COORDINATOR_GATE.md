# Gate coordinador Frame V1.2

**Veredicto: GATE_INCONCLUSIVE.** Gate ejecutado completo, exit 1 por dos
guardas de paridad no adjudicadas. No se solicita merge. No se ha integrado
ni activado el Frame. No hay un fallo funcional demostrado en esta corrida.

## Alcance y preservación

AGENTS.md, CARRILES.md, diseño congelado y las adjudicaciones B/B.1/B.2/B.3
consultados. La asignación explícita del usuario autoriza este carril y una
suite global serial. Sin app, merge, push, despliegue, runtime ni reglas nuevas.

`reconciliation.json` conserva commit, SHA256 y archivo original→activo;
`case_map.json` clasifica sólo casos Frame afectados/complementarios.
Los originales de los cinco archivos modificados de tests/ están archivados
byte a byte con extensión `.original` fuera de colección. Los originales
históricos B.2/B.3 y su evidencia permanecen en sus rutas, sin modificaciones.
Las copias activas preservan fixtures, helpers y aserciones vigentes; las
variantes sustituidas se retiran de parametrización, sin deselects ni xfails.

Tres ports B.1 y dos expectativas F5 usan exactamente las adjudicaciones
referenciadas por B.3. En B.3 se conservan los controles positivos de B.2 y se
ejecutan sólo los ports complementarios correspondientes, sin contar dos veces
las variantes equivalentes. No se archivan completos archivos mixtos.

## Reloj y reset

El helper pasaba TODAY a QueryRouter y no a collect_claims. Ahora propaga la
misma fecha explícita a ambos. Se conserva la aserción histórica 2026-09-07 y
se añaden dos confirmaciones en fechas distintas con continuación/recarga
posterior; su periodo confirmado no cambia. Se conservan todos los controles
del reloj del workflow real y las propiedades Hypothesis existentes.

El nuevo control ejecuta A confirmado → B con fsync fallido → /reset real →
lectura desde otro proceso → nueva consulta → continuación. Observa que el
handler real llama ConversationStore.clear; exige ausencia de sobre/historial
tras reset y que la publicación nueva sea la solicitada, sin B ni continuidad
anterior. No redefine el conjunto de confirmados desde lo que quedó legible.

La primera versión del nuevo test exigía historial legacy no vacío tras un
perfil publicado: supuesto erróneo del montaje, pues el perfil usa su sobre.
Se conservó el rojo en focal_focused.log y se corrigió para exigir el sobre
actual y la ausencia de B. El reset se repitió focalmente, sin tocar runtime.

## P2 y límites de operación

P2-1 de B.3 sigue presente: si falla unlink después de hacerse visible, falla
el rearme y además falla escribir la marca después de truncate, puede quedar
admitido B, un registro anterior expresamente no publicado. Se lanza
PublicationRecoveryError. La excepción no certifica bloqueo ni inmunidad
física. No es sólo el caso de que quede íntegro el sobre nuevo.

Registrar este límite permite continuar el gate para solicitar integración
con Frame OFF. No es PRODUCTION_GO ni aceptación de riesgo para terceros.
El control de /reset usa almacenamiento recuperado después del fallo
inyectado: no demuestra que un reset siempre funcione con almacenamiento
averiado. probe_reset.py sólo llama advance_epoch; no ejecuta handle_reset
ni ConversationStore.clear. Su observación no sustituye el control de comando.

## Instrumentación

Preflight: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph CLI
rechazó Node 26.8.1 al solicitar versión/status; no se confió en su índice ni
se declaró MCP activo. Fallback explícito a rg, lectura de consumidores/helpers
y Git. Se añadieron controles de fechas, no una propiedad ornamental para
el cambio mecánico del helper. Las propiedades existentes se ejecutan en el gate.

## Pendientes antes de integración

El bloqueo pendiente son las dos divergencias de hashes descritas abajo;
requieren adjudicación explícita antes de actualizar cualquier guarda.
Cualquier merge, push, dogfood, despliegue o activación requiere decisión
posterior. La integración deberá comprobar nuevamente main, preservar Frame
OFF y reflejar el resultado real en la bitácora canónica, sin anticiparlo.

## Propiedad heredada con oráculo circular

La copia intacta `active/b3_independent/test_f4_property.py` se ejecuta sólo
como reproducción diagnóstica histórica, NO como garantía de seguridad: su
rama reset llama advance_epoch y añade a confirmados lo que quedó legible.
Ese mecanismo no acredita el contrato de /reset. Se rectifica su categoría en
case_map.json; no se modifica su código durante el gate ni el original.
La garantía de comando la acredita test_coordinator_reset (dos variantes),
con expectativas previas independientes de lo que dejó la primitiva. La
propiedad no_promotion_property de B.3 remediación conserva su modelo A/blocked,
y verifica clear/CAS con primitivas reales. No se usan estos verdes para
prometer un reset exitoso mientras el almacenamiento siga averiado.

## Resultado ejecutado y bloqueo

GATE_CODE_HEAD: `db9b39195d144fd4fe1c8c80f085597fd7a94402`.
MAIN_BEFORE = MAIN_AFTER: `7868664757b28bed16c583ea50327d669f1501e3`.
Main sigue siendo ancestro del candidato. El intervalo está registrado en
full_refs.json y main_candidate_interval.txt. El commit final sólo incorpora
evidencia/documentación; su SHA literal se entrega en la respuesta final de
la tarea, fuera de su propio contenido. Para resolver HEAD_FINAL:
`git log -1 --format=%H -- docs/audits/entity_profile_frame_v1_2_coordinator_gate/gate_verdict.json`.

Salidas literales principales:

```text
Ruff global y complementario:
All checks passed!

Semgrep base y candidato:
Ran 3 rules on 326 files: 10 findings.

Suite global:
2 failed, 13055 passed, 11 skipped, 16 xfailed in 1081.90s (0:18:01)

Corpus adicional:
Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
```

Los logs completos de cada componente están en full_*.log; comandos, SHA,
duración y código de salida en full_results.json. Semgrep completó sin errores
ambos escaneos, con baseline rojo 10, candidato 10, cero nuevos y cero cambios
de ubicación. La exclusión por tamaño reportada no afecta a ningún archivo
Python de app/; no hay .py de más de 1 MB en ese árbol.

Aceptación complementaria (sesiones secuenciales sin superposición de node IDs):
B.2: 47 passed; B.3 remediación: 303 passed; B.3 independiente: 115 passed.
Seis casos de esos bloques son diagnósticos, no garantías (dos observaciones
B.2, tres límites físicos y la propiedad heredada con oráculo circular).
Cero skips, xfails, deselects o llamadas HTTP en los bloques complementarios.
El tamaño de módulos pasó dentro de la suite global. lint-core y diff-check
pasaron. No hubo una segunda corrida global.

Bloqueos reproducidos:

| Guarda | Diferencia | Focal candidato | Runtime auditado | Main |
|---|---|---|---|---|
| test_control_language_repinned_files_match_their_pinned_state | pagination.py | FAIL | FAIL | PASS |
| test_m2_1_modified_consumers_match_their_pinned_state | streamlit_app.py | FAIL | FAIL | PASS |

El módulo completo de paridad dio `2 failed, 12 passed in 1.81s` al reproducir
focalmente. Los dos mismos tests sobre una exportación de 4edc248 dieron
`2 failed in 0.20s`; sobre main dieron `2 passed in 0.18s`. Los cambios de
pagination.py provienen de ff2a5844c3f2c35a756e965f27f66465ea50af9c; streamlit
cambió en 20aea784110cf7d62ad3e30133d9254c9f7353d1 y ff2a5844c3f2c35a756e965f27f66465ea50af9c.
Hashes y commits completos en pin_drift_provenance.json. Los dos archivos son
byte a byte iguales entre runtime auditado y GATE_CODE_HEAD.

**Clasificación:** divergencia de guarda heredada del candidato auditado,
pendiente de adjudicación. No es un error de red ni un defecto introducido por
la reconciliación. Tampoco se demuestra un defecto funcional simplemente por
un hash distinto. Los controles de comportamiento verdes no autorizan ignorar
la guarda ni repinear sin revisar/acreditar la divergencia. Por eso se usa
GATE_INCONCLUSIVE, y no GO ni REMEDIATION_REQUIRED de runtime.

El primer diagnóstico de main con el registrador de flags falló por montaje:
main todavía no define entity_profile_frame_enabled. Se conserva ese intento
como diagnostic_main.log (exit 3). La reproducción compatible usó los mismos
tests intactos, offline_guard y persistencia temporal, sin el registrador de
una API inexistente. No es una corrida global ni una reparación de producto.

## Excepciones, flags y red

Inventario completo por node ID, motivo y commit de procedencia en
exceptions_with_provenance.json. Once skips: ocho requieren índice productivo
Actor, uno verifica aislamiento sólo si ese índice existe, dos son recall
live ya marcados como no asertables. No se usó el índice real ni se retiraron
esas marcas para fingir cobertura. Dieciséis xfails: seis de Observer/mock de
aggregate, dos de Actor región+buyer, tres de gazetteer y cinco casos heredados
de follow-up/ordering/opportunity/referencia. No se añadieron marcas. Sus
archivos coinciden con BASE; no se afirma haber corrido una suite global de
main para calcular un delta dinámico de outcomes.

El entorno inicial fue Frame OFF. El registrador posterior a fixtures observó
ON en 257 casos globales y OFF en 12824; tres skips de setup no entraron al
cuerpo de la prueba. Los bloques complementarios respetaron las fixtures ON;
las pruebas directas de almacenamiento no requieren activación del Frame.
Los valores por caso, antes y después del cuerpo, están en full_*.json. No se
atribuyen a esa medición cambios intermedios no observados dentro de un test.

Se observaron 917 intentos HTTP en la suite global, todos al host público
www.datos.gov.co y autorizados por la guarda; esto no acredita 917 respuestas
exitosas. Los tres bloques Frame registraron cero conexiones HTTP y usaron
además el bloqueo IP offline. No se ejecutó dogfood live, tráfico LLM de pago,
mensajes, radares reales ni datos del usuario. La guarda serializó las
peticiones reales; no se introdujeron barridos ni consultas nuevas.

## Preservación y siguiente paso

full_hashes_before.json/full_hashes_after.json son idénticos: runtime y toda
la evidencia histórica preservados. code_frozen=true acredita que pruebas,
helpers y configuración permanecieron congelados durante el gate. Sólo se
precisó documentación/clasificación de observaciones durante la corrida.
La colección ejecutada coincide con la colección inventariada.

Fuera del carril y sin tocar: las dos guardas de tests/test_truth_layer_public_parity.py,
los dos archivos productivos afectados, baseline Semgrep y skips/xfails ajenos.
El siguiente paso es adjudicar las dos divergencias de paridad y determinar
el cambio acotado de guardas y su validación. No se repinea automáticamente.
No está autorizado solicitar merge mientras ese bloqueo permanezca. Dogfood,
aceptación de riesgos de terceros y activación son decisiones posteriores.


Almacenamiento de salidas literales: algunos logs y XML contienen espacios
finales emitidos por pytest/Semgrep. Se conservan byte a byte comprimidos con
gzip, sin limpiar ni reescribir la salida. artifact_storage.json mapea los
nombres lógicos de los comandos/resultados a .gz y registra ambos SHA256.
La descompresión reproduce exactamente la salida original. Esto permite
verificar diff-check también sobre el commit documental final.

## Segunda corrida — adjudicación autorizada de las dos guardas

La primera corrida y su GATE_INCONCLUSIVE se conservan íntegros arriba. El
usuario autorizó el cierre de las dos huellas y una nueva corrida completa si
sus hunks se acreditaban. La adjudicación está en round2/ADJUDICATION.md.

Los cinco hunks de los dos archivos pertenecen al transporte/consumo de
referentes B/B.1 F6; no hay cambios ajenos. Sesenta controles focales offline
pasaron, incluidos 14 controles acotados que ejecutan UI real extraída por AST
y controles legacy de orden/universo con Frame ON. Se actualizaron sólo los
dos SHA256 calculados de archivos idénticos al runtime auditado; todas las
funciones/aserciones y restantes hashes del módulo permanecen idénticos.
El módulo de paridad completo pasó 14 pruebas tras la actualización.

Ruff pasó; Semgrep completó con el baseline rojo de 10 y cero nuevos;
diff-check pasó. Se reutiliza el runner con --output-dir round2 para no
sobrescribir la primera corrida. Esa adaptación sólo cambia rutas de salida,
no targets, flags, restricciones de red ni criterios de aprobación.

El nuevo SHA congelado y los resultados de la segunda corrida se consignarán
en round2/full_refs.json, round2/full_results.json y round2/CLOSEOUT.md. Los
límites P2, las observaciones de reset y los baselines siguen vigentes. Este
cierre no autoriza merge, push, despliegue, dogfood ni activación.

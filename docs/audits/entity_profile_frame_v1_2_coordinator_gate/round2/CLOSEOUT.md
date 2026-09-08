# Cierre de las dos guardas — segunda corrida

**V1_2_FRAME_GO_FOR_MERGE_OFF.** Adjudicación, validación focal y gate completo
satisfactorios. Puede solicitarse integración con Frame OFF. No se efectuó
merge, push, despliegue, dogfood ni activación. No es PRODUCTION_GO.

## Qué cambió

Se adjudicaron todos los hunks de pagination.py y streamlit_app.py entre main
y el candidato. La divergencia autorizada B/B.1 F6 es consumir el referente
validado y transportar result_id/revisión/chat desde la UI. Los filtros, orden,
universo y controles legacy se conservan. ADJUDICATION.md detalla antes,
autorización, controles e invariantes por cada uno de los cinco hunks.

Se actualizaron exclusivamente las dos huellas de
`tests/test_truth_layer_public_parity.py`, calculadas desde los archivos
verificados contra el runtime auditado. Se añadieron comentarios breves con
commits/controles; las demás huellas y el AST de todas sus funciones son
idénticos. Se añadieron 14 controles acotados para el transporte UI real y
la ejecución de controles legacy exactos con Frame ON. El único ajuste del
runner es --output-dir para conservar ambas corridas sin sobrescrituras.
No cambió runtime, configuración productiva, reglas ni dependencias.

## SHAs y compatibilidad

- Cierre anterior: `2de487228a091842a3a553f0db503f78f44008a4`.
- Primera corrida, GATE_INCONCLUSIVE: `db9b39195d144fd4fe1c8c80f085597fd7a94402`.
- Runtime auditado e intacto: `4edc248e60610efdbeebb3cb8010bc7b59445cad`.
- Nuevo GATE_CODE_HEAD: `24d67fcb9195e5d79b9dca59efa38f42e0973195`.
- MAIN_BEFORE = MAIN_AFTER: `7868664757b28bed16c583ea50327d669f1501e3`.

Main no avanzó y sigue siendo ancestro del nuevo candidato. El HEAD final de
entrega sólo incorpora documentación/evidencia posterior al gate; el SHA
literal se entrega en la respuesta de la tarea, fuera del commit que nombra.
Se resuelve también con:

```sh
git log -1 --format=%H -- docs/audits/entity_profile_frame_v1_2_coordinator_gate/round2/gate_verdict.json
```

## Salidas literales

Validación focal anterior a congelar el nuevo candidato:

```text
60 passed in 9.98s
14 passed in 1.58s
```

Gate completo sobre el nuevo SHA, una sola corrida serial autorizada:

```text
Ruff global y complementario:
All checks passed!

Semgrep, base y candidato:
Ran 3 rules on 326 files: 10 findings.

Suite global:
13071 passed, 11 skipped, 16 xfailed in 1009.65s (0:16:49)

Complementarios vigentes bajo docs/audits/:
47 passed in 16.96s
303 passed in 40.56s
115 passed in 33.42s

Corpus adicional:
Summary: 89/89 PASS, 0 FAIL (mode=all, severity=any, limit=none)
```

Runner exit 0. Todos los componentes de full_results.json tienen pass=true,
incluidos lint-core, diff-check, preservación y congelación del código. El
tamaño de módulos pasó dentro de la suite global. Los logs/XML/JSON completos
se encuentran en este directorio. Los archivos comprimidos conservan la salida
literal; artifact_storage.json registra ruta lógica, gzip y SHA256 original.

Semgrep completó con cero errores, 10 findings heredados, 10 en el candidato,
cero nuevos y cero cambios de ubicación. Baseline rojo, no cero findings.
La evaluación compara regla/ruta/fragmento, no sólo conteos. Ruff 0.16.4,
Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph CLI no inició con Node 26.8.1;
se usaron rg/Git/lectura manual, sin instalaciones ni un índice supuesto.
Las propiedades Hypothesis existentes se ejecutaron; no se añadió una
propiedad ornamental para un cambio de huellas y destinos de logs.

## Colección, excepciones y red

La colección global pasó de 13084 a 13098: exactamente los 14 controles nuevos,
sin retirar un solo node ID. Los tres bloques complementarios conservaron
exactamente sus colecciones de 47/303/115, sin skips/xfails/deselecciones.
Seis casos complementarios siguen siendo observaciones diagnósticas, no
garantías, conforme al manifiesto de la primera corrida. No se reabrió F4,
proveedores ni los ports: se ejecutó la aceptación existente.

Los 11 skips y los 16 xfails son los mismos node IDs/motivos de la primera
corrida; delta cero. Se conservan sus causas: índice productivo Actor ausente,
recall live no asertable, y deuda heredada de Observer/Actor/gazetteer/follow-up.
No se usaron datos reales del usuario para eliminar esos skips. No hay nuevas
marcas ni fallos no adjudicados.

Frame inició OFF. Tras las fixtures, el registrador observó ON en 265 cuerpos
de prueba globales y OFF en 12830; tres skips de setup no entraron al cuerpo.
Los valores efectivos por caso y al terminar su cuerpo están en full_*.json.
Se respetaron las fixtures ON de aceptación; las pruebas directas de storage
no requieren activar Frame. No se modificó .env ni configuración productiva.

Se observaron 911 intentos HTTP de pruebas históricas al host público
www.datos.gov.co, todos autorizados por la guarda serial; no se afirma que
cada intento obtuviera respuesta exitosa. Los 14 controles nuevos y los tres
bloques complementarios tuvieron cero intentos HTTP. Los bloques Frame además
usaron offline_guard. Persistencia temporal desde antes de colección.

## Preservación y límites que continúan

Los 128 archivos protegidos de evidencia de la primera corrida son idénticos
por SHA256; su informe se conserva como prefijo íntegro, con secciones nuevas.
La primera corrida continúa registrada como GATE_INCONCLUSIVE. No se borraron
sus fallos ni se sustituyeron sus logs. Runtime y evidencia histórica también
coinciden antes/después según full_hashes_*.json. El código quedó congelado
al SHA probado durante toda la ejecución.

El P2 de recuperación permanece sin reparar: unlink visible + rearme fallido
+ fallo de escritura de la marca después de truncate puede dejar admitido B,
un registro anterior no publicado, y lanzar PublicationRecoveryError. La
excepción no acredita bloqueo. El reset real pasó sobre almacenamiento
recuperado; no promete éxito mientras el almacenamiento siga averiado. La
propiedad heredada que cambia su ledger según lo legible sigue siendo sólo
observación. Estos límites no son aceptación de riesgo para terceros.

Fuera del alcance y sin tocar: runtime, Semgrep heredado, skips/xfails ajenos,
bitácora canónica y configuración productiva. No quedan bloqueos de este gate.
Pendiente: decisión explícita de integración, comprobación de main en ese
momento, actualización canónica posterior al resultado real y decisión
separada sobre dogfood/activación. Este veredicto permite solicitar el merge
con Frame OFF; no lo ejecuta ni autoriza producción.

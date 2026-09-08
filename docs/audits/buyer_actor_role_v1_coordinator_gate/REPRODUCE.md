# Reproducir el gate Buyer–Actor V1

GATE_CODE_HEAD: `9f68ed90007153fcbdf4f5aaebf7eec7fbc3fd54`.
Rama: `integration/buyer-actor-role-v1-gate`.
Worktree original: `/Users/rodrigoortiz/Documents/secoppal-buyer-actor-role-v1-gate`.
BASE y MAIN_BEFORE: `52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386`.
Auditoría GO: `7c8cd9be7e071924763d887e8edf14c8804f5b18`.
Runtime auditado: `e06c60482756e2d4238052818117568f9cd4e946`.

Los resultados existentes no se sobrescriben. Para reproducir una corrida
completa, crear otro worktree desde GATE_CODE_HEAD, con espacio disponible,
y coordinar la exclusividad de pytest. No lanzar de nuevo la corrida global
por rutina ni para buscar un verde; hace falta una causa corregida autorizada.

Desde la raíz de esa copia:

```sh
PY=/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python
RUN=docs/audits/buyer_actor_role_v1_coordinator_gate/run_gate.py
$PY "$RUN" collect
$PY "$RUN" full
```

El adapter reutiliza run_gate, gate_support, gate_plugin y los tres directorios
active del gate Frame integrado. Sólo sustituye BASE/runtime de referencia,
destino, entorno y agrega aceptación/locales. Ruff incluye tanto el directorio
nuevo como todos los Python activos del gate anterior. Todos los componentes
contribuyen a execution_manifest.json y al exit final.

El proceso limpia credenciales del entorno, apaga Burr antes de importar,
arranca Frame OFF y Actor ON, y redirige feedback JSONL/SQLite, conversaciones,
índice ambiental, Hypothesis y temporales de pytest antes de colección.
gate_plugin observa el Frame después de fixtures; gate_checks registra los
lectores Actor/Frame. La aceptación independiente además observa los lectores
dentro de cada workflow y ejercita OFF/ON, con Actor positivo.

Sólo la suite histórica tests/ permite GET/HEAD a datos.gov.co y POST a la
ruta de consultas SODA3, bajo el lock serial original. Los demás destinos
fallan. gate_checks verifica el registro externo tras cada test y al terminar
la sesión, de modo que un error absorbido no dé un verde. Aceptación Buyer y
complementarios Frame agregan la guarda socket estricta independiente.
El corpus adicional es puro parsing/follow-up. Ningún comando inicia apps.

Los ocho opt-in usan exclusivamente BUYER_ACTOR_LOCAL_INDEX apuntando al
índice original immutable, con lookups exactos de Cartagena/Barranquilla y
controles de tamaño/mtime/inode. No habilitan un índice ambiental para la
suite ni exportan candidatos personales. SECOP siempre sintético allí.

Diagnóstico focal posterior, con etiqueta nueva:

```sh
BUYER_GATE_STRICT=1 $PY "$RUN" focal \
  tests/test_actor_conversation_corpus.py::test_all_expanded_sequences_mutate_real_query_frames \
  tests/test_actor_profile_render.py::test_a_specific_aggregate_question_still_gets_its_specific_answer \
  --run-id review_harness
```

Para repetir el control de retirada usar una copia temporal donde no exista
harness_removal_clone; el script check_harness_removal.py rechaza sobrescribir
ese directorio. La copia APFS recibe únicamente cambios en la fixture de tests,
se ejecuta primero verde, luego sin aggregate/with_timeout, registra ambos
intentos fuera del producto y restaura el helper antes de eliminarla. No
repetir campañas de mutación runtime ni historia ya aprobadas.

Cada JSON por componente conserva colección, resultados por fase, excepciones,
skips, xfails, deselects, flags y conexiones observadas. Los comandos literales
y sus salidas están en logs; los comprimidos se leen con `gzip -cd`. Los
manifiestos de almacenamiento registran SHA256 de los bytes originales, sin
limpiar whitespace de pytest/Semgrep. No se reescribe evidencia histórica.

La colección se compara automáticamente con collection_manifest.json del
Frame. Sólo se clasifican los módulos nuevos y los dos montajes afectados;
test_short_negation_characterization es caracterización de deuda, no una
garantía de exclusión de proveedor.

## Corrida entregada e interrupción

La corrida `full` entregada abortó por ENOSPC durante global. No tiene totales
finales ni JUnit global. `execution_manifest.json` agrega explícitamente ese
componente incompleto a los resultados de los componentes pendientes,
ejecutados después en serie mediante los mismos argv de BLOCKS y environment
del adapter. Cada continuación tiene `continuation_after_global_abort=true`,
temporales nuevos por componente y el mismo GATE_CODE_HEAD. No es otra corrida
de tests/. Los comandos exactos y su SHA están en ese manifiesto.

En esas continuaciones, el nombre `full_buyer_local` es necesario para que
environment habilite exclusivamente BUYER_ACTOR_LOCAL_INDEX. No sustituirlo
por una ejecución focal sin esa configuración, que omitiría los opt-in.
El resultado local fue 8 passed y 4 errores de teardown del autor, por 20
intentos agregados bloqueados. Los cuatro independientes pasaron.

El diagnóstico H4 del candidato se reproduce con el adapter focal y el node
ID de COORDINATOR_GATE.md. Para BASE, h4_base_direct/source.json registra argv
y los archivos runtime restituidos exclusivamente desde blobs de BASE en una
copia temporal APFS. Se usa environment del adapter y pytest directamente:
el snapshot Git del runner no admite un módulo nuevo ausente en BASE. El test
H4 y las configuraciones de pytest permanecen idénticos. No se ejecuta la suite
BASE ni se quitan xfails: BASE dio XFAIL; candidato dio XPASS(strict).

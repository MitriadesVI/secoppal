**Reproducción de la auditoría independiente B.1.** Candidato `af8f0a3407102dcb807089e87e470d754b835d61`, rama `audit/entity-profile-frame-v1-2-b1-independent`, worktree `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b1-independent`. El SHA del HEAD de auditoría está en la entrega final y se obtiene con `git rev-parse HEAD` en esta rama. No hace falta cambiar ningún archivo del candidato para reproducir.

Intérprete canónico: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`. Los scripts propios redirigen sus salidas exclusivamente a este directorio. `replay` invoca los comandos pytest exactos del REPRODUCE congelado de B; `acceptance` usa los paths del árbol Git congelado para evitar incorporar por accidente los nuevos tests propios al glob B.1. Ningún script de reproducción del autor se ejecuta escribiendo en su directorio de evidencia.

Todos los `.log.gz` son salidas literales comprimidas sin pérdida; `literal_log_manifest.json` contiene SHA256 y número de bytes descomprimidos. Por ejemplo:

```bash
gzip -dc docs/audits/entity_profile_frame_v1_2_b1_independent/own_final.log.gz
```

Al repetir los comandos se generan nuevos `.log` dentro de este directorio; los históricos del candidato no se modifican. No sumar esta nueva ejecución con las anteriores como garantías distintas.

**Preflight y preservación.** Python 3.14.7, Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10, Burr 0.40.2. Codegraph CLI 0.9.6 requiere el Node 22 instalado; el Node predeterminado 26 es rechazado. MCP ausente, `codegraph status` devuelve `Not initialized`; no se creó índice ni se asumió vigencia de otro worktree. Se usa `rg` más lectura manual; mapa actual en `consumers.txt`.

```bash
git rev-parse --show-toplevel
git rev-parse HEAD
PATH=/opt/homebrew/opt/node@22/bin:$PATH codegraph --version
PATH=/opt/homebrew/opt/node@22/bin:$PATH codegraph status
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/integrity.py
```

Integridad: ascendencia B→auditoría B→HEAD probado→candidato, mismo runtime/tests probado→final, 485 originales iguales a BASE, 35 archivos literales del autor verificados, 2.410 archivos del candidato iguales a sus blobs. `candidate_sha256.json` incluye todos los archivos app/tests/Streamlit y las autoridades protegidas. La auditoría sólo añade sus rutas autorizadas.

**Targets completos y partición, todos offline.** El runner añade `PYTHONPATH=scripts/audits/entity_profile_frame_v1_2_b_independent`, `PYTEST_ADDOPTS='-p offline_guard'` y flag Frame OFF como default de control; las fixtures pertinentes lo encienden. Conftest redirige feedback, DB, conversaciones e índice Actor a temporales. Workers usan Settings con persistencia temporal explícita. Los clientes SECOP/narración y efectos Radar son sintéticos; se mantienen parser, workflow, handlers y validadores reales.

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/reproduce.py replay
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/reproduce.py originals
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/reproduce.py acceptance
```

`originals` sale con código 1 por los cinco montajes adjudicados; ejecutar `acceptance` como comando siguiente, sin condicionar su ejecución con `&&`. `replay` conserva el exit de cada subprocess en su log; su propio exit no significa que todos los targets pasaron.

| Archivo literal | Resultado |
|---|---|
| `author_4.log.gz` | `2 passed in 0.14s` |
| `author_5.log.gz` | `2 failed, 1903 passed in 141.88s (0:02:21)` |
| `author_6.log.gz` | `472 passed, 2 xfailed in 14.54s` |
| `author_7.log.gz` | `276 passed in 1.09s` |
| `author_8.log.gz` | `15 passed in 3.66s` |
| `author_9.log.gz` | `408 passed in 1.98s` |
| `originals.log.gz` | `5 failed, 79 passed in 34.87s` |
| `acceptance.log.gz` | `106 passed, 9 deselected in 32.91s` |

Los siete rojos históricos se adjudican en RECONCILIATION. Los cuatro controles verdes apartados sí se ejecutaron en originals. Los cinco probes observacionales no acreditan corrección por pasar. Dos xfails ajenos siguen intactos; no hay marcas nuevas. Esta es reproducción dirigida de A2/R3d, contraejemplos, R1/R2/R4, escenarios B, focos y consumidores nombrados, no pytest global.

**Pruebas propias juntas.**

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/reproduce.py own_final \
  tests/test_v12b1_independent_controls.py \
  tests/test_v12b1_independent_records.py \
  tests/test_v12b1_independent_storage.py \
  tests/test_v12b1_independent_supplier_overflow.py \
  tests/test_v12b1_independent_mutation_oracles.py
```

```text
11 failed, 45 passed in 16.80s
EXIT_CODE=1
```

Los once rojos son aserciones de producto/API: tres F4, dos F5 contando una propiedad, dos F9, cuatro del overflow. No hay errores de colección/import/montaje en la corrida final. Los 45 verdes incluyen controles de publicaciones válidas, preparación y generaciones, permisos Radar, token/TTL/identidad Actor, referentes UI y daño individual rechazado. Hypothesis de multiplicidad es determinista, sin I/O externo; su caso mínimo conserva un job válido y duplica sólo una llamada observada.

Logs focales anteriores se conservan: `storage.log.gz`, `records.log.gz`, `supplier.log.gz`, `controls_final.log.gz`. El primer control Actor de identidad agregaba una clave `identity` que no consume el parser del candidato; la corrección propia altera el campo real `document_type`. Se conservan `controls.log.gz`/`controls_initial_observer.log.gz` como error del observador, no como hallazgo Actor. La corrida final acredita el rechazo con 13/13 controles verdes en ese archivo.

**Mutaciones, todas en exportaciones desechables.**

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/reproduce.py mutations
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/mutation_oracles.py
```

La campaña congelada se ejecutó antes del commit de auditoría, con HEAD=`af8f0a3`. El wrapper reutiliza el runner del autor con OUT propio; al repetir desde HEAD de auditoría éste exporta ese HEAD, cuyo runtime/tests del candidato se comprueban idénticos con integrity. Los targets de la campaña son explícitos; no incluyen nuevos tests propios por glob. Los oráculos adicionales M2/M3 exportan siempre el SHA congelado indicado en su script.

```text
{'DETECTED': 27} restored True
M2-old-cursor-after-corruption 0 0 True
M3-seed-bypasses-admission 0 0 True
```

Las dos columnas cero son exit de baseline y mutante de los oráculos de producto. Clasificación independiente: 25 detecciones conductuales, dos cambios sólo de etiqueta. `mutations.json` y `mutation_product_oracles.json` conservan cada baseline, target, salida completa y restauración. No se cuentan timeout/import/setup como detección. Las copias temporales se eliminan al finalizar y se verifica igualdad de hashes app antes/después; el runtime del worktree no se muta.

**Overflow histórico y Semgrep comparable.**

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/historical.py
```

La misma prueba de proveedores se copia a dos exportaciones, sin tocar ningún otro test ni app. Runtime B: `4 failed, 2 passed in 2.76s`. Base anterior a B: `4 failed, 2 passed in 2.58s`. Se conservan salidas y hashes en `supplier_runtime_b.log.gz`, `supplier_before_b.log.gz` y `historical_controls.json`. La función `format_currency` y entrada/stack alcanzados están en la salida de cada caso; no se ejecuta un barrido de analytics.

El script también escanea Semgrep en la exportación B con raíz Git local, para aplicar las mismas exclusiones que el worktree. La primera prueba sin Git dio once findings, incluido Settings en `app/config.py` por distinta raíz de paths; se conserva en `semgrep_base_no_git.*`. No se eliminó ese resultado para aparentar diez: se corrigió la comparabilidad del montaje. Baseline comparable: diez; nueve fragmentos idénticos y uno con handlers except idénticos por AST y cambio de contexto de paginación.

**Puertas literales y colección.**

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
make lint-core
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/gates.py
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_independent/collection.py
git diff --check 708ea3b3f609af4d9d3abe471f6963ee23513d7a..HEAD
git diff --check af8f0a3407102dcb807089e87e470d754b835d61..HEAD
git diff --cached --check
git status --short
git log -3 --oneline
```

```text
All checks passed!
Ran 3 rules on 323 files: 10 findings.
app/core/ limpio
```

Diff-check: salida vacía, exit 0. Semgrep es baseline global rojo/delta cero, no verde global. Tamaño se ejecutó en author_4 y en adyacentes; no se cambió ningún límite. `gates.py` conserva versiones, salidas y comparación detallada de regla/ruta/líneas/fragmentos. `collection.py` sólo recolecta los targets que constan en sus logs, sin ejecutar sus casos, y comprueba los nueve deselect/cuatro controles/cinco probes. Tras archivar, repetir primero los targets para regenerar los logs de entrada o descomprimirlos en este directorio propio.

La recolección inicial tenía `-q` duplicado y emitía conteos por archivo en lugar de node IDs; se corrigió exclusivamente el script de colección. Ese error y el de comparación Semgrep inicial no son fallos de producto ni muertes de mutantes.

**Corrí estos targets, no la suite.** Sin app, red SECOP/LLM, datos de usuario, radares reales, mensajes, merge, push o activación. La reconciliación activa y el gate global quedan para el coordinador después de remediar lo abierto. `archive_logs.py` conserva las salidas sin whitespace añadido a Git y verifica hashes al comprimir; actúa sólo en este directorio de auditoría.

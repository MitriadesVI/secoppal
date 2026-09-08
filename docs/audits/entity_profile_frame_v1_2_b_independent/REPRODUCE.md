# Reproducción independiente y salidas literales

Ejecutar desde `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b-independent`. Intérprete canónico: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`. Los scripts comprueban el candidato por hashes sin importar Settings. Los tests heredan el aislamiento autouse del repositorio; toda persistencia está en tmp_path. La guarda offline se carga como plugin pytest.

## 1. Candidato y preservación

```bash
git rev-parse --show-toplevel
git branch --show-current
git merge-base --is-ancestor 4c86d5bafaa3296b73c6523465b33efa4311910a 1b2a85f89e5d556f1de392e11b9da437da6540d1
git diff --name-status 4c86d5bafaa3296b73c6523465b33efa4311910a..1b2a85f89e5d556f1de392e11b9da437da6540d1
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b_independent/reconcile.py
```

```text
candidate files changed: [] semgrep identical: True
```

`candidate_hashes.json` es el manifiesto tomado antes de añadir tests; `integrity.json` compara todos sus archivos y los hashes históricos originales. `candidate.diff.gz` conserva el diff completo de B. El HEAD final de auditoría es el commit de esta entrega, no un candidato runtime nuevo.

## 2. Instrumentos

Salida de `preflight.log`:

```text
ruff 0.16.4
1.176.0
3.14.7 (main, Aug  5 2026, 10:29:49) [Clang 21.0.0 (clang-2100.1.1.101)]
Hypothesis 6.165.10
Burr 0.40.2
0.9.6
v22.23.2
```

Semgrep=1.176.0; Codegraph=0.9.6. El Node habitual 26.8.1 fue rechazado por el CLI. Se usó `PATH=/opt/homebrew/opt/node@22/bin:$PATH codegraph status` y `codegraph impact reserve_turn`. El estado es **Not initialized**; no hay índice local ni MCP Codegraph cargado. No se creó un índice fuera de los directorios de escritura autorizados. Se usó **rg y lectura manual**, con evidencia en `consumer_search.txt`. No se afirma cobertura de grafo.

## 3. Tres puertas obligatorias

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
```

Salida literal (`ruff_final.log`):

```text
All checks passed!
```

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
```

Extracto literal (`semgrep_final.log` conserva la salida completa y los diez fragmentos):

```text
  Scanning 332 files tracked by git with 3 Code rules:
  Scanning 315 files with 3 python rules.
 • Findings: 10 (10 blocking)
 • Rules run: 3
 • Targets scanned: 315
Ran 3 rules on 315 files: 10 findings.
```

No se declara Semgrep verde por exit 0. BASE y candidato tienen **los mismos diez hallazgos**, comparados por regla/archivo/línea final y fragmento exacto de Git; delta propio=0 (`semgrep_comparison.json`). La primera exportación de BASE sin metadata Git alteró el tratamiento de exclusiones y produjo un hallazgo extra en app/config.py. Se conserva `semgrep_base_without_git.log`, pero **no se usa para adjudicar delta**. Se repitió sobre exportación idéntica inicializada como repositorio temporal, con archivos añadidos al índice; la comparación resultó idéntica. No se cambió la regla ni el código para lograrlo.

```bash
PYTHONPATH=scripts/audits/entity_profile_frame_v1_2_b_independent \
PYTEST_ADDOPTS='-p offline_guard' \
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_v12b_independent*.py -q -s --tb=short
```

Salida final literal (`independent_final.log`, completa):

```text
53 failed, 31 passed in 21.71s
```

Son **53 contraejemplos parametrizados de diez causas**, no 53 defectos distintos. Se entregan rojos deliberados con expectativas del contrato, sin xfail, skip o corrección de runtime. Los 31 verdes incluyen cinco probes observacionales para comparar BASE/candidato; no se cuentan como cinco demostraciones autónomas de corrección. Los fallos finales son aserciones conductuales o rechazo esperado que no ocurrió; no setup, import error, timeout ni fallo de colección. La propiedad propia (seed sin referente) queda roja y determinista. Las propiedades existentes se ejecutaron en los conjuntos originales descritos abajo.

**Corrí los tests dirigidos listados, no la suite.**

## 4. Reproducción del cierre del autor

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b_independent/replay.py
```

Este script extrae los comandos pytest exactos de `docs/audits/entity_profile_frame_v1_2_b/REPRODUCE.md`, expande sus globs, mantiene el orden de puertas y agrega la guarda offline. Fija B OFF por entorno salvo fixture explícita del test. No extrae ni ejecuta la campaña de mutación sobre el worktree.

Salidas literales independientes, sin sumar conjuntos superpuestos:

| Log | Conjunto | Salida |
|---|---|---|
| author_4.log | guardas tamaño | `2 passed in 0.13s` |
| author_5.log | target R3d ampliado + Entity Profile/B y adyacentes nombrados | `1905 passed in 134.55s (0:02:14)` |
| author_6.log | consumidores adyacentes nombrados | `472 passed, 2 xfailed in 13.81s` |
| author_7.log | corpus semántico | `276 passed in 0.96s` |
| author_8.log | utilidad obligatoria | `15 passed in 3.55s` |
| author_9.log | núcleo/ports/A2/mutaciones API/propiedades/corpus | `408 passed in 1.86s` |

Los trece casos `tests/test_v12a1_independent_counterexamples.py` están incluidos sin deselect, al igual que R3d y la regresión de rollback. Las propiedades `test_entity_profile_frame_properties.py` y `test_entity_profile_frame_b_properties.py` están dentro de estos comandos y se preservaron intactas.

Se repitieron las pruebas propias del autor con instrumentación de flag en el hook de llamada:

```bash
AUDIT_FLAG_LOG=docs/audits/entity_profile_frame_v1_2_b_independent/author_b_flags.json \
PYTHONPATH=scripts/audits/entity_profile_frame_v1_2_b_independent \
PYTEST_ADDOPTS='-p offline_guard' SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED=false \
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_entity_profile_frame_b_*.py -q --tb=short
```

```text
196 passed in 16.87s
```

Inventario exacto: 60 ON/136 OFF. Es estado del flag en el test, no conteo de workflows: muchos son puros.

## 5. BASE, xfail y mutaciones desechables

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b_independent/base_and_mutations.py
```

El script exporta BASE/candidato en TemporaryDirectory. Compara:

```bash
python -m pytest tests/test_actor_hardening_v2_1_remediation.py::test_actor_region_and_buyer_together_still_resolve -q -rx
python -m pytest tests/test_actor_hardening_v2_1_remediation.py::test_actor_region_and_buyer_together_still_resolve -q --runxfail --tb=short
```

BASE: `2 xfailed in 1.51s`; sin marca: `2 failed in 1.17s`. Candidato: `2 xfailed in 1.29s`; sin marca vuelve a fallar la misma aserción `CANDIDATES != RESOLVED`. Los archivos completos son idénticos por SHA256 (`xfail_comparison.json`).

Campaña del autor repetida **sólo en la exportación del candidato**:

```text
baseline verde: 13 passed in 4.35s
{"detected": 12, "survived": 0, "inconclusive": 0, "anchor_not_found": 0, "total": 12}
```

`mutations_replayed.log` conserva cada salida; `mutation_replay.json` los targets/digests/veredictos. `mutation_runtime_restoration.json` compara todos los app/*.py antes/después: identical=true. Las alteraciones de sobres/sidecars de los nuevos tests son fixtures temporales, no mutaciones del runtime entregado. No se presenta la prueba aislada de flock como prueba de serialización de todos los handlers.

`tests/test_v12b_independent_base_comparison.py` también se ejecutó byte a byte sobre una exportación de BASE y sobre candidato: `base_product_sequences.log` / `candidate_product_sequences.log`. Cinco secuencias por árbol. Son probes observacionales con rutas, llamadas, respuesta y SAVE simulado; prueban la procedencia detallada en F1/F2/F3/F10. Para repetir BASE, exportar `4c86d5b` en directorio temporal y copiar allí exclusivamente este test nuevo; ejecutar el mismo comando con el plugin offline cuyo PYTHONPATH apunta a la auditoría.

## 6. Otras puertas y alcance final

```bash
make lint-core
```

```text
app/core/ limpio
```

Guardas de tamaño: salida literal ya indicada en author_4.log. No se subió ningún límite.

```bash
git diff --check 4c86d5bafaa3296b73c6523465b33efa4311910a..HEAD
git diff --cached --check
```

Sin salida, exit 0. Se repite el intervalo después del commit final. Los archivos tracked/staged del worktree de auditoría quedan limpios al entregar. Los untracked preexistentes de main no fueron modificados ni eliminados.

Logs preliminares `independent_first.log`, `boundaries_first.log` y `expanded_first.log` se conservan por trazabilidad: hubo ajustes del observador (campo actor_scope, token opaco emitido, y focos como follow-up de un perfil). Sus errores de setup **no cuentan** como hallazgos. La evidencia adjudicada es la corrida final, con controles reales y sin esos errores.

No se ejecutó pytest global, app, red SECOP/LLM, envío de mensajes, radar real, merge, push o activación. No se añadió ninguna decisión de producto ni remediación. El gate global del coordinador queda pendiente después de resolver los incumplimientos.

## Conservación de salida cruda

`literal_outputs.json.gz` conserva byte por byte el texto capturado de todos los logs antes de normalizar únicamente espacios finales y líneas vacías finales para la guarda de whitespace de Git. Los `.log` son vistas legibles de ese mismo contenido. `candidate.diff.gz` conserva el diff completo crudo, incluidos los espacios de las líneas de contexto de un patch. Se pueden leer con gzip sin regenerar evidencia. Esta normalización no cambia aserciones, resultados ni datos; la copia cruda permite verificarlo.

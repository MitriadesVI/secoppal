# B.1 — Reproducción y salidas

Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b1-remediation`.
Intérprete: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.
HEAD probado: `64f9836edf96a89051ffc4eb21b0b3397091228c`.
El HEAD final es el commit de evidencia entregado; su delta respecto al probado
contiene exclusivamente documentación, logs y herramienta de verificación.
No se atribuye un SHA autorreferente al propio commit documental.

Todos los logs se archivan con gzip **sin alterar sus bytes** para conservar
salidas literales sin introducir whitespace en Git. `literal_log_manifest.json`
registra sha256 de los bytes descomprimidos; `gzip -dc <archivo.log.gz>` los muestra.
Los scripts generan nuevamente los `.log` en este directorio al reproducir.
La evidencia de intentos previos está conservada y no sustituye las corridas finales.

## Entorno y seguridad de pruebas

Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10; Codegraph CLI 0.9.6 con Node 22.
`codegraph status`: **Not initialized**. MCP Codegraph no cargado. Fallback
explícito a `rg` y lectura manual, con [consumidores](consumers.txt). No se
instaló infraestructura ni dependencias. El Node predeterminado 26 fue rechazado
por Codegraph; se usó el Node 22 que ya existía.

La guarda `offline_guard` rechaza sockets IP. Los tests heredan aislamiento
conftest; los procesos de concurrencia construyen Settings con feedback temporal
explícito y usan exclusivamente clientes sintéticos. No se arranca la aplicación.

## Tres puertas y complementos

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_remediation/acceptance.py
make lint-core
```

Salidas literales relevantes (archivos completos archivados):

```text
All checks passed!
Ran 3 rules on 323 files: 10 findings.
app/core/ limpio
```

Semgrep **no está verde globalmente**: diez findings heredados, cero nuevos.
`semgrep_comparison.json` conserva regla, ruta y fragmentos antes/después.
Nueve fragmentos son idénticos; Radar se mueve de 109 a 110. En paginación,
177→179, la rama F6 modifica el cuerpo del mismo try; el AST de los handlers
except permanece idéntico. No se equipara igualdad de conteo con identidad,
ni se añaden supresiones o se limpia el baseline fuera del encargo.

`replay.py` extrae y ejecuta los comandos pytest exactos de REPRODUCE de B,
expande globs y añade la guarda offline. Esto conserva A2/R3d, los trece
contraejemplos, R1/R2/R4, los ocho escenarios con recarga, utilidad y consumidores.

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_remediation/replay.py
```

Los números no se suman entre conjuntos superpuestos. `RESULTS.json` conserva
las últimas líneas literales de las corridas definitivas y su HEAD probado.

| Target | Resultado; log completo |
|---|---|
| Autor principal, sin excluir ninguna expectativa original | 1903 passed, 2 failed; `author_5.log.gz`. Dos expectativas incompatibles con F5, explicadas en PORTS |
| Consumidores adyacentes documentados | 472 passed, 2 xfailed; `author_6.log.gz`. Xfails preexistentes intactos |
| Corpus | 276 passed; `author_7.log.gz` |
| Utilidad | 15 passed; `author_8.log.gz` |
| Núcleo/ports/A2/propiedades/corpus | 408 passed; `author_9.log.gz` |
| Guardas de tamaño | 2 passed; `author_4.log.gz` |
| Auditoría completa intacta | 79 passed, 5 failed; `independent_final.log.gz`. Sólo tres montajes autorizados, ver PORTS |
| Auditoría reutilizable + nuevos controles/ports | 106 passed, 9 deselected; `acceptance_final.log.gz`. Los nueve se enumeran por nombre y también se ejecutaron íntegros |

**Corrí esos targets, no la suite.** No se ejecutó pytest global. Ningún skip o
xfail nuevo para defectos; los deselect son exclusivamente la partición explícita
de montajes autorizados. Los cinco probes observacionales incluidos en auditoría
no se presentan como cinco garantías de producto.

Corrida íntegra de originales:

```bash
PYTHONPATH=scripts/audits/entity_profile_frame_v1_2_b_independent \
PYTEST_ADDOPTS='-p offline_guard' \
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_v12b_independent*.py -q --tb=short
```

## Campaña de mutaciones

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_remediation/mutations.py
```

La campaña definitiva usa exportación del mismo HEAD probado; **27 DETECTED**,
**runtime_restored=true**. Cada mutación tiene baseline positivo previo, salidas
completas y restauración. M7 conserva su semántica al mover el punto de inyección
a `turn_coordination.py`. M9 ejecuta el original y una aserción adicional de
respuesta honesta. `mutations_first.json`, `mutations_second.json` y la repetición
M9 conservan los intentos de instrumentación y por qué se repitieron.
No se cuentan errores de importación, setup, deadlock o timeout como kills.

## Preservación, referencias y diff

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/entity_profile_frame_v1_2_b1_remediation/verify_evidence.py
git diff --check 708ea3b3f609af4d9d3abe471f6963ee23513d7a..HEAD
git diff --name-status 64f9836edf96a89051ffc4eb21b0b3397091228c..HEAD
git status --short
git log -6 --oneline
```

El verificador coteja los 485 originales congelados y la identidad de findings,
conserva la campaña final y comprime sin pérdida los logs que aún no lo estén.
El diff-check debe tener salida vacía; el delta probado→final no cambia app,
Streamlit o tests. Las referencias congeladas, CARRILES, ADR, bitácora, pins,
flags por defecto y los worktrees ajenos permanecen intactos.

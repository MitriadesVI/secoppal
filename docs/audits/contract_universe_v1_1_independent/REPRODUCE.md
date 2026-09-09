# Reproducción acotada

Worktree: `/Users/rodrigoortiz/Documents/secoppal-contract-universe-v1-1-independent`.
Rama: `audit/contract-universe-v1-1-independent`.
SHA auditado (código y pruebas): `8fc5643651d6a603d3bc3849369d9773ab55ea11`.
HEAD final de la remediación: `2e564d06a4f18a8ef4d3b5a1fa462000e21a00da`.
Candidato V1: `067b39770592640b5aef3898cbc99d8f82c59e0f`.
Primera auditoría: `2fb9c6e2974803a8ae46b3fcc27257e200e4609d`.
BASE main: `c52d7eda355f44278164d4cb9803a199eec83486`.

Intérprete canónico: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.
Todo pytest usa `-p tests.contract_universe_offline_guard` (ledger de intentos
de red, incluso absorbidos) y `-p tests.contract_universe_sql_dates` (emulación
del dialecto temporal para las fixtures congeladas).

## Los 43 congelados, intactos

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_contract_universe_independent.py tests/test_contract_universe_independent_consumers.py tests/test_contract_universe_independent_legacy.py tests/test_contract_universe_independent_safety.py tests/test_module_size_limits.py -q
```

→ `10 failed, 33 passed`.

## Prueba de causalidad de los diez rojos

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates -p tests.cu_v1_policy_shim tests/test_contract_universe_independent.py tests/test_contract_universe_independent_consumers.py tests/test_contract_universe_independent_legacy.py tests/test_contract_universe_independent_safety.py tests/test_module_size_limits.py -q
```

→ `43 passed`. El shim restaura **sólo** la regla V1 de Aprobado.

## Controles propios de esta revisión

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_cu_v11_audit_ports.py tests/test_cu_v11_audit_f1_f5.py tests/test_cu_v11_audit_aprobado.py tests/test_cu_v11_audit_persistence.py tests/test_cu_v11_audit_regressions.py tests/test_cu_v11_audit_properties.py -q
```

→ `390 passed`.

## Incumplimiento reproducido (queda ROJO)

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest -p tests.contract_universe_offline_guard -p tests.contract_universe_sql_dates tests/test_cu_v11_audit_findings.py -q
```

→ `8 failed`. Sin xfail, skip ni deselect en ningún punto de esta revisión.

## Puertas

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/run_query_corpus.py --mode all
make lint-core
git diff --check
```

Comandos, códigos de salida y colas de stdout/stderr en
[validation.json](validation.json).

## Artefacto V1 real

`v1_independent_artifacts.json` se generó ejecutando los serializadores del
candidato V1 en un worktree desechable de `067b3977` (conversación, buffer,
Frame, plan, registro de ejecución y radar), no quitando claves a un dict V1.1.
El generador asertó `POLICY_VERSION == 'contract-universe-1'` y
`CATALOG_VERSION == 'entity-profile-frame-2-contract-universe-1'` antes de
serializar. `tests/test_cu_v11_audit_persistence.py` lo reproduce contra V1.1.

## Mutaciones

`mutations.json` lo produce un arnés propio sobre un worktree desechable de
`2e564d0`: aplica cada mutación como reemplazo literal, ejecuta como killer un
test de **esta** auditoría y restaura el archivo. Un kill sólo cuenta con
fallos de aserción (`failed`) y cero `error` de colección o importación. El
arnés verifica al final que el worktree queda limpio.

Dos mutaciones necesitaron redirigirse: la de match léxico global se apuntó
primero a una guarda que no era la efectiva, y la de sustitución global llevaba
un regex mal escapado. Ambas quedan registradas aquí; el resultado que se
reporta es el de la versión que sí reproduce el defecto.

## Red

Cuatro lecturas públicas anónimas al portal de datos abiertos, autorizadas por
el §8 del encargo para acreditar el dialecto, documentadas en
[socrata_dialect_reads.json](socrata_dialect_reads.json). Ninguna otra salida
de red: el ledger offline estuvo activo en todos los pytest y no registró
intentos.

## Límites

No se ejecutó la suite global. No hubo LLM, app, despliegue, activación de
flags, merge, push ni escritura en almacenes del usuario; los datos de prueba
viven en temporales de pytest. No se modificó ningún otro worktree: los dos
worktrees desechables (`v1-probe`, `mut`) se crearon en el scratchpad de la
sesión y se eliminaron al terminar. No se editó el runtime.

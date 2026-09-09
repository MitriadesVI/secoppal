# Reproducción acotada

Worktree: `/Users/rodrigoortiz/Documents/secoppal-contract-universe-v1-1-f2-1`.
Rama: `fix/contract-universe-v1-1-f2-1`.
Base de remediación: `2e564d06a4f18a8ef4d3b5a1fa462000e21a00da`.
SHA probado anterior: `8fc5643651d6a603d3bc3849369d9773ab55ea11`.
Segunda auditoría: `298c79271e1a57ff57c1a841c05dd35d2a0a3b10`.

Intérprete canónico: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.
Todo pytest usa `-p tests.contract_universe_offline_guard` y
`-p tests.contract_universe_sql_dates`, como el lote anterior.

## Todas las puertas

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/contract_universe_f2_1_validate.py
```

Escribe [validation.json](validation.json) con cada comando, su exit code y su
salida. Incluye ruff, semgrep, simetría, completitud, los tests del auditor, los 85 de
remediación, los 1.217 targets del autor, los 43 congelados literales, el
control de causalidad con el shim V1 de Aprobado, corpus, lint-core y
diff-check. **No** ejecuta la suite global.

## Matriz antes/después

```bash
git worktree add --detach /tmp/f21before 2e564d06a4f18a8ef4d3b5a1fa462000e21a00da
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/contract_universe_f2_1_matrix.py /tmp/f21before
git worktree remove --force /tmp/f21before
```

Sondea el adaptador real en ambos árboles sobre las mismas 23 consultas —los
siete pares en los dos órdenes, los controles positivos y los de F1— y escribe
[matrix.json](matrix.json).

## Mutaciones

El arnés vive en el scratchpad de la sesión porque opera sobre una copia
desechable: crea un worktree de la base, le aplica encima el fix, y luego muta,
ejecuta el killer y restaura. Comprueba al final que cada archivo mutado volvió
byte por byte a su contenido post-fix —comparar contra git no serviría, porque
el fix mismo aparece como diferencia frente a la base—. Los killers son `test_contract_universe_f2_1_symmetry.py`,
`test_contract_universe_f2_1_completeness.py`, `test_cu_v11_audit_findings.py` y
`test_cu_v11_audit_f1_f5.py`; sólo cuenta un kill con fallos de aserción y cero
errores de colección. El arnés vive en
[mutation_harness.py](mutation_harness.py) y sus resultados en
[mutations.json](mutations.json).

Una corrida intermedia salió con baseline rojo y restauración fallida porque dos
arneses compartieron el mismo worktree desechable. Se descartó y se repitió sobre
un árbol nuevo, con el baseline verde comprobado **antes** de mutar; los
resultados que se reportan son los de esa segunda corrida.

## Preservación de los tests del auditor

Los siete archivos del auditor y su plugin se trajeron con
`git checkout 298c792 -- …` y se verificó que su SHA-256 coincide con el de la
auditoría. No se editó ninguno; tampoco los tests independientes congelados.
Ninguna prueba usa xfail, skip ni deselect.

## Aislamiento

Sin red, sin LLM, sin app, sin despliegue, sin flags, sin merge ni push. El
ledger offline estuvo activo en cada pytest y no registró intentos. Los datos de
prueba viven en temporales de pytest. Los dos worktrees desechables se
eliminaron al terminar; ningún otro worktree fue modificado.

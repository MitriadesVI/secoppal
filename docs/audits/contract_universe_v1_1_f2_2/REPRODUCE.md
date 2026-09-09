# Reproducir F2.2 sin suite global

SHA de runtime/tests: `4a22efc330101c3511c034a53cd02d4790bb192f`.
CWD: este worktree. Python: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.

Primero los tests nuevos e intactos del auditor:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  -p tests.contract_universe_offline_guard \
  -p tests.contract_universe_sql_dates \
  tests/test_cu_f21_final_delta.py -q --tb=short
```

Resultado anterior al fix: `4 failed, 15 passed` (`auditor_red.txt`).
Resultado del candidato: `19 passed`. No se modificó ese archivo: su SHA-256
coincide con el blob de `8dfd574`, registrado en `test_preservation.json`.
El directorio hermano `contract_universe_v1_1_f2_1_final/.gitkeep` sólo mantiene
el destino fijo de trazas del test intacto.

Pruebas de producto y propiedades F2.2:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  -p tests.contract_universe_offline_guard \
  -p tests.contract_universe_sql_dates \
  tests/test_contract_universe_f2_2_materialization.py -q --tb=short
```

`validation.json` registra todos los comandos de la corrida final, sus códigos
de salida y stdout/stderr íntegros: symmetry, completeness, auditorías previas,
remediation, los 1217 targets del autor, frozen literal, shim, corpus y puertas.
Son comandos focales; ninguno invoca la suite global. Los `.txt` sólo eliminan
espacios de fin de línea para permitir `git diff --check`; el JSON conserva la
salida literal. `validation_before_temporal_preservation.json` es evidencia de
una iteración anterior, no valida el candidato final.

Para reproducir todos los comandos registrados (la excepción esperada es
`frozen_43_literal`, con código 1 y exactamente diez fallos adjudicados):

```python
import json
import subprocess
from pathlib import Path

record = json.loads(Path('docs/audits/contract_universe_v1_1_f2_2/validation.json').read_text())
for name, gate in record.items():
    completed = subprocess.run(gate['command'])
    assert completed.returncode == gate['exit_code'], name
```

## Mutaciones

Crear un worktree DESECHABLE de `4a22efc`; no apuntar el harness a otro carril:

```bash
git worktree add --detach /tmp/secoppal-f22-reproduce 4a22efc
git worktree add --detach /tmp/secoppal-f22-reference 4a22efc
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python \
  docs/audits/contract_universe_v1_1_f2_2/mutation_harness.py \
  /tmp/secoppal-f22-reproduce \
  /tmp/secoppal-f22-reference
```

Los logs se escriben en el snapshot de referencia desechable, preservando esta
evidencia histórica. El harness exige baseline verde antes y después, restaura cada archivo y compara
sus bytes con el candidato. M1–M6 conservan los cuatro archivos killer originales
(350 tests); M7/M8 usan los 19 del auditor más los 34 de F2.2. El baseline reúne
los seis archivos: 403 tests. No hay xfail, skip ni deselect.
`mutation_logs/run-1.txt` es el baseline inicial; runs 2–9 corresponden a M1,
M2, M3, M6, M4, M5, M7 y M8; run 10 es el baseline restaurado.
La carpeta `mutation_interrupted` documenta una corrida obsoleta detenida durante
la implementación; no cuenta para los kills ni el veredicto final.

Los tests ejercitan workflow y SQL sobre fixtures SQLite. El guard offline
rechaza incluso intentos de red silenciados por otras capas. Hypothesis usa
semillas deterministas y directorios/monkeypatch nuevos por ejemplo.

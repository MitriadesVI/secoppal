# Reproducción acotada

Worktree: `/Users/rodrigoortiz/Documents/secoppal-contract-universe-v1-1-remediation`.
Rama: `fix/contract-universe-v1-1-remediation`.
SHA de código y pruebas: `8fc5643651d6a603d3bc3849369d9773ab55ea11`.
Base del candidato: `067b39770592640b5aef3898cbc99d8f82c59e0f`.
Auditoría: `2fb9c6e2974803a8ae46b3fcc27257e200e4609d`.
BASE main: `c52d7eda355f44278164d4cb9803a199eec83486`.

Desde el worktree, usando el entorno canónico sin cambiar cwd:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/contract_universe_remediation_validate.py
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python scripts/audits/contract_universe_remediation_mutations.py
```

`validation.json` guarda cada comando expandido, exit_code y stdout/stderr
literal. El driver ejecuta Ruff, Semgrep, remediación, los 43 independientes
(incluye dos controles de tamaño), 1.217 targets del autor y relacionados,
corpus, lint-core y diff-check. No ejecuta la suite global. No hay xfail/deselect.
El driver sigue tras el lote independiente para recoger todas las puertas;
su proceso no sustituye los códigos de cada entrada de validation.json.

Todos los pytest usan `-p tests.contract_universe_offline_guard` y
`-p tests.contract_universe_sql_dates`. El primero conserva el ledger de
intentos de red, incluso absorbidos. El segundo sólo extiende SQLite con
funciones temporales SoQL: usa datetime.fromisoformat, no eligible ni una
allowlist de estados; no modifica las filas ni las expectativas congeladas.
Los helpers y tests independientes se conservan byte por byte; sus hashes y
procedencia están en independent_preservation.json. Las afirmaciones del
transporte SQL quedan limitadas a esa emulación, sin consultas SECOP live.

`independent_red.txt` conserva la reproducción inicial de los 12 fallos
funcionales (29 verdes sin los dos controles de tamaño). El resultado final
completo tiene 10 rojos adjudicados exclusivamente por Aprobado; REMEDIATION.md
explica sus ports, sin sustituir el test original ni ocultarlo.

`mutations.json` contiene baseline verde, seis mutaciones, fallos funcionales,
ports propios sobre el candidato congelado y generación de artefactos V1.
Las copias desechables se eliminan al terminar. El generador V1 es una copia
propia temporal del generador histórico: cambia base/schema esperados y estampa
params con la autoridad real V1; no modifica el generador independiente.
Los import errors, errores de transporte y timeouts no cuentan como kills.
`prior_mutations.json` conserva la repetición de las cuatro mutaciones previas.

Para repetir estas últimas sin sobrescribir la evidencia histórica:

```python
import importlib.util
from pathlib import Path
p = Path('scripts/audits/contract_universe_mutations.py')
s = importlib.util.spec_from_file_location('prior_mutations', p)
m = importlib.util.module_from_spec(s)
s.loader.exec_module(m)
m.OUT = Path('docs/audits/contract_universe_v1_1_remediation/prior_mutations.json').resolve()
m.main()
```

Las primeras iteraciones se conservan como diagnóstico: first/second/third_fixes,
author_first/author_second y mutation_run no representan el gate final.
El primer recolector de mutaciones no reconocía el formato AssertionError;
mutation_run_final y mutations.json corrigen el recolector y repiten los casos.

La entrega final agrega únicamente evidencia/documentación al SHA probado.
El HEAD final se comunica en la respuesta de entrega y se obtiene con
`git rev-parse HEAD` en este worktree. La identidad de runtime/tests/scripts
respecto del SHA probado se comprueba con `git diff SHA HEAD -- app tests scripts`.
No hay merge, push, despliegue, activación, reinicio ni escritura de Settings
contra persistencia real. Los datos de prueba viven en temporales de pytest.

Los .txt son vistas sin espacios finales para satisfacer git diff --check.
`literal_text_logs.json` conserva íntegramente cada captura original, con su
SHA-256, incluidos espacios finales, CR y escapes ANSI. validation.json mantiene
stdout/stderr finales literales. La normalización es sólo del empaquetado de
logs, nunca del contenido fuente que el producto presenta.

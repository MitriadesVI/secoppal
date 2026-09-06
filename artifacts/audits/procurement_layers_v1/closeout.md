# Entrega — Procurement Layers V1

Se implementó un modelo de instancias sobre la base `d608336`, reutilizando sus
autoridades y recibos. API offline, exportación JSON y navegación exacta de 0–3
saltos. La interfaz de la app se deja para una entrega posterior, conforme a la
preferencia confirmada por el usuario.

Baranoa: 1.037 nodos, 1.376 relaciones observadas; 248 asociaciones proceso→contrato
y 102 participaciones. El resumen de las cinco cohortes está en `cohort_summary.json`.
Son conteos de capturas acotadas con cobertura parcial, no del universo actual.

## Puertas y alcance

Ruff, comando `.venv/bin/ruff check app/ tests/ scripts/`:

```text
All checks passed!
```

Semgrep, comando `.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off`:

```text
Ran 3 rules on 241 files: 10 findings.
```

Baseline rojo: los mismos diez hallazgos conocidos. No se modificó ningún archivo
de `app/` respecto de `d608336`. `semgrep.log` conserva la salida completa.
Análisis adicional explícito de los scripts nuevos, `semgrep_owned.log`:

```text
Ran 3 rules on 9 files: 0 findings.
```

Pytest, comando:

```bash
.venv/bin/python -m pytest tests/test_procurement_layers_model.py tests/test_procurement_layers_sources.py tests/test_procurement_relationships.py tests/test_module_size_limits.py -q
```

```text
77 passed in 1.84s
```

Corrí estos cuatro archivos focales, no la suite. Hypothesis usa semillas
deterministas (`derandomize=True`) para separación de identidades, conservación
de códigos y documentos, idempotencia y permutación de evidencia. Los tests de
integración bloquean el transporte HTTP y comprueban las cinco cohortes.

Preflight: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10, Codegraph 0.9.6.
Codegraph se usó por CLI: query/impact de `result_identity` en la base; índice
propio inicializado y sincronizado, status actualizado y query de los adaptadores.
`affected` no encontró tests incluso con índice actualizado. Se verificaron
los imports encontrados por query y se seleccionaron manualmente los focales.
Esto no constituye validación de todas las dependencias productivas del repo.

Replay independiente de V0, sin reescribir evidencia:

```text
{"byte_exact": true, "graph_database": "NONE", "runtime_effect": "ZERO", "verdict": "PASS"}
```

## Fuera de esta entrega

Se preservaron los diez hallazgos Semgrep de `app/`, todos ajenos a los archivos
nuevos. El adaptador declara falta de ID lógico para Atlántico, sin inventar una
institución. No se cambiaron los registros de identidad, aliases, bindings,
territorio ni el contrato V0. No se tocó el trabajo de otros worktrees.

Cuenta→Actor y PAA→proceso siguen diferidos porque falta su autoridad de fuente.
El adaptador de Actor acepta lecturas IdentityMatch existentes con evidencia;
no consulta datos nuevos ni acredita personalidad jurídica. Las capturas usadas
no prueban tipo jurídico ni vigencia de membresía de los grupos.

No se hicieron llamadas SECOP, cambios de runtime, arranque de app, merge ni push.
La integración productiva queda pendiente; esta entrega permite ejecutar y
revisar el modelo con evidencia real congelada.

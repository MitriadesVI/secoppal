# Cómo reproducir Entity Profile Frame V1.2-A2

Todos los comandos son offline. No arrancan la aplicación, no construyen
`Settings()` y no llaman a SECOP ni a un LLM.

## Entorno y referencias

```bash
cd /Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-a2-remediation
git rev-parse HEAD
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python --version
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff --version
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --version
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -c \
  'import hypothesis; print(hypothesis.__version__)'
PATH=/opt/homebrew/opt/node@22/bin:$PATH codegraph --version
PATH=/opt/homebrew/opt/node@22/bin:$PATH codegraph status
```

Resultados observados: Python 3.14.7, Ruff 0.16.4, Semgrep 1.176.0,
Hypothesis 6.165.10 y Codegraph 0.9.6 con Node 22.23.2. El Node default 26.8.1
fue rechazado por la guarda de versión de Codegraph; se usó el Node 22 ya
instalado, sin instalar ni cambiar configuración global.

La referencia A1 estaba tracked/untracked limpia y exactamente en
`9164df0606b0ba9424d627aa37b12e742a7cd707`. La rama A2 se creó desde ese SHA;
`9da35b527e88b6d8612e94a5c5828f854d53a138` sí es ancestro. Los commits de
diseño `02d65ddfdc221f74a8fac75352931dcf6673a171` y auditoría
`6a6ec5c6c65019a1fae0a56a97df178851bb1d99` se leyeron como referencias, no se
incorporaron a la ascendencia.

## Reproducción sobre A1 intacto

```bash
cd /Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-a1-remediation
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-a1-remediation \
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  /Users/rodrigoortiz/Downloads/test_v12a1_independent_counterexamples.py -q
```

Salida literal resumida:

```text
FFFFFF.FFFFFF                                                            [100%]
12 failed, 1 passed in 0.33s
```

Esto ejecuta los módulos reales del worktree A1. Antes de reparar también se
comprobaron `sys.executable` y los `__file__` de `immutability.py`,
`source_occurrence.py`, `query_router.py` y `frame_migration.py`.

## Contraejemplo exacto y propiedades A2

```bash
cd /Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-a2-remediation
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_v12a1_independent_counterexamples.py -q
```

```text
.............                                                            [100%]
13 passed in 0.21s
```

El SHA-256 de la copia en `tests/` y del adjunto es idéntico:
`6d5c5f59014d301fc6db153a8b78513bd7e72e0f6f1e1a3117222ee9250b18f2`.

## Target agregado, no suite global

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_entity_profile*.py \
  tests/test_query_router_source_occurrence.py \
  tests/test_v12a1_independent_counterexamples.py \
  tests/test_query_router.py \
  tests/test_actor_routing.py tests/test_actor_conversation.py \
  tests/test_actor_geography_precedence.py tests/test_actor_identity.py \
  tests/test_money_identifier_arbitration.py tests/test_money_polarity_e2e.py \
  tests/test_money_polarity_v2.py tests/test_money_range_atomicity.py \
  tests/test_money_v2_invariants.py tests/test_modality_parser_integration.py \
  tests/test_modality_frame_adapter.py tests/test_modality_followup_pipeline.py \
  tests/test_negation_binding.py tests/test_negation_lifecycle.py \
  tests/test_phrase_catalog_collisions.py tests/test_phrase_spec.py \
  -q -k 'not blocked_turn_rolls_back_orthogonal_deltas'
```

```text
1692 passed, 1 deselected in 102.25s (0:01:42)
```

Corrí ese target, no la suite. La exclusión se reproduce igual en ambos HEAD:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_modality_followup_pipeline.py::test_blocked_turn_rolls_back_orthogonal_deltas -q
```

```text
assert ['alimentacion', 'procesos'] == ['alimentacion']
1 failed
```

## Puertas obligatorias

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep \
  --config .semgrep/secoppal-laws.yml app/ --metrics=off
make lint-core
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_module_size_limits.py -q
git diff --check 9164df0606b0ba9424d627aa37b12e742a7cd707..HEAD
```

Salidas literales relevantes:

```text
All checks passed!

✅ Scan completed successfully.
 • Findings: 10 (10 blocking)
 • Rules run: 3
 • Targets scanned: 298
Ran 3 rules on 298 files: 10 findings.

app/core/ limpio
..                                                                       [100%]
2 passed in 0.10s
```

`git diff --check` no produjo salida. Semgrep sigue rojo por su baseline; la
comparación JSON de A1/A2 contiene exactamente la misma regla, archivo y línea
en los diez resultados, por lo que el delta propio es cero.

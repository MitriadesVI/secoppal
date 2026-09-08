# Reproduccion y comandos R3d

Todos los comandos se ejecutaron en:

```text
/Users/rodrigoortiz/Documents/secoppal-entity-profile-r3d-postposed-source
```

No se levanto la app, no se uso red y no se construyo `Settings()` en snippets.

## Preflight

```text
/Users/rodrigoortiz/Documents/secoppal-entity-profile-r3d-postposed-source
a952805de050ec28d571b7a637e9240f88c71a45
fix/entity-profile-r3d-postposed-source
## fix/entity-profile-r3d-postposed-source
Python 3.14.7
ruff 0.16.4
1.176.0
Hypothesis 6.165.10
v26.8.1
v22.23.2
0.9.6
```

Codegraph inicialmente informo `Not initialized`; se inicializo localmente con
Node 22 y sin producir archivos Git visibles. El indice final informa 895
archivos, 21.162 nodos, 51.248 aristas e `Index is up to date`.

## Reproduccion roja en BASE_A2

Comando:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_modality_followup_pipeline.py::test_blocked_turn_rolls_back_orthogonal_deltas -q
```

Salida literal relevante:

```text
F                                                                        [100%]
=================================== FAILURES ===================================
_____________ test_blocked_turn_rolls_back_orthogonal_deltas ______________
>       assert params.get("objeto") == ["alimentacion"]
E       AssertionError: assert ['alimentacion', 'procesos'] == ['alimentacion']
E         Left contains one more item: 'procesos'
1 failed in 0.98s
PYTEST_EXIT=1
```

La primera envoltura uso accidentalmente `status`, una variable reservada de
zsh, despues de que pytest ya habia mostrado el fallo. Se repitio de inmediato
con `pytest_rc`; el bloque anterior corresponde a esa repeticion limpia.

El adjunto A2 se verifico antes del cambio:

```bash
shasum -a 256 tests/test_v12a1_independent_counterexamples.py \
  /Users/rodrigoortiz/Downloads/test_v12a1_independent_counterexamples.py
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_v12a1_independent_counterexamples.py -q
```

```text
6d5c5f59014d301fc6db153a8b78513bd7e72e0f6f1e1a3117222ee9250b18f2  tests/test_v12a1_independent_counterexamples.py
6d5c5f59014d301fc6db153a8b78513bd7e72e0f6f1e1a3117222ee9250b18f2  /Users/rodrigoortiz/Downloads/test_v12a1_independent_counterexamples.py
.............                                                            [100%]
13 passed in 0.25s
PYTEST_EXIT=0
```

## Pruebas despues del cambio

R3d nuevo, con dos propiedades Hypothesis deterministas de 36 ejemplos maximos:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_entity_profile_r3d_postposed_source.py -q
```

```text
................                                                         [100%]
16 passed in 5.11s
```

La propiedad de framing se amplio despues para generar tambien mayusculas y
Title Case; su corrida focal fue:

```text
.                                                                        [100%]
1 passed in 4.37s
```

El target final de 1.709 pruebas que sigue incluye el archivo R3d ya ampliado.

Reproduccion exacta ya verde:

```text
.                                                                        [100%]
1 passed in 1.34s
```

Adjunto A2 exacto despues del cambio:

```text
.............                                                            [100%]
13 passed in 0.22s
```

Target A2 obligatorio, sin excluir rollback ni R3d:

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
  tests/test_phrase_catalog_collisions.py tests/test_phrase_spec.py -q
```

```text
........................................................................ [  4%]
........................................................................ [  8%]
........................................................................ [ 12%]
........................................................................ [ 16%]
........................................................................ [ 21%]
........................................................................ [ 25%]
........................................................................ [ 29%]
........................................................................ [ 33%]
........................................................................ [ 37%]
........................................................................ [ 42%]
........................................................................ [ 46%]
........................................................................ [ 50%]
........................................................................ [ 54%]
........................................................................ [ 58%]
........................................................................ [ 63%]
........................................................................ [ 67%]
........................................................................ [ 71%]
........................................................................ [ 75%]
........................................................................ [ 80%]
........................................................................ [ 84%]
........................................................................ [ 88%]
........................................................................ [ 92%]
........................................................................ [ 96%]
.............................................................            [100%]
1709 passed in 119.54s (0:01:59)
```

Conteo final del target: `selected=1709`, `passed=1709`, `failed=0`,
`skipped=0`, `deselected=0`, `xfailed=0`. No hubo motivos que registrar para
categorias no verdes.

## Tres puertas en el HEAD de codigo probado

Ruff:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
```

```text
All checks passed!
```

Semgrep:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep \
  --config .semgrep/secoppal-laws.yml app/ --metrics=off
```

```text
Scanning 315 files tracked by git with 3 Code rules:
Scanning 298 files with 3 python rules.

✅ Scan completed successfully.
 • Findings: 10 (10 blocking)
 • Rules run: 3
 • Targets scanned: 298
 • Parsed lines: ~100.0%

Ran 3 rules on 298 files: 10 findings.
```

El target pytest es el bloque literal de 1.709 pruebas anterior. Corrimos ese
target, no la suite global.

## Puertas complementarias

```bash
make lint-core
```

```text
app/core/ limpio
```

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_module_size_limits.py -q
```

```text
..                                                                       [100%]
2 passed in 0.23s
```

```bash
git diff --check a952805de050ec28d571b7a637e9240f88c71a45..HEAD
```

Salida literal: ninguna; exit 0.

No se ejecuto campana de mutacion: `MUTATION_CAMPAIGN=NOT_RUN`.
El modelo y nivel efectivo de razonamiento no son verificables desde el
worktree: `MODEL_AND_REASONING=NO_VERIFICABLE`.

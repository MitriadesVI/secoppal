# Reproducción de la revisión independiente B.3

Worktree: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b3-independent`
sobre `4edc248e60610efdbeebb3cb8010bc7b59445cad`.
Intérprete canónico: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.

`run_audit.py` fija OUT en **este** directorio; nunca escribe evidencia del
implementador ni de auditorías previas. Añade al `PYTHONPATH` la raíz, `tests/`,
el plugin `offline_guard`, los directorios de auditoría B.2/B.3 y el propio;
carga `-p offline_guard` y deja Frame OFF en entorno. El `conftest.py` local
redirige feedback JSONL, SQLite, conversaciones e índice Actor a `tmp_path`.

## Reproducción sobre el SHA final

```bash
D=docs/audits/entity_profile_frame_v1_2_b3_remediation
V=/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python
$V docs/audits/entity_profile_frame_v1_2_b3_independent/run_audit.py own_full \
  $D/test_publication_ports.py $D/test_publication_chains.py \
  $D/test_supplier_ports.py $D/test_ranking_matrix.py --tb=short
```

`321 passed` con el helper definitivo sobre el candidato. Los originales B.2,
intactos:

```bash
B=docs/audits/entity_profile_frame_v1_2_b2_independent
$V docs/audits/entity_profile_frame_v1_2_b3_independent/run_audit.py originals_b2 \
  $B/test_publication_sequences.py $B/test_record_buyer.py $B/test_supplier_topologies.py --tb=line
```

## Targets congelados del implementador, repetidos aquí

`replay.py` extrae el comando literal de cada `.txt.gz`, sustituye la ruta
absoluta del worktree del implementador por la de este worktree y descarta su
`--junitxml`.

```bash
PYTHONPATH=docs/audits/entity_profile_frame_v1_2_b3_independent \
$V docs/audits/entity_profile_frame_v1_2_b3_independent/replay.py \
  acceptance historical_current final_ports \
  regressions_4 regressions_5 regressions_6 regressions_7 regressions_8 regressions_9
```

## Pruebas propias

```bash
for t in test_f4_chained_failures test_f4_property test_f4_limits test_sup_ranking_matrix; do
  $V docs/audits/entity_profile_frame_v1_2_b3_independent/run_audit.py own_$t \
     docs/audits/entity_profile_frame_v1_2_b3_independent/$t.py --tb=short
done
```

- `test_f4_chained_failures.py` (7): cadenas A→B→C sobre `ConversationStore`
  real, lectura desde un intérprete nuevo por `subprocess`, marca inerte,
  publicación legítima sin reset, reset, y las tres formas de prefijo bloqueado.
- `test_f4_property.py` (1, Hypothesis, 40 ejemplos, `derandomize=True`):
  ninguna cadena de 2–5 fallos admite un registro no publicado.
- `test_f4_limits.py` (3): **observaciones** del límite de recuperación, no
  garantías. Escriben `limit_marker_write.json` y `limit_marker_flush.json`.
- `test_sup_ranking_matrix.py` (104): 5×5 estados × 2 focos × disjunto/coincidente.

## Mutaciones

```bash
$V docs/audits/entity_profile_frame_v1_2_b3_independent/mutate_independent.py all
$V docs/audits/entity_profile_frame_v1_2_b3_independent/mutate_independent.py \
   drop-blocked-prefix-capture docs/audits/entity_profile_frame_v1_2_b3_independent/test_f4_property.py
```

Exporta `git archive` del SHA a scratchpad, corre el control, aplica el mutante,
corre de nuevo, restaura y comprueba SHA256; luego verifica con `git diff --quiet`
que el worktree no cambió. Resultados en `mutations_*.json`.

## Puertas

```bash
V=/Users/rodrigoortiz/Documents/secoppal/.venv/bin
$V/ruff check app/ tests/ scripts/
$V/ruff check docs/audits/entity_profile_frame_v1_2_b3_independent
$V/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off --json \
  --output docs/audits/entity_profile_frame_v1_2_b3_independent/semgrep_head.json
make lint-core
git diff --check a7e12ebf..HEAD && git diff --check && git diff --cached --check
```

Para el delta de Semgrep se exporta `a7e12ebf` a scratchpad, se crea una raíz Git
temporal (Semgrep sólo escanea archivos trackeados) y se compara por regla, ruta,
línea y fragmento, no por conteo: `semgrep_delta.json`.

## Preexistencia (probada, no asumida)

```bash
# Los tres rojos amplios sobre la base a7e12ebf exportada
$V/python -m pytest -q \
  tests/test_entity_profile_frame_b_publication.py::test_a_record_from_another_catalog_does_not_govern_the_next_turn \
  tests/test_entity_profile_frame_b_semantics.py::test_a_forged_execution_record_does_not_change_the_next_turn \
  tests/test_entity_profile_frame_integration_contract.py::test_a_relative_period_does_not_get_reinterpreted_on_reload
# El reset que desactiva la barrera, en base y en candidato
$V/python docs/audits/entity_profile_frame_v1_2_b3_independent/probe_reset.py
```

## Cobertura de la colección por defecto

```bash
$V/python -m pytest --collect-only -q            # 13089, ninguno de B.3
$V/python -m pytest --collect-only -q docs/audits/entity_profile_frame_v1_2_b3_remediation/   # 321
$V/python -m pytest --collect-only -q docs/audits/entity_profile_frame_v1_2_b3_independent/   # 115
```

No se ejecutó pytest global, app, SECOP/LLM live, datos del usuario, mensajes ni
radares reales. Sin merge, push ni activación.

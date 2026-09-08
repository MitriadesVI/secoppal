# Reproducción y mapa para Test Safety

Ejecutar desde `/Users/rodrigoortiz/Documents/secoppal-buyer-actor-role-arbitration-v1-independent`.
El Python canónico es `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.
Los runners crean persistencia temporal **antes** de importar dependencias,
apagan telemetría Burr, fijan Actor ON y controlan Frame por proceso.

```sh
PY=/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python
RUN=scripts/audits/buyer_actor_independent_run.py

$PY "$RUN" own-repeat -- "$PY" -m pytest -p tests.buyer_actor_independent_guard tests/test_buyer_actor_independent_product.py tests/test_buyer_actor_independent_boundaries.py -q --tb=short -ra
$PY "$RUN" --local local-repeat -- "$PY" -m pytest -p tests.buyer_actor_independent_guard tests/test_buyer_actor_independent_local.py -q -ra
$PY "$RUN" --local author-local-repeat -- "$PY" -m pytest -p tests.buyer_actor_independent_guard tests/test_buyer_actor_role_local_v1.py -q -ra
$PY "$RUN" --manifest docs/audits/buyer_actor_role_arbitration_v1/focal-manifest.txt focal-strict-repeat --
$PY "$RUN" --frame true --local --manifest docs/audits/buyer_actor_role_arbitration_v1/frame-on-manifest.txt frame-strict-repeat --
$PY "$RUN" corpus-repeat -- "$PY" scripts/run_query_corpus.py --mode all
$PY scripts/audits/buyer_actor_independent_experiments.py mutations
$PY scripts/audits/buyer_actor_independent_experiments.py history
```

El manifiesto con guarda estricta puede terminar rojo por los dos defectos
heredados del arnés documentados. No se deseleccionan para declarar verde.
Para reproducir exactamente los targets y plugin del autor, usar el array
`command` de `author-exact.command.json` o `frame-on-exact.command.json` a través
del runner. Sus salidas históricas de esta auditoría preceden la desactivación
de telemetría; ésta sólo controla una frontera externa, no semántica.

Ruff, Semgrep, lint-core, tamaño y diff:

```sh
$PY "$RUN" ruff-repeat -- /Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
$PY "$RUN" semgrep-repeat -- /Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off --json
$PY "$RUN" lint-repeat -- make lint-core
$PY "$RUN" size-repeat -- "$PY" -m pytest tests/test_module_size_limits.py -q
git diff --check
```

En los comandos por garantía, sustituir `<NODE>` por el node ID de la tabla:

```sh
$PY "$RUN" focal-guarantee -- "$PY" -m pytest -p tests.buyer_actor_independent_guard '<NODE>' -q --tb=short -ra
```

| Garantía de producto | Node ID vigente | Comando/selector |
|---|---|---|
| Semillas, SQL, respuesta, recarga y páginas disjuntas | `tests/test_buyer_actor_independent_product.py::test_seed_universe_and_reload` | Comando por garantía; 20 combinaciones |
| Proveedores positivos, relación e institución proveedora UIS | `tests/test_buyer_actor_independent_product.py::test_positive_actors` | Comando por garantía; 32 combinaciones |
| Nueva búsqueda tras Actor confirmado/pendiente | `tests/test_buyer_actor_independent_product.py::test_new_search_after_actor` | Comando por garantía; 8 combinaciones |
| Refinamiento conserva Actor | `tests/test_buyer_actor_independent_product.py::test_dependent_actor_refinement` | Comando por garantía; 4 combinaciones |
| Artículos, filtros y otras instituciones | `tests/test_buyer_actor_independent_boundaries.py::test_variants` | Comando por garantía |
| Prefijo/fuzzy no acreditan mención completa | `tests/test_buyer_actor_independent_boundaries.py::test_partial_or_fuzzy_buyer_cannot_veto_whole_mention` | Comando por garantía |
| Geografía, tema y negaciones soportadas | `tests/test_buyer_actor_independent_boundaries.py::test_geography_topic_negation` | Comando por garantía |
| City-to-entity integrado no impone rol | `tests/test_buyer_actor_independent_boundaries.py::test_city_autopromotion_does_not_suppress_actor` | Comando por garantía |
| Perfil institucional conserva dueño | `tests/test_buyer_actor_independent_boundaries.py::test_explicit_institution_profile_preserves_owner` | Comando por garantía |
| Estabilidad con oráculo positivo | `tests/test_buyer_actor_independent_boundaries.py::test_positive_buyer_index_invariance` | Comando por garantía; Hypothesis 35 ejemplos |
| Flags efectivos con índice real | `tests/test_buyer_actor_independent_local.py::test_local_effective_flags_and_product` | Comando local-repeat, opt-in obligatorio |
| Caracterización de negación incorrecta heredada | `tests/test_buyer_actor_independent_boundaries.py::test_short_negation_characterization` | Comando por garantía y runner history; no es aceptación del comportamiento |
| Detectar agregado externo absorbido en corpus | `tests/test_actor_conversation_corpus.py::test_all_expanded_sequences_mutate_real_query_frames` | Comando por garantía con guarda estricta; rojo heredado esperado |
| Detectar agregado externo absorbido en render | `tests/test_actor_profile_render.py::test_a_specific_aggregate_question_still_gets_its_specific_answer` | Comando por garantía con guarda estricta; rojo heredado esperado |

`nodeids.txt` contiene la colección expandida de pruebas propias. No se copian
pruebas ni hooks al carril Test Safety: éste puede tomar los node IDs como mapa.

## Evidencia y preservación

- `*.command.json`: argv, cwd, flags declarados por runner y exit.
- `*.stdout.txt.gz` / `*.stderr.txt.gz`: bytes originales sin pérdida.
- `traces/*.tar.gz`: trazas completas por ejecución; incluye runs exploratorios
  fallidos del arnés, que no cuentan como hallazgos de producto ni mutation kills.
- `evidence-packaging.json`: SHA256 de los bytes originales empaquetados.
- `matrix.json`: resumen legible de semillas, contrastes, historia y mutaciones.
- `mutations.json`: baseline, mutante y hashes tras restauración.
- `semgrep-delta.json`: comparación por regla/ruta/fragmento, no sólo contador.
- `preservation-before.json` y `preservation-final.json`: bytes del candidato
  preservados, separado de archivos nuevos propios.

Las exportaciones históricas y mutaciones son temporales. Las mutaciones se
restauran y verifican antes de retirar cada copia. El script no modifica el
checkout auditado. El historial pre-Frame sólo se corre OFF; no tiene el flag
Frame. Para releer una traza: `tar -xOf traces/<run>.tar.gz '<archivo>.json'`.
Para un log literal: `gzip -cd <run>.stdout.txt.gz`.

No ejecutar pytest global, dev.sh, make dev/restart ni consultas live como parte
de esta reproducción. No reconstruir el índice local ni ampliar sus menciones.

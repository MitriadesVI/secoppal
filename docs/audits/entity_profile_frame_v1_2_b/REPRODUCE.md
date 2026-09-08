# V1.2-B — Reproducción y salidas literales

Todo se ejecutó en:

```text
/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-b-integration
```

Rama `feature/entity-profile-frame-v1-2-b-integration`, base congelada
`4c86d5bafaa3296b73c6523465b33efa4311910a` (verificada como descendiente del
HEAD de código probado de R3d, `e4b85db855d50f7e5671a515bfe1b26c882c2867`).

No se levantó la app, no se usó red, no se construyó `Settings()` fuera de las
fixtures de pytest, no se crearon radares ni se enviaron mensajes, y no se
escribió en los `data/` del usuario. El intérprete es el venv canónico del
worktree principal: `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python`.

## 1. Preflight

```text
Python 3.14.7
ruff 0.16.4
semgrep 1.176.0
Hypothesis 6.165.10
burr 0.40.2
node v22.23.2 (para Codegraph; el node del PATH es v26.8.1 y Codegraph lo rechaza)
codegraph 0.9.6
```

Se leyó la API instalada de Burr (`burr.core.ApplicationBuilder`, `expr`,
`Application.run(halt_after=…)`) en el propio grafo del flujo normal, no de
memoria.

**Codegraph:** el índice no existía en este worktree; se inicializó e indexó
localmente con Node 22 y se excluyó de Git por `.git/info/exclude`.

```text
codegraph status
  Files:     849
  Nodes:     19,714
  Edges:     18,878
  Index is up to date
```

Ese conteo de aristas es notablemente menor que el que reporta el diseño para
el índice de la auditoría (49.751). **Se declara el límite:** el grafo se usó
para `impact` sobre los símbolos tocados y sus resultados se completaron con
`rg` y lectura manual; no se afirma cobertura equivalente a la de aquel índice.
`impact(profile_query_params)`, `impact(compile_profile)`,
`impact(render_entity_profile)`, `impact(append_turn)` (87 símbolos) y
`impact(run_query)` (162) guiaron el conjunto de consumidores adyacentes.

## 2. Puertas

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
```

```text
All checks passed!
```

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep \
  --config .semgrep/secoppal-laws.yml app/ --metrics=off
```

```text
  Scanning 332 files tracked by git with 3 Code rules:
  Scanning 315 files with 3 python rules.
 • Findings: 10 (10 blocking)
 • Rules run: 3
 • Targets scanned: 315
Ran 3 rules on 315 files: 10 findings.
```

Baseline y delta **por identidad**, no sólo por conteo. Se ejecutó el mismo
comando en el worktree de la base (`secoppal-entity-profile-r3d-postposed-source`)
y se compararon `(archivo, línea, regla)`:

```text
BASE findings: 10                       HEAD findings: 10
  app/core/formatter.py 171               app/core/formatter.py 171
  app/core/formatter.py 699               app/core/formatter.py 699
  app/core/response_policy.py 508         app/core/response_policy.py 508
  app/core/response_policy.py 577         app/core/response_policy.py 577
  app/core/workflow/actions/execution.py 365   idem
  app/core/workflow/handlers/direct_lookup.py 366  idem
  app/core/workflow/handlers/normal_flow.py 171    idem
  app/core/workflow/handlers/pagination.py 177     idem
  app/core/workflow/handlers/radar.py 109          idem
  app/core/workflow/handlers/refinement.py 93      idem
```

`diff` de las dos listas: **idéntico**. Delta = 0 hallazgos nuevos, 0 movidos,
0 en archivos propios. El exit 0 de la herramienta no convierte esos diez en
verde.

```bash
make lint-core
```

```text
app/core/ limpio
```

```bash
git diff --check 4c86d5bafaa3296b73c6523465b33efa4311910a..HEAD
```

```text
(sin salida; exit 0)
```

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest tests/test_module_size_limits.py -q
```

```text
..                                                                       [100%]
2 passed in 0.12s
```

## 3. Target completo de R3d, sin excluir su prueba de rollback, más B

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_entity_profile_frame_b_integration.py \
  tests/test_entity_profile_frame_b_admission.py \
  tests/test_entity_profile_frame_b_publication.py \
  tests/test_entity_profile_frame_b_semantics.py \
  tests/test_entity_profile_frame_b_properties.py \
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
1887 passed in 125.51s (0:02:05)
```

R3d cerró ese mismo target en 1.709; el delta son las pruebas nuevas de B más
`test_entity_profile_frame_b_utility.py`, que se añadió después de esa corrida
y se ejecuta aparte más abajo. `selected=1887`, `passed=1887`, `failed=0`,
`skipped=0`, `deselected=0`, `xfailed=0`.

## 4. Consumidores adyacentes, justificados por el mapa de impacto

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_orchestrator.py tests/test_orchestrator_golden.py \
  tests/test_workflow_architecture.py tests/test_workflow_persistence.py \
  tests/test_workflow_results.py tests/test_conversation_store.py \
  tests/test_conversation_snapshot.py tests/test_pagination_buffer.py \
  tests/test_pagination_buffer_lifecycle.py tests/test_pagination_transparency.py \
  tests/test_newsearch_pagination.py tests/test_suggestion_selection.py \
  tests/test_contextual_suggestions.py tests/test_refinement_recovery.py \
  tests/test_direct_lookup.py tests/test_direct_lookup_entity_scope.py \
  tests/test_radar_commands.py tests/test_radar_builder.py \
  tests/test_dual_dataset_conversation.py tests/test_module_size_limits.py \
  tests/test_streamlit_operational_state.py tests/test_actor_flag_off.py \
  tests/test_degrade_query.py tests/test_count_lazy.py \
  tests/test_guard_where_1_1_equivalence.py \
  tests/test_actor_hardening_v2_1_remediation.py tests/test_frame_leak_canary.py -q
```

```text
472 passed, 2 xfailed in 39.35s
```

Los dos `xfail` son preexistentes y su motivo está escrito en el propio archivo:

```text
XFAIL tests/test_actor_hardening_v2_1_remediation.py::test_actor_region_and_buyer_together_still_resolve[…]
  - Pre-existing in BASE f9cb8c9, not a V2 regression […]
```

## 5. Corpus y puerta de utilidad, en modo offline

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_entity_profile_semantic_corpus.py -q
```

```text
276 passed in 0.90s
```

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_entity_profile_frame_b_utility.py -q
```

```text
15 passed in 3.34s
```

Adversariales de A2 y contratos independientes:

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest \
  tests/test_entity_profile_frame_core.py \
  tests/test_entity_profile_frame_independent_review.py \
  tests/test_entity_profile_frame_independent_contracts.py \
  tests/test_entity_profile_frame_integration_contract.py \
  tests/test_v12a1_independent_counterexamples.py \
  tests/test_entity_profile_frame_mutations.py \
  tests/test_entity_profile_frame_properties.py \
  tests/test_entity_profile_semantic_corpus.py -q
```

```text
408 passed in 1.69s
```

Los trece contraejemplos de A2 siguen verdes por separado:

```text
tests/test_v12a1_independent_counterexamples.py: 13 passed in 0.16s
```

## 6. Hypothesis determinista

`tests/test_entity_profile_frame_b_properties.py` — nueve propiedades,
`settings(max_examples=25, derandomize=True, deadline=None)`, sin I/O externo.

```text
9 passed in 0.37s
```

Las propiedades del núcleo (round-trip del Frame, KEEP, CLEAR idempotente,
parches independientes que conmutan, métricas no finitas) ya viven en
`tests/test_entity_profile_frame_properties.py` y no se duplican; las de B
cubren lo que sólo existe cuando el universo sobrevive a un proceso.

## 7. Campaña de mutaciones

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python \
  scripts/audits/entity_profile_frame_v1_2_b/mutation_campaign.py
```

```text
baseline verde: 13 passed in 3.89s

M1-pending-without-selection: DETECTED (restored=True) — 1 failed in 1.19s
M2-old-cursor-after-corruption: DETECTED (restored=True) — 1 failed in 0.90s
M3-seed-bypasses-admission: DETECTED (restored=True) — 1 failed in 0.93s
M4-subject-and-event-swapped: DETECTED (restored=True) — 1 failed in 0.82s
M5-or-flattened-into-and: DETECTED (restored=True) — 1 failed in 0.90s
M6-exclusion-dropped-from-scope: DETECTED (restored=True) — 1 failed in 0.80s
M7-legacy-reader-back-in-the-path: DETECTED (restored=True) — 2 failed in 1.26s
M8-transport-success-fabricated: DETECTED (restored=True) — 1 failed in 0.90s
M9-nonfinite-metric-accredited: DETECTED (restored=True) — 1 failed in 0.89s
M10-month-globalised: DETECTED (restored=True) — 1 failed in 0.88s
M12-cross-process-lock-removed: DETECTED (restored=True) — 1 failed in 0.71s
M11-injected-clock-ignored: DETECTED (restored=True) — 1 failed in 0.91s

{"detected": 12, "survived": 0, "inconclusive": 0, "anchor_not_found": 0, "total": 12}
```

Detalle por mutación en [mutation_campaign.json](mutation_campaign.json), con la
tesis de cada una, su archivo, el sha256 antes y después (idénticos: `restored`
verdadero en las doce) y la cola literal de pytest.

El clasificador distingue **DETECTADA** de **INCONCLUSA**: un `ImportError`, un
error de recolección, un `INTERNALERROR` o un timeout no cuentan como muerte.
Las doce murieron por una aserción de comportamiento.

**Un intento fallido, declarado:** la primera versión de M12 apuntaba a
`test_the_exclusion_holds_between_processes_not_only_threads` —cuatro procesos
publicando a la vez— y **SOBREVIVIÓ**. La razón es real y vale la pena decirla:
la comprobación de `expected_revision` bajo la exclusión ya cierra casi toda la
ventana por sí sola, y la carrera restante es demasiado estrecha para forzarla
de forma determinista con cuatro procesos. En vez de inventar una carrera, se
añadió una prueba del MECANISMO —dos procesos no pueden sostener el candado del
mismo chat— que sí discrimina de forma determinista, y M12 apunta a ella. La
prueba de los cuatro publicadores se conserva: comprueba el resultado
observable (ninguna revisión repetida, secuencia sin huecos), aunque no sea el
oráculo que mata la mutación.

**M11 encontró un defecto real de esta entrega**, no sólo confirmó una guarda:
`today` llegaba a `run_profile_turn` y no bajaba a `collect_claims`, de modo que
un periodo relativo se resolvía con la fecha del sistema. Está corregido en
`11399da` y cubierto por
`test_the_injected_clock_is_the_one_that_resolves_relative_periods`.

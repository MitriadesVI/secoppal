# Mapa de targets obligatorios para el coordinador

## Puertas que deben quedar verdes

| Puerta | Comando | Estado en `8fc5643` |
|---|---|---|
| Ruff | `ruff check app/ tests/ scripts/` | verde |
| Semgrep | `semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off` | baseline rojo estable: 10 hallazgos, **0 nuevos y 0 resueltos** frente a BASE main y frente a V1, comparados por regla + ruta + fragmento con multiplicidad ([semgrep_delta.json](semgrep_delta.json)) |
| Targets dirigidos del autor | los 41 archivos declarados en `REMEDIATION.md` | `1217 passed` |
| Corpus | `scripts/run_query_corpus.py --mode all` | `89/89 PASS` |
| lint-core | `make lint-core` | `app/core/ limpio` |
| Tamaño | `tests/test_module_size_limits.py` | 2 passed |
| diff-check | `git diff --check` | exit 0 |

Todo pytest de contratos requiere `-p tests.contract_universe_offline_guard` y
`-p tests.contract_universe_sql_dates`.

## Oráculos congelados a actualizar (autorizado)

Los diez son expectativas numéricas obsoletas; su contrato semántico ya pasa.
Justificación y aserción exacta en [FROZEN_TEST_ADJUDICATION.md](FROZEN_TEST_ADJUDICATION.md).

| Archivo | Test | Cambio de expectativa |
|---|---|---|
| `tests/test_contract_universe_independent.py` | `test_independent_sql_matrix` ×4 | `24` → `22` (línea 34; y `24 *` → `22 *` en 35 y 41) |
| `tests/test_contract_universe_independent.py` | `test_direct_record_does_not_leak_to_search` | `24` → `22` (línea 168) |
| `tests/test_contract_universe_independent.py` | `test_radar_cancellation_event_and_digest` | `24 … 23` → `22 … 21` (línea 184) |
| `tests/test_contract_universe_independent_consumers.py` | `test_actor_name_state_word_has_no_universe_authority` ×2 | `26` → `24` (línea 43) |
| `tests/test_contract_universe_independent_consumers.py` | `test_materialized_dual_children_and_source_names[celebrated]` | parámetro `24` → `22` (línea 48) |
| `tests/test_contract_universe_independent_legacy.py` | `test_real_base_conversation_requires_review` | `24` → `22` (línea 30) |

Al actualizarlos conviene añadir la composición explícita que el conteo no
expresa: `A-7-dated` dentro, `A-7-absent` y `A-7-invalid` fuera. Los ports de
esta revisión ya la incluyen y pueden reutilizarse.

## Bloqueante antes del gate

El P1 de F2 descrito en [F1_F5_MATRIX.md](F1_F5_MATRIX.md) y congelado en
`tests/test_cu_v11_audit_findings.py` (8 rojos). Es en parte una **regresión**
introducida por la propia remediación, no sólo un residuo. Debe cerrarse en el
carril de remediación, no en el gate.

## Trazabilidad

Ni V1 ni V1.1 reservaron ADR ni tocaron `docs/decisions_log.md`. El coordinador
debe registrar la política y su versión (`contract-universe-1.1`,
`entity-profile-frame-2-contract-universe-1.1`) al integrar, con el número de
ADR tomado del máximo global sobre los worktrees, no del máximo en `main`.

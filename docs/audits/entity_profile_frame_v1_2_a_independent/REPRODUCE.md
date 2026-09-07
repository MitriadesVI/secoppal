# Reproducción acotada

Cwd: `/Users/rodrigoortiz/Documents/secoppal-entity-frame-v1-2-a-independent`.
El candidato runtime es `9da35b527e88b6d8612e94a5c5828f854d53a138`.
Los tests añadidos son auditoría con aserciones rojas deliberadamente visibles;
no usar este branch como entrega funcional verde.

Usar el Python del entorno canónico evita instalar dependencias o crear
persistencia de Settings. Los tests independientes bloquean sockets y congelan
la fecha civil de claims además del reloj del parser (2026-09-07).

```bash
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/ruff check app/ tests/ scripts/
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep --config .semgrep/secoppal-laws.yml app/ --metrics=off
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest tests/test_entity_profile_frame_core.py tests/test_entity_profile_frame_migration.py tests/test_entity_profile_frame_evidence.py tests/test_entity_profile_frame_properties.py tests/test_entity_profile_frame_mutations.py tests/test_entity_profile_frame_catalog_parity.py tests/test_entity_profile_frame_integration_contract.py -q
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest tests/test_entity_profile_frame_independent_review.py tests/test_entity_profile_frame_independent_contracts.py -q --tb=short
/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python -m pytest tests/test_entity_profile_queries.py tests/test_soql_builder.py tests/test_soql_builder_phrase.py tests/test_object_negation_soql.py tests/test_query_router.py tests/test_module_size_limits.py -q
make lint-core
git diff --check 62ec330..HEAD
```

Esperado en la auditoría: candidato 86 passed; independientes 15 failed y
31 passed; adyacentes 131 passed. El harness original tiene un reloj de claims
implícito: para reproducir los tests del autor en otra fecha debe suministrarse
2026-09-07 a `collect_claims` desde un adaptador de test. Los independientes ya
lo congelan. No se modificó ese harness del candidato.

Los cuatro tests `test_required_mutation_is_killed_by_original_invariant`
corren primero la aserción original y después el mismo test con una mutación en
memoria. Debe lanzar AssertionError; no se escriben módulos mutados a disco.

Para regenerar los JSON de observación, establecer `FRAME_REVIEW_OUTPUT` a un
directorio propio ya creado al ejecutar los independientes. Sin esa variable,
los tests no escriben evidencia a este directorio. Las salidas JSON del commit
son observaciones sintéticas del parser/Frame/SQL, no datos extraídos de SECOP.

No correr suite completa, dev.sh, make dev ni make restart. No se necesita
conversación persistida, transporte SECOP ni la app levantada.

Herramientas: Ruff 0.16.4; Semgrep 1.176.0; Hypothesis 6.165.10;
Codegraph CLI 0.9.6 con `/opt/homebrew/opt/node@22/bin` primero en PATH.
Codegraph MCP no está cargado. `affected` vacío requiere fallback manual,
como documenta el informe; no se toma como prueba de ausencia de consumidores.

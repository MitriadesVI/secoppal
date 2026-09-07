# Evidencia técnica y mapa de impacto A2

## Proveniencia de adjuntos

| Artefacto | SHA-256 | Uso |
|---|---|---|
| `test_v12a1_independent_counterexamples.py` | `6d5c5f59014d301fc6db153a8b78513bd7e72e0f6f1e1a3117222ee9250b18f2` | Copia byte a byte ejecutada antes/después. |
| `v12a1_isolated_review_results.json` original | `d524f86267a4fb084ff33a3ed9de7de7e26cf41e6bedea6cfb026ec73c28f589` | Evidencia histórica; no se presenta como pytest del repo. |
| Copia JSON en este directorio | `e2a84e24615f511bad5a725efad6df49a0397c89719525ccd67faae3df734fad` | Mismo contenido; sólo añade el salto de línea final requerido por el repositorio. |
| `SECOPPAL_PROMPT_FRAME_V1_2_A2_REMEDIACION.md` | `0cbc0752ee466ff9f829d50bbd6c403a396e9ab57e6436d8302f8824735ca2f6` | Orden de trabajo, no evidencia de producto. |

El original permanece intacto en Downloads; la copia canónica de este
directorio sólo difiere en el salto de línea final. No se reinterpreta
`migration_hypothesis: NOT_RUN`: esa hipótesis sí se ejecutó luego contra A1 y
quedó confirmada.

## Codegraph

El índice se sincronizó en el worktree A2 con Node 22 y quedó:

```text
Files: 894
Nodes: 21,124
Edges: 50,883
python: 872
yaml: 22
Index is up to date
```

Consultas usadas:

```bash
codegraph callers deep_freeze
codegraph impact deep_freeze --depth 3
codegraph callers source_head_occurrences
codegraph impact source_head_occurrences --depth 3
codegraph callers migrate_legacy_context
codegraph callers mutable_copy
codegraph affected <archivos modificados>
```

Resultados materiales:

- `deep_freeze`: consumidores runtime directos `ProfilePlan.__post_init__` y
  `SourcePlan.__post_init__`; 21 símbolos de impacto a profundidad 3.
- `source_head_occurrences`: `source_request._clause_head_sources`,
  `QueryRouter.parse` y `mask_source_heads`; 31 símbolos a profundidad 3.
- `mutable_copy`: un consumidor runtime, `query_runner.execute_jobs`, más el
  adapter de test que compila directamente un `SourcePlan`.
- `migrate_legacy_context`: sólo tests; no hay lector productivo.
- `affected` no encontró tests, una limitación del índice; por eso la selección
  se completó con `rg`, revisión manual y las corridas adyacentes explícitas.

## Paridad A1/A2 de Search

Se ejecutaron dos procesos Python aislados, cada uno con `PYTHONPATH` y `cwd`
de su worktree, y se comparó el JSON completo de `QueryRouter.params`.

```text
PARITY_UNAFFECTED=15/15
UNEXPECTED_DIVERGENCES=[]
TARGETED_DIVERGENCES=5/5
```

Corpus sin divergencia inesperada: tiempo por fuente, oportunidades vigentes,
dinero, exclusiones, frase de catálogo, proveedor, territorio, gestión
contractual, administración de contratos, elección `procesos o contratos`,
BOTH, búsqueda sin fuente y el conflicto histórico procesos/proveedores.

Los cambios objetivo fueron:

- `busca|buscar|cuales|por favor contratos de software`: desaparece únicamente
  `contratos` del tema positivo.
- `contratos de gestión de procesos y contratos`: desaparece el BOTH falso,
  Search queda CONTRACTS y el tema recupera la última ocurrencia.

## Semgrep baseline y delta

El conjunto `(regla, archivo, línea)` es idéntico en A1 y A2:

```text
semgrep.except-exception-que-silencia app/core/formatter.py 171
semgrep.except-exception-que-silencia app/core/formatter.py 699
semgrep.except-exception-que-silencia app/core/response_policy.py 508
semgrep.except-exception-que-silencia app/core/response_policy.py 577
semgrep.except-exception-que-silencia app/core/workflow/actions/execution.py 365
semgrep.except-exception-que-silencia app/core/workflow/handlers/direct_lookup.py 366
semgrep.except-exception-que-silencia app/core/workflow/handlers/normal_flow.py 171
semgrep.except-exception-que-silencia app/core/workflow/handlers/pagination.py 177
semgrep.except-exception-que-silencia app/core/workflow/handlers/radar.py 109
semgrep.except-exception-que-silencia app/core/workflow/handlers/refinement.py 93
```

Ningún archivo propio aparece en los hallazgos. Baseline: 10; delta: 0.

## Activación productiva

La búsqueda estática fuera de `app/core/entity_profile/` de
`EntityProfileFrame`, `ProfilePlan`, `compile_profile`, `collect_claims` y
`migrate_legacy_context` no devuelve archivos. El diff no toca workflow, UI ni
configuración de activación. Resultado: Frame productivo OFF y consumidores
productivos nuevos NONE.

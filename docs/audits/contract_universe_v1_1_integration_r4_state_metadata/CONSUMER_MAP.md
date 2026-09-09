# Consumidores runtime y alcance de la inspección

Preflight: Ruff 0.16.4, Semgrep 1.176.0, Hypothesis 6.165.10. Codegraph 0.9.6
bajo Node 22: `codegraph status` devuelve **Not initialized**. El Node 26 del
PATH original fue rechazado. No hay índice acreditado ni se afirma MCP activo.
Fallback explícito: `rg` y lectura de productores/consumidores. La búsqueda
completa se conserva en `consumer_search.txt`; no se confunde disponibilidad
con cobertura de impacto.

| Consumidor | Lectura y contrato observado | Verificación |
|---|---|---|
| `query_router.py:862-881,1123` | Productor de familia, filtro, estado/campo legacy; después aplica universo | Base/candidato after_parse, tests firmados e invariantes |
| `estado_projection.py:58` | Rellena proyección completa solo sin filtro previo | Familia activa de tres estados intacta |
| `contract_universe_intent.py:127` | Adaptador perdía las tres claves en estado compuesto escalar | Hook mínimo al nuevo contract_state_metadata; C11/C13 |
| `soql_builder.py:530-551` | Omite estado legacy si ya existe un campo físico; compila lista completa | SQL_PARITY y controles con metadata retirada |
| `followup_engine.py:630`, `followup_mutations.py:243` | Estado se reemplaza como grupo; rebase de familia al dataset heredado | tests followup_mutations y controles reemplazo Cerrado → Suspendido/terminado; limitación bare suspendidos documentada |
| `query_frame.py:60`, `conversation_store.py:66,553` | Conservan metadata en modificadores/params, merge y persistencia | QueryFrame, ConversationStore, nuevo reload real en tmp_path |
| `refinement_recovery.py:23,44` | Describe estado a partir de estado/estado_family; no etiqueta estado_contrato | Nuevo consumidor de recuperación y mutación M2 |
| `entity_profile/frame_migration.py:264-330` | Valida campo y valores legacy contra filtros físicos, y family contra fuente | Suite de migración; no se cambia su contrato |
| `entity_profile/source_plan.py:146-159` | Proyecta campos pertinentes a la fuente; no mezclar metadata de procesos | Frozen y consumidores de perfil |
| `entity_profile/claim_adapters.py:470` | Acepta legacy estado/campo con fuente compatible, sin atribuir un default a una claim explícita | Frozen Frame OFF/ON y suite B de integración |
| `entity_profile/material_filters.py:212`, `effective_scope.py:57` | Declaran filtros ejecutados como Estado actual | P17 adjudicado y control Suspendido |
| `entity_profile/render.py:241-244`, `frame_render.py:195` | Universo se declara por separado; estado explícito usa su línea propia | Seis renders base ya incluyen label; M1 falla al inventar Estado actual |
| `contextual_suggestions.py:119-125` | Ya reconoce estado, family y campo físico para no repetir sugerencias | Suite contextual_suggestions y control tras reload |
| `formatter.py:197-199` | Sugerencia de relajar estado acepta legacy o campo físico; estado de filas no es params | Lectura directa; no se altera traducción de filas |
| `contract_invariants.py:114-125` | Purga estados ficticios y campos legacy huérfanos | Suite de caracterización y tests de firmado |
| `followup_mutations.py:130-149,315-325` | Diferencia delta de ejecución y campos atribuidos al turno | C12, source_records explícito, M4 |
| `modality_followup_wiring.py:369` | Delega solo sus claves de modalidad; no consume metadata universo | Lectura directa; no se cambia |

No todos necesitan metadata legacy para SQL, pero sí hay contratos productivos
que la consumen o validan. Por eso no se adjudican C11/C13 como tests obsoletos.
Se mantienen los tests originales; solo P17 cambia, después de registrar la
adjudicación y observar los seis perfiles completos.

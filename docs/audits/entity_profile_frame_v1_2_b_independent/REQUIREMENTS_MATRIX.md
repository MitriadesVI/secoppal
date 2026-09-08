# Matriz de requisitos y evidencia

Todos los paths de tests son relativos a este worktree. `I` = integración real con fronteras externas sintéticas; `A` = API aislada; `S` = lectura estática; `R` = reproducción dirigida del autor. PASS aplica al caso probado, no a todas sus posibles variantes.

| Requisito / frontera | Evidencia y alcance | Resultado |
|---|---|---|
| SHA, raíz, rama, ascendencia BASE | git rev-parse/worktree/merge-base; candidate.diff.gz | PASS |
| Runtime/test original intacto | candidate_hashes.json, integrity.json; 765 archivos | PASS |
| Diseño §§1–16 y autoridades A2/R3d | lectura de documentos congelados | LEÍDO |
| Orden B original | búsqueda de documento local; no apareció | NO DISPONIBLE; no reconstruida |
| Instrumentos | preflight.log, codegraph_status.log, codegraph_impact.log | Ruff/Semgrep/Hypothesis activos; Codegraph sin índice ni MCP; fallback rg |
| Cierre autor sobre SHA final | author_5.log / author_6.log | R: 1905 PASS; 472 PASS + 2 XFAIL |
| 1887 vs 1905, 11399da vs candidato | git diff; runtime idéntico; +15 utilidad, +3 publicación | RECONCILIADO |
| Inventario B | 17 app nuevos, 8 modificados, streamlit; diff_inventory.txt | DESFASE DOCUMENTAL, no defecto funcional |
| Flag realmente usado | author_b_flags.json por nodeid | 60 ON / 136 OFF; incluye tests puros |
| Xfail preexistentes | xfail_comparison.json y BASE/candidate --runxfail | MISMA aserción CANDIDATES != RESOLVED |
| A: aclaración conserva confirmed | continuity::test_clarification_preserves...; T1/T2/T3, ocho variantes | I: F1 |
| A: pendiente conserva estado al aclarar | mismo test con pending=True | I: F1 |
| A: `1`, chip viejo, repetición | mismo test; cero llamadas de aclaración, estado perdido | I: F1 |
| A: cancelación distinta de reset | test_explicit_cancel... / external reset | I: PASS |
| A: nueva consulta inequívoca | external_commands + full_reformulation | I: F2/F3; controles positivos |
| B: pending real sin alternativas | full_reformulation, sin/con confirmed y ambos compradores | I: F2, 4 rojos/2 verdes |
| B: cancelación control | explicit_cancel_is_a_control... | I: PASS |
| C: Radar LIST/RUN confirmado vs pendiente | external_commands; radar_run... captura owner y efecto sintético | I: F3; confirmado PASS |
| C: lookup por ID | external_commands, delegated_results_publish... | I: F3 pendiente; F6 publicación |
| C: documento Actor | external_commands | I: F3 pendiente; confirmado ejecuta |
| C: API/token Actor vigente | current_actor_api_token...; control handler real | I + A: F3 |
| C: reset | external_commands y reset con reserva intercalada | I: serial PASS; concurrencia F4 |
| C: nueva tabla | external_commands | I: confirmado PASS; pendiente F3 |
| C: Radar SAVE / perfil no exportable | radar_save_cannot_export... spy en frontera externa | I: F10, preexistencia probada |
| D: dos reservas misma revisión antes de publicación | reservation_serializes... con Barrier | I: F4; no CAS corrupto |
| D: ejecución A→B, publicación A→B/B→A | OrderedClient/events y publish wrapper | I: F4 |
| D: chats distintos | mismo test same_chat=False | I: PASS, ambos publican |
| D: fallo de publicación | failed_delegated_publication..., legado/turno siguiente observados | I: F4 |
| D: reset intercalado | reset_invalidates_an_already_reserved_initial_turn | I: F4, resurrección |
| D: exclusión fcntl disponible | autor test_a_second_process_cannot_hold... y M12 | R: PASS del mecanismo |
| D: ausencia fcntl | missing_fcntl... y two_processes_both_enter... pipes | A/procesos: F8 |
| Cursor/generación/fingerprint/grupo/orden/expiración publicados | pagination_consumes... seis mutaciones del sobre temporal | I: F6, filas 26–50 aceptadas |
| Expiración real sidecar | real_sidecar_expiry... | I: F6, dos llamadas/25 filas |
| «más» corto tabular | bare_more_uses_the_same... | I: F6, heuristic_only/siete llamadas |
| Seed ausente/incompleto/ajeno/válido | ui_seed_requires... | I: F6 ausente; otros controles PASS |
| Seed sin ref propiedad | reference_free_seed_never... derandomize=True | A: F6, contraejemplo mínimo |
| Actor/Radar no adquieren capacidades por payload | resultados reales publicados; delegated_contract estático | I: owner NONE indebido; no se observó cursor Actor/Radar real |
| Lookup sin capacidad tabular implícita | delegated_results_publish... ID contractual | I: F6, TABULAR con fingerprint vacío |
| Inputs estructurados | seed/API Actor | I: F3/F6 |
| R5a periodo heredado durable | author integration test_r5a... | R/I: PASS |
| R5b completitud EXPLICIT + selección | author test_r5b... | R/I: PASS |
| R5c pendiente/selección/foco/agosto | author test_r5c... (texto/ordinal) | R/I: PASS |
| R5d pendiente no ejecuta ni filtra confirmed | author test_r5d... + cancel | R/I: PASS |
| R6 origen tras recarga/default control | author test_r6... | R/I: PASS |
| R7a no seed/página desde perfil | author test_r7a... | R/I: cero llamadas PASS; continuidad F1 |
| R7b CLEAR durable | author test_r7b..., independent clear_preserves... | R/I: PASS |
| R7c cursor viejo inerte tras perfil | author test_r7c... | R/I: primer rechazo PASS; continuación F1 |
| Migración real legacy | legacy_profile_migrates_or_blocks... | I: F7 |
| Migración core spans/estado/evento | 408 core/corpus + A2 trece | R/A: PASS; no acredita lector productivo |
| Plan/Evidence/Frame/resultado/chat cruzados | cross_references... nueve alteraciones aisladas | I: F5 |
| Versión sobre desconocida y cola truncada | author publication test_an_unknown... / truncated_tail... | R/I: PASS |
| Versiones ejecución/catálogo desconocidas | cross_references... record_version/catalog_version | I: F5 |
| Sidecar huérfano tras perfil | author test_an_orphan... | R/I: PASS del rechazo inicial; no universal |
| Rollback real OFF con historial previo | rollback_does_not_reactivate... | I: F7 |
| Params/trazas antiguas no heredan en Frame íntegro | author tampering_with_legacy_params... | R/I: PASS |
| Params legacy al activar sin sobre | legacy_profile_migrates_or_blocks... | I: F7 |
| SQL enviado idéntico al plan/snapshot | published_execution_matches... seis focos | I: PASS |
| Frame exacto en render | renderer_rejects... revisión/comprador ajenos | A: F9; no alcanzabilidad ordinaria probada |
| Evidence dataset/job/SQL correcto en flujo íntegro | published_execution_matches..., author semantics | I/R: PASS; corrupción recarga F5 |
| expected_sections con consumidor | frame_sections.build_frame_sections | S + controles focos: PASS |
| not_requested_sections con consumidor explícito | rg + frame_sections/execution_record | S: sólo serializado; desviación literal por adjudicar |
| Conteo válido / monto inválido | author semantics unreadable/nonfinite + core evidence tests | R/I/A: PASS |
| Fallo parcial/total sin cero ni rescate | author semantics supplier failure/total failure | R/I: PASS |
| Scope visible buyer/source/date/topic/exclusions | author semantics, own SQL controls y clear | R/I: PASS en casos enumerados |
| CLEAR no inventa ni elimina exclusión legítima | clear_preserves_a_legitimate_topic_exclusion | I: PASS |
| Reloj y fecha relativa tras recarga | relative_clock_remains... 2024→2026 | I: PASS |
| Positivos y presupuestos seis focos | author utility 15 + own execution seis | R/I: PASS |
| Paridad externa B OFF | author adjacent + base_product_sequences + API control OFF | R/I: sólo controles enumerados |
| Todas combinaciones dual grupo/TTL/API/catalog/UI | no campaña exhaustiva adicional | NO CERTIFICADO; no extrapolar PASS |
| Mutaciones restauradas | mutation_replay.json y mutation_runtime_restoration.json | 12 DETECTED, 0 setup/timeout; runtime idéntico |
| Ruff/Semgrep/lint-core/size/diff | logs propios | PASS lint; baseline Semgrep 10, delta0 |
| Suite global | prohibida en esta revisión | NO EJECUTADA |

Prefijos abreviados de tests: `continuity` = `tests/test_v12b_independent_continuity.py`; los demás nombres propios están en `test_v12b_independent_boundaries.py`, `...publication.py`, `...execution.py` y `...base_comparison.py`. Los originales del autor mantienen sus nombres y bytes.

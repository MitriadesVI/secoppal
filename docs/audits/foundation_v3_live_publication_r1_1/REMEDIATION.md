# Foundation V3 live publication — R1.1 (remediación ZEUS NO_GO)

Auditoría: ZEUS fresco sobre BASE=efb0dfc2, CODE=a7d4941f, HEAD=7a4c59cf → NO_GO (Z1 P1, Z2 P2).

## Z1 · P1 · el fallback publicaba filas normalizadas como evidencia (reparación B)

Causa: `run_basic_fallback` consulta fuera del `ExecutionGuard`; `SecopClient.query` sólo devolvía
las filas del transporte con el guard activo y, si no, `_normalize_row` (dinero → float, `urlproceso`
None → URL sintetizada o `''`). `results` publicaba esa copia.

Remedio: `foundation_guard.raw_source_rows()` marca el bloque de consulta del fallback;
`SecopClient.query` devuelve `deepcopy` del transporte cuando `source_rows_raw()` (guard activo O
bloque del fallback). La validación de count de Foundation sigue atada sólo al guard
(`foundation_source_active`), porque el fallback no tiene grant ni plan. Las tarjetas siguen
proyectándose con `Formatter.display_rows`, igual que el camino Foundation normal.

## Z2 · P2 · el anuncio contaba tarjetas que el canal de texto no mostraba (reparación A)

Remedio: `foundation_presentation.shown_cards()` es la única autoridad de "cuántas tarjetas
muestra este canal" (todas en web, `limit` en texto); la usan `_group_lines` y el fallback.

## Oráculos nuevos (tests/test_foundation_publication_adapter.py)

- `test_basic_fallback_results_are_the_transport_rows_untouched[contratos|procesos]`:
  `results == filas del transporte` y cada campo sembrado conserva valor Y tipo
  (`'9007199254740993'`, `'1500000000'`, `urlproceso=None`).
- `test_basic_fallback_announces_the_cards_it_actually_shows[telegram|whatsapp|streamlit]`:
  el número anunciado coincide con las líneas numeradas del mensaje (streamlit: 30 anunciadas, 0 en texto).

Mutación por reversión: en un worktree en CODE con sólo los tests nuevos, 4 de 5 fallan
(los dos de Z1 y telegram/whatsapp de Z2; streamlit es control y pasa en ambos). Los dos
contraejemplos de ZEUS (`test_contract_counterexamples.py`) pasan sobre R1.1.

## Gates corridos (no la suite global)

- Entorno igualado (worktree detached en CODE, mismo intérprete): `tests/test_foundation*.py`,
  formatter display_rows, export xlsx, frozen delivery, telegram pilot/webhook,
  `tests/product_contract`, source_call_receipts → CODE 2028 passed / 0 failed;
  R1.1 2033 passed / 0 failed (+5 = oráculos nuevos).
- En el árbol principal el mismo conjunto da 25 rojos idénticos de entorno (data/ sucio y
  preexistente; product_contract y receipts), ausentes en el worktree limpio en ambos lados.
- Ruff sobre los archivos tocados: limpio. `olympus-semgrep-delta --base a7d4941f`: TOTAL=0, NEW_DELTA=0.

Pendiente: ZEUS fresco sobre R1.1 y global pareado por nodeid.

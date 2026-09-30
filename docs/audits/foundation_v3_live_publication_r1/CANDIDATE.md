# FOUNDATION_V3_LIVE_PUBLICATION_R1 — candidato congelado

| | SHA |
|---|---|
| BASE (main) | `efb0dfc2c35491cab86a54d48d3bf1bfd67f2c93` |
| CODE | `a7d4941f9f5fc66434631562ee147c3d62a82ad9` |
| HEAD | el commit que añade este directorio (sólo `docs/audits/` y `scripts/audits/`) |

Rama: `fix/foundation-v3-live-publication-r1`. Sin merge, push, deploy, restart ni
reactivación.

## Incidente

Con `SECOPPAL_FOUNDATION_MODE=foundation_v3`, las tres consultas de dogfood
(A, B y C, abajo) se veían "Sin titulo / 💰 ⚪ / Pendiente de publicación", con
un volcado de `Referencia: CO1.…` como texto. El SoQL ejecutado (recibos de la
traza) era correcto en las tres. El defecto era de publicación: `execute_accepted`
ponía las filas crudas en `rows`, que la UI y los canales leen como tarjetas.
Además, el Excel quedaba bloqueado (sin sello de universo) y "Ver más" llevaba
a una continuación que no está soportada.

## Commits

1. `a60b0858` fix(foundation): publish verified source rows through display adapter
2. `a7d4941f` feat(foundation): expose dogfood trace without changing execution

## Archivos (BASE..CODE)

```
M app/core/formatter.py                  (mapeo por fila extraído a _card; display_rows)
M app/core/foundation_basic_fallback.py  (tarjetas + un reloj por turno)
A app/core/foundation_presentation.py    (tarjetas, texto por canal, export_fields)
M app/core/foundation_runtime.py         (rows=tarjetas; results/groups=fuente)
A app/core/foundation_trace_view.py      (trace diagnóstico, lectura pura)
M streamlit_app.py                       (adiciones: Excel, aviso dual, sin "Ver más", trace)
M tests/test_entity_profile_executive_ux_boundary.py (adiciones declaradas)
A tests/test_formatter_display_rows.py
A tests/test_foundation_publication_adapter.py
M tests/test_foundation_runtime.py       (rows == fuente → rows = tarjetas alineadas)
A tests/test_foundation_trace_view.py
M tests/test_truth_layer_public_parity.py (re-pins de streamlit_app.py y formatter.py)
```

## Contrato

```
results        = filas crudas de la fuente
groups[].rows  = filas crudas de la fuente
rows           = tarjetas publicables
```

La proyección visual no muta la fuente, no consulta SECOP, no deduplica, no
reordena ni corta, conserva duplicados e identidad, y lee un solo
`reference_instant` por turno. En el caso del fallback, esa lectura ocurre dentro
del fallback, con la misma autoridad de tiempo que la búsqueda Foundation.

Exportación:
- turno de contratos → sello `contract-universe-1.1/celebrated`;
- turno de procesos → sin sello;
- turno dual → bloqueo explícito con aviso (`DUAL_EXPORT_UNSUPPORTED`), nunca
  una hoja mezclada.

Mutación: 12/12 muertas. Las mutaciones fueron: rows crudas, results = tarjetas,
sin instante, dedupe, corte, mutación de la fuente, reordenamiento, fallback con
rows crudas, fallback sin instante, fallback con volcado de IDs, export dual
permitido y texto con volcado de IDs.

## Gates

Entorno común: `SECOPPAL_FOUNDATION_MODE=legacy`, `PYTHONHASHSEED=0`,
`--hypothesis-seed=19`, ejecución en serie, la misma venv, y red no loopback
bloqueada por `scripts/audits/foundation_v3_live_publication_r1/netblock_ledger.py`.

- **Focal** (108 archivos: runtime/fallback de Foundation, formatter y display_rows,
  Streamlit, Excel, paridad de canales, recibos de fuente, primera infancia,
  Actor/Suppliers/CA y byte guards con sus tests inversos): 4422 passed,
  11 xfailed, 1 failed. El rojo es
  `test_supplier_request_authority.py::test_arbitrary_digit_boundaries_never_raise_or_silently_authorize`,
  que falla igual en BASE con la seed 19 (preexistente). Se bloquearon 30 intentos de red.
- **Global pareado**, en worktrees limpios en BASE y en CODE (`global_delta.json`):

  | | BASE | CANDIDATE |
  |---|---|---|
  | nodeids | 18619 | 18631 (+12 nuevos) |
  | passed | 18437 | 18449 |
  | failed / error | 121 / 2 | 121 / 2 |
  | red bloqueada | 77 | 77 |

  El conjunto de rojos es idéntico por nodeid (123, `shared_red_nodeids.txt`).
  `CANDIDATE_ONLY_FAILURES=0`, `NEW_ERRORS=0`, `BASE_PASSED_NOW_MISSING=0`,
  `NEW_TESTS_FAILED=0`.
- **ruff**: limpio en los archivos cambiados.
- **semgrep delta** (`--base efb0dfc2` sobre los 6 archivos de runtime de CODE): TOTAL=5,
  BASELINE=5, NEW_DELTA=0.

## Casos productivos en vivo (`live_cases.json`, sobre CODE)

| Caso | Consulta | Total | Tarjetas | Comprobaciones |
|---|---|---|---|---|
| A | contratos de primera infancia de la alcaldía de barranquilla | 1372 | 50 | tarjetas completas, tema y universo celebrado en el SoQL, Excel sellado, sin volcado de IDs, results↔rows alineados |
| B | qué licitaciones de más de 1000 millones están para presentarme en atlántico | 43 | 43 | valor visible; `precio_base > 1e9` en las 43 filas crudas; estado y fecha visibles |
| C | licitaciones de la alcaldía de barranquilla en 2025 | 2370 | 50 | tarjetas completas; valor = `format_cop(precio_base)` (2 filas con `precio_base='0'` muestran "Sin valor", igual que el legacy) |
| D | procesos y contratos de alimentación escolar en bolívar durante 2026 | 220 | 100 | dos grupos, ambos datasets en tarjetas, ambos en el texto, Excel dual bloqueado de forma explícita; en Telegram las tarjetas van en el mensaje |
| E | primera infancia (A) con el productor forzado a caer | 1372 | 50 | fallback básico rotulado, `PROVIDER_ERROR`, tarjetas completas, filas crudas preservadas |

Nota E: un primer intento con "contratos de aseo del municipio de soledad"
recibió un fallo real de SECOP (presupuesto SODA3 agotado en `jbjy-vk9h`). El
fallback lo reportó como error de fuente ("No afirmo que no existan
resultados"), sin publicar cero. E se repitió con la consulta acotada por
entidad.

## Estado preexistente

Los dos JSONL de `docs/audits/contract_universe_v1_1_*` ya estaban sucios antes
de la misión y la suite les añade bloques (`open('a')`). Se restauraron a sus
bytes pre-misión: HEAD como prefijo exacto + 5 bloques deterministas (25 y 101
líneas). El bloque de la suite, idéntico al anterior, se retiró. Esos JSONL no
entran en ningún commit.

## Fuera de alcance (mejoras separadas)

- SELECT sin `urlproceso` ni `proveedor_adjudicado`: los procesos no tienen
  enlace directo y los contratos no muestran contratista.
- Deduplicación: las filas duplicadas de la fuente se publican tal cual.
- No se tocó nada de lo siguiente: prompt v4.2, H7, universe_kind,
  eligible_first_search, O1B, modalidades, comprador, Actor/Suppliers/CA,
  held-out.

## Prompt para ZEUS (fresco, sin esta narrativa)

```
Eres ZEUS, auditor adversarial. Intenta demostrar que el candidato NO debe
mergearse. No modifiques código.

Repo: /Users/rodrigoortiz/Documents/secoppal
BASE = efb0dfc2c35491cab86a54d48d3bf1bfd67f2c93
CODE = a7d4941f9f5fc66434631562ee147c3d62a82ad9 (rama fix/foundation-v3-live-publication-r1)
Audita el diff BASE..CODE en un worktree limpio en CODE. Ignora docs/ y scripts/audits/ salvo como
material para reproducir.

Contrato declarado de un turno Foundation (SECOPPAL_FOUNDATION_MODE=foundation_v3):
  results = filas crudas; groups[].rows = filas crudas; rows = tarjetas publicables.
La proyección visual no muta la fuente, no consulta SECOP, no deduplica, no reordena, no corta,
conserva duplicados e identidad y usa un solo reference_instant por turno.
Excel: contratos → sello de universo; dual → bloqueo explícito; "Ver más" oculto en turnos
Foundation. El trace de Streamlit es lectura pura y la respuesta pública nunca muestra códigos
internos.

Busca en especial:
1. Cualquier consumidor de result["rows"] o result["results"] (Streamlit, Telegram/WhatsApp,
   export, follows/radar, conversation_store, log_trace, analytics, natural_alpha) que ahora
   reciba la forma equivocada en turnos Foundation o legacy.
2. Que Formatter.to_rows siga byte-equivalente en comportamiento (dedupe, corte, mojibake,
   una lectura de reloj), y que display_rows produzca la misma tarjeta para la misma fila.
3. Lecturas de reloj ocultas por turno, mutación de filas de evidencia, o identidad publicada
   (row_ids, result_ids en el historial) divergente de las tarjetas.
4. Los re-pins de streamlit_app.py y formatter.py en tests/test_truth_layer_public_parity.py y
   las adiciones en tests/test_entity_profile_executive_ux_boundary.py: ¿declaran exactamente
   el cambio o relajan la guarda?
5. Texto público: códigos internos, IDs crudos o afirmaciones falsas ("más recientes" sobre una
   muestra sin orden, totales exactos sobre cotas inferiores).
6. Export: ¿algún camino exporta un turno dual, o sella algo que no es universo celebrado?
Reproduce el global pareado con scripts/audits/foundation_v3_live_publication_r1/
(netblock_ledger.py + compare_ledgers.py; SECOPPAL_FOUNDATION_MODE=legacy, PYTHONHASHSEED=0,
--hypothesis-seed=19, en serie).
Veredicto: GO / GO_WITH_FIXES / NO_GO con hallazgos P0–P3, cada uno con archivo:línea,
reproducción y distinción fondo vs forma.
```

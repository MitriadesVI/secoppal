# Corpus de integridad semántica de retrieval

Benchmark reproducible del sprint que separa **recuperación** (qué trae Socrata) de
**significado** (qué pidió el usuario). Caso ancla: *"manual de contratación" (2024)*.

## Por qué existe

El enfoque actual de SECOPPAL (tokens, `acción AND %MANUALES%`) para esta consulta
recupera 139 filas de las que **solo ~6 son del tema**: precisión ~4% y candidate
recall ~6%. El significado se pierde antes del `WHERE` (token plural suelto: tira los
singulares "manual de contratación" y trae cualquier "manuales de [otra cosa]"). Este
corpus congela ese piso para que ninguna solución futura "parezca elegante" sin
superarlo de verdad.

## Archivos

- `manual_contratacion_2024.jsonl` — snapshot mínimo de filas reales etiquetadas
  (`reference, entidad, objeto, descripcion, label, match_type, reason, split`).
- `manual_contratacion_manifest.yaml` — conteos, baseline congelado, split, gates.
  Incluye `coverage_status` y `synonym_probe` (honestidad metodológica: los positivos
  son **silver**, no oro — derivados con el patrón tolerante, ⇒ validarlos con ese
  mismo patrón sería circular).

## Etiquetas

| label | match_type | qué es |
|---|---|---|
| relevant | `exact_singular` / `exact_plural` | frase literal manual(es) de contratación |
| relevant | `intercalated` | palabra intercalada ("manual **interno** de contratación") |
| relevant | `dirty_newline` | frase rota por salto de línea / puntuación en el texto crudo |
| irrelevant | `anchor_false_positive` | "manual" y "contratación" incidentales y dispersos (p.ej. IDU) |
| irrelevant | `hard_negative` | manual de **otra** cosa (funciones, cobro coactivo, técnico, manualidades) |

`intercalated` + `dirty_newline` = los que la frase exacta **literal** pierde por formato,
no por semántica. Son la prueba de que la precisión vive en la validación cliente
(normalizar + frase tolerante), no en el `WHERE` de SoQL.

## Split development / holdout

El **holdout NO se usa para diseñar `PhraseSpec`/el validador** — solo se ejecuta al
final del commit de validación (anti-sobreajuste). El generador garantiza de forma
determinista que el holdout contenga casos críticos (`dirty_newline`, `intercalated`,
`anchor_false_positive`).

## Uso

```bash
python3 scripts/build_phrase_corpus.py     # (re)genera el snapshot — toca red, NO producción
python3 scripts/exp_phrase_retrieval.py    # benchmark OFFLINE: recall + precision por split
```

Hoy el único predicado es `baseline_tokens` (rojo, ~6%/~4%). Cuando exista
`phrase_validator.py` (Commit 4) se registra como nuevo predicado y se compara contra
este piso, sin tocar red.

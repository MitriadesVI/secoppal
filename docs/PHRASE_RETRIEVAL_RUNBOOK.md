# Runbook: extender el catálogo de frases (phrase-aware retrieval)

**Pregunta que responde:** ¿cómo agrego mañana `mantenimiento preventivo`,
`planta de tratamiento de aguas residuales` o `centro de bienestar` sin romper nada?

Contexto y reglas: ADR-036 y `docs/SEMANTIC_RETRIEVAL_SPRINT_2026-06.md`.

---

## ¿PhraseSpec o no? (decidir primero)

`PhraseSpec` resuelve **detección y preservación de una unidad literal**. NO resuelve sinónimos,
siglas, polisemia ni categorías amplias — eso es enriquecimiento curado y vive en otra capa.

| Caso | Herramienta |
|---|---|
| `aire acondicionado`, `manual de contratación` | **PhraseSpec** (frase literal con anclas) |
| `PAE → alimentación escolar` (sigla→concepto) | nodo de dominio / alias semántico (NO PhraseSpec) |
| `dotación` (uniformes vs equipos) | **polisemia** (`polysemic_topics`), decide sentido primero |
| `infraestructura` (categoría amplia) | NO es frase literal — no forzar PhraseSpec |

Regla: una palabra que normalmente es stopword/estructural NO se elimina si forma parte de una
frase contractual detectada (p.ej. "contratación" dentro de "manual de contratación").

---

## Procedimiento (no saltarse pasos)

1. **Caso real primero.** Tomar una consulta real (de feedback/dogfooding), no inventada. Añadir
   sus filas etiquetadas al corpus de **development** (nunca al holdout).
2. **Sonda de sinónimos (offline).** Buscar el concepto bajo otros nombres SIN la palabra ancla. Si
   aparecen muchos (p.ej. PAE para "alimentación escolar"), el caso necesita nodo de dominio, no
   solo PhraseSpec. Si ~0 (como "manual de contratación"), el léxico basta.
3. **Definir el `PhraseSpec`** en `app/core/phrase_catalog.py` (formas normalizadas: minúsculas,
   sin tildes):
   ```python
   PhraseSpec(
       canonical="aire acondicionado",
       ordered_anchors=("aire", "acondicionado"),
       variants=("aires acondicionados",),
       max_gap_tokens=1,   # tokens intermedios permitidos entre anclas
   )
   ```
4. **Confirmar ownership** frente a `KNOWN_BIGRAMS` (`tests/test_phrase_catalog_collisions.py`):
   el catálogo debe detectar y consumir la frase. Si ya está en `KNOWN_BIGRAMS`, el catálogo tiene
   precedencia por orden de ejecución — verificar que no haya doble procesamiento.
5. **Probar detección antes del scrub** — `detect_phrases("…")` produce `objeto_phrases=[canonical]`
   y NO deja las anclas como tokens de `objeto`.
6. **Probar singular/plural y texto sucio** — `MAYÚSCULAS`, `\n`, puntuación, palabra intercalada.
7. **Probar el builder recall-first** — anclas co-ocurrentes en el MISMO campo, `LIMIT 200`, sin
   `WHERE 1=1`.
8. **Probar el validador** — `direct` para la frase y variantes; `none` cuando las anclas caen en
   campos distintos.
9. **Probar el ranking** — `direct > probable > weak`, BM25 solo desempata dentro del nivel.
10. **Suite completa** verde (`make test` o `.venv/bin/python -m pytest tests/ -q`).
11. **Smoke live** — construir el SoQL con el nuevo `objeto_phrases`, fetch real, confirmar que
    referencias conocidas entran y validan `direct`.
12. **NO mirar el holdout** durante el desarrollo. Solo al cerrar, una vez, para reportar.
13. **Registrar métricas** separadas: candidate recall, validation recall, Precision@25.

---

## Qué NO hacer (crímenes que el ADR-036 previene)

- Volver a un único `limit` para consulta y pantalla (`LIMIT 50` mataba 52/100 positivos).
- Mover el ranking dentro del formatter (el BM25 ahí no llega a pantalla).
- Borrar los `none` (son no-confirmados, no irrelevantes; quedan en cuarentena).
- Añadir un `if "frase concreta" in query` (rompe la generalidad; usar datos en el catálogo).
- Cruzar anclas entre `objeto` y `descripción`.
- Meter semántica de dominio (siglas/sinónimos) en `morphological_variants.py`.
- Corregir el holdout v1 mirándolo.

---

## Mapa de archivos

| Pieza | Archivo |
|---|---|
| Contrato de frase | `app/core/phrase_spec.py` |
| Normalización pura | `app/core/retrieval_text.py` |
| Primitivos de matching | `app/core/phrase_match.py` |
| Catálogo + detección | `app/core/phrase_catalog.py` |
| WHERE recall-first | `app/core/soql_builder.py` (`_phrase_recall_conditions`) |
| candidate_limit | `app/core/workflow/actions/planning.py` (`_candidate_limit`) |
| Validador de filas | `app/core/phrase_validator.py` |
| Ranking + cuarentena | `app/core/phrase_ranking.py` |
| Etapa formal de ranking | `app/core/workflow/actions/ranking.py` (`rank_results`) |
| Header/trace honestos | `app/core/phrase_presentation.py` |
| Benchmark + corpus | `scripts/exp_phrase_retrieval.py`, `tests/fixtures/phrase_retrieval/` |

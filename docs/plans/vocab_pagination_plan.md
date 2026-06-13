# Plan VOCAB-PAGE: comprensión de objeto (verbos) + paginación estable

**Estado:** Propuesto · listo para ejecución
**Origen:** queries reales calificadas 👎 el 2026-06-03 (casos "construir parques",
"soy constructor de parques", paginación que repite).
**Referencias:** [morphological_variants.py](../../app/data/morphological_variants.py),
[pagination.py](../../app/core/workflow/handlers/pagination.py),
[formatter.py](../../app/core/formatter.py)

---

## 0. Reparto por complejidad (LEER PRIMERO)

Este plan tiene **tres tracks independientes**, deliberadamente separados porque
NO tienen el mismo riesgo. **No ejecutar como un solo sprint.**

| Track | Qué | Complejidad | Modelo recomendado |
|---|---|---|---|
| **T1 — Lematización** | verbos/agentes → raíz nominal en el diccionario | Baja (datos) | **DeepSeek Pro** (apto) |
| **T2 — Paginación** | offset basado en lo realmente mostrado | Media (estado) | **Opus / Codex 5.5 high** |
| **T3 — Demote AND→OR** | aflojar el AND de objeto multi-token | Alta (global) | **Opus / Codex** · y **DIFERIR** hasta medir T1 |

**Secuencia:** T1 primero (barato, alto impacto). Luego **medir** si el AND sigue
matando recall — porque T1 lo alivia parcialmente (`construir parques` pasa de
`construir ∧ parques` a `construccion ∧ parques`, mucho más sano). Solo si tras T1
el recall sigue mal, abrir T3. T2 es independiente, se puede hacer en paralelo.

---

# TRACK 1 — Lematización verbo→sustantivo  (DeepSeek Pro)

## T1.1 Diagnóstico (verificado)

`MORPHOLOGICAL_VARIANTS` mapea **superficie→raíz** ([morphological_variants.py:24](../../app/data/morphological_variants.py)).
El parser aplica `VARIANT_TO_ROOT.get(surface, surface)` al extraer objeto
([query_router.py:1136,1147](../../app/core/query_router.py)). Hoy el dict cubre
sustantivos (`construccion`, `pavimentacion`…) **pero NO los verbos ni agentes**:

- `"construir"` → no está → SoQL busca `'%construir%'` → **no matchea** "CONSTRUCCIÓN DE PARQUES".
- `"constructor"` → ídem.

Como el contrato real dice "construcción", no "construir", la query trae 0–5
resultados viejos. **El fix es añadir entradas verbo/agente → raíz nominal.**
Confirmado que añadir al dict basta: el parser ya consulta `VARIANT_TO_ROOT`.

## T1.2 Etapas y entregables

### Etapa 0 — Caracterización (red de seguridad)
- `tests/test_verb_lemmatization_characterization.py`: para una lista de queries
  (`"construir parques"`, `"remodelar colegios"`, `"pavimentar vias"`, …) snapshotea
  hoy el `objeto` que produce `QueryRouter().parse(q).params`. Congela el baseline.
- **Criterio:** verde sobre el código actual.

### Etapa 1 — Añadir entradas verbo/agente → raíz nominal
- En `MORPHOLOGICAL_VARIANTS`, añadir mapeos. Lista **curada y verificada contra
  valores reales SECOP** (no inventar formas que no aparezcan en los datos):

  | Verbo / agente (superficie) | Raíz nominal (valor) |
  |---|---|
  | construir, construyendo, constructor, constructores | construccion |
  | remodelar, remodelando | remodelacion |
  | pavimentar, pavimentando | pavimentacion |
  | mejorar, mejorando | mejoramiento |
  | adecuar, adecuando | adecuacion |
  | suministrar, suministrando, proveer | suministro |
  | dotar, dotando | dotacion |
  | mantener, manteniendo | mantenimiento |
  | reparar, reparando | reparacion |
  | rehabilitar, rehabilitando | rehabilitacion |
  | instalar, instalando | instalacion |
  | demoler, demoliendo | demolicion |
  | señalizar | señalizacion |
  | interventor, interventores | interventoria |
  | consultor, consultores | consultoria |

  (El ejecutor PUEDE extender la lista con verbos del mismo patrón **solo si la
  raíz nominal ya existe como valor en el dict** o aparece literalmente en SECOP.)

- **Regla dura:** la raíz (valor) debe ser un término que **realmente aparezca en
  los objetos de SECOP**. Si dudas, no lo agregues. NO inventar roots.

### Etapa 2 — Verificación
- `tests/test_verb_lemmatization.py`: assert que `parse("construir parques").params["objeto"]`
  contiene `"construccion"` (no `"construir"`), idem para cada verbo de la tabla.
- **Verificación E2E real** (con red): correr una probe que confirme que
  `"construir parques"` ahora trae resultados de construcción de parques. Usar el
  patrón de `scripts/exp_recall_rerank.py` (parser → SoQLBuilder → SecopClient).
- **No-regresión:** `pytest -q` (baseline 1306 passed) · `run_query_corpus.py --mode all`
  (89/89) · `eval_rated.py` no baja · `make lint-core` limpio.

## T1.3 Guardrails (para DeepSeek)
1. **SOLO** añadir claves a `MORPHOLOGICAL_VARIANTS`. NO tocar `query_router.py`,
   NO tocar la estructura del dict ni las constantes derivadas (`VARIANT_TO_ROOT`,
   `KNOWN_BIGRAMS` se construyen solas).
2. **No inventar roots.** Cada valor debe existir ya en el dict o en SECOP real.
3. Claves en minúscula, sin acentos (igual que las existentes).
4. Cada entrada nueva = un caso en el test de Etapa 2.
5. Commit atómico, suite verde antes de cerrar. Si algo no relacionado se rompe,
   detente y reporta.

---

# TRACK 2 — Paginación estable  (Opus / Codex)

## T2.1 Diagnóstico (verificado)

[pagination.py:56-58](../../app/core/workflow/handlers/pagination.py):
```python
prev_offset = int(prev_params.get("offset", 0))
prev_limit  = int(prev_params.get("limit", 10))
new_offset  = prev_offset + prev_limit
```
El `new_offset` se calcula con el **`limit` de params** (p.ej. 10), pero el primer
turno **mostró 25** resultados (display ≠ limit-params). Resultado real observado:
turno 1 muestra 1–25; "más resultados" hace offset=10 → muestra 11–20 (**ya vistos**);
"muestrame mas" offset=20 → 21–30 (**ya vistos**). **Duplicados entre páginas.**

Causa raíz: hay **tres números distintos** que hoy se confunden:
1. `limit` del SoQL (cuántas filas pide a SECOP),
2. cuántas quedan tras `_dedupe_results` + `_reorder_active_first` ([formatter.py:94,130](../../app/core/formatter.py)),
3. cuántas se **muestran** (display).
El offset de la siguiente página debe basarse en **(3) lo realmente mostrado**, no en (1).

## T2.2 Etapas

### Etapa 0 — Caracterización multi-turno
- Test que reproduce la secuencia: consulta inicial → "más resultados" → "muestrame mas",
  y **afirma el bug actual** (hay solapamiento de ids entre páginas). Congela el
  comportamiento roto como referencia para probar que se arregla.

### Etapa 1 — Hacer el offset consistente con el display
- Decisión de diseño (elegir la conservadora): que el primer turno **persista en
  `parsed_params` el `limit` realmente mostrado** (o que la paginación derive el
  offset del nº de filas únicas ya mostradas, no de `limit`-params). Reordenar/
  dedupe deben ocurrir **antes** de decidir el corte de página, no después.
- Mantener `_reorder_active_first` y `_dedupe` pero garantizar que la frontera de
  página sea estable entre turnos (sin re-reordenar lo ya entregado).

### Etapa 2 — Verificación
- El test de Etapa 0 ahora afirma **cero solapamiento** de ids entre páginas
  consecutivas, y que no se saltan resultados.
- No-regresión: suite + corpus + lint.

## T2.3 Guardrails
- No cambiar el orden visible dentro de una página respecto a hoy (salvo el dedup).
- Cuidado con la interacción `offset` ↔ `_reorder_active_first`: el reorden NO debe
  reintroducir en la página N algo ya mostrado en la N-1.
- Caso límite: primer turno con `limit=25`, paginación con `limit=10` — documentar
  cuál gana y por qué.

---

# TRACK 3 — Demote AND→OR de objeto  (Opus / Codex · DIFERIDO)

## T3.1 Diagnóstico

[soql_builder.py:427-429](../../app/core/soql_builder.py): `_should_demote_objeto`
exige **≥4 términos** para pasar de AND a OR. Con 2 términos (`construir`∧`parques`,
`dotacion`∧`hospitales`) queda AND → a menudo 0 resultados. El AND de un término
raro + uno común mata el recall.

## T3.2 Por qué DIFERIR (no abrir aún)

1. **T1 lo alivia.** Con lematización, `construir parques` → `construccion ∧ parques`
   (dos términos buenos), mucho más tolerable. Hay que **medir** si el problema
   persiste tras T1 antes de tocar esto.
2. **Riesgo global.** Cambiar el umbral AND/OR afecta TODA query multi-token. Bajar
   a ≥2 ciego volvería `mantenimiento vial` en OR → ruido. `test_multi_and_demotion`
   ya congela comportamiento aquí; tocarlo sin caracterización fuerte es regresión.

## T3.3 Si se abre (tras medir)
- Caracterización amplia: snapshot del nº de resultados y precision@10 para 30+
  queries multi-token reales (usar `exp_recall_rerank.py`) **antes** de tocar.
- Heurística candidata (a evaluar, no asumir): demote a OR solo cuando un término es
  raro (∉ `COMMON_TOKENS`) y otro común, o cuando un probe del AND da 0. Medir
  precision@10 antes/después; aceptar solo si sube sin romper los casos congelados.
- Guardrail: ningún caso de `test_multi_and_demotion` puede regresar sin decisión
  explícita documentada.

---

## Resumen de ejecución

1. **DeepSeek Pro → Track 1** (lematización). Barato, datos, alto impacto, bajo riesgo.
2. **Medir** con el harness si el AND sigue matando recall tras T1.
3. **Opus / Codex → Track 2** (paginación) en paralelo; **Track 3** solo si el paso 2
   lo justifica.

Definición de hecho global: las queries 👎 de hoy ("construir parques",
"remodelar X", paginación repetida) dejan de fallar, verificado con el harness y
sin regresión en suite/corpus/eval.

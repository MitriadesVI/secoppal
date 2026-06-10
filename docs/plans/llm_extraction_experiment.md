# Experimento: extracción de params HEURÍSTICO vs LLM-first

**Fecha:** 2026-06-04
**Estado:** harness listo · smoke validado · pendiente corrida de 60 queries
**Decisión que responde:** ¿el cuello del 26% E2E es *comprensión* (→ pivote a
LLM-first) o es *ranking/datos* (→ seguir con heurística + arreglar fugas)?

---

## 1. Pregunta y diseño

¿Extraer params con un LLM bien contextualizado sube **precision@10 E2E** frente
al parser heurístico, sobre queries reales? Diseño A/B — **solo cambia el extractor**:

```
query real ──┬─ A (control):    QueryRouter heurístico ──┐
             └─ B (tratamiento): LLM + contexto robusto ──┘
                                                          ▼
             ambos → compute_query_plan (validators/invariantes de QP-INV)
                   → SoQLBuilder → SecopClient → RelevanceScorer (BM25)
                   → precision@10  +  0-results  +  intent preservado  +  latencia
```

El output del LLM **pasa por los mismos validators** (`contract_invariants` vía
`compute_query_plan`) que el heurístico: el LLM **propone**, los validators
**gobiernan**. El experimento también mide cuántas veces los guardrails corrigen al LLM.

---

## 2. Archivos

### Creados
| Archivo | Qué es |
|---|---|
| [`scripts/exp_llm_extraction.py`](../../scripts/exp_llm_extraction.py) | **El harness A/B.** System prompt robustecido + extractor LLM + extractor heurístico + métrica. ~340 LOC. |
| [`scripts/exp_recall_rerank.py`](../../scripts/exp_recall_rerank.py) | Harness antecedente (fugas ranking/ciudad). Reusa el patrón de fetch/BM25/precision. |

### Modificados
**Ninguno de producción.** El experimento es aislado (`git status app/` vacío). Por diseño: mide, no implementa.

### Leídos / reutilizados (no modificados)
- `app/core/llm_handler.py` — se reusa `SECOPPAL_TOOLS` (tool schema con enums de estados).
- `app/core/query_plan.py` — `compute_query_plan` (los validators, incl. P10–P13 de QP-INV).
- `app/core/query_router.py` — `QueryRouter().parse` (rama A heurística).
- `app/core/relevance.py` — `RelevanceScorer` (BM25, igual para ambas ramas).
- `app/core/soql_builder.py` · `app/core/secop_client.py` — construcción + fetch (idénticos A/B).
- `app/config.py` — key DeepSeek (`DEEPSEEK_API_KEY`).

---

## 3. Ingeniería de contexto (el corazón)

El system prompt robustecido (`ROBUST_SYSTEM_PROMPT` en el script) se ensambla del
conocimiento de dominio ya consolidado:
- **Invariantes jurídicas** (de `contract_invariants`): firmado→contratos sin estado, etc.
- **Regla de intent reforzada**: señales de oportunidad → siempre `opportunity_search`.
- **Vocabulario de dominio**: `construir→construccion`, `adulto mayor→[ancianos, centros día]`,
  `basuras→aseo`, "soy/vendo X"→objeto=X. Ataca la *clase* de bug, no el caso.
- **Few-shot**: ejemplos reales query→params (construir parques, soy constructor,
  adulto mayor actualmente, firmado en ejecución, negación monetaria).

Va en **SYSTEM** (estable, cacheable). El user message es solo la query.

---

## 4. Cómo correr

```bash
# smoke (3 queries, valida plumbing — ya ejecutado ✓)
.venv/bin/python scripts/exp_llm_extraction.py --smoke --model deepseek-v4-pro

# corrida real (60 queries de tópico desde data/secoppal.db)
.venv/bin/python scripts/exp_llm_extraction.py --max 60 --model deepseek-v4-pro
.venv/bin/python scripts/exp_llm_extraction.py --max 60 --model deepseek-v4-flash  # comparar velocidad
```

**Setup técnico:**
- Modelo: `deepseek-v4-pro` (confirmado vía API; `deepseek-v4-flash` = alternativa rápida).
  ⚠️ El config de PRODUCCIÓN (`config.py`) sigue en `deepseek-chat` (alias viejo) — actualizar aparte.
- `temperature=0` (correcto para extracción determinista; no se baja más).
- Requiere `DEEPSEEK_API_KEY` en `.env` (presente) + red a `datos.gov.co`.

---

## 5. Hallazgos del smoke (3 queries)

| Query | A (heur) | B (llm) | Nota |
|---|---|---|---|
| "alguna oportunidad para construir parques" | 0.20 | 0.00* | A trae procesos viejos cerrados; B filtra a abiertos → 0 (más correcto) |
| "soy constructor de parques" | 1.00 | 1.00 | B normaliza `constructor→construccion` |
| "adulto mayor actualmente" | 1.00 | 1.00 | ambos OK; B preserva intent tras refuerzo |

- ✅ **Lematización**: B resuelve `construir→construccion` sin parche por verbo (la clase que frustraba).
- ✅ **Intent reforzado**: B preserva `opportunity_search` 3/3 (antes 1/3).
- ⚠️ **La métrica engaña**: `precision@10` sola castiga a B cuando filtra correctamente
  a oportunidades abiertas y no hay → 0. A "gana" trayendo basura vieja.
- **Latencia v4-pro: ~5s** (alto para UX; v4-flash sería más rápido).

---

## 6. Cómo leer la corrida de 60 (NO solo el promedio)

Decisión con **cinco señales juntas**, no con el número solo:
1. **precision@10 A vs B** — relevancia de lo que trae.
2. **0-results A vs B** — ¿B da 0 por filtrar bien o por romper la query? (mirar el dump).
3. **intent preservado** (columna `int`, ✓/✗) — ¿B marca opportunity cuando toca? (ojo: el
   LLM es inconsistente aquí incluso a temp=0 — el smoke dio ✓✓ y ✓✗ para la MISMA query).
4. **dump de params lado a lado** — dónde gana B (lematización, sinónimos), dónde pierde (scope, intent).
5. **costo + latencia** (columnas `tok(p/c)`, `$`, `lat` + agregado) — viabilidad en producción.

### Medición de costo (tokens reales)
El harness captura `usage` de cada respuesta: prompt/completion tokens + cache-hit/miss.
Reporta $/query y proyección a 5K queries/día. **Smoke (v4-pro):** ~1676 prompt + 357
completion, **cache-hit 99%** (el system prompt se cachea casi instantáneo), **~$0.0009/query
→ ~$136/mes a 5K/día** (tarifa `PRICING` placeholder — AJUSTAR a la real). **El costo no es
el obstáculo; la latencia sí** (~7.6s media en v4-pro → comparar con `v4-flash`).
Los tokens son reales; el USD solo es correcto con la tarifa real en `PRICING`.

| Resultado | Lectura | Acción |
|---|---|---|
| B ≫ A en precisión, latencia/costo OK | comprensión ES el cuello | **pivote a LLM-first** |
| B ≈ A | el cuello es ranking/datos | enfocar fugas, no tocar el parser |
| B alucina pero validators atrapan | arquitectura "LLM propone, validators gobiernan" funciona | LLM-first viable con red |

---

## 7. Contexto (de dónde viene)

- El gap parser-88% / E2E-26% motivó la pregunta (ver `memory/project_secoppal_arquitectura_y_eval.md`).
- QP-INV (ADR-024) construyó los validators que hacen seguro soltar un LLM → este experimento los reusa.
- Las fugas A/B medidas (`exp_recall_rerank.py`, +0.21 p@10) son el camino alterno si B ≈ A.

# SECOPPAL — Plan de Trabajo: Eliminar LLM, Fortalecer Heurística

> **Para Hermes:** Implementar task por task. Cada task es independiente y testeable.

**Fecha:** 2026-04-15
**Estado:** En progreso

---

## Checkpoint actual

- Git: `66e6242` — stopwords fix, 30/30 tests, README actualizado
- Feedback: 11 queries, 0 usaron LLM, 5/7 rated negativas (71% fallo)
- Accuracy benchmark: 95.5% (53 queries) con V2 router + V3 entity resolver
- DeepSeek: 0% de uso real → candidato a eliminación

## Diagnóstico

### Fallos detectados en producción (feedback.jsonl)

| # | Query | Fallo | Causa raíz |
|---|-------|-------|------------|
| 3-4 | "gobernacion de santander 2024" | Resolvió a GOBERNACION DE CALDAS | Entity scan pierde "de santander" — año lo corta |
| 5 | "ccontratos por pretacion de servid=cios..." | 0 resultados | Typos pasan como objeto sin corrección |
| 7-8 | "alimentacion actualmente?" / "mantenimiento ahora?" | 0 resultados | Stopwords faltantes (YA ARREGLADO) |

### Problema estructural del LLM

El LLM (DeepSeek) está configurado como fallback pero NUNCA se activa porque:
1. La heurística siempre extrae *algo* (aunque sea incorrecto)
2. `_needs_llm()` solo retorna True cuando no hay señales O hay señales avanzadas ("top", "ranking")
3. No hay mecanismo de "confianza baja" que active el LLM como validador

**Decisión: ELIMINAR el LLM del pipeline.** Razones:
- 0% de uso en producción real
- Los fallos actuales son de la heurística, no por falta de LLM
- Simplifica el stack (sin API key, sin latencia, sin costo)
- Mantener la interfaz `LLMHandler` por si se necesita en el futuro

---

## Plan de trabajo (7 tasks)

### Task 1: Corrección de typos en queries del usuario
**Prioridad:** Alta — causa 100% de fallo en query con typos

**Objetivo:** Agregar paso de corrección ortográfica ANTES del parsing, usando
distancia de edición contra vocabulario conocido de contratación pública.

**Archivos:**
- Crear: `app/utils/spell_correction.py`
- Crear: `app/data/vocabulary.py` — vocabulario de contratación pública
- Modificar: `app/core/query_router v2.py` — integrar corrección pre-parse
- Crear: `tests/test_spell_correction.py`

**Vocabulario semilla:**
```
prestacion, servicios, profesionales, contrato, contratos, licitacion,
alcaldia, gobernacion, construccion, mantenimiento, pavimentacion,
alimentacion, suministro, interventoria, consultoria, obra, transporte,
infraestructura, educacion, salud, vivienda, acueducto, saneamiento
```

**Enfoque:** SymSpell o fuzzy simple contra vocabulario fijo. NO usar LLM.
Corrección conservadora: solo corregir si distancia <= 2 y hay match > 85%.

**Test clave:**
```python
def test_corrects_common_typos():
    assert correct("pretacion") == "prestacion"
    assert correct("servid=cios") == "servicios"
    assert correct("ccontratos") == "contratos"
    assert correct("alcladia") == "alcaldia"
    # No debe corregir palabras ya correctas
    assert correct("construccion") == "construccion"
    # No debe corregir nombres propios
    assert correct("valledupar") == "valledupar"
```

---

### Task 2: Fix entity scan cuando año corta la captura
**Prioridad:** Alta — causa fallo en "gobernacion de santander 2024"

**Objetivo:** El año `2024` se limpia del scrubbed text ANTES del entity scan,
pero deja espacios múltiples que rompen el matching. Además, "de santander"
queda separado de "gobernacion" después de limpiar el año.

**Archivos:**
- Modificar: `app/core/query_router v2.py` — asegurar que year stripping
  no rompa entity scan (reordenar pasos o normalizar espacios)
- Modificar: `tests/test_query_router.py` — test de regresión

**Test clave:**
```python
def test_gobernacion_santander_with_year():
    parsed = router.parse("contratos mas caros de la gobernacion de santander 2024")
    resolved = parsed.params.get("entidad_resolved", "").lower()
    assert "santander" in resolved
    assert "caldas" not in resolved
```

---

### Task 3: Eliminar LLM del pipeline de producción
**Prioridad:** Media — simplificación, cero impacto funcional

**Objetivo:** Remover la dependencia de DeepSeek del flujo principal.
Mantener `llm_handler.py` como módulo dormido (no borrar).

**Archivos:**
- Modificar: `app/core/orchestrator v2.py` — skip paso `llm_parse` siempre
- Modificar: `app/core/query_router v2.py` — `_needs_llm()` siempre retorna False
- Modificar: `tests/test_query_router.py` — actualizar tests de needs_llm
- NO borrar: `app/core/llm_handler.py` — queda como módulo inactivo

**Test clave:**
```python
def test_needs_llm_always_false():
    # Incluso queries que antes activaban el LLM
    parsed = router.parse("top 5 contratos más caros de Colombia")
    assert parsed.needs_llm is False
```

---

### Task 4: Mejorar stopwords — meses y palabras temporales
**Prioridad:** Media — la query 5 tiene "abril" como objeto

**Objetivo:** Agregar meses del año y más palabras temporales a STOPWORDS.

**Archivos:**
- Modificar: `app/core/query_router v2.py` — expandir STOPWORDS
- Modificar: `tests/test_query_router.py` — tests de regresión

**Stopwords a agregar:**
```python
# Meses
"enero", "febrero", "marzo", "abril", "mayo", "junio",
"julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
# Más temporales
"semestre", "trimestre", "bimestre", "periodo", "anual", "mensual",
"pasado", "anterior", "siguiente", "proximo",
```

---

### Task 5: Detección de meses como fechas
**Prioridad:** Media — "del mes de abril de 2026" debería generar fecha_desde/hasta

**Objetivo:** Extraer "mes de abril de 2026" → fecha_desde=2026-04-01, fecha_hasta=2026-04-30.

**Archivos:**
- Modificar: `app/core/query_router v2.py` — nuevo regex en `_extract_dates()`
- Modificar: `tests/test_query_router.py`

**Test clave:**
```python
def test_month_year_extraction():
    parsed = router.parse("contratos de abril de 2026")
    assert parsed.params["fecha_desde"] == "2026-04-01"
    assert parsed.params["fecha_hasta"] == "2026-04-30"
```

---

### Task 6: Confidence score y fallback a búsqueda amplia
**Prioridad:** Media — reduce 0-resultados

**Objetivo:** Cuando la query tiene baja confianza (muchos tokens desconocidos,
entidad no resuelta, etc.), relajar los filtros en vez de fallar silenciosamente.
Ej: si objeto tiene 4+ terms y da 0 resultados, reintentar con solo los 2 primeros.

**Archivos:**
- Modificar: `app/core/orchestrator v2.py` — lógica de retry con filtros relajados
- Crear: `tests/test_retry_logic.py`

---

### Task 7: Actualizar README y ADR
**Prioridad:** Baja — documentación

**Archivos:**
- Modificar: `README.md` — documentar eliminación del LLM, corrección de typos, meses
- Agregar: ADR-008 — Decisión de eliminar LLM del pipeline

---

## Orden de implementación

```
Task 1 (typos)  ─┐
Task 2 (entity)  ─┼─ Paralelos, independientes
Task 4 (months)  ─┘
       │
Task 5 (month→date) ─ Depende de Task 4
       │
Task 3 (kill LLM) ─── Después de 1+2 (confirmar que heurística es suficiente)
       │
Task 6 (retry) ─────── Último feature
       │
Task 7 (docs) ──────── Final
```

## Criterio de éxito

- 30/30 tests existentes siguen pasando
- Nuevos tests para cada task
- Las 5 queries negativas del feedback deben funcionar correctamente
- 0 dependencia de API externa para el flujo principal
- accuracy_test.py >= 95.5% (no regresión)

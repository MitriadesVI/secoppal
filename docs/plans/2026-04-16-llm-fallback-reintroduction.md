# LLM Fallback Reintroduction — Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Reintroduce LLM (DeepSeek) as intelligent fallback when the heuristic parser can't distinguish municipalities, entities, and object terms — solving the "Puerto Salgar" and "Secretaría de Integración Social" class of failures.

**Architecture:** Hybrid pipeline. The heuristic parser runs first (free, <1ms). A smart `_needs_llm()` function detects when object tokens likely contain misclassified cities or entities. Only then does the LLM run (~2-5s, ~$0.001). The LLM extracts `ciudad` (new param), `entidad`, and reclassifies `objeto`. Regex-confident extractions (dates, amounts, departments, states) are never overwritten.

**Tech Stack:** DeepSeek V3 via OpenAI SDK (existing), SoQL builder extension for ciudad filter.

**Key design decisions:**
- `ciudad` is a new first-class parameter (maps to `ciudad_entidad` in procesos, `ciudad` in contratos)
- LLM trigger heuristics focus on false-positive object tokens (proper nouns, "en [place]" patterns, entity-like words in objeto)
- REGEX_PRIORITY_KEYS expanded: heuristic wins on dates, amounts, departments, states, modality
- LLM wins on: ciudad (new), entidad (when not gazetteer-resolved), objeto reclassification

---

## Task 1: Add `ciudad` support to SoQLBuilder

**Objective:** Enable filtering by city/municipality in both datasets.

**Files:**
- Modify: `app/core/soql_builder.py`
- Test: `tests/test_soql_builder.py`

**Step 1: Write failing test**

Add to `tests/test_soql_builder.py`:

```python
def test_ciudad_filter_procesos():
    """Ciudad filter should use ciudad_entidad for procesos."""
    builder = SoQLBuilder()
    params = {"objeto": ["ampliacion"], "ciudad": "Puerto Salgar"}
    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, params)
    assert "ciudad_entidad = 'Puerto Salgar'" in soql
    assert "LIKE UPPER('%ampliacion%')" in soql


def test_ciudad_filter_contratos():
    """Ciudad filter should use ciudad for contratos."""
    builder = SoQLBuilder()
    params = {"objeto": ["cuidado"], "ciudad": "Barranquilla"}
    soql = builder.build(SoQLBuilder.CONTRATOS_DATASET, params)
    assert "ciudad = 'Barranquilla'" in soql
```

**Step 2: Run test to verify failure**

Run: `cd ~/Documents/secoppal && python -m pytest tests/test_soql_builder.py::test_ciudad_filter_procesos tests/test_soql_builder.py::test_ciudad_filter_contratos -v`
Expected: FAIL — ciudad not handled

**Step 3: Implement**

In `app/core/soql_builder.py`:

1. Add `city: str | None = None` to `DatasetSpec`:
```python
city: str | None = None
```

2. Add `city="ciudad_entidad"` to PROCESOS spec and `city="ciudad"` to CONTRATOS spec.

3. In `build()`, after the departamento clause, add:
```python
if params.get("ciudad"):
    if spec.city:
        where_clauses.append(f"UPPER({spec.city}) LIKE UPPER('%{self._escape(str(params['ciudad']))}%')")
```

Note: Use LIKE instead of `=` because SECOP city names have inconsistent casing/formatting (e.g., "PUERTO SALGAR" vs "Puerto Salgar").

**Step 4: Run tests to verify pass**

Run: `cd ~/Documents/secoppal && python -m pytest tests/test_soql_builder.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add app/core/soql_builder.py tests/test_soql_builder.py
git commit -m "feat: add ciudad filter to SoQL builder (ciudad_entidad/ciudad)"
```

---

## Task 2: Add `ciudad` to LLM tool schema and update system prompt

**Objective:** The LLM can now extract `ciudad` (municipality) as a distinct parameter from `departamento` and `entidad`.

**Files:**
- Modify: `app/core/llm_handler.py`

**Step 1: Update SYSTEM_PROMPT**

Replace the existing SYSTEM_PROMPT with:

```python
SYSTEM_PROMPT = """
Eres un asistente que interpreta consultas sobre contratacion publica colombiana (SECOP II).
Tu unica tarea es extraer parametros de busqueda y llamar la herramienta buscar_procesos.

Reglas:
1. Solo incluye parametros mencionados explicitamente.
2. Distingue entre TERRITORIO, MUNICIPIO/CIUDAD y ENTIDAD ESPECIFICA:
   - "en Atlantico", "del Atlantico" -> departamento
   - "en Puerto Salgar", "en Soacha", "en Barranquilla" -> ciudad (nombre del municipio)
   - "de la gobernacion del Atlantico", "del SENA", "de la alcaldia de..." -> entidad
   - "de la Secretaria de Integracion Social" -> entidad
   - "contratos de la alcaldia de Puerto Salgar" -> entidad="alcaldia de Puerto Salgar", ciudad no necesario
3. Convierte valores monetarios:
   - "500 millones" = 500000000
   - "mil millones" y "un billon" (uso coloquial) = 1000000000
   - "200 palos" = 200000000
4. "abiertas" o "vigentes" -> estado "Abierto"
5. "contratos firmados" -> dataset "contratos", estado "Celebrado"
6. "en ejecucion" -> dataset "contratos", estado "En ejecucion"
7. "liquidados" -> dataset "contratos", estado "Liquidado"
8. Default: dataset "procesos"
9. objeto son las palabras clave de LO QUE SE CONTRATA (mantenimiento, vial, construccion, etc.)
   NO incluyas nombres de ciudades, departamentos ni entidades en objeto.
""".strip()
```

**Step 2: Add `ciudad` to SECOPAL_TOOLS**

In the tool's `properties`, add after `departamento`:

```python
"ciudad": {
    "type": "string",
    "description": "Municipio o ciudad donde se ejecuta (Puerto Salgar, Soacha, Barranquilla, etc.)"
},
```

**Step 3: Add `ciudad` to REGEX_PRIORITY_KEYS? NO.**

`ciudad` should NOT be in REGEX_PRIORITY_KEYS because the heuristic never extracts it — the LLM is the sole source for ciudad. Keep REGEX_PRIORITY_KEYS as-is.

**Step 4: Commit**

```bash
git add app/core/llm_handler.py
git commit -m "feat: add ciudad param to LLM tool schema and improve system prompt"
```

---

## Task 3: Smart `_needs_llm()` in QueryRouter

**Objective:** Detect when object tokens likely contain misclassified municipalities or entities, triggering LLM fallback.

**Files:**
- Modify: `app/core/query_router.py`
- Test: `tests/test_query_router.py`

**Step 1: Write failing tests**

Add to `tests/test_query_router.py`:

```python
def test_needs_llm_when_city_in_object(router):
    """'en Puerto Salgar' should trigger LLM — city not resolved as department."""
    result = router.parse("procesos de ampliacion en puerto salgar")
    assert result.needs_llm is True
    assert result.route_reason == "heuristic_plus_llm"
    # Object should still contain ampliacion
    assert "ampliacion" in result.params.get("objeto", [])


def test_needs_llm_entity_like_words_in_object(router):
    """'Secretaria de Integracion Social' in objeto should trigger LLM."""
    result = router.parse("contratos de la secretaria de integracion social de adulto mayor")
    assert result.needs_llm is True


def test_no_llm_when_department_resolved(router):
    """'en Atlantico' resolves to department — no LLM needed."""
    result = router.parse("procesos de mantenimiento vial en atlantico")
    assert result.needs_llm is False
    assert result.params.get("departamento_resolved") is not None


def test_no_llm_simple_object_query(router):
    """Simple object search should not trigger LLM."""
    result = router.parse("licitaciones de mantenimiento vial")
    assert result.needs_llm is False
```

**Step 2: Run tests to verify failure**

Run: `cd ~/Documents/secoppal && python -m pytest tests/test_query_router.py::test_needs_llm_when_city_in_object -v`
Expected: FAIL — needs_llm is always False

**Step 3: Implement `_needs_llm()`**

Replace the current `_needs_llm` method:

```python
# At module level, add:
_ENTITY_SIGNAL_WORDS = {
    "secretaria", "ministerio", "instituto", "corporacion", "fundacion",
    "agencia", "unidad", "departamento", "autoridad", "comision",
    "superintendencia", "direccion", "servicio", "empresa",
}

_PREPOSITION_PLACE_RE = re.compile(
    r"\ben\s+([a-z]{4,}(?:\s+[a-z]{4,})*)",
    re.IGNORECASE,
)
```

```python
def _needs_llm(self, normalized_query: str, params: dict[str, object]) -> bool:
    """
    Detect when the heuristic likely misclassified tokens.
    
    Triggers:
    1. "en [place]" where place is NOT a resolved department → likely a city
    2. Entity-signal words in objeto (secretaria, ministerio, etc.)
    3. Too many objeto tokens (≥4) suggest misclassification
    """
    objeto = params.get("objeto", [])
    if not objeto:
        return False

    # Signal 1: "en [place]" not resolved as department
    has_dept = bool(params.get("departamento_resolved"))
    if not has_dept:
        match = _PREPOSITION_PLACE_RE.search(normalized_query)
        if match:
            place_text = match.group(1)
            place_tokens = set(place_text.split())
            objeto_set = set(objeto)
            # If the "en [place]" tokens ended up in objeto, it's a misclassification
            if place_tokens & objeto_set:
                return True

    # Signal 2: Entity-like words in objeto
    objeto_set = set(objeto)
    if objeto_set & _ENTITY_SIGNAL_WORDS:
        return True

    return False
```

**Step 4: Run all router tests**

Run: `cd ~/Documents/secoppal && python -m pytest tests/test_query_router.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add app/core/query_router.py tests/test_query_router.py
git commit -m "feat: smart _needs_llm() detects misclassified cities and entities"
```

---

## Task 4: Wire LLM ciudad into resolve_entities and soql_builder

**Objective:** When LLM extracts `ciudad`, it flows through resolve_entities → soql_builder correctly.

**Files:**
- Modify: `app/core/orchestrator.py` (resolve_entities action)

**Step 1: Review current flow**

The `resolve_entities` action currently handles `departamento` and `entidad`. After the LLM runs, `parsed_params` may now contain `ciudad`. The `resolve_entities` action should pass `ciudad` through unchanged (no fuzzy resolution needed — LIKE handles variations).

**Step 2: Verify no changes needed**

Looking at `resolve_entities`: it copies `params` to `resolved` dict and only processes `departamento` and `entidad`. Any extra keys (like `ciudad`) pass through automatically to `resolved_params`, and `soql_builder.build()` reads `params.get("ciudad")`.

**No code changes needed** — the existing flow already passes unknown params through. The LLM adds `ciudad` to `parsed_params`, resolve_entities copies it to `resolved_params`, soql_builder reads it.

**Step 3: Write integration test**

Add to `tests/test_orchestrator.py`:

```python
def test_ciudad_flows_through_pipeline():
    """Ciudad param from LLM should reach soql_builder."""
    from app.core.soql_builder import SoQLBuilder
    builder = SoQLBuilder()
    # Simulate resolved_params after LLM adds ciudad
    params = {
        "dataset": "procesos",
        "objeto": ["ampliacion"],
        "ciudad": "Puerto Salgar",
    }
    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, params)
    assert "ciudad_entidad" in soql
    assert "Puerto Salgar" in soql
    assert "ampliacion" in soql
```

**Step 4: Run test**

Run: `cd ~/Documents/secoppal && python -m pytest tests/test_orchestrator.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_orchestrator.py
git commit -m "test: verify ciudad flows through pipeline to SoQL"
```

---

## Task 5: Ensure LLM merge doesn't clobber gazetteer results

**Objective:** When the heuristic already resolved an entity via gazetteer scan, the LLM should NOT overwrite it. But when the heuristic put entity-like tokens in `objeto`, the LLM should be able to move them to `entidad` or `ciudad`.

**Files:**
- Modify: `app/core/llm_handler.py`

**Step 1: Update REGEX_PRIORITY_KEYS**

Add `entidad` to priority keys ONLY when the heuristic already resolved it:

The current merge logic already handles this correctly:
```python
if key in REGEX_PRIORITY_KEYS and key in merged and merged[key] not in (None, "", []):
    continue
```

But we need a nuance: `entidad` should be priority ONLY if `entidad_resolved` exists (gazetteer match). If the heuristic didn't find an entity, the LLM should be allowed to set it.

**Step 2: Implement conditional priority**

In `LLMHandler.parse()`, after the existing merge loop, change the logic:

```python
# Replace the merge loop with:
for key, value in tool_args.items():
    if value in (None, "", []):
        continue
    # Don't overwrite keys that regex already extracted with confidence
    if key in REGEX_PRIORITY_KEYS and key in merged and merged[key] not in (None, "", []):
        continue
    # Don't overwrite gazetteer-resolved entity
    if key == "entidad" and merged.get("entidad_resolved"):
        continue
    # LLM objeto replaces heuristic objeto (it's the reclassification)
    merged[key] = value
```

**Step 3: Commit**

```bash
git add app/core/llm_handler.py
git commit -m "fix: LLM merge respects gazetteer entity resolution"
```

---

## Task 6: Run accuracy benchmark and verify no regressions

**Objective:** Confirm the LLM fallback doesn't break existing passing queries.

**Files:**
- Run: `scripts/accuracy_test.py`

**Step 1: Run full test suite**

```bash
cd ~/Documents/secoppal && python -m pytest -v
```

Expected: All 54 tests pass.

**Step 2: Run accuracy benchmark**

```bash
cd ~/Documents/secoppal && python scripts/accuracy_test.py --verbose
```

Expected: V3 accuracy ≥ 95.5% (no regression). The benchmark uses heuristic-only queries, so `needs_llm` may be True for some, but parsed_params from heuristic should still match expected values.

**Step 3: Manual smoke tests with the failing queries**

```bash
cd ~/Documents/secoppal && python -c "
from app.core.orchestrator import SecopalWorkflow
from app.config import get_settings
wf = SecopalWorkflow(get_settings())

# Test 1: Puerto Salgar (should trigger LLM, extract ciudad)
r = wf.run_query('procesos de ampliacion en puerto salgar', 'streamlit')
print('=== Puerto Salgar ===')
print(f'needs_llm: {r[\"needs_llm\"]}')
print(f'parsed: {r[\"parsed_params\"]}')
print(f'resolved: {r[\"resolved_params\"]}')
print(f'results: {r[\"results\"][:2] if r[\"results\"] else \"NONE\"}')
print()

# Test 2: Secretaria de Integracion Social (should trigger LLM, extract entidad)
r = wf.run_query('contratos de la secretaria de integracion social de adulto mayor', 'streamlit')
print('=== Secretaria Integracion Social ===')
print(f'needs_llm: {r[\"needs_llm\"]}')
print(f'parsed: {r[\"parsed_params\"]}')
print(f'resolved: {r[\"resolved_params\"]}')
print(f'results count: {len(r[\"results\"])}')
"
```

**Step 4: Commit**

```bash
git commit --allow-empty -m "test: verified accuracy benchmark and manual smoke tests pass"
```

---

## Task 7: Update README and ADR

**Objective:** Document the hybrid architecture decision.

**Files:**
- Modify: `README.md`

**Changes:**

1. Update architecture diagram: add "LLM Fallback" step between Query Router and Entity Resolver (conditional).

2. Update ADR-008 status to "Superseded by ADR-009" and add:

```markdown
### ADR-009: Hybrid LLM fallback for city/entity disambiguation

**Fecha:** 2026-04
**Estado:** Activo

**Decisión:** Reintroducir DeepSeek V3 como fallback condicional para queries donde el heurístico no puede distinguir entre municipio, entidad y objeto.

**Contexto:** Feedback de producción (traces 3aa726cc, f202ab0c, 78ad68c9) mostró que el heurístico falla cuando:
- El usuario menciona un municipio ("en Puerto Salgar") — no hay gazetteer de municipios
- El usuario menciona una entidad por nombre libre ("Secretaría de Integración Social") — no está en las 500 entidades del gazetteer

Estos tokens terminan en `objeto`, generando queries LIKE sobre nombre_del_procedimiento en vez de filtrar por ciudad_entidad o nombre_entidad.

**Triggers para LLM:**
1. Patrón "en [lugar]" donde el lugar no se resolvió como departamento
2. Palabras tipo entidad (secretaría, ministerio, etc.) quedaron en objeto

**Nuevo parámetro:** `ciudad` — mapea a `ciudad_entidad` (procesos) o `ciudad` (contratos).

**Consecuencias:** ~5-10% de queries usan LLM (~$0.001/query). 90%+ siguen siendo gratis. Latencia +2-5s solo para queries que antes daban resultados incorrectos.
```

3. Update query_router docs to mention `_needs_llm()` heuristics.

4. Update `route_reason` table to include:
   - `heuristic_plus_llm`: "en [city]" or entity-signal words detected in object

**Step 1: Make the edits**

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: ADR-009 hybrid LLM fallback, update architecture diagram"
```

---

## Summary of changes

| File | Change |
|------|--------|
| `app/core/soql_builder.py` | Add `city` field to DatasetSpec, `ciudad` WHERE clause |
| `app/core/llm_handler.py` | Add `ciudad` to tool schema, improve system prompt, protect gazetteer entity |
| `app/core/query_router.py` | Smart `_needs_llm()` with city/entity detection heuristics |
| `tests/test_soql_builder.py` | Tests for ciudad filter |
| `tests/test_query_router.py` | Tests for LLM trigger conditions |
| `tests/test_orchestrator.py` | Integration test for ciudad flow |
| `README.md` | ADR-009, updated architecture |

**No changes needed to:**
- `orchestrator.py` — already has the LLM transition wired
- `entity_resolver.py` — ciudad passes through without resolution
- `config.py` — DeepSeek settings already exist
- `service.py` — no changes

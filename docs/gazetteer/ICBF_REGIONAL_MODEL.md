# ICBF Regional Ecosystem Model

**Status:** Draft — pure resolver, not yet integrated into query hot path  
**Files:** `app/data/icbf_regionals.json`, `app/core/icbf_resolver.py`  
**Tests:** `tests/test_icbf_regionals.py` (47 tests)  
**Commit:** TBD (part of `feature/gazetteer-append-manager`)

---

## 1. Why ICBF Before SENA?

ICBF is structurally simpler than SENA for regional disambiguation because:

| Property | ICBF | SENA |
|----------|------|------|
| Regionales | ~1:1 por departamento (32 regionals) | Multiple centers per dept + cross-dept groups |
| Official name pattern | `ICBF REGIONAL {DEPT}` or `ICBF {DEPT}//` | `SENA REGIONAL {DEPT} Grupo {xxx}` |
| Alias pattern | `icbf {dept}`, `bienestar familiar {dept}` | `sena {dept}`, `sena regional {dept}`, `{dept} sena` |
| NIT base | `899999239` (shared) | Multiple NITs per regional |
| National entity | `ICBF SEDE NACIONAL` | `SENA SECRETARIA GENERAL` + `SENA DIRECCIÓN GENERAL` |

---

## 2. Resolution Modes

### 2.1 ICBF National (global)

**Triggers:** "icbf" alone, "procesos del icbf", "contratos del instituto colombiano de bienestar familiar"

**Behavior:**
- Returns `resolution_type="icbf_national"`
- `official_names` includes sede nacional + all 32 regionales
- Metadata includes a warning: "ICBF se interpretó como ecosistema nacional"
- User can filter further by department or macro-region

**Design decision:** "ICBF" alone should NOT resolve to an arbitrary regional.

### 2.2 ICBF Regional (specific department)

**Triggers:** "icbf atlántico", "bienestar familiar córdoba", "instituto colombiano de bienestar familiar regional bolivar"

**Behavior:**
- Returns `resolution_type="icbf_regional"`
- `department` is set to the canonical SECOP department name
- `official_names` contains the matching regional(s) — usually 1
- Supports tilded and non-tilded department names

**Alias matching:**
- "icbf" + department name
- "bienestar familiar" + department name
- "instituto colombiano de bienestar familiar" + department name
- Department detection uses existing `DEPARTAMENTOS` module for broad coverage

### 2.3 ICBF Macro Region (multi-department)

**Triggers:** "icbf en costa atlántica", "procesos del icbf en el caribe"

**Behavior:**
- Returns `resolution_type="icbf_macro_region"`
- `region_macro` is the canonical region key (e.g. "costa_atlantica", "caribe")
- `official_names` includes all ICBF regionals in that macro region
- Uses the same macro region mapping as REGION-MACRO-001 (not yet integrated)

---

## 3. Architecture

```
app/data/icbf_regionals.json        ← catalog (sede + 32 regionales + macro regions)
    │
    ▼
app/core/icbf_resolver.py           ← pure resolver
    │  resolve_icbf_reference()     ← input: raw query
    │  build_icbf_filter_metadata() ← output: filter metadata (no SoQL)
    │
    ▼
tests/test_icbf_regionals.py        ← 47 tests: national/regional/macro/negative
```

### 3.1 Pure Resolver Contract

```python
def resolve_icbf_reference(raw_query: str) -> ICBFResolution | None:
    """
    Returns None if no ICBF mention.
    Otherwise returns ICBFResolution with resolution_type, department,
    region_macro, official_names, aliases_matched, confidence.
    No SECOP calls. No SoQL. No side effects.
    """

def build_icbf_filter_metadata(resolution: ICBFResolution) -> dict:
    """
    Returns filter-friendly dict with:
    - entity_family="ICBF"
    - resolution_type
    - official_names
    - department / region_macro
    - warning (if national)
    No SoQL — consumer decides how to translate to WHERE clause.
    """
```

### 3.2 ICBFResolution Dataclass

```python
@dataclass(frozen=True)
class ICBFResolution:
    resolution_type: Literal["icbf_national", "icbf_regional", "icbf_macro_region", "none"]
    label: str
    department: str | None = None
    region_macro: str | None = None
    official_names: list[str] = field(default_factory=list)
    aliases_matched: list[str] = field(default_factory=list)
    confidence: Literal["high", "medium", "low"] = "low"
```

---

## 4. Catalog Structure

**File:** `app/data/icbf_regionals.json`

```json
{
  "_meta": {
    "description": "ICBF Regional Catalog",
    "nit_base": "899999239"
  },
  "sede_nacional": { ... },
  "regionales": [
    { "departamento": "Atlántico", "official_name": "ICBF REGIONAL ATLANTICO", "aliases": [...] },
    ...
  ],
  "_macro_regions": {
    "caribe": ["Atlántico", "Bolívar", ...],
    "costa_atlantica": ["Atlántico", "Bolívar", ...],
    ...
  }
}
```

32 regionals total, one per department (some shared names like Bolívar/Boyacá use `INSTITUTO COLOMBIANO DE BIENESTAR FAMILIAR REGIONAL {DEPT}` instead of `ICBF REGIONAL {DEPT}`).

---

## 5. NIT Structure

- **NIT base:** `899999239` for most regionals
- Some regionals may have a different NIT or additional verification digit
- NIT is not used in the resolver (no SECOP queries) but is documented for future integration
- When integrating into the query path, NIT-based filtering via `documento_proveedor` could be more precise than entity name LIKE matching

---

## 6. Integration Path (future)

1. **Pipeline integration:** Call `resolve_icbf_reference()` in `query_router.parse()` after entity scan but before SoQL building
2. **SoQL translation:** If `icbf_national`, generate `entidad IN (...33 names...)` or `entidad_ecosystem LIKE` for broader match
3. **Response formatting:** Show ICBF as a single entity family with expandable regionals
4. **Radar/Market nodes:** Precompute ICBF contract volumes by department

---

## 7. Known Limitations

| Limitation | Impact | Mitigation |
|-----------|--------|------------|
| No integration into query hot path | ICBF resolver exists but doesn't affect search results | Documented — integration is next step |
| Macro region detection uses `_macro_regions` in catalog JSON, not shared `geo_regions.py` | Duplicated region definitions | Will be unified when REGION-MACRO-001 is implemented |
| Department detection uses substring match on normalized query | May match partial words (e.g. "ica" in "icbf" + "caldas" = OK; but "icbf atl" matches "atlántico") | Low risk — ICBF trigger must be present first |
| Bogotá/Distrito Capital detection | "icbf bogotá" could match ICBF REGIONAL BOGOTA (regional) or ICBF SEDE NACIONAL (national) | Currently resolves to regional; national requires explicit "sede nacional" or "nacional" |
| Only 2 macro regions have ICBF regionals (caribe, costa_atlantica share) | Others (eje_cafetero, pacifico, etc.) would work if ICBF regionals exist there (they do) | All 32 regionals are in catalog — all macros work |

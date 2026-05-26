# SENA Entity Ecosystem Model

**Status:** Draft — pure resolver, not yet integrated into query hot path  
**Files:** `app/data/sena_units.json`, `app/core/sena_resolver.py`  
**Tests:** `tests/test_sena_resolver.py` (41 tests)  
**Audit:** `docs/gazetteer/SENA_ENTITY_AUDIT.md`

---

## 1. Why SENA is More Complex Than ICBF

| Property | ICBF | SENA |
|----------|------|------|
| Regionales | ~1:1 por departamento (32) | Multiples por depto: parent + admin groups + centers |
| Official names | `ICBF REGIONAL {DEPT}` (uniforme) | Variantes: `SENA REGIONAL {DEPT}`, `SENA {DEPT}//`, `SERVICIO NACIONAL DE APRENDIZAJE SENA {DEPT}` |
| Admin groups | No aplica | `Grupo de Apoyo Administrativo Mixto`, `Grupo Administrativo {CBA,CDA,...}`, `Grupo Mixto` |
| Training centers | No aplica | `Centro Agropecuario`, `Centro Industrial`, etc. |
| Intercentros | No aplica | `Grupo Administrativo Intercentros` |
| Total entidades SECOP | 32 | **89** (según audit live) |

---

## 2. SENA Entity Types (from Audit)

| Tipo | Cantidad | Volumen | Descripción |
|------|----------|---------|-------------|
| `national_secretaria_general` | 1 | 21,545 | SENA Secretaría General (nivel nacional) |
| `national_direccion_general` | 2 | 8,817 | SENA Dirección General |
| `regional_parent` | 28 | 179,712 | Entidad SENA principal por departamento |
| `regional_admin_group` | 54 | 395,848 | Grupo administrativo regional (mayor volumen) |
| `training_center` | 1 | 4,491 | Centro de formación |
| `intercentros` | 2 | 5,793 | Grupo administrativo multi-centro |
| `needs_review` | 1 | 4 | Nombre atípico |

**Hallazgo clave:** Los `regional_admin_group` tienen **más del doble de volumen** que los `regional_parent` (395K vs 179K). Esto significa que la mayoría de procesos/contratos SENA están registrados bajo grupos administrativos, no bajo la entidad regional principal.

---

## 3. Resolution Approach

### 3.1 Agrupar para buscar, no fusionar para registrar

SENA + departamento = **ecosistema institucional territorial**.

Cuando un usuario busca "sena magdalena", el resolver devuelve:
- `resolution_type="sena_regional_ecosystem"`
- `contains_all=["SENA", "MAGDALENA"]` — filtro para LIKE compuesto
- No intenta resolver a una entidad específica

El consumidor downstream (SoQL builder) debe generar:
```sql
(UPPER(entidad) LIKE '%SENA%' AND UPPER(entidad) LIKE '%MAGDALENA%')
```

### 3.2 Modos de resolución

| Modo | Trigger | Output |
|------|---------|--------|
| `sena_national` | "sena" solo, "servicio nacional de aprendizaje" | national entity names + warning |
| `sena_regional_ecosystem` | "sena {dept}", "sena en {dept}" | `contains_all` + department |
| `sena_macro_region` | "sena en {macro}" | `contains_all` multi-dept |
| `sena_unit_exact` | Nombre exacto de unidad SENA | official_name exacto |

---

## 4. Architecture

```
scripts/audit_sena_entities.py        ← read-only SECOP audit
          │
          ▼
app/data/sena_units.json              ← catalog (national + 15 regionales + macros)
          │
          ▼
app/core/sena_resolver.py             ← pure resolver
    ├── resolve_sena_reference()
    └── build_sena_filter_metadata()
          │
          ▼
tests/test_sena_resolver.py           ← 41 tests
```

### 4.1 Pure Resolver Contract

```python
def resolve_sena_reference(raw_query: str) -> SENAResolution | None:
    """
    Returns None if no SENA mention.
    Otherwise returns SENAResolution with resolution_type, department,
    region_macro, contains_all, official_names, aliases_matched, warning.
    No SECOP calls. No SoQL. No side effects.
    """

def build_sena_filter_metadata(resolution: SENAResolution) -> dict:
    """
    Returns filter-friendly dict with:
    - entity_family="SENA"
    - resolution_type
    - department / region_macro
    - contains_all (for LIKE composition)
    - warning (if national)
    No SoQL — consumer decides how to translate to WHERE clause.
    """
```

### 4.2 SENAResolution Dataclass

```python
@dataclass(frozen=True)
class SENAResolution:
    resolution_type: Literal["sena_national", "sena_regional_ecosystem",
                             "sena_macro_region", "sena_unit_exact", "none"]
    label: str
    department: str | None
    region_macro: str | None
    official_names: list[str]
    contains_all: list[str]       # For LIKE composition: ["SENA", "MAGDALENA"]
    aliases_matched: list[str]
    confidence: Literal["high", "medium", "low"]
    warning: str | None           # Only for national
```

---

## 5. Catalog Structure

**File:** `app/data/sena_units.json`

```json
{
  "_meta": { "nit_base_common": "899999034" },
  "national": {
    "trigger_aliases": ["sena", "servicio nacional de aprendizaje", ...],
    "official_names": ["SENA SECRETARIA GENERAL", "SENA DIRECCIÓN GENERAL..."]
  },
  "regional_groups": [
    {
      "department": "Magdalena",
      "aliases": ["sena magdalena", "sena regional magdalena", ...],
      "member_name_contains_all": ["SENA", "MAGDALENA"]
    },
    ...15 grupos por departamento...
  ],
  "macro_regions": {
    "caribe": [...8 deptos...],
    "costa_atlantica": [...8 deptos...],
    ...
  }
}
```

15 departamentos en catálogo: Magdalena, Santander, Bolívar, Atlántico, Antioquia, Valle del Cauca, Cauca, Nariño, Cundinamarca, Distrito Capital, Córdoba, La Guajira, Sucre, Cesar, Norte de Santander.

---

## 6. Audit Findings

El audit (`SENA_ENTITY_AUDIT.md`) reveló:

| Hallazgo | Detalle |
|----------|---------|
| Total entidades SENA únicas | **89** en SECOP II |
| Volumen total | **616,210** procesos + contratos |
| Departamentos cubiertos | **33** (todos los deptos de Colombia) |
| Grupos administrativos dominan | 54 entidades, 395K volumen (64%) |
| Necesitan revisión | 1 entidad ("SENA - REGIONAL SANTANDER") |
| Falso positivo | 1 ("CONGRESO... SENADO..." matchea por substring) |

---

## 7. Integration Path (future)

1. **Pipeline integration:** Llamar `resolve_sena_reference()` después de entity scan en query_router
2. **SoQL translation:**
   - `sena_national` → `entidad IN (lista de oficial_names)`
   - `sena_regional_ecosystem` → `(entidad LIKE '%SENA%' AND entidad LIKE '%MAGDALENA%')`
   - `sena_macro_region` → LIKE compuesto multi-departamento
3. **Response formatting:** Mostrar SENA como ecosistema con opción de expandir por regional
4. **NIT-based filtering:** Usar NIT 899999034 como complemento a LIKE matching

---

## 8. Known Limitations

| Limitación | Impacto | Mitigación |
|-----------|--------|------------|
| No integrado al hot path | Resolver existe pero no afecta búsquedas reales | Documentado — integración es siguiente paso |
| `contains_all` es LIKE compuesto, no IN | Puede traer entidades no-SENA si comparten palabras clave | Bajo riesgo — SENA es un término distintivo |
| Solo 15 departamentos en catálogo | Departamentos no listados usan DEPARTAMENTOS lookup con confianza medium | Extender a 32 en fase de integración |
| Training centers e intercentros no tienen catálogo individual | No se puede buscar "centro agropecuario sena" específico | `sena_unit_exact` necesita extensión |
| SENA usa `LIKE '%SENA%'` que matchea "SENADO" (falso positivo) | Consultas sobre el Senado podrían contaminar | Filtrar con `AND NOT LIKE '%SENADO%'` en SoQL builder |

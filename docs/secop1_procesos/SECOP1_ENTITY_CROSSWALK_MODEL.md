# SECOP1_ENTITY_CROSSWALK_MODEL — Diseño de Aliases por Fuente

**Fecha:** 2026-05-29
**Versión:** 0.2.0 (C1A.1 — hardening)
**Task:** C1A + C1A.1

---

## Principio

> **No crear dos gazetteers separados.**
> Una entidad canónica + aliases diferenciados por fuente (dataset).

---

## Modelo de Datos

### EntityCrosswalkEntry (v0.2.0)

```python
@dataclass
class EntityCrosswalkEntry:
    """Maps a SECOP I entity name to a canonical entity."""

    # ── Identidad canónica ──
    canonical_id: str            # Slug único y estable (ej: municipio_ataco_tolima)
    canonical_name: str          # Nombre normalizado único
    canonical_nit: str | None    # NIT si está disponible

    # ── Aliases por tipo ──
    source_aliases: list[SourceAlias]   # Nombres tal cual aparecen en cada dataset
    user_aliases: list[UserAlias]       # Variantes generadas para búsqueda del usuario

    # ── Metadata ──
    classification: str          # matched_existing | secop1_only | ambiguous | bad_name | needs_review
    integration_ready: bool      # False para bad_name y needs_review
    exclusion_reason: str | None # Razón si integration_ready=False
    total_2026_count: int        # Actividad total en 2026
    sources: dict                # {"f789-7hwg": N, "rpmr-utcd": N, "convocado": N}

@dataclass
class SourceAlias:
    """A name variant from a specific dataset."""
    name: str                    # Nombre tal cual aparece en el dataset
    source: str                  # "f789-7hwg" | "rpmr-utcd" | "p6dx-8zbt" | "jbjy-vk9h" | "heuristic"
    normalization: str           # Nombre normalizado (sin tildes, uppercase)

@dataclass
class UserAlias:
    """A search-friendly alias generated automatically."""
    alias: str                   # Variante para búsqueda
    generated_by: str            # "auto_rules_c1a1"
```

### canonical_id — Reglas de Generación

| Tipo de entidad | Patrón canonical_id | Ejemplo |
|----------------|---------------------|---------|
| Alcaldía | `municipio_{municipio}_{departamento}` | `municipio_ataco_tolima` |
| Cámara de Comercio | `camara_comercio_{ciudad}_{departamento}` | `camara_comercio_bucaramanga_santander` |
| Gobernación | `gobernacion_{departamento}` | `gobernacion_guainia` |
| ESE / Hospital | `ese_{nombre}_{departamento}` | `ese_hospital_san_jose_guaviare` |
| Otros | `{nombre_slug}` | `escuela_nacional_del_deporte` |

Reglas:
- Sin tildes, lowercase, snake_case.
- Estable: no cambia si el nombre en Socrata cambia ligeramente.
- Único: no puede haber dos entidades con el mismo canonical_id.
- Derivado del nombre de la entidad y su departamento, no de un contador.

---

## source_aliases vs user_aliases

| Dimensión | source_aliases | user_aliases |
|-----------|---------------|-------------|
| Origen | Directo del dataset Socrata | Generados automáticamente |
| Propósito | Matching exacto contra API | Búsqueda del usuario (fuzzy) |
| Formato | Tal cual aparece en los datos | Normalizado, variantes cortas |
| Ejemplos | `SANTANDER - CAMARA DE COMERCIO DE BUCARAMANGA` | `CAMARA DE COMERCIO BUCARAMANGA`, `CC BUCARAMANGA` |

---

## Reglas de Generación de user_aliases

### R1 — Remover prefijo departamental
`DEPARTAMENTO - ENTIDAD` → `ENTIDAD`
Ej: `TOLIMA - ALCALDIA MUNICIPIO DE ATACO` → `ALCALDIA MUNICIPIO DE ATACO`

### R2 — Alcaldía: variantes
`ALCALDIA MUNICIPIO DE X` genera:
- `ALCALDIA DE X`
- `MUNICIPIO DE X`
- `X DEPARTAMENTO` (municipio + departamento, dos tokens mínimo)

### R3 — Cámara de Comercio: variantes
`CAMARA DE COMERCIO DE X` genera:
- `CAMARA DE COMERCIO X` (sin "DE")
- `CC X` (abreviación)

### R4 — Gobernación
`GOBERNACION X` genera:
- `GOBERNACION DE X` (si no está ya presente)

### R5 — Remover sufijo de ciudad redundante
`ENTIDAD - CIUDAD` → `ENTIDAD` (si ciudad es redundante con el nombre)

### R6 — No aliases de un solo token
Un alias debe tener ≥2 tokens, excepto el patrón `MUNICIPIO DEPARTAMENTO`
que es suficientemente específico.

---

## integration_ready — Filtro de Calidad

Solo las entidades clasificadas como `matched_existing` o `secop1_only`
son integration_ready. Las clasificadas como `bad_name` o `needs_review`
se excluyen:

| Clasificación | integration_ready | Razón |
|--------------|-------------------|-------|
| matched_existing | ✅ True | Ya existe en gazetteer |
| secop1_only | ✅ True | Nueva, pero nombre válido |
| needs_review | ❌ False | Requiere verificación manual (ESEs, nombres ambiguos) |
| bad_name | ❌ False | Nombre truncado o malformado en Socrata |

### Muestra C1A.1

| Métrica | Valor |
|---------|-------|
| Total entidades | 86 |
| integration_ready | 69 (80.2%) |
| Excluidas bad_name | 3 |
| Excluidas needs_review | 14 |
| User aliases generados | 249 |

---

## Casos Concretos Verificados

### Ataco, Tolima
```
canonical_id: municipio_ataco_tolima
source_aliases:
  - TOLIMA - ALCALDIA MUNICIPIO DE ATACO (f789-7hwg)
user_aliases:
  - ALCALDIA DE ATACO
  - ALCALDIA MUNICIPIO DE ATACO
  - ATACO TOLIMA
  - MUNICIPIO DE ATACO
integration_ready: true
classification: matched_existing
```

### Cámara de Comercio de Bucaramanga
```
canonical_id: camara_comercio_bucaramanga_santander
source_aliases:
  - SANTANDER - CAMARA DE COMERCIO DE BUCARAMANGA (f789-7hwg)
user_aliases:
  - CAMARA DE COMERCIO BUCARAMANGA
  - CAMARA DE COMERCIO DE BUCARAMANGA
  - CC DE BUCARAMANGA
integration_ready: true
classification: matched_existing
```

### Cámara de Comercio de Cali
```
canonical_id: camara_comercio_cali_valle_del_cauca
source_aliases:
  - VALLE DEL CAUCA - CAMARA DE COMERCIO DE CALI (f789-7hwg)
user_aliases:
  - CAMARA DE COMERCIO CALI
  - CAMARA DE COMERCIO DE CALI
  - CC DE CALI
integration_ready: true
classification: matched_existing
```

---

## Pipeline de Resolución (futuro)

```python
def resolve_entity(name: str, source: str) -> EntityMatch | None:
    # 1. Buscar en crosswalk por fuente específica (source_alias exacto)
    match = crosswalk.find_source_alias(source=source, name=name)
    if match:
        return EntityMatch(canonical_id=match.canonical_id, ...)

    # 2. Buscar en user_aliases (búsqueda fuzzy del usuario)
    match = crosswalk.find_user_alias(name)
    if match:
        return EntityMatch(canonical_id=match.canonical_id, ...)

    # 3. Normalizar y buscar en gazetteer existente
    normalized = normalize_secop1_name(name)
    return gazetteer.search(normalized)
```

---

## Referencias

- [ENTITY_ALIAS_AUDIT.md](ENTITY_ALIAS_AUDIT.md) — Resultados de la auditoría
- [SCHEMA_AUDIT.md](SCHEMA_AUDIT.md) — Schema de f789-7hwg
- [../datasets/SECOP_HISTORICAL_DATASET_COMPARISON.md](../datasets/SECOP_HISTORICAL_DATASET_COMPARISON.md) — Comparativa datasets

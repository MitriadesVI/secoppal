# SECOPPAL — Plan de implementación Sprint 1A (para Codex)

**Fecha:** 2026-05-20
**Audiencia:** ejecutor (Codex) + revisor (humano + Claude)
**Alcance:** 9 fixes CRITICAL identificados en `docs/CORPUS_BACKLOG.md` + interfaz preparada para ENTITY-ECOSYSTEM (Sprint 3).
**Estimación total:** 20-25 horas de implementación + 4-6 horas de tests.
**Pre-requisitos:** M0 baseline capturado (commit `b16ed0b`); plan de robustecimiento sistémico (commit `94abfde`).

---

## Cómo leer este documento

Cada bloque (B1–B9 + B0) es **atómico, verificable y reversible**:

- **Pre-requisitos:** qué debe estar listo antes de arrancar.
- **Archivos a tocar:** rutas exactas. Cualquier otro archivo es **off-limits**.
- **Especificación funcional:** qué debe hacer el código nuevo.
- **Tests:** rutas y contenido mínimo de tests.
- **Reglas locales:** restricciones específicas del bloque.
- **Criterios de aceptación:** condiciones medibles para cerrar el bloque.
- **Verificación pre/post:** comandos exactos para confirmar no-regresión.

**Orden de ejecución no negociable:** B0 → B1 → B2 → B3 → B4 → B5 → B6 → B7 → B8 → B9. Cada bloque debe quedar en verde antes de empezar el siguiente.

---

## Reglas globales (aplican a TODOS los bloques)

### Regla G1 — Línea roja de no-regresión

Al cerrar cada bloque, **DEBE** cumplirse:

```bash
pytest -q                                                      # ≥ 579 passed, ≤3 xfailed, 0 failed
python scripts/run_query_corpus.py --mode all                  # 88/88 PASS, 0 FAIL
make lint-core                                                 # exit 0
git diff --stat data/feedback.jsonl                            # vacío (sin cambios)
```

Si **cualquier** condición falla, el bloque queda abierto. No avanzar al siguiente.

### Regla G2 — Snapshot por bloque

Antes de empezar el bloque BN:

```bash
mkdir -p baseline
pytest -q > baseline/BN_pytest_before.txt 2>&1
python scripts/run_query_corpus.py --mode all > baseline/BN_corpus_before.txt 2>&1
make lint-core > baseline/BN_lint_before.txt 2>&1
git log --oneline -1 > baseline/BN_commit_before.txt
```

Al cerrar:

```bash
pytest -q > baseline/BN_pytest_after.txt 2>&1
python scripts/run_query_corpus.py --mode all > baseline/BN_corpus_after.txt 2>&1
make lint-core > baseline/BN_lint_after.txt 2>&1
git log --oneline -1 > baseline/BN_commit_after.txt
diff baseline/BN_pytest_before.txt baseline/BN_pytest_after.txt
```

### Regla G3 — Commits atómicos por bloque

Un bloque = un commit. Formato del commit:

```
fix(sprint1a): <BUG-ID> — <descripción breve>

<contexto del bug>

<resumen del fix>

Validation:
- pytest: 579 → N passed (Δ +M nuevos)
- corpus: 88/88 → 88/88 (sin regresión)
- lint-core: verde
- caso reproducido: <referencia específica del proceso>
```

### Regla G4 — No tocar archivos fuera del scope del bloque

Cada bloque lista archivos permitidos. **Cualquier modificación a archivos no listados invalida el bloque.** Si el bloque revela que un cambio externo es necesario, parar y consultar al revisor — no asumir.

### Regla G5 — No introducir dependencias nuevas sin justificación explícita

Si un bloque parece requerir un paquete pip nuevo, parar y consultar. La excepción son librerías estándar de Python ya en uso.

### Regla G6 — Tests deben ser específicos del bug, no genéricos

Cada bloque crea o extiende un archivo de tests. Los tests deben:
- Reproducir el caso real reportado (con la query exacta del usuario).
- Verificar el comportamiento esperado post-fix con asserts concretos.
- Incluir al menos un caso de "no romper lo que funcionaba" (regresión inversa).

### Regla G7 — Lenguaje del código y comentarios

- Código en inglés.
- Identificadores semánticos (`is_value_max_phrase`, no `xvm`).
- Comentarios solo cuando el WHY no es obvio. Sin docstrings extensas.
- Mensajes de error / clarification al usuario en español.

---

## B0 — Pre-trabajo de auditoría y baseline

**Tiempo:** 30-45 min
**Pre-requisitos:** ninguno

### Objetivo

Confirmar el estado actual del sistema y descubrir empíricamente el catálogo real de `estado_del_procedimiento` en SECOP. Esto alimenta B9.

### Archivos a tocar

- `baseline/B0_pytest.txt` (crear)
- `baseline/B0_corpus.txt` (crear)
- `baseline/B0_estados_p6dx.json` (crear)
- `baseline/B0_estados_jbjy.json` (crear)

### Pasos

1. Capturar snapshot M(B0) según Regla G2.
2. Ejecutar query de descubrimiento de estados:
   ```bash
   curl -s "https://www.datos.gov.co/resource/p6dx-8zbt.json?\$select=estado_del_procedimiento,count(*)&\$group=estado_del_procedimiento&\$order=count_(*)+DESC" \
       > baseline/B0_estados_p6dx.json
   curl -s "https://www.datos.gov.co/resource/jbjy-vk9h.json?\$select=estado_contrato,count(*)&\$group=estado_contrato&\$order=count_(*)+DESC" \
       > baseline/B0_estados_jbjy.json
   ```
3. Inspeccionar manualmente el JSON resultante. Anotar valores no contemplados en `app/core/estado_families.py`.

### Criterio de aceptación

- 3 archivos en `baseline/` creados.
- Inventario de estados reales documentado como comentario en commit.

### Commit

```
chore(sprint1a): B0 — capture baseline + discover estado catalog
```

---

## B1 — MIN-MAX-INVERSION-001

**Tiempo:** 2-3 horas
**Pre-requisitos:** B0 cerrado
**Caso canónico:** Palermo IP-017-2026 (Huila transporte escolar)

### Objetivo

Corregir la polaridad del parser para frases de negación monetaria. "No sean mayores a X" debe extraer `valor_max=X`, no `valor_min=X`.

### Archivos a tocar

- `app/core/query_router.py` (modificar lógica de extracción monetaria)
- `tests/test_value_polarity.py` (crear)

### Especificación funcional

Mapeo correcto de patrones de polaridad:

| Patrón | Campo |
|---|---|
| "mayor a X", "mayor que X", "más de X", "superior a X", "supere X", "exceda X", "por encima de X", "al menos X", "mínimo X", "desde X", "a partir de X" | `valor_min` |
| "menor a X", "menor que X", "menos de X", "inferior a X", "por debajo de X", "hasta X", "máximo X", "no más de X" | `valor_max` |
| **"no sea mayor a X", "no sean mayores a X", "que no supere X", "que no exceda X"** | `valor_max` (NEGACIÓN INVERTIDA) |
| **"no sea menor a X", "no sean menores a X", "que no baje de X"** | `valor_min` (NEGACIÓN INVERTIDA) |
| "entre X y Y" | `valor_min=X, valor_max=Y` |

### Tests (mínimos)

```python
# tests/test_value_polarity.py
import pytest
from app.core.query_router import QueryRouter

router = QueryRouter()

POSITIVE_CASES = [
    ("mayores a 30 millones", {"valor_min": 30_000_000}),
    ("menos de 20 millones", {"valor_max": 20_000_000}),
    ("entre 20 y 30 millones", {"valor_min": 20_000_000, "valor_max": 30_000_000}),
    ("al menos 50 millones", {"valor_min": 50_000_000}),
    ("hasta 100 millones", {"valor_max": 100_000_000}),
]

NEGATION_CASES = [
    # CRITICAL: estos son los que estaban invertidos
    ("no sean mayores a 30 millones", {"valor_max": 30_000_000}),
    ("que no supere los 25 millones", {"valor_max": 25_000_000}),
    ("que no exceda 50 millones", {"valor_max": 50_000_000}),
    ("no sean menores a 10 millones", {"valor_min": 10_000_000}),
    ("que no baje de 5 millones", {"valor_min": 5_000_000}),
]

NO_FILLER_CASES = [
    # "supere" / "exceda" no deben quedar en objeto cuando ya inferimos el valor
    ("transporte escolar no sean mayores a 30 millones",
     {"valor_max": 30_000_000, "objeto_not_contains": ["mayores", "sean", "no"]}),
]

@pytest.mark.parametrize("query,expected", POSITIVE_CASES + NEGATION_CASES)
def test_value_polarity(query, expected):
    params = router.parse(query)
    for key, value in expected.items():
        assert params.get(key) == value, f"For query='{query}': expected {key}={value}, got {params.get(key)}"

@pytest.mark.parametrize("query,expected", NO_FILLER_CASES)
def test_value_phrase_scrub(query, expected):
    params = router.parse(query)
    objeto = params.get("objeto", [])
    for forbidden in expected["objeto_not_contains"]:
        assert forbidden not in objeto, f"Token '{forbidden}' should be scrubbed when value extracted"
```

### Reglas locales

- Implementación dentro de `query_router.py`. No tocar `soql_builder.py`, `query_plan.py` (no existe aún), ni otros.
- La regex/lógica de negación debe ser declarativa, no condicionales encadenados.

### Criterios de aceptación

- `pytest tests/test_value_polarity.py` → todos verdes.
- Regla G1 verde.
- Query exacta reproducida: `"muestrame procesos en huila transporte escolar 2026 no sean mayores a 30 millones"` → `valor_max=30_000_000` en params extraídos.

### Commit

```
fix(sprint1a): MIN-MAX-INVERSION-001 — correct polarity for negation phrases

Patterns "no sean mayores a X" / "que no supere X" were parsed as
valor_min=X instead of valor_max=X. This inverted the user's intent
directly: a proponent asking "processes within my capacity" received
the opposite.

Fix: declarative mapping of negation patterns in query_router.py
with explicit polarity. Also scrub "mayores"/"supere"/"exceda" from
objeto when the value modifier was successfully extracted.

Validation:
- tests/test_value_polarity.py: 18 new tests pass
- pytest: 579 → 597 passed
- corpus: 88/88 → 88/88
- Caso Palermo IP-017-2026: ahora aparece dentro de "no mayores a 30M"
```

---

## B2 — ACCENT-NORMALIZATION-001

**Tiempo:** 3 horas
**Pre-requisitos:** B1 cerrado
**Casos canónicos:** Paicol (turísticos), Olaya (sólidos), Putumayo (cafetería/jardinería), Ipiales (DISEÑO)

### Objetivo

Hacer que las búsquedas LIKE sobre `nombre_del_procedimiento` y `descripci_n_del_procedimiento` (y campos equivalentes de contratos) sean insensibles a diacríticos (tildes y ñ).

### Archivos a tocar

- `app/core/soql_builder.py` (envolver columnas con normalización)
- `tests/test_accent_normalization.py` (crear)

### Especificación funcional

Cambiar el patrón actual:
```sql
UPPER(nombre_del_procedimiento) LIKE UPPER('%termino%')
```
A:
```sql
regexp_replace(UPPER(nombre_del_procedimiento), '[ÁÉÍÓÚÜÑ]', X, '', '') LIKE
regexp_replace(UPPER('%termino%'), '[ÁÉÍÓÚÜÑ]', X, '', '')
```

Donde X es la sustitución correcta:
- Á→A, É→E, Í→I, Ó→O, Ú→U, Ü→U, Ñ→N

Socrata soporta `regexp_replace`. Alternativamente, evaluar si Socrata expone función `unaccent` directamente. Verificar con:
```bash
curl -s "https://www.datos.gov.co/resource/p6dx-8zbt.json?\$query=SELECT%20unaccent('cafetería')%20LIMIT%201"
```

**Si `unaccent` está disponible**, usarla (más limpio). Si no, `regexp_replace` con cadena de reemplazos.

Aplicar normalización en **ambos lados** (campo y patrón). El parser ya recibe queries sin tildes; el campo del dataset las tiene. Normalizar el campo es lo crítico.

### Tests (mínimos)

```python
# tests/test_accent_normalization.py
def test_soql_normalizes_accents_in_field():
    """Para topic sin tilde, SoQL debe matchear dato con tilde."""
    from app.core.soql_builder import SoQLBuilder
    builder = SoQLBuilder()
    params = {"dataset": "procesos", "objeto": ["consultoria"], "departamento_resolved": "Bolívar"}
    soql = builder.build_query(params)
    # El SoQL debe contener algo que normalice (regexp_replace o unaccent)
    assert "regexp_replace" in soql or "unaccent" in soql

def test_soql_normalizes_n_with_tilde():
    """diseno (sin ñ) debe matchear DISEÑO (con ñ)."""
    params = {"dataset": "procesos", "objeto": ["diseno"]}
    soql = SoQLBuilder().build_query(params)
    assert "regexp_replace" in soql or "unaccent" in soql

def test_accent_normalization_e2e_with_mock():
    """Mock SECOP devolviendo registro con tilde, query sin tilde, esperamos match."""
    # ... (mock SECOP con dato "CONSULTORÍA CUARTOS FRÍOS"; query "consultoria"; expect 1 result)
```

### Reglas locales

- Solo modificar `soql_builder.py`. NO tocar `query_router.py`.
- Mantener la lógica de `UPPER` existente; solo agregar `regexp_replace` o `unaccent` encima.
- Verificar performance: si la normalización agrega >500ms al query promedio, revertir y consultar.

### Criterios de aceptación

- `pytest tests/test_accent_normalization.py` verde.
- Regla G1 verde.
- Smoke manual: `"consultoría cuartos fríos cartagena"` ahora encuentra el proceso MC-SEGD-001-2026.

### Commit

```
fix(sprint1a): ACCENT-NORMALIZATION-001 — make LIKE diacritic-insensitive

UPPER() does not strip diacritics. Patterns LIKE '%consultoria%' did
not match data 'CONSULTORÍA'. Confirmed across 9+ user-reported
cases (consultoría, logístico, turísticos, sólidos, DISEÑO,
cafetería, jardinería).

Fix: wrap both field and pattern with regexp_replace (or unaccent
if available) in soql_builder.py.

Validation:
- new tests pass
- corpus 88/88 maintained
- caso MC-SEGD-001-2026 (CONSULTORÍA Cartagena): ahora matchea
```

---

## B3 — ENTITY-MISRESOLUTION-FALLBACK-001

**Tiempo:** 2-3 horas
**Pre-requisitos:** B2 cerrado
**Casos canónicos:** Paipa→Manizales, Medellín→INDER, Bolívar→ICBF

### Objetivo

Que `rewrite_alcald[ií]a` y `rewrite_gobernaci[oó]n` devuelvan `entidad_resolved=None` + `confidence=low` cuando no encuentran match canónico exacto, **en vez de** una entidad arbitraria con `confidence=high`. Adicionalmente, **dejar interfaz preparada** para ENTITY-ECOSYSTEM-EXPANSION-001 (Sprint 3).

### Archivos a tocar

- `app/core/entity_resolver.py` (modificar funciones de rewrite)
- `app/core/entity_types.py` (extender estructura de retorno si aplica)
- `tests/test_entity_resolver_fallback.py` (crear)

### Especificación funcional

#### 3.1 — Fix del fallback

Para `rewrite_alcaldia` y `rewrite_gobernacion`:

```python
def rewrite_alcaldia(text: str, gazetteer: Gazetteer) -> EntityResolution:
    # ... extraer "alcaldía de X" ...
    canonical_name = build_canonical_alcaldia_name(extracted_city)
    matches = gazetteer.exact_match(canonical_name)
    if matches:
        return EntityResolution(
            central_entity=matches[0],
            ecosystem_entities=[],   # llenado en Sprint 3
            method="rewrite_alcaldia",
            confidence="high",
        )
    # Fallback: NO devolver entidad arbitraria
    return EntityResolution(
        central_entity=None,
        ecosystem_entities=[],
        method="rewrite_alcaldia",
        confidence="low",
        clarification_needed=True,
        clarification_hint=f"No encontré '{extracted_text}' en el catálogo. ¿Cuál es el nombre oficial?",
    )
```

Mismo patrón para `rewrite_gobernacion`.

#### 3.2 — Interfaz preparada para ecosistema

Extender `EntityResolution` dataclass:

```python
@dataclass
class EntityResolution:
    central_entity: str | None        # antes: "value"
    ecosystem_entities: list[str]      # NUEVO: vacío por default, llenado en Sprint 3
    method: str
    confidence: Literal["high", "medium", "low"]
    like_value: str | None = None
    metadata: dict = field(default_factory=dict)
    clarification_needed: bool = False
    clarification_hint: str | None = None
```

**Mantener compatibilidad:** los campos `value` y `like_value` siguen funcionando para llamadores existentes. La nueva forma se va adoptando incremental en B4-B9.

### Tests (mínimos)

```python
# tests/test_entity_resolver_fallback.py

def test_alcaldia_paipa_returns_null_not_manizales():
    """Paipa no está en gazetteer canónico → no devolver Manizales arbitraria."""
    resolution = entity_resolver.resolve("alcaldía de paipa")
    assert resolution.central_entity is None
    assert resolution.confidence == "low"
    assert resolution.clarification_needed is True

def test_alcaldia_medellin_returns_null_not_inder():
    """Medellín entidad central no está exacta → no devolver INDER."""
    # ASUMIMOS que MUNICIPIO DE MEDELLIN está en gazetteer como canónico.
    # Si SÍ está, este test verifica que devuelve eso, no INDER.
    resolution = entity_resolver.resolve("alcaldía de medellín")
    if resolution.central_entity is not None:
        assert "MEDELLIN" in resolution.central_entity.upper()
        assert "INDER" not in resolution.central_entity.upper()
        assert "DEPORTES" not in resolution.central_entity.upper()

def test_gobernacion_bolivar_returns_null_not_icbf():
    resolution = entity_resolver.resolve("gobernación de bolívar")
    if resolution.central_entity is not None:
        assert "ICBF" not in resolution.central_entity.upper()
        assert "BIENESTAR FAMILIAR" not in resolution.central_entity.upper()

def test_ecosystem_entities_empty_in_sprint_1a():
    """Sprint 1A: ecosystem_entities siempre vacío. Sprint 3 lo llena."""
    resolution = entity_resolver.resolve("alcaldía de medellín")
    assert resolution.ecosystem_entities == []
```

### Reglas locales

- NO modificar el comportamiento de scan_exact / fuzzy de entidades NO municipales (eso funciona).
- Mantener `like_value` para retrocompatibilidad con orchestrator y formatter.

### Criterios de aceptación

- `pytest tests/test_entity_resolver_fallback.py` verde.
- Regla G1 verde.
- Smoke: `"contratos de la alcaldía de paipa"` → `needs_clarification=True` con mensaje claro, NO query a Manizales.

### Commit

```
fix(sprint1a): ENTITY-MISRESOLUTION-FALLBACK-001 — null+low instead of random+high

rewrite_alcaldia and rewrite_gobernacion returned arbitrary
municipal/departmental entities with confidence=high when input
didn't match gazetteer canonically. Operational deception.

Fix: when canonical match fails, return central_entity=None +
confidence=low + clarification_needed=True. Also extended
EntityResolution dataclass with ecosystem_entities field (empty
in Sprint 1A, to be filled in Sprint 3).
```

---

## B4 — PAGINATION-CUTS-OFF-RELEVANT-001

**Tiempo:** 3 horas
**Pre-requisitos:** B3 cerrado
**Caso canónico:** Doncello CMC-2026-019 (1,647 resultados totales en Caquetá mejoramiento)

### Objetivo

Cuando `total_count > LIMIT`, exponer al usuario que está viendo una porción y permitir paginación o refinamiento. No ocultar silenciosamente.

### Archivos a tocar

- `app/core/response_policy.py` (agregar advertencia cuando total_count > limit)
- `app/core/formatter.py` (mostrar "Te muestro N de TOTAL más recientes")
- `app/core/observer.py` (extender UniverseInsights con `truncation_warning`)
- `tests/test_pagination_transparency.py` (crear)

### Especificación funcional

Cuando `total_count > limit` (default 50):

1. `observer.UniverseInsights` agrega `truncation_warning=True` y `truncation_ratio=limit/total_count`.
2. `formatter` muestra en el header: `"Encontré 1,647 resultados. Te muestro los 50 más recientes — hay 1,597 más."`
3. `response_policy` agrega una sugerencia automática: `"Para reducir resultados, agrega filtros: año, modalidad, rango de valor, o entidad específica."`

### Tests (mínimos)

```python
# tests/test_pagination_transparency.py

def test_truncation_warning_when_total_exceeds_limit():
    insights = observer.compute_insights(
        rows=[...50 mock rows...],
        total_count=1647,
        limit=50,
    )
    assert insights.truncation_warning is True
    assert 0.02 < insights.truncation_ratio < 0.04  # ~50/1647

def test_no_truncation_warning_when_total_fits():
    insights = observer.compute_insights(rows=[...10 mock rows...], total_count=10, limit=50)
    assert insights.truncation_warning is False

def test_formatter_header_shows_truncation():
    response = formatter.format(
        rows=[...],
        total_count=1647,
        limit=50,
        params={"objeto": ["mejoramiento"], "departamento_resolved": "Caquetá"},
    )
    assert "1,647" in response or "1.647" in response
    assert "50" in response
    assert "más" in response.lower()
```

### Reglas locales

- No cambiar el LIMIT 50 por default. El cambio es solo en transparencia, no en cantidad.
- No tocar `secop_client.py` (capa de fetching).

### Criterios de aceptación

- Tests verdes.
- Regla G1 verde.
- Smoke: `"procesos de mejoramientos en caquetá"` muestra header con "1647" y aviso de truncación.

---

## B5 — LLM-EXPANSION-AND-001

**Tiempo:** 2 horas
**Pre-requisitos:** B4 cerrado
**Caso canónico:** Olaya residuos (`"residuos sólidos"` + `"manejo de residuos"` AND obligatorio)

### Objetivo

Cuando el LLM emite múltiples bigrams sinónimos en `objeto`, unirlos con OR en SoQL (alternativas), no con AND (obligatorios).

### Archivos a tocar

- `app/core/llm_handler.py` (post-procesar tool_call output)
- `app/core/soql_builder.py` (soportar shape `objeto_or`)
- `tests/test_llm_expansion_or.py` (crear)

### Especificación funcional

#### Detección de expansión sinónima

Cuando `objeto = ["A B", "C D"]` (dos bigrams con overlap léxico parcial), considerarlos alternativas:

```python
def is_synonym_expansion(objeto: list[str]) -> bool:
    """Heurística: 2 bigrams con overlap de ≥1 token = expansión sinónima."""
    if len(objeto) != 2 or not all(" " in t for t in objeto):
        return False
    tokens1 = set(objeto[0].lower().split())
    tokens2 = set(objeto[1].lower().split())
    return len(tokens1 & tokens2) >= 1
```

Si es expansión sinónima, post-procesar a shape `objeto_or`:

```python
{"objeto_or": ["residuos sólidos", "manejo de residuos"]}
# en vez de:
{"objeto": ["residuos sólidos", "manejo de residuos"]}  # interpretado como AND
```

#### SoQL para objeto_or

```sql
( LIKE '%residuos sólidos%' OR LIKE '%manejo de residuos%' )
```

en vez de:

```sql
( LIKE '%residuos sólidos%' ) AND ( LIKE '%manejo de residuos%' )
```

### Tests (mínimos)

```python
# tests/test_llm_expansion_or.py

def test_detect_synonym_expansion():
    assert llm_handler.is_synonym_expansion(["residuos sólidos", "manejo de residuos"])
    assert not llm_handler.is_synonym_expansion(["residuos", "sólidos"])  # tokens sueltos
    assert not llm_handler.is_synonym_expansion(["mantenimiento", "vial"])

def test_soql_or_shape():
    params = {"dataset": "procesos", "objeto_or": ["residuos sólidos", "manejo de residuos"]}
    soql = soql_builder.build_query(params)
    assert " OR " in soql
    assert "residuos sólidos" in soql
    assert "manejo de residuos" in soql
    # NO debe ser AND
    assert "manejo de residuos%') AND" not in soql
```

### Reglas locales

- Mantener el shape `objeto` (lista AND) intacto para casos no-sinónimos.
- Heurística simple (overlap léxico); evitar regex complejas para detectar sinonimia.

### Criterios de aceptación

- Tests verdes.
- Regla G1 verde.
- Smoke: query "residuos sólidos manejo de residuos" con LLM activado → encuentra al menos 1 resultado.

---

## B6 — OPP-TIMEOUT-001

**Tiempo:** 2 horas
**Pre-requisitos:** B5 cerrado

### Objetivo

Cuando SECOP da timeout, no reportar "0 resultados" sino "no pude confirmar". UX transparente.

### Archivos a tocar

- `app/core/orchestrator.py` (función `execute_query`, manejo de excepción de timeout)
- `app/core/response_policy.py` (nuevo camino `timeout_response()`)
- `tests/test_timeout_transparency.py` (crear)

### Especificación funcional

En `execute_query`, cuando `secop_client.query()` lanza `httpx.TimeoutException` o equivalente:

```python
# En vez de:
total_count = 0
rows = []
# (y dejar que el formatter diga "No encontré resultados")

# Hacer:
state["risk_flag_timeout"] = True
state["total_count"] = None  # NO 0, NO se confunde con "no hay"
state["rows"] = []
# ResponsePolicy.build detecta risk_flag_timeout y emite timeout_response()
```

`timeout_response()` produce:
```
SECOP no respondió a tiempo. No puedo confirmar si hay o no resultados.
La consulta era: {resumen de filtros}

Puedo intentar:
1. Reducir el alcance (agregar más filtros)
2. Buscar en una ventana de fechas más pequeña
3. Reintentar en unos minutos
```

### Tests

```python
# tests/test_timeout_transparency.py

def test_timeout_does_not_become_zero():
    # Mock secop_client.query lanzando TimeoutException
    with mock.patch("app.core.secop_client.query", side_effect=httpx.TimeoutException("timeout")):
        result = workflow.run_query("contratos de mantenimiento en bogotá")
    assert result["risk_flag_timeout"] is True
    assert result["total_count"] is None
    assert "no pude confirmar" in result["response"].lower() or \
           "no respondió" in result["response"].lower()
    assert "0 resultados" not in result["response"]
    assert "No encontré" not in result["response"]
```

### Criterios de aceptación

- Test verde.
- Regla G1 verde.

---

## B7 — MULTI-AND-OVER-RESTRICTIVE-001

**Tiempo:** 3 horas
**Pre-requisitos:** B6 cerrado
**Caso canónico:** Putumayo aseo (6 tokens en AND)

### Objetivo

Cuando `objeto` tiene ≥4 tokens, degradar AND a OR sobre los tokens menos específicos. Mantener AND solo sobre tokens raros.

### Archivos a tocar

- `app/core/soql_builder.py` (lógica de degradación)
- `app/core/intent_vocabulary.py` (lista de tokens "comunes")
- `tests/test_multi_and_demotion.py` (crear)

### Especificación funcional

Definir `COMMON_TOKENS` (palabras frecuentes en objetos contractuales):
```python
COMMON_TOKENS = {
    "servicios", "servicio", "general", "generales",
    "mantenimiento", "suministro", "suministros",
    "prestacion", "prestación", "apoyo", "gestión",
    "contratacion", "contratación", "adquisicion", "adquisición",
}
```

Lógica:
```python
def _split_objeto_by_rarity(objeto: list[str]) -> tuple[list[str], list[str]]:
    """Separa objeto en (raros que van AND, comunes que van OR-juntos)."""
    raros = [t for t in objeto if t.lower() not in COMMON_TOKENS]
    comunes = [t for t in objeto if t.lower() in COMMON_TOKENS]
    return raros, comunes

def build_objeto_where(objeto: list[str]) -> str:
    if len(objeto) < 4:
        # comportamiento actual: todos en AND
        return " AND ".join(like(t) for t in objeto)
    raros, comunes = _split_objeto_by_rarity(objeto)
    parts = [like(t) for t in raros]  # AND sobre raros
    if comunes:
        parts.append("(" + " OR ".join(like(t) for t in comunes) + ")")  # OR entre comunes
    return " AND ".join(parts)
```

### Tests

```python
# tests/test_multi_and_demotion.py

def test_three_or_less_tokens_use_full_and():
    params = {"dataset": "procesos", "objeto": ["mantenimiento", "vial"]}
    soql = soql_builder.build_query(params)
    # 2 tokens → ambos en AND, sin degradación
    assert soql.count(" AND ") >= 2  # depto + objeto

def test_six_tokens_degrades_common_to_or():
    params = {"dataset": "procesos",
              "objeto": ["servicios", "general", "aseo", "cafeteria", "jardineria", "mantenimiento"]}
    soql = soql_builder.build_query(params)
    # raros (aseo, cafeteria, jardineria) en AND
    # comunes (servicios, general, mantenimiento) entre OR
    assert "aseo" in soql
    assert "cafeteria" in soql
    assert "jardineria" in soql
    assert " OR " in soql  # debe haber al menos un OR
```

### Reglas locales

- La lista `COMMON_TOKENS` empieza pequeña (10-15 términos). Crecerá con feedback.
- No tocar si `len(objeto) < 4`.

### Criterios de aceptación

- Tests verdes.
- Regla G1 verde.
- Smoke: query Putumayo "ASEO, CAFETERÍA, JARDINERÍA Y MANTENIMIENTO" → encuentra UNIPUTUMAYO-MC-013-2026.

---

## B8 — FOLLOWUP-VALUE-INHERIT-001 + FOLLOWUP-ENTITY-PURGE-001

**Tiempo:** 2-3 horas
**Pre-requisitos:** B7 cerrado
**Casos canónicos:** Yarumal (valor heredado), gobernación Bolívar→Magangué (entidad heredada)

### Objetivo

Cuando el follow-up cambia el topic completamente o el scope territorial, purgar modificadores heredados: `valor_min`, `valor_max`, `modalidad`, `entidad_resolved`.

### Archivos a tocar

- `app/core/followup_engine.py` (heurística de overlap + purga)
- `tests/test_followup_purge.py` (crear)

### Especificación funcional

Heurística de overlap de tokens entre turno actual y anterior:

```python
def _topics_overlap(prev_objeto: list[str], curr_objeto: list[str]) -> bool:
    """≥1 token en común = mismo dominio temático."""
    if not prev_objeto or not curr_objeto:
        return False
    s1 = set(t.lower() for t in prev_objeto)
    s2 = set(t.lower() for t in curr_objeto)
    return bool(s1 & s2)

def _scope_changed(prev_params, curr_params) -> bool:
    """Cambio de scope territorial: depto → ciudad, ciudad → depto, etc."""
    prev_geo = (prev_params.get("departamento_resolved"), prev_params.get("ciudad"))
    curr_geo = (curr_params.get("departamento_resolved"), curr_params.get("ciudad"))
    return prev_geo != curr_geo and curr_geo != (None, None)

# En merge:
if not _topics_overlap(prev["objeto"], curr["objeto"]):
    # Topic distinto → purgar modificadores monetarios
    curr.pop("valor_min", None)
    curr.pop("valor_max", None)
    curr.pop("modalidad", None)

if _scope_changed(prev, curr):
    # Scope cambió → purgar entidad heredada
    curr.pop("entidad", None)
    curr.pop("entidad_resolved", None)
    curr.pop("entidad_resolution", None)
```

### Tests

```python
# tests/test_followup_purge.py

def test_value_purged_when_topic_changes():
    """residuos sólidos ≤20M → silvopastoriles: purgar valor_max."""
    prev = {"objeto": ["residuos", "sólidos"], "valor_max": 20_000_000}
    curr = {"objeto": ["silvopastoriles"]}
    merged = followup_engine.merge(prev, curr)
    assert "valor_max" not in merged

def test_value_kept_when_topic_overlaps():
    """transporte escolar ≤30M → 'menos de 30M también de Huila': mantener valor."""
    prev = {"objeto": ["transporte", "escolar"], "valor_max": 30_000_000}
    curr = {"objeto": ["transporte", "escolar"], "departamento_resolved": "Huila"}
    merged = followup_engine.merge(prev, curr)
    assert merged.get("valor_max") == 30_000_000

def test_entity_purged_when_scope_changes():
    """gobernación Bolívar → 'magangue': purgar entidad heredada."""
    prev = {"entidad_resolved": "GOBERNACION DE BOLIVAR", "departamento_resolved": "Bolívar"}
    curr = {"ciudad": "Magangué"}
    merged = followup_engine.merge(prev, curr)
    assert "entidad_resolved" not in merged
    assert merged.get("ciudad") == "Magangué"
```

### Criterios de aceptación

- Tests verdes.
- Regla G1 verde.
- Smoke: dos turnos secuenciales reproducen Yarumal y gobernación→Magangué sin contaminación cruzada.

---

## B9 — STATE-CATALOG-INCOMPLETE-001

**Tiempo:** 2 horas + audit
**Pre-requisitos:** B8 cerrado + datos de B0
**Caso canónico:** Mariquita SAMC-JCT-006 (estado real puede no estar en lista)

### Objetivo

Completar el catálogo de `estado_del_procedimiento` en la familia `oferta_abierta` basado en datos reales de B0.

### Archivos a tocar

- `app/core/estado_families.py` (extender lista `oferta_abierta`)
- `tests/test_state_catalog.py` (extender con valores reales)

### Especificación funcional

1. Revisar `baseline/B0_estados_p6dx.json`.
2. Identificar valores que representan procesos vigentes para postular pero no están en `oferta_abierta`.
3. Añadirlos a la familia.

Hipótesis (a verificar con la data real de B0): además de `Publicado`, `Borrador`, `Abierto`, pueden faltar: `Convocatoria` (visto en licitaciones.info), `En Proceso`, `Presentación`, otros.

### Reglas locales

- Solo agregar valores que la data real confirme. No inventar.
- Documentar en el commit los valores nuevos con el conteo de B0.

### Criterios de aceptación

- Tests verdes con valores reales.
- Regla G1 verde.

---

## Cómo el revisor valida la entrega

Al final de Sprint 1A (después de B9), el revisor (Claude + humano) verifica:

### Macro

```bash
# Estado final
pytest -q                                           # esperado: ≥620 passed (579 + ~40 nuevos)
python scripts/run_query_corpus.py --mode all       # 88/88 sin regresión
make lint-core                                       # verde
git log --oneline B0..HEAD                          # 10 commits (B0..B9), uno por bloque
diff baseline/B0_pytest.txt baseline/B9_pytest_after.txt  # solo passed→passed o passed→added
```

### Casos canónicos (smoke manual)

| Bug | Query a probar | Resultado esperado |
|---|---|---|
| MIN-MAX | "transporte escolar huila no mayores a 30M" | IP-017-2026 Palermo aparece |
| ACCENT | "consultoría cartagena" | MC-SEGD-001-2026 aparece |
| ENTITY-MISRESOLUTION | "alcaldía de paipa" | Pide aclaración, NO Manizales |
| PAGINATION | "mejoramientos caquetá" | Header dice "50 de 1,647 más" |
| LLM-EXPANSION-AND | "manejo de residuos sólidos" con LLM activo | >0 resultados |
| TIMEOUT | (mock timeout) | "no pude confirmar", NO "0 resultados" |
| MULTI-AND | "aseo cafetería jardinería mantenimiento" | UNIPUTUMAYO aparece |
| FOLLOWUP-VALUE-INHERIT | turno A: "≤20M residuos"; turno B: "silvopastoriles antioquia" | turno B sin valor_max |
| STATE-CATALOG | (estados nuevos de B0) | filtro oferta_abierta incluye los estados reales |

### Métrica final agregada

```bash
python scripts/run_query_corpus.py --mode all --json > sprint1a_after.json
python scripts/quality_report.py \
    --before baseline/M0_corpus.json \
    --after sprint1a_after.json
```

Esperado: transiciones FAIL→PASS positivas, 0 transiciones PASS→FAIL.

---

## Lo que NO está en Sprint 1A

Aplazado a Sprint 2-3:

- **CITY-TO-ENTITY-AUTOPROMOTION-001** → Sprint 2 (QueryPlan completo).
- **TEMPORAL-MONTH-PARSING-001** → Sprint 2.
- **POLYSEMIC-TOPIC-001** → Sprint 2 (DialoguePolicy completo).
- **BIDDER-CATALOG-AND-001** → Sprint 2.
- **PROCUREMENT-ACTION-001** → Sprint 2.
- **DEDUP-PROCESS-001** → Sprint 2-3.
- **RELEVANCE-PHRASE-001** → Sprint 3 (Relevance).
- **OPP-INTENT-001** → Sprint 2 (DialoguePolicy detecta interrogativos como bidder).
- **ENTITY-MULTI-REGIONAL-001** → Sprint 3 (catálogo SENA/ICBF/ministerios).
- **ENTITY-ECOSYSTEM-EXPANSION-001** → Sprint 3 (catálogo de ecosistemas).
- **NUMBER-CONTEXT-PHRASE-001** → Sprint 2 (QueryPlan post-extracción scrubbing).
- **NO-FOLLOWUP-DETECTION-001** → Sprint 2 (DialoguePolicy detecta quejas).
- **NIT-AMOUNT-CONFUSION-001** → Sprint 2.
- **MORPHO-EXPANSION-INCONSISTENT-001** → Sprint 2.
- **MOJIBAKE-DISPLAY-001** → Sprint 4 (formatter).
- **HEADER-TOPIC-OMISSION-001** → Sprint 4.
- **AUTO-FALLBACK-WITHOUT-QUALITY-001** → Sprint 4 (ResponsePolicy).
- **SOURCE-COVERAGE-001** → MEDIUM, diagnóstico UX en Sprint 2.

---

## Si algo sale mal — escalación al revisor

Codex **debe** parar y consultar antes de continuar si:

- Cualquier bloque hace fallar Regla G1 y la causa no es obvia.
- Un fix requiere modificar archivos fuera de la lista declarada del bloque.
- Aparece la necesidad de instalar una librería nueva.
- Un test escrito según especificación no se puede hacer pasar sin "trampa" (asserts laxos, mocks elaborados).
- La especificación de un bloque resulta contradictoria con código existente que no se anticipó.

Comunicación esperada al revisor:

```
Bloque: B<N>
Estado: BLOQUEADO
Problema: <descripción precisa>
Hipótesis: <causa probable>
Archivos involucrados: <rutas>
Necesito decisión sobre: <opciones A / B / C>
```

---

## Resumen del cronograma

| Bloque | Bug | Tiempo | Acumulado |
|---|---|---:|---:|
| B0 | Baseline + audit | 0.5h | 0.5h |
| B1 | MIN-MAX-INVERSION | 3h | 3.5h |
| B2 | ACCENT-NORMALIZATION | 3h | 6.5h |
| B3 | ENTITY-MISRESOLUTION-FALLBACK | 3h | 9.5h |
| B4 | PAGINATION-CUTS-OFF | 3h | 12.5h |
| B5 | LLM-EXPANSION-AND | 2h | 14.5h |
| B6 | OPP-TIMEOUT | 2h | 16.5h |
| B7 | MULTI-AND-OVER-RESTRICTIVE | 3h | 19.5h |
| B8 | FOLLOWUP-VALUE+ENTITY-PURGE | 3h | 22.5h |
| B9 | STATE-CATALOG-INCOMPLETE | 2h | 24.5h |

**Total: ~25 horas de implementación + tests.** Distribuible en 5-8 días de trabajo enfocado.

---

## Una última nota al ejecutor

Este sprint es **defensa antes que ataque**. No estamos construyendo features nuevas; estamos arreglando bugs que destruyen el modelo de negocio. La métrica de éxito no es "cuántas líneas escribí" sino "cuántos de los 9 casos canónicos del corpus humano ahora funcionan".

Si al final de B9 los 9 casos canónicos pasan **y** la suite no regresa, Sprint 1A está cerrado. Sprint 2 (QueryPlan estructural) puede arrancar.

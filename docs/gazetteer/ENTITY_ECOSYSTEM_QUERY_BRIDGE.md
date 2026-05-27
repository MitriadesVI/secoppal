# Entity Ecosystem Query Bridge

**Status:** Documentado — integrado en pipeline principal
**Files:** `app/core/entity_ecosystem_router.py`
**Tests:** `tests/test_entity_ecosystem_router.py` + `tests/test_entity_ecosystem_query_bridge.py`
**Commits:** `d78ebf3` (bridge), `9bd7dda` (typo fix), `85b07cf` (hardening)
**ADR:** ADR-020

---

## 1. ¿Qué es el Bridge?

El Entity Ecosystem Bridge es una capa de middleware entre el parser (`QueryRouter`) y el constructor de SoQL (`SoQLBuilder`) que resuelve ecosistemas institucionales (ICBF, SENA) a filtros de entidad compuestos.

Vive en `entity_ecosystem_router.py` y se inyecta en el pipeline vía el orchestrator (`orchestrator.py`). No es parte de `query_router.py` (anti-monolito, explícitamente prohibido tocar).

---

## 2. Flujo Completo

```
raw_query → parse() → QueryPlan
  → normalize_institutional_acronyms()
  → resolve_entity_ecosystem()
  → build_entity_ecosystem_filter()
  → scrub_institutional_tokens_from_objeto()
  → SoQLBuilder.build_query()
  → SECOP
```

### 2.1 normalize_institutional_acronyms()

Corrige typos en acrónimos institucionales antes de que lleguen al resolver:

| Entrada | Salida | Regla |
|---------|--------|-------|
| `icfb` | `icbf` | `\bicfb\b` → `icbf` |
| `ICFB` | `ICBF` | Case-insensitive |

Usa `re.sub` con `\b` word boundary para evitar falsos positivos como "bicfbtest".

### 2.2 resolve_entity_ecosystem()

Router que determina qué ecosistema aplicar:

1. Si `params["entidad"]` contiene "icbf" y un departamento → delegar a `icbf_resolver.resolve_regional()`
2. Si `params["objeto"]` contiene "icbf" y un departamento → mover a entidad, delegar
3. Si `params["entidad"]` o `params["objeto"]` matchean `_is_sena_mention()` → delegar a `sena_resolver.resolve()`
4. Si no hay match → `resolution_type = "none"`, no modifica params

### 2.3 build_entity_ecosystem_filter()

Traduce `EcosystemResolution` a parámetros que el SoQL builder entiende:

| Modo | Parámetros que setea |
|------|---------------------|
| `icbf_national` | `entidad_in_candidates` (32 nombres oficiales), `departamento_in_candidates` (32 deptos) |
| `icbf_regional` | `entidad_like`, `departamento` |
| `sena_national` | `entidad_in_candidates` (national entities), advertencia |
| `sena_regional_ecosystem` | `entidad_contains_all` (["SENA", "MAGDALENA"]), `departamento` |

### 2.4 scrub_institutional_tokens_from_objeto()

Cuando un ecosistema institucional se activa, remueve vocabulario institucional de `params["objeto"]` para que no contamine el `LIKE`:

Tokens scrub: `icbf`, `sena`, `instituto`, `bienestar`, `familiar`, `servicio`, `nacional`, `aprendizaje`

Ejemplo: "contratos del ICBF en magdalena" → objeto queda vacío (se buscaba por entidad, no por tema).

---

## 3. Reglas Críticas

1. **No override:** `departamento_resolved` ni `entidad_resolved` existentes no se sobrescriben.
2. **Filtros sustanciales:** `entidad_contains_all` e `entidad_in_candidates` cuentan como filtros válidos (evitan WHERE 1=1).
3. **Scrub solo si hay ecosistema activo:** no se remueven tokens institucionales si el ecosistema no resolvió.
4. **Frontera léxica SENA:** `_is_sena_mention()` usa regex `\bsena\b` que rechaza "senado"/"senador" pero acepta "sena".

### 3.1 ¿Por qué `\bsena\b`?

"senado" y "senador" son términos frecuentes en SECOP (CONGRESO DE LA REPÚBLICA - HONORABLE SENADO, etc.). Sin word boundary, `contains_all=["SENA", "MAGDALENA"]` matchearía "CONGRESO... SENADO... MAGDALENA" como falso positivo.

---

## 4. Regresión: CORPUS-SM011

El corpus de humo (SMOKE) incluye CORPUS-SM011 que prueba el flujo completo:

```yaml
- id: CORPUS-SM011
  query: "procesos de mantenimiento del icbf en atlántico"
  expected:
    entity_ecosystem_resolution: icbf_regional
    departamento: Atlantico
    objeto_not_contains: ["icbf"]
```

Esto verifica que:
1. "icbf" se detecta como ecosistema
2. "atlántico" se resuelve a departamento
3. "icbf" se scrube del objeto

---

## 5. Integración en SoQL Builder

El SoQL builder recibe los parámetros del bridge y genera:

```sql
-- Caso SENA regional ecosystem
WHERE (
  UPPER(unaccent(nombre_entidad)) LIKE UPPER(unaccent('%SENA%'))
  AND UPPER(unaccent(nombre_entidad)) LIKE UPPER(unaccent('%MAGDALENA%'))
)

-- Caso ICBF regional
WHERE (
  UPPER(unaccent(nombre_entidad)) LIKE UPPER(unaccent('%ICBF REGIONAL MAGDALENA%'))
)

-- Caso ICBF nacional (entidad_in_candidates)
WHERE (
  nombre_entidad IN ('ICBF REGIONAL MAGDALENA Grupo ...', 'ICBF REGIONAL ATLANTICO ...', ...)
)
```

Los filtros de ecosistema se combinan con AND respecto a otros filtros (objeto, valor, fecha, etc.).

---

## 6. Próximos Pasos

| Posible ecosistema | Prioridad | Complejidad |
|-------------------|-----------|-------------|
| Gobernaciones | Media | Baja (1:1 por depto, similar a ICBF) |
| ESEs (Empresas Sociales del Estado) | Baja | Alta (múltiples por municipio) |
| Hospitales públicos | Baja | Alta (nombres muy variables) |
| Superintendencias | Baja | Media (pocas entidades, nombres estables) |

Cada nuevo ecosistema solo agrega:
- Un resolver puro en `app/core/` (sin SECOP calls)
- Un JSON data en `app/data/`
- Tests en `tests/`
- Una entrada en el router de `entity_ecosystem_router.py`

Sin tocar `query_router.py`, `orchestrator.py` (solo registro) ni `soql_builder.py`.

---

## 7. Links

- ADR-020: decisión arquitectónica del bridge → `docs/decisions_log.md`
- ICBF model: `docs/gazetteer/ICBF_REGIONAL_MODEL.md`
- SENA model: `docs/gazetteer/SENA_ENTITY_MODEL.md`
- SENA audit: `docs/gazetteer/SENA_ENTITY_AUDIT.md`

# DECISIONES TÉCNICAS — SECOPPAL Entity Ecosystem Sprint (G7A–G7C.2)

## G7A — ICBF Regional Catalog (2026-05-26)

**Decisión:** Crear catálogo ICBF separado del gazetteer (`icbf_regionals.json`) y resolver puro (`icbf_resolver.py`).

**Rationale:** ICBF es estructuralmente más simple que SENA (1:1 regional/depto vs SENA multi-centro). Crear resolver puro primero, integrar después.

**Archivos:** `app/data/icbf_regionals.json`, `app/core/icbf_resolver.py`, `tests/test_icbf_regionals.py`

**Commit:** `9bbe79b`

---

## G7B — SENA Entity Ecosystem (2026-05-26)

**Decisión:** SENA no es 1:1 por departamento. Tiene múltiples entidades por departamento: parent + admin groups + centers + intercentros.

**Patrón adoptado:** `contains_all` (LIKE compuesto) para búsqueda ecosistema regional, NO entity name matching.

**Archivos:** `scripts/audit_sena_entities.py`, `app/data/sena_units.json`, `app/core/sena_resolver.py`, `tests/test_sena_resolver.py`

**Commit:** `a73e741`

---

## G7C — Entity Ecosystem Bridge (2026-05-26)

**Decisión:** Integrar ICBF/SENA al pipeline sin tocar `query_router.py`. El bridge opera conceptualmente *después del parser* y *antes del EntityResolver/SoQLBuilder*.

**Reglas críticas:**
- No override `departamento_resolved` ni `entidad_resolved` existentes.
- `entidad_contains_all` e `entidad_in_candidates` cuentan como filtros sustanciales (evitan WHERE 1=1).

**Archivos:** `app/core/entity_ecosystem_router.py`, `app/core/orchestrator.py`, `app/core/soql_builder.py`

**Commit:** `d78ebf3`

---

## G7C.1 — ICFB Typo Fix (2026-05-26)

**Decisión:** Normalizar acrónimos institucionales frecuentes (`icfb` → `icbf`) y consumir/scrub tokens institucionales del objeto cuando un ecosistema se activa.

**Implementación:**
- `INSTITUTIONAL_ACRONYM_FIXES` con reemplazo por palabra completa (case-insensitive).
- `scrub_institutional_tokens_from_objeto()` remueve vocabulario institucional (`icbf`, `sena`, `bienestar`, etc.) de `params["objeto"]`.

**Archivos:** `app/core/entity_ecosystem_router.py`, `app/core/orchestrator.py`

**Commit:** `9bd7dda`

---

## G7C.2 — Hardening Post-ICFB Fix (2026-05-26)

**Decisión:** Endurecer el matching para evitar falsos positivos y corrupción de palabras.

**Cambios:**
1. `normalize_institutional_acronyms()` usa `\b` word boundaries: "bicfbtest" intacto, "icfb magdalena" → "icbf magdalena".
2. `_is_sena_mention()` usa regex `\bsena\b`: "senado"/"senador" rechazados.
3. Corpus runner (`run_query_corpus.py`) evalúa `resolve_entity_ecosystem()` solo cuando el entry espera campos `entity_ecosystem_*`.

**Nota técnica sobre el runner:**
El modo `parser` del corpus ya no es "QueryRouter puro" cuando el caso declara ecosystem fields. Es una extensión consciente: el ecosistema vive conceptualmente *después del parser* y *antes del SoQL builder*. Para limpieza futura esto podría renombrarse conceptualmente a `parser_plus_ecosystem`.

**Archivos:** `app/core/entity_ecosystem_router.py`, `app/core/sena_resolver.py`, `scripts/run_query_corpus.py`, `tests/fixtures/query_corpus_v1.yaml`, `tests/test_entity_ecosystem_router.py`

**Commit:** `85b07cf`

---

## Reglas transversales del sprint

1. **No tocar `query_router.py`:** Toda integración va a través del orchestrator.
2. **No tocar `aliases_db.json`:** Los ecosistemas viven fuera del gazetteer.
3. **Append-only:** Los catálogos (ICBF, SENA) son archivos JSON separados.
4. **Pure resolvers:** Ningún resolver hace llamadas SECOP ni genera SoQL.
5. **Scrub institucional:** Tokens institucionales no son temas contractuales.

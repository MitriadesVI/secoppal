# Plan: Expansion Paulatina del Gazetteer (SECOPPAL)

**Estado:** Aprobado  
**Fecha:** 2026-05-25  
**Entidades actuales:** 509  
**Meta a 6 sprints:** ~1,000 entidades  
**Incremento por sprint:** ~50 entidades  
**Rama:** `feature/gazetteer-append-manager`

---

## 1. Diagnostico del Estado Actual

### Pipeline existente

| Script | Rol | Estado |
|--------|-----|--------|
| `scripts/fetch_top_entities.py` | Extrae top 500 de cada dataset | Legacy; no incremental |
| `scripts/generate_aliases.py` | Genera aliases con DeepSeek | Legacy; costoso; no incremental |
| `scripts/build_gazetteer.py` | Orquesta extract + aliases + stats | Rebuild completo; lento y caro |
| `scripts/add_bogota_local_alcaldias.py` | One-shot manual | Bien para casos especiales, no escalable |

### Gap real contra SECOP II (top 1000 merged)

- **En gazetteer:** 502 del top 1000 merged ya estan cubiertas
- **Fuera del gazetteer:** ~782 entidades del top 1000 merged faltan
- **Volumen del gap:** entidades con ~1,900 a ~6,300 procesos/contratos
- **Tipos faltantes:** ESEs, SENA regionales, ICBF regionales, alcaldias municipales, secretarias departamentales, superintendencias, fondos (DAPRE, UGPP, etc.)

### Problemas del rebuild completo

1. **Costo:** ~500 llamadas a DeepSeek = $5-10 USD por run
2. **Tiempo:** ~30-60 min con rate limiting
3. **Riesgo:** regenerar aliases para entidades existentes puede introducir cambios no deseados
4. **No hay append:** no se puede agregar 50 entidades nuevas sin tocar las 509 existentes

---

## 2. Principios del Plan

1. **Append-only:** nuevas entidades se agregan, las existentes no se tocan salvo bugfix
2. **Lote 00 primero — higiene:** limpiar aliases genericos peligrosos antes de expandir
3. **Validacion por lote:** cada lote de ~50 entidades pasa tests de regresion antes de merge
4. **Heuristica primero, LLM solo cuando hace falta:** ~80% de entidades tipicas (alcaldias, ESEs, SENA) tienen aliases generados por patrones sin LLM
5. **Lote 01 mixto:** no puro volumen; 70% volumen + 20% tipos criticos + 10% equidad territorial
6. **Cobertura cuantificada:** cada sprint reporta "% de entidades del top-1000 cubiertas"
7. **No romper el runtime:** `aliases_db.json` mantiene su estructura actual; los cambios son puros APPEND (o cleanup controlado en Lote 00)

---

## Estado: Post-G7D (Documentation Consolidation)

**Última actualización:** 2026-05-26  
**Rama:** `main` (feature/gazetteer-append-manager mergeada y borrada)  
**Corpus:** 89/89 PASS  
**Tests ecosystem + gazetteer:** 205 passed, 13 xfailed  
**Entidades en gazetteer:** 559 (Lote 01 completado)

---

## Sprints Completados

### Sprint A0/A5 — Lote 01: 50 entidades + gazetteer_manager.py

**Estado:** COMPLETED ✅  
**Commit:** `7781d6d`  
**SHA del baseline:** `d78ebf3` (commit base pre-ecosystems)

**Entregables:**
- `scripts/gazetteer_manager.py` — append manager con validación, diff, stats, hygiene
- `scripts/lote_01_top_gap_mixed.json` — 50 entidades (70% volumen + 20% tipos críticos + 10% equidad)
- `tests/test_gazetteer_lote_01.py` — test de colisión + cobertura
- `tests/test_gazetteer_manager.py` — tests del manager
- `docs/gazetteer/lote_01_debt.md` — deuda técnica identificada (13 entidades con <3 aliases, 13 xfail)
- `docs/gazetteer/LOTE_01_POST_AUDIT.md` — audit post-lote
- `baseline/gazetteer_append/` — 5 archivos de baseline pre-cambios

**Resultado:** 509 → 559 entidades en gazetteer. Suite completa estable.

### G7A — ICBF Regional Catalog (ecosystem pattern MVP)

**Estado:** COMPLETED ✅  
**Commit:** `9bbe79b`

**Entregables:**
- `app/data/icbf_regionals.json` — catálogo de 32 regionales ICBF
- `app/core/icbf_resolver.py` — resolver puro (sin SECOP calls)
- `tests/test_icbf_regionals.py` — 47 tests
- `docs/gazetteer/ICBF_REGIONAL_MODEL.md`

### G7B — SENA Entity Ecosystem (audit-first)

**Estado:** COMPLETED ✅  
**Commit:** `a73e741`

**Entregables:**
- `scripts/audit_sena_entities.py` — audit contra SECOP II live (89 entidades únicas)
- `app/data/sena_units.json` — catálogo con 7 tipos y 89 entidades
- `app/core/sena_resolver.py` — resolver con patrón `contains_all`
- `tests/test_sena_resolver.py` — 41 tests
- `docs/gazetteer/SENA_ENTITY_MODEL.md`
- `docs/gazetteer/SENA_ENTITY_AUDIT.md`

### G7C / G7C.1 / G7C.2 — Entity Ecosystem Bridge + typo fix + hardening

**Estado:** COMPLETED ✅  
**Commits:** `d78ebf3`, `9bd7dda`, `85b07cf`

**Entregables:**
- `app/core/entity_ecosystem_router.py` — bridge entre parser y SoQL builder
- `app/core/orchestrator.py` — integración (solo registro, ~24 líneas)
- `app/core/soql_builder.py` — soporte para `entidad_contains_all` y `entidad_in_candidates`
- `tests/test_entity_ecosystem_router.py` + `tests/test_entity_ecosystem_query_bridge.py` — 59 tests
- `docs/gazetteer/ENTITY_ECOSYSTEM_QUERY_BRIDGE.md` (G7D)

### G7C.2+ — UX Layer (chips, moneda, sidebar)

**Estado:** COMPLETED ✅  
**Commit:** `30b6c7a`

**Entregables:**
- `streamlit_app.py` — chips adaptativos, sidebar sticky, montos por percentil
- Chips con p33/p66/p90, threshold 5, widget keys estables (msg_idx-based)
- `format_cop()` exacto en cards, `format_money()` compacto en chat
- `docs/ux/CONTEXTUAL_REFINEMENT_FILTERS.md` (G7D)

### G7D — Documentation Consolidation (este sprint)

**Estado:** COMPLETED ✅  
**Commits:** `b7469d6` (pre), este commit

**Entregables:**
- `docs/decisions_log.md` — ADR-018 a ADR-022 agregados
- `docs/DECISIONES.md` — vaciado a redirect stub
- `docs/documento_tecnico_...md` §13 — vaciado a redirect, §13.1 actualizado
- `docs/gazetteer/ENTITY_ECOSYSTEM_QUERY_BRIDGE.md` — doc nuevo
- `docs/ux/CONTEXTUAL_REFINEMENT_FILTERS.md` — doc nuevo
- `docs/planes/gazetteer-expansion-plan.md` — este archivo, actualizado
- `docs/QUERY_CORPUS.md` — header actualizado a main

---

## Lecciones Aprendidas

### SENA audit-first vs ICBF catálogo plano

ICBF se resolvió con catálogo 1:1 (32 regionales → 32 entradas). SENA requirió audit contra SECOP II live antes de diseñar. El audit reveló 89 entidades en 7 tipos, con los `regional_admin_group` teniendo el doble de volumen que los `regional_parent`. Sin el audit, se habría diseñado un catálogo 1:1 incorrecto.

**Lección:** Para ecosistemas institucionales complejos, audit-first antes de diseñar el catálogo.

### Bug ICFB demostró valor de ecosystems

El typo "icfb" → "icbf" (corregido en G7C.1) no se habría corregido fácilmente sin el bridge: antes, cada query "icfb magdalena" entraba como objeto literal, generando `LIKE '%icfb%'` que producía 0 resultados. Con el bridge, es una línea en `INSTITUTIONAL_ACRONYM_FIXES` que normaliza antes de cualquier resolución.

**Lección:** Un punto único de normalización institucional es más valioso de lo que parece al diseñarlo.

### query_router.py mantenido intacto (anti-monolito)

El plan explícitamente prohibía tocar `query_router.py`. El bridge logró integrar ICBF + SENA + typo fix + hardening + corpus extension sin cambiar una línea del router. Esto valida el diseño anti-monolito.

**Lección:** La disciplina de no tocar query_router.py forzó una mejor arquitectura.

### 3 lugares de decisiones es 2 lugares de más

Antes de G7D, las decisiones vivían en tres lugares: `docs/decisions_log.md` (ADR format), `docs/DECISIONES.md` (G7A-G7C.2) y `docs/documento_tecnico_...md §13` (D1-D21). Cada uno con numeración distinta. Consolidar en decisions_log.md elimina el drift.

**Lección:** Un solo log de decisiones. Append-only. Sin mirror en otros docs.

---

## Próximos Pasos

| Prioridad | Sprint | Descripción |
|-----------|--------|-------------|
| Alta | B1/B2 | Lotes 02-03 de expansión de gazetteer (top volumen + tipos estratégicos) |
| Media | G7E | Gobernaciones como ecosistema (1:1 por depto, similar a ICBF) |
| Media | G7F | Bridge SENA — validación contra SECOP live del patrón `contains_all` |
| Baja | E | Regiones macro (caribe, pacífico, etc.) en catálogo geográfico separado |
| Baja | C | SECOP I audit (postergado hasta tener SECOP II coverage estable) |

---

## 4. Estructura de Entrada en Gazetteer (Schema Extendido)

```json
{
  "official_name": "ALCALDIA MUNICIPAL DE TURBACO BOLIVAR",
  "departamento": "Bolivar",
  "nit": "1234567890",
  "total_procesos": 9549,
  "total_contratos": 0,
  "source_datasets": ["procesos"],
  "entity_type": "alcaldia_municipal",
  "aliases": [
    "alcaldia de turbaco", "alcaldia turbaco",
    "municipio de turbaco", "municipio turbaco"
  ]
}
```

Campos:
- `official_name` (obligatorio): nombre exacto en SECOP
- `departamento` (obligatorio): departamento asignado
- `nit` (opcional): para validacion futura, dedup, SECOP I
- `total_procesos`, `total_contratos` (opcionales): metadatos del diff
- `source_datasets` (obligatorio): `["procesos"]`, `["contratos"]` o ambos
- `entity_type` (opcional): tipo inferido para tracking y heuristicas
- `aliases` (obligatorio): minimo 3, generados heuristico o LLM

---

## 5. Fases de Ejecucion

### Sprint A — Gazetteer Manager + Higiene + Lote 01

#### A0 — Baseline (HECHO)

| Check | Resultado |
|-------|-----------|
| pytest entity_resolver + query_router | 84 passed |
| query corpus | 88/88 PASS |
| lint-core | limpio |
| commit | `91f0d49` |

#### A1 — Gazetteer Manager (G1)

**Entregable:** `scripts/gazetteer_manager.py` — CLI con 4 comandos.

**Comando `diff`:**
```bash
python scripts/gazetteer_manager.py diff --top 1000 --format markdown
```
- Consulta SECOP live (ambos datasets, top N merged)
- Compara contra `aliases_db.json` por `official_name` normalizado
- Reporta entidades faltantes ordenadas por volumen descendente
- Output: tabla markdown con columns `rank | volumen | nombre oficial | departamento | tipo inferido | prioridad`

**Tipo inferido:** heuristica simple basada en palabras clave del nombre:
- Contiene "ALCALDIA" + "MUNICIPAL" → `alcaldia_municipal`
- Contiene "ESE" + "HOSPITAL" → `ese_hospital`
- Contiene "SENA" + "REGIONAL" → `sena_regional`
- Contiene "ICBF" + "REGIONAL" → `icbf_regional`
- Contiene "SECRETARIA" → `secretaria`
- Contiene "FONDO" → `fondo`
- Contiene "CORPORACION" + "AUTONOMA" → `corporacion_autonoma`
- Contiene "MINISTERIO" → `ministerio`
- Contiene "SUPERINTENDENCIA" → `superintendencia`
- Contiene "GOBERNACION" → `gobernacion`
- Default → `otro`

**Prioridad:**
- `P0` = top 100 faltantes por volumen
- `P1` = entidades de departamentos con <5 entidades en gazetteer
- `P2` = resto

**Comando `append`:**
```bash
# Modo A: desde lista manual
python scripts/gazetteer_manager.py append --from-list scripts/lote_01.json --aliases heuristic

# Modo B: top N automatico del gap
python scripts/gazetteer_manager.py append --top 50 --start-at 501 --aliases heuristic

# Modo C: con LLM para entidades no-tipicas
python scripts/gazetteer_manager.py append --from-list scripts/lote_especial.json --aliases llm
```

- Lee `aliases_db.json`
- Verifica duplicados por `official_name` normalizado
- Genera aliases (heuristico o LLM segun flag)
- Agrega al array existente
- Guarda backup en `app/data/aliases_db.json.bak.YYYYMMDD`
- Escribe `aliases_db.json` con indent=2

**Comando `validate`:**
```bash
python scripts/gazetteer_manager.py validate --tests tests/test_entity_resolver.py
```

- Corre tests indicados
- Test adicional inline: para cada entidad nueva, verificar que ningun alias genera falso positivo en entidades existentes (colision scan)
- Falla si:
  1. Algun test existente falla
  2. Colision scan detecta overlap problematico
  3. Entidad nueva tiene <3 aliases

**Comando `stats`:**
```bash
python scripts/gazetteer_manager.py stats
```

- Total entidades, total aliases, promedio aliases/entidad
- Distribucion por departamento
- Distribucion por tipo inferido
- Coverage vs top-1000: X/1000 cubiertos
- Coverage vs top-2000: X/2000 cubiertos
- Entidades sin aliases (deberia ser 0)

#### A2 — Heuristic Alias Generator (G2)

**Entregable:** `scripts/generate_heuristic_aliases.py` — modulo reutilizable.

Patrones por tipo:

```python
PATTERNS = {
    "alcaldia_municipal": [
        "alcaldia de {nombre}", "alcaldia {nombre}",
        "municipio de {nombre}", "municipio {nombre}",
        "alcaldia municipal de {nombre}", "{nombre} alcaldia",
    ],
    "ese_hospital": [
        "ese {nombre}", "hospital {nombre}", "clinica {nombre}",
        "centro de salud {nombre}", "{nombre} hospital",
    ],
    "sena_regional": [
        "sena {region}", "sena regional {region}",
        "servicio nacional de aprendizaje {region}",
        "regional {region} sena",
    ],
    "icbf_regional": [
        "icbf {region}", "icbf regional {region}",
        "instituto colombiano de bienestar familiar {region}",
        "bienestar familiar {region}",
    ],
    "secretaria": [
        "secretaria de {nombre}", "secretaria {nombre}",
        "secretaria de estado {nombre}",
    ],
    "fondo": [
        "fondo de {nombre}", "fondo {nombre}", "{nombre} fondo",
    ],
    "corporacion_autonoma": [
        "corporacion {nombre}", "corp {nombre}",
        "autoridad ambiental {nombre}",
        "corporacion autonoma {nombre}",
    ],
    "ministerio": [
        "ministerio de {nombre}", "ministerio {nombre}", "min {nombre}",
    ],
    "superintendencia": [
        "superintendencia de {nombre}", "superintendencia {nombre}",
        "sup {nombre}",
    ],
    "gobernacion": [
        "gobernacion de {nombre}", "gobernacion {nombre}",
        "gobernacion del {nombre}", "departamento de {nombre}",
        "{nombre} gobernacion",
    ],
}
```

**Reglas transversales (duras):**
- Siempre incluir nombre oficial en minuscula
- Siempre incluir variante sin tildes
- Siempre incluir variante con articulo "de" y sin el
- **NUNCA** incluir alias que sea un solo token generico (`alcaldia`, `municipio`, `gobernacion`, `ministerio`, `secretaria`, `corporacion`, `hospital`, `fondo`, `instituto`, `empresa`, `entidad`) — defensa en profundidad ademas de `_is_unqualified_generic()`
- Normalizar: `nariño` → `narino`, `engativa` → `engativa` (sin tilde en alias porque `normalize_text` del query_router las quita antes de scan)

**Criterio de eleccion: heuristico vs LLM:**

| Tipo inferido | Usar heuristico | Ejemplo |
|---------------|---------------|---------|
| alcaldia_municipal | Si | ALCALDIA MUNICIPAL DE TURBACO |
| ese_hospital | Si | ESE METROSALUD |
| sena_regional | Si | SENA REGIONAL CUNDINAMARCA |
| icbf_regional | Si | ICBF REGIONAL CORDOBA |
| secretaria | Si | SECRETARIA DE EDUCACION |
| fondo | Si | FONDO DE PASIVO SOCIAL FERROCARRILES |
| corporacion_autonoma | Si | CORNARE, CORPOCALDAS |
| ministerio | Si | MINISTERIO DE JUSTICIA |
| superintendencia | Si | SUPERINTENDENCIA NACIONAL DE SALUD |
| gobernacion | Si | GOBERNACION DEL VALLE |
| otro | NO — requiere LLM | INRAVISION, DAPRE, UPRA, IU Digital |

**Estimacion:** ~70-80% de las entidades del gap son tipicas y se cubren con heuristico. Solo las "otro" necesitan LLM.

#### A3 — Validate / Collision Scan (G3)

**Entregable:** `tests/test_gazetteer_manager.py` + logica de colision en manager.

Validacion dura:
- Minimo 3 aliases por entidad
- Ningun alias generico puro
- Ningun alias roba entidad existente
- Tests entity_resolver pasan
- Query corpus pasa

**Colision scan test:**
```python
def test_lote_X_no_colisiona():
    resolver = EntityResolver(ALIASES_DB_PATH)
    for entity in nuevas_entidades:
        for alias in entity["aliases"]:
            result = resolver.scan_entity(alias)
            if result and result.official_name != entity["official_name"]:
                assert False, f"Alias '{alias}' colisiona"
```

Falsos positivos legitimos (ej. "sena bogota" podria ser regional o nacional) se marcan como `xfail` con justificacion.

#### A4 — Lote 00: Higiene de Aliases (G4)

**Entregable:**
- `scripts/lote_00_alias_hygiene.json` (lista de aliases a remover, no entidades nuevas)
- `tests/test_gazetteer_lote_00_hygiene.py`

**Objetivo:** remover aliases genericos peligrosos que ya existen en `aliases_db.json` antes de expandir.

**Lista de aliases a remover (target):**
- `"alcaldia"` (suelto)
- `"municipio"` (suelto)
- `"gobernacion"` (suelto)
- `"ministerio"` (suelto)
- `"secretaria"` (suelto)
- `"corporacion"` (suelto)
- `"hospital"` (suelto)
- `"fondo"` (suelto)
- `"instituto"` (suelto)
- `"empresa"` (suelto)
- `"entidad"` (suelto)
- `"el ministerio"`, `"la secretaria"`, `"la corporacion"`, `"el fondo"` (articulo + generico)
- `"la alcaldia"`, `"el municipio"`, `"el distrito"`, `"la gobernacion"`

**Proceso:**
1. Cargar `aliases_db.json`
2. Para cada entidad, filtrar aliases que matchean la lista de prohibidos
3. Verificar que despues del filtrado cada entidad sigue con >= 3 aliases
4. Si una entidad queda con <3 aliases, loggear warning y no remover (para no dejarla sin aliases)
5. Guardar backup
6. Guardar `aliases_db.json` limpio
7. Correr tests de regresion

**Tiempo estimado:** 30-60 min.

#### A5 — Lote 01: Expansion Mixta (G5)

**Entregable:**
- `scripts/lote_01_top_gap_mixed.json`
- `tests/test_gazetteer_lote_01.py`
- `app/data/aliases_db.json` (actualizado)
- `docs/gazetteer/gap_YYYYMMDD.md`

**Composicion:** 50 entidades = 35 top volumen + 10 tipos criticos + 5 equidad territorial

**Criterio de seleccion:**
1. Top 35 por volumen del gap (P0)
2. 10 entidades de tipos criticos no cubiertos por el top 35:
   - 3 SENA regionales faltantes
   - 2 ICBF regionales faltantes
   - 3 ESEs con buen volumen
   - 2 secretarias departamentales
3. 5 entidades de departamentos con <5 entidades en gazetteer actual (P1)

**Cierre obligatorio:**
```bash
pytest tests/test_entity_resolver.py tests/test_gazetteer_lote_01.py -q
python scripts/run_query_corpus.py --mode all
make lint-core
```

---

### Sprint B — Lotes de Expansion Continua

#### B1 — Lote 02: Top Volumen (50 entidades)
- Siguientes 50 del gap por volumen
- Mix esperado: ~40 tipicas + ~10 "otro"

#### B2 — Lote 03: Tipos Estrategicos (45 entidades)
- Todos los SENA regionales faltantes (~20)
- Todos los ICBF regionales faltantes (~10)
- Todas las ESEs con >3000 procesos (~15)
- Secretarias departamentales faltantes (~5)

#### B3 — Cobertura Departamental (50 entidades)
- Identificar departamentos con <10 entidades en gazetteer
- Priorizar entidades de esos departamentos aunque tengan menor volumen
- Meta: minimo 10 entidades por departamento

#### B4+ — Completar Top 1000
- Meta final sprint B: ~750-800 entidades, ~16,000 aliases totales
- Coverage top-1000: ~75-80%

---

### Sprint C — SECOP I Audit

**Rama:** `experiment/secop1-dataset-audit`

**Objetivo:** Responder: "El dataset SECOP I se puede mapear al modelo comun de SECOPPAL?"

**Entregables:**
- `docs/secop1/SCHEMA_AUDIT.md`
- `docs/secop1/FIELD_MAPPING.md`
- `scripts/inspect_secop1_dataset.py`
- `tests/fixtures/secop1_sample.json`

**Mapping minimo a `NormalizedProcurementRecord`:**
```
source_dataset: "secop1" | "secop2_procesos" | "secop2_contratos"
record_id
referencia
entidad
departamento
ciudad
objeto
modalidad
estado
valor
fecha_publicacion
fecha_firma
proveedor
url
```

**Lo que NO se hace en esta rama:**
- No mezclar SECOP I con SECOP II en una sola respuesta
- No cambiar QueryRouter
- No cambiar ResponsePolicy
- No tocar SoQLBuilder salvo adapter separado

---

### Sprint D — SECOP I Integration

**Rama:** `feature/secop1-adapter` → `feature/secop1-search-integration`

1. Dataset selector
2. Results formatter
3. Response policy diferenciada
4. Query corpus secop1

---

### Sprint E — REGION-MACRO-001

**No vive en el gazetteer.** Catálogo geografico propio:
- `app/data/geo_regions.py` o `app/data/geo_regions.json`

**Regiones iniciales (conservadoras):**
```python
REGION_MACROS = {
    "caribe": ["Atlantico", "Bolivar", "Cesar", "Cordoba", "La Guajira", "Magdalena", "Sucre", "San Andres, Providencia y Santa Catalina"],
    "costa atlantica": ["Atlantico", "Bolivar", "Cesar", "Cordoba", "La Guajira", "Magdalena", "Sucre", "San Andres, Providencia y Santa Catalina"],
    "eje cafetero": ["Caldas", "Quindio", "Risaralda"],
    "pacifico": ["Choco", "Valle del Cauca", "Cauca", "Narino"],
    "amazonia": ["Amazonas", "Caqueta", "Guainia", "Guaviare", "Putumayo", "Vaupes"],
    "orinoquia": ["Arauca", "Casanare", "Meta", "Vichada"],
}
```

**Ambiguas (requieren aclaracion):**
- `interior`, `centro`, `norte`, `sur`, `occidente`, `oriente`

**Parser esperado:**
- `procesos de mantenimiento en la costa atlantica` → `departamento_entidad IN (lista caribe)`
- `contratos de suministro en eje cafetero` → `departamento IN (lista eje)`
- `procesos de PAE en pacifico` → `departamento_entidad IN (lista pacifico)`
- `contratos en el interior` → `needs_clarification`

---

## 6. Runbook: Como Agregar un Lote

```bash
# 1. Ver gap
python scripts/gazetteer_manager.py diff --top 1000 > docs/gazetteer/gap_$(date +%Y%m%d).md

# 2. Preparar lista del lote (manual o auto)
python scripts/gazetteer_manager.py diff --top 50 --start-at 501 --format json > scripts/lote_XX.json
# O editar manualmente scripts/lote_XX.json para seleccionar entidades especificas

# 3. Generar aliases y append
python scripts/gazetteer_manager.py append --from-list scripts/lote_XX.json --aliases heuristic

# 4. Validar
python scripts/gazetteer_manager.py validate
# Si falla, revisar colisiones y ajustar aliases manualmente en scripts/lote_XX.json, reintentar

# 5. Test de regresion completo
pytest tests/test_entity_resolver.py tests/test_query_router.py -q
python scripts/run_query_corpus.py --mode all
make lint-core

# 6. Stats finales
python scripts/gazetteer_manager.py stats

# 7. Commit
# Incluir: scripts/lote_XX.json (fuente del lote), app/data/aliases_db.json (resultado),
#          tests/test_gazetteer_lote_XX.py (tests de colision + cobertura),
#          docs/planes/gazetteer-expansion-plan.md (actualizar tabla de coverage)
```

---

## 7. Metricas de Exito

| Metrica | Baseline (A0) | Meta Sprint A | Meta Sprint B | Meta 6 sprints |
|---------|---------------|---------------|---------------|----------------|
| Entidades en gazetteer | 509 | ~560 | ~800 | ~1,000 |
| Top-1000 coverage | 50.2% | ~56% | ~75% | ~85% |
| Aliases por entidad (promedio) | 20.8 | >= 15 | >= 15 | >= 15 |
| Tests entity_resolver pass | 100% | 100% | 100% | 100% |
| Costo LLM por lote | N/A | <$0.50 | <$0.50 | <$0.50 |
| Tiempo de append por lote | N/A | <30 min | <30 min | <30 min |
| Departamentos con <5 entidades | ~5 | ~3 | 0 | 0 |
| Aliases genericos peligrosos | ~16 | 0 | 0 | 0 |

---

## 8. Riesgos y Mitigaciones

| Riesgo | Probabilidad | Impacto | Mitigacion |
|--------|-------------|---------|------------|
| Colision de aliases introduce ENTITY-MISRESOLUTION | Media | Alto | Colision scan test obligatorio por lote; heuristico genera aliases largos |
| Alias heuristica genera 0 resultados para tipos nuevos | Media | Medio | Fallback a LLM para tipo "otro"; revisar manualmente primer lote |
| Tiempo de scan_entity crece con mas aliases | Alta | Medio | `_scan_index` es O(aliases) por primera palabra; ~20K aliases aun manejable |
| SECOP renombra entidades → aliases_db desfase | Media | Bajo | El `diff` detecta renombres; substring-match tolera variaciones |
| Backup de aliases_db.json se pierde | Baja | Alto | Cada append crea backup con timestamp; git controla versiones |
| Lote 00 deja entidades con <3 aliases | Media | Medio | Validacion previa: si filtrado deja <3 aliases, no filtrar esa entidad |

---

## 9. Archivos a Mantener Versionados

- `app/data/aliases_db.json` — siempre el artefacto final
- `scripts/lote_*.json` — fuente de cada lote (lista de entidades con aliases)
- `tests/test_gazetteer_lote_*.py` — tests de colision + cobertura por lote
- `docs/gazetteer/gap_*.md` — snapshots del gap en puntos del tiempo
- `baseline/gazetteer_append/` — baseline del proyecto

---

## 10. Apendice: Estructura del lote JSON

```json
[
  {
    "official_name": "ALCALDIA MUNICIPAL DE TURBACO BOLIVAR",
    "departamento": "Bolivar",
    "nit": "1234567890",
    "total_procesos": 9549,
    "total_contratos": 0,
    "source_datasets": ["procesos"],
    "entity_type": "alcaldia_municipal",
    "aliases": [
      "alcaldia de turbaco", "alcaldia turbaco",
      "municipio de turbaco", "municipio turbaco",
      "turbaco alcaldia", "turbaco municipio",
      "alcaldia municipal de turbaco",
      "alcaldia municipal de turbaco bolivar"
    ]
  }
]
```

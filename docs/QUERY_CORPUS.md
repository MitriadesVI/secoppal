# QUERY_CORPUS V1 — SECOPPAL

## Propósito
Corpus vivo de **99 queries** reales para depuración sistemática, regresión y cobertura de casos edge en SECOPPAL. Sembrado con 31 queries marcadas `rating=0` en `feedback.jsonl` + 11 queries top-frecuentes + 7 casos manuales reportados directamente por el usuario en la última semana.

## Validation modes — clave para entender el runner

Cada query declara su **`validation_mode`**:

| Mode | Qué valida | Velocidad | Claves soportadas |
|---|---|---|---|
| `parser` | Solo lo que produce `QueryRouter.parse()`. Sin red, sin pipeline. | < 50 ms/query | `dataset`, `intent_type`, `estado_family`, `departamento_resolved`, `ciudad`, `entidad_resolved`, `valor_min`, `valor_max`, `fecha_desde`, `fecha_hasta`, `ordering_signal`, `objeto_contains`, `objeto_not_contains`, `soql_contains`, `soql_not_contains` |
| `workflow` | `SecopalWorkflow.run_query()` end-to-end con mocks de SECOP. Cubre follow-up, paginación, analytics, anti-WHERE 1=1, timeout UX. | ~200 ms/query | Todas las de `parser` + `followup_intent_type`, `needs_clarification`, `analytical_intent`, `analytical_response_contains`, `response_contains`, `response_not_contains`, `route_reason`, `total_count_min/max` |

**Por qué dos modos:** el bug del corpus original era pedirle a `parse()` claves que solo existen tras el pipeline (`followup_intent_type`, `needs_clarification`, etc.). Eso generaba FAILs falsos. La separación elimina ese ruido.

## Cómo agregar casos

Editar `tests/fixtures/query_corpus_v1.yaml`:

```yaml
- id: CORPUS-X999
  category: bidder_intent          # ver lista abajo
  query: "quiero vender carpas, alguna oportunidad?"
  previous_turns: []                # para follow-up: lista de strings
  validation_mode: parser           # parser | workflow
  expected:
    dataset: procesos
    intent_type: opportunity_search
    objeto_contains: ["carpa"]
    objeto_not_contains: ["vender"]
  severity: critical                # low | medium | high | critical
  source: feedback_rating_0         # de dónde viene
  notes: optional, una línea
  simulate_timeout: true            # solo workflow: mockea SECOP con Exception
```

## Categorías (11)

- `bidder_intent` — proponente busca oportunidades (verbos `vender`, `ofrezco`, `presentarme`).
- `opportunity_search` — sinónimos explícitos: `convocatorias`, `oportunidades`, `licitaciones abiertas`.
- `followup` — turnos sucesivos: `change_order`, `change_scope`, `pagination_more`, `refine_filter`.
- `value_range` — rangos monetarios (incluye compactos `1000-3000 millones`).
- `estado_policy` — semántica de estados SECOP (`firmados`, `en ejecución`, `cerrado`).
- `entity_geo` — resolución de entidades, ciudades, departamentos.
- `object_complex` — frases multi-token, OR, anáforas.
- `analytical` — agregaciones `aggregate_sum`.
- `anti_global` — guard anti-WHERE 1=1.
- `timeout_ux` — comportamiento ante SECOP lento.
- `smoke` — queries frecuentes que deben pasar siempre.

## Cómo correr

```bash
source .venv/bin/activate

# Todas las queries en su modo declarado (default).
python scripts/run_query_corpus.py --mode all

# Solo parser (rápido, sin red).
python scripts/run_query_corpus.py --mode parser

# Solo workflow (con mocks de SECOP).
python scripts/run_query_corpus.py --mode workflow

# Solo una categoría.
python scripts/run_query_corpus.py --mode all --category followup

# Solo mostrar FAILs (para triage).
python scripts/run_query_corpus.py --mode all --show-fails

# Limitar para iteración rápida.
python scripts/run_query_corpus.py --mode all --limit 20

# JSON output para CI.
python scripts/run_query_corpus.py --mode all --json > corpus_report.json
```

## Modo live (HTTP real, sin mocks)

```bash
python scripts/run_query_corpus.py --live --limit 10
```

Marca `TIMEOUT` por query individual sin abortar la corrida completa.

## Exit code

- `0` → 0 FAILs (corpus verde).
- `1` → al menos un FAIL.

Útil para CI: `python scripts/run_query_corpus.py --mode all && echo OK`.

## Criterios de severidad

- `critical`: rompe flujo principal, pérdida de contexto en follow-up, ejecuta WHERE 1=1, devuelve 0 resultados cuando los hay.
- `high`: bug que un usuario nota (entidad mal resuelta, rango monetario incompleto, intent_type ausente cuando debería estar).
- `medium`: caso edge, variación menor, estado raro.
- `low`: variación de redacción sin impacto en resultados.

## Smoke vs Regression vs Exploratory

- **Smoke** (`category: smoke`): queries más frecuentes del feedback. Deben pasar siempre.
- **Regression**: queries que cubren bugs ya corregidos (CORPUS-R001..R031 vienen de `feedback_rating_0`).
- **Exploratory**: queries `source: manual` para descubrir gaps. Pueden fallar mientras se trabaja.

## Cobertura actual

| Categoría | Queries | Modo dominante |
|---|---:|---|
| followup | 15 | workflow |
| opportunity_search | 12 | parser |
| bidder_intent | 12 | parser |
| smoke | 10 | parser |
| object_complex | 9 | parser |
| value_range | 9 | parser |
| entity_geo | 9 | parser |
| analytical | 7 | workflow |
| estado_policy | 6 | parser |
| anti_global | 5 | workflow |
| timeout_ux | 5 | workflow |
| **Total** | **99** | 70 parser / 29 workflow |

**Fuentes:**
- `feedback_rating_0`: 31 — bugs reales reportados por usuarios.
- `feedback_top_frequent`: 11 — queries más ejecutadas.
- `manual_user_session`: 7 — casos reportados en sesiones recientes.
- `manual`: 32 — diseñadas para cubrir ramas.
- Resto: de tests existentes (`test_value_ranges`, `test_opportunity_timeout_ux`, etc.).

## CORPUS-001-B — Cierre 2026-05-17

**Estado:** Infraestructura lista para diagnostico. No es aun suite CI bloqueante completa.

| Metrica | Valor |
|---|---|
| Queries totales | 99 |
| De feedback real rating=0 | 31 |
| Top-frecuentes | 11 |
| Casos manuales recientes | 7 |
| Categorias | 11 |
| Modo parser | 70 |
| Modo workflow | 29 |
| Pass rate inicial | 77/99 |
| FAIL restantes | 22 (backlog accionable, no ruido del runner) |

Los 22 FAIL se documentan en docs/CORPUS_BACKLOG.md y se priorizan por severidad.

**Regla:** El corpus no bloquea CI. Solo se usa como herramienta de diagnostico y triage. Antes de integrarlo como gate de CI se requiere implementar known_fail/expected_fail (CORPUS-CI-001) o un baseline de regresion.

## CORPUS-002 — Critical failures cerrados 2026-05-17

**Commit:** 9d1660b

**Resultado:**
- Critical: 5/5 PASS
- Corpus total: 82/88 PASS
- Sin regresiones en pytest
- feedback.jsonl estable

**Fixes aplicados:**
- `convocadas` → `oferta_abierta`
- `oferta_abierta + procesos` promueve a `opportunity_search`
- follow-up con topic nuevo ya no cae automáticamente como `refine_filter`

**Pendientes high (6):**
- CORPUS-R003 — limpiar
- CORPUS-OC004/OC005 — bidder intent indirecto
- CORPUS-V005 — "X o más"
- CORPUS-V007 — "mil millones"
- CORPUS-AN003 — "cuánto suman"

## Mantenimiento

- Después de cambios en parser/workflow: correr `--mode all --show-fails`. FAILs nuevos = posible regresión; FAILs viejos cerrados = fix exitoso.
- **Nunca** modificar parser para hacer pasar el corpus si la query produce el resultado correcto a ojos del usuario. El corpus se ajusta al producto, no al revés.
- **Sí** abrir issue/bug por cada FAIL clasificado como `critical` o `high`.
- Cada vez que aparezca un nuevo `rating=0` en `feedback.jsonl`, añadirlo al corpus con `source: feedback_rating_0`.

## Workflow recomendado

1. Antes de tocar parser o pipeline: `python scripts/run_query_corpus.py --mode all > before.txt`.
2. Hacer el cambio.
3. `python scripts/run_query_corpus.py --mode all > after.txt`.
4. `diff before.txt after.txt` — debe haber **solo** transiciones FAIL→PASS, nunca PASS→FAIL.
5. Si una regresión es intencional (cambio de contrato), actualizar el `expected` y dejarlo justificado en `notes`.

# Handoff para ZEUS fresco — FOUNDATION_V3_PROMOTION_R1 (propiedad de ruta ACTOR + polaridad de VALUE)

Contexto fresco. Tu trabajo es intentar demostrar que R1 **no** debe integrarse. No modifiques código.

## Objeto

- Rama `fix/foundation-v3-promotion-r1`, desde `e26730861397c16c47421675e6b66ddb61bccc74`.
- **Código congelado: `3f7475f3`** (tres commits: `627636b7`, `ab66282f`, `3f7475f3`). El commit de esta
  documentación no cambia código. Un cambio de código posterior invalida la certificación.
- Auditoría que originó R1: `audit/foundation-v3-promotion-zeus` @ `9e0898bb` (NO_GO: P0 propiedad ACTOR, P1 Z09).
- Base de la comparación global definitiva pedida: `main` `68090765` (la haces tú, seed 19).

## Qué cambió (todo el diff de `app/`)

1. `app/core/workflow/actions/actor_resolution.py`: `resolve_actor_name` pasa a ser un envoltorio de una línea sobre
   `resolve_actor_params(raw_query, parsed_params, resolver, negation_requests) -> dict | None`. Transformación
   mecánica (`return state` → `None`, `state.update(parsed_params=X)` → `X`). El cuerpo no cambia.
2. `app/core/foundation_route_owner.py`: `actor_claims()` llama a **esa misma función** y aplica el predicado del
   orquestador para el dueño ACTOR (`actor_scope`/`actor_pending`, más `contratista` y `actor_intent_conflict`).
   `specialized_owner` la consulta antes de permitir a Foundation. No hay vocabulario de actor en Foundation.

No se tocó: prompt v4.2, H7, `universe_kind`, `eligible_first_search`, el fallback, held-out, mensajes.

## Contrato a verificar

1. Si legacy (Foundation OFF) termina con `_continuation_owner == ACTOR`, con Foundation ON: 0 llamadas al productor,
   0 al fallback, mismas llamadas a fuente y misma respuesta. Oráculo diferencial, no lista escrita a mano.
2. Con caída del proveedor, un turno ACTOR nunca llega a `foundation_basic_fallback`.
3. VALUE con polaridad NEGATIVE nunca se proyecta como cota positiva (proyección sola y ruta real).
4. Seed / selección estructurada nunca es primer turno de Foundation (Z04).
5. Flag OFF: sin diferencias materiales respecto de `main` y de `e2673086`.
6. Foundation sigue reclamando lo que legacy no da a ACTOR (búsquedas temáticas, negación con política).

## Evidencia (verifícala, no confíes)

Tests nuevos: `tests/test_foundation_actor_ownership.py` (62), `tests/test_foundation_value_polarity_and_seed.py` (12).
Mutantes: `scripts/audits/foundation_promotion_mutants.py` (M01–M25; el runner acepta `MUTANT_ONLY=M14,M25`).

| Gate | Resultado |
|---|---|
| Reproducción sobre `e2673086` sin tocar | 41 de 60 fallos (8 casos ZEUS + O'Brien, matriz, índice presente, negación) |
| Alias exacto resuelto con índice presente | también lo reclamaba Foundation (no reportado por ZEUS) |
| Focales Actor/Foundation/Suppliers/CA/Radar/Entity Profile | 0 fallos exclusivos; mismo conjunto rojo que la base |
| Mutantes | 25/25 muertos: 20 originales + Z09 (M21) + Z04 (M22) + M23/M24/M25 de propiedad de actor |
| Global pareado seed 19, base `e2673086` vs `3f7475f3` | CANDIDATE_ONLY_FAILURES=0, NEW_ERRORS=0, BASE_PASSED_NOW_MISSING=0, NEW_TESTS_FAILED=0 (74 nuevos) |
| Subconjunto seed 20260930 (actor, foundation, supplier_request_authority, buyer_actor) | mismos cuatro criterios en 0 |
| Barrido de 222 consultas elegibles | Foundation reclama 219; `claimed_while_legacy_specialized=0` (antes 1) |
| Paridad OFF, 714 consultas × {Frame OFF, Frame ON+Actor}, vs `main` y vs `e2673086` | 0 diferencias materiales |
| ruff, lint-core, diff-check | limpios |
| semgrep delta `--base e2673086` | TOTAL 0, NEW_DELTA 0 |

Detalle en `evidence/GLOBAL_DELTA_R1.txt` y `evidence/PARITY_AND_SWEEP_R1.txt`.

## Límites declarados (no los ocultes al auditar)

- Los 120 rojos globales son la base ya roja (fixture sqlite sin `date_extract_y`); el conjunto es **idéntico** en base y
  candidato, no cero.
- La paridad usó un normalizador propio adicional (timestamps ISO, hora impresa `consultado el …`, orden de llamadas
  paralelas de Entity Profile); no es comparación byte a byte. Las 55 diferencias crudas eran sólo ese ruido.
- El gate usa el parse determinista. Si en producción `llm_parse` cambiara el `parsed_params` antes de
  `resolve_actor_name`, el gate no lo ve (también era así en el candidato anterior).
- El dry-run repite `resolve_exact` (y `resolve` fuzzy en la rama entity-like) por cada primer turno elegible con Foundation ON.
  No se midió latencia; queda para dogfood.
- Una negación con política (`que no sean de Cali`) no es ACTOR en legacy: Foundation la conserva, como antes.
- Sólo un caso de actor con `INDEX_UNAVAILABLE`/índice sintético; el índice real de 662 MB no se usó.
- Antes de recibir la orden de una sola comparación global corrí además `main` vs R1 con seeds 19 y 20260930
  (0 fallos exclusivos en ambas). Son evidencia extra, no la comparación definitiva.

## Ángulos sugeridos

- Un primer turno que legacy dé a ACTOR por una vía que `resolve_actor_params` no cubra (p. ej. `actor_conversation_probe`,
  documento explícito con método ya fijado, ambigüedad con `actor_pending`).
- Sobre-declinación: un turno que Foundation debería conservar y ahora se lleva Actor (nombres con forma de persona,
  topónimos, temas con `de`/`y`), mirando el fuzzy en la rama `person_shape_only`.
- El mutante `if False` sobre la guarda `actor_scope/actor_pending/contratista` sobrevivía hasta añadir el test unitario:
  ¿hay otra guarda redundante sin test?
- Que `resolve_actor_params` no mute `parsed.params` compartido (copia superficial `dict(params)`).

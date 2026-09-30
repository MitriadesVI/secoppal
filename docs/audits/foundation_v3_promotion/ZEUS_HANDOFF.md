# Handoff para ZEUS fresco — FOUNDATION_V3_SELECTIVE_PROMOTION_V1

Prompt cerrado. Contexto fresco, sin la narrativa de implementación. Tu trabajo es
intentar demostrar que este candidato **no** debe promoverse. No modifiques código.

## Objeto de la auditoría

- Repo: `/Users/rodrigoortiz/Documents/secoppal` (worktree del candidato:
  `/Users/rodrigoortiz/Documents/secoppal-foundation-v3-promotion-v1`).
- Rama: `feature/foundation-v3-promotion-v1`. Base: `main` = `680907653b190a277993ce813b070b1984aee2b5`.
- Candidato congelado: el HEAD de la rama indicado en `FINAL_REPORT.md` (§ Candidato). Cualquier
  commit posterior invalida la certificación.
- Laboratorio de origen (sólo referencia, no se mergea): `audit/foundation-v3-heldout-da6-zeus` = `f703504b`.

## Contrato que debe cumplir

1. Foundation apagada por defecto (`SECOPPAL_FOUNDATION_MODE=legacy`); con flag OFF, el
   comportamiento observable es el de `main`.
2. Un solo perfil `foundation_v3` = productor B, prompt v4.2 con SHA-256
   `a5867d40cb0027c1ebd32714b61efc9d97bfc82730afaee1e9d58007214340f4`, `universe_kind`
   obligatorio sin default y H7 siempre aplicado. No existe un perfil con H7 apagado.
3. `HISTORICAL_TENDER_COMPLEMENT` ausente e inalcanzable; B_GUARDAS no son autoridad runtime.
4. Foundation no preempta a ningún dueño vigente (Natural Alpha, SUPPLIERS, Contract Activity,
   Entity/Actor Profile, Radar, direct lookup, paginación, sugerencias, refinamiento, reset,
   follow-ups); sólo reclama SEARCH de primer turno; no amplía `eligible_first_search`.
5. Ninguna condición material desaparece en silencio en el host (varias citas de tema,
   sustantivo de universo como tema, composición de estados, dirección del operador monetario).
6. El usuario nunca lee un código interno, un nombre de excepción ni un traceback.
7. El fallback por caída del proveedor sólo corre por `PROVIDER_ERROR`, sin segunda llamada LLM,
   nunca global, rotulado, y no corre por `INVALID_PROPOSAL` ni por otro terminal.
8. Sin red ni gasto en tests.

## Evidencia a verificar (no confiar en ella)

`docs/audits/foundation_v3_promotion/`: `PREFLIGHT.json`, `BASELINE_NODEIDS.json`,
`SELECTIVE_INVENTORY.md`, `ROUTE_OWNERSHIP_MATRIX.md`, `BASIC_FALLBACK_CONTRACT.md`,
`COMPONENT_GATES.md`, `GLOBAL_DELTA.json`, `MUTATION_REPORT.json`, `FINAL_REPORT.md`;
ADR: `docs/decisions/2026-09-29_ADR-102_foundation-v3-selective-promotion.md`.

Herramientas reproducibles: `scripts/audits/pytest_netblock.py`,
`scripts/audits/foundation_promotion_suite.sh`, `scripts/audits/foundation_flag_off_parity.py`,
`scripts/audits/foundation_promotion_mutants.py` + `..._run_mutants.py`.

## Ángulos sugeridos (no exhaustivos)

- ¿Existe una frase de primer turno que pase `eligible_first_search`, que ningún detector de
  `app/core/foundation_route_owner.py` reclame, y que hoy atienda un runtime especializado
  (analytics, perfil, actor, contract activity) en `main`?
- ¿Algún camino con flag OFF ejecuta código Foundation con efecto observable (recibos, claves
  nuevas en el resultado, SoQL distinto, guard)? Revisa `soql_builder.py`, `secop_client.py`,
  `helpers.py`, `observer.py`, `feedback.py`, `orchestrator.py`.
- ¿Puede una propuesta del productor hacer que el host ejecute sólo parte de lo citado (spans
  solapados, contiguos con signos, citas repetidas con distinto significado, BUYER/GEOGRAPHY)?
- ¿`foundation_state_composition` o `foundation_value_direction` dejan pasar una inversión o un
  colapso con otra forma léxica, o bloquean frases V1/S3 que el held-out acreditó?
- ¿El fallback puede ejecutar algo que el parser determinista interpreta mal (claves
  toleradas, `contract_universe`, fechas) o mostrar resultados sin rótulo?
- ¿La guarda de continuación tras un turno Foundation bloquea búsquedas nuevas legítimas o deja
  pasar fragmentos que pierden el tema?
- ¿El prompt en `app/core/foundation_prompts.py` es byte-idéntico al medido en `f703504b`?
- ¿`GLOBAL_DELTA.json` compara corridas en condiciones realmente iguales?

## Entrega

Veredicto `GO_MERGE_CANDIDATE` / `NO_GO` con hallazgos P0/P1/P2 (archivo:línea, reproducción,
impacto). Distingue fondo de forma. No propongas activar, mergear ni desplegar.

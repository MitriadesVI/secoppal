# Misión M0 — instrumentación y desbloqueo mínimo (FOUNDATION_ENTRY_ADMISSION)

Base: `8e0b9ca3` (rama `fix/foundation-v3-live-publication-r1`, pendiente de ZEUS). Rama de
integración: `mission/foundation-entry-admission-m0`. Sin merge, push, deploy ni `dev.sh`.

## Norte (no negociable)

La puerta de entrada es **andamio transitorio**: el destino es un solo intérprete (Foundation),
autoridades deterministas por dimensión y capacidades que ejecutan operaciones. M0 NO construye un
subsistema semántico de admisión. Prohibido en M0:

- ampliar `eligible_first_search` (regex, saludos, historial) — se borra con la puerta en M1;
- leer texto crudo para decidir rutas en código nuevo;
- añadir fallback de Foundation a legacy;
- cambiar cualquier respuesta observable salvo la corrección declarada de L2.

## Carriles (ownership disjunto)

| Carril | Objetivo | Archivos propios |
|---|---|---|
| L1 | Corpus + harness de diagnóstico (hecho) | `docs/audits/foundation_entry_admission_m0/` |
| L2 | Frontera léxica de actor | `app/core/actor_conversation.py`, tests nuevos de actor, `tests/test_foundation_route_ownership.py` (sólo el caso Barranquilla) |
| L3 | Ledger de turno v1 + `admission_trace` | `app/core/turn_ledger.py` (nuevo), enganche mínimo en `foundation_runtime.py` / `orchestrator.py` / `feedback.py`, tests nuevos |
| L4 | Shadow diferencial legacy (módulo puro, SIN enganche) | `app/core/legacy_shadow.py` (nuevo), tests nuevos |

La integración L4 → ledger (etapa `SHADOW`) la hace el orquestador después de L3 y L4.

## Ledger de turno v1 (contrato de L3)

Una fila por etapa, append-only, por turno:

```
Stage(name, authority, decision, reason_code, input_digest, output_digest, detail)
name ∈ {PROTOCOL, ALPHA, ADMISSION, INTERPRETER, HOST, CAPABILITY, EXECUTION, PRESENTATION, SHADOW}
```

- `detail`: dict pequeño, JSON-serializable, sin texto crudo del usuario ni payload del proveedor.
- Digests: sha256 corto de entradas/salidas canónicas cuando existan; `None` si no aplica.
- V1 sólo llena lo que ya existe: `ADMISSION` (siempre que el flag Foundation esté ON y se llegue
  a `run_foundation_search`), e `INTERPRETER`/`HOST`/`EXECUTION` mapeados desde
  `foundation_telemetry` cuando Foundation produce.
- Códigos de `ADMISSION` (enum cerrado): `FLAG_OFF` (no se registra fila, ver paridad),
  `CONTINUATION_BLOCKED`, `INELIGIBLE_SEED_OR_SELECTION`, `INELIGIBLE_HISTORY`,
  `INELIGIBLE_LEXICAL`, `DECLINED_OWNER:<ACTOR|ENTITY_PROFILE|CONTRACT_ACTIVITY|ANALYTICS|CAPABILITY_BINDING|NATURAL_ALPHA_JOB>`,
  `ADMITTED`.
- Persistencia: `log_trace` corre ANTES de que `_dispatch` añada claves al resultado en la ruta
  legacy; por eso el registro vive en un recorder por turno (contextvar abierto en `run_query`) y
  `log_trace` lo lee con una función `turn_ledger_trace(...)`, igual que `trace_metadata`.
- Paridad: con `SECOPPAL_FOUNDATION_MODE=legacy` no aparece ninguna clave nueva ni en el resultado
  ni en la traza. Con flag ON, ninguna respuesta, ruta, llamada a fuente ni orden cambia.

## Shadow diferencial (contrato de L4)

Función pura `compare(raw, today, legacy_params, acceptance) -> ShadowDiff`: proyecta los params
de `QueryRouter.parse` y las resoluciones aceptadas de Foundation al vocabulario
`SemanticDimension` y devuelve, por dimensión, `AGREE | ONLY_LEGACY | ONLY_FOUNDATION | DIFFER`
con valores normalizados. Sin red, sin mutar entradas, sin lectura del texto para decidir
nada (el `raw` sólo sirve para digest). Incluye `ORDER` (← `ordering_signal`) y `ACTOR`
(← `actor_scope`/`contratista`) aunque Foundation aún no los tenga.

## Gates

Por carril: ruff sobre archivos tocados, tests focales, tests expandidos del área; mutantes que
maten cada guarda nueva. Al cierre (orquestador): suite global pareada por nodeid contra la base,
semgrep delta con `--base`, candidate freeze y ZEUS fresco (L2 cambia propiedad de turnos).

Entorno de worktree: usar `/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python` con
`PYTHONPATH=.`; el worktree no tiene `.env` ni `data/` (correcto para tests; para el harness usar
`SECOPPAL_ACTOR_INDEX`).

# M0 — candidato congelado para revisión independiente

- Rama: `mission/foundation-entry-admission-m0`
- **CODE (congelado): `e69f0c1f`** · BASE: `8e0b9ca3` (publicación en vivo R1, pendiente de ZEUS)
- Contrato: `BRIEF.md`. Roadmap: `docs/planes/2026-09-30_FOUNDATION_SINGLE_INTERPRETER_ROADMAP.md`.
- Sin merge, push ni deploy.

## Qué cambia

| Carril | Commit | Efecto |
|---|---|---|
| L1 | `592cfc48` | Harness y diagnóstico (corpus personal fuera de git) |
| L2 | `b38e3fbf` | `_NON_ACTOR_SEARCH_LEADS`: singular/plural/artículo + selección abreviada, mínima cuantía, concurso de méritos. «busca licitaciones de alcaldía de barranquilla en 2024» deja de ser actor en legacy y en la puerta Foundation |
| L3 | `c85f88ea` | Ledger de turno v1 (sólo traza): `ADMISSION` con código cerrado + `INTERPRETER`/`HOST`/`EXECUTION` |
| L4 | `8c7904d8` | `legacy_shadow.compare`, módulo puro |
| Integración | `e69f0c1f` | Etapa `SHADOW` (veredicto por dimensión, sin valores; fallo → `SHADOW_ERROR`) |

Único cambio de comportamiento observable: L2. Todo lo demás es observacional (traza).

## Gates

Entorno: `SECOPPAL_FOUNDATION_MODE=legacy`, `PYTHONHASHSEED=0`, `--hypothesis-seed=19`, serie,
misma venv, red no loopback bloqueada (`netblock_ledger.py`), worktrees limpios.

- **Global pareado BASE `8e0b9ca3` vs CODE `e69f0c1f`** (`global_delta.json`):
  CANDIDATE_ONLY_FAILURES=0, NEW_ERRORS=0, NEW_TESTS_FAILED=0 (131 nuevos),
  BASE_PASSED_NOW_MISSING=1 (`…[legacy-actor-parse-keeps-the-turn]`, invertido a propósito en
  `test_a_universe_noun_search_of_a_buyer_is_no_longer_an_actor_turn`). Rojos idénticos en ambos
  (121 failed + 2 errors; fixture sin `date_extract_y`). 77 intentos de red bloqueados en cada lado.
- Focales de integración: 195 passed. Ruff limpio en archivos tocados.
- `olympus-semgrep-delta --base 8e0b9ca3` sobre los 7 módulos: TOTAL=0, NEW_DELTA=0.
- Mutantes: L2 5/5, L3 14/14, L4 14/14 (dos sobrevivían y se cerraron con tests).
- Harness sobre el corpus (620): exactamente 3 decisiones cambian (x0, x1, x7: ACTOR → ADMIT); 0
  cambios en las 585 reales.

## Límites declarados

- Turnos que no llegan a `run_foundation_search` no tienen fila de ledger (handlers previos,
  multi-job, follow ligado, Frame).
- El shadow sólo compara acceptances READY de una petición; ORDER y ACTOR siempre `ONLY_LEGACY`.
- L2 amplía el léxico: un nombre real que empiece por un sustantivo de universo («Licitaciones y
  Contratos SAS») sólo es alcanzable como actor con marcador de rol o documento.
- Preexistente, no tocado: legacy convierte «licitaciones» en modalidad licitación pública (choca
  con DA-2bis).

## Prompt para ZEUS fresco (Opus, sin la narrativa de implementación)

> Audita adversarialmente el candidato `e69f0c1f` (rama `mission/foundation-entry-admission-m0`)
> contra la base `8e0b9ca3` en el repo SECOPPAL. Contrato: `docs/audits/foundation_entry_admission_m0/BRIEF.md`.
> Intenta demostrar que NO debe mergearse. En particular: (1) que el cambio de
> `_NON_ACTOR_SEARCH_LEADS` roba turnos legítimos de ACTOR (razones sociales, personas, NIT, «cambia a …»)
> o produce búsquedas equivocadas en legacy; enumera, no muestrees; (2) que el ledger o el shadow
> cambian alguna respuesta, ruta, orden de llamadas a fuente, `_continuation_owner` o claves del
> resultado, con flag ON u OFF; (3) que el ledger filtra texto del usuario o valores; (4) que algún
> código ADMISSION se registra mal o en el orden equivocado; (5) que el shadow puede lanzar o mutar
> entradas. Usa worktrees limpios, sin `dev.sh`, sin red. Veredicto GO / NO_GO con P0–P2 y evidencia
> reproducible.

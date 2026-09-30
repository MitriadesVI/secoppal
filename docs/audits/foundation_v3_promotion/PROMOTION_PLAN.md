# Plan — FOUNDATION_V3_SELECTIVE_PROMOTION_V1

**Riesgo:** HIGH_RISK. **Escritor único.** **Base:** `main` = `68090765` (no avanzó).
**Rama:** `feature/foundation-v3-promotion-v1`. **Sin** merge, push, deploy, restart,
activación, red ni gasto.

## Unidad promovida

Foundation Search = productor B con prompt v4.2 congelado (SHA `a5867d40…`) +
`universe_kind` obligatorio + H7 núcleo obligatorio + admisión/proyección/guard de
ejecución + recibos de llamadas a fuente, detrás de un único flag
`SECOPPAL_FOUNDATION_MODE` (`legacy` por defecto). Rollback = `legacy`.

## Qué no entra

`HISTORICAL_TENDER_COMPLEMENT`; B_GUARDAS; prompts v3/v4/v4.1 y brazo C como
perfiles; perfil `foundation_v3 + H7 off`; el hook previo a `coordinate_turn`; el
paquete DA-6 en CI (se referencia por digest, ver inventario §3); scripts,
capturas, ledgers y Gold de desarrollo.

## Hallazgos del inventario que cambian el trasplante

1. **Preempción** (ya advertida): `f703504b` ejecuta Foundation en
   `_admit_and_dispatch()` antes de `coordinate_turn()`. Se descarta; la entrada va en
   `SecopalWorkflow._dispatch()` después de los handlers estructurados, de la
   observación Natural Alpha (multi-job, follow ligado, job no-SEARCH) y de un chequeo
   seco con los detectores de cada dueño sobre el mismo `QueryRouter.parse` que ya usa
   la admisión (`external_owner`, `detect_entity_profile_turn`, analytics `detect`,
   `admit_contract_activity`).
2. **Paridad flag OFF rota en `soql_builder`** (nuevo): en `f703504b` la política O1B
   satisface la guarda anti-consulta-global para *todos* los llamadores; en `main`
   una búsqueda de oportunidades sin otro filtro lanza `UnsafeGlobalQueryError`. Se
   acota a un opt-in explícito que sólo la proyección Foundation pasa.
3. **Productor por defecto C** en `f703504b` (`SECOPPAL_FOUNDATION_PRODUCER=C`),
   mientras que el held-out midió B v4.2. Se elimina la opción: B es el único productor.
4. **Sin traza de feedback** en el camino Foundation de `f703504b`
   (`run_foundation_search` no llama `log_trace`). Se añade para observabilidad (§16).
5. `count` de `SecopClient` reintenta con `sleep` un `FOUNDATION_COUNT_PAYLOAD_INVALID`
   (error de contrato, no de red). Se registra como deuda; no se cambia la política
   de reintentos del cliente compartido en esta misión.

## Fases y commits

| # | Commit | Gate |
|---|---|---|
| 0–1 | `docs(foundation): freeze selective promotion plan and main baseline` | PREFLIGHT, inventario, baseline (el nodeid list se añade al terminar la corrida, que empezó sobre el SHA fijado antes de escribir código) |
| 2 | `feat(foundation): transplant v4.2 core with mandatory H7 behind one flag` | focales Foundation, hash del prompt, test de importación del complemento, paridad pin |
| 3 | `feat(foundation): integrate search after existing route owners` | ROUTE_OWNERSHIP_MATRIX + 9 tests de preservación con flags ON y OFF |
| 4a–d | un `fix(foundation)` por ruta silenciosa (G1/T4, G2, OR de estados, operador monetario) | focal + propiedad + mutante + cero llamadas |
| 5 | `feat(foundation): render human clarification messages` | registro exhaustivo + test de patrones crudos |
| 6 | fallback rotulado **sólo si** su contrato pasa; si no, `DEFERRED_UNSAFE` | contrato escrito antes |
| 7 | `test(foundation): add route-preservation and end-to-end product gates` | primera infancia + controles |
| 8 | `docs(foundation): freeze promotion candidate and independent handoff` | suite global vs baseline, mutaciones, ADR-102, ZEUS_HANDOFF |

## Protocolo de verificación

- Suite global: misma orden, mismo intérprete, red bloqueada por plugin,
  `PYTHONHASHSEED=0`, `TZ=America/Bogota`, worktree limpio desacoplado por corrida.
  Gate: `CANDIDATE_ONLY_FAILURES = 0`; fallos comunes idénticos; sin errores nuevos.
- Corpora de auditoría (`make audit-*`) en base y candidato con la misma red bloqueada.
- Estáticos: ruff (scope), `make lint-core`, `olympus-semgrep-delta --base 68090765`,
  `git diff --check`, greps de complemento, de defaults de `universe_kind` y de códigos
  crudos públicos.
- Mutaciones: la lista mínima de la misión §15.4, cada una muerta por el test que
  afirma la propiedad.

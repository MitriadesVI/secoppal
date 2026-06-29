# Runbook — Latencia acotada por deadline (Etapa 6) + recuperación degradada

Guía operativa de la arquitectura de latencia del Sprint A. Para entender, mantener o
extender el deadline de turno, el count no bloqueante, el fallback H4 y el acote de
direct_lookup. Decisiones en ADR-039/040/041 (`docs/decisions_log.md`).

## Modelo mental en una frase

Un turno tiene **un presupuesto monotónico de 15s** (`Deadline`, ContextVar). Ninguna
llamada externa puede usar más que `restante − reserva`. Las filas mandan; el conteo y el
orden son ejes secundarios que se degradan honestamente, nunca fabrican EMPTY ni un ranking
falso.

## Reparto del presupuesto (8/3/4)

```
deadline total = 15s
  query principal   ≤ 8s   (PRIMARY_FETCH_SECONDS)
  fallback sin ORDER BY ≤ 3s (FALLBACK_FETCH_SECONDS)
  reserva de cierre  ≥ 4s   (CLOSING_RESERVE_SECONDS: count ≤3s + formato/narración)
```
`Deadline.fetch_budget_s(source_timeout, cap_s)` = `min(source, restante − reserva[, cap])`.
Un turno con fallback APILA primary+fallback+count → puede rozar 15.5s (excepción explicada).

## Archivos clave

| pieza | archivo | qué hace |
|-------|---------|----------|
| Deadline + ContextVar | `app/core/deadline.py` | `Deadline`, `begin/end_deadline`, `current_deadline`, `fetch_budget_s`, reloj inyectable (`set_clock`/`reset_clock`) |
| arranque del turno | `app/core/orchestrator.py` `run_query` | `begin_deadline(run_started_at)` ANTES de `_dispatch`; `attach_deadline_telemetry` al cerrar |
| fetch + count acotados | `app/core/workflow/helpers.py` `_query_first_then_count` | query primero (cap por fase), count solo con presupuesto; `_client_for` deriva el cliente acotado |
| cliente derivado thread-safe | `app/core/secop_client.py` `derived_for_timeout` | NO muta el singleton; devuelve `self` si los métodos están monkeypatcheados (preserva mocks) |
| fallback H4 | `app/core/workflow/actions/execution.py` `execute_query` (bloque `except`) | un fallback sin ORDER BY ante timeout recuperable + orden presentacional |
| gate semántico | `app/core/order_semantics.py` | `order_is_semantic/presentational` (sobre `ordering_signal`), `sort_page_presentational`, constantes de `result_order_guarantee` |
| direct_lookup acotado | `app/core/workflow/handlers/direct_lookup.py` | `_probe_budget_s`, corte de variantes seriales (`budget_cut`) |

## Gotchas (lo que muerde)

1. **Hay DOS generadores de respuesta.** `formatter.py` (canales) Y
   `response_policy.build_advisor_response` (la respuesta visible real en el flujo normal).
   Cualquier cambio de cómo se afirma el orden/total debe tocar **ambos**. El bug de H4
   ("muestra sin orden" que igual decía "más recientes") fue exactamente esto.
2. **El gate semántico se apoya en `ordering_signal`.** Presente ("más caros"→`valor_desc`,
   "últimos"→`fecha_desc`) ⟹ SEMÁNTICO ⟹ NO degradar. Ausente ⟹ presentacional ⟹ fallback OK.
   El gate lee el turno ACTUAL; una fuga de `ordering_signal` heredado solo sobre-bloquea
   (conservador, no miente). Es por-turno: ver `test_gate_reads_current_turn_only`.
3. **Orden ≠ conteo.** No fuerces `total_count_approximate` en el fallback: si el count
   terminó, el total es exacto aunque la muestra no tenga orden garantizado.
4. **El fallback solo dispara ante errores RECUPERABLES** (`_is_timeout_exception`). Un 400 de
   SoQL/esquema/auth cae al camino honesto (no se oculta el defecto retirando el ORDER BY).
5. **Solo el fetch normal y direct_lookup están acotados.** Si añades un path nuevo que llama
   a Socrata, cablea `current_deadline()`/`fetch_budget_s` como en `_query_first_then_count`,
   o reintroduces una cola sin techo (fue el bug de los 67s de direct_lookup).
6. **`begin_deadline` debe correr ANTES del handler.** Está en `run_query` antes de
   `_dispatch`; cualquier trabajo fuera de ese scope NO ve el deadline (ContextVar).

## Telemetría (campos en el result)

- `turn_deadline_ms`, `turn_elapsed_ms`, `time_remaining_before_fetch_ms`, `fetch_budget_ms`,
  `fetch_elapsed_ms`, `fetch_status` (completed/timed_out/skipped), `deadline_exhausted`,
  `request_skipped_due_to_budget`, `count_skipped_due_to_budget`.
- H4: `fallback_attempted`, `fallback_used`, `fallback_strategy`, `fallback_query_status`,
  `primary_query_status`, `result_order_guarantee`, `transformations_applied`,
  `substantive_filters_preserved`.
- H1: `count_status` (exact/timed_out/skipped), `total_count_approximate`.

## Cómo perfilar (antes/después)

```
.venv/bin/python scripts/run_dogfood.py --input tests/fixtures/dogfood_queries.json \
    --output-dir artifacts/dogfood --delay 1.5 --run-id <id>
.venv/bin/python scripts/analyze_h4_profile.py \
    artifacts/dogfood/dogfood_<id>.jsonl artifacts/dogfood/dogfood_profile_h2_01.jsonl
```
El análisis emite p50/p90/max, inconclusas sin filas, fallbacks intentados/exitosos,
rankings falsos (debe ser 0), falsos EMPTY (0), y la inspección de los turnos `direct_lookup`.
**Ojo:** el recall recuperado por H4 es VARIABLE con la franja de Socrata (corre 2× en franjas
distintas antes de fijar cifras; lo enseñó Run1-vs-Run2).

## Gate de cierre (lo que se exigió)

p50 estable · p90 <15s · max ~deadline (excepción explicada) · turnos>15s=0/explicado ·
falsos EMPTY=0 · rankings falsos=0 · direct_lookup>15s=0 · filtros sustantivos preservados.

## Pendiente

H3/H5 DEFERRED (evidencia no los justifica). Etapa 8B (buffer canónico de paginación) PENDIENTE
— ver `docs/ESPEC_ETAPA8B_BUFFER_CANONICO.md` (unificar la ruta de ranking ANTES de cachear).

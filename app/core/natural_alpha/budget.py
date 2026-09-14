"""Approved additive Alpha latency; restore the enclosing turn in every exit."""
from contextlib import contextmanager

from app.core.deadline import begin_deadline, end_deadline, now, record_deadline


@contextmanager
def legacy_budget(observed: bool):
    if not observed:
        yield
        return
    # Alpha has its own provider timeout. Legacy gets its full original 15s.
    # Keep HandlerContext.run_started_at unchanged for end-to-end timings.
    started = now()
    tokens = begin_deadline(started)
    try:
        yield
    finally:
        telemetry = end_deadline(tokens)
        record_deadline(**telemetry)
        record_deadline(natural_alpha_legacy_budget_ms=15_000,
                        natural_alpha_legacy_elapsed_ms=round((now() - started) * 1000, 3))

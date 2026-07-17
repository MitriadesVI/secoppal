"""Tests de app/core/notify/retry.py — la aritmética pura del tiempo del claim.

Ninguno toca la base ni el reloj: si estas fórmulas necesitaran una base de datos
para verificarse, no serían recalibrables sin montar el sistema entero.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.core.db import connect_sqlite
from app.core.notify.retry import (
    DEFAULT_CLAIM_DURATION,
    DEFAULT_PROVIDER_TIMEOUT,
    DEFAULT_RECONCILIATION_CLAIM_DURATION,
    DEFAULT_RECONCILIATION_SAFETY_MARGIN,
    DEFAULT_SAFETY_MARGIN,
    DEFAULT_SQLITE_BUSY_TIMEOUT,
    RetryPolicy,
    backoff_delay,
    claim_duration,
    next_not_before,
    reconciliation_claim_duration,
    should_retry,
)


# ── 17 · la fórmula del claim ────────────────────────────────────────────────────

def test_17_claim_duration_default_is_120_seconds():
    """17 · 10 s + 2×30 s + 50 s = 120 s. El 2× no es holgura: son las dos escrituras
    que pueden esperar el lock durante la vida del claim."""
    result = claim_duration(
        provider_timeout=timedelta(seconds=10),
        sqlite_busy_timeout=timedelta(seconds=30),
        safety_margin=timedelta(seconds=50),
    )
    assert result == timedelta(seconds=120)


def test_17b_default_constant_matches_the_documented_derivation():
    """17b · El default publicado es el de la fórmula, no un 120 escrito a mano."""
    assert DEFAULT_CLAIM_DURATION == timedelta(seconds=120)
    assert DEFAULT_CLAIM_DURATION == claim_duration(
        provider_timeout=DEFAULT_PROVIDER_TIMEOUT,
        sqlite_busy_timeout=DEFAULT_SQLITE_BUSY_TIMEOUT,
        safety_margin=DEFAULT_SAFETY_MARGIN,
    )


def test_17c_claim_duration_counts_busy_timeout_twice():
    """17c · Un solo busy_timeout dejaría el claim por debajo de las dos escrituras
    bloqueables. Subir el busy_timeout en 1 s debe subir el claim en 2 s."""
    base = claim_duration(
        provider_timeout=timedelta(seconds=10),
        sqlite_busy_timeout=timedelta(seconds=30),
        safety_margin=timedelta(seconds=50),
    )
    bumped = claim_duration(
        provider_timeout=timedelta(seconds=10),
        sqlite_busy_timeout=timedelta(seconds=31),
        safety_margin=timedelta(seconds=50),
    )
    assert bumped - base == timedelta(seconds=2)


def test_17d_claim_duration_exceeds_provider_timeout_plus_two_busy():
    """17d · La propiedad que hace útil la fórmula: el claim siempre sobrevive al
    peor caso de proveedor + los dos locks (INV-W21)."""
    pt, busy = DEFAULT_PROVIDER_TIMEOUT, DEFAULT_SQLITE_BUSY_TIMEOUT
    assert DEFAULT_CLAIM_DURATION > pt + 2 * busy


# ── 18 · la fórmula de reconciliación ────────────────────────────────────────────

def test_18_reconciliation_claim_duration_default_is_70_seconds():
    """18 · 2×30 s + 10 s = 70 s. Sin término de proveedor: la reconciliación no
    envía nada, así que pagar por un timeout que no ocurre sería alargar sin razón el
    tiempo en que el huérfano es intocable."""
    result = reconciliation_claim_duration(
        sqlite_busy_timeout=timedelta(seconds=30),
        safety_margin=timedelta(seconds=10),
    )
    assert result == timedelta(seconds=70)


def test_18b_reconciliation_default_constant_matches_formula():
    """18b · El default publicado sale de la fórmula."""
    assert DEFAULT_RECONCILIATION_CLAIM_DURATION == timedelta(seconds=70)
    assert DEFAULT_RECONCILIATION_CLAIM_DURATION == reconciliation_claim_duration(
        sqlite_busy_timeout=DEFAULT_SQLITE_BUSY_TIMEOUT,
        safety_margin=DEFAULT_RECONCILIATION_SAFETY_MARGIN,
    )


def test_18c_reconciliation_has_no_provider_term():
    """18c · La reconciliación no acepta provider_timeout: no hay proveedor que
    esperar. Si algún día lo aceptara, alguien estaría enviando desde ahí."""
    with pytest.raises(TypeError):
        reconciliation_claim_duration(
            provider_timeout=timedelta(seconds=10),
            sqlite_busy_timeout=timedelta(seconds=30),
            safety_margin=timedelta(seconds=10),
        )
    assert DEFAULT_RECONCILIATION_CLAIM_DURATION < DEFAULT_CLAIM_DURATION


# ── 19 · negativos ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("field", ["provider_timeout", "sqlite_busy_timeout",
                                   "safety_margin"])
def test_19_claim_duration_rejects_negative_inputs(field):
    """19 · Un input negativo no es una duración: acortaría el claim en silencio."""
    kwargs = {
        "provider_timeout": timedelta(seconds=10),
        "sqlite_busy_timeout": timedelta(seconds=30),
        "safety_margin": timedelta(seconds=50),
    }
    kwargs[field] = timedelta(seconds=-1)
    with pytest.raises(ValueError):
        claim_duration(**kwargs)


@pytest.mark.parametrize("field", ["sqlite_busy_timeout", "safety_margin"])
def test_19b_reconciliation_rejects_negative_inputs(field):
    """19b · Lo mismo en la fórmula de reconciliación."""
    kwargs = {
        "sqlite_busy_timeout": timedelta(seconds=30),
        "safety_margin": timedelta(seconds=10),
    }
    kwargs[field] = timedelta(seconds=-1)
    with pytest.raises(ValueError):
        reconciliation_claim_duration(**kwargs)


def test_19c_zero_everything_is_rejected_as_non_positive_result():
    """19c · Ceros son individualmente válidos, pero un claim de duración cero vence
    en el instante en que se toma: el resultado tiene que ser positivo."""
    with pytest.raises(ValueError, match="positivo"):
        claim_duration(
            provider_timeout=timedelta(0),
            sqlite_busy_timeout=timedelta(0),
            safety_margin=timedelta(0),
        )
    with pytest.raises(ValueError, match="positivo"):
        reconciliation_claim_duration(
            sqlite_busy_timeout=timedelta(0), safety_margin=timedelta(0)
        )


def test_19d_partial_zeros_still_produce_a_positive_claim():
    """19d · Un cero aislado no invalida nada mientras el total sea positivo."""
    assert claim_duration(
        provider_timeout=timedelta(0),
        sqlite_busy_timeout=timedelta(0),
        safety_margin=timedelta(seconds=1),
    ) == timedelta(seconds=1)


# ── 20 · tipos ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", [True, False, 10, 10.0, "10s", None, timedelta])
@pytest.mark.parametrize("field", ["provider_timeout", "sqlite_busy_timeout",
                                   "safety_margin"])
def test_20_claim_duration_rejects_non_timedelta(field, bad):
    """20 · bool, int y str no son duraciones. `provider_timeout=10` sería ambiguo
    entre segundos y minutos, y bool es el error que produce copiar una firma con
    keywords."""
    kwargs = {
        "provider_timeout": timedelta(seconds=10),
        "sqlite_busy_timeout": timedelta(seconds=30),
        "safety_margin": timedelta(seconds=50),
    }
    kwargs[field] = bad
    with pytest.raises(TypeError):
        claim_duration(**kwargs)


@pytest.mark.parametrize("bad", [True, 30, "30", None])
@pytest.mark.parametrize("field", ["sqlite_busy_timeout", "safety_margin"])
def test_20b_reconciliation_rejects_non_timedelta(field, bad):
    """20b · Lo mismo en la fórmula de reconciliación."""
    kwargs = {
        "sqlite_busy_timeout": timedelta(seconds=30),
        "safety_margin": timedelta(seconds=10),
    }
    kwargs[field] = bad
    with pytest.raises(TypeError):
        reconciliation_claim_duration(**kwargs)


def test_20c_inputs_are_keyword_only():
    """20c · Posicionales invitan a intercambiar provider_timeout y busy_timeout sin
    que nada lo note."""
    with pytest.raises(TypeError):
        claim_duration(timedelta(seconds=10), timedelta(seconds=30),
                       timedelta(seconds=50))
    with pytest.raises(TypeError):
        reconciliation_claim_duration(timedelta(seconds=30), timedelta(seconds=10))


# ── 21 · independencia del reloj y de la zona ────────────────────────────────────

@pytest.mark.parametrize("tz", [
    timezone.utc,
    timezone(timedelta(hours=-5)),   # Bogotá
    timezone(timedelta(hours=+9)),   # Tokio
])
def test_21_result_is_timezone_independent(tz):
    """21 · La duración es un intervalo, no un instante: aplicada en cualquier zona
    produce el mismo desplazamiento absoluto. Un claim no puede durar distinto según
    dónde corra el worker."""
    moment = datetime(2026, 7, 15, 12, 0, tzinfo=tz)
    until = moment + DEFAULT_CLAIM_DURATION
    assert (until - moment) == timedelta(seconds=120)
    assert (until.astimezone(timezone.utc) - moment.astimezone(timezone.utc)) == (
        timedelta(seconds=120)
    )


def test_21b_formula_is_pure_and_deterministic():
    """21b · Sin estado: mil llamadas, un resultado. No lee reloj ni configuración."""
    results = {
        claim_duration(
            provider_timeout=timedelta(seconds=10),
            sqlite_busy_timeout=timedelta(seconds=30),
            safety_margin=timedelta(seconds=50),
        )
        for _ in range(1000)
    }
    assert results == {timedelta(seconds=120)}


def test_21c_formula_needs_no_database(tmp_path):
    """21c · T-CLAIM-PURE: la fórmula se verifica sin base. El test lo prueba
    ejecutándose sin abrir ninguna."""
    assert not list(tmp_path.iterdir())
    assert claim_duration(
        provider_timeout=timedelta(milliseconds=1),
        sqlite_busy_timeout=timedelta(milliseconds=1),
        safety_margin=timedelta(milliseconds=1),
    ) == timedelta(milliseconds=4)
    assert not list(tmp_path.iterdir())


def test_21d_documented_busy_timeout_matches_the_real_pragma(tmp_path):
    """21d · `DEFAULT_SQLITE_BUSY_TIMEOUT` es una copia de `db.py` (no hay constante
    que importar). Aquí se compara contra el PRAGMA real: si alguien cambia db.py sin
    recalibrar la fórmula, el claim quedaría por debajo de sus dos escrituras
    bloqueables y este test lo dice."""
    conn = connect_sqlite(tmp_path / "pragma.db")
    try:
        busy_ms = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    finally:
        conn.close()
    assert timedelta(milliseconds=busy_ms) == DEFAULT_SQLITE_BUSY_TIMEOUT


# ══ R2B · Política de reintentos ═════════════════════════════════════════════════

_NOW = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)


def test_22a_backoff_is_exponential_from_the_base():
    policy = RetryPolicy(base=timedelta(seconds=30), factor=2.0,
                         cap=timedelta(hours=1), jitter_ratio=0.0)
    assert backoff_delay(attempt_number=1, policy=policy) == timedelta(seconds=30)
    assert backoff_delay(attempt_number=2, policy=policy) == timedelta(seconds=60)
    assert backoff_delay(attempt_number=3, policy=policy) == timedelta(seconds=120)


def test_22b_backoff_is_capped():
    """Sin techo, el octavo reintento esperaría más de una hora y el batch moriría de
    viejo antes de volver a intentarse."""
    policy = RetryPolicy(base=timedelta(seconds=30), factor=2.0,
                         cap=timedelta(minutes=5), jitter_ratio=0.0)
    assert backoff_delay(attempt_number=20, policy=policy) == timedelta(minutes=5)


def test_22c_jitter_only_subtracts():
    """El jitter nunca suma: sumar podría empujar el reintento más allá del TTL y
    convertir una espera en una muerte silenciosa."""
    policy = RetryPolicy(base=timedelta(seconds=100), factor=1.0,
                         cap=timedelta(hours=1), jitter_ratio=0.2)
    entero = backoff_delay(attempt_number=1, policy=policy, jitter=0.0)
    máximo = backoff_delay(attempt_number=1, policy=policy, jitter=0.999)
    assert entero == timedelta(seconds=100)
    assert timedelta(seconds=80) <= máximo < entero


def test_22d_jitter_out_of_range_is_rejected():
    for bad in (-0.1, 1.0, 2.0):
        with pytest.raises(ValueError, match="jitter debe estar"):
            backoff_delay(attempt_number=1, jitter=bad)


def test_22e_should_retry_exhausts_at_max_attempts():
    policy = RetryPolicy(max_attempts=3)
    assert should_retry(attempt_number=1, policy=policy) is True
    assert should_retry(attempt_number=2, policy=policy) is True
    assert should_retry(attempt_number=3, policy=policy) is False
    assert should_retry(attempt_number=4, policy=policy) is False


def test_22f_attempt_number_below_one_is_rejected():
    """El ledger numera desde 1: un cero significaría un fallo que nunca se intentó."""
    with pytest.raises(ValueError, match="al menos 1"):
        should_retry(attempt_number=0)
    with pytest.raises(TypeError):
        should_retry(attempt_number=True)


def test_22g_next_not_before_takes_the_latest():
    """Manda el que espera más: obedecer solo al proveedor dejaría que un
    ``retry_after`` de cero nos hiciera martillearlo; obedecer solo al nuestro
    ignoraría un 429 explícito."""
    policy = RetryPolicy(base=timedelta(seconds=30), jitter_ratio=0.0)
    tarde = _NOW + timedelta(hours=2)
    assert next_not_before(
        attempt_number=1, now=_NOW, policy=policy, retry_after=tarde
    ) == tarde
    assert next_not_before(
        attempt_number=1, now=_NOW, policy=policy,
        retry_after=_NOW + timedelta(seconds=1),
    ) == _NOW + timedelta(seconds=30)


def test_22h_next_not_before_without_retry_after_uses_the_backoff():
    policy = RetryPolicy(base=timedelta(seconds=30), jitter_ratio=0.0)
    assert next_not_before(
        attempt_number=1, now=_NOW, policy=policy
    ) == _NOW + timedelta(seconds=30)


def test_22i_naive_instants_are_rejected():
    with pytest.raises(ValueError, match="zona horaria"):
        next_not_before(attempt_number=1, now=datetime(2026, 7, 16, 10, 0))
    with pytest.raises(ValueError, match="zona horaria"):
        next_not_before(
            attempt_number=1, now=_NOW, retry_after=datetime(2026, 7, 16, 11, 0)
        )


def test_22j_incoherent_policies_are_rejected_at_construction():
    """Una política incoherente revienta donde se escribe, no tres capas más abajo
    cuando alguien la usa."""
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError, match="cap"):
        RetryPolicy(base=timedelta(minutes=10), cap=timedelta(seconds=30))
    with pytest.raises(ValueError, match="factor"):
        RetryPolicy(factor=0.5)
    with pytest.raises(ValueError, match="jitter_ratio"):
        RetryPolicy(jitter_ratio=1.0)
    with pytest.raises(ValueError, match="base"):
        RetryPolicy(base=timedelta(0))


def test_22k_the_module_stays_pure(tmp_path):
    """Sin base, sin reloj, sin ``random``: la aritmética se verifica sin parchear
    nada. Si el jitter se sacara de dentro, este módulo dejaría de ser comprobable."""
    import app.core.notify.retry as module

    assert "random" not in dir(module)
    backoff_delay(attempt_number=3)
    next_not_before(attempt_number=3, now=_NOW)
    assert not list(tmp_path.iterdir())

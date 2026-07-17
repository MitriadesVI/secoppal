from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.core.notify.quiet_hours import QuietHoursError, quiet_hours_not_before


BOGOTA = "America/Bogota"
NIGHT = {"start": "22:00", "end": "07:00"}      # cruza medianoche
DAWN = {"start": "01:00", "end": "07:00"}       # ventana normal


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text)


def _at(local_hhmm: str, *, day="2026-07-15", tz=BOGOTA):
    """Instante aware construido desde la hora local de la persona."""
    from zoneinfo import ZoneInfo

    return datetime.fromisoformat(f"{day}T{local_hhmm}:00").replace(tzinfo=ZoneInfo(tz))


def _call(now, *, start=None, end=None, tz=BOGOTA):
    return quiet_hours_not_before(now, start=start, end=end, timezone_name=tz)


# ── Sin ventana ───────────────────────────────────────────────────────────────

def test_no_window_configured_never_defers():
    assert _call(_at("23:00")) is None


def test_start_equal_to_end_disables_the_window():
    # Una ventana de duración cero no es "silencio siempre": está apagada.
    assert _call(_at("23:00"), start="22:00", end="22:00") is None


# ── Ventana normal ────────────────────────────────────────────────────────────

def test_normal_window_before_it_starts_delivers_now():
    assert _call(_at("00:30"), **DAWN) is None


def test_normal_window_inside_returns_its_end():
    assert _call(_at("03:00"), **DAWN) == "2026-07-15T12:00:00+00:00"  # 07:00 -05:00


def test_exactly_at_the_start_is_inside():
    assert _call(_at("01:00"), **DAWN) == "2026-07-15T12:00:00+00:00"


def test_exactly_at_the_end_is_outside():
    # El borde superior es abierto: a las 07:00 en punto ya se puede entregar.
    assert _call(_at("07:00"), **DAWN) is None


# ── Ventana que cruza medianoche ──────────────────────────────────────────────

def test_crossing_midnight_evening_leg_ends_next_morning():
    assert _call(_at("22:30"), **NIGHT) == "2026-07-16T12:00:00+00:00"


def test_crossing_midnight_dawn_leg_ends_the_same_morning():
    assert _call(_at("03:00"), **NIGHT) == "2026-07-15T12:00:00+00:00"


def test_crossing_midnight_exactly_at_the_end_is_outside():
    assert _call(_at("07:00"), **NIGHT) is None


def test_crossing_midnight_midday_is_outside():
    assert _call(_at("12:00"), **NIGHT) is None


# ── Zona horaria ──────────────────────────────────────────────────────────────

def test_bogota_window_is_converted_to_utc():
    # 22:30 en Bogotá es 03:30 UTC del día siguiente: la ventana es de la persona,
    # no del servidor.
    now = _utc("2026-07-16T03:30:00+00:00")
    assert _call(now, **NIGHT) == "2026-07-16T12:00:00+00:00"


def test_dst_zone_uses_the_offset_in_force_not_a_hardcoded_one():
    """America/New_York: mismo 07:00 local, dos UTC distintos según la estación.

    Si alguien fijara el offset, uno de los dos saldría con una hora de error —y esa
    hora es justo la diferencia entre respetar el silencio y no hacerlo.
    """
    winter = _call(_utc("2026-01-15T03:00:00+00:00"), **NIGHT, tz="America/New_York")
    summer = _call(_utc("2026-07-15T03:00:00+00:00"), **NIGHT, tz="America/New_York")
    assert winter == "2026-01-15T12:00:00+00:00"   # 07:00 EST = UTC-5
    assert summer == "2026-07-15T11:00:00+00:00"   # 07:00 EDT = UTC-4
    assert winter[11:13] != summer[11:13]


# ── Fallos cerrados ───────────────────────────────────────────────────────────

def test_only_start_or_only_end_is_half_a_configuration_and_fails():
    with pytest.raises(QuietHoursError, match="inicio y fin"):
        _call(_at("23:00"), start="22:00", end=None)
    with pytest.raises(QuietHoursError, match="inicio y fin"):
        _call(_at("23:00"), start=None, end="07:00")


@pytest.mark.parametrize("start,end", [
    ("7:00", "08:00"), ("22:00", "7:00"), ("24:00", "07:00"), ("22:60", "07:00"),
    ("22h00", "07:00"), ("", "07:00"), ("22:00:00", "07:00"),
])
def test_invalid_hhmm_fails(start, end):
    with pytest.raises(QuietHoursError):
        _call(_at("23:00"), start=start, end=end)


@pytest.mark.parametrize("tz", ["Marte/Olympus", "", "UTC+5", None])
def test_invalid_timezone_fails_and_never_falls_back_to_utc(tz):
    with pytest.raises(QuietHoursError):
        _call(_utc("2026-07-16T03:30:00+00:00"), **NIGHT, tz=tz)


def test_naive_now_fails():
    with pytest.raises(QuietHoursError, match="zona horaria"):
        _call(datetime(2026, 7, 15, 23, 0), **NIGHT)


def test_timezone_is_validated_even_when_there_is_no_window():
    # Si no se validara, una zona rota pasaría inadvertida hasta el día en que
    # alguien configure silencio.
    with pytest.raises(QuietHoursError):
        _call(_utc("2026-07-16T03:30:00+00:00"), start=None, end=None, tz="Marte/Olympus")


# ── Determinismo ──────────────────────────────────────────────────────────────

def test_one_hundred_repetitions_are_identical():
    now = _utc("2026-07-16T03:30:00+00:00")
    expected = _call(now, **NIGHT)
    assert expected is not None
    for _ in range(100):
        assert _call(now, **NIGHT) == expected


def test_result_is_always_utc_and_after_now():
    for hhmm in ("22:00", "23:59", "00:00", "03:00", "06:59"):
        now = _at(hhmm)
        result = _call(now, **NIGHT)
        assert result is not None and result.endswith("+00:00")
        assert _utc(result) > now


# ── DST: las dos trampas de convertir hora de pared a instante ────────────────
#
# `datetime.combine(fecha, hora, tzinfo=zone)` deja `fold=0` implícito. Eso es
# correcto 363 días al año y un bug silencioso los otros dos.

NY = "America/New_York"


def test_ambiguous_local_end_picks_the_later_occurrence_not_a_past_instant():
    """Otoño 2026-11-01 en NY: 01:00–02:00 ocurre dos veces (EDT y luego EST).

    `now` es la segunda ocurrencia de las 01:15 (06:15Z). Con `fold=0` el fin de
    silencio se calcula como 01:30 EDT = 05:30Z, **45 minutos en el pasado**: el
    batch queda `pending` con un `not_before` vencido, `claim_candidate` lo acepta
    de inmediato y el worker entra en un bucle de reclamar-y-diferir mientras la
    persona duerme. Justo lo contrario de lo que la ventana promete.
    """
    now = _utc("2026-11-01T06:15:00+00:00")
    result = _call(now, start="22:00", end="01:30", tz=NY)
    assert result == "2026-11-01T06:30:00+00:00"      # 01:30 EST, la segunda vez
    assert _utc(result) > now


def test_nonexistent_local_end_advances_to_the_first_valid_minute():
    """Primavera 2026-03-08 en NY: 02:00 salta a 03:00, así que "las 02:30" no
    existen esa noche. No hay una hora que respetar: se usa el primer instante
    local que sí existe después."""
    now = _utc("2026-03-08T06:00:00+00:00")            # 01:00 EST
    result = _call(now, start="22:00", end="02:30", tz=NY)
    assert result == "2026-03-08T07:00:00+00:00"       # 03:00 EDT
    assert _utc(result) > now


def test_being_inside_the_window_always_yields_a_future_instant():
    """La red de seguridad: si estamos dentro del silencio, su fin es futuro por
    construcción. Un resultado en el pasado sería un bucle, no una espera."""
    for minutes in range(0, 24 * 60, 7):
        now = _utc("2026-11-01T04:00:00+00:00") + timedelta(minutes=minutes)
        result = _call(now, start="22:00", end="01:30", tz=NY)
        if result is not None:
            assert _utc(result) > now, f"not_before en el pasado con now={now}"


def test_resolve_local_instant_policies_cover_both_dst_edges():
    """Los dos extremos de la ventana quieren lados opuestos de la hora repetida, y
    los dos eligen el que produce más silencio."""
    from zoneinfo import ZoneInfo

    from app.core.notify.quiet_hours import resolve_local_instant

    zone = ZoneInfo(NY)
    repeated = datetime(2026, 11, 1, 1, 30)

    latest = resolve_local_instant(repeated, zone, ambiguous="latest")
    assert latest.astimezone(timezone.utc) == _utc("2026-11-01T06:30:00+00:00")
    assert latest.fold == 1

    earliest = resolve_local_instant(repeated, zone, ambiguous="earliest")
    assert earliest.astimezone(timezone.utc) == _utc("2026-11-01T05:30:00+00:00")
    assert earliest.fold == 0

    for policy in ("earliest", "latest"):
        nonexistent = resolve_local_instant(
            datetime(2026, 3, 8, 2, 30), zone, ambiguous=policy
        )
        assert nonexistent.astimezone(timezone.utc) == _utc("2026-03-08T07:00:00+00:00")
        assert nonexistent.hour == 3 and nonexistent.minute == 0   # 03:00 EDT

        normal = resolve_local_instant(
            datetime(2026, 7, 15, 7, 0), zone, ambiguous=policy
        )
        assert normal.astimezone(timezone.utc) == _utc("2026-07-15T11:00:00+00:00")


def test_resolve_local_instant_requires_an_explicit_ambiguity_policy():
    from zoneinfo import ZoneInfo

    from app.core.notify.quiet_hours import resolve_local_instant

    with pytest.raises(TypeError):
        resolve_local_instant(datetime(2026, 11, 1, 1, 30), ZoneInfo(NY))
    with pytest.raises(QuietHoursError, match="ambigua"):
        resolve_local_instant(
            datetime(2026, 11, 1, 1, 30), ZoneInfo(NY), ambiguous="whatever"
        )


# ── Pertenencia por intervalo absoluto, no por hora de pared ──────────────────

def test_membership_uses_absolute_intervals_across_the_fall_back_hour():
    """El bloqueante: a las 05:45Z el reloj de pared marca 01:45 y la ventana cierra
    a la "01:30", así que comparar horas de pared dice "ya salió del silencio". Pero
    esas 01:45 son la *primera* ocurrencia: la ventana que abrió a las 22:00 EDT del
    31-oct dura en tiempo real de 02:00Z a 06:30Z, y 05:45Z cae dentro. La hora de
    pared no puede responder "¿01:45 va antes de 01:30?" en una noche donde las 01:45
    ocurren dos veces.
    """
    now = _utc("2026-11-01T05:45:00+00:00")
    assert _call(now, start="22:00", end="01:30", tz=NY) == "2026-11-01T06:30:00+00:00"


def test_after_the_ambiguous_close_the_window_is_over():
    """Misma hora de pared (01:45), segunda ocurrencia: ya pasó el cierre real."""
    now = _utc("2026-11-01T06:45:00+00:00")
    assert _call(now, start="22:00", end="01:30", tz=NY) is None


def test_window_fully_inside_the_repeated_hour_starts_early_and_ends_late():
    """Ventana 01:00–01:30 en la noche del cambio: el inicio se resuelve a la
    ocurrencia temprana y el cierre a la tardía, así que dura 90 minutos reales
    (05:00Z–06:30Z) en vez de 30. Errar hacia más silencio, en los dos extremos."""
    now = _utc("2026-11-01T05:15:00+00:00")             # 01:15, primera ocurrencia
    assert _call(now, start="01:00", end="01:30", tz=NY) == "2026-11-01T06:30:00+00:00"
    # Y el borde inferior sigue siendo cerrado, el superior abierto.
    assert _call(_utc("2026-11-01T05:00:00+00:00"), start="01:00", end="01:30", tz=NY) \
        == "2026-11-01T06:30:00+00:00"
    assert _call(_utc("2026-11-01T04:59:00+00:00"), start="01:00", end="01:30", tz=NY) is None
    assert _call(_utc("2026-11-01T06:30:00+00:00"), start="01:00", end="01:30", tz=NY) is None


def test_every_minute_of_both_dst_days_is_consistent():
    """Barrido denso sobre las dos transiciones: nunca un `not_before` en el pasado,
    y nunca un hueco dentro de la ventana absoluta."""
    for day in ("2026-11-01T00:00:00+00:00", "2026-03-08T00:00:00+00:00"):
        for minutes in range(0, 24 * 60, 5):
            now = _utc(day) + timedelta(minutes=minutes)
            result = _call(now, start="22:00", end="01:30", tz=NY)
            if result is not None:
                assert _utc(result) > now, f"not_before en el pasado con now={now}"


def test_a_past_close_means_outside_the_window_never_a_past_not_before():
    """La garantía de "nunca un `not_before` vencido" dejó de ser una comprobación
    aparte: `start_utc <= now < end_utc` es a la vez la condición de estar dentro y
    la de que el cierre es futuro. Una ventana cuyo cierre ya pasó no es una ventana
    de la que haya que salir — es una en la que no estamos.

    Lo que antes era una guarda defensiva (y una rama que ningún input alcanzaba) es
    ahora el propio criterio de pertenencia. Se prueba el invariante, no la guarda.
    """
    closed = _utc("2026-07-16T13:00:00+00:00")          # 08:00 en Bogotá: ya amaneció
    assert _call(closed, **NIGHT) is None
    for minutes in range(0, 24 * 60, 11):
        now = _utc("2026-07-15T00:00:00+00:00") + timedelta(minutes=minutes)
        result = _call(now, **NIGHT)
        assert result is None or _utc(result) > now

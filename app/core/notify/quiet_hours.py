"""Política de quiet hours: una sola implementación, pura y determinista.

Vive aparte de ``selector`` y ``worker_store`` porque los dos la necesitan y una
copia en cada uno sería lo peor posible: el selector congelaría un ``not_before``
con una ventana y el worker abriría el intento con otra. Que el silencio se
respete no puede depender de que dos algoritmos gemelos no se separen nunca.

Sin SQLite, sin stores, sin reloj global: ``now`` entra por parámetro. Fallar
cerrado es deliberado — una configuración que este módulo no entiende no puede
degradarse a "entrega ahora", porque el error se paga despertando a alguien.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class QuietHoursError(ValueError):
    """Configuración o instante que la política no puede interpretar."""


_HHMM = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def parse_hhmm(value: str | None, label: str) -> time:
    match = _HHMM.fullmatch(value or "")
    if match is None:
        raise QuietHoursError(f"quiet_hours_{label} inválida: {value!r}; use HH:MM")
    return time(int(match.group(1)), int(match.group(2)))


def resolve_zone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as exc:
        raise QuietHoursError(f"Timezone inválida: {timezone_name!r}") from exc


# Un salto DST no puede durar más de un día; la cota evita un bucle infinito si
# alguna zona exótica hiciera algo que este módulo no prevé.
_MAX_GAP_MINUTES = 24 * 60


def _is_real_local_time(aware: datetime) -> bool:
    """Round-trip zona→UTC→zona: una hora que no existe vuelve como otra distinta."""
    roundtrip = aware.astimezone(timezone.utc).astimezone(aware.tzinfo)
    return roundtrip.replace(tzinfo=None, fold=0) == aware.replace(tzinfo=None, fold=0)


AMBIGUOUS_EARLIEST = "earliest"
AMBIGUOUS_LATEST = "latest"


def resolve_local_instant(
    naive: datetime, zone: ZoneInfo, *, ambiguous: str
) -> datetime:
    """Hora local de pared → instante real, resolviendo los dos casos del DST.

    ``datetime.combine(fecha, hora, tzinfo=zone)`` deja ``fold=0`` implícito, y eso
    es un bug silencioso dos veces al año: en la hora repetida del otoño elige
    siempre la primera ocurrencia.

    ``ambiguous`` es obligatorio y no tiene default porque los dos extremos de la
    ventana quieren cosas opuestas: el inicio, la ocurrencia más temprana; el
    cierre, la más tardía. Las dos eligen el lado que produce **más silencio** —
    retrasar un mensaje se deshace, sonar el teléfono de madrugada no—. Un default
    acertaría en un extremo y fallaría en el otro sin decirlo.
    """
    if ambiguous not in (AMBIGUOUS_EARLIEST, AMBIGUOUS_LATEST):
        raise QuietHoursError(
            f"Política de hora ambigua inválida: {ambiguous!r}; use "
            f"{AMBIGUOUS_EARLIEST!r} o {AMBIGUOUS_LATEST!r}"
        )
    candidates: list[datetime] = []
    for fold in (0, 1):
        aware = naive.replace(tzinfo=zone, fold=fold)
        if _is_real_local_time(aware):
            candidates.append(aware)
    if candidates:
        pick = min if ambiguous == AMBIGUOUS_EARLIEST else max
        return pick(candidates, key=lambda moment: moment.astimezone(timezone.utc))
    # Hora inexistente (salto de primavera): el primer minuto local que sí existe
    # después. No hay "las 02:30" que respetar si esa noche nunca las hubo.
    for offset in range(1, _MAX_GAP_MINUTES + 1):
        aware = (naive + timedelta(minutes=offset)).replace(tzinfo=zone, fold=0)
        if _is_real_local_time(aware):
            return aware
    raise QuietHoursError(
        f"No existe ninguna hora local válida a partir de {naive.isoformat()} "
        f"en {zone.key!r} dentro de 24 horas"
    )


def _window_bounds(
    base_date: date, start_t: time, end_t: time, zone: ZoneInfo
) -> tuple[datetime, datetime]:
    """La ventana que **empieza** en ``base_date``, como intervalo absoluto UTC."""
    start_utc = resolve_local_instant(
        datetime.combine(base_date, start_t), zone, ambiguous=AMBIGUOUS_EARLIEST
    ).astimezone(timezone.utc)
    # Si cruza medianoche, el cierre es del día siguiente al que abrió.
    end_date = base_date + timedelta(days=1) if start_t > end_t else base_date
    end_utc = resolve_local_instant(
        datetime.combine(end_date, end_t), zone, ambiguous=AMBIGUOUS_LATEST
    ).astimezone(timezone.utc)
    return start_utc, end_utc


def quiet_hours_not_before(
    now: datetime,
    *,
    start: str | None,
    end: str | None,
    timezone_name: str,
) -> str | None:
    """Próximo fin de silencio en UTC ISO-8601, o ``None`` si se puede entregar.

    La ventana se evalúa en la zona de la persona, no en la del servidor: 22:00
    significa las diez de la noche donde ella vive. Una ventana que cruza
    medianoche (``22:00–07:00``) es el caso normal, no el raro.
    """
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise QuietHoursError("now debe incluir zona horaria")
    zone = resolve_zone(timezone_name)
    if start is None and end is None:
        return None
    if (start is None) != (end is None):
        # Media configuración no es "sin silencio": es una intención a medio
        # escribir, y adivinar cuál de las dos mitades vale es inventarla.
        raise QuietHoursError("quiet hours requiere inicio y fin")
    assert start is not None and end is not None
    start_t, end_t = parse_hhmm(start, "start"), parse_hhmm(end, "end")
    if start_t == end_t:
        return None

    # La pertenencia se decide sobre instantes absolutos, no sobre horas de pared.
    # Comparar el reloj de pared sería preguntar "¿01:45 está antes de 01:30?" en una
    # noche donde las 01:45 ocurren dos veces: la respuesta es "depende de cuál", y la
    # hora sola no lo dice. La ventana 22:00–01:30 del 31-oct dura, en tiempo real,
    # de 02:00Z a 06:30Z; a las 05:45Z todavía es de noche aunque el reloj de pared
    # ya marque 01:45. Solo el intervalo absoluto sabe eso.
    local = now.astimezone(zone)
    for base_date in (local.date() - timedelta(days=1), local.date()):
        start_utc, end_utc = _window_bounds(base_date, start_t, end_t, zone)
        if start_utc <= now < end_utc:
            # ``now < end_utc`` es la condición de estar dentro *y* la garantía de
            # que lo devuelto es futuro: no hacen falta dos comprobaciones.
            return end_utc.isoformat()
    return None


__all__ = [
    "QuietHoursError",
    "parse_hhmm",
    "quiet_hours_not_before",
    "resolve_local_instant",
    "resolve_zone",
]

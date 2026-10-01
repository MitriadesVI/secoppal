"""Autoridad determinística para scopes temporales de consultas SECOPPAL."""
from __future__ import annotations

import calendar
import re
from dataclasses import dataclass, replace
from datetime import date as Date, timedelta
from typing import Any

from app.core import temporal_window_anchor
from app.core.temporal_bounds import apply_bound, find_bound, open_suffix_end


@dataclass(frozen=True)
class TemporalResolution:
    """Resolución única y enmascarable de una expresión temporal."""

    matched: bool = False
    fecha_desde: str | None = None
    fecha_hasta: str | None = None
    phrase: str = ""
    span: tuple[int, int] | None = None
    precision: str = ""
    error_code: str | None = None
    clarification: str | None = None
    # "" = explícita; "relative" = depende del reloj («este mes»); "inferred_year" =
    # el año salió del reloj porque el texto no lo dice («marzo»).
    clock: str = ""
    # Sólo con ``clock == "inferred_year"``: primer día de la última unidad nombrada
    # (día o mes) en el año asumido. La autoridad nueva lo compara con ``today``.
    anchor: str | None = None

    @property
    def should_clarify(self) -> bool:
        return self.error_code is not None


_MONTH_ALIASES: dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    "ene": 1, "feb": 2, "mar": 3, "abr": 4,
    "may": 5, "jun": 6, "jul": 7, "ago": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dic": 12,
}
_MONTH_TOKEN = "|".join(
    sorted((re.escape(name) for name in _MONTH_ALIASES), key=len, reverse=True)
)
_YEAR_TOKEN = r"20\d{2}"
# «marzo de 2025», «marzo del 2025», «marzo, 2025», «marzo-2025», «marzo 2025»
_YEAR_JOIN = r"(?:\s*[,/-]\s*|\s+(?:(?:de|del)\s+)?)"
_COUNT_WORDS = {
    "un": 1, "uno": 1, "una": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5,
    "seis": 6, "siete": 7, "ocho": 8, "nueve": 9, "diez": 10, "once": 11,
    "doce": 12, "quince": 15, "veinte": 20, "treinta": 30,
}
_COUNT = r"\d+|" + "|".join(sorted(_COUNT_WORDS, key=len, reverse=True))
_MONEY_AFTER_YEAR = (
    r"(?!\s*(?:millones?|millon|mil\s+millones|billones?|palos?|barras?|pesos))"
)
_HACE_MESES = re.compile(rf"\bhace\s+({_COUNT})\s+mes(?:es)?\b", re.IGNORECASE)
_HACE_ANIOS = re.compile(rf"\bhace\s+({_COUNT})\s+a[nñ]os?\b", re.IGNORECASE)
_ULTIMOS_MESES = re.compile(
    rf"\b(?:los\s+)?(?:ultimos|últimos)\s+({_COUNT})\s+meses\b", re.IGNORECASE,
)
_ULTIMOS_ANIOS = re.compile(
    rf"\b(?:los\s+)?(?:ultimos|últimos)\s+({_COUNT})\s+a[nñ]os\b", re.IGNORECASE,
)
_ULTIMA_SEMANA = re.compile(r"\b(?:la\s+)?(?:ultima|última)\s+semana\b", re.IGNORECASE)
_HOY = re.compile(r"\bhoy\b", re.IGNORECASE)
_AYER = re.compile(r"\bayer\b", re.IGNORECASE)
_ESTA_SEMANA = re.compile(r"\besta\s+semana\b", re.IGNORECASE)
_SEMANA_PASADA = re.compile(r"\b(?:la\s+)?semana\s+pasada\b", re.IGNORECASE)
_ULTIMOS_DIAS = re.compile(
    rf"\b(?:ultimos|últimos)\s+({_COUNT})\s+(?:dias|días)\b", re.IGNORECASE,
)
_ULTIMAS_SEMANAS = re.compile(
    rf"\b(?:las\s+)?(?:ultimas|últimas)\s+({_COUNT})\s+semanas\b", re.IGNORECASE,
)

_ORDINALS = {
    "primer": 1, "primero": 1, "primera": 1, "segundo": 2, "segunda": 2,
    "tercer": 3, "tercero": 3, "tercera": 3, "cuarto": 4, "cuarta": 4,
    "ultimo": 0, "ultima": 0,
}
_QUARTER = re.compile(
    rf"\b(?:el\s+|del\s+)?(primer[oa]?|segund[oa]|tercer[oa]?|cuart[oa]|[uú]ltim[oa]|[1-4]"
    rf"(?:\.?\s?(?:er|ro|do|to|º|°))?)\s+(trimestre|semestre)"
    rf"(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?\b",
    re.IGNORECASE,
)

_INVALID_DATE_QUESTION = (
    "La fecha indicada no existe en el calendario. ¿Qué fecha querías usar?"
)
_REVERSE_RANGE_QUESTION = (
    "El inicio del rango temporal queda después del final. "
    "¿Cuál era el intervalo correcto?"
)
_MISSING_YEAR_QUESTION = (
    "Ese rango temporal necesita un año explícito para interpretarlo sin inventar contexto. "
    "¿De qué año se trata?"
)
_UNSUPPORTED_TIME_QUESTION = (
    "Reconocí una referencia temporal, pero no puedo resolverla con seguridad. "
    "Indica un día, mes, año o rango explícito."
)


def civil_today(value: Date | None = None) -> Date:
    """Único punto de lectura del reloj civil; acepta inyección en tests/audits."""
    return value if value is not None else Date.today()


def _month_number(value: str) -> int:
    return _MONTH_ALIASES[value.lower()]


def _safe_date(year: int, month: int, day: int) -> Date | None:
    try:
        return Date(year, month, day)
    except ValueError:
        return None


def _last_day(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _count(value: str) -> int:
    return int(value) if value.isdigit() else _COUNT_WORDS[value.lower()]


def _months_back(day: Date, months: int) -> Date:
    """El mismo día `months` meses atrás, acotado al último día del mes destino."""
    year, zero_based = divmod(day.year * 12 + day.month - 1 - months, 12)
    return Date(year, zero_based + 1, min(day.day, _last_day(year, zero_based + 1)))


def _quarter_resolution(match: re.Match[str]) -> TemporalResolution:
    """Trimestre o semestre con año explícito; la ordinal fuera de rango se aclara."""
    word = match.group(1).lower().replace("ú", "u")
    ordinal = _ORDINALS.get(word) if not word[0].isdigit() else int(word[0])
    months, label = (3, "quarter") if match.group(2).lower() == "trimestre" else (6, "semester")
    slots = 12 // months
    if ordinal is None or ordinal > slots:
        return _error_resolution(
            match, "unsupported_temporal_expression", _UNSUPPORTED_TIME_QUESTION,
        )
    slot = ordinal or slots  # «último» = la última franja del año
    year = int(match.group(3))
    first = (slot - 1) * months + 1
    return _resolution(
        match,
        desde=Date(year, first, 1),
        hasta=Date(year, first + months - 1, _last_day(year, first + months - 1)),
        precision=label,
    )


def _resolution(
    match: re.Match[str],
    *,
    desde: Date | str | None,
    hasta: Date | str | None,
    precision: str,
    clock: str = "",
    anchor: Date | None = None,
) -> TemporalResolution:
    def _iso(value: Date | str | None) -> str | None:
        return value.isoformat() if isinstance(value, Date) else value

    return TemporalResolution(
        matched=True,
        fecha_desde=_iso(desde),
        fecha_hasta=_iso(hasta),
        phrase=match.group(0),
        span=match.span(),
        precision=precision,
        clock=clock,
        anchor=_iso(anchor),
    )


def _error_resolution(
    match: re.Match[str] | None,
    code: str,
    question: str,
) -> TemporalResolution:
    return TemporalResolution(
        matched=match is not None,
        phrase=match.group(0) if match else "",
        span=match.span() if match else None,
        precision="invalid",
        error_code=code,
        clarification=question,
    )


def _relative_resolution(kind: str, match: re.Match[str], clock: Date) -> TemporalResolution:
    """Período que depende del reloj inyectado; la cuenta fuera de rango se aclara."""
    def month(year: int, number: int) -> TemporalResolution:
        return _resolution(
            match, desde=Date(year, number, 1),
            hasta=Date(year, number, _last_day(year, number)), precision="month",
        )

    def year_of(year: int) -> TemporalResolution:
        return _resolution(
            match, desde=Date(year, 1, 1), hasta=Date(year, 12, 31), precision="year",
        )

    def trailing(days_back: Date, precision: str) -> TemporalResolution:
        return _resolution(match, desde=days_back, hasta=clock, precision=precision)

    def too_many(code: str) -> TemporalResolution:
        return _error_resolution(match, code, _UNSUPPORTED_TIME_QUESTION)

    count = _count(match.group(1)) if match.groups() else 0
    if kind == "this_month":
        return month(clock.year, clock.month)
    if kind == "last_month":
        last = _months_back(Date(clock.year, clock.month, 1), 1)
        return month(last.year, last.month)
    if kind == "months_ago":
        if not 1 <= count <= 120:
            return too_many("unsupported_month_count")
        back = _months_back(Date(clock.year, clock.month, 1), count)
        return month(back.year, back.month)
    if kind == "this_year":
        return year_of(clock.year)
    if kind == "last_year":
        return year_of(clock.year - 1)
    if kind == "years_ago":
        return year_of(clock.year - count) if 1 <= count <= 30 else too_many("unsupported_year_count")
    if kind == "trailing_months":
        if not 1 <= count <= 120:
            return too_many("unsupported_month_count")
        return trailing(_months_back(clock, count), "trailing_months")
    if kind == "trailing_years":
        if not 1 <= count <= 30:
            return too_many("unsupported_year_count")
        return trailing(_months_back(clock, count * 12), "trailing_years")
    if kind == "this_week":
        monday = clock - timedelta(days=clock.weekday())
        return _resolution(match, desde=monday, hasta=monday + timedelta(days=6), precision="week")
    if kind == "last_week":
        monday = clock - timedelta(days=clock.weekday() + 7)
        return _resolution(match, desde=monday, hasta=monday + timedelta(days=6), precision="week")
    if kind == "last_7_days":
        return trailing(clock - timedelta(days=7), "trailing_weeks")
    if kind == "trailing_weeks":
        if not 1 <= count <= 52:
            return too_many("unsupported_week_count")
        return trailing(clock - timedelta(days=count * 7), "trailing_weeks")
    if kind == "trailing_days":
        if not 1 <= count <= 365:
            return too_many("unsupported_day_count")
        return trailing(clock - timedelta(days=count), "trailing_days")
    value = clock if kind == "today" else clock - timedelta(days=1)
    return _resolution(match, desde=value, hasta=value, precision="day")


_RELATIVE_PATTERNS = (
    (re.compile(r"\b(?:de\s+|del\s+)?este\s+mes\b", re.IGNORECASE), "this_month"),
    (re.compile(r"\b(?:de\s+|del\s+|el\s+)?mes\s+pasado\b", re.IGNORECASE), "last_month"),
    (re.compile(r"\b(?:de\s+|del\s+)?este\s+a[nñ]o\b", re.IGNORECASE), "this_year"),
    (re.compile(r"\b(?:de\s+|del\s+|el\s+)?a[nñ]o\s+pasado\b", re.IGNORECASE), "last_year"),
    (_HACE_MESES, "months_ago"),
    (_HACE_ANIOS, "years_ago"),
    (_ULTIMOS_MESES, "trailing_months"),
    (_ULTIMOS_ANIOS, "trailing_years"),
    (_ULTIMA_SEMANA, "last_7_days"),
    (_ESTA_SEMANA, "this_week"),
    (_SEMANA_PASADA, "last_week"),
    (_ULTIMAS_SEMANAS, "trailing_weeks"),
    (_ULTIMOS_DIAS, "trailing_days"),
    (_HOY, "today"),
    (_AYER, "yesterday"),
)
# Ventanas contadas hacia atrás desde el reloj; junto a otro periodo se aclaran
# (``temporal_window_anchor``). «hoy» es vigencia, no un periodo ancla.
_WINDOW_KINDS = frozenset(
    {"trailing_months", "trailing_years", "last_7_days", "trailing_weeks", "trailing_days"}
)
_WINDOW_PATTERNS = tuple(pattern for pattern, kind in _RELATIVE_PATTERNS if kind in _WINDOW_KINDS)
_NOT_AN_ANCHOR = (_HOY,)


def _overlaps_protected(match: re.Match[str], spans: tuple[tuple[int, int], ...]) -> bool:
    start, end = match.span()
    return any(
        start < protected_end and end > protected_start
        for protected_start, protected_end in spans
    )


def _resolve_base(
    text: str,
    *,
    today: Date | None = None,
    protected_spans: tuple[tuple[int, int], ...] = (),
    implicit_year: int | None = None,
) -> TemporalResolution:
    """Resuelve un scope por precedencia conservadora y sin LLM.

    Orden: rangos de día/mes/año → día exacto → relativos → abiertos →
    mes → año. Mes o día sin año usa el año civil actual; rango sin año
    aclara para no inventar contexto. ``implicit_year`` (sólo la autoridad nueva)
    reemplaza al año civil en lo que el texto no dice y permite rangos sin año;
    nunca cambia lo que el texto sí dice.
    """
    clock = civil_today(today)
    assumed = implicit_year if implicit_year is not None else clock.year

    iso_matches = list(re.finditer(r"\b\d{4}-\d{2}-\d{2}\b", text))
    if iso_matches:
        parsed = []
        for item in iso_matches[:2]:
            try:
                parsed.append(Date.fromisoformat(item.group(0)))
            except ValueError:
                return _error_resolution(item, "invalid_date", _INVALID_DATE_QUESTION)
        if len(parsed) == 2 and parsed[0] > parsed[1]:
            encompassing = re.search(
                rf"{re.escape(iso_matches[0].group(0))}.*?"
                rf"{re.escape(iso_matches[1].group(0))}",
                text,
            )
            return _error_resolution(
                encompassing or iso_matches[0], "reverse_range", _REVERSE_RANGE_QUESTION,
            )
        first = iso_matches[0]
        if len(parsed) == 1:
            return _resolution(first, desde=parsed[0], hasta=None, precision="open_from")
        encompassing = re.search(
            rf"{re.escape(iso_matches[0].group(0))}.*?"
            rf"{re.escape(iso_matches[1].group(0))}",
            text,
        ) or first
        return _resolution(
            encompassing, desde=parsed[0], hasta=parsed[1], precision="day_range",
        )

    day_range_re = re.compile(
        rf"\b(?:entre\s+(?:el\s+)?|del?\s+)?"
        rf"(\d{{1,2}})\s+(?:de\s+)?({_MONTH_TOKEN})"
        rf"(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?\s+"
        rf"(?:y|al|a)\s+(?:el\s+)?(\d{{1,2}})\s+(?:de\s+)?({_MONTH_TOKEN})"
        rf"(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?\b",
        re.IGNORECASE,
    )
    if match := day_range_re.search(text):
        year1_raw, year2_raw = match.group(3), match.group(6)
        if not year1_raw and not year2_raw and implicit_year is None:
            return _error_resolution(match, "missing_range_year", _MISSING_YEAR_QUESTION)
        year1, year2 = int(year1_raw or year2_raw or assumed), int(year2_raw or year1_raw or assumed)
        start = _safe_date(year1, _month_number(match.group(2)), int(match.group(1)))
        end = _safe_date(year2, _month_number(match.group(5)), int(match.group(4)))
        if start is None or end is None:
            return _error_resolution(match, "invalid_date", _INVALID_DATE_QUESTION)
        if start > end:
            return _error_resolution(match, "reverse_range", _REVERSE_RANGE_QUESTION)
        implicit = not year1_raw and not year2_raw
        return _resolution(
            match, desde=start, hasta=end, precision="day_range",
            clock="inferred_year" if implicit else "", anchor=end if implicit else None,
        )

    single_month_days_re = re.compile(
        rf"\b(?:entre\s+(?:el\s+)?|del?\s+)?(\d{{1,2}})\s+(?:y|al|a)\s+(?:el\s+)?"
        rf"(\d{{1,2}})\s+(?:de\s+)?({_MONTH_TOKEN})(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?\b",
        re.IGNORECASE,
    )
    if match := single_month_days_re.search(text):
        month = _month_number(match.group(3))
        year = int(match.group(4) or assumed)
        start = _safe_date(year, month, int(match.group(1)))
        end = _safe_date(year, month, int(match.group(2)))
        if start is None or end is None:
            return _error_resolution(match, "invalid_date", _INVALID_DATE_QUESTION)
        if start > end:
            return _error_resolution(match, "reverse_range", _REVERSE_RANGE_QUESTION)
        return _resolution(
            match, desde=start, hasta=end, precision="day_range",
            clock="" if match.group(4) else "inferred_year",
            anchor=None if match.group(4) else end,
        )

    month_range_re = re.compile(
        rf"\b(?:entre\s+|de\s+)?({_MONTH_TOKEN})"
        rf"(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?(?:\s+(?:y|a|al)\s+|\s*-\s*)"
        rf"({_MONTH_TOKEN})(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?\b",
        re.IGNORECASE,
    )
    if match := month_range_re.search(text):
        year1_raw, year2_raw = match.group(2), match.group(4)
        if not year1_raw and not year2_raw and implicit_year is None:
            return _error_resolution(match, "missing_range_year", _MISSING_YEAR_QUESTION)
        year1, year2 = int(year1_raw or year2_raw or assumed), int(year2_raw or year1_raw or assumed)
        month1, month2 = _month_number(match.group(1)), _month_number(match.group(3))
        start = Date(year1, month1, 1)
        end = Date(year2, month2, _last_day(year2, month2))
        if start > end:
            return _error_resolution(match, "reverse_range", _REVERSE_RANGE_QUESTION)
        implicit = not year1_raw and not year2_raw
        return _resolution(
            match, desde=start, hasta=end, precision="month_range",
            clock="inferred_year" if implicit else "",
            anchor=Date(year2, month2, 1) if implicit else None,
        )

    year_range_re = re.compile(
        rf"\b(?:entre\s+(?:el\s+)?|de\s+|desde\s+)({_YEAR_TOKEN})\s+"
        rf"(?:y|a|hasta)\s+(?:el\s+)?({_YEAR_TOKEN})\b"
        rf"|(?<!\d)({_YEAR_TOKEN})\s*(?:-|a|y)\s*({_YEAR_TOKEN})(?!\d)",
        re.IGNORECASE,
    )
    if match := year_range_re.search(text):
        years = [int(value) for value in match.groups() if value]
        if years[0] > years[1]:
            return _error_resolution(match, "reverse_range", _REVERSE_RANGE_QUESTION)
        return _resolution(
            match,
            desde=Date(years[0], 1, 1),
            hasta=Date(years[1], 12, 31),
            precision="year_range",
        )

    exact_day_re = re.compile(
        rf"\b(?:(?:el\s+)?d[ií]a\s+|del?\s+|el\s+)?(\d{{1,2}})\s+(?:de\s+)?({_MONTH_TOKEN})"
        rf"(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?\b",
        re.IGNORECASE,
    )
    if match := exact_day_re.search(text):
        left_context = text[max(0, match.start() - 48):match.start()]
        if re.search(
            r"\b(?:referencia|radicado|numero|nro|no\.?|id)\s*$",
            left_context,
            re.IGNORECASE,
        ):
            return TemporalResolution()
        year = int(match.group(3) or assumed)
        value = _safe_date(year, _month_number(match.group(2)), int(match.group(1)))
        if value is None:
            return _error_resolution(match, "invalid_date", _INVALID_DATE_QUESTION)
        return _resolution(
            match, desde=value, hasta=value, precision="day",
            clock="" if match.group(3) else "inferred_year",
            anchor=None if match.group(3) else value,
        )

    if (match := _QUARTER.search(text)) and match.group(3):
        return _quarter_resolution(match)

    for pattern, kind in _RELATIVE_PATTERNS:
        if match := pattern.search(text):
            return replace(_relative_resolution(kind, match, clock), clock="relative")

    open_month_re = re.compile(
        rf"\b(desde|hasta)\s+(?:el\s+mes\s+de\s+)?({_MONTH_TOKEN})"
        rf"(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?\b",
        re.IGNORECASE,
    )
    if match := open_month_re.search(text):
        year, month = int(match.group(3) or assumed), _month_number(match.group(2))
        inferred = "" if match.group(3) else "inferred_year"
        anchor = None if match.group(3) else Date(year, month, 1)
        if match.group(1).lower() == "desde":
            return _resolution(
                match, desde=Date(year, month, 1), hasta=None, precision="open_from",
                clock=inferred, anchor=anchor,
            )
        return _resolution(
            match,
            desde=None,
            hasta=Date(year, month, _last_day(year, month)),
            precision="open_until",
            clock=inferred,
            anchor=anchor,
        )

    month_re = re.compile(
        rf"\b(?:(?:del\s+|el\s+)?mes\s+de\s+|(?:en|de|durante)\s+)?({_MONTH_TOKEN})"
        rf"(?:{_YEAR_JOIN}({_YEAR_TOKEN}))?\b",
        re.IGNORECASE,
    )
    if match := month_re.search(text):
        year, month = int(match.group(2) or assumed), _month_number(match.group(1))
        return _resolution(
            match,
            desde=Date(year, month, 1),
            hasta=Date(year, month, _last_day(year, month)),
            precision="month",
            clock="" if match.group(2) else "inferred_year",
            anchor=None if match.group(2) else Date(year, month, 1),
        )

    open_year_re = re.compile(
        rf"\b(desde|hasta)\s+({_YEAR_TOKEN})\b{_MONEY_AFTER_YEAR}", re.IGNORECASE,
    )
    if match := open_year_re.search(text):
        if not _overlaps_protected(match, protected_spans):
            year = int(match.group(2))
            if match.group(1).lower() == "desde":
                return _resolution(
                    match, desde=Date(year, 1, 1), hasta=None, precision="open_from",
                )
            return _resolution(
                match, desde=None, hasta=Date(year, 12, 31), precision="open_until",
            )

    closed_year_re = re.compile(
        rf"\b(?:(?:en|de|del|durante)\s+)?({_YEAR_TOKEN})\b{_MONEY_AFTER_YEAR}",
        re.IGNORECASE,
    )
    for match in closed_year_re.finditer(text):
        if _overlaps_protected(match, protected_spans):
            continue
        year = int(match.group(1))
        return _resolution(
            match, desde=Date(year, 1, 1), hasta=Date(year, 12, 31), precision="year",
        )

    unresolved = re.search(
        rf"\b(?:trimestres?|semestres?|(?:ultimo|último)\s+a[nñ]o|"
        rf"entre\b[^,.]*?(?:{_MONTH_TOKEN}|{_YEAR_TOKEN}{_MONEY_AFTER_YEAR})|"
        rf"(?:mes|a[nñ]o|semanas?|d[ií]as?)\s+(?:pasado|pasada|anterior))\b",
        text,
        re.IGNORECASE,
    )
    if unresolved:
        return _error_resolution(
            unresolved, "unsupported_temporal_expression", _UNSUPPORTED_TIME_QUESTION,
        )
    return TemporalResolution()


_BOUNDABLE = frozenset({"day", "month", "year", "week", "quarter", "semester"})
_ISO_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")
_UNTIL_TODAY = re.compile(r"(?<=\S)\s+hasta\s+hoy\b", re.IGNORECASE)


def _anchored_window(
    text: str,
    today: Date | None,
    protected_spans: tuple[tuple[int, int], ...],
    implicit_year: int | None,
) -> TemporalResolution | None:
    """Error resolution when a relative window sits next to another period, else ``None``."""
    def rest(masked: str) -> tuple[bool, tuple[int, int] | None]:
        other = _resolve_base(
            masked, today=today, protected_spans=protected_spans, implicit_year=implicit_year,
        )
        return other.matched, other.span

    found = temporal_window_anchor.find_anchored_window(
        text, _WINDOW_PATTERNS, _NOT_AN_ANCHOR, rest,
    )
    if found is None:
        return None
    return TemporalResolution(
        matched=True, phrase=text[found.span[0]:found.span[1]], span=found.span,
        precision="invalid",
        error_code=temporal_window_anchor.ERROR_CODE,
        clarification=temporal_window_anchor.clarification(found),
    )


def resolve_temporal_scope(
    text: str,
    *,
    today: Date | None = None,
    protected_spans: tuple[tuple[int, int], ...] = (),
    implicit_year: int | None = None,
) -> TemporalResolution:
    """Resuelve el scope y aplica el comparador que gobierna el período, si lo hay.

    «después de 2022» es una cota abierta, no el año 2022: sin esta capa el
    período citado se ejecutaba como si no hubiera comparador.
    """
    # «desde 2024 hasta hoy»: «hoy» es el fin de la cota, no un día que gane al año.
    masked = _UNTIL_TODAY.sub(lambda hit: " " * len(hit.group(0)), text)
    found = _resolve_base(
        masked, today=today, protected_spans=protected_spans, implicit_year=implicit_year,
    )
    if not found.matched and masked != text:
        found = _resolve_base(
            text, today=today, protected_spans=protected_spans, implicit_year=implicit_year,
        )
    if not found.error_code and (anchored := _anchored_window(masked, today, protected_spans, implicit_year)):
        return anchored
    if found.error_code or not found.matched or found.span is None or not found.fecha_desde:
        return found
    lone_iso = found.precision == "open_from" and _ISO_DAY.fullmatch(found.phrase)
    if found.precision == "open_from" and not lone_iso:
        end = open_suffix_end(text, found.span[1])
        if end is None:
            return found
        return replace(found, phrase=text[found.span[0]:end], span=(found.span[0], end))
    if found.precision not in _BOUNDABLE and not lone_iso:
        return found
    bound = find_bound(text, *found.span)
    if bound is None:
        return found
    desde = Date.fromisoformat(found.fecha_desde)
    hasta = Date.fromisoformat(found.fecha_hasta) if found.fecha_hasta else desde
    new_desde, new_hasta, precision = apply_bound(bound.kind, desde, hasta)
    return replace(
        found,
        fecha_desde=new_desde.isoformat() if new_desde else None,
        fecha_hasta=new_hasta.isoformat() if new_hasta else None,
        phrase=text[bound.start:bound.end],
        span=(bound.start, bound.end),
        precision=precision,
    )


def consume_temporal_metadata(
    params: dict[str, Any],
    signals: list[Any],
) -> tuple[str | None, str | None]:
    """Retira metadata efímera y devuelve un bloqueo temporal, si existe."""
    error = params.pop("_temporal_error", None)
    question = params.pop("_temporal_clarification", None)
    params.pop("_temporal_replace_scope", None)
    if error:
        return str(error), str(question or _UNSUPPORTED_TIME_QUESTION)
    invalid = next((signal for signal in signals if signal.kind == "temporal_invalid"), None)
    if invalid is None:
        return None, None
    return (
        str(invalid.payload.get("code") or "invalid"),
        str(invalid.payload.get("question") or _UNSUPPORTED_TIME_QUESTION),
    )

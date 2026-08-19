"""Los aspectos de la huella contractual — un registro, no un monolito.

Cada pieza del perfil (compradores, años, modalidades, territorios,
nombres observados, contratos recientes) es una entrada de ``ASPECTS`` con
tres responsabilidades declaradas: cómo se construye su SoQL, cómo se lee
la respuesta, y con qué método de transporte se pide. La orquestación
—concurrencia, presupuesto, fallo parcial— vive una sola vez en
``assembler``, igual que ``analytics/registry`` la tiene una sola vez para
los analyzers.

Añadir un aspecto es un ``ProfileAspect`` más en la lista. Eso es
deliberado: la alternativa que AP0 desaconsejó era abrir seis rutas de
routing —una por pregunta— y multiplicar por seis la superficie de
colisión con el objeto contractual.

Ningún parser lanza. Una respuesta con la forma inesperada produce el
valor vacío del aspecto, no una excepción que derribe el perfil entero:
que la fuente devuelva algo raro en la distribución por año no puede
costar el conteo de contratos.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.core.actor_profile.models import ObservedName

__all__ = ["ASPECTS", "ProfileAspect", "aspect_names"]

# Cuántas filas trae cada distribución. Un perfil no es un volcado: son las
# cifras que caben en una respuesta y se pueden leer de un vistazo.
TOP_LIMIT = 5
OBSERVED_NAMES_LIMIT = 10
RECENT_CONTRACTS_LIMIT = 5


@dataclass(frozen=True, slots=True)
class ProfileAspect:
    """Una pieza del perfil: cómo pedirla y cómo leerla.

    ``core=True`` marca el hecho central (cuántos contratos y por cuánto
    valor). Que falle no derriba el perfil —el resto sigue siendo cierto—
    pero sí cambia lo que se puede afirmar, así que el ensamblador lo
    distingue en vez de tratarlo como una distribución más.
    """

    name: str
    build: Callable[[Any, str, dict], str]
    parse: Callable[[list[dict], Any], Any]
    transport: str = "aggregate"   # "aggregate" | "query"
    core: bool = False


def aspect_names() -> tuple[str, ...]:
    return tuple(aspect.name for aspect in ASPECTS)


# ---------------------------------------------------------------------------
# Lectores. Todos toleran ausencia: en esta fuente un campo nulo llega como
# clave AUSENTE, no como null (verificado en el contrato de AP2-A).
# ---------------------------------------------------------------------------


def _as_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pairs(rows: list[dict], key: str) -> tuple[tuple[str, int], ...]:
    """``[(etiqueta, conteo)]`` de un GROUP BY, saltando etiquetas vacías."""
    out: list[tuple[str, int]] = []
    for row in rows or []:
        label = row.get(key)
        if label in (None, ""):
            continue
        out.append((str(label), _as_int(row.get("cnt"))))
    return tuple(out)


def _parse_core(rows: list[dict], spec: Any) -> dict:
    if not rows:
        return {"contract_count": 0, "total_reported_value": None}
    row = rows[0]
    return {
        "contract_count": _as_int(row.get("total_count")),
        "total_reported_value": _as_float(row.get("total_value")),
    }


def _parse_date_range(rows: list[dict], spec: Any) -> dict:
    if not rows:
        return {"first_observed_date": None, "last_observed_date": None}
    row = rows[0]
    return {
        "first_observed_date": _iso_date(row.get("date_min")),
        "last_observed_date": _iso_date(row.get("date_max")),
    }


def _iso_date(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value[:10]


def _parse_years(rows: list[dict], spec: Any) -> tuple[tuple[str, int], ...]:
    """``date_trunc_y`` devuelve "2026-01-01T00:00:00.000"; el año son sus cuatro
    primeros caracteres."""
    out: list[tuple[str, int]] = []
    for row in rows or []:
        raw = row.get("yr")
        if not isinstance(raw, str) or len(raw) < 4:
            continue
        out.append((raw[:4], _as_int(row.get("cnt"))))
    return tuple(out)


def _parse_observed_names(rows: list[dict], spec: Any) -> tuple[ObservedName, ...]:
    name_field = getattr(spec, "contractor", None)
    document_field = getattr(spec, "contractor_document", None)
    type_field = getattr(spec, "contractor_document_type", None)
    if not name_field or not document_field:
        return ()

    out: list[ObservedName] = []
    for row in rows or []:
        name = row.get(name_field)
        if name in (None, ""):
            continue
        out.append(ObservedName(
            name=str(name),
            document=str(row.get(document_field) or ""),
            document_type=(str(row[type_field]) if type_field and row.get(type_field) else None),
            contract_count=_as_int(row.get("cnt")),
        ))
    return tuple(out)


def _parse_recent(rows: list[dict], spec: Any) -> tuple[dict, ...]:
    return tuple(rows or [])


# ---------------------------------------------------------------------------
# Builders. Reusan los del repo; solo territorios y nombres son nuevos.
# ---------------------------------------------------------------------------


def _build_core(builder: Any, dataset_id: str, params: dict) -> str:
    from app.core.analytics.aggregate_sum import build_aggregate_sum_query
    return build_aggregate_sum_query(builder, dataset_id, params)


def _build_recent(builder: Any, dataset_id: str, params: dict) -> str:
    # Los contratos recientes salen de la consulta tabular normal: trae los
    # mismos campos que el resto del sistema ya sabe formatear, con el
    # `urlproceso` reparado por `SecopClient._normalize_row`.
    recent_params = dict(params)
    recent_params["limit"] = RECENT_CONTRACTS_LIMIT
    return builder.build(dataset_id, recent_params)


ASPECTS: tuple[ProfileAspect, ...] = (
    ProfileAspect(
        name="core",
        build=_build_core,
        parse=_parse_core,
        core=True,
    ),
    ProfileAspect(
        name="date_range",
        build=lambda b, d, p: b.build_date_range(d, p),
        parse=_parse_date_range,
    ),
    ProfileAspect(
        name="buyers",
        build=lambda b, d, p: b.build_top_entities(d, p, limit=TOP_LIMIT),
        parse=lambda rows, spec: _pairs(rows, spec.entity),
    ),
    ProfileAspect(
        name="years",
        build=lambda b, d, p: b.build_temporal_dist(d, p),
        parse=_parse_years,
    ),
    ProfileAspect(
        name="modalities",
        build=lambda b, d, p: b.build_top_modalities(d, p, limit=TOP_LIMIT),
        parse=lambda rows, spec: _pairs(rows, spec.modality),
    ),
    ProfileAspect(
        name="territories",
        build=lambda b, d, p: b.build_top_territories(d, p, limit=TOP_LIMIT),
        parse=lambda rows, spec: _pairs(rows, spec.department),
    ),
    ProfileAspect(
        name="observed_names",
        build=lambda b, d, p: b.build_observed_names(d, p, limit=OBSERVED_NAMES_LIMIT),
        parse=_parse_observed_names,
    ),
    ProfileAspect(
        name="recent_contracts",
        build=_build_recent,
        parse=_parse_recent,
        transport="query",
    ),
)

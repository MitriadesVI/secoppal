"""detail.py — la oportunidad que un movimiento del feed representa (R1).

Radar vigila y produce HECHOS FECHADOS (``radar_events``). Este módulo los lee y
arma lo que una persona necesita para estudiar la oportunidad sin salir de
SECOPPAL. Es una proyección de lectura: no decide qué se vigila, no consulta
SECOP y no escribe nada.

Tres reglas que definen su alcance, y que son la razón de que exista como
módulo puro en vez de vivir dentro de Streamlit:

1. **Snapshot, no estado de hoy.** Todo sale del evento, que quedó congelado en
   su ``ts``. Abrir un movimiento de la semana pasada no puede cambiar lo que
   dice porque SECOP haya cambiado después. La única lectura del reloj es la
   RELACIÓN de la fecha de cierre observada con el presente, y viaja aparte
   (:class:`ClosingFact`) para que nadie la confunda con un dato del snapshot.

2. **Ausencia se preserva.** Un campo que el evento no trae sale ``None`` y la
   vista lo omite. Nunca se sustituye una fecha por otra, ni se rellena un
   estado, ni se fabrica una URL a partir de un identificador (esa política sí
   existe en ``formatter._safe_url``, para las tarjetas de búsqueda; aquí sería
   afirmar que un enlace fue observado cuando no lo fue).

3. **Ningún árbitro nuevo.** El estado que se muestra es el de la fuente, tal
   cual. La interpretación operativa ("recibe ofertas", "ya no") tiene dueño en
   este repo —``process_operational_state`` sobre el Truth Layer— y exige
   ``estado_del_procedimiento`` y ``fase``, que el snapshot del evento no
   conserva. Mientras no los conserve, este módulo NO la deriva: inventar aquí
   una segunda respuesta a "¿está abierta?" es peor que no responderla.

Las autoridades que sí se reutilizan, y por qué esas:

* Zona horaria → ``radar.presentation``, el módulo de presentación del Radar,
  que declara ``COLOMBIA_TZ`` una sola vez. Se usa ``parse_secop_timestamp`` y
  NO ``format_colombia_timestamp``: un ``fecha_de_recepcion_de`` viene SIN
  offset y ya es hora local colombiana, mientras que el segundo lee un naive
  como UTC —correcto para lo que Radar persiste (``ts``, ``last_run_at``) y
  equivocado por cinco horas para un dato de SECOP—. Son dos preguntas
  distintas sobre la misma zona, no dos implementaciones de zona.
  (El Truth Layer declara la misma decisión en ``temporal_value``, pero sus
  módulos temporales siguen sin consumidores en runtime por diseño; el guard
  ``test_temporal_modules_have_no_runtime_consumers`` es quien lo sostiene.)
* Nombres de columna por dataset → ``SoQLBuilder.SPECS``. Aquí solo se consulta
  si el dataset EXPONE el cierre de ofertas, para no decir "sin fecha de cierre"
  sobre un contrato, que por definición no la tiene.
* Dinero → ``app.utils.money.format_cop``, el formato exacto que el propio
  módulo declara para vistas de detalle.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal
from collections.abc import Mapping

from app.core.radar.object_excerpt import build_object_excerpt
from app.core.radar.presentation import COLOMBIA_TZ, parse_secop_timestamp
from app.core.soql_builder import SoQLBuilder
from app.utils.money import format_cop, safe_money

__all__ = [
    "ClosingFact",
    "OpportunityDetail",
    "build_opportunity_detail",
    "is_opportunity_event",
    "observed_secop_url",
    "resolve_dataset_id",
]

# Movimientos que hablan de UN proceso identificable. ``baseline`` no está y no
# puede estar: es un hecho operativo del radar ("empecé a vigilar N resultados"),
# no una oportunidad, y ofrecerle "Ver oportunidad" prometería una ficha que no
# existe.
OPPORTUNITY_EVENT_KINDS = frozenset(
    {"new", "changed_state", "changed_value", "disappeared", "followed"}
)

ClosingRelation = Literal["future", "today", "past", "unknown"]

# Nombres de dataset que usa el producto, junto a los ids de Socrata. El feed
# guarda uno u otro según la ruta que emitió el evento (el runner escribe el
# nombre; follows escribe el id), así que la lectura acepta ambos y NO cae a
# procesos ante algo desconocido: un default aquí clasificaría una fila ajena
# como proceso y le afirmaría un cierre de ofertas que nadie observó.
_DATASET_ALIASES = {
    "procesos": SoQLBuilder.PROCESOS_DATASET,
    "contratos": SoQLBuilder.CONTRATOS_DATASET,
}


def resolve_dataset_id(raw: object) -> str | None:
    """Id de Socrata del dataset del evento, o ``None`` si no se reconoce."""
    if raw in SoQLBuilder.SPECS:
        return str(raw)
    return _DATASET_ALIASES.get(str(raw or "").strip().lower())


def is_opportunity_event(kind: object) -> bool:
    """¿Este movimiento representa un proceso que se puede abrir?"""
    return str(kind or "") in OPPORTUNITY_EVENT_KINDS


def _text(value: object) -> str | None:
    """Texto no vacío, o ``None``. "" y "   " son el mismo hecho: no hay dato."""
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def observed_secop_url(value: object) -> str | None:
    """La URL que SECOP entregó, o ``None``. Nunca una construida.

    Socrata devuelve ``urlproceso`` unas veces como escalar y otras como
    ``{"url": ...}``; desenvolver esa forma es leer el mismo dato, no inventarlo.
    Se exige esquema http(s) porque el valor termina en un enlace: sin esa
    verificación, un ``"{'url': ...}"`` heredado se renderizaría como enlace
    relativo y llevaría a ninguna parte dentro de la propia app.
    """
    if isinstance(value, Mapping):
        value = value.get("url")
    url = _text(value)
    if url is None:
        return None
    return url if url.lower().startswith(("http://", "https://")) else None


@dataclass(frozen=True, slots=True)
class ClosingFact:
    """La fecha de cierre observada y su relación con el presente.

    ``supported`` distingue las dos ausencias que la vista NO puede confundir:
    un proceso sin fecha de cierre en los datos (se dice, porque el usuario
    necesita saber que no se sabe) y un contrato, que no tiene cierre de ofertas
    porque el proceso ya terminó (callar es lo correcto).

    ``relation`` compara la fecha observada con ``now``. Es aritmética sobre un
    dato, no un veredicto sobre el proceso: dice si ESA FECHA ya pasó, nunca si
    la oportunidad sigue abierta —eso depende de estado y fase, que el evento no
    conserva—. ``unknown`` cubre tanto la ausencia como el texto ilegible.
    """

    supported: bool = False
    display: str | None = None
    relation: ClosingRelation = "unknown"
    days_remaining: int | None = None

    @property
    def is_known(self) -> bool:
        return self.display is not None


def _civil_date_in_source_zone(moment: datetime) -> date:
    return moment.astimezone(COLOMBIA_TZ).date()


def _build_closing_fact(
    raw: object, *, supported: bool, now: datetime,
) -> ClosingFact:
    """Interpreta el texto crudo del cierre. No adivina formatos.

    Una fecha civil ("2026-09-02") se compara como día calendario colombiano y
    se muestra sin hora: promoverla a medianoche la daría por vencida durante
    todo el día que todavía corre. La lectura de la zona la hace
    ``parse_secop_timestamp``; aquí solo se compara y se rotula.
    """
    if not supported:
        return ClosingFact(supported=False)
    text = _text(raw)
    if text is None:
        return ClosingFact(supported=True)

    kind, parsed = parse_secop_timestamp(text)
    if kind == "unreadable":
        # No se muestra un texto que no sabemos leer, ni se afirma ausencia:
        # el campo queda como desconocido, que es lo que de verdad se sabe.
        return ClosingFact(supported=True)

    today = _civil_date_in_source_zone(now)
    if kind == "civil_date":
        civil: date = parsed  # type: ignore[assignment]
        display = civil.isoformat()
    else:
        local: datetime = parsed  # type: ignore[assignment]
        civil = local.date()
        display = local.strftime("%Y-%m-%d %H:%M")

    delta = (civil - today).days
    return ClosingFact(
        supported=True,
        display=display,
        relation=_relation_from_days(delta),
        days_remaining=delta,
    )


def _relation_from_days(delta: int) -> ClosingRelation:
    if delta > 0:
        return "future"
    return "today" if delta == 0 else "past"


def _location(city: object, department: object) -> str | None:
    """Ciudad y departamento, sin repetir el nombre cuando coinciden."""
    ciudad = _text(city)
    depto = _text(department)
    if ciudad and depto and ciudad.casefold() != depto.casefold():
        return f"{ciudad}, {depto}"
    return ciudad or depto


def _money(value: object) -> str | None:
    """Valor exacto, u omisión. ``format_cop`` rotula 0 y negativos como "Sin
    valor"; en un detalle esa línea es ruido, así que el campo no se emite."""
    if _text(value) is None or safe_money(value) <= 0:
        return None
    return format_cop(value)


@dataclass(frozen=True, slots=True)
class OpportunityDetail:
    """Lo que se puede afirmar de una oportunidad desde el evento que la reportó.

    Todo campo opcional en ``None`` significa "el evento no lo trae". La vista
    omite esos campos en vez de imprimir "s/d": una ficha llena de huecos
    rotulados no informa más, solo estorba.
    """

    event_id: str
    kind: str
    radar_name: str | None
    result_key: str | None
    dataset_id: str | None
    observed_at: str | None

    title: str | None = None
    object_excerpt: str | None = None
    entity: str | None = None
    location: str | None = None
    value_display: str | None = None
    modality: str | None = None
    source_status: str | None = None
    contractor: str | None = None
    signature_date: str | None = None
    reference: str | None = None
    secop_url: str | None = None
    closing: ClosingFact = ClosingFact()

    previous_status: str | None = None
    previous_value_display: str | None = None

    @property
    def has_secop_link(self) -> bool:
        return self.secop_url is not None


def build_opportunity_detail(
    event: Mapping[str, Any], *, now: datetime,
) -> OpportunityDetail | None:
    """Proyecta un evento del feed a su ficha, o ``None`` si no es una.

    ``None`` para un ``baseline`` (y para cualquier movimiento sin identidad de
    resultado) no es un error: es la respuesta a "¿esto se puede abrir?".

    ``now`` se inyecta —jamás se lee el reloj aquí— para que la función sea
    determinista y su único uso temporal, la relación de la fecha de cierre,
    quede a la vista del caller.
    """
    if not isinstance(event, Mapping):
        return None
    kind = str(event.get("kind") or "")
    if not is_opportunity_event(kind):
        return None
    event_id = _text(event.get("id"))
    result_key = _text(event.get("result_key"))
    if event_id is None or result_key is None:
        # Sin identidad estable no hay ficha que abrir ni a qué volver. Se
        # prefiere no ofrecerla antes que abrir una vista que no identifica
        # inequívocamente el proceso.
        return None

    dataset_id = resolve_dataset_id(event.get("dataset"))
    spec = SoQLBuilder.SPECS.get(dataset_id) if dataset_id else None
    closing_supported = bool(spec and spec.closing_date)

    return OpportunityDetail(
        event_id=event_id,
        kind=kind,
        radar_name=_text(event.get("radar_name")),
        result_key=result_key,
        dataset_id=dataset_id,
        observed_at=_text(event.get("ts")),
        title=_text(event.get("title")),
        # Defensivo sobre payloads armados a mano: la escritura ya acota el
        # extracto (radar/keys.py), y la función es idempotente sobre su salida.
        object_excerpt=build_object_excerpt(event.get("object_excerpt")),
        entity=_text(event.get("entity")),
        location=_location(event.get("city"), event.get("department")),
        value_display=_money(event.get("value")),
        modality=_text(event.get("modality")),
        source_status=_text(event.get("status")),
        contractor=_text(event.get("contractor")),
        signature_date=_text(event.get("signature_date")),
        reference=_text(event.get("referencia")),
        secop_url=observed_secop_url(event.get("urlproceso")),
        closing=_build_closing_fact(
            event.get("closing_date"), supported=closing_supported, now=now,
        ),
        previous_status=_text(event.get("prev_status")),
        previous_value_display=_money(event.get("prev_value")),
    )

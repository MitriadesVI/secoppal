"""La petición explícita de una vista de proveedores: métrica y cantidad exacta.

SUPPLIERS V1 servía UNA vista —los 5 principales por número de contratos y por
valor— y ese 5 era una constante del SQL. V1.1 separa las dos decisiones que la
constante confundía: QUÉ se ordena (métrica) y CUÁNTOS se piden (cantidad
exacta, 1..MAX). El request es inmutable y viaja ya resuelto: la autoridad de
cardinalidad acreditó el número, `resolve_supplier_ranking_request` lo convierte
en la petición ejecutable, y compilador, ejecutor y presentación la consumen tal
cual. Un límite fuera de rango o una cantidad no exacta no se truncan ni se
reinterpretan: no hay request y el turno aclara (fail-closed).

V1 ⊂ V1.1: la consulta genérica «proveedores de X» produce `BOTH` con el límite
por defecto, que es exactamente la vista V1.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re

from app.core.capability_binding.ranking_criterion import ranking_metric_kind
from app.core.capability_binding.supplier_request_types import (
    CardinalityRelation,
    SupplierRequestEvidence,
)
from app.utils.money import normalize_text

DEFAULT_SUPPLIER_LIMIT = 5
MAX_SUPPLIER_LIMIT = 25

# Singular implícito: un interrogativo singular más un criterion de ranking
# piden al mayor, no a los mayores. Es la única construcción sin número que
# acredita `limit=1`, y exige la evidencia conjunta (mención y criterio).
_SINGULAR_INTERROGATIVE = re.compile(r"\bquien\b")
_SINGULAR_NOUNS = frozenset({"proveedor", "contratista"})


class SupplierRankingMetric(str, Enum):
    CONTRACT_COUNT = "count"
    CONTRACT_VALUE = "value"
    BOTH = "both"

    def asks(self, ranking: str) -> bool:
        """¿La métrica pedida ejecuta este ranking ('count'|'value')?"""
        return self is SupplierRankingMetric.BOTH or self.value == ranking


@dataclass(frozen=True, slots=True)
class SupplierRankingRequest:
    """Una vista de proveedores ejecutable: su métrica y su corte exacto."""

    metric: SupplierRankingMetric
    limit: int

    def __post_init__(self) -> None:
        if type(self.metric) is not SupplierRankingMetric:
            raise ValueError("INVALID_RANKING_METRIC")
        if isinstance(self.limit, bool) or type(self.limit) is not int:
            raise ValueError("INVALID_RANKING_LIMIT")
        if not 1 <= self.limit <= MAX_SUPPLIER_LIMIT:
            raise ValueError("RANKING_LIMIT_OUT_OF_RANGE")

    @property
    def fetch_limit(self) -> int:
        """Filas que se piden a la fuente: el corte pedido más su testigo de empate.

        La fila extra nunca es un resultado: sólo dice si el corte cae dentro de
        un empate (contrato §12). Se pide siempre, dentro del máximo seguro.
        """
        return self.limit + 1


DEFAULT_SUPPLIER_REQUEST = SupplierRankingRequest(
    SupplierRankingMetric.BOTH, DEFAULT_SUPPLIER_LIMIT,
)

# La voz de la capability cuando la cantidad pedida no es servible. Una sola
# copia: el carril Frame y el carril V1 la dicen igual.
UNSUPPORTED_CARDINALITY_TEXT = (
    "La vista de proveedores sirve una cantidad exacta de 1 a 25, o la vista "
    "completa de los principales. Entendí una cantidad que no puedo ejecutar y "
    "no la voy a cambiar en silencio. ¿Quieres los 5 principales por número de "
    "contratos y por valor?"
)


# Cuantificadores que aproximan o generalizan sin decir un número. La vista no
# sirve ninguna de esas formas y no las reinterpreta como un default: fail-closed
# (contrato §6). Con un número delante, la propia gramática de cardinalidad ya
# las refuta como residuo; aquí se cierran las que no llevan número.
_UNBOUNDED_QUANTIFIERS = re.compile(
    r"\b(?:varios|varias|algunos|algunas|unos|unas|todos|todas|ambos|ambas|"
    r"aproximadamente|cerca\s+de|alrededor\s+de|mas\s+o\s+menos)\b"
)
# «el 10% de proveedores»: el símbolo corta el segmento y ninguna gramática lo
# lee como cantidad; sigue siendo una proporción que la vista no puede servir.
_PERCENT = re.compile(r"\d\s*%")


def resolve_supplier_ranking_request(
    user_query: str,
    evidence: SupplierRequestEvidence,
) -> SupplierRankingRequest | None:
    """La petición ejecutable que la evidencia acredita, o ``None`` (fail-closed).

    Dos regímenes, según de dónde venga la adjudicación:

    * con menciones del host, la autoridad de cardinalidad ya respondió y su
      veredicto manda: ``None`` si no es ejecutable (no exacta, fuera de rango,
      polaridad no positiva o evidencia incompleta), ``EXACT`` para el número
      acreditado y la forma singular o el default para ``NONE``;
    * sin menciones —«con quién ha contratado…», adjudicado por credencial— no
      hay claim del host que refutar: deciden el criterio del texto y la forma
      singular, y el default V1 responde cuando ninguno habla.
    """
    from app.core.capability_binding.supplier_request_types import Authorization

    metric = ranking_metric(user_query)
    cardinality = evidence.cardinality
    if evidence.mentions:
        if evidence.authorization is not Authorization.EXECUTABLE:
            return None
        if cardinality.relation is CardinalityRelation.EXACT:
            if len(cardinality.values) != 1:
                return None
            return SupplierRankingRequest(metric, cardinality.values[0])
        if cardinality.relation is not CardinalityRelation.NONE:
            return None
    elif cardinality.relation not in (
        CardinalityRelation.NONE, CardinalityRelation.UNKNOWN,
    ):
        # Sin menciones no existen claims: NONE y UNKNOWN son las dos formas de
        # «el host no dijo nada», y cualquier otra relación no fue producida por
        # esta autoridad y no se interpreta.
        return None
    if _unbounded(user_query):
        return None
    limit = 1 if _singular_form(user_query, evidence, metric) else DEFAULT_SUPPLIER_LIMIT
    return SupplierRankingRequest(metric, limit)


def _unbounded(user_query: str) -> bool:
    """¿La construcción pide una cantidad que la vista no puede servir?"""
    normalized = normalize_text(user_query)
    return bool(
        _UNBOUNDED_QUANTIFIERS.search(normalized) or _PERCENT.search(normalized)
    )


# Transporte del request ya resuelto por el canal de params del carril V1: sólo
# primitivas JSON-safe, y el receptor lo reconstruye validado — nunca se confía
# en el dict.
LIMIT_PARAM = "_supplier_ranking_limit"
METRIC_PARAM = "_supplier_ranking_metric"


def transport_params(request: SupplierRankingRequest) -> dict:
    """Las dos primitivas con las que el request viaja entre carriles."""
    return {LIMIT_PARAM: request.limit, METRIC_PARAM: request.metric.value}


def request_from_params(params: dict) -> SupplierRankingRequest | None:
    """El request transportado, reconstruido y validado; ``None`` si no cuadra.

    Ausente significa «la vista V1 por defecto». Presente y malformado es una
    petición que no se ejecuta: fail-closed, jamás un default silencioso.
    """
    metric = params.get(METRIC_PARAM)
    limit = params.get(LIMIT_PARAM)
    if metric is None and limit is None:
        return DEFAULT_SUPPLIER_REQUEST
    try:
        return SupplierRankingRequest(SupplierRankingMetric(metric), limit)
    except (TypeError, ValueError):
        return None


def ranking_metric(user_query: str) -> SupplierRankingMetric:
    """La métrica que el texto nombra; BOTH cuando no nombra una sola."""
    kind = ranking_metric_kind(normalize_text(user_query))
    if kind == "count":
        return SupplierRankingMetric.CONTRACT_COUNT
    if kind == "value":
        return SupplierRankingMetric.CONTRACT_VALUE
    return SupplierRankingMetric.BOTH


def _singular_form(
    user_query: str,
    evidence: SupplierRequestEvidence,
    metric: SupplierRankingMetric,
) -> bool:
    """``True`` sólo cuando la construcción completa acredita el singular.

    La evidencia conjunta es: solicitud SUPPLIERS (ya dada por el caller), un
    criterion de ranking explícito y una forma singular inequívoca — el
    sustantivo en singular o un interrogativo singular. La palabra «proveedor»
    suelta no basta, y sin criterion tampoco: «proveedores de Galapa» y «con
    quién ha contratado» siguen siendo la vista default.
    """
    if metric is SupplierRankingMetric.BOTH:
        return False
    if _SINGULAR_INTERROGATIVE.search(normalize_text(user_query)):
        return True
    for mention in evidence.mentions:
        start, end = mention.span.original
        if user_query[start:end].casefold() in _SINGULAR_NOUNS:
            return True
    return False

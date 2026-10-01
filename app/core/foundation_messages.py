"""The one boundary from Foundation reason codes to what the user reads (ADR-102 §11).

Internal codes stay in the trace (`foundation_internal_reason`); the response only
carries deterministic Spanish text, with concrete choices when the proposal
names them. No LLM writes these messages, and an unknown code gets a safe
generic message instead of the code.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.core.semantic_dimensions import SemanticDimension


@dataclass(frozen=True, slots=True)
class FoundationUserMessage:
    text: str
    choices: tuple[str, ...] = ()


GENERIC = FoundationUserMessage(
    "No pude completar esta búsqueda sin cambiar su significado. ¿Puedes reformularla indicando qué "
    "buscas (procesos o contratos), el tema, la entidad o el periodo?")

_DIMENSION = {
    "STATE": "No pude determinar qué estado quieres aplicar. ¿Te refieres a procesos publicados, abiertos, "
             "cerrados u otro estado?",
    "BUYER": "No pude identificar con certeza la entidad compradora que mencionas. ¿Puedes escribir su "
             "nombre completo, por ejemplo «Alcaldía de Soledad»?",
    "GEOGRAPHY": "No pude identificar el departamento que mencionas. ¿Puedes escribir su nombre completo?",
    "TIME": "No pude determinar el periodo que quieres consultar: por ahora solo filtro por un año completo, "
            "sin meses ni rangos. ¿Puedes indicar un solo año, por ejemplo 2025?",
    "VALUE": "No pude leer el monto que indicas. ¿Puedes escribirlo con cifras, por ejemplo «más de 300 "
             "millones»?",
    "MODALITY": "No pude identificar una sola modalidad de contratación. ¿Cuál quieres, por ejemplo "
                "licitación pública, mínima cuantía o contratación directa?",
    "CONTRACT_TYPE_OR_NATURE": "No pude identificar un solo tipo de contrato. ¿Cuál quieres, por ejemplo "
                               "obra, prestación de servicios o suministro?",
}

_EXACT = {
    "AMBIGUOUS_HOST_DIMENSION:BUYER": FoundationUserMessage(
        "El nombre de la entidad que mencionas corresponde a más de una entidad compradora y no puedo "
        "elegir una sin cambiar el significado. ¿Puedes escribir su nombre completo, por ejemplo "
        "«Alcaldía de Soledad»?"),
    "HOST_UNIVERSE_RULE_CONFLICT": FoundationUserMessage(
        "Encontré dos señales incompatibles sobre si buscas oportunidades actuales o registros históricos. "
        "¿Quieres procesos abiertos para participar o resultados históricos?",
        ("Procesos abiertos para participar", "Resultados históricos")),
    "CONTRADICTORY_STATE": FoundationUserMessage(
        "Pediste estados que no pueden cumplirse a la vez (por ejemplo, abiertos y cerrados). ¿Cuál quieres?"),
    "STATE_COMPOSITION_UNSUPPORTED": FoundationUserMessage(
        "La consulta combina varios estados y no puedo conservar esa combinación sin cambiar el significado. "
        "¿Quieres procesos publicados, abiertos u otro estado?",
        ("Publicados", "Abiertos")),
    "VALUE_OPERATOR_EVIDENCE_MISMATCH": FoundationUserMessage(
        "El texto y el límite de valor apuntan en direcciones distintas. ¿Buscas valores mayores o menores "
        "al monto indicado?", ("Mayores al monto", "Menores al monto")),
    "VALUE_OPERATOR_BOUNDARY_MISMATCH": FoundationUserMessage(
        "Indicaste un límite que incluye el monto exacto (como «al menos» o «hasta») y por ahora solo puedo "
        "buscar valores estrictamente mayores o menores. ¿Te sirve «más de» o «menos de» ese monto?",
        ("Más de ese monto", "Menos de ese monto")),
    "VALUE_OPERATOR_UNGROUNDED": FoundationUserMessage(
        "No encontré en tu mensaje si el valor debe ser mayor o menor al monto. ¿Buscas valores de más de o "
        "de menos de ese monto?", ("Más de ese monto", "Menos de ese monto")),
    "VALUE_OPERATOR_AMBIGUOUS": FoundationUserMessage(
        "Indicaste más de un límite de valor en la misma condición y por ahora puedo aplicar uno. ¿Buscas "
        "valores mayores o menores a qué monto?"),
    "NONCONJUNCTIVE_EXPRESSION": FoundationUserMessage(
        "Tu consulta combina condiciones con «o» o con exclusiones que todavía no puedo ejecutar sin cambiar "
        "su significado. ¿Puedes hacer una búsqueda por cada alternativa?"),
    "MULTI_REQUEST": FoundationUserMessage(
        "Tu mensaje pide varias búsquedas a la vez y no puedo combinarlas sin cambiar el significado. "
        "¿Puedes hacerlas una por una?"),
    "UNSUPPORTED_OPERATION": FoundationUserMessage(
        "Por ahora esta búsqueda solo lista procesos y contratos; no calcula conteos ni sumas. ¿Quieres ver "
        "la lista de resultados?"),
    "UNSUPPORTED_SOURCE": FoundationUserMessage(
        "Solo puedo consultar SECOP II. ¿Quieres buscar allí?"),
    "UNSUPPORTED_UNIVERSE": FoundationUserMessage(
        "Por ahora solo puedo buscar procesos y contratos de SECOP II. ¿Cuál de los dos quieres?",
        ("Procesos", "Contratos")),
    "FOUNDATION_CONTINUATION_UNSUPPORTED": FoundationUserMessage(
        "La búsqueda anterior la resolvió el intérprete avanzado, que todavía no admite ajustes sobre su "
        "resultado. Escribe la consulta completa (qué buscas, de qué entidad y de qué periodo) y la ejecuto "
        "como una búsqueda nueva."),
}

_BY_STATUS = {
    "INVALID_PROPOSAL": FoundationUserMessage(
        "No pude interpretar tu consulta con seguridad. ¿Puedes reformularla indicando qué buscas (procesos "
        "o contratos), el tema, la entidad o el periodo?"),
    "PROVIDER_ERROR": FoundationUserMessage(
        "El intérprete avanzado no respondió en este momento. Puedes reintentar la consulta en unos segundos."),
    "NO_ACTION": FoundationUserMessage(
        "No encontré una búsqueda en tu mensaje. ¿Qué procesos o contratos quieres ver?"),
    "UNSUPPORTED": FoundationUserMessage(
        "Tu consulta incluye una condición que todavía no puedo ejecutar sin cambiar su significado. "
        "¿Puedes reformularla sin esa condición o dividirla en búsquedas más simples?"),
}

SOURCE_UNAVAILABLE = {
    "fetch_timeout": "SECOP no respondió a tiempo. No afirmo que no existan resultados; puedes reintentar.",
    "source_error": "SECOP no permitió completar la consulta. No afirmo que no existan resultados; puedes "
                    "reintentar.",
}

# Every code the runtime can surface, for the exhaustiveness tests.
KNOWN_CODES = frozenset({*_EXACT, *(f"UNRESOLVED_HOST_DIMENSION:{d}" for d in _DIMENSION),
                         "MULTI_EVIDENCE_TOPIC_UNSUPPORTED", "MULTI_EVIDENCE_BUYER_UNSUPPORTED",
                         "MULTI_EVIDENCE_GEOGRAPHY_UNSUPPORTED", "TOPIC_OVERLAPS_REQUEST_UNIVERSE",
                         "TOPIC_ONLY_REQUEST_UNIVERSE"})


def _quotes(proposal, dimension: SemanticDimension) -> tuple[str, ...]:
    if proposal is None:
        return ()
    for condition in proposal.conditions:
        if condition.dimension is dimension and len(condition.evidence) > 1:
            return tuple(dict.fromkeys(span.quote for span in condition.evidence))
    return ()


def _universe_quote(proposal) -> str:
    requests = proposal.requests if proposal is not None else ()
    return next((r.universe_evidence.quote for r in requests if r.universe_evidence is not None), "")


def user_message(terminal: str, reason: str | None, proposal=None) -> FoundationUserMessage:
    """The deterministic text for one Foundation terminal; never the code itself."""
    code = reason or ""
    head = code.split(":", 1)[0]
    if code in _EXACT or head in _EXACT:
        return _EXACT.get(code) or _EXACT[head]
    if head == "UNRESOLVED_HOST_DIMENSION":
        text = _DIMENSION.get(code.split(":", 1)[1] if ":" in code else "")
        return FoundationUserMessage(text) if text else GENERIC
    if head.startswith("MULTI_EVIDENCE_") and head.endswith("_UNSUPPORTED"):
        dimension = head[len("MULTI_EVIDENCE_"):-len("_UNSUPPORTED")]
        quotes = _quotes(proposal, SemanticDimension[dimension]) if dimension in SemanticDimension.__members__ else ()
        subject = {"TOPIC": "varios temas", "BUYER": "varias entidades", "GEOGRAPHY": "varios departamentos"}.get(
            dimension, "varios valores")
        if len(quotes) >= 2:
            listed = ", ".join(f"«{quote}»" for quote in quotes[:-1]) + f" o «{quotes[-1]}»"
            return FoundationUserMessage(
                f"La consulta contiene {subject} y no puedo conservar su relación sin cambiar el significado. "
                f"¿Quieres buscar {listed}, o hacer una búsqueda por cada uno?", (*quotes, "Una búsqueda por cada uno"))
        return FoundationUserMessage(
            f"La consulta contiene {subject} y no puedo conservar su relación sin cambiar el significado. "
            "¿Puedes indicar uno solo o hacer una búsqueda por cada uno?")
    if head in {"TOPIC_OVERLAPS_REQUEST_UNIVERSE", "TOPIC_ONLY_REQUEST_UNIVERSE"}:
        noun = _universe_quote(proposal) or "resultados"
        return FoundationUserMessage(
            f"No pude separar el tema de la búsqueda de la palabra «{noun}». ¿Sobre qué tema buscas {noun}?")
    return _BY_STATUS.get(terminal, GENERIC)

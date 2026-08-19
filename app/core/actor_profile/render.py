"""Render del perfil contractual — un texto para todos los canales.

Reusa el principio del ``formatter``: una sola construcción para WhatsApp
y Streamlit, sin ramas por canal que se desincronicen.

Las dos líneas finales no son decoración
----------------------------------------
Un perfil que dice "4.230 contratos" y nada más se lee como una
verificación. Declarar qué fuente se consultó, cuándo, y —sobre todo— qué
NO se consultó es lo que impide que el usuario entienda "empresa
verificada" donde el sistema solo dijo "esto publicó SECOP II". Por eso
el límite declarado es parte del producto y no un descargo legal al pie
(INV-SCOPE-DECLARED).

Lo que este módulo nunca hace
-----------------------------
No emite conclusiones agregadas de riesgo —"proveedor riesgoso",
"contratista frecuente del municipio"— ni ordena a los actores en ninguna
escala. La huella se describe; interpretarla es del lector
(INV-NO-VERDICT).

No llama "valor real" ni "valor ejecutado" a la suma. El universo incluye
contratos en borrador y cancelados, y ``valor_del_contrato`` es lo que la
entidad reportó, no lo que se pagó. Se nombra por lo que es: valor
contractual reportado (INV-SOURCE-VALUE).

No reproduce la cédula de una persona natural. El documento completo se
usa para el join, en memoria; en pantalla va enmascarado. Un NIT sí se
muestra entero: es un identificador mercantil público, no un dato personal
(INV-PII).
"""
from __future__ import annotations

from app.core.actor_profile.models import ActorProfileResult
from app.core.analytics.base import format_currency

__all__ = ["mask_document", "render_actor_profile"]

# Tipos documentales de PERSONA NATURAL, en las escrituras que la fuente
# usa y en la de AP1. Su número se enmascara en pantalla.
_PERSONAL_DOCUMENT_TYPES = frozenset({
    "CC", "CE", "TI",
    "cédula de ciudadanía", "cedula de ciudadania",
    "cédula de extranjería", "cedula de extranjeria",
    "tarjeta de identidad", "registro civil", "pasaporte",
    "permiso por protección temporal", "permiso especial de permanencia",
})

_LIMITS_NOTICE = (
    "Esta es la actividad contractual publicada en SECOP II. No incluye "
    "registros mercantiles (RUES/RUP), antecedentes disciplinarios, fiscales "
    "ni penales, ni la composición de uniones temporales."
)


def _is_personal(document_type: str | None) -> bool:
    if not document_type:
        return False
    value = document_type.strip()
    return value in _PERSONAL_DOCUMENT_TYPES or value.lower() in _PERSONAL_DOCUMENT_TYPES


def mask_document(document: str, document_type: str | None) -> str:
    """Documento listo para pantalla: entero si es NIT, enmascarado si es personal.

    Quien preguntó ya conocía el número —lo escribió—, así que repetirlo
    completo no le aporta nada y sí deja la cédula de un ciudadano escrita
    en un historial de conversación y en cualquier captura de pantalla.
    """
    if not document:
        return ""
    if not _is_personal(document_type):
        return document
    tail = document[-4:] if len(document) > 4 else document
    return f"{'*' * max(len(document) - len(tail), 0)}{tail}"


def _identity_line(profile: ActorProfileResult) -> str:
    identity = profile.identity or {}
    document = str(identity.get("base_number") or "")
    declared_type = identity.get("document_type")
    observed_type = profile.document_types_observed[0] if profile.document_types_observed else None
    label = _type_label(declared_type, observed_type)
    shown = mask_document(document, observed_type or declared_type)

    primary_name = profile.observed_names[0].name if profile.observed_names else None
    head = f"{label} {shown}".strip()
    return f"{primary_name} · {head}" if primary_name else head


def _type_label(declared_type: str | None, observed_type: str | None) -> str:
    if declared_type == "NIT" or observed_type == "NIT":
        return "NIT"
    if _is_personal(observed_type) or _is_personal(declared_type):
        return str(observed_type or "Documento de identidad")
    return "Documento"


def _distribution(title: str, pairs: tuple[tuple[str, int], ...]) -> str | None:
    if not pairs:
        return None
    lines = [f"*{title}*"]
    lines.extend(f"- {label}: {count:,}".replace(",", ".") for label, count in pairs)
    return "\n".join(lines)


def _recent_block(profile: ActorProfileResult) -> str | None:
    if not profile.recent_contracts:
        return None
    lines = ["*Contratos recientes*"]
    for row in profile.recent_contracts:
        entity = row.get("nombre_entidad") or "Entidad no reportada"
        objeto = str(row.get("objeto_del_contrato") or "").strip() or "Objeto no reportado"
        if len(objeto) > 90:
            objeto = f"{objeto[:87]}..."
        line = f"- {entity}: {objeto}"
        url = row.get("urlproceso")
        if isinstance(url, dict):
            url = url.get("url")
        if url:
            line += f"\n  {url}"
        lines.append(line)
    return "\n".join(lines)


def _coverage_notes(profile: ActorProfileResult) -> list[str]:
    """Lo que el perfil no pudo decir, dicho.

    Un aspecto caído se nombra por lo que es —no se sabe— y nunca se deja
    pasar como ausencia de actividad: la diferencia entre "no tiene
    contratos en Antioquia" y "no pude consultar los territorios" es la
    diferencia entre informar y afirmar algo falso.
    """
    notes: list[str] = []
    if profile.failed_aspects:
        readable = {
            "core": "el total de contratos y su valor",
            "date_range": "el rango de fechas",
            "buyers": "los compradores",
            "years": "la distribución por año",
            "modalities": "las modalidades",
            "territories": "los territorios",
            "observed_names": "los nombres registrados",
            "recent_contracts": "los contratos recientes",
        }
        missing = ", ".join(
            readable.get(name, name) for name in profile.failed_aspects
        )
        notes.append(
            f"⚠️ No pude consultar {missing}: la fuente no respondió. "
            "Eso no significa que no exista — significa que no lo sé."
        )
    if profile.has_mixed_document_types:
        types = ", ".join(profile.document_types_observed)
        notes.append(
            f"⚠️ Ese número aparece en SECOP bajo más de un tipo de documento "
            f"({types}). Pueden ser actores distintos que comparten número, así "
            "que no sumo sus contratos como si fueran el mismo."
        )
    if profile.first_observed_date or profile.last_observed_date:
        notes.append(
            "El rango de actividad se calcula sobre los contratos que publican "
            "fecha de firma; cerca del 7% no la reporta."
        )
    return notes


def render_actor_profile(profile: ActorProfileResult) -> str:
    """Texto del perfil para cualquier canal."""
    blocks: list[str] = [_identity_line(profile)]

    stamp = profile.queried_at.strftime("%d/%m/%Y %H:%M")
    blocks.append(
        f"*Huella contractual observada* — SECOP II contratos, "
        f"consultado el {stamp} (hora de Colombia)"
    )

    headline: list[str] = []
    if profile.contract_count is not None:
        headline.append(f"- {profile.contract_count:,} contratos".replace(",", "."))
    if profile.total_reported_value:
        headline.append(
            f"- ${format_currency(profile.total_reported_value)} "
            "en valor contractual reportado"
        )
    if profile.first_observed_date and profile.last_observed_date:
        headline.append(
            f"- actividad observada entre {profile.first_observed_date[:4]} "
            f"y {profile.last_observed_date[:4]}"
        )
    if not profile.has_core_metrics:
        # El hecho central falta. Se dice antes que nada: sin esta línea, las
        # distribuciones de abajo se leerían como el universo completo del
        # actor cuando son lo poco que sí se pudo obtener.
        headline.insert(
            0,
            "No pude obtener el total de contratos ni su valor: la fuente no "
            "respondió a esa consulta.",
        )
    if headline:
        blocks.append("\n".join(headline))

    for title, pairs in (
        ("Principales compradores", profile.buyers),
        ("Actividad por año", profile.years),
        ("Modalidades", profile.modalities),
        ("Territorios", profile.territories),
    ):
        section = _distribution(title, pairs)
        if section:
            blocks.append(section)

    if len(profile.observed_names) > 1:
        names = "\n".join(
            f"- {observed.name} ({observed.contract_count:,})".replace(",", ".")
            for observed in profile.observed_names
        )
        blocks.append(
            "*Nombres con que aparece en SECOP*\n"
            f"{names}\n"
            "_Son los nombres que las entidades escribieron en cada contrato, "
            "no una razón social registrada._"
        )

    recent = _recent_block(profile)
    if recent:
        blocks.append(recent)

    blocks.extend(_coverage_notes(profile))
    blocks.append(_LIMITS_NOTICE)
    return "\n\n".join(blocks)

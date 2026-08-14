"""Autoridad única de identidad de un resultado SECOP (RG-1).

Antes de este módulo, la misma regla de identidad estaba escrita tres veces:

- ``app/core/radar/keys.py::radar_result_key``      → ``str | None``
- ``app/core/phrase_ranking.py::_dedupe_results``   → clave interna, truthiness
- ``app/core/pagination_buffer.py::row_identity``   → ``str`` (``""`` si falta)

Las tres coincidían por convención, no por construcción: cada una decidía por
su cuenta qué campo mirar, en qué orden y qué contaba como "ausente". Este
módulo es la única definición; las tres delegan y conservan su contrato
externo intacto.

Qué NO es identidad, por decisión explícita: objeto, entidad, valor, fecha,
similitud textual, ``source_row_id`` y cualquier hash de la fila completa.
``source_row_id`` queda excluido porque se recrea entre lotes de la fuente
(ADR-050/W2.0E-R1): el spine entre corridas es ``id_del_proceso``, no la
identidad física de la fila.

Dos fronteras cerradas en RG-1.1, antes de que esta autoridad se use para unir
candidatos Classic y Market Pack: solo un ``str`` no vacío puede ser identidad
estable, y un dataset desconocido no se resuelve con los campos de otro. En
ambos casos la respuesta segura es "no sé", porque una identidad fabricada
fusiona resultados distintos y ese daño no es observable aguas abajo.

Módulo puro: sin I/O, red, reloj, configuración ni feature flags. No importa
nada de ``app`` a propósito — es la capa más baja y no debe depender del
constructor de consultas. Los identificadores de dataset se declaran aquí y
``tests/test_result_identity.py`` comprueba que no se separen de
``SoQLBuilder``.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

__all__ = [
    "CONTRATOS_DATASET",
    "PROCESOS_DATASET",
    "ResultIdentity",
    "result_identity",
]

# Espejo de SoQLBuilder.PROCESOS_DATASET / CONTRATOS_DATASET. La igualdad se
# comprueba en tests, no por import: este módulo no depende de app/.
PROCESOS_DATASET = "p6dx-8zbt"
CONTRATOS_DATASET = "jbjy-vk9h"

# Orden de precedencia por dataset: id primario, luego referencia. Es el mismo
# orden que ya aplicaban las tres implementaciones.
_IDENTITY_FIELDS: dict[str, tuple[str, str]] = {
    PROCESOS_DATASET: ("id_del_proceso", "referencia_del_proceso"),
    CONTRATOS_DATASET: ("id_contrato", "referencia_del_contrato"),
}

REASON_PRIMARY_ID = "IDENTITY_PRIMARY_ID"
REASON_REFERENCE_FALLBACK = "IDENTITY_REFERENCE_FALLBACK"
REASON_ABSENT = "IDENTITY_ABSENT"
REASON_BLANK_DISCARDED = "IDENTITY_BLANK_DISCARDED"
REASON_INTENTIONAL_DIVERGENCE = "INTENTIONAL_DIVERGENCE_FROM_LEGACY"
REASON_INVALID_TYPE = "IDENTITY_INVALID_TYPE"
REASON_UNSUPPORTED_DATASET = "IDENTITY_UNSUPPORTED_DATASET"


@dataclass(frozen=True, slots=True)
class ResultIdentity:
    """Identidad de UNA fila, con el porqué de la decisión.

    ``stable_key`` es ``None`` exactamente cuando ``identity_status`` es
    ``"unstable"``. La pareja ``(dataset_id, stable_key)`` es el espacio de
    nombres real: la misma cadena en procesos y en contratos son dos
    identidades distintas, y por eso el dataset viaja dentro del objeto en vez
    de quedarse en el sitio de llamada.
    """

    dataset_id: str
    stable_key: str | None
    identity_field: str | None
    identity_status: Literal["stable", "unstable"]
    reason_codes: tuple[str, ...] = ()


def _candidate_text(value: object) -> tuple[str | None, tuple[str, ...]]:
    """Devuelve ``(texto_utilizable, códigos_de_descarte)``.

    El texto se conserva VERBATIM: recortar los bordes cambiaría la clave de
    una referencia con espacios y, con ella, qué filas se agrupan. El
    ``strip`` se usa solo para DETECTAR el vacío, nunca para producir la
    clave.

    Solo un ``str`` puede ser identidad. Un entero, un booleano, unos bytes o
    un objeto arbitrario no se convierten con ``str(...)``: fabricar una
    cadena a partir de un dato que la fuente no dio como texto es inventar
    una identidad, y una identidad inventada es peor que ninguna cuando se
    van a unir dos pools de candidatos.
    """
    if value is None:
        return None, ()
    if not isinstance(value, str):
        return None, (REASON_INVALID_TYPE,)
    if value == "":
        return None, ()
    if not value.strip():
        # Divergencia declarada frente a las tres implementaciones heredadas:
        # "   " producía una clave estable compuesta solo de espacios.
        return None, (REASON_BLANK_DISCARDED, REASON_INTENTIONAL_DIVERGENCE)
    return value, ()


def _dedupe_codes(codes: list[str]) -> tuple[str, ...]:
    """Conserva el orden de aparición y elimina repeticiones."""
    seen: set[str] = set()
    out: list[str] = []
    for code in codes:
        if code not in seen:
            seen.add(code)
            out.append(code)
    return tuple(out)


def result_identity(row: Mapping[str, object], dataset_id: str) -> ResultIdentity:
    """Resuelve la identidad de ``row`` dentro de ``dataset_id``. Función pura.

    Falla CERRADO: un dataset que esta autoridad no conoce no se resuelve con
    los campos de otro. Adivinar el dataset produciría una identidad plausible
    y falsa —justo el error que ``keys.py::extract_event_metadata`` ya evita—
    y aquí el precio de equivocarse es fusionar dos resultados distintos.
    """
    fields = _IDENTITY_FIELDS.get(dataset_id)
    if fields is None:
        return ResultIdentity(
            dataset_id=dataset_id,
            stable_key=None,
            identity_field=None,
            identity_status="unstable",
            reason_codes=(REASON_UNSUPPORTED_DATASET,),
        )

    reasons: list[str] = []
    for position, field in enumerate(fields):
        text, discard_codes = _candidate_text(row.get(field))
        reasons.extend(discard_codes)
        if text is None:
            continue
        reasons.append(
            REASON_PRIMARY_ID if position == 0 else REASON_REFERENCE_FALLBACK
        )
        return ResultIdentity(
            dataset_id=dataset_id,
            stable_key=text,
            identity_field=field,
            identity_status="stable",
            reason_codes=_dedupe_codes(reasons),
        )

    reasons.append(REASON_ABSENT)
    return ResultIdentity(
        dataset_id=dataset_id,
        stable_key=None,
        identity_field=None,
        identity_status="unstable",
        reason_codes=_dedupe_codes(reasons),
    )

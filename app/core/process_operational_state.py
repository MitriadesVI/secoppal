"""Resumen operativo de un proceso para la tarjeta (M2, PROCESS-OP-STATE-001).

Costura pequeña entre el Truth Layer y el producto. Traduce la salida técnica
del observer neutral a lo mínimo que la tarjeta necesita mostrar. NO reimplementa
la interpretación: la autoridad del estado es ``process_lifecycle``, resuelto por
:func:`observe_process_truth_axes`. Aquí solo se rotula y se decide qué mostrar.

Por qué existe (auditoría M2, 2026-08-17): ``estado_de_apertura_del_proceso`` es
casi una constante — 7.888.926 de 9.018.738 filas dicen "Abierto" (87,5%), y de
esas el 65,7% está en un estado que ya no admite ofertas (60,1% Seleccionado,
4,1% Evaluación, 0,8% Cancelado). La tarjeta mostraba ese campo como si fuera el
estado del proceso. Este módulo no oculta ese dato: lo degrada a corroboración y
lo conserva visible cuando contradice al estado operativo.

Límites deliberados:
  * PURO: sin I/O, sin reloj, sin LLM. El instante de referencia se inyecta.
  * El eje temporal es SOPORTE, nunca el motor: solo puede impedir que un plazo
    vencido se pinte de verde. Nunca decide el estado por sí solo.
  * NO consulta contratos. Un proceso "Publicado" que ya tiene contrato firmado
    en otro dataset no es detectable aquí (OPP-CROSS-DATASET-001).
  * NO decide ofertabilidad, rol, ni conveniencia. Eso no es M2.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.core.truth_layer.observers import observe_process_truth_axes

__all__ = [
    "ProcessOperationalSummary",
    "summarize_process_operational_state",
]

SUMMARY_VERSION = "1.0"

# Campo crudo que la tarjeta mostraba como estado. Se preserva siempre.
RAW_OPENING_FIELD = "estado_de_apertura_del_proceso"

# ── Rótulos por etapa canónica ──────────────────────────────────────────────
# Las claves son EXACTAMENTE los valores de ``ProcessLifecycleStage``. No se
# inventa taxonomía: si el Truth Layer añadiera una etapa, cae al fallback
# conservador en vez de recibir una etiqueta adivinada.
#
# El icono no replica "verde = abierto": el verde queda reservado a la única
# etapa que indica recepción de ofertas observada, y aun esa lo pierde si el
# plazo ya venció.
_STAGE_LABELS: dict[str, tuple[str, str]] = {
    "offer_phase_observed": ("Recepción de ofertas", "🟢"),
    # "Publicado" NO afirma recepción activa: SECOP lo publica sin confirmar
    # fase de oferta. Rotularlo "Recepción de ofertas" sería inventar; rotularlo
    # "Abierto" es el error que M2 corrige. Se dice lo que la fuente dice.
    "published": ("Publicado", "🔵"),
    "evaluation": ("En evaluación", "🟠"),
    "selected": ("Seleccionado", "⚫"),
    "cancelled": ("Cancelado", "🔴"),
    "suspended": ("Suspendido", "🟠"),
    "draft": ("Borrador", "⚪"),
    "approval": ("En aprobación", "⚪"),
}

# Fallback (§10): evidencia insuficiente NUNCA se convierte en una categoría.
_UNKNOWN_LABEL = "Estado por verificar"
_UNKNOWN_ICON = "⚪"

# Etapas en las que el proceso todavía se presenta como disponible. Solo estas
# pueden entrar en contradicción con una apertura "Cerrado", y solo fuera de
# ellas una apertura "Abierto" resulta engañosa.
_AVAILABILITY_STAGES = frozenset({"offer_phase_observed", "published"})

# Única etapa que afirma una ventana de ofertas abierta; la única a la que el
# eje temporal puede rebajarle el verde.
_OFFER_RECEPTION_STAGE = "offer_phase_observed"

_RAW_OPEN = "abierto"
_RAW_CLOSED = "cerrado"

_UNKNOWN_DEADLINE_LABEL = "Publicado · Vigencia por verificar"
_PAST_DEADLINE_LABEL = "Plazo de recepción reportado vencido"


def _fold(value: object) -> str:
    """Forma de comparación: minúsculas, sin tildes, espacios colapsados.

    Solo para comparar. El valor crudo se conserva intacto en ``raw_state``.
    """
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value).casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


@dataclass(frozen=True, slots=True)
class ProcessOperationalSummary:
    """Lo mínimo que la tarjeta necesita. Serializable, sin árbol de evidencia.

    ``state`` es el valor canónico del Truth Layer, no un rótulo: se conserva
    para que un consumidor posterior no tenga que reinterpretar el texto visible.
    La trazabilidad completa (reason codes, evidencia, calidad) NO viaja aquí;
    sigue siendo interna al Truth Layer.
    """

    state: str
    label: str
    icon: str
    raw_state: str
    show_raw_state: bool
    deadline_passed: bool

    @property
    def is_conclusive(self) -> bool:
        """False cuando el Truth Layer no pudo concluir una etapa."""
        return self.state != "unknown"

    def as_dict(self) -> dict[str, Any]:
        """Payload público. Cuatro hechos, ningún sidecar diagnóstico."""
        return {
            "state": self.state,
            "label": self.label,
            "icon": self.icon,
            "raw_state": self.raw_state,
            "show_raw_state": self.show_raw_state,
            "deadline_passed": self.deadline_passed,
        }


# Socrata entrega ``urlproceso`` como objeto ``{"url": ...}``, no como escalar,
# y el transporte T1A del Truth Layer exige escalares (FieldObservation rechaza
# cualquier otra cosa). Adaptar la forma es responsabilidad del consumidor, no
# del observer. El formatter ya hace lo propio en ``_safe_url``.
_URL_FIELD = "urlproceso"


def _scalar_row(row: Mapping[str, Any]) -> Mapping[str, Any]:
    """Vista escalar de la fila. No muta el original."""
    url = row.get(_URL_FIELD)
    if not isinstance(url, dict):
        return row
    view = dict(row)
    view[_URL_FIELD] = url.get("url") or ""
    return view


def _raw_opening(row: Mapping[str, Any]) -> str:
    value = row.get(RAW_OPENING_FIELD)
    if value is None:
        return ""
    return str(value).strip()


def _discrepancy(stage: str, raw_folded: str) -> bool:
    """¿La apertura cruda contradice materialmente la etapa operativa?

    Determinista y simétrica:
      * "Abierto" sobre una etapa que ya no admite ofertas  → contradice.
      * "Cerrado" sobre una recepción de ofertas observada  → contradice.
      * Etapa no concluida con apertura informada           → contradice, porque
        el dato crudo es lo único que hay y ocultarlo dejaría la tarjeta muda.

    Cuando no hay contradicción material no se emite segunda línea (§12): una
    etapa "Publicado" junto a "SECOP reporta: Abierto" es ruido redundante.
    """
    if not raw_folded:
        return False
    if raw_folded == _RAW_OPEN:
        return stage not in _AVAILABILITY_STAGES
    if raw_folded == _RAW_CLOSED:
        return stage == _OFFER_RECEPTION_STAGE
    # Valor de apertura fuera del catálogo conocido: se muestra tal cual en vez
    # de descartarlo en silencio.
    return True


def summarize_process_operational_state(
    row: Mapping[str, Any], *, reference_instant: datetime,
) -> ProcessOperationalSummary:
    """Resume una fila de proceso. Pura: no muta ``row``, no lee el reloj.

    ``reference_instant`` debe ser el mismo para todos los resultados de un
    turno; quien formatea lo obtiene una sola vez y lo inyecta.
    """
    axes = observe_process_truth_axes(
        _scalar_row(row), reference_instant=reference_instant
    )
    stage = axes["process_lifecycle"]
    raw_state = _raw_opening(row)

    # El eje temporal es soporte: solo puede desmentir una ventana de ofertas
    # que el estado declara viva, nunca crear ni cambiar una etapa. Requiere una
    # relación temporal concluyente — "unknown" jamás cuenta como vencido.
    temporal_relation = axes["temporal"]["relation"]
    deadline_passed = (
        stage == _OFFER_RECEPTION_STAGE and temporal_relation == "past"
    )

    label, icon = _STAGE_LABELS.get(stage, (_UNKNOWN_LABEL, _UNKNOWN_ICON))
    if stage == _OFFER_RECEPTION_STAGE and temporal_relation == "future":
        # La única afirmación verde exige un plazo futuro observado. La fecha se
        # incorpora al rótulo porque la tarjeta hoy solo tiene una línea de fecha
        # primaria (publicación) y no debe esconder la evidencia que la sustenta.
        raw_deadline = str(row.get("fecha_de_recepcion_de") or "").strip()
        deadline_date = raw_deadline[:10]
        if deadline_date:
            label = f"Recepción de ofertas · límite {deadline_date}"
    elif deadline_passed:
        # No se cambia la etapa canónica; solo se limita lo afirmable al usuario.
        label = _PAST_DEADLINE_LABEL
        icon = "🟠"
    elif stage == _OFFER_RECEPTION_STAGE:
        # Fase de oferta sin deadline observado no prueba recepción vigente.
        label = _UNKNOWN_DEADLINE_LABEL
        icon = "🔵"

    return ProcessOperationalSummary(
        state=stage,
        label=label,
        icon=icon,
        raw_state=raw_state,
        show_raw_state=_discrepancy(stage, _fold(raw_state)),
        deadline_passed=deadline_passed,
    )

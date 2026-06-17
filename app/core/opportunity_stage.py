"""OpportunityStage — clasifica la accionabilidad de un proceso (OPP-STAGE-001).

Función PURA y determinística sobre UNA fila de proceso. No toca workflow,
trace, SoQL, ranking, Radar ni persistencia (eso es Fase 2B en adelante).

Por qué existe (Fase 1, 2026-06-13): SECOP parte el ciclo de vida en dos ejes
desalineados — `estado_del_procedimiento` (grueso) y `fase` (etapa real). Un
mismo `Publicado` abarca varias fases. Además NO hay campo de fecha de cierre
poblado (auditadas las 11 columnas de fecha del dataset: solo publicación, ~99%
NULL en recepción/apertura). Por eso esta capa NO afirma vigencia temporal
("puedes ofertar hoy") — solo qué INDICA el snapshot.

Limitación explícita: clasifica únicamente señales del snapshot de procesos.
NO confirma ausencia de adjudicación/contrato en otros datasets (ver
OPP-CROSS-DATASET-001). Un proceso puede figurar "en oferta" y ya tener
contrato firmado; eso no es detectable desde aquí.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

CLASSIFIER_VERSION = "0.1"

OpportunityStage = Literal[
    "offer_phase_indicated",
    "published_pre_offer_signal",   # público pero temprano (observaciones/manifestación)
    "unpublished_draft_signal",     # borrador interno sin señales de publicación
    "post_offer_signal",
    "conflicting_signals",
    "unknown",
]


@dataclass(frozen=True, slots=True)
class OpportunityStageAssessment:
    """Resultado auditable: la categoría, su confianza y el porqué.

    ``confidence`` se deriva del ACUERDO de señales, no de porcentajes
    inventados: tres señales alineadas = high; parcial sin conflicto = medium;
    lo débil cae en stage="unknown" (low), nunca como (stage fuerte, low).
    ``raw_phase`` preserva el dato fuente para descubrir variantes nuevas.
    """

    stage: OpportunityStage
    confidence: Literal["high", "medium", "low"]
    reasons: tuple[str, ...] = ()
    conflicts: tuple[str, ...] = ()
    raw_phase: str | None = None


# ── Catálogo explícito de fases (normalizadas) → clase semántica ────────────
# NUNCA por substring "oferta": "presentación de oferta" y "selección de
# ofertas" comparten la palabra y significan lo opuesto. Valores en inglés/
# portugués quedan sin mapear (→ unknown) hasta muestrear su semántica real.
_PHASE_ALIASES: dict[str, str] = {
    "presentacion de oferta": "offer",
    "fase de ofertas": "offer",
    "fase de seleccion presentacion de ofertas": "offer",
    "presentacion de observaciones": "observations",
    "manifestacion de interes menor cuantia": "interest",
    "seleccion de ofertas": "selection",
    "seleccion de ofertas borrador": "selection",
}

# "Abierto" (estado) es un estado real de oferta: ~4.318 con apertura=Abierto
# son oportunidades legítimas. (Los ~27.618 con apertura=Cerrado caen en
# conflicto por la regla offer_phase_but_opening_closed.)
_OFFER_COMPATIBLE_ESTADOS = frozenset({"publicado", "abierto"})
_PRE_OFFER_ESTADOS = frozenset({"borrador", "en aprobacion", "aprobado"})
# Sub-estado del borrador, conservado como razón (no todos valen igual en Fase 3).
_DRAFT_STATE_REASON = {
    "borrador": "procedure_state_draft",
    "en aprobacion": "procedure_state_in_approval",
    "aprobado": "procedure_state_approved_unpublished",
}
_POST_OFFER_ESTADOS = frozenset({"evaluacion", "seleccionado", "cancelado", "suspendido"})

_PHASE_REASON = {
    "offer": "phase_offer_submission",
    "observations": "phase_observations",
    "interest": "phase_interest_manifestation",
    "selection": "phase_offer_selection",
}


def _norm(value: object) -> str:
    """minúsculas, sin tildes, sin puntuación/paréntesis, espacios colapsados."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value).lower())
    text = "".join(c for c in text if not unicodedata.combining(c))
    out = []
    for ch in text:
        out.append(ch if ch.isalnum() or ch.isspace() else " ")
    return " ".join("".join(out).split())


def classify_opportunity_stage(row: Mapping[str, Any]) -> OpportunityStageAssessment:
    """Clasifica una fila de proceso. Pura: no muta ``row``.

    No recibe fecha: la auditoría de Fase 1 probó que no hay campo de plazo
    poblado, así que el clasificador NO evalúa vigencia. Cuando exista una
    fuente temporal confiable se cambiará la firma con tests — no se reserva
    un parámetro hoy para no insinuar una capacidad que no existe.
    """
    if not isinstance(row, Mapping):
        return OpportunityStageAssessment("unknown", "low", reasons=("invalid_row_type",))

    estado = _norm(row.get("estado_del_procedimiento"))
    apertura = _norm(row.get("estado_de_apertura_del_proceso"))
    raw_phase = row.get("fase")
    phase = _PHASE_ALIASES.get(_norm(raw_phase))  # offer|observations|interest|selection|None
    has_pub_date = bool(str(row.get("fecha_de_publicacion_del") or "").strip())

    estado_published = estado == "publicado"
    estado_pre = estado in _PRE_OFFER_ESTADOS
    estado_post = estado in _POST_OFFER_ESTADOS
    opening_open = apertura == "abierto"
    opening_closed = apertura == "cerrado"

    # Sin ninguna señal de proceso (p. ej. una fila de contratos) → unknown,
    # nunca excepción.
    if not estado and not apertura and phase is None and not str(raw_phase or "").strip():
        return OpportunityStageAssessment(
            "unknown", "low", reasons=("no_process_signals",), raw_phase=raw_phase
        )

    phase_reason = _PHASE_REASON.get(phase)

    # 1) CONFLICTO — la contradicción tiene precedencia máxima.
    conflicts: list[str] = []
    if phase == "offer" and opening_closed:
        conflicts.append("offer_phase_but_opening_closed")
    if phase == "offer" and estado_post:
        conflicts.append("offer_phase_but_state_post_offer")
    if estado_pre and phase == "selection":
        conflicts.append("draft_state_but_selection_phase")
    if conflicts:
        reasons = [r for r in (phase_reason,) if r]
        if estado_post:
            reasons.append("procedure_state_post_offer")
        return OpportunityStageAssessment(
            "conflicting_signals", "high",
            reasons=tuple(reasons), conflicts=tuple(conflicts), raw_phase=raw_phase,
        )

    # 2) POST-OFERTA
    if estado_post or phase == "selection":
        reasons = []
        if estado_post:
            reasons.append("procedure_state_post_offer")
        if phase == "selection":
            reasons.append("phase_offer_selection")
        # Anomalía a MEDIR en 2B, no a tratar como conflicto en v0.1:
        # apertura=Abierto en un post-oferta (apertura es campo grueso/laggy).
        # Señales no plenamente alineadas → baja la confianza a medium.
        if opening_open:
            reasons.append("opening_state_open_observed")
        conf = "high" if (estado_post and not opening_open) else "medium"
        return OpportunityStageAssessment(
            "post_offer_signal", conf, reasons=tuple(reasons), raw_phase=raw_phase,
        )

    # 3) FASE DE OFERTA INDICADA (no "accionable ahora": sin fecha no hay vigencia)
    if phase == "offer":
        estado_offer_compatible = estado in _OFFER_COMPATIBLE_ESTADOS
        reasons = ["phase_offer_submission"]
        if estado_published:
            reasons.append("procedure_state_published")
        elif estado == "abierto":
            reasons.append("procedure_state_open")
        if opening_open:
            reasons.append("opening_open")
        conf = "high" if (estado_offer_compatible and opening_open) else "medium"
        return OpportunityStageAssessment(
            "offer_phase_indicated", conf, reasons=tuple(reasons), raw_phase=raw_phase,
        )

    # 4) PRE-OFERTA PÚBLICA — proceso publicado pero en etapa temprana
    # (observaciones/manifestación). Es inteligencia comercial valiosa.
    if estado in _OFFER_COMPATIBLE_ESTADOS and phase in ("observations", "interest"):
        reasons = [phase_reason]
        reasons.append("procedure_state_published" if estado_published else "procedure_state_open")
        return OpportunityStageAssessment(
            "published_pre_offer_signal", "high", reasons=tuple(reasons), raw_phase=raw_phase,
        )

    # 5) BORRADOR NO PUBLICADO — requisición/borrador interno sin señales de
    # publicación. NO es "oportunidad temprana": puede no publicarse nunca.
    # El sub-estado se conserva como razón porque NO todos valen igual:
    # Borrador (muy débil) < En aprobación < Aprobado (señal temprana más fuerte).
    if estado_pre:
        reasons = [_DRAFT_STATE_REASON.get(estado, "procedure_state_draft")]
        if not has_pub_date:
            reasons.append("no_publication_date")
        if phase in ("observations", "interest"):
            reasons.append(phase_reason)
        return OpportunityStageAssessment(
            "unpublished_draft_signal", "high" if not has_pub_date else "medium",
            reasons=tuple(reasons), raw_phase=raw_phase,
        )

    # 6) Señal pública temprana sin estado claro (fase observaciones/manifestación)
    if phase in ("observations", "interest"):
        return OpportunityStageAssessment(
            "published_pre_offer_signal", "medium",
            reasons=(phase_reason,), raw_phase=raw_phase,
        )

    # 7) DESCONOCIDO — low reservado para insuficiencia/contradicción no resuelta
    reasons = []
    if estado_published:
        reasons.append("procedure_state_published")
    if raw_phase and phase is None:
        reasons.append("phase_unrecognized")
    if not reasons:
        reasons.append("insufficient_signals")
    return OpportunityStageAssessment(
        "unknown", "low", reasons=tuple(reasons), raw_phase=raw_phase,
    )


# ── Shadow summarizer (Fase 2B): agrega el clasificador sobre un lote ────────
# PURO: recibe el conjunto de resultados RECUPERADO (no el universo) y devuelve
# un payload serializable para el trace. No muta filas, no consulta nada.

_STAGE_ORDER = (
    "offer_phase_indicated", "published_pre_offer_signal", "unpublished_draft_signal",
    "post_offer_signal", "conflicting_signals", "unknown",
)
_CONFIDENCE_ORDER = ("high", "medium", "low")


def _reference(row: Mapping[str, Any]) -> str | None:
    for key in ("referencia_del_proceso", "id_del_proceso"):
        value = row.get(key)
        if value:
            return str(value)
    return None


def summarize_opportunity_stages(
    results: Sequence[Mapping[str, Any]],
    *,
    total_count: int | None = None,
    displayed_count: int | None = None,
    classifier_version: str = CLASSIFIER_VERSION,
    sample_limit: int = 10,
) -> dict:
    """Resumen shadow del lote clasificado. Determinístico y serializable.

    ``computed_from="current_result_set"``: las cifras describen el lote
    RECUPERADO (p. ej. 50 filas), NUNCA el universo del WHERE.
    """
    rows = list(results or [])
    assessed = [(r, classify_opportunity_stage(r)) for r in rows]

    counts = {s: 0 for s in _STAGE_ORDER}
    conf_counts = {c: 0 for c in _CONFIDENCE_ORDER}
    unknown_phases: dict[str, list] = {}   # norm -> [count, raw_example]
    conflict_patterns: dict[str, int] = {}

    for _, a in assessed:
        counts[a.stage] = counts.get(a.stage, 0) + 1
        conf_counts[a.confidence] = conf_counts.get(a.confidence, 0) + 1
        if a.stage == "unknown" and a.raw_phase and str(a.raw_phase).strip():
            key = _norm(a.raw_phase)
            slot = unknown_phases.setdefault(key, [0, str(a.raw_phase)])
            slot[0] += 1
        for code in a.conflicts:
            conflict_patterns[code] = conflict_patterns.get(code, 0) + 1

    def _entry(row: Mapping[str, Any], a: OpportunityStageAssessment) -> dict:
        return {
            "reference": _reference(row),
            "stage": a.stage,
            "confidence": a.confidence,
            "reasons": list(a.reasons),
            "conflicts": list(a.conflicts),
        }

    # Sample determinista: prioriza conflictos → unknown → representativos,
    # preservando el orden original; acotado.
    conflicts_s = [_entry(r, a) for r, a in assessed if a.stage == "conflicting_signals"][:5]
    unknown_s = [_entry(r, a) for r, a in assessed if a.stage == "unknown"][:3]
    rep_s = [_entry(r, a) for r, a in assessed
             if a.stage not in ("conflicting_signals", "unknown")][:2]
    sample = (conflicts_s + unknown_s + rep_s)[:sample_limit]

    return {
        "classifier_version": classifier_version,
        "computed_from": "current_result_set",
        "classified_count": len(rows),
        "displayed_count": len(rows) if displayed_count is None else displayed_count,
        "total_count": total_count,
        "counts": {s: counts[s] for s in _STAGE_ORDER if counts[s]},
        "confidence_counts": {c: conf_counts[c] for c in _CONFIDENCE_ORDER if conf_counts[c]},
        "unknown_phases": sorted(
            ([raw, n] for n, raw in unknown_phases.values()),
            key=lambda x: (-x[1], x[0]),
        ),
        "conflict_patterns": sorted(
            ([code, n] for code, n in conflict_patterns.items()),
            key=lambda x: (-x[1], x[0]),
        ),
        "sample": sample,
    }

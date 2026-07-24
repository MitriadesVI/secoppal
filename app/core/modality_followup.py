"""Motor puro de mutaciones conversacionales de modalidad (M3C-B / R1).

Reconoce únicamente *lenguaje de control* (no modalidades jurídicas: eso es
autoridad de M2) y aplica álgebra determinista sobre ``ModalityFilter``.
Produce una instrucción tipada y una decisión con trace JSON-native. No hace
wiring con workflow, Burr, ConversationStore ni FollowupEngine; no carga YAML;
no toca HTTP ni raws SECOP. Contratos importados congelados: ``ModalityFilter``
(M3B-1) y ``ResolutionStatus`` (M2).

Frontera con M2 (R1): el clasificador NO reparsea "sin"/"excepto"; deriva la
polaridad de la *forma* del filtro que M2 ya resolvió (include-only vs
exclude-only) más el status. Así "contratación directa sin ofertas" (modalidad
positiva, include-only) nunca se confunde con "sin mínima cuantía" (exclusión,
exclude-only). El clasificador solo lee la forma del filtro, jamás los valores
de los procedure_ids ni el lexicón.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from app.core.modality_filter import ModalityFilter
from app.core.modality_resolution_models import ResolutionStatus


class ModalityFollowupOperation(str, Enum):
    REPLACE = "replace"
    ADD_INCLUDE = "add_include"
    ADD_EXCLUDE = "add_exclude"
    REMOVE_MEMBER = "remove_member"
    REMOVE_EXCLUDE = "remove_exclude"
    RESTORE_INCLUDE = "restore_include"
    CLEAR = "clear"


_OP = ModalityFollowupOperation
_STATUS_NONE, _STATUS_RESOLVED = "none", "resolved"
_STATUS_AMBIGUOUS, _STATUS_CONFLICT = "ambiguous", "conflict"
_VALID_STATUS = (_STATUS_NONE, _STATUS_RESOLVED, _STATUS_AMBIGUOUS, _STATUS_CONFLICT)

# Status M2 ejecutables (aplican operaciones dirigidas) y NO_MATCH.
_ALLOWED = {ResolutionStatus.RESOLVED.value, ResolutionStatus.RESOLVED_WITH_ASSUMPTION.value}
_NO_MATCH = ResolutionStatus.NO_MATCH.value


def _status_value(status) -> str:
    return status.value if isinstance(status, Enum) else str(status)


@dataclass(frozen=True)
class ModalityFollowupInstruction:
    """Resultado del clasificador (JSON-serializable salvo el enum, que es str)."""

    status: str
    operation: ModalityFollowupOperation | None
    control_code: str | None
    construction_id: str | None
    source_text: str
    control_spans: tuple[tuple[int, int], ...]
    target_required: bool
    explanation: str

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUS:
            raise ValueError(f"status inválido: {self.status!r}")
        if self.status == _STATUS_RESOLVED and self.operation is None:
            raise ValueError("status 'resolved' requiere operación")
        if self.status != _STATUS_RESOLVED and self.operation is not None:
            raise ValueError(f"status {self.status!r} no admite operación")
        if self.status == _STATUS_RESOLVED:
            # Falla cerrada: sólo el enum propio; ni strings ni enums homónimos.
            if not isinstance(self.operation, ModalityFollowupOperation):
                raise ValueError(f"operation debe ser ModalityFollowupOperation, no {type(self.operation).__name__}")
            expected = self.operation is not ModalityFollowupOperation.CLEAR
            if self.target_required is not expected:
                raise ValueError(f"target_required debe ser {expected} para {self.operation.value}")
        n, last = len(self.source_text), 0
        for start, end in self.control_spans:
            if not (0 <= start < end <= n):
                raise ValueError(f"span inválido ({start}, {end}) sobre {n}")
            if start < last:
                raise ValueError("control_spans deben ir ordenados y sin solaparse")
            last = end


@dataclass(frozen=True)
class ModalityMutationDecision:
    """Decisión del motor. ``final_filter`` siempre es un ModalityFilter válido."""

    should_apply: bool
    final_filter: ModalityFilter
    reason: str
    needs_clarification: bool
    clarification_code: str | None
    no_op: bool
    trace: dict

    def __post_init__(self) -> None:
        if self.needs_clarification and self.should_apply:
            raise ValueError("needs_clarification=True implica should_apply=False")
        if not isinstance(self.final_filter, ModalityFilter):
            raise ValueError("final_filter debe ser ModalityFilter")


# Normalización a minúsculas sin acentos, preservando longitud (offsets 1:1).
_ACCENTS = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n",
            "Á": "a", "É": "e", "Í": "i", "Ó": "o", "Ú": "u", "Ü": "u", "Ñ": "n"}


def _normalize(text: str) -> str:
    out = []
    for ch in text:
        mapped = _ACCENTS.get(ch)
        if mapped is None:
            low = ch.lower()
            mapped = low if len(low) == 1 else ch
        out.append(mapped)
    return "".join(out)


# Construcciones cerradas: (regex, operación, construction_id). Requieren evidencia
# de modalidad (se saltan bajo NO_MATCH).
_CLOSED = [
    (re.compile(r"\b(?:cualquier modalidad|todas las modalidades)\s+excepto\b"),
     _OP.REPLACE, "replace_all_except"),
    (re.compile(r"\bno importa la modalidad,?\s+(?:solo|busca)\b"),
     _OP.REPLACE, "clear_then_replace"),
]

# Señales atómicas: (regex, operación, code). "excepto"/"excluye" son verbos de
# control inequívocos (no aparecen en nombres jurídicos); "sin" se trata aparte.
_ATOMIC = [
    (re.compile(r"\bahora\b"), _OP.REPLACE, "replace_ahora"),
    (re.compile(r"\bcambia a\b"), _OP.REPLACE, "replace_cambia_a"),
    (re.compile(r"\busa\b"), _OP.REPLACE, "replace_usa"),
    (re.compile(r"\bsolo\b"), _OP.REPLACE, "replace_solo"),
    (re.compile(r"\bunicamente\b"), _OP.REPLACE, "replace_unicamente"),
    (re.compile(r"\bademas\b"), _OP.ADD_INCLUDE, "add_include_ademas"),
    (re.compile(r"\btambien\b"), _OP.ADD_INCLUDE, "add_include_tambien"),
    (re.compile(r"\bagrega\b"), _OP.ADD_INCLUDE, "add_include_agrega"),
    (re.compile(r"\bincluye\b"), _OP.ADD_INCLUDE, "add_include_incluye"),
    (re.compile(r"\bexcepto\b"), _OP.ADD_EXCLUDE, "add_exclude_excepto"),
    (re.compile(r"\bexcluye\b"), _OP.ADD_EXCLUDE, "add_exclude_excluye"),
    (re.compile(r"\bvuelve a incluir\b"), _OP.RESTORE_INCLUDE, "restore_include_vuelve_a_incluir"),
    (re.compile(r"\breincorpora\b"), _OP.RESTORE_INCLUDE, "restore_include_reincorpora"),
    (re.compile(r"\bdeja de excluir\b"), _OP.REMOVE_EXCLUDE, "remove_exclude_deja_de_excluir"),
    (re.compile(r"\bpermite\b"), _OP.REMOVE_EXCLUDE, "remove_exclude_permite"),
    (re.compile(r"\bquita\b"), _OP.REMOVE_MEMBER, "remove_member_quita"),
    (re.compile(r"\belimina\b"), _OP.REMOVE_MEMBER, "remove_member_elimina"),
    (re.compile(r"\bsaca\b"), _OP.REMOVE_MEMBER, "remove_member_saca"),
]

# Frases CLEAR inequívocas ("cualquier modalidad"/"todas las modalidades" solas;
# la variante "... excepto X" ya fue capturada como construcción cerrada).
_CLEAR = [re.compile(p) for p in (
    r"\bsin filtro de modalidad\b",
    r"\bquita el filtro de modalidad\b",
    r"\belimina el filtro de modalidad\b",
    r"\bcualquier modalidad\b",
    r"\btodas las modalidades\b",
    r"\bno importa la modalidad\b",
)]

_AMBIGUOUS_SIN = re.compile(r"\bsin\s+modalidad\b")
_SIN_FILTRO = re.compile(r"\bsin\s+filtro\s+de\s+modalidad\b")
_SIN_WORD = re.compile(r"\bsin\b")

# Relativizadores que abren cláusula descriptiva; fronteras fuertes que la cierran.
_RELATIVIZER = re.compile(r"\b(?:que|quien|quienes|cuyo|cuya|cuyos|cuyas|donde)\b")
_STRONG_BOUNDARY = re.compile(r"[,;:]")

# Prefijo de "inicio lógico" (§7): solo espacios y puntuación/signos de apertura.
_LEADING_NOISE = re.compile(r"[\s¿¡.,;:?!\"'()\[\]{}\-–—]*")


def _subsumed(span, others) -> bool:
    s, e = span
    return any(
        (os, oe) != span and os <= s and e <= oe and (oe - os) > (e - s)
        for os, oe in others
    )


def _dedup_spans(spans):
    """Ordena y descarta solapamientos: spans válidos, ordenados y disjuntos."""
    kept: list[tuple[int, int]] = []
    for s, e in sorted(set(spans)):
        if not kept or s >= kept[-1][1]:
            kept.append((s, e))
    return tuple(kept)


def _make_instruction(status, operation, control_code, construction_id, text,
                      spans, target_required, explanation):
    return ModalityFollowupInstruction(
        status=status, operation=operation, control_code=control_code,
        construction_id=construction_id, source_text=text,
        control_spans=_dedup_spans(spans), target_required=target_required,
        explanation=explanation,
    )


def _within_target(cs: int, ce: int, target_spans) -> bool:
    """True si [cs,ce) está dentro de / solapa algún target span de M2 (paso 1)."""
    return any(ts < ce and cs < te for ts, te in target_spans)


def _sin_control_spans(norm, exclude_only, executable, has_evidence, target_spans):
    """Spans de "sin" que SÍ son control de exclusión.

    Regla R1 anti-colisión con nombres jurídicos que contienen "sin":
      * dentro de un consumed_span de M2 → jurídico consumido: NO es control (R3′ §4);
      * ejecutable + filtro exclude-only  → M2 confirmó exclusión: "sin" es control;
      * status bloqueado con evidencia    → "sin" es control solo como prefijo del
        turno (no hay forma de filtro fiable en que apoyarse);
      * include-only o sin evidencia      → "sin" es interno/jurídico: NO es control.
    """
    reserved = {(m.start(), m.start() + 3) for m in _AMBIGUOUS_SIN.finditer(norm)}
    reserved |= {(m.start(), m.start() + 3) for m in _SIN_FILTRO.finditer(norm)}
    spans = []
    for m in _SIN_WORD.finditer(norm):
        if m.span() in reserved:
            continue
        s, e = m.span()
        if _within_target(s, e, target_spans):  # R3′: "sin" dentro del nombre → jurídico
            continue
        if executable and exclude_only:
            spans.append((s, e))
        elif has_evidence and not executable and _at_logical_start(norm, s):
            spans.append((s, e))
    return spans


def _at_logical_start(text: str, position: int) -> bool:
    """True si antes de ``position`` solo hay espacios y puntuación inicial (§7).

    No usa ``.strip()`` (que ignora solo espacios): ``¿``/``¡``/comilla/paréntesis
    inicial también cuenta como inicio lógico del turno.
    """
    return _LEADING_NOISE.fullmatch(text[:position]) is not None


def _relative_clause_active(segment: str) -> bool:
    """True si en el segmento hay un relativizador sin frontera fuerte posterior."""
    rel = [m.start() for m in _RELATIVIZER.finditer(segment)]
    if not rel:
        return False
    bnd = [m.start() for m in _STRONG_BOUNDARY.finditer(segment)]
    return not bnd or rel[-1] > bnd[-1]


def _governs_target(cs: int, ce: int, norm: str, target_spans, *, blocked_fallback: bool) -> bool:
    """¿El candidato [cs,ce) gobierna un target M2? (contrato §4). Las conjunciones
    y/e/o/u no son relativizador ni frontera fuerte, así que no alteran la decisión."""
    if _within_target(cs, ce, target_spans):                 # 1: dentro/solapa target → descriptivo
        return False
    right = [ts for ts, te in target_spans if ts >= ce]      # 2: target completamente a la derecha
    if not right:                                            # 3: sin target derecho
        # §6 fallback: control imperativo al inicio del turno bajo status bloqueado
        # con spans vacíos → preservar la operación para que el motor emita su bloqueo.
        return blocked_fallback and _at_logical_start(norm, cs)
    prev_ends = [te for ts, te in target_spans if te <= cs]  # 4: target anterior
    if not prev_ends:                                        # gobierna el target derecho
        return True
    # 5-7: cláusula relativa activa entre el target anterior y el candidato → descriptivo.
    return not _relative_clause_active(norm[max(prev_ends):cs])


def _validate_target_spans(target_spans, n: int) -> None:
    """Valida fail-closed la geometría de los spans de M2 (§8): un span corrupto
    lanza ``ValueError``; nunca degrada silenciosamente a clasificación libre."""
    if type(target_spans) is not tuple:
        raise ValueError(f"target_spans debe ser tuple, no {type(target_spans).__name__}")
    last = 0
    for span in target_spans:
        if type(span) is not tuple or len(span) != 2:
            raise ValueError(f"cada target span debe ser tuple de dos: {span!r}")
        start, end = span
        if type(start) is not int or type(end) is not int:  # rechaza bool/float/str/Decimal
            raise ValueError(f"índices de target span deben ser int exactos: {span!r}")
        if not (0 <= start < end <= n):
            raise ValueError(f"target span fuera de rango o invertido: {span!r} sobre {n}")
        if start < last:
            raise ValueError("target_spans deben ir ordenados y sin solaparse")
        last = end


def classify_modality_followup_control(
    text: str,
    *,
    current_filter: ModalityFilter,
    resolution_status,
    target_spans: tuple[tuple[int, int], ...],
    allow_implicit_replace: bool = False,
) -> ModalityFollowupInstruction:
    """Clasifica el lenguaje de control de un turno. No conoce modalidades: usa la
    *forma* de ``current_filter``, ``resolution_status`` y los ``target_spans`` de M2.
    Un verbo cuenta como control SOLO si gobierna un target M2 (contrato §4)."""
    _validate_target_spans(target_spans, len(text))
    status = _status_value(resolution_status)
    has_evidence = status != _NO_MATCH          # M2 vio dominio de modalidad
    executable = status in _ALLOWED             # solo estos aplican mutación
    exclude_only = (executable and bool(current_filter.exclude_procedure_ids)
                    and not current_filter.include_procedure_ids)
    norm = _normalize(text)
    # §6: fallback acotado a status bloqueado con spans vacíos (NO_MATCH excluido).
    blocked_fallback = (not target_spans) and (status in _STATUS_TO_CODE)

    # 1) Construcciones cerradas primero (evidencia + ancladas al inicio lógico §7).
    if has_evidence:
        for pat, op, cid in _CLOSED:
            m = pat.search(norm)
            if m and _at_logical_start(norm, m.start()):
                return _make_instruction(_STATUS_RESOLVED, op, cid, cid, text,
                                         [m.span()], True, f"closed construction {cid}")

    # 2) Recolectar señales; los atómicos cuentan solo si gobiernan un target M2.
    raw: list[tuple] = []
    for pat in _CLEAR:
        raw += [(_OP.CLEAR, m.start(), m.end(), "clear_phrase") for m in pat.finditer(norm)]
    for pat, op, code in _ATOMIC:
        for m in pat.finditer(norm):
            if _governs_target(m.start(), m.end(), norm, target_spans,
                               blocked_fallback=blocked_fallback):
                raw.append((op, m.start(), m.end(), code))
    raw += [("ambiguous", m.start(), m.end(), "ambiguous_clear_or_null_value")
            for m in _AMBIGUOUS_SIN.finditer(norm)]
    for s, e in _sin_control_spans(norm, exclude_only, executable, has_evidence, target_spans):
        raw.append((_OP.ADD_EXCLUDE, s, e, "add_exclude_sin"))

    all_spans = [(s, e) for _, s, e, _ in raw]
    signals = [t for t in raw if not _subsumed((t[1], t[2]), all_spans)]
    # Sin evidencia de modalidad (NO_MATCH) los verbos dirigidos no crean operación.
    if not has_evidence:
        signals = [t for t in signals if t[0] in ("ambiguous", _OP.CLEAR)]

    distinct_ops = {t[0] for t in signals if t[0] != "ambiguous"}
    has_ambiguous = any(t[0] == "ambiguous" for t in signals)
    spans = [(t[1], t[2]) for t in signals]

    # 3) Decisión sin precedencia numérica.
    if (has_ambiguous and distinct_ops) or len(distinct_ops) >= 2:
        return _make_instruction(_STATUS_CONFLICT, None, "control_conflict", None,
                                 text, spans, False, "incompatible control operations")
    if has_ambiguous:
        return _make_instruction(_STATUS_AMBIGUOUS, None, "ambiguous_clear_or_null_value",
                                 None, text, spans, False,
                                 "ambiguous between clear-filter and null-value")
    if len(distinct_ops) == 1:
        op = next(iter(distinct_ops))
        contrib = sorted((t for t in signals if t[0] == op), key=lambda t: t[1])
        return _make_instruction(_STATUS_RESOLVED, op, contrib[0][3], None, text,
                                 [(t[1], t[2]) for t in contrib],
                                 op is not _OP.CLEAR, f"atomic control {op.value}")

    # 4) Sin señal: REPLACE implícito (solo ejecutable + filtro no vacío) o herencia.
    if executable and allow_implicit_replace and not current_filter.is_empty():
        return _make_instruction(_STATUS_RESOLVED, _OP.REPLACE, "implicit_target_replace",
                                 None, text, [], True, "implicit replace of previous modality")
    return _make_instruction(_STATUS_NONE, None, None, None, text, [], False,
                             "no modality control language")


_STATUS_TO_CODE = {
    ResolutionStatus.PARTIAL.value: "partial_target",
    ResolutionStatus.AMBIGUOUS.value: "ambiguous_target",
    ResolutionStatus.CONFLICT.value: "conflicting_target",
    ResolutionStatus.NOT_SEARCHABLE.value: "non_searchable_target",
    ResolutionStatus.DATASET_INAPPLICABLE.value: "dataset_inapplicable_target",
}


def _trace(instruction, previous, targets, final, reason, no_op) -> dict:
    return {
        "instruction_status": instruction.status,
        "operation": instruction.operation.value if instruction.operation else None,
        "control_code": instruction.control_code,
        "construction_id": instruction.construction_id,
        "previous": {"include": list(previous.include_procedure_ids), "exclude": list(previous.exclude_procedure_ids)},
        "current_targets": sorted(targets),
        "final": {"include": list(final.include_procedure_ids), "exclude": list(final.exclude_procedure_ids)},
        "reason": reason,
        "no_op": no_op,
    }


def _blocked(instruction, previous, code, targets=()) -> ModalityMutationDecision:
    return ModalityMutationDecision(
        should_apply=False, final_filter=previous, reason=code,
        needs_clarification=True, clarification_code=code, no_op=False,
        trace=_trace(instruction, previous, targets, previous, code, False),
    )


def apply_modality_followup_operation(
    previous_filter: ModalityFilter,
    current_filter: ModalityFilter,
    instruction: ModalityFollowupInstruction,
    resolution_status,
    known_procedure_ids,
) -> ModalityMutationDecision:
    """Aplica la operación resuelta. No muta los objetos recibidos; fail-closed.

    Política de IDs (congelada): se validan los IDs de ``previous_filter`` y —salvo
    CLEAR, cuyo filtro actual se descarta— los de ``current_filter``; ningún ID
    desconocido puede llegar al filtro final.
    """
    known = set(known_procedure_ids)
    op = instruction.operation

    if instruction.status == _STATUS_NONE:
        return ModalityMutationDecision(
            should_apply=False, final_filter=previous_filter, reason="no_operation",
            needs_clarification=False, clarification_code=None, no_op=False,
            trace=_trace(instruction, previous_filter, (), previous_filter,
                         "no_operation", False),
        )
    if instruction.status in (_STATUS_AMBIGUOUS, _STATUS_CONFLICT):
        default = "ambiguous_control" if instruction.status == _STATUS_AMBIGUOUS else "control_conflict"
        return _blocked(instruction, previous_filter, instruction.control_code or default)

    status = _status_value(resolution_status)

    # Validación de IDs conocidos en previous (+ current salvo CLEAR): fail-closed.
    ids = set(previous_filter.include_procedure_ids) | set(previous_filter.exclude_procedure_ids)
    if op is not _OP.CLEAR:
        ids |= set(current_filter.include_procedure_ids) | set(current_filter.exclude_procedure_ids)
    if ids - known:
        return _blocked(instruction, previous_filter, "unknown_procedure_id")

    P_inc = set(previous_filter.include_procedure_ids)
    P_exc = set(previous_filter.exclude_procedure_ids)
    C_inc = set(current_filter.include_procedure_ids)
    C_exc = set(current_filter.exclude_procedure_ids)
    targets = C_inc | C_exc

    if op is _OP.CLEAR:
        final = ModalityFilter()
        no_op = previous_filter.is_empty()
        reason = "clear_noop" if no_op else "clear_applied"
        return ModalityMutationDecision(
            should_apply=True, final_filter=final, reason=reason,
            needs_clarification=False, clarification_code=None, no_op=no_op,
            trace=_trace(instruction, previous_filter, (), final, reason, no_op),
        )

    # Gate M2 para operaciones dirigidas.
    if status == _NO_MATCH:
        return _blocked(instruction, previous_filter, "target_missing")
    if status in _STATUS_TO_CODE:
        return _blocked(instruction, previous_filter, _STATUS_TO_CODE[status])
    if status not in _ALLOWED or not targets:
        return _blocked(instruction, previous_filter, "target_missing")

    # Guard de polaridad mixta: una sola operación dirigida no puede representar
    # el include y el exclude que M2 resolvió a la vez. REPLACE conserva el filtro
    # completo y CLEAR se resolvió antes; el resto se bloquea (no se usa la unión).
    if (op is not _OP.REPLACE and current_filter.include_procedure_ids
            and current_filter.exclude_procedure_ids):
        return _blocked(instruction, previous_filter, "mixed_target_polarity", targets)

    # Álgebra de mutación (targets = current.include ∪ current.exclude).
    if op is _OP.REPLACE:
        F_inc, F_exc = C_inc, C_exc
    elif op is _OP.ADD_INCLUDE:
        F_inc, F_exc = P_inc | targets, P_exc - targets
    elif op is _OP.ADD_EXCLUDE:
        F_inc, F_exc = P_inc - targets, P_exc | targets
    elif op is _OP.REMOVE_MEMBER:
        F_inc, F_exc = P_inc - targets, P_exc - targets
    elif op is _OP.REMOVE_EXCLUDE:
        F_inc, F_exc = P_inc, P_exc - targets
    elif op is _OP.RESTORE_INCLUDE:  # retira exclusión; agrega a include solo si hay whitelist.
        F_exc = P_exc - targets
        F_inc = (P_inc | targets) if P_inc else set()
    else:  # inalcanzable tras validar la instrucción; sin fallback algebraico.
        raise ValueError(f"operación no soportada: {op!r}")

    final = ModalityFilter(include_procedure_ids=sorted(F_inc),
                           exclude_procedure_ids=sorted(F_exc))
    if (set(final.include_procedure_ids) | set(final.exclude_procedure_ids)) - known:
        return _blocked(instruction, previous_filter, "unknown_procedure_id", targets)

    no_op = (final.include_procedure_ids == previous_filter.include_procedure_ids
             and final.exclude_procedure_ids == previous_filter.exclude_procedure_ids)
    if not no_op:
        reason = "mutation_applied"
    elif op in (_OP.REMOVE_MEMBER, _OP.REMOVE_EXCLUDE):
        reason = "remove_absent_target"
    else:
        reason = "mutation_noop"
    return ModalityMutationDecision(
        should_apply=True, final_filter=final, reason=reason,
        needs_clarification=False, clarification_code=None, no_op=no_op,
        trace=_trace(instruction, previous_filter, targets, final, reason, no_op),
    )

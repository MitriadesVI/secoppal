"""Tests del motor puro de mutaciones conversacionales de modalidad (M3C-B / R1).

Cubre: clasificador de control (señales atómicas, construcciones cerradas,
conflictos, `sin modalidad` ambiguo, REPLACE implícito, ataques adversariales,
protección de "sin" jurídico vía forma del filtro M2, preservación de operación
bajo status bloqueado), álgebra completa (matriz + RESTORE_INCLUDE
whitelist/abierto), gate por status M2, flujo real clasificador→motor, IDs
desconocidos (fail-closed) y property tests deterministas (sin Hypothesis, que
no es dependencia del proyecto).
"""
from __future__ import annotations

import random

import pytest

from app.core.modality_filter import ModalityFilter
from app.core.modality_followup import (
    ModalityFollowupInstruction,
    ModalityFollowupOperation as Op,
    ModalityMutationDecision,
    apply_modality_followup_operation,
    classify_modality_followup_control,
)
from app.core.modality_resolution_models import ResolutionStatus


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

RESOLVED = ResolutionStatus.RESOLVED
RWA = ResolutionStatus.RESOLVED_WITH_ASSUMPTION
NO_MATCH = ResolutionStatus.NO_MATCH


def mf(include=(), exclude=()) -> ModalityFilter:
    return ModalityFilter(include_procedure_ids=tuple(include),
                          exclude_procedure_ids=tuple(exclude))


def instr(op: Op, control_code="c", construction_id=None) -> ModalityFollowupInstruction:
    return ModalityFollowupInstruction(
        status="resolved", operation=op, control_code=control_code,
        construction_id=construction_id, source_text="x", control_spans=(),
        target_required=op is not Op.CLEAR, explanation="test",
    )


def apply(prev, cur, op, status=RESOLVED, known=None, **kw):
    if known is None:
        known = set(prev.include_procedure_ids) | set(prev.exclude_procedure_ids)
        known |= set(cur.include_procedure_ids) | set(cur.exclude_procedure_ids)
    return apply_modality_followup_operation(prev, cur, instr(op, **kw), status, known)


def classify(text, *, status=RESOLVED, current=None, implicit=False):
    """Nueva API R1: el clasificador recibe forma del filtro M2 + status.

    Por defecto simula un target ejecutable include-only no vacío.
    """
    if current is None:
        current = mf(("proc_target",)) if status in (RESOLVED, RWA) else mf()
    return classify_modality_followup_control(
        text, current_filter=current, resolution_status=status,
        allow_implicit_replace=implicit)


# --------------------------------------------------------------------------- #
# 1. Clasificador — señales atómicas
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text, op", [
    ("ahora licitación pública", Op.REPLACE),
    ("cambia a mínima cuantía", Op.REPLACE),
    ("solo subasta inversa", Op.REPLACE),
    ("usa régimen especial", Op.REPLACE),
    ("únicamente subasta inversa", Op.REPLACE),
    ("además contratación directa", Op.ADD_INCLUDE),
    ("también régimen especial", Op.ADD_INCLUDE),
    ("agrega contratación directa", Op.ADD_INCLUDE),
    ("excepto contratación directa", Op.ADD_EXCLUDE),
    ("excluye mínima cuantía", Op.ADD_EXCLUDE),
    ("quita contratación directa", Op.REMOVE_MEMBER),
    ("elimina régimen especial", Op.REMOVE_MEMBER),
    ("saca mínima cuantía", Op.REMOVE_MEMBER),
    ("permite contratación directa", Op.REMOVE_EXCLUDE),
    ("deja de excluir mínima cuantía", Op.REMOVE_EXCLUDE),
    ("vuelve a incluir contratación directa", Op.RESTORE_INCLUDE),
    ("reincorpora contratación directa", Op.RESTORE_INCLUDE),
])
def test_atomic_signals(text, op):
    ins = classify(text, status=RESOLVED)
    assert ins.status == "resolved"
    assert ins.operation is op
    assert ins.construction_id is None
    assert ins.target_required is True
    assert ins.control_spans


def test_sin_x_as_exclusion_via_exclude_only_shape():
    # "sin mínima cuantía": M2 lo resuelve como exclusión (exclude-only) → ADD_EXCLUDE.
    for text in ("sin mínima cuantía", "sin licitación pública"):
        ins = classify(text, status=RESOLVED, current=mf((), ("proc_excl",)))
        assert ins.operation is Op.ADD_EXCLUDE, text
        assert ins.control_spans


@pytest.mark.parametrize("text", [
    "sin filtro de modalidad",
    "quita el filtro de modalidad",
    "elimina el filtro de modalidad",
    "cualquier modalidad",
    "todas las modalidades",
    "no importa la modalidad",
])
def test_clear_phrases(text):
    ins = classify(text, status=NO_MATCH)
    assert ins.status == "resolved"
    assert ins.operation is Op.CLEAR
    assert ins.target_required is False


def test_sin_modalidad_is_ambiguous():
    ins = classify("sin modalidad", status=NO_MATCH)
    assert ins.status == "ambiguous"
    assert ins.operation is None
    assert ins.control_code == "ambiguous_clear_or_null_value"


def test_sin_modalidad_ambiguous_even_with_resolved_target():
    ins = classify("sin modalidad", status=RESOLVED, current=mf(("proc_t",)))
    assert ins.status == "ambiguous"
    assert ins.operation is None


# --------------------------------------------------------------------------- #
# 2. R1 — "sin" jurídico protegido por la forma del filtro M2
# --------------------------------------------------------------------------- #

# Estas modalidades positivas contienen "sin" en su nombre jurídico; M2 las
# resuelve include-only. El clasificador NO debe leerlas como exclusión.
_LEGAL_SIN = [
    "contratación directa sin ofertas",
    "contrataciones directas sin ofertas",
    "régimen especial sin ofertas",
    "menor cuantía sin manifestación",
    "menor cuantía sin manifestación de interés",
    "selección abreviada sin manifestación de interés",
]


@pytest.mark.parametrize("text", _LEGAL_SIN)
def test_legal_sin_is_not_add_exclude(text):
    ins = classify(text, status=RESOLVED, current=mf(("proc_pos",)))  # include-only
    assert ins.operation is not Op.ADD_EXCLUDE, text


@pytest.mark.parametrize("text", _LEGAL_SIN)
def test_legal_sin_becomes_implicit_replace(text):
    ins = classify(text, status=RESOLVED, current=mf(("proc_pos",)), implicit=True)
    assert ins.operation is Op.REPLACE
    assert ins.control_code == "implicit_target_replace"


@pytest.mark.parametrize("text, op", [
    ("además contratación directa sin ofertas", Op.ADD_INCLUDE),
    ("ahora régimen especial sin ofertas", Op.REPLACE),
    ("quita menor cuantía sin manifestación", Op.REMOVE_MEMBER),
])
def test_explicit_verb_with_internal_legal_sin(text, op):
    # El verbo explícito conserva su operación; el "sin" interno no participa.
    ins = classify(text, status=RESOLVED, current=mf(("proc_pos",)))  # include-only
    assert ins.operation is op
    assert ins.status != "conflict"


# --------------------------------------------------------------------------- #
# 3. Clasificador — construcciones cerradas
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text", [
    "cualquier modalidad excepto contratación directa",
    "todas las modalidades excepto mínima cuantía",
])
def test_closed_replace_all_except(text):
    ins = classify(text, status=RESOLVED, current=mf((), ("proc_excl",)))
    assert ins.operation is Op.REPLACE
    assert ins.construction_id == "replace_all_except"


@pytest.mark.parametrize("text", [
    "no importa la modalidad, solo subasta inversa",
    "no importa la modalidad, busca licitación pública",
])
def test_closed_clear_then_replace(text):
    ins = classify(text, status=RESOLVED, current=mf(("proc_t",)))
    assert ins.operation is Op.REPLACE
    assert ins.construction_id == "clear_then_replace"


def test_clear_then_replace_needs_evidence():
    # Sin evidencia de modalidad (NO_MATCH) queda CLEAR limpio, no REPLACE.
    ins = classify("no importa la modalidad, solo alimentación", status=NO_MATCH)
    assert ins.operation is Op.CLEAR


# --------------------------------------------------------------------------- #
# 4. Clasificador — conflictos y señales equivalentes
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text", [
    "además, excepto contratación directa",
    "quita y agrega contratación directa",
    "quita licitación y agrega mínima cuantía",
    "sin modalidad excepto mínima cuantía",
])
def test_control_conflict(text):
    ins = classify(text, status=RESOLVED, current=mf(("proc_t",)))
    assert ins.status == "conflict"
    assert ins.operation is None
    assert ins.control_code == "control_conflict"


def test_equivalent_signals_collapse_to_single_operation():
    ins = classify("agrega e incluye contratación directa", status=RESOLVED)
    assert ins.status == "resolved"
    assert ins.operation is Op.ADD_INCLUDE
    assert len(ins.control_spans) == 2


def test_no_numeric_precedence_two_signals_conflict_not_first_wins():
    ins = classify("quita y agrega contratación directa", status=RESOLVED)
    assert ins.status == "conflict"


# --------------------------------------------------------------------------- #
# 5. R1 — Finding 2: la operación se preserva bajo status bloqueado
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text, op, m2, code", [
    ("además subasta", Op.ADD_INCLUDE, ResolutionStatus.AMBIGUOUS, "ambiguous_target"),
    ("quita subasta", Op.REMOVE_MEMBER, ResolutionStatus.PARTIAL, "partial_target"),
    ("excepto prueba de plataforma", Op.ADD_EXCLUDE, ResolutionStatus.NOT_SEARCHABLE, "non_searchable_target"),
    ("además régimen", Op.ADD_INCLUDE, ResolutionStatus.DATASET_INAPPLICABLE, "dataset_inapplicable_target"),
])
def test_blocked_status_preserves_operation(text, op, m2, code):
    # El clasificador conserva la operación (no la pierde en status=none)...
    ins = classify(text, status=m2, current=mf())
    assert ins.status == "resolved"
    assert ins.operation is op
    # ...y el motor bloquea con el código específico del status M2.
    prev = mf(("proc_x",))
    d = apply_modality_followup_operation(prev, mf(), ins, m2, {"proc_x"})
    assert d.should_apply is False
    assert d.clarification_code == code
    assert d.final_filter == prev


def test_sin_subasta_prefix_under_ambiguous_status():
    # "sin subasta" + AMBIGUOUS: "sin" es control por prefijo → ADD_EXCLUDE preservado.
    ins = classify("sin subasta", status=ResolutionStatus.AMBIGUOUS, current=mf())
    assert ins.operation is Op.ADD_EXCLUDE
    prev = mf(("proc_x",))
    d = apply_modality_followup_operation(prev, mf(), ins, ResolutionStatus.AMBIGUOUS, {"proc_x"})
    assert d.clarification_code == "ambiguous_target"


# --------------------------------------------------------------------------- #
# 6. Clasificador — REPLACE implícito y ataques sin evidencia (NO_MATCH)
# --------------------------------------------------------------------------- #

def test_implicit_replace():
    ins = classify("mínima cuantía", status=RESOLVED, current=mf(("proc_t",)), implicit=True)
    assert ins.operation is Op.REPLACE
    assert ins.control_code == "implicit_target_replace"


def test_implicit_replace_not_triggered_without_flag():
    assert classify("mínima cuantía", status=RESOLVED, current=mf(("proc_t",))).status == "none"


def test_implicit_replace_not_triggered_without_executable_status():
    ins = classify("subasta", status=ResolutionStatus.AMBIGUOUS, current=mf(), implicit=True)
    assert ins.status == "none"


def test_implicit_replace_not_triggered_on_empty_filter():
    assert classify("algo", status=RESOLVED, current=mf(), implicit=True).status == "none"


@pytest.mark.parametrize("text", [
    "además alimentación",
    "sin alimentación",
    "solo alimentación",
    "quita estado abierto",
    "permite procesos cerrados",
    "elimina todas",
    "solo esas",
    "quita la anterior",
])
def test_adversarial_no_modality_evidence(text):
    # NO_MATCH: sin evidencia de modalidad no hay operación válida.
    ins = classify(text, status=NO_MATCH, current=mf())
    assert ins.status == "none"
    assert ins.operation is None


def test_elimina_todas_is_not_clear():
    ins = classify("elimina todas", status=NO_MATCH, current=mf())
    assert ins.operation is not Op.CLEAR
    assert ins.status == "none"


# --------------------------------------------------------------------------- #
# 7. Álgebra — matriz del documento
# --------------------------------------------------------------------------- #

def test_algebra_matrix():
    A, B = "proc_a", "proc_b"
    cases = [
        ((), (), (A,), (), Op.REPLACE, (A,), ()),
        ((A,), (), (B,), (), Op.REPLACE, (B,), ()),
        ((A,), (), (B,), (), Op.ADD_INCLUDE, (A, B), ()),
        ((A,), (), (A,), (), Op.ADD_INCLUDE, (A,), ()),
        ((), (A,), (A,), (), Op.ADD_INCLUDE, (A,), ()),
        ((A,), (), (A,), (), Op.ADD_EXCLUDE, (), (A,)),
        ((A, B), (), (A,), (), Op.REMOVE_MEMBER, (B,), ()),
        ((), (A, B), (A,), (), Op.REMOVE_MEMBER, (), (B,)),
        ((), (A,), (A,), (), Op.REMOVE_EXCLUDE, (), ()),
        ((A,), (), (), (), Op.CLEAR, (), ()),
        ((), (), (), (), Op.CLEAR, (), ()),
        ((A,), (), (A,), (), Op.REMOVE_MEMBER, (), ()),
        ((A,), (B,), (B,), (), Op.RESTORE_INCLUDE, (A, B), ()),
        ((), (B,), (B,), (), Op.RESTORE_INCLUDE, (), ()),
        ((), (), (B,), (), Op.RESTORE_INCLUDE, (), ()),
        ((A,), (), (B,), (), Op.RESTORE_INCLUDE, (A, B), ()),
        ((A,), (B,), (B,), (), Op.REMOVE_EXCLUDE, (A,), ()),
    ]
    for p_inc, p_exc, c_inc, c_exc, op, f_inc, f_exc in cases:
        d = apply(mf(p_inc, p_exc), mf(c_inc, c_exc), op)
        assert d.should_apply is True, (op, p_inc, p_exc, c_inc, c_exc)
        assert d.final_filter.include_procedure_ids == tuple(sorted(f_inc)), op
        assert d.final_filter.exclude_procedure_ids == tuple(sorted(f_exc)), op


def test_restore_include_whitelist():
    d = apply(mf(("proc_a",), ("proc_b",)), mf(("proc_b",), ()), Op.RESTORE_INCLUDE)
    assert d.final_filter.include_procedure_ids == ("proc_a", "proc_b")
    assert d.final_filter.exclude_procedure_ids == ()


def test_restore_include_open_mode_never_creates_whitelist():
    d = apply(mf((), ("proc_b",)), mf(("proc_b",), ()), Op.RESTORE_INCLUDE)
    assert d.final_filter.include_procedure_ids == ()
    assert d.final_filter.exclude_procedure_ids == ()


def test_targets_from_exclude_polarity():
    d = apply(mf(("proc_x",), ()), mf((), ("proc_a",)), Op.ADD_INCLUDE)
    assert d.final_filter.include_procedure_ids == ("proc_a", "proc_x")


# --------------------------------------------------------------------------- #
# 7b. R2 — Filtro M2 mixto (include y exclude a la vez)
# --------------------------------------------------------------------------- #

_MIXED = mf(("licitacion_publica",), ("contratacion_directa",))
_KNOWN_MIXED = {"minima_cuantia", "licitacion_publica", "contratacion_directa"}


def test_mixed_replace_allowed():
    prev = mf(("minima_cuantia",))
    d = apply(prev, _MIXED, Op.REPLACE, known=_KNOWN_MIXED)
    assert d.should_apply is True
    assert d.final_filter.include_procedure_ids == ("licitacion_publica",)
    assert d.final_filter.exclude_procedure_ids == ("contratacion_directa",)


def test_mixed_clear_allowed():
    d = apply(mf(("minima_cuantia",)), _MIXED, Op.CLEAR, known=_KNOWN_MIXED)
    assert d.should_apply is True
    assert d.final_filter.is_empty()
    assert d.reason == "clear_applied"


@pytest.mark.parametrize("op", [
    Op.ADD_INCLUDE, Op.ADD_EXCLUDE, Op.REMOVE_MEMBER, Op.REMOVE_EXCLUDE, Op.RESTORE_INCLUDE])
def test_mixed_directed_operations_blocked(op):
    prev = mf(("minima_cuantia",))
    d = apply(prev, _MIXED, op, known=_KNOWN_MIXED)
    assert d.should_apply is False
    assert d.needs_clarification is True
    assert d.clarification_code == "mixed_target_polarity"
    assert d.final_filter == prev  # previous intacto
    # nunca se cuela el lado equivocado
    assert "contratacion_directa" not in d.final_filter.include_procedure_ids


def test_mixed_guard_after_status_gate():
    # Un status bloqueado conserva su código específico, no mixed_target_polarity.
    prev = mf(("minima_cuantia",))
    d = apply(prev, _MIXED, Op.ADD_INCLUDE, status=ResolutionStatus.PARTIAL, known=_KNOWN_MIXED)
    assert d.clarification_code == "partial_target"


# --------------------------------------------------------------------------- #
# 8. Gate por status M2 (motor aislado)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("status", [RESOLVED, RWA])
def test_gate_allowed_statuses_apply(status):
    d = apply(mf(("proc_x",)), mf(("proc_a",)), Op.ADD_INCLUDE, status=status)
    assert d.should_apply is True
    assert d.needs_clarification is False


@pytest.mark.parametrize("status, code", [
    (ResolutionStatus.PARTIAL, "partial_target"),
    (ResolutionStatus.AMBIGUOUS, "ambiguous_target"),
    (ResolutionStatus.CONFLICT, "conflicting_target"),
    (ResolutionStatus.NOT_SEARCHABLE, "non_searchable_target"),
    (ResolutionStatus.DATASET_INAPPLICABLE, "dataset_inapplicable_target"),
])
def test_gate_blocked_statuses(status, code):
    prev = mf(("proc_x",))
    d = apply(prev, mf(("proc_a",)), Op.ADD_INCLUDE, status=status)
    assert d.should_apply is False
    assert d.needs_clarification is True
    assert d.clarification_code == code
    assert d.final_filter == prev


def test_gate_no_match_blocks_targeted_operation():
    prev = mf(("proc_x",))
    d = apply(prev, mf(), Op.ADD_INCLUDE, status=NO_MATCH)
    assert d.should_apply is False
    assert d.clarification_code == "target_missing"
    assert d.final_filter == prev


def test_gate_no_match_allows_clear():
    prev = mf(("proc_x",))
    d = apply(prev, mf(), Op.CLEAR, status=NO_MATCH)
    assert d.should_apply is True
    assert d.final_filter.is_empty()
    assert d.reason == "clear_applied"


def test_targeted_operation_without_targets_is_target_missing():
    prev = mf(("proc_x",))
    d = apply(prev, mf(), Op.REMOVE_MEMBER, status=RESOLVED)
    assert d.clarification_code == "target_missing"
    assert d.final_filter == prev


# --------------------------------------------------------------------------- #
# 9. Flujo real clasificador → motor (no solo instrucciones manuales)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text, m2, code", [
    ("además subasta", ResolutionStatus.AMBIGUOUS, "ambiguous_target"),
    ("quita subasta", ResolutionStatus.PARTIAL, "partial_target"),
    ("excepto x", ResolutionStatus.NOT_SEARCHABLE, "non_searchable_target"),
    ("además y", ResolutionStatus.DATASET_INAPPLICABLE, "dataset_inapplicable_target"),
])
def test_classifier_engine_integration_blocked(text, m2, code):
    prev = mf(("proc_x",))
    ins = classify(text, status=m2, current=mf())
    d = apply_modality_followup_operation(prev, mf(), ins, m2, {"proc_x"})
    assert d.should_apply is False
    assert d.clarification_code == code


def test_classifier_engine_integration_applies():
    prev = mf(("proc_x",))
    cur = mf(("proc_a",))
    ins = classify("además contratación directa", status=RESOLVED, current=cur)
    d = apply_modality_followup_operation(prev, cur, ins, RESOLVED, {"proc_x", "proc_a"})
    assert d.should_apply is True
    assert d.final_filter.include_procedure_ids == ("proc_a", "proc_x")


def test_real_m2_mixed_replace_preserves_polarity():
    # M2 real: "licitación pública sin contratación directa" → filtro mixto.
    from app.core.modality_resolver import resolve
    r = resolve("licitación pública sin contratación directa")
    inc = tuple(sorted({p for m in (r.includes or []) for p in m.procedure_ids}))
    exc = tuple(sorted({p for m in (r.excludes or []) for p in m.procedure_ids}))
    cur = mf(inc, exc)
    assert inc and exc, "el caso debe ser mixto"
    ins = classify("licitación pública sin contratación directa",
                   status=r.status, current=cur, implicit=True)
    assert ins.operation is Op.REPLACE  # implícito
    prev = mf(("minima_cuantia",))
    d = apply_modality_followup_operation(
        prev, cur, ins, r.status, set(inc) | set(exc) | {"minima_cuantia"})
    assert d.final_filter.include_procedure_ids == inc
    assert d.final_filter.exclude_procedure_ids == exc


def test_real_m2_mixed_add_include_blocked():
    # M2 real: "además licitación pública sin contratación directa" → ADD_INCLUDE
    # sobre filtro mixto → el motor bloquea; contratación directa NO se incluye.
    from app.core.modality_resolver import resolve
    r = resolve("además licitación pública sin contratación directa")
    inc = tuple(sorted({p for m in (r.includes or []) for p in m.procedure_ids}))
    exc = tuple(sorted({p for m in (r.excludes or []) for p in m.procedure_ids}))
    cur = mf(inc, exc)
    assert inc and exc, "el caso debe ser mixto"
    ins = classify("además licitación pública sin contratación directa",
                   status=r.status, current=cur)
    assert ins.operation is Op.ADD_INCLUDE
    prev = mf(("minima_cuantia",))
    d = apply_modality_followup_operation(
        prev, cur, ins, r.status, set(inc) | set(exc) | {"minima_cuantia"})
    assert d.should_apply is False
    assert d.clarification_code == "mixed_target_polarity"
    assert not (set(exc) & set(d.final_filter.include_procedure_ids))


def test_classifier_engine_legal_sin_does_not_invert():
    # Regresión Finding 1 end-to-end: previa licitación pública + "contratación
    # directa sin ofertas" (include-only) NO debe excluir contratación directa.
    prev = mf(("licitacion_publica",))
    cur = mf(("contratacion_directa",))  # M2 include-only
    ins = classify("contratación directa sin ofertas", status=RESOLVED,
                   current=cur, implicit=True)
    d = apply_modality_followup_operation(
        prev, cur, ins, RESOLVED, {"licitacion_publica", "contratacion_directa"})
    assert ins.operation is Op.REPLACE
    assert d.final_filter.include_procedure_ids == ("contratacion_directa",)
    assert d.final_filter.exclude_procedure_ids == ()


# --------------------------------------------------------------------------- #
# 10. IDs desconocidos — fail-closed
# --------------------------------------------------------------------------- #

def test_unknown_id_in_previous():
    d = apply(mf(("unk",)), mf(("proc_a",)), Op.ADD_INCLUDE, known={"proc_a"})
    assert d.should_apply is False
    assert d.clarification_code == "unknown_procedure_id"
    assert d.final_filter.include_procedure_ids == ("unk",)


def test_unknown_id_in_current():
    prev = mf(("proc_a",))
    d = apply(prev, mf(("unk",)), Op.ADD_INCLUDE, known={"proc_a"})
    assert d.should_apply is False
    assert d.clarification_code == "unknown_procedure_id"
    assert d.final_filter == prev


def test_unknown_id_would_reach_result():
    prev = mf(("proc_a",))
    d = apply(prev, mf(("unk",)), Op.ADD_EXCLUDE, known={"proc_a"})
    assert d.should_apply is False
    assert d.clarification_code == "unknown_procedure_id"
    assert d.final_filter == prev


def test_clear_ignores_unknown_current_id():
    # Política congelada: CLEAR descarta current; un ID desconocido allí no bloquea.
    prev = mf(("proc_a",))
    d = apply(prev, mf(("unk",)), Op.CLEAR, known={"proc_a"})
    assert d.should_apply is True
    assert d.final_filter.is_empty()
    assert d.reason == "clear_applied"


def test_clear_validates_unknown_previous_id():
    # Política congelada: CLEAR sí valida previous.
    d = apply(mf(("unk",)), mf(), Op.CLEAR, known={"proc_a"})
    assert d.should_apply is False
    assert d.clarification_code == "unknown_procedure_id"


# --------------------------------------------------------------------------- #
# 11. No-op
# --------------------------------------------------------------------------- #

def test_noop_add_present_include():
    prev = mf(("proc_a",))
    d = apply(prev, mf(("proc_a",)), Op.ADD_INCLUDE)
    assert d.should_apply is True and d.no_op is True
    assert d.reason == "mutation_noop"
    assert d.final_filter == prev


def test_noop_remove_absent_target():
    prev = mf(("proc_a",))
    d = apply(prev, mf(("proc_b",)), Op.REMOVE_MEMBER)
    assert d.no_op is True
    assert d.reason == "remove_absent_target"


def test_noop_permite_absent_exclusion():
    prev = mf(("proc_a",))
    d = apply(prev, mf(("proc_b",)), Op.REMOVE_EXCLUDE)
    assert d.no_op is True
    assert d.reason == "remove_absent_target"


def test_clear_noop_on_empty_filter():
    d = apply(mf(), mf(), Op.CLEAR)
    assert d.should_apply is True and d.no_op is True
    assert d.reason == "clear_noop"


# --------------------------------------------------------------------------- #
# 12. Status none / ambiguous / conflict en el motor
# --------------------------------------------------------------------------- #

def test_engine_none_no_mutation():
    prev = mf(("proc_a",))
    ins = classify("solo alimentación", status=NO_MATCH, current=mf())
    d = apply_modality_followup_operation(prev, mf(), ins, NO_MATCH, {"proc_a"})
    assert d.should_apply is False
    assert d.needs_clarification is False
    assert d.final_filter == prev
    assert d.reason == "no_operation"


def test_engine_ambiguous_needs_clarification():
    prev = mf(("proc_a",))
    ins = classify("sin modalidad", status=NO_MATCH, current=mf())
    d = apply_modality_followup_operation(prev, mf(), ins, NO_MATCH, {"proc_a"})
    assert d.needs_clarification is True
    assert d.clarification_code == "ambiguous_clear_or_null_value"
    assert d.final_filter == prev


def test_engine_conflict_needs_clarification():
    prev = mf(("proc_a",))
    ins = classify("quita y agrega contratación directa", status=RESOLVED)
    d = apply_modality_followup_operation(prev, mf(), ins, RESOLVED, {"proc_a"})
    assert d.needs_clarification is True
    assert d.clarification_code == "control_conflict"


# --------------------------------------------------------------------------- #
# 13. Trace JSON-native
# --------------------------------------------------------------------------- #

def test_trace_is_json_native():
    import json
    d = apply(mf(("proc_x",)), mf(("proc_a",)), Op.ADD_INCLUDE)
    reloaded = json.loads(json.dumps(d.trace))
    assert reloaded["operation"] == "add_include"
    assert reloaded["previous"]["include"] == ["proc_x"]
    assert reloaded["current_targets"] == ["proc_a"]
    assert reloaded["final"]["include"] == ["proc_a", "proc_x"]
    assert reloaded["no_op"] is False
    for section in ("previous", "final"):
        assert isinstance(d.trace[section]["include"], list)


def test_trace_keys_stable_and_ids_ordered():
    d = apply(mf(("proc_z", "proc_a")), mf(("proc_m",)), Op.ADD_INCLUDE)
    assert set(d.trace) == {
        "instruction_status", "operation", "control_code", "construction_id",
        "previous", "current_targets", "final", "reason", "no_op",
    }
    assert d.trace["final"]["include"] == sorted(d.trace["final"]["include"])


# --------------------------------------------------------------------------- #
# 14. Invariantes de contratos
# --------------------------------------------------------------------------- #

def test_instruction_resolved_requires_operation():
    with pytest.raises(ValueError):
        ModalityFollowupInstruction("resolved", None, None, None, "x", (), False, "e")


def test_instruction_non_resolved_forbids_operation():
    with pytest.raises(ValueError):
        ModalityFollowupInstruction("conflict", Op.REPLACE, None, None, "x", (), False, "e")


def test_instruction_spans_must_be_ordered_and_valid():
    with pytest.raises(ValueError):
        ModalityFollowupInstruction("none", None, None, None, "abc", ((2, 1),), False, "e")
    with pytest.raises(ValueError):
        ModalityFollowupInstruction("none", None, None, None, "abc", ((0, 2), (1, 3)), False, "e")


def test_decision_clarification_implies_not_apply():
    with pytest.raises(ValueError):
        ModalityMutationDecision(True, mf(), "r", True, "code", False, {})


def test_instruction_rejects_string_operation():
    with pytest.raises(ValueError):
        ModalityFollowupInstruction("resolved", "add_include", "c", None, "x", (), True, "e")


def test_instruction_rejects_foreign_enum_operation():
    import enum

    class ForeignOperation(str, enum.Enum):
        ADD_INCLUDE = "add_include"

    with pytest.raises(ValueError):
        ModalityFollowupInstruction(
            "resolved", ForeignOperation.ADD_INCLUDE, "c", None, "x", (), True, "e")


def test_instruction_target_required_invariant():
    # CLEAR debe declarar target_required=False; una op dirigida, True.
    with pytest.raises(ValueError):
        ModalityFollowupInstruction("resolved", Op.CLEAR, "c", None, "x", (), True, "e")
    with pytest.raises(ValueError):
        ModalityFollowupInstruction("resolved", Op.ADD_INCLUDE, "c", None, "x", (), False, "e")


def test_classifier_instructions_pass_type_invariants():
    # Sanidad: toda instrucción que produce el clasificador es contractualmente válida.
    for text, st, cur in [
        ("además contratación directa", RESOLVED, mf(("proc_t",))),
        ("sin filtro de modalidad", NO_MATCH, mf()),
        ("sin modalidad", NO_MATCH, mf()),
        ("además subasta", ResolutionStatus.AMBIGUOUS, mf()),
        ("alimentación", NO_MATCH, mf()),
    ]:
        ins = classify(text, status=st, current=cur)  # no lanza
        if ins.status == "resolved":
            assert isinstance(ins.operation, Op)
            assert ins.target_required is (ins.operation is not Op.CLEAR)


# --------------------------------------------------------------------------- #
# 15. Property tests deterministas (sin Hypothesis)
# --------------------------------------------------------------------------- #

_UNIVERSE = ["p0", "p1", "p2", "p3", "p4"]
_TARGETED_OPS = [Op.REPLACE, Op.ADD_INCLUDE, Op.ADD_EXCLUDE, Op.REMOVE_MEMBER,
                 Op.REMOVE_EXCLUDE, Op.RESTORE_INCLUDE]


def _random_filter(rng):
    ids = rng.sample(_UNIVERSE, rng.randint(0, len(_UNIVERSE)))
    inc, exc = [], []
    for i in ids:
        (inc if rng.random() < 0.5 else exc).append(i)
    return mf(inc, exc)


def _cases(n=400):
    rng = random.Random(20260723)
    for _ in range(n):
        prev = _random_filter(rng)
        cur = _random_filter(rng)
        op = rng.choice(_TARGETED_OPS + [Op.CLEAR])
        yield prev, cur, op


def test_property_result_is_valid_ordered_disjoint():
    for prev, cur, op in _cases():
        d = apply(prev, cur, op, known=set(_UNIVERSE))
        f = d.final_filter
        assert isinstance(f, ModalityFilter)
        inc, exc = f.include_procedure_ids, f.exclude_procedure_ids
        assert inc == tuple(sorted(inc))
        assert exc == tuple(sorted(exc))
        assert not (set(inc) & set(exc))


def test_property_operation_target_semantics():
    for prev, cur, op in _cases():
        if op is Op.CLEAR:
            continue
        targets = set(cur.include_procedure_ids) | set(cur.exclude_procedure_ids)
        d = apply(prev, cur, op, known=set(_UNIVERSE))
        if not d.should_apply or not targets:
            continue
        inc = set(d.final_filter.include_procedure_ids)
        exc = set(d.final_filter.exclude_procedure_ids)
        if op is Op.ADD_INCLUDE:
            assert not (targets & exc)
        elif op is Op.ADD_EXCLUDE:
            assert not (targets & inc)
        elif op is Op.REMOVE_MEMBER:
            assert not (targets & (inc | exc))
        elif op is Op.RESTORE_INCLUDE:
            assert not (targets & exc)


def test_property_clear_always_empty():
    for prev, cur, _ in _cases():
        d = apply(prev, cur, Op.CLEAR, known=set(_UNIVERSE))
        assert d.final_filter.is_empty()


def test_property_restore_open_mode_never_creates_whitelist():
    rng = random.Random(11)
    for _ in range(200):
        exc = rng.sample(_UNIVERSE, rng.randint(1, 3))
        prev = mf((), exc)
        target = rng.choice(exc)
        cur = mf((target,), ())
        d = apply(prev, cur, Op.RESTORE_INCLUDE, known=set(_UNIVERSE))
        assert d.final_filter.include_procedure_ids == ()


def test_property_determinism_decision_and_trace():
    for prev, cur, op in _cases(200):
        d1 = apply(prev, cur, op, known=set(_UNIVERSE))
        d2 = apply(prev, cur, op, known=set(_UNIVERSE))
        assert d1 == d2
        assert d1.trace == d2.trace


def test_property_idempotent_operation_is_noop():
    for prev, cur, op in _cases(300):
        d1 = apply(prev, cur, op, known=set(_UNIVERSE))
        if not d1.should_apply:
            continue
        d2 = apply(d1.final_filter, cur, op, known=set(_UNIVERSE))
        assert d2.should_apply is True
        assert d2.no_op is True
        assert d2.final_filter == d1.final_filter

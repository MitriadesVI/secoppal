"""M1-D-STATE/MOD: the silent failures of A2 §8 in ``estado_families`` (each test fails on the base).

STATE-015/016: «en ejecución» / «en evaluación» (the way a user spells them) did not resolve because
the input was lower-cased but not accent-folded, while every catalog key is folded.
STATE-017: «adjudicados» on ``procesos`` widened to the whole evaluation family although
``ESTADO_PROCEDIMIENTO_SYNONYMS`` says «Seleccionado».
"""
from __future__ import annotations

import unicodedata

import pytest

from app.core import estado_families as ef
from app.data.estados import ESTADO_PROCEDIMIENTO_SYNONYMS


def _strip(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if not unicodedata.combining(c))


def _accented_forms(token: str) -> set[str]:
    forms = {token}
    for plain, accented in (("ion", "ión"), ("cion", "ción"), ("ucion", "ución"), ("acion", "ación")):
        if plain in token:
            forms.add(token.replace(plain, accented))
    return forms


def _shape(resolved):
    """Everything but the echoed span: the span legitimately keeps the user's spelling."""
    return None if resolved is None else {k: v for k, v in resolved.items() if k != "consumed_span"}


def test_accented_spelling_resolves_like_the_folded_one_state_015_016():
    for dataset, token in (("contratos", "en ejecución"), ("procesos", "en evaluación")):
        folded = ef.resolve_estado(_strip(token), dataset)
        assert folded is not None, (dataset, token)
        assert _shape(ef.resolve_estado(token, dataset)) == _shape(folded)
        assert ef.resolve_estado(token.upper(), dataset)["consumed_span"] == token.upper()


def test_every_declared_token_is_accent_and_case_insensitive():
    """Exhaustive over the vocabulary the legacy lexicons declare, for both datasets."""
    vocabulary = {*ef.NATURAL_TO_FAMILY, *ef.LEXICAL_EXACT_STATE, *ef.DATASET_AWARE_FAMILY}
    checked = 0
    for token in sorted(vocabulary):
        for dataset in ("procesos", "contratos"):
            expected = _shape(ef.resolve_estado(token, dataset))
            for form in _accented_forms(token):
                assert _shape(ef.resolve_estado(form.upper(), dataset)) == expected, (dataset, form)
                checked += 1
    assert checked >= 2 * len(vocabulary)


@pytest.mark.parametrize("token", ["adjudicado", "adjudicada", "adjudicados", "adjudicadas"])
def test_adjudicados_on_processes_is_the_exact_literal_not_the_family_state_017(token):
    declared = ESTADO_PROCEDIMIENTO_SYNONYMS["adjudicado"]
    resolved = ef.resolve_estado(token, "procesos")
    assert resolved is not None
    assert resolved["kind"] == "exact"
    assert resolved["filters"] == {"estado_del_procedimiento": [declared]}
    # one spelling, one reading: the same literal the lexicon gives «seleccionados»
    assert resolved["filters"] == ef.resolve_estado("seleccionados", "procesos")["filters"]


def test_process_mentions_read_adjudicados_as_the_literal():
    mentions = ef.read_process_state_mentions("procesos cancelados y adjudicados de aseo")
    assert [(m.literal, m.family is not None) for m in mentions] == [("Cancelado", True), ("Seleccionado", True)]
    assert all(mention.reading == mention.literal for mention in mentions)


def test_detect_family_scrubs_adjudicados_as_the_exact_state():
    resolved = ef.detect_estado_family("procesos adjudicados de aseo", "procesos")
    assert resolved["kind"] == "exact"
    assert resolved["filters"] == {"estado_del_procedimiento": ["Seleccionado"]}


def test_the_adjudicado_alias_is_derived_from_the_lexicon_not_hardcoded():
    """If the lexicon ever re-points «adjudicado», the resolver follows it (single source)."""
    assert ef.process_state_literal("adjudicados") == ESTADO_PROCEDIMIENTO_SYNONYMS["adjudicado"]


def test_the_literal_lookup_folds_accents_too_for_direct_callers():
    assert ef.process_state_literal("evaluación") == "Evaluación"
    assert ef.process_state_literal("EVALUACIÓN") == "Evaluación"
    assert ef.process_state_literal("En Aprobación") == ef.process_state_literal("en aprobacion")

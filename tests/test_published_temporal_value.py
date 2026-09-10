"""The public temporal boundary, and the two runtime consumers that use it.

Contract Universe and the formatter both ask one question — is this published
value really a date? — and both used to answer it by importing a private Truth
Layer resolver. The guard in tests/test_truth_layer_temporal.py exists to stop
exactly that. The question now goes through the package's public surface, and
these tests pin both the answer and the two behaviours that depend on it.

Adjudicated in docs/audits/contract_universe_v1_1_final_closeout/.
"""
from __future__ import annotations

import pytest

from app.core.contract_universe import eligible
from app.core.published_temporal_value import observed_date_is_usable


USABLE = [
    ("2026-08-01", "civil date"),
    ("2026-08-01T00:00:00.000", "naive datetime"),
    ("2026-08-01T00:00:00+00:00", "offset datetime"),
    ("2026-08-01T12:34:56-05:00", "Bogota offset"),
]
UNUSABLE = [
    ("no es fecha", "free text"),
    ("", "blank"),
    ("   ", "whitespace"),
    ("2026-13-45", "impossible calendar date"),
    ("01/08/2026", "non-ISO format"),
    ("2026-08-01T25:00:00", "impossible time"),
]


@pytest.mark.parametrize("value, kind", USABLE)
def test_a_published_date_is_usable(value, kind):
    assert observed_date_is_usable(value) is True, kind


@pytest.mark.parametrize("value, kind", UNUSABLE)
def test_text_that_is_not_a_date_is_not_usable(value, kind):
    assert observed_date_is_usable(value) is False, kind


def test_the_boundary_comes_from_the_public_package_surface():
    """No private resolver is reachable through this module."""
    import app.core.published_temporal_value as module

    assert module.resolve_temporal_value.__module__.startswith("app.core.truth_layer")
    assert not hasattr(module, "parse_iso_text")


@pytest.mark.parametrize("value, expected", [
    ("2026-08-01T00:00:00.000", True),
    ("2026-08-01", True),
    ("no es fecha", False),
    ("", False),
    (None, False),
    (12345, False),
])
def test_aprobado_enters_the_universe_only_with_a_usable_signature(value, expected):
    row = {"estado_contrato": "Aprobado", "fecha_de_firma": value}
    assert eligible(row) is expected


@pytest.mark.parametrize("state", ["En ejecución", "Cerrado", "Suspendido"])
def test_an_admitted_historical_state_needs_no_date_at_all(state):
    assert eligible({"estado_contrato": state, "fecha_de_firma": "no es fecha"}) is True


@pytest.mark.parametrize("value, shown", [
    ("2026-08-01T00:00:00.000", True),
    ("no es fecha", False),
])
def test_the_formatter_shows_a_contract_date_only_when_it_is_one(value, shown):
    from app.core.formatter import Formatter

    rows = Formatter().to_rows(
        [{"id_contrato": "CO1.PCCNTR.1", "nombre_entidad": "X",
          "objeto_del_contrato": "OBRA", "valor_del_contrato": "100",
          "fecha_de_firma": value}],
        "jbjy-vk9h",
    )
    assert (rows[0]["fecha"] == value) is shown
    if not shown:
        assert rows[0]["fecha"] == ""
        assert rows[0]["fecha_label"] == "Fecha de firma no publicada"

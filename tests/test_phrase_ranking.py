"""Commit 6B: ranking de candidatos por validación de frase."""
from __future__ import annotations

from app.core.phrase_catalog import PHRASE_CATALOG
from app.core.phrase_ranking import rank_by_phrase

SPEC = PHRASE_CATALOG[0]  # manual de contratacion
DS = "jbjy-vk9h"


def _row(objeto="", descripcion=""):
    return {"objeto_del_contrato": objeto, "descripcion_del_proceso": descripcion, "_o": objeto}


def test_invariant_counts_sum_to_total():
    rows = [
        _row("manual de contratacion"),                       # direct
        _row("manual interno de contratacion"),               # direct (gap 2)
        _row("manual de funciones y procesos de contratacion"),  # weak
        _row("pavimentacion de vias"),                        # none
    ]
    ranked, counts = rank_by_phrase(rows, [SPEC], DS)
    assert counts["direct"] + counts["probable"] + counts["weak"] + counts["none"] == len(rows)
    assert len(ranked) == counts["direct"] + counts["probable"] + counts["weak"]
    assert counts["none"] == 1  # la de pavimentacion se descarta


def test_order_direct_before_weak_then_stable():
    rows = [
        _row("manual de funciones para procesos de contratacion"),  # weak (idx 0)
        _row("manual de contratacion municipal"),                   # direct (idx 1)
        _row("actualizacion del manual de contratacion"),           # direct (idx 2)
    ]
    ranked, _ = rank_by_phrase(rows, [SPEC], DS)
    objs = [r["_o"] for r in ranked]
    # los dos direct primero, en su orden original; el weak al final
    assert objs[0].startswith("manual de contratacion")
    assert objs[1].startswith("actualizacion")
    assert "funciones" in objs[2]


def test_none_dropped_but_all_none_returns_empty():
    rows = [_row("pavimentacion"), _row("alumbrado publico")]
    ranked, counts = rank_by_phrase(rows, [SPEC], DS)
    assert ranked == []
    assert counts["none"] == 2  # el caller (rendering) aplica el fallback de no-vaciar

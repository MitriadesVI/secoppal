from __future__ import annotations

from decimal import Decimal
import random

from app.core.notify.grouping import (
    SelectableRadarEvent,
    group_events,
    sort_grouped_events,
)


def _event(seq, *, key=None, kind="new", value=None, **over):
    data = dict(
        event_id=f"ev-{seq}", event_seq=seq, kind=kind, result_key=key,
        title=f"Título {seq}", entity="Entidad", value=value, status=None,
        prev_status=None, prev_value=None, urlproceso=None, referencia=None,
    )
    data.update(over)
    return SelectableRadarEvent(**data)


def test_same_result_key_collapses_and_preserves_trace_and_edges():
    events = [
        _event(1, key="K", kind="changed_state", status="Publicado",
               prev_status="Borrador", prev_value="10", value="20"),
        _event(2, key="K", kind="changed_state", status="Adjudicado",
               prev_status="Publicado", prev_value="20", value="30",
               title="Título final", urlproceso="https://secop/K"),
        _event(3, key="K", kind="changed_value", status="Adjudicado",
               prev_status="Adjudicado", prev_value="30", value="40"),
    ]
    group = group_events(reversed(events))[0]
    assert group.prev_status == "Borrador"
    assert group.status == "Adjudicado"
    assert group.prev_value == "10"
    assert group.value == "40"
    assert group.title == "Título 3"
    assert group.urlproceso == "https://secop/K"
    assert group.event_ids == ("ev-1", "ev-2", "ev-3")
    assert group.event_seqs == (1, 2, 3)


def test_distinct_and_missing_result_keys_do_not_merge():
    groups = group_events([
        _event(1, key="A"), _event(2, key="B"),
        _event(3, key=None), _event(4, key=""),
    ])
    assert len(groups) == 4
    assert {g.event_ids for g in groups} == {
        ("ev-1",), ("ev-2",), ("ev-3",), ("ev-4",),
    }


def test_missing_key_cannot_collide_with_a_real_result_key():
    groups = group_events([
        _event(1, key=None, event_id="abc"),
        _event(2, key="event:abc", event_id="def"),
    ])
    assert len(groups) == 2


def test_numeric_value_separator_policy():
    from app.core.notify.grouping import numeric_value

    assert numeric_value("$ 3.000") == Decimal("3000")
    assert numeric_value("3,000") == Decimal("3000")
    assert numeric_value("3.50") == Decimal("3.50")
    assert numeric_value("3,50") == Decimal("3.50")
    assert numeric_value("1.234.567,89") == Decimal("1234567.89")
    assert numeric_value("1,234,567.89") == Decimal("1234567.89")


def test_order_is_kind_then_numeric_value_desc_then_min_seq():
    events = [
        _event(9, key="d", kind="disappeared", value="999999"),
        _event(8, key="bad", kind="new", value="sin valor"),
        _event(7, key="v", kind="changed_value", value="999999"),
        _event(5, key="n-low", kind="new", value="$ 2.000"),
        _event(4, key="n-high", kind="new", value="$ 3.000"),
        _event(3, key="s", kind="changed_state", value="999999"),
        _event(2, key="n-high-tie", kind="new", value="3000"),
    ]
    assert [g.result_key for g in group_events(events)] == [
        "n-high-tie", "n-high", "n-low", "bad", "s", "v", "d",
    ]


def test_invalid_value_never_raises_and_original_is_preserved():
    group = group_events([_event(1, key="x", value="COP no disponible")])[0]
    assert group.value == "COP no disponible"


def test_event_seq_is_the_final_tiebreak_even_if_groups_arrive_reversed():
    groups = group_events([
        _event(1, key="first", kind="new", value="100"),
        _event(2, key="second", kind="new", value="100"),
    ])
    assert [group.representative_seq for group in sort_grouped_events(reversed(groups))] == [1, 2]


def test_randomized_input_is_identical_one_hundred_times():
    base = [
        _event(i, key=f"K-{i // 3}", kind=("new", "changed_state", "changed_value")[i % 3],
               value=str(i * 100), status=f"S-{i}")
        for i in range(1, 31)
    ]
    expected = group_events(base)
    rng = random.Random(20260715)
    for _ in range(100):
        shuffled = base[:]
        rng.shuffle(shuffled)
        assert group_events(shuffled) == expected

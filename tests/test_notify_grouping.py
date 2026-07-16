from __future__ import annotations

from decimal import Decimal
import random

import pytest

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


def test_frozen_metadata_reaches_the_group_from_the_last_event():
    group = group_events([
        _event(1, key="K", dataset="procesos", modality="Selección abreviada",
               closing_date="2026-08-01T17:00:00", contractor=None,
               signature_date=None, observed_at="2026-07-15T10:00:00+00:00"),
        _event(2, key="K", kind="changed_value", dataset="procesos",
               modality="Licitación pública", closing_date="2026-08-20T17:00:00",
               contractor="UT Alimentar 2026", signature_date="2026-03-11",
               observed_at="2026-07-16T15:30:00+00:00"),
    ])[0]
    assert group.dataset == "procesos"
    assert group.modality == "Licitación pública"
    assert group.closing_date == "2026-08-20T17:00:00"
    assert group.contractor == "UT Alimentar 2026"
    assert group.signature_date == "2026-03-11"
    assert group.observed_at == "2026-07-16T15:30:00+00:00"


def test_metadata_absent_in_last_event_is_not_refilled_from_an_older_one():
    group = group_events([
        _event(1, key="K", dataset="procesos", modality="Licitación pública",
               closing_date="2026-08-20T17:00:00", contractor="UT Vieja",
               signature_date="2026-03-11", observed_at="2026-07-15T10:00:00+00:00"),
        _event(2, key="K", kind="changed_state", dataset="procesos",
               modality=None, closing_date=None, contractor=None,
               signature_date=None, observed_at="2026-07-16T10:00:00+00:00"),
    ])[0]
    # La fuente dejó de reportarlos: resucitar el dato viejo afirmaría que
    # todavía los reporta, y esa fecha es la que alguien usaría para decidir.
    assert group.closing_date is None
    assert group.modality is None
    assert group.contractor is None
    assert group.signature_date is None
    assert group.observed_at == "2026-07-16T10:00:00+00:00"


def test_normalize_dataset_maps_socrata_ids_to_the_human_alias():
    from app.core.notify.grouping import normalize_dataset

    assert normalize_dataset("p6dx-8zbt") == normalize_dataset("procesos") == "procesos"
    assert normalize_dataset("JBJY-VK9H") == normalize_dataset(" Contratos ") == "contratos"
    assert normalize_dataset("otro_dataset") == "otro_dataset"
    assert normalize_dataset(None) == normalize_dataset("") == ""


@pytest.mark.parametrize("alias,canonical", [
    ("p6dx-8zbt", "procesos"),
    ("jbjy-vk9h", "contratos"),
])
def test_dataset_aliases_are_one_identity_not_two(alias, canonical):
    groups = group_events([
        _event(1, key="SAME", dataset=canonical, status="Publicado"),
        _event(2, key="SAME", dataset=alias, kind="changed_state", status="Adjudicado"),
    ])
    # El id Socrata y el alias humano son el mismo dataset: partirlos mostraría
    # el mismo resultado dos veces, y el id crudo saldría impreso en el mensaje.
    assert len(groups) == 1
    assert groups[0].dataset == canonical
    assert groups[0].event_ids == ("ev-1", "ev-2")
    assert groups[0].status == "Adjudicado"


def test_absent_dataset_stays_none_after_normalization():
    assert group_events([_event(1, key="K", dataset=None)])[0].dataset is None


def test_same_result_key_in_different_datasets_never_collapses():
    groups = group_events([
        _event(1, key="SAME", dataset="procesos", status="Publicado"),
        _event(2, key="SAME", dataset="contratos", status="Firmado",
               contractor="UT Alimentar 2026"),
    ])
    assert len(groups) == 2
    assert {g.dataset for g in groups} == {"procesos", "contratos"}
    assert {g.event_ids for g in groups} == {("ev-1",), ("ev-2",)}


def test_every_event_id_and_seq_is_represented_across_datasets():
    events = [
        _event(1, key="SAME", dataset="procesos"),
        _event(2, key="SAME", dataset="procesos"),
        _event(3, key="SAME", dataset="contratos"),
    ]
    groups = group_events(events)
    assert sorted(i for g in groups for i in g.event_ids) == ["ev-1", "ev-2", "ev-3"]
    assert sorted(s for g in groups for s in g.event_seqs) == [1, 2, 3]


def test_randomized_input_with_datasets_is_identical_one_hundred_times():
    base = [
        _event(i, key=f"K-{i // 3}", dataset=("procesos", "contratos")[i % 2],
               kind=("new", "changed_state", "changed_value")[i % 3],
               value=str(i * 100), modality=f"M-{i}", closing_date=f"2026-08-{i:02d}",
               observed_at=f"2026-07-16T10:{i:02d}:00+00:00")
        for i in range(1, 31)
    ]
    expected = group_events(base)
    rng = random.Random(20260716)
    for _ in range(100):
        shuffled = base[:]
        rng.shuffle(shuffled)
        assert group_events(shuffled) == expected


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

from __future__ import annotations

import json
import random

from app.core.notify.grouping import SelectableRadarEvent, group_events
from app.core.notify.render import build_payload, render_payload, serialize_payload


def _groups(n=3, *, missing=()):
    return group_events([
        SelectableRadarEvent(
            event_id=f"e{i}", event_seq=i, kind="new", result_key=f"K{i}",
            title=f"Proceso <{i}> & especial", entity="Entidad", value=str(i * 100),
            status="Publicado", urlproceso=None if i in missing else f"https://x/{i}",
        )
        for i in range(1, n + 1)
    ])


def _payload(groups, *, max_items=3, channel="telegram"):
    return build_payload(
        groups, principal_id="p", endpoint_id="e", radar_id="r",
        radar_name="Radar PAE", channel=channel,
        created_at="2026-07-15T12:00:00+00:00", max_items=max_items,
    )


def test_max_items_limits_render_not_frozen_items():
    payload = _payload(_groups(10), max_items=3)
    rendered = render_payload(payload, channel="telegram")
    assert len(payload["items"]) == payload["total_items"] == 10
    assert payload["rendered_items"] == rendered.item_count == 3
    assert payload["truncated_count"] == rendered.truncated_count == 7
    assert "Y 7 más" in rendered.text
    assert rendered.text.count("Radar:") == 1


def test_missing_link_keeps_item_and_counts_all_frozen_items():
    payload = _payload(_groups(4, missing={1, 4}), max_items=2)
    rendered = render_payload(payload, channel="whatsapp")
    assert payload["total_items"] == 4
    assert payload["links_missing"] == rendered.links_missing == 2
    assert any(item["result_key"] == "K1" for item in payload["items"])


def test_payload_has_data_and_no_recipient_or_secret_fields():
    payload = _payload(_groups())
    assert payload["version"] == "p1b-payload-v1"
    assert payload["items"][0]["event_ids"]
    serialized = serialize_payload(payload).lower()
    for forbidden in ("external_recipient", "claim_token", "address_hash", "email", "hmac"):
        assert forbidden not in serialized


def test_serialization_and_render_are_byte_deterministic_for_all_channels():
    groups = _groups(8, missing={2})
    for channel in ("telegram", "whatsapp", "email"):
        payload = _payload(groups, max_items=5, channel=channel)
        first = (serialize_payload(payload), render_payload(payload, channel=channel))
        for _ in range(20):
            assert (serialize_payload(payload), render_payload(payload, channel=channel)) == first
        assert json.loads(first[0]) == payload


def test_shuffled_input_produces_same_payload_and_special_characters_are_plain():
    events = [
        SelectableRadarEvent(event_id=f"e{i}", event_seq=i, kind="new",
                             result_key=f"K{i}", title="*[x] <b> & salto\n línea")
        for i in range(1, 11)
    ]
    expected = serialize_payload(_payload(group_events(events)))
    rng = random.Random(7)
    for _ in range(100):
        rng.shuffle(events)
        assert serialize_payload(_payload(group_events(events))) == expected
    text = render_payload(_payload(group_events(events))).text
    assert "salto línea" in text

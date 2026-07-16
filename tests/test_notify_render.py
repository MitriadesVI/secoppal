from __future__ import annotations

import json
import random

import pytest

import app.core.notify.render as render_module
from app.core.notify.grouping import SelectableRadarEvent, group_events
from app.core.notify.render import (
    CHANNEL_BYTE_BUDGETS,
    PAYLOAD_VERSION,
    PAYLOAD_VERSION_V1,
    PAYLOAD_VERSION_V2,
    build_payload,
    render_payload,
    serialize_payload,
    truncate_field,
)


URL_LABEL = "Ver en SECOP: "


def _groups(n=3, *, missing=(), **over):
    return group_events([
        SelectableRadarEvent(
            event_id=f"e{i}", event_seq=i, kind="new", result_key=f"K{i}",
            dataset="procesos", title=f"Proceso <{i}> & especial", entity="Entidad",
            value=str(i * 100), status="Publicado",
            observed_at="2026-07-16T15:30:00+00:00",
            urlproceso=None if i in missing else f"https://x/{i}",
            **over,
        )
        for i in range(1, n + 1)
    ])


def _payload(groups, *, max_items=3, channel="telegram", timezone="America/Bogota"):
    return build_payload(
        groups, principal_id="p", endpoint_id="e", radar_id="r",
        radar_name="Radar PAE", channel=channel, timezone=timezone,
        created_at="2026-07-15T12:00:00+00:00", max_items=max_items,
    )


def _one(**over):
    """Un item v2 suelto, sin pasar por build_payload."""
    item = {
        "dataset": "procesos", "result_key": "K1", "kind": "new",
        "title": "Alimentación escolar", "entity": "Alcaldía de Ejemplo",
        "value": "1350000000", "status": "Publicado",
        "prev_status": None, "prev_value": None,
        "modality": "Licitación pública", "closing_date": None,
        "contractor": None, "signature_date": None,
        "observed_at": "2026-07-16T15:30:00+00:00",
        "urlproceso": "https://secop/K1", "referencia": "REF-1",
        "event_ids": ["e1"], "event_seqs": [1],
    }
    item.update(over)
    return item


def _v2(items, *, channel="telegram", timezone="America/Bogota", max_items=30):
    return {
        "version": PAYLOAD_VERSION_V2, "principal_id": "p", "endpoint_id": "e",
        "radar_id": "r", "radar_name": "Radar PAE", "channel": channel,
        "timezone": timezone, "created_at": "2026-07-15T12:00:00+00:00",
        "max_items": max_items, "items": items, "total_items": len(items),
        "links_missing": sum(1 for i in items if not i.get("urlproceso")),
    }


# ---------------------------------------------------------------------------
# Compatibilidad v1 — golden literal capturado ANTES de este bloque.
# ---------------------------------------------------------------------------

V1_PAYLOAD = {
    "version": "p1b-payload-v1",
    "principal_id": "pr-1", "endpoint_id": "ep-1", "radar_id": "rd-1",
    "radar_name": "Radar PAE", "channel": "telegram",
    "created_at": "2026-07-15T12:00:00+00:00",
    "items": [
        {"result_key": "K1", "kind": "new", "title": "Alimentación escolar",
         "entity": "Alcaldía de Ejemplo", "value": "1350000000", "status": "Publicado",
         "prev_status": None, "prev_value": None, "urlproceso": "https://secop/K1",
         "referencia": "REF-1", "event_ids": ["e1"], "event_seqs": [1]},
        {"result_key": "K2", "kind": "changed_state", "title": "Obra pública",
         "entity": "Gobernación", "value": "900000000", "status": "Adjudicado",
         "prev_status": "Publicado", "prev_value": "900000000", "urlproceso": None,
         "referencia": "REF-2", "event_ids": ["e2"], "event_seqs": [2]},
        {"result_key": "K3", "kind": "changed_value", "title": "Suministro",
         "entity": "Municipio", "value": "500000000", "status": "Publicado",
         "prev_status": "Publicado", "prev_value": "400000000",
         "urlproceso": "https://secop/K3", "referencia": None,
         "event_ids": ["e3"], "event_seqs": [3]},
    ],
    "total_items": 3, "rendered_items": 2, "truncated_count": 1, "links_missing": 1,
}

# Literal, no reconstruido con las funciones bajo prueba.
V1_GOLDEN = (
    "Radar: Radar PAE\n"
    "\n"
    "• Nuevo: Alimentación escolar — Publicado · 1350000000 · https://secop/K1\n"
    "• Cambio de estado: Obra pública — Publicado → Adjudicado · 900000000\n"
    "• Y 1 más"
)


def test_v1_payload_still_renders_byte_for_byte():
    rendered = render_payload(V1_PAYLOAD, channel="telegram")
    assert rendered.text == V1_GOLDEN
    assert (rendered.item_count, rendered.truncated_count, rendered.links_missing) == (2, 1, 1)
    assert rendered.budget_bytes is None


def test_bumping_the_current_version_does_not_reinterpret_v1_as_v2(monkeypatch):
    monkeypatch.setattr(render_module, "PAYLOAD_VERSION", PAYLOAD_VERSION_V2)
    assert render_payload(V1_PAYLOAD, channel="telegram").text == V1_GOLDEN


def test_external_channel_cannot_replace_the_frozen_channel():
    # Sustituir telegram por email daría 12.000 bytes de presupuesto a un payload
    # congelado para 3.800: el argumento estaría reescribiendo un hecho persistido.
    with pytest.raises(ValueError):
        render_payload(V1_PAYLOAD, channel="email")
    with pytest.raises(ValueError):
        render_payload(_v2([_one()], channel="telegram"), channel="email")


def test_payload_without_channel_is_not_completed_by_the_argument():
    for payload in (dict(V1_PAYLOAD), _v2([_one()])):
        payload.pop("channel")
        with pytest.raises(ValueError):
            render_payload(payload, channel="telegram")


def test_matching_channel_argument_is_accepted():
    assert render_payload(V1_PAYLOAD, channel="telegram").text == V1_GOLDEN
    assert render_payload(_v2([_one()]), channel="telegram").item_count == 1


def test_v2_payload_uses_the_new_renderer():
    rendered = render_payload(_v2([_one()]))
    assert rendered.text.startswith("🔔 Radar: Radar PAE")
    assert "🆕 Nueva coincidencia" in rendered.text
    assert rendered.budget_bytes == CHANNEL_BYTE_BUDGETS["telegram"]


@pytest.mark.parametrize("version", ["p1b-payload-v3", "", "v1", None, 2])
def test_unknown_or_missing_version_fails_closed(version):
    payload = _v2([_one()])
    if version is None:
        payload.pop("version")
    else:
        payload["version"] = version
    with pytest.raises(ValueError):
        render_payload(payload)


# ---------------------------------------------------------------------------
# Contrato del payload v2
# ---------------------------------------------------------------------------

def test_build_payload_produces_v2_with_frozen_metadata():
    payload = _payload(_groups(1, modality="Licitación pública",
                               closing_date="2026-08-20T17:00:00",
                               contractor="UT Alimentar", signature_date="2026-03-11"))
    assert payload["version"] == PAYLOAD_VERSION_V2 == PAYLOAD_VERSION
    item = payload["items"][0]
    assert item["dataset"] == "procesos"
    assert item["observed_at"] == "2026-07-16T15:30:00+00:00"
    assert item["modality"] == "Licitación pública"
    assert item["closing_date"] == "2026-08-20T17:00:00"
    assert item["contractor"] == "UT Alimentar"
    assert item["signature_date"] == "2026-03-11"
    assert payload["timezone"] == "America/Bogota"


def test_max_items_is_frozen_but_every_item_stays_in_the_payload():
    payload = _payload(_groups(10), max_items=3)
    assert payload["max_items"] == 3
    assert len(payload["items"]) == payload["total_items"] == 10
    # Lo que se ve depende también del presupuesto: no se congela como hecho.
    assert "rendered_items" not in payload and "truncated_count" not in payload
    assert render_payload(payload).item_count <= 3


def test_payload_has_no_recipient_or_secret_fields():
    serialized = serialize_payload(_payload(_groups())).lower()
    for forbidden in ("external_recipient", "claim_token", "address_hash",
                      "email", "hmac", "telefono", "idempotency"):
        assert forbidden not in serialized


def test_serialization_is_byte_deterministic_one_hundred_times():
    payload = _payload(_groups(8, missing={2}), max_items=5)
    expected = serialize_payload(payload)
    for _ in range(100):
        assert serialize_payload(payload) == expected
    assert json.loads(expected) == payload


def test_shuffled_input_produces_the_same_payload():
    events = [
        SelectableRadarEvent(event_id=f"e{i}", event_seq=i, kind="new", result_key=f"K{i}",
                             dataset="procesos", title="*[x] <b> & salto\n línea")
        for i in range(1, 11)
    ]
    expected = serialize_payload(_payload(group_events(events)))
    rng = random.Random(7)
    for _ in range(100):
        rng.shuffle(events)
        assert serialize_payload(_payload(group_events(events))) == expected


# ---------------------------------------------------------------------------
# Lenguaje factual
# ---------------------------------------------------------------------------

FORBIDDEN_CLAIMS = (
    "fecha de cierre", "vence", "vencimiento", "cierra el", "vigente",
    "accionable", "puedes presentarte", "faltan", "oportunidad abierta",
    "cierre confirmado", "contratista definitivo", "adjudicación confirmada",
)


def _assert_factual(text):
    lowered = text.casefold()
    for claim in FORBIDDEN_CLAIMS:
        assert claim not in lowered, f"lenguaje no factual: {claim!r}"


def test_process_with_closing_reports_reception_never_a_deadline():
    text = render_payload(_v2([_one(closing_date="2026-08-20T17:00:00")])).text
    assert "Recepción de ofertas reportada: 20 ago 2026, 17:00" in text
    _assert_factual(text)


def test_contract_reports_contractor_and_signature():
    text = render_payload(_v2([_one(
        dataset="contratos", kind="changed_value", title="Objeto del contrato",
        prev_value="900000000", value="1350000000",
        contractor="UNION TEMPORAL ALIMENTAR 2026", signature_date="2026-03-11",
        modality=None, closing_date=None,
    )])).text
    assert "💰 Cambio de valor — Contrato" in text
    assert "Contratista reportado: UNION TEMPORAL ALIMENTAR 2026" in text
    assert "Firma reportada: 11 mar 2026" in text
    assert "Recepción de ofertas reportada" not in text
    _assert_factual(text)


def test_absent_fields_are_omitted_not_rendered_as_placeholders():
    text = render_payload(_v2([_one(
        entity=None, modality=None, closing_date=None, contractor=None,
        signature_date=None, referencia=None, status=None, value=None,
    )])).text
    for placeholder in ("None", "N/A", "No disponible", "null"):
        assert placeholder not in text
    for label in ("Entidad:", "Modalidad:", "Contratista reportado:",
                  "Firma reportada:", "Estado reportado:", "Valor reportado:"):
        assert label not in text


def test_changed_state_and_changed_value_show_the_transition():
    state = render_payload(_v2([_one(
        kind="changed_state", prev_status="Publicado", status="Adjudicado")])).text
    assert "Estado reportado: Publicado → Adjudicado" in state
    value = render_payload(_v2([_one(
        kind="changed_value", prev_value="900000000", value="1350000000")])).text
    assert "Valor reportado: 900000000 → 1350000000" in value


def test_observed_at_is_converted_to_the_frozen_timezone():
    text = render_payload(_v2([_one(observed_at="2026-07-16T15:30:00+00:00")])).text
    assert "Observado: 16 jul 2026, 10:30" in text


def test_invalid_or_missing_timezone_fails_closed():
    for bad in ("Marte/Olympus", "", None):
        payload = _v2([_one()], timezone=bad)
        with pytest.raises(ValueError):
            render_payload(payload)


def test_naive_source_date_is_not_shifted_nor_labelled_utc():
    # 17:00 naive no es 17:00Z: convertirla mostraría las 12:00 e inventaría un
    # hecho que la fuente no reportó.
    text = render_payload(_v2([_one(closing_date="2026-08-20T17:00:00")])).text
    assert "17:00" in text and "12:00" not in text
    assert "UTC" not in text and "+00:00" not in text


def test_unparseable_source_date_is_shown_as_clean_text():
    text = render_payload(_v2([_one(closing_date="por definir")])).text
    assert "Recepción de ofertas reportada: por definir" in text


# ---------------------------------------------------------------------------
# Presupuesto interno por canal
# ---------------------------------------------------------------------------

def _heavy(n=30, *, url_len=60):
    return [
        _one(
            result_key=f"K{i}", title=f"Ítem {i} — " + "Alimentación escolar ñandú 🍎 " * 20,
            entity="Alcaldía Metropolitana del Atlántico ñ 🏛 " * 3,
            modality="Licitación pública nacional 🇨🇴 " * 3,
            contractor="UNIÓN TEMPORAL ALIMENTAR ñ 🍽 " * 4,
            closing_date="2026-08-20T17:00:00", signature_date="2026-03-11",
            prev_status="Publicado", status="Adjudicado",
            prev_value="900000000", value="1350000000",
            referencia=f"REF-{i}-ÑÑÑ", urlproceso="https://secop/" + "a" * url_len + f"/{i}",
        )
        for i in range(1, n + 1)
    ]


@pytest.mark.parametrize("channel", ["telegram", "whatsapp", "email"])
def test_thirty_heavy_multibyte_items_never_exceed_the_channel_budget(channel):
    payload = _v2(_heavy(), channel=channel, max_items=30)
    rendered = render_payload(payload)
    budget = CHANNEL_BYTE_BUDGETS[channel]
    assert len(rendered.text.encode("utf-8")) <= budget
    assert rendered.byte_count == len(rendered.text.encode("utf-8")) <= budget
    assert rendered.budget_bytes == budget
    assert rendered.item_count + rendered.truncated_count == payload["total_items"] == 30
    assert rendered.item_count >= 1


def test_budget_is_measured_in_utf8_bytes_not_characters():
    payload = _v2(_heavy(), channel="whatsapp", max_items=30)
    rendered = render_payload(payload)
    # Si se midiera por caracteres el mensaje pasaría: hay más bytes que letras.
    assert len(rendered.text) < len(rendered.text.encode("utf-8"))
    assert len(rendered.text.encode("utf-8")) <= 3800


def test_truncation_summary_reports_everything_not_shown():
    rendered = render_payload(_v2(_heavy(), max_items=30))
    assert f"Y {rendered.truncated_count} novedades más no mostradas" in rendered.text
    assert rendered.item_count + rendered.truncated_count == 30


def test_no_url_is_ever_cut_in_half():
    payload = _v2(_heavy(url_len=200), max_items=30)
    rendered = render_payload(payload)
    urls = {item["urlproceso"] for item in payload["items"]}
    shown = [
        line[len(URL_LABEL):] for line in rendered.text.splitlines()
        if line.startswith(URL_LABEL)
    ]
    assert all(url in urls for url in shown)


def test_url_that_does_not_fit_is_omitted_whole_and_counted():
    huge = "https://secop.gov.co/" + "a" * 4200
    rendered = render_payload(_v2([_one(urlproceso=huge)], channel="telegram"))
    assert rendered.links_omitted_due_to_budget == 1
    assert rendered.links_missing == 0
    assert URL_LABEL not in rendered.text
    assert "https://secop.gov.co/aaa" not in rendered.text
    # El item sigue mostrándose, sin su enlace.
    assert rendered.item_count == 1
    assert "Alimentación escolar" in rendered.text
    assert rendered.byte_count <= CHANNEL_BYTE_BUDGETS["telegram"]


def test_urls_of_items_hidden_by_budget_are_counted_as_omitted():
    payload = _v2(_heavy(30), max_items=30)
    rendered = render_payload(payload)
    # Todos los items traen URL: las de los items que no caben existen y no se
    # ven por espacio, igual que la URL sacrificada dentro de un item visible.
    assert rendered.truncated_count > 0
    assert rendered.links_omitted_due_to_budget >= rendered.truncated_count


def test_items_hidden_by_max_items_are_not_counted_as_budget_omissions():
    rendered = render_payload(_v2(_heavy(4), max_items=1))
    assert (rendered.item_count, rendered.truncated_count) == (1, 3)
    # Los otros tres se omiten por preferencia, no por presupuesto.
    assert rendered.links_omitted_due_to_budget == 0


def test_a_fifty_thousand_character_radar_name_stays_within_budget():
    payload = _v2([_one()])
    payload["radar_name"] = "Ñ🍎" * 25_000
    rendered = render_payload(payload)
    assert rendered.byte_count <= CHANNEL_BYTE_BUDGETS["telegram"]
    assert rendered.text.splitlines()[0].endswith("…")
    assert rendered.item_count == 1


def test_missing_link_counts_as_links_missing_not_as_budget_omission():
    rendered = render_payload(_v2([_one(urlproceso=None), _one(result_key="K2")]))
    assert rendered.links_missing == 1
    assert rendered.links_omitted_due_to_budget == 0


def test_a_single_fifty_thousand_character_title_stays_within_budget():
    rendered = render_payload(_v2([_one(title="Ñ🍎" * 25_000)]))
    assert rendered.byte_count <= CHANNEL_BYTE_BUDGETS["telegram"]
    assert rendered.item_count == 1
    assert "…" in rendered.text


def test_truncate_field_cuts_whole_characters_with_ellipsis():
    assert truncate_field("abc", 10) == "abc"
    assert truncate_field("🍎🍎🍎🍎", 3) == "🍎🍎…"
    assert len(truncate_field("x" * 500, 220)) == 220
    with pytest.raises(ValueError):
        truncate_field("x", 0)


@pytest.mark.parametrize("channel", ["telegram", "whatsapp", "email"])
def test_render_is_byte_identical_one_hundred_times(channel):
    payload = _v2(_heavy(), channel=channel, max_items=30)
    expected = render_payload(payload)
    for _ in range(100):
        assert render_payload(payload) == expected


def test_render_always_produces_exactly_one_message():
    rendered = render_payload(_v2(_heavy(), max_items=30))
    assert rendered.text.count("🔔 Radar:") == 1
    assert rendered.text.count("novedades más no mostradas") == 1

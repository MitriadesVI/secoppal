from __future__ import annotations

import json
import random

import pytest

import app.core.notify.render as render_module
from app.core.notify.grouping import SelectableRadarEvent, group_events
from app.core.notify.idempotency import idempotency_key
from app.core.notify.render import (
    CHANNEL_BYTE_BUDGETS,
    PAYLOAD_VERSION,
    PAYLOAD_VERSION_V1,
    PAYLOAD_VERSION_V2,
    PAYLOAD_VERSION_V3,
    build_object_excerpt,
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


def _v3(items, *, channel="telegram", timezone="America/Bogota", max_items=30):
    """Payload v3 armado a mano (sin pasar por build_payload) — para probar
    _render_v3 directamente, incluidos casos límite que build_payload ya
    normalizaría (p. ej. object_excerpt sin truncar)."""
    return {
        "version": PAYLOAD_VERSION_V3, "principal_id": "p", "endpoint_id": "e",
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


# ---------------------------------------------------------------------------
# Compatibilidad v2 — golden literal capturado EJECUTANDO la implementación
# original de HEAD 5883aa8 (antes de TG-P2C), NO calculado con el renderer
# nuevo. v2 es un contrato congelado exactamente igual que v1: TG-P2C agrega
# capacidad en v3, nunca modifica v2. Nótese el "object_excerpt": null en el
# item — v2 lo ignora por completo aunque la clave esté presente (grouping.py
# ya la agrega a TODO item vía as_payload_item, incluida esta corrida golden).
# ---------------------------------------------------------------------------

V2_PAYLOAD = {
    "version": "p1b-payload-v2",
    "principal_id": "p", "endpoint_id": "e", "radar_id": "r",
    "radar_name": "Radar PAE", "channel": "telegram", "timezone": "America/Bogota",
    "created_at": "2026-07-15T12:00:00+00:00", "max_items": 5,
    "items": [{
        "dataset": "procesos", "result_key": "K1", "kind": "new",
        "title": "MR- SASIP-002-2026 (Presentación de oferta)",
        "entity": "ALCALDÍA MUNICIPAL DE RÁQUIRA", "value": "186600000.0",
        "status": "Abierto", "prev_status": None, "prev_value": None,
        "modality": "Selección abreviada subasta inversa",
        "closing_date": "2026-07-24T00:00:00.000", "contractor": None,
        "signature_date": None, "object_excerpt": None,
        "observed_at": "2026-07-16T15:30:00+00:00",
        "urlproceso": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.10541141",
        "referencia": "MR- SASIP-002-2026 (Presentación de oferta)",
        "event_ids": ["e1"], "event_seqs": [1],
    }],
    "total_items": 1, "links_missing": 0,
}

# Literal, capturado ejecutando render_head.render_payload() (git show
# 5883aa8:app/core/notify/render.py) sobre V2_PAYLOAD. Nótese: valor CRUDO sin
# formatear (186600000.0) y Referencia repetida (mismo código que el título) —
# ese es el comportamiento HISTÓRICO de v2, y sigue siéndolo.
V2_GOLDEN = (
    "🔔 Radar: Radar PAE\n"
    "1 novedad\n"
    "\n"
    "🆕 Nueva coincidencia — Proceso\n"
    "MR- SASIP-002-2026 (Presentación de oferta)\n"
    "Entidad: ALCALDÍA MUNICIPAL DE RÁQUIRA\n"
    "Referencia: MR- SASIP-002-2026 (Presentación de oferta)\n"
    "Modalidad: Selección abreviada subasta inversa\n"
    "Valor reportado: 186600000.0\n"
    "Estado reportado: Abierto\n"
    "Recepción de ofertas reportada: 24 jul 2026, 00:00\n"
    "Observado: 16 jul 2026, 10:30\n"
    "Ver en SECOP: https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.10541141"
)


def test_v2_payload_still_renders_byte_for_byte_like_head():
    """v2 restaurado = byte-idéntico al comportamiento de HEAD 5883aa8: valor
    crudo sin formatear, referencia repetida, object_excerpt ignorado."""
    rendered = render_payload(V2_PAYLOAD)
    assert rendered.text == V2_GOLDEN
    assert rendered.byte_count == 509


def test_v2_ignores_object_excerpt_even_though_the_key_is_present():
    assert V2_PAYLOAD["items"][0]["object_excerpt"] is None  # grouping.py lo agrega siempre
    text = render_payload(dict(V2_PAYLOAD, items=[
        dict(V2_PAYLOAD["items"][0], object_excerpt="Esto NO debe aparecer")
    ])).text
    assert "Esto NO debe aparecer" not in text
    assert "Objeto:" not in text
    assert text == V2_GOLDEN  # ni siquiera cambia el byte_count/posición


def test_v2_still_shows_raw_money_and_duplicated_reference():
    """Confirmación explícita (además del golden) de los dos puntos que TG-P2C
    NO debía tocar en v2: dinero crudo y referencia duplicada."""
    assert "Valor reportado: 186600000.0" in V2_GOLDEN
    assert "186600000.0" in V2_GOLDEN  # no "$186,600,000"
    assert V2_GOLDEN.count("MR- SASIP-002-2026 (Presentación de oferta)") == 2  # título Y referencia


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


@pytest.mark.parametrize("version", ["p1b-payload-v4", "", "v1", None, 2])
def test_unknown_or_missing_version_fails_closed(version):
    payload = _v2([_one()])
    if version is None:
        payload.pop("version")
    else:
        payload["version"] = version
    with pytest.raises(ValueError):
        render_payload(payload)


# ---------------------------------------------------------------------------
# Contrato de build_payload — produce v3 (TG-P2C, FASE1 regla 4)
# ---------------------------------------------------------------------------

def test_build_payload_produces_v3_with_frozen_metadata():
    payload = _payload(_groups(1, modality="Licitación pública",
                               closing_date="2026-08-20T17:00:00",
                               contractor="UT Alimentar", signature_date="2026-03-11"))
    assert payload["version"] == PAYLOAD_VERSION_V3 == PAYLOAD_VERSION
    item = payload["items"][0]
    assert item["dataset"] == "procesos"
    assert item["observed_at"] == "2026-07-16T15:30:00+00:00"
    assert item["modality"] == "Licitación pública"
    assert item["closing_date"] == "2026-08-20T17:00:00"
    assert item["contractor"] == "UT Alimentar"
    assert item["signature_date"] == "2026-03-11"
    assert item["object_excerpt"] is None  # _groups() no lo provee
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
    # v2 restaurado: crudo, sin formatear (el formateo es exclusivo de v3).
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


# ---------------------------------------------------------------------------
# v3 (TG-P2C) — object_excerpt, dinero formateado con guardas, referencia
# deduplicada. TODO lo de aquí usa _v3()/build_payload, NUNCA _v2(): v2 se
# prueba arriba y queda intacto.
# ---------------------------------------------------------------------------

def test_v3_shows_an_objeto_line():
    """2 · payload v3 con object_excerpt muestra "Objeto: …"."""
    text = render_payload(_v3([_one(
        object_excerpt="Suministro de kits de apoyo nutricional para Adulto Mayor",
    )])).text
    assert "Objeto: Suministro de kits de apoyo nutricional para Adulto Mayor" in text


def test_object_excerpt_travels_from_event_to_v3_payload_json():
    """3 · raw evento -> SelectableRadarEvent -> GroupedRadarEvent -> payload_json
    v3 (build_payload real, no _v3() a mano)."""
    events = [SelectableRadarEvent(
        event_id="e1", event_seq=1, kind="new", result_key="K1", dataset="procesos",
        title="MR- SASIP-002-2026", object_excerpt="Programas de Adulto Mayor",
    )]
    payload = _payload(group_events(events))
    assert payload["version"] == PAYLOAD_VERSION_V3
    assert payload["items"][0]["object_excerpt"] == "Programas de Adulto Mayor"
    assert "Objeto: Programas de Adulto Mayor" in render_payload(payload).text


def test_build_payload_normalizes_object_excerpt_automatically_before_freezing():
    """FASE 3 · 'no depender de que cada caller recuerde invocarlo': un evento
    con una descripción CRUDA y larga (sin pasar por build_object_excerpt)
    llega ya acotada a <=180 caracteres dentro del payload congelado."""
    raw_description = (
        "SUMINISTRO DE KITS DE APOYO NUTRICIONAL PARA POBLACION VULNERABLE "
        "PROGRAMAS ADULTO MAYOR Y POBLACIÓN EN CONDICION DE DISCAPACIDAD DEL "
        "MUNICIPIO DE RAQUIRA BOYACA PARA LA VIGENCIA 2026 CON TEXTO ADICIONAL "
        "MUY LARGO QUE DEFINITIVAMENTE SUPERA LOS CIENTO OCHENTA CARACTERES"
    )
    assert len(raw_description) > 180
    events = [SelectableRadarEvent(
        event_id="e1", event_seq=1, kind="new", result_key="K1",
        object_excerpt=raw_description,
    )]
    payload = _payload(group_events(events))
    stored = payload["items"][0]["object_excerpt"]
    assert stored is not None
    assert len(stored) <= 180
    assert stored.endswith("…")
    assert stored == build_object_excerpt(raw_description)  # misma política, aplicada sola


def test_build_payload_reapplication_of_object_excerpt_is_idempotent():
    """FASE 1 · la fuente de verdad acotada es radar/keys.py; build_payload la
    reaplica DEFENSIVAMENTE. Sobre un extracto ya acotado no cambia nada."""
    already_bounded = build_object_excerpt("SUMINISTRO KITS ADULTO MAYOR " * 40)
    assert already_bounded is not None and len(already_bounded) <= 180
    events = [SelectableRadarEvent(
        event_id="e1", event_seq=1, kind="new", result_key="K1",
        object_excerpt=already_bounded,
    )]
    payload = _payload(group_events(events))
    assert payload["items"][0]["object_excerpt"] == already_bounded  # no re-truncado
    # Reaplicar el helper una vez más tampoco lo cambia.
    assert build_object_excerpt(already_bounded) == already_bounded


def test_render_reexports_the_single_object_excerpt_helper_no_divergent_copy():
    """El helper de render ES el de radar/object_excerpt.py — una sola
    implementación, no una segunda copia que pueda divergir."""
    from app.core.radar.object_excerpt import build_object_excerpt as radar_helper
    assert render_module.build_object_excerpt is radar_helper


def test_preview_and_rerender_of_the_same_v3_payload_are_byte_identical():
    """4 · preview y un re-render posterior del MISMO payload_json (ej. un
    reintento, o un futuro re-render) producen bytes idénticos."""
    payload = _v3([_one(object_excerpt="Suministro de kits de apoyo nutricional")])
    preview = render_payload(payload)
    rerender = render_payload(json.loads(serialize_payload(payload)))
    assert preview.text == rerender.text
    assert preview.byte_count == rerender.byte_count


def test_v3_numeric_value_uses_the_canonical_money_formatter():
    """5 · nunca 186600000.0 — usa format_cop."""
    text = render_payload(_v3([_one(value="186600000.0")])).text
    assert "Valor reportado: $186,600,000" in text
    assert "186600000.0" not in text


@pytest.mark.parametrize("bad_value", [
    None, "", "COP no disponible", "NaN", "Infinity", "0", "-100",
])
def test_v3_invalid_or_non_positive_value_omits_the_whole_line(bad_value):
    """FASE 2 · None, vacío, cero, negativo, NaN, infinito o texto no numérico:
    la línea "Valor reportado" se omite ENTERA — nunca texto crudo, nunca
    "Sin valor". ``float("nan")``/``float("inf")`` no lanzan en Python, así
    que sin la guarda explícita format_cop produciría "$nan"/"$inf"."""
    text = render_payload(_v3([_one(value=bad_value, prev_value=None)])).text
    assert "Valor reportado:" not in text
    assert "Sin valor" not in text


def test_v3_valid_positive_value_shows_formatted():
    """Complemento del parametrize anterior: el único caso que SÍ muestra la
    línea, con el formateador canónico."""
    text = render_payload(_v3([_one(value="186600000.0", prev_value=None)])).text
    assert "Valor reportado: $186,600,000" in text


def test_v3_value_transition_omitted_if_current_is_invalid_even_with_valid_prev():
    """Transición seguida solo si AMBOS extremos son representables: si el
    valor actual no es válido, no se muestra ni siquiera el previo."""
    text = render_payload(_v3([_one(
        kind="changed_value", prev_value="900000000", value="COP no disponible",
    )])).text
    assert "Valor reportado:" not in text


def test_v3_value_transition_shows_only_current_when_prev_is_invalid():
    """Un extremo previo inventado sería peor que no mostrar de dónde viene:
    si prev no es válido, se muestra solo el valor actual, sin "→"."""
    text = render_payload(_v3([_one(
        kind="changed_value", prev_value="NaN", value="186600000.0",
    )])).text
    value_line = next(line for line in text.splitlines() if line.startswith("Valor reportado:"))
    assert value_line == "Valor reportado: $186,600,000"
    assert "→" not in value_line


def test_v3_title_equal_reference_normalized_omits_referencia():
    """7 · strip + casefold + espacios normalizados, sin fuzzy matching."""
    text = render_payload(_v3([_one(
        title="MR- SASIP-002-2026 (Presentación de oferta)",
        referencia="  mr- sasip-002-2026   (presentación de oferta)  ",
    )])).text
    assert "Referencia:" not in text


def test_v3_title_different_from_reference_keeps_referencia():
    """8 · distintos de verdad: la referencia se conserva."""
    text = render_payload(_v3([_one(
        title="MR- SASIP-002-2026", referencia="REF-DISTINTA-9",
    )])).text
    assert "Referencia: REF-DISTINTA-9" in text


def test_object_excerpt_outranks_modality_value_and_referencia_in_v3_drop_order():
    """9a · invariante estructural: en ``_OPTIONAL_DROP_ORDER_V3``,
    object_excerpt aparece DESPUÉS (más protegido, cae más tarde) que
    modality/value/referencia. v2 (``_OPTIONAL_DROP_ORDER``, sin
    object_excerpt) no se toca."""
    order_v3 = render_module._OPTIONAL_DROP_ORDER_V3
    assert order_v3.index("object_excerpt") > order_v3.index("modality")
    assert order_v3.index("object_excerpt") > order_v3.index("value")
    assert order_v3.index("object_excerpt") > order_v3.index("referencia")
    assert "object_excerpt" not in render_module._OPTIONAL_DROP_ORDER


def test_object_excerpt_survives_budget_pressure_that_drops_modality_and_referencia():
    """9b · bajo presupuesto Telegram real: una URL que por sí sola desborda el
    presupuesto fuerza la cascada de sacrificio hasta el índice de ``url`` en
    ``_OPTIONAL_DROP_ORDER_V3`` (mismo mecanismo que ya prueba
    ``test_url_that_does_not_fit_is_omitted_whole_and_counted`` para v2) — y
    ``referencia``/``modality`` son anteriores en ese orden, así que quedan
    caídos también, mientras object_excerpt (el último del orden) sobrevive."""
    huge_url = "https://secop.gov.co/" + "a" * 4200
    payload = _v3([_one(
        object_excerpt="Suministro de kits de apoyo nutricional para Adulto Mayor",
        modality="Selección abreviada subasta inversa",
        referencia="REF-1",
        urlproceso=huge_url,
    )], channel="telegram", max_items=1)
    rendered = render_payload(payload)
    assert rendered.byte_count <= CHANNEL_BYTE_BUDGETS["telegram"]
    assert "Objeto: Suministro de kits de apoyo nutricional para Adulto Mayor" in rendered.text
    assert "Modalidad:" not in rendered.text
    assert "Referencia:" not in rendered.text


def test_build_object_excerpt_truncates_without_cutting_a_word():
    """10 · normaliza espacios, no supera 180 caracteres INCLUIDA la elipsis,
    sin cortar a media palabra, elipsis solo si hubo truncamiento."""
    short = build_object_excerpt("  Suministro   de kits  ")
    assert short == "Suministro de kits"  # espacios normalizados, sin elipsis

    long_text = (
        "SUMINISTRO DE KITS DE APOYO NUTRICIONAL PARA POBLACION VULNERABLE "
        "PROGRAMAS ADULTO MAYOR Y POBLACIÓN EN CONDICION DE DISCAPACIDAD DEL "
        "MUNICIPIO DE RAQUIRA BOYACA PARA LA VIGENCIA 2026"
    )
    excerpt = build_object_excerpt(long_text, limit=180)
    assert len(excerpt) <= 180  # incluida la elipsis
    assert excerpt.endswith("…")
    assert not excerpt[:-1].endswith(" ")
    # No corta a media palabra: lo que queda antes de la elipsis es un
    # prefijo de palabras completas del texto original.
    prefix = excerpt[:-1].rstrip()
    assert long_text.startswith(prefix)
    assert long_text[len(prefix)] == " "  # el corte cae justo en un espacio


def test_build_object_excerpt_single_long_token_returns_none():
    """FASE 3 · una única palabra/token más largo que el límite no se corta a
    media palabra: se omite el extracto entero (política segura documentada)."""
    assert build_object_excerpt("A" * 250, limit=180) is None
    # Justo en el límite: no hace falta truncar, se conserva completo.
    assert build_object_excerpt("A" * 180, limit=180) == "A" * 180
    # Uno más allá del límite y sin espacios: no hay dónde cortar sin partir.
    assert build_object_excerpt("A" * 181, limit=180) is None


def test_build_object_excerpt_accepts_none_and_empty_as_absence():
    """11 (mitad helper) · None/vacío/blanco -> None, nunca cadena vacía."""
    assert build_object_excerpt(None) is None
    assert build_object_excerpt("") is None
    assert build_object_excerpt("   ") is None


def test_none_or_empty_object_excerpt_produces_no_objeto_line_in_v3():
    """11 · None o vacío no produce línea "Objeto" en el render v3."""
    for value in (None, "", "   "):
        text = render_payload(_v3([_one(object_excerpt=value)])).text
        assert "Objeto:" not in text


def test_idempotency_key_does_not_depend_on_object_excerpt_or_rerender():
    """13 · un re-render del mismo payload (con o sin object_excerpt) no
    cambia external_idempotency_key: la clave nunca lee payload_json."""
    kwargs = dict(endpoint_id="ep-1", radar_id="rd-1", event_seqs=[1, 2, 3])
    without = idempotency_key(**kwargs)

    payload = _v3([_one(object_excerpt="Suministro de kits de apoyo nutricional")])
    render_payload(payload)
    render_payload(json.loads(serialize_payload(payload)))  # re-render simulado

    with_excerpt = idempotency_key(**kwargs)
    assert without == with_excerpt

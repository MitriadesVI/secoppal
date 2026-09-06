"""ADR-005 reachability from projected source strings, with no external I/O."""

from copy import deepcopy
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from hypothesis import given, settings, strategies as st
from openpyxl import load_workbook

from app.core.direct_lookup import build_entity_ref_soql
from app.core.export_xlsx import build_results_xlsx
from app.core.formatter import Formatter
from app.core.radar.follows import fetch_current_rows_by_keys
from app.core.radar.keys import extract_fields, radar_result_key, snapshot_hash
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder
from app.utils.value_sanity import RATIO_SUSPECT_THRESHOLD

CONTRACTS = "jbjy-vk9h"
PROCESSES = "p6dx-8zbt"
# Exact pre-fix projection at b123c5a: removing the new field must recover it.
BASE_FIELDS = (
    "id_contrato", "referencia_del_contrato", "nombre_entidad", "departamento",
    "ciudad", "objeto_del_contrato", "descripcion_del_proceso", "tipo_de_contrato",
    "modalidad_de_contratacion", "valor_del_contrato", "estado_contrato",
    "fecha_de_firma", "proveedor_adjudicado", "documento_proveedor", "urlproceso",
)
PARAMS = {"departamento_resolved": "ATLANTICO", "ordering_signal": "valor_desc"}


def _payload(value="344398000000", billed="2108000000"):
    # Offline witness recorded by audit/entity-profile-value-quality-v1.
    return {
        "id_contrato": "CO1.PCCNTR.9281738",
        "referencia_del_contrato": "CD-58-2026-6993",
        "nombre_entidad": "EDUBAR S.A.",
        "valor_del_contrato": value,
        "valor_facturado": billed,
        "estado_contrato": "Modificado",
        "fecha_de_firma": "2026-01-30",
    }


def _client(payload):
    def get(dataset, *, query):
        assert dataset == CONTRACTS
        fields = query.split(" WHERE ")[0].removeprefix("SELECT ").split(", ")
        # Faithful projection: never return a field absent from the sent SELECT.
        return [{key: value for key, value in payload.items() if key in fields}]

    source = Mock()
    source.get.side_effect = get
    return SecopClient("example.invalid", None, client=source), source


def _search(payload):
    client, source = _client(payload)
    query = SoQLBuilder().build(CONTRACTS, PARAMS)
    items = client.query(CONTRACTS, query)
    source.get.assert_called_once_with(CONTRACTS, query=query)
    rows = Formatter().to_rows(items, CONTRACTS)
    return items[0], rows[0]


def test_source_string_reaches_detector_and_existing_channel_warnings():
    payload = _payload()
    original = deepcopy(payload)
    item, row = _search(payload)
    assert payload == original
    assert isinstance(item.get("valor_facturado"), (int, float))
    assert item["valor_facturado"] == 2_108_000_000
    assert item["valor_del_contrato"] == 344_398_000_000
    assert row["valor"] == "$344,398,000,000"
    assert row["value_quality"] == "suspect"
    assert row["value_reference"] == 2_108_000_000
    assert "difiere significativamente" in row["value_warning"]
    assert "valor_facturado" not in row
    for channel in ("whatsapp", "telegram", "streamlit"):
        text = getattr(Formatter(), f"format_{channel}")(
            [row], dataset_id=CONTRACTS, total_count=1,
        )
        assert "inconsistencia de fuente" in text
        if channel != "streamlit":
            assert "valor atípico en datos abiertos" in text
            assert "valor facturado: $2,108,000,000" in text


@pytest.mark.parametrize("billed, expected", [
    ("10692000", 10_692_000.0), ("0", 0.0), (None, None),
    ("-1", -1.0), ("", None), ("No Definido", None),
])
def test_normal_and_not_evaluable_do_not_assert_clean(billed, expected):
    item, row = _search(_payload("30000000", billed))
    assert item["valor_facturado"] == expected
    assert row["valor"] == "$30,000,000"
    # Absence of an anomaly is never an affirmative CLEAN classification.
    assert not {"value_quality", "value_warning", "value_reference"} & row.keys()


def test_missing_billed_value_stays_missing():
    payload = _payload()
    del payload["valor_facturado"]
    item, row = _search(payload)
    assert "valor_facturado" not in item
    assert "value_quality" not in row


@pytest.mark.parametrize("raw, expected", [
    ("2108000000.25", 2_108_000_000.25), ("$ 2,108,000,000.25", 2_108_000_000.25),
    (2108000000, 2_108_000_000.0), (2108000000.25, 2_108_000_000.25),
])
def test_billed_uses_existing_money_conversion_without_formatter(raw, expected):
    client, _ = _client({})
    payload = _payload(value=raw, billed=raw)
    normalized = client._normalize_row(CONTRACTS, payload)
    assert normalized["valor_facturado"] == expected
    assert normalized["valor_facturado"] == normalized["valor_del_contrato"]
    assert payload["valor_facturado"] == raw


@settings(max_examples=100, derandomize=True, deadline=None)
@given(billed=st.integers(1, 10**10), delta=st.sampled_from((-1, 0, 1)))
def test_ratio_boundary_and_source_value_survive_numeric_transport(billed, delta):
    assert RATIO_SUSPECT_THRESHOLD == 100
    value = 100 * billed + delta
    item, row = _search(_payload(str(value), str(billed)))
    assert item["valor_del_contrato"] == value
    assert item["valor_facturado"] == billed
    assert row["valor"] == f"${value:,}"
    if delta >= 0:
        assert row["value_quality"] == "suspect"
        assert row["value_reference"] == billed
    else:
        assert "value_quality" not in row


def test_projection_is_additive_and_count_where_order_limit_are_unchanged():
    builder = SoQLBuilder()
    spec = builder.SPECS[CONTRACTS]
    assert spec.select_fields.count("valor_facturado") == 1
    assert tuple(f for f in spec.select_fields if f != "valor_facturado") == BASE_FIELDS
    legacy = SoQLBuilder()
    legacy.SPECS = {**builder.SPECS, CONTRACTS: replace(spec, select_fields=BASE_FIELDS)}
    query = builder.build(CONTRACTS, PARAMS)
    assert query.replace(", valor_facturado", "") == legacy.build(CONTRACTS, PARAMS)
    assert builder.build_count(CONTRACTS, PARAMS) == legacy.build_count(CONTRACTS, PARAMS)


def test_processes_do_not_transport_or_normalize_billed_value():
    assert "valor_facturado" not in SoQLBuilder().build(PROCESSES, PARAMS)
    client, _ = _client({})
    normalized = client._normalize_row(PROCESSES, {"valor_facturado": "123"})
    assert normalized["valor_facturado"] == "123"


def test_direct_reference_lookup_transports_the_warning():
    client, _ = _client(_payload())
    query = build_entity_ref_soql(CONTRACTS, "CD-58-2026-6993", exact=True)
    assert "valor_facturado" in query.split(" WHERE ")[0]
    assert query.split(" WHERE ")[1] == (
        "UPPER(referencia_del_contrato) = UPPER('CD-58-2026-6993') "
        "ORDER BY fecha_de_firma DESC, id_contrato ASC LIMIT 5"
    )
    rows = Formatter().to_rows(client.query(CONTRACTS, query), CONTRACTS)
    assert rows[0]["value_quality"] == "suspect"
    assert rows[0]["id_contrato"] == "CO1.PCCNTR.9281738"


def test_radar_follow_projection_preserves_identity_value_and_snapshot():
    client, source = _client(_payload())
    items = fetch_current_rows_by_keys(
        SimpleNamespace(secop_client=client), CONTRACTS, ["CO1.PCCNTR.9281738"],
    )
    query = source.get.call_args.kwargs["query"]
    assert "valor_facturado" in query.split(" WHERE ")[0]
    assert query.split(" WHERE ")[1] == (
        "id_contrato in('CO1.PCCNTR.9281738') OR "
        "referencia_del_contrato in('CO1.PCCNTR.9281738') LIMIT 50"
    )
    item = items[0]
    previous = {key: value for key, value in item.items() if key != "valor_facturado"}
    for consumer in (radar_result_key, extract_fields, snapshot_hash):
        assert consumer(item, CONTRACTS) == consumer(previous, CONTRACTS)
    assert item["valor_facturado"] == 2_108_000_000


def test_export_preserves_source_amount_and_existing_columns():
    _, row = _search(_payload())
    payload = build_results_xlsx(
        rows=[row], query="contratos", resolved_params=PARAMS, dataset_id=CONTRACTS,
    )
    workbook = load_workbook(BytesIO(payload))
    sheet = workbook["Resultados"]
    assert sheet["E2"].value == 344_398_000_000
    assert tuple(cell.value for cell in sheet[1]) == (
        "Referencia", "Título / objeto", "Entidad", "Contratista", "Valor (COP)",
        "Estado", "Fecha", "Tipo de fecha", "Enlace SECOP",
    )

"""Exact public oracles captured from main before dormant M1 integration.

M1 froze these because its contract was ``VISIBLE_PRODUCT_CHANGE = NO``. M2
(PROCESS-OP-STATE-001) authorizes exactly ONE visible divergence — the process
state presentation — so three oracles move on purpose. Re-pinning them alone
would throw away the guard, so the divergence is pinned AND bounded: the tests
below prove that stripping the new fields restores the pre-M2 bytes exactly, and
that WHERE/ORDER/LIMIT, ranking, ordering, counts and pagination did not move.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from app.core.formatter import Formatter
from app.core.pagination_buffer import PaginationBuffer, pagination_fingerprint
from app.core.phrase_ranking import prepare_results_for_display
from app.core.response_policy import build_advisor_response
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.results import empty_result, tabular_result


# Files M2 is authorized to touch, re-pinned. Their divergence is not taken on
# trust: it is bounded by the structural tests at the end of this module.
M2_MODIFIED_FILE_SHA256 = {
    "app/core/formatter.py": "0e51c8f76c4f9d9139d10c6e6b98a623eec8364e1e3656161c67758a91782be7",
    "app/core/response_policy.py": "863595d7f47e7955b693afec9cb75def93b323ce3373abdf7c65998f1b8d5059",
    # O1B adds a predicate gated by opportunity metadata; the ordinary-query
    # oracle below remains byte-identical and bounds that authorization.
    "app/core/soql_builder.py": "9095c502509adaec0342739467c1f533226c2f4bd780bffdd86a042a4d0b7449",
}

# M2.1 corrige únicamente dos consumidores visibles. Ambos quedan congelados y
# su divergencia se demuestra funcionalmente en sus tests focales.
M2_1_MODIFIED_FILE_SHA256 = {
    "app/core/workflow/handlers/direct_lookup.py": "6dcfc63405824e82b46a4b07bbdd503fd2bd504ec65a5640a0c3cc628ee90b6b",
    "streamlit_app.py": "6ef082fa2e17014b029780ac7b17ca292929562f0ad4f204339c34253e05b976",
}

# Still byte-frozen: M2 touches neither retrieval, nor ranking, nor pagination,
# nor the workflow handlers.
BASELINE_FILE_SHA256 = {
    "app/core/pagination_buffer.py": "18165ce221b19f49aa190f0c57a399112f72ce4a6906c49062967b7978c474ea",
    "app/core/phrase_ranking.py": "0a99c8f4ebc10dd71556f1796a40c9192404475831a424964871954bab598c5a",
    "app/core/workflow/dual_dataset.py": "5ccaca040a78273ff873c37ca422645a0005b8bb241ac79bd018a88aed2dcba8",
    "app/core/workflow/handlers/normal_flow.py": "ccd0e9267de2b20e7cb91ce893706abc6b2822ccd4e2954d45ed5d040d8e732a",
    "app/core/workflow/handlers/pagination.py": "5d46abd695d30b0c4dbb7cac734b9d5c102482af5391ad1790cd19be80744dc3",
    "app/core/workflow/handlers/refinement.py": "fe1a08d26550d5a39fb4fdddbcb40fd623b89e7ab09c634d0bba7a95db00e076",
    "app/core/workflow/results.py": "b4e869d985dadcb50679a19aafda27d92ead18ef756d4833a0ad9e4100d51057",
    "app/main.py": "bad1f025e142301e036fd40d6dbde78077719850bfda45e45ff1ff0bf4dda282",
}

BASELINE_ORACLE_SHA256 = {
    # Moved by M2 — SELECT projection only; WHERE/ORDER/LIMIT asserted unchanged.
    "soql": "7f02ccbe3544cc7cc07e0acdecd5e967baa379a2f3cfb3f01e7e923787d6a94d",
    "ranked": "132fc4c6a549c9150ced9adf715e5b6afe8760bda4728e3dc54da86eeea36a76",
    "response": "55fcb0abf29f2abf7f59acdc8185fa7cf133041ed3465ed93887b6c5b4a0cf21",
    # Moved by M2 — additive process-state fields only.
    "cards": "61c8c20daa142efab8463e17fa7ec2423a7c5f1744f7ecf73883b8639d350b56",
    "empty": "c9cc48f43781f66cc7753f02e31ddc3a5f97d9d8959a68b84062bbbea367fde2",
    # Moved by M2 — carries `cards` inside.
    "tabular": "2c274be3ca019a18374419981d5f99130c463fe389dd546b3434f53d6e86468e",
    "buffer": "24b3d80a010d8d46e145ed74206d43343566d670830110e19ea61530ff805f00",
    "page": "dd5a788979d1ec2ec6d550390326887ed71982d3d11c7219e7d8b1e54865ead8",
    # M2 cambia únicamente el fragmento de estado dentro de la respuesta asesora.
    "advisor_response": "4208f28a762febd7efa59827242e22b5b5dba40e1a4668b9a1e2dbd46fe667f1",
}

# Pre-M2 oracles. Restoring them by removing the new fields is the proof that
# M2 added and did not rewrite.
PRE_M2_ORACLE_SHA256 = {
    "soql": "1df7633eeede2783ac4d0321fdb26f5c3bac948d6366840e149ef81912d31a60",
    "cards": "e6864547ebf7717e98ecf6f361586abbe5b0a5bd78b4f29da972342862f854e3",
    "tabular": "6faf58c7f8241804978114e5628093d408479403fe205df4136ce692c013388a",
    "advisor_response": "4f51370fbf0498e77be464976001fda1dc462a18b9680930e1847c19d14da3cc",
}

# The only field M2 adds to the process SELECT, and the only keys it adds to a
# process card. Any other addition must fail the tests below.
M2_ADDED_SELECT_FIELD = "estado_resumen"
M2_ADDED_CARD_KEYS = frozenset({
    "estado_op", "estado_op_label", "estado_op_icono",
    "estado_op_raw", "estado_op_mostrar_raw",
})


def _strip_m2_card_fields(cards: list[dict]) -> list[dict]:
    return [{k: v for k, v in card.items() if k not in M2_ADDED_CARD_KEYS}
            for card in cards]


def _advisor_context(cards: list[dict]) -> dict:
    return {
        "user_query": "aseo en Antioquia",
        "resolved_params": {
            "dataset": "procesos", "objeto": ["aseo"],
            "departamento_resolved": "ANTIOQUIA",
        },
        "total_count": 3,
        "total_count_approximate": False,
        "rows": cards,
        "universe_insights": None,
        "suggestions": [
            {"label": "Filtrar por entidad"},
            {"label": "Ordenar por mayor valor"},
        ],
        "dataset_id": "p6dx-8zbt",
        "channel": "telegram",
        "needs_clarification": False,
        "clarification_reason": "",
        "degraded": False,
        "degraded_hint": "",
        "query_error": "",
        "timeout_suggestions": [],
        "result_order_guarantee": "exact_order",
    }


def _advisor_non_state_structure(response: str) -> list[str]:
    """Normaliza exclusivamente label/icon y nota raw del estado de procesos."""
    normalized: list[str] = []
    for line in response.splitlines():
        if line.startswith("   SECOP reporta apertura:"):
            continue
        if line.startswith("   $") and line.count(" | ") == 2:
            value, _state, date = line.split(" | ")
            line = f"{value} | <PROCESS_STATE_PRESENTATION> | {date}"
        normalized.append(line)
    return normalized


def _json_hash(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _oracle_values() -> dict[str, object]:
    dataset_id = "p6dx-8zbt"
    rows = [
        {
            "id_del_proceso": "P-2",
            "referencia_del_proceso": "R-2",
            "nombre_del_procedimiento": "Aseo 2",
            "entidad": "ALCALDIA TEST",
            "precio_base": "2000000",
            "estado_de_apertura_del_proceso": "Publicado",
            "fecha_de_publicacion_del": "2026-08-02T00:00:00.000",
            "urlproceso": "https://example.test/p2",
        },
        {
            "id_del_proceso": "P-1",
            "referencia_del_proceso": "R-1",
            "nombre_del_procedimiento": "Aseo 1",
            "entidad": "ALCALDIA TEST",
            "precio_base": "1000000",
            "estado_de_apertura_del_proceso": "Publicado",
            "fecha_de_publicacion_del": "2026-08-01T00:00:00.000",
            "urlproceso": "https://example.test/p1",
        },
        {
            "id_del_proceso": "P-1",
            "referencia_del_proceso": "R-1",
            "nombre_del_procedimiento": "Aseo duplicado",
            "entidad": "ALCALDIA TEST",
            "precio_base": "999999",
            "estado_de_apertura_del_proceso": "Publicado",
            "fecha_de_publicacion_del": "2026-07-31T00:00:00.000",
            "urlproceso": "https://example.test/p1b",
        },
    ]
    params = {
        "dataset": "procesos",
        "objeto": ["aseo"],
        "departamento": "Antioquia",
        "limit": 50,
        "offset": 0,
    }
    ranked = prepare_results_for_display(rows, dataset_id, params)
    response, cards = Formatter(max_results=25).format_for_channel(
        ranked,
        dataset_id,
        "streamlit",
        params=params,
        total_count=3,
    )
    fingerprint = pagination_fingerprint(params)
    buffer = PaginationBuffer.create(
        query_fingerprint=fingerprint,
        group="procesos",
        dataset_id=dataset_id,
        query_params=params,
        candidate_buffer=rows,
        validated_ranked_buffer=ranked,
        display_cursor=0,
        source_cursor=50,
        source_candidate_total=3,
        now=datetime(2026, 8, 13, tzinfo=timezone.utc),
    )
    page, advanced = buffer.take(1)
    empty = empty_result(response="Sin resultados", route_reason="oracle")
    tabular = tabular_result(
        response="R",
        rows=cards,
        results=ranked,
        dataset_id=dataset_id,
        soql_query="Q",
        parsed_params=params,
        resolved_params=params,
        route_reason="oracle",
        total_count=3,
    )
    assert fingerprint == "3e69c27c8cf7914b54161c47dafd30d402c4397210f5f7afeda946f99e2fe62f"
    assert advanced.displayed_so_far == 1
    advisor_response = build_advisor_response(_advisor_context(cards))
    return {
        "soql": SoQLBuilder().build(dataset_id, params),
        "ranked": ranked,
        "response": response,
        "cards": cards,
        "empty": empty,
        "tabular": tabular,
        "buffer": buffer.to_dict(),
        "page": page,
        "advisor_response": advisor_response,
    }


def test_public_product_files_are_byte_identical_to_pre_m1_main():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in BASELINE_FILE_SHA256
    }
    assert actual == BASELINE_FILE_SHA256


def test_m2_modified_files_match_their_pinned_state():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in M2_MODIFIED_FILE_SHA256
    }
    assert actual == M2_MODIFIED_FILE_SHA256


def test_m2_1_modified_consumers_match_their_pinned_state():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in M2_1_MODIFIED_FILE_SHA256
    }
    assert actual == M2_1_MODIFIED_FILE_SHA256


def test_m2_response_policy_is_covered_by_parity_guard():
    assert "app/core/response_policy.py" in M2_MODIFIED_FILE_SHA256


def test_public_keys_values_types_ordering_and_serialization_match_baseline():
    values = _oracle_values()
    actual = {
        name: (
            hashlib.sha256(value.encode()).hexdigest()
            if isinstance(value, str)
            else _json_hash(value)
        )
        for name, value in values.items()
    }
    assert actual == BASELINE_ORACLE_SHA256


def test_m2_soql_divergence_is_confined_to_the_select_projection():
    """WHERE, ORDER BY y LIMIT deben ser byte-idénticos al oráculo pre-M2."""
    soql = _oracle_values()["soql"]
    select, where = soql.split(" WHERE ", 1)
    fields = select[len("SELECT "):].split(", ")
    assert M2_ADDED_SELECT_FIELD in fields
    # Reconstruir el SELECT pre-M2 quitando SOLO el campo añadido devuelve el
    # oráculo original: ni un campo más, ni un reordenamiento.
    pre = "SELECT " + ", ".join(f for f in fields if f != M2_ADDED_SELECT_FIELD)
    restored = f"{pre} WHERE {where}"
    assert hashlib.sha256(restored.encode()).hexdigest() == PRE_M2_ORACLE_SHA256["soql"]


def test_m2_card_divergence_is_purely_additive():
    """Quitar los campos nuevos restaura las tarjetas pre-M2 byte a byte."""
    values = _oracle_values()
    cards = values["cards"]
    added = set().union(*(set(c) & M2_ADDED_CARD_KEYS for c in cards))
    assert added == M2_ADDED_CARD_KEYS
    stripped = _strip_m2_card_fields(cards)
    assert _json_hash(stripped) == PRE_M2_ORACLE_SHA256["cards"]
    # `estado` conserva su significado crudo: el radar y la persistencia de
    # membresías lo consumen y M2 no cambia dato persistido.
    assert [c["estado"] for c in cards] == ["Publicado", "Publicado"]

    tabular = dict(values["tabular"])
    tabular["rows"] = _strip_m2_card_fields(tabular["rows"])
    assert _json_hash(tabular) == PRE_M2_ORACLE_SHA256["tabular"]


def test_m2_response_policy_divergence_is_only_process_state_presentation():
    cards = _oracle_values()["cards"]
    current = build_advisor_response(_advisor_context(cards))
    restored = build_advisor_response(
        _advisor_context(_strip_m2_card_fields(cards))
    )

    assert hashlib.sha256(current.encode()).hexdigest() == BASELINE_ORACLE_SHA256["advisor_response"]
    assert hashlib.sha256(restored.encode()).hexdigest() == PRE_M2_ORACLE_SHA256["advisor_response"]
    assert _advisor_non_state_structure(current) == _advisor_non_state_structure(restored)

    # Los hechos públicos no relacionados siguen presentes y exactos.
    for stable in (
        "Procesos de aseo en ANTIOQUIA",
        "Encontre 3 resultados. Te muestro los 2 más recientes; hay 1 mas.",
        "1. Filtrar por entidad",
        "2. Ordenar por mayor valor",
    ):
        assert stable in current


def test_m2_does_not_move_ranking_ordering_counts_or_pagination():
    """Los oráculos de recuperación y paginación no pueden moverse en M2."""
    values = _oracle_values()
    for name in ("ranked", "response", "empty", "buffer", "page"):
        value = values[name]
        actual = (hashlib.sha256(value.encode()).hexdigest()
                  if isinstance(value, str) else _json_hash(value))
        assert actual == BASELINE_ORACLE_SHA256[name], f"{name} moved"


def test_normal_application_import_does_not_activate_truth_layer():
    code = "import sys; import app.main; print(any(n.startswith('app.core.truth_layer') for n in sys.modules))"
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "False"

"""Exact public oracles captured from main before dormant M1 integration."""
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
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.results import empty_result, tabular_result


BASELINE_FILE_SHA256 = {
    "app/core/formatter.py": "fb519e9ddca121d45a8f5a054e46e0354aacfe5149086c3606c13dd47201154e",
    "app/core/pagination_buffer.py": "18165ce221b19f49aa190f0c57a399112f72ce4a6906c49062967b7978c474ea",
    "app/core/phrase_ranking.py": "0a99c8f4ebc10dd71556f1796a40c9192404475831a424964871954bab598c5a",
    "app/core/soql_builder.py": "1b0a7d15f795c8201a1e31fa2239888fe2a2111ef8729dd627c718ccf530897d",
    "app/core/workflow/dual_dataset.py": "5ccaca040a78273ff873c37ca422645a0005b8bb241ac79bd018a88aed2dcba8",
    "app/core/workflow/handlers/direct_lookup.py": "5f833ed3ae84a34572a7123cb3cbc1853732c5124581867d420add97e61c0bb6",
    "app/core/workflow/handlers/normal_flow.py": "ccd0e9267de2b20e7cb91ce893706abc6b2822ccd4e2954d45ed5d040d8e732a",
    "app/core/workflow/handlers/pagination.py": "5d46abd695d30b0c4dbb7cac734b9d5c102482af5391ad1790cd19be80744dc3",
    "app/core/workflow/handlers/refinement.py": "fe1a08d26550d5a39fb4fdddbcb40fd623b89e7ab09c634d0bba7a95db00e076",
    "app/core/workflow/results.py": "b4e869d985dadcb50679a19aafda27d92ead18ef756d4833a0ad9e4100d51057",
    "app/main.py": "bad1f025e142301e036fd40d6dbde78077719850bfda45e45ff1ff0bf4dda282",
}

BASELINE_ORACLE_SHA256 = {
    "soql": "1df7633eeede2783ac4d0321fdb26f5c3bac948d6366840e149ef81912d31a60",
    "ranked": "132fc4c6a549c9150ced9adf715e5b6afe8760bda4728e3dc54da86eeea36a76",
    "response": "55fcb0abf29f2abf7f59acdc8185fa7cf133041ed3465ed93887b6c5b4a0cf21",
    "cards": "e6864547ebf7717e98ecf6f361586abbe5b0a5bd78b4f29da972342862f854e3",
    "empty": "c9cc48f43781f66cc7753f02e31ddc3a5f97d9d8959a68b84062bbbea367fde2",
    "tabular": "6faf58c7f8241804978114e5628093d408479403fe205df4136ce692c013388a",
    "buffer": "24b3d80a010d8d46e145ed74206d43343566d670830110e19ea61530ff805f00",
    "page": "dd5a788979d1ec2ec6d550390326887ed71982d3d11c7219e7d8b1e54865ead8",
}


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
    return {
        "soql": SoQLBuilder().build(dataset_id, params),
        "ranked": ranked,
        "response": response,
        "cards": cards,
        "empty": empty,
        "tabular": tabular,
        "buffer": buffer.to_dict(),
        "page": page,
    }


def test_public_product_files_are_byte_identical_to_pre_m1_main():
    actual = {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in BASELINE_FILE_SHA256
    }
    assert actual == BASELINE_FILE_SHA256


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


def test_normal_application_import_does_not_activate_truth_layer():
    code = "import sys; import app.main; print(any(n.startswith('app.core.truth_layer') for n in sys.modules))"
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "False"

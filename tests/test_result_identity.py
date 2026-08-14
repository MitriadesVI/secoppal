"""RG-1 — contrato de la autoridad neutral y dormida de identidad.

M1 porta la librería sin cablearla a ranking, radar ni paginación. Las
divergencias aprobadas para IDs no string, espacios y datasets desconocidos se
prueban directamente sobre la autoridad, sin cambiar consumidores legacy.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from app.core.result_identity import (
    CONTRATOS_DATASET,
    PROCESOS_DATASET,
    ResultIdentity,
    result_identity,
)
from app.core.soql_builder import SoQLBuilder

PROC = PROCESOS_DATASET
CONTR = CONTRATOS_DATASET


# ── Contrato de la autoridad ────────────────────────────────────────────────

def test_dataset_ids_mirror_soql_builder():
    """El módulo no importa SoQLBuilder; esta prueba impide que se separen."""
    assert PROCESOS_DATASET == SoQLBuilder.PROCESOS_DATASET
    assert CONTRATOS_DATASET == SoQLBuilder.CONTRATOS_DATASET


def test_proceso_resuelve_por_id_del_proceso():
    identity = result_identity(
        {"id_del_proceso": "CO1.REQ.1", "referencia_del_proceso": "SAMC-2026"}, PROC,
    )
    assert identity == ResultIdentity(
        dataset_id=PROC,
        stable_key="CO1.REQ.1",
        identity_field="id_del_proceso",
        identity_status="stable",
        reason_codes=("IDENTITY_PRIMARY_ID",),
    )


def test_proceso_cae_a_referencia_cuando_falta_el_id():
    identity = result_identity({"referencia_del_proceso": "SAMC-2026"}, PROC)
    assert identity.stable_key == "SAMC-2026"
    assert identity.identity_field == "referencia_del_proceso"
    assert identity.identity_status == "stable"
    assert identity.reason_codes == ("IDENTITY_REFERENCE_FALLBACK",)


def test_contrato_resuelve_por_id_contrato():
    identity = result_identity(
        {"id_contrato": "CO1.PCCNTR.9", "referencia_del_contrato": "REF-9"}, CONTR,
    )
    assert identity.stable_key == "CO1.PCCNTR.9"
    assert identity.identity_field == "id_contrato"


def test_contrato_cae_a_referencia_del_contrato():
    identity = result_identity({"referencia_del_contrato": "REF-9"}, CONTR)
    assert identity.stable_key == "REF-9"
    assert identity.identity_field == "referencia_del_contrato"
    assert identity.reason_codes == ("IDENTITY_REFERENCE_FALLBACK",)


@pytest.mark.parametrize("value", [None, ""])
def test_valores_ausentes_no_producen_clave(value):
    identity = result_identity(
        {"id_del_proceso": value, "referencia_del_proceso": value}, PROC,
    )
    assert identity.stable_key is None
    assert identity.identity_field is None
    assert identity.identity_status == "unstable"
    assert identity.reason_codes == ("IDENTITY_ABSENT",)


def test_fila_sin_ningun_campo_de_identidad_es_unstable():
    identity = result_identity({"entidad": "ALCALDIA", "precio_base": 1000}, PROC)
    assert identity.identity_status == "unstable"
    assert identity.stable_key is None


# ── Divergencia intencional: whitespace-only ───────────────────────────────

@pytest.mark.parametrize("blank", ["   ", "\t", "\n", " \t\n "])
def test_whitespace_only_es_unstable_y_queda_registrado(blank):
    identity = result_identity({"id_del_proceso": blank}, PROC)
    assert identity.identity_status == "unstable"
    assert identity.stable_key is None
    assert "IDENTITY_BLANK_DISCARDED" in identity.reason_codes
    assert "INTENTIONAL_DIVERGENCE_FROM_LEGACY" in identity.reason_codes


def test_whitespace_en_el_id_permite_caer_a_la_referencia():
    identity = result_identity(
        {"id_del_proceso": "  ", "referencia_del_proceso": "SAMC-2026"}, PROC,
    )
    assert identity.stable_key == "SAMC-2026"
    assert identity.identity_field == "referencia_del_proceso"
    assert "INTENTIONAL_DIVERGENCE_FROM_LEGACY" in identity.reason_codes


def test_los_espacios_de_los_bordes_no_se_recortan_en_la_clave():
    """Recortar cambiaría qué filas se agrupan hoy; el strip solo detecta vacío."""
    identity = result_identity({"id_del_proceso": " CO1.REQ.1 "}, PROC)
    assert identity.stable_key == " CO1.REQ.1 "


# ── Tipos y campos prohibidos ──────────────────────────────────────────────

# ── RG-1.1: solo un str no vacío puede ser identidad ───────────────────────

class _Arbitrario:
    def __str__(self) -> str:  # pragma: no cover - nunca debe invocarse
        raise AssertionError("result_identity no debe convertir con str(...)")


_NON_STRING_VALUES = [0, 1, False, True, 12.5, (), (1,), [], [1], {}, {"a": 1},
                      b"CO1.REQ.1", _Arbitrario()]


@pytest.mark.parametrize("value", _NON_STRING_VALUES)
@pytest.mark.parametrize("dataset_id", [PROC, CONTR])
def test_valores_no_str_nunca_son_identidad(value, dataset_id):
    fields = (
        ("id_del_proceso", "referencia_del_proceso") if dataset_id == PROC
        else ("id_contrato", "referencia_del_contrato")
    )
    identity = result_identity({f: value for f in fields}, dataset_id)
    assert identity.stable_key is None
    assert identity.identity_field is None
    assert identity.identity_status == "unstable"
    assert "IDENTITY_INVALID_TYPE" in identity.reason_codes


@pytest.mark.parametrize("value", _NON_STRING_VALUES)
def test_primario_no_str_permite_caer_a_la_referencia_valida(value):
    identity = result_identity(
        {"id_del_proceso": value, "referencia_del_proceso": "REF-123"}, PROC,
    )
    assert identity.stable_key == "REF-123"
    assert identity.identity_field == "referencia_del_proceso"
    assert identity.reason_codes == (
        "IDENTITY_INVALID_TYPE", "IDENTITY_REFERENCE_FALLBACK",
    )


def test_no_se_convierte_con_str():
    """El objeto arbitrario revienta si alguien lo pasa por ``str(...)``."""
    identity = result_identity({"id_del_proceso": _Arbitrario()}, PROC)
    assert identity.identity_status == "unstable"


def test_subclase_de_str_sigue_siendo_identidad():
    class Ref(str):
        pass

    identity = result_identity({"id_del_proceso": Ref("CO1.REQ.1")}, PROC)
    assert identity.stable_key == "CO1.REQ.1"
    assert identity.identity_status == "stable"


def test_source_row_id_nunca_es_identidad():
    """ADR-050/W2.0E-R1: la identidad física se recrea entre lotes."""
    row = {"source_row_id": "row-abc", ":id": "row-abc", "uid": "CORE-001::0"}
    assert result_identity(row, PROC).identity_status == "unstable"
    assert result_identity(row, CONTR).identity_status == "unstable"


def test_la_similitud_textual_nunca_fusiona():
    a = {"id_del_proceso": "CO1.REQ.1", "nombre_del_procedimiento": "ATENCION",
         "entidad": "ALCALDIA", "precio_base": 1000}
    b = dict(a, id_del_proceso="CO1.REQ.2")
    assert result_identity(a, PROC).stable_key != result_identity(b, PROC).stable_key


def test_procesos_y_contratos_no_colisionan_con_la_misma_cadena():
    proceso = result_identity({"id_del_proceso": "X-1"}, PROC)
    contrato = result_identity({"id_contrato": "X-1"}, CONTR)
    assert proceso.stable_key == contrato.stable_key == "X-1"
    assert proceso != contrato
    assert (proceso.dataset_id, proceso.identity_field) != (
        contrato.dataset_id, contrato.identity_field,
    )


# ── RG-1.1: dataset desconocido falla cerrado ──────────────────────────────

_UNKNOWN_DATASET_ROWS = [
    {"id_contrato": "C-1", "referencia_del_contrato": "REF-C"},
    {"id_del_proceso": "P-1", "referencia_del_proceso": "REF-P"},
    {"id_del_proceso": "P-1", "id_contrato": "C-1",
     "referencia_del_proceso": "REF-P", "referencia_del_contrato": "REF-C"},
]


@pytest.mark.parametrize("row", _UNKNOWN_DATASET_ROWS)
@pytest.mark.parametrize("dataset_id", ["otro-dataset", "", "P6DX-8ZBT", "xxxx-yyyy"])
def test_dataset_desconocido_falla_cerrado(row, dataset_id):
    identity = result_identity(row, dataset_id)
    assert identity.stable_key is None
    assert identity.identity_field is None
    assert identity.identity_status == "unstable"
    assert identity.reason_codes == ("IDENTITY_UNSUPPORTED_DATASET",)


def test_dataset_desconocido_no_lanza_excepcion():
    assert result_identity({}, "otro-dataset").identity_status == "unstable"


def test_no_muta_la_fila_de_entrada():
    row = {"id_del_proceso": "CO1.REQ.1", "entidad": "ALCALDIA"}
    snapshot = dict(row)
    result_identity(row, PROC)
    assert row == snapshot


def test_result_identity_es_hashable_y_congelada():
    identity = result_identity({"id_del_proceso": "CO1.REQ.1"}, PROC)
    assert hash(identity) == hash(result_identity({"id_del_proceso": "CO1.REQ.1"}, PROC))
    with pytest.raises(Exception):
        identity.stable_key = "otro"  # type: ignore[misc]


def test_el_modulo_no_depende_de_app():
    """Capa más baja: sin imports de app, sin I/O, sin reloj."""
    source = Path("app/core/result_identity.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    assert not [m for m in modules if m.split(".")[0] == "app"]
    assert set(modules) <= {"__future__", "collections.abc", "dataclasses", "typing"}

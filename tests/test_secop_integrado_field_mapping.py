"""Tests de field mapping para SECOP Integrado (rpmr-utcd) → NormalizedContractRecord.

Validan que:
- El sample se puede mapear al modelo normalizado
- Campos obligatorios mínimos presentes o documentados como missing
- No se usa rpmr-utcd para oportunidades abiertas
- "NO DEFINIDO" se mapea a None
- Origen se normaliza correctamente
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

# ── Modelo normalizado (mismo que en docs/secop_integrado/FIELD_MAPPING.md) ──

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
SENTINEL = "NO DEFINIDO"
ORIGIN_MAP = {"SECOPI": "SECOP I", "SECOPII": "SECOP II"}


class NormalizedContractRecord:
    """Registro normalizado de contrato desde SECOP Integrado (rpmr-utcd)."""

    def __init__(self, row: dict[str, Any]) -> None:
        self.source_dataset = "rpmr-utcd"
        self.source_platform = ORIGIN_MAP.get(
            (row.get("origen") or "").strip(),
            (row.get("origen") or "").strip(),
        )
        self.contract_id = (row.get("numero_del_contrato") or "").strip()
        self.process_reference = (row.get("numero_de_proceso") or "").strip()
        self.entity_name = (row.get("nombre_de_la_entidad") or "").strip()
        self.entity_nit = (row.get("nit_de_la_entidad") or "").strip()
        self.department = (row.get("departamento_entidad") or "").strip()
        self.city = (row.get("municipio_entidad") or "").strip()
        self.object_description = (row.get("objeto_del_proceso") or "").strip()
        self.modality = (row.get("modalidad_de_contrataci_n") or "").strip()
        self.contract_type = (row.get("tipo_de_contrato") or "").strip()
        self.process_status = (row.get("estado_del_proceso") or "").strip()
        self.contract_value = self._parse_float(row.get("valor_contrato"), default=0.0)
        self.signature_date = self._parse_date(row.get("fecha_de_firma_del_contrato"))
        self.start_date = self._parse_date(row.get("fecha_inicio_ejecuci_n"))
        self.end_date = self._parse_date(row.get("fecha_fin_ejecuci_n"))
        self.contractor_name = self._clean_sentinel(row.get("nom_raz_social_contratista"))
        self.contractor_document = self._clean_sentinel(row.get("documento_proveedor"))
        self.contractor_doc_type = self._clean_sentinel(row.get("tipo_documento_proveedor"))
        self.url = (row.get("url_contrato") or "").strip()

    @staticmethod
    def _clean_sentinel(value: str | None) -> str | None:
        if value is None:
            return None
        v = value.strip()
        return None if v.upper() == SENTINEL else v

    @staticmethod
    def _parse_float(raw: str | None, default: float = 0.0) -> float:
        if raw is None:
            return default
        try:
            return float(raw)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        if raw is None:
            return None
        try:
            # Socrata format: "2025-01-15T00:00:00.000"
            return datetime.fromisoformat(
                raw.replace("T00:00:00.000", "").replace("T00:00:00", "")
            ).date()
        except (ValueError, TypeError):
            return None

    def is_post_award(self) -> bool:
        """¿El registro representa un contrato firmado/en ejecución?"""
        post_states = {"Celebrado", "Liquidado", "En ejecución", "Cerrado", "cedido"}
        return self.process_status in post_states and self.signature_date is not None

    def is_pre_award(self) -> bool:
        """¿El registro es precontractual (sin contrato firmado)?"""
        pre_states = {"Borrador", "Convocado", "En aprobación", "Activo", "Aprobado",
                      "enviado Proveedor", "Expresión de Interés", "Invitación Abierta",
                      "Invitación Cerrada", "Lista Corta", "Lista Multiusos"}
        return self.process_status in pre_states


# ── Fixtures ──

@pytest.fixture(scope="module")
def integrado_sample() -> list[dict[str, Any]]:
    path = FIXTURE_DIR / "secop_integrado_sample.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def integrado_secop1_sample() -> list[dict[str, Any]]:
    path = FIXTURE_DIR / "secop_integrado_secop1_sample.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def integrado_secop2_sample() -> list[dict[str, Any]]:
    path = FIXTURE_DIR / "secop_integrado_secop2_sample.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Tests: Mapping básico ──

def test_fixture_loads(integrado_sample):
    """El fixture de SECOP Integrado carga correctamente."""
    assert len(integrado_sample) > 0, "Fixture vacío"
    assert len(integrado_sample) == 50


def test_map_all_rows(integrado_sample):
    """Todas las filas del fixture se mapean sin excepción."""
    records = []
    for row in integrado_sample:
        record = NormalizedContractRecord(row)
        records.append(record)
    assert len(records) == 50


def test_required_fields_present(integrado_sample):
    """Campos obligatorios mínimos están presentes en todas las filas mapeadas."""
    for row in integrado_sample:
        record = NormalizedContractRecord(row)
        assert record.source_dataset == "rpmr-utcd"
        assert record.source_platform in ("SECOP I", "SECOP II")
        assert record.contract_id, f"contract_id vacío: {record.process_reference}"
        assert record.entity_name, f"entity_name vacío"
        assert record.department, f"department vacío"
        assert record.process_status, f"process_status vacío"


def test_origin_normalization(integrado_sample):
    """Origen 'SECOPI' y 'SECOPII' se normalizan correctamente."""
    for row in integrado_sample:
        record = NormalizedContractRecord(row)
        raw = (row.get("origen") or "").strip()
        if raw == "SECOPI":
            assert record.source_platform == "SECOP I"
        elif raw == "SECOPII":
            assert record.source_platform == "SECOP II"
        else:
            assert record.source_platform == raw  # unchanged


def test_sentinel_maps_to_none(integrado_sample):
    """Valores 'NO DEFINIDO' se mapean a None."""
    for row in integrado_sample:
        record = NormalizedContractRecord(row)
        raw_contractor = (row.get("nom_raz_social_contratista") or "").strip()
        if raw_contractor.upper() == "NO DEFINIDO":
            assert record.contractor_name is None, (
                f"Expected None for NO DEFINIDO, got {record.contractor_name}"
            )


def test_contract_value_is_float(integrado_sample):
    """contract_value siempre es float."""
    for row in integrado_sample:
        record = NormalizedContractRecord(row)
        assert isinstance(record.contract_value, float)


# ── Tests: Clasificación de estados ──

def test_pre_award_states_exist(integrado_sample):
    """El fixture contiene estados precontractuales (Borrador, Convocado, etc.)."""
    pre_records = [NormalizedContractRecord(r) for r in integrado_sample
                   if NormalizedContractRecord(r).is_pre_award()]
    assert len(pre_records) > 0, (
        "Se esperaban registros precontractuales en el fixture. "
        "Esto confirma que rpmr-utcd NO es solo contratos."
    )


def test_not_all_records_are_contracts(integrado_sample):
    """No todos los registros tienen contrato firmado."""
    records = [NormalizedContractRecord(r) for r in integrado_sample]
    post = sum(1 for r in records if r.is_post_award())
    pre = sum(1 for r in records if r.is_pre_award())
    total = len(records)
    # Al menos algunos deben ser pre-award (la descripción oficial dice "contratos" pero el dataset contiene precontractuales)
    assert pre > 0 or post < total, (
        f"Todos los {total} registros son post-award. "
        "rpmr-utcd podría estar limitado a contratos en esta muestra, "
        "pero la auditoría de schema muestra estados precontractuales."
    )


# ── Tests: No usar para oportunidades abiertas ──

def test_rpmr_utcd_not_for_open_opportunities(integrado_sample):
    """rpmr-utcd no debe usarse para búsqueda de oportunidades abiertas.

    Verifica que los registros precontractuales NO tienen fecha de publicación.
    Sin fecha de publicación, no se puede determinar si una oportunidad está abierta.
    """
    # rpmr-utcd no tiene campo de fecha de publicación. El campo más cercano
    # es fecha_de_firma_del_contrato, que es post-adjudicación.
    row = integrado_sample[0]
    # Verificar que NO existe un campo de fecha de publicación/apertura
    publication_fields = [k for k in row if "publicacion" in k.lower()
                          or "apertura" in k.lower() or "fecha_public" in k.lower()]
    assert len(publication_fields) == 0, (
        f"rpmr-utcd tiene campo de publicación: {publication_fields}. "
        "Esto cambiaría la recomendación de no usar para oportunidades."
    )


# ── Tests: URLs ──

def test_urls_point_to_contratos_gov(integrado_sample):
    """Las URLs apuntan a contratos.gov.co (formato SECOP I)."""
    for row in integrado_sample:
        record = NormalizedContractRecord(row)
        if record.url:
            assert "contratos.gov.co" in record.url, (
                f"URL no esperada: {record.url}"
            )


# ── Tests: Muestras por origen ──

def test_secop1_origin_sample(integrado_secop1_sample):
    """La muestra SECOP I solo contiene registros de SECOP I."""
    for row in integrado_secop1_sample:
        origen = (row.get("origen") or "").strip()
        assert origen == "SECOPI", f"Esperado SECOPI, encontrado {origen}"


def test_secop2_origin_sample(integrado_secop2_sample):
    """La muestra SECOP II solo contiene registros de SECOP II."""
    for row in integrado_secop2_sample:
        origen = (row.get("origen") or "").strip()
        assert origen == "SECOPII", f"Esperado SECOPII, encontrado {origen}"


def test_date_parsing_handles_missing(integrado_sample):
    """Fechas ausentes (null/None) no causan excepción."""
    for row in integrado_sample:
        record = NormalizedContractRecord(row)
        # Las fechas pueden ser None sin problema
        assert record.signature_date is None or isinstance(record.signature_date, date)
        assert record.start_date is None or isinstance(record.start_date, date)
        assert record.end_date is None or isinstance(record.end_date, date)

"""Tests de field mapping para SECOP I Procesos (f789-7hwg) → NormalizedHistoricalProcessRecord.

Validan que:
- El sample se puede mapear al modelo normalizado
- Campos obligatorios mínimos presentes o documentados como missing
- Procesos sin contrato se identifican correctamente (has_contract=False)
- No se trata como contratos consolidados (es solo SECOP I)
- Campos UNSPSC existen cuando están disponibles
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

# ── Modelo normalizado (mismo que en docs/secop1_procesos/FIELD_MAPPING.md) ──

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


class NormalizedHistoricalProcessRecord:
    """Registro normalizado de proceso histórico SECOP I (f789-7hwg)."""

    def __init__(self, row: dict[str, Any]) -> None:
        self.source_dataset = "f789-7hwg"

        # Identificadores
        self.process_id = (row.get("uid") or "").strip()
        self.process_reference = (
            (row.get("numero_de_constancia") or row.get("numero_de_proceso") or "")
        ).strip()

        # Entidad
        self.entity_name = (row.get("nombre_entidad") or "").strip()
        self.entity_nit = (row.get("nit_de_la_entidad") or "").strip()
        self.department = (row.get("departamento_entidad") or "").strip()
        self.city = (row.get("municipio_entidad") or "").strip()

        # Objeto y modalidad
        detail = (row.get("detalle_del_objeto_a_contratar") or "").strip()
        self.object_description = detail if detail else (row.get("objeto_a_contratar") or "").strip()
        self.unspsc_group = (row.get("nombre_grupo") or "").strip() or None
        self.unspsc_family = (row.get("nombre_familia") or "").strip() or None
        self.unspsc_class = (row.get("nombre_clase") or "").strip() or None
        self.modality = (row.get("modalidad_de_contratacion") or "").strip()
        self.contract_type = (row.get("tipo_de_contrato") or "").strip() or None

        # Estado
        self.process_status = (row.get("estado_del_proceso") or "").strip()

        # Valores
        self.estimated_value = self._parse_float_zero(row.get("cuantia_proceso"))
        self.contract_value = self._parse_float(row.get("cuantia_contrato"))
        self.contract_value_with_additions = self._parse_float(
            row.get("valor_contrato_con_adiciones")
        )

        # Fechas
        self.publication_date = self._parse_date(row.get("fecha_de_cargue_en_el_secop"))
        self.signature_date = self._parse_date(row.get("fecha_de_firma_del_contrato"))
        self.start_date = self._parse_date(row.get("fecha_ini_ejec_contrato"))
        self.end_date = self._parse_date(row.get("fecha_fin_ejec_contrato"))
        self.liquidation_date = self._parse_date(row.get("fecha_liquidacion"))

        # Contratista
        self.contractor_name = (row.get("nom_razon_social_contratista") or "").strip() or None
        self.contractor_document = (row.get("identificacion_del_contratista") or "").strip() or None
        self.contractor_doc_type = (row.get("tipo_identifi_del_contratista") or "").strip() or None

        # URL — Socrata devuelve tipo 'url' como dict {'url': 'https://...'}
        raw_url = row.get("ruta_proceso_en_secop_i")
        if isinstance(raw_url, dict):
            raw_url = raw_url.get("url", "")
        self.url = (raw_url or "").strip() or None

        # Derivados
        self.has_contract = self.signature_date is not None
        self.is_postconflict = self._parse_bool(row.get("es_postconflicto"))

    @staticmethod
    def _parse_float(raw: str | None) -> float | None:
        if raw is None:
            return None
        try:
            return float(raw)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_float_zero(raw: str | None) -> float:
        val = NormalizedHistoricalProcessRecord._parse_float(raw)
        return val if val is not None else 0.0

    @staticmethod
    def _parse_date(raw: str | None) -> date | None:
        if raw is None:
            return None
        try:
            return datetime.fromisoformat(
                raw.replace("T00:00:00.000", "").replace("T00:00:00", "")
            ).date()
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_bool(raw: str | None) -> bool:
        if raw is None:
            return False
        try:
            return int(raw) == 1
        except (ValueError, TypeError):
            return False

    def is_celebrated(self) -> bool:
        return self.process_status == "CELEBRADO"

    def is_liquidated(self) -> bool:
        return self.process_status == "LIQUIDADO"

    def is_without_contract(self) -> bool:
        return not self.has_contract


# ── Fixtures ──

@pytest.fixture(scope="module")
def secop1_sample() -> list[dict[str, Any]]:
    path = FIXTURE_DIR / "secop1_procesos_sample.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Tests: Mapping básico ──

def test_fixture_loads(secop1_sample):
    """El fixture de SECOP I Procesos carga correctamente."""
    assert len(secop1_sample) > 0, "Fixture vacío"
    assert len(secop1_sample) == 20


def test_map_all_rows(secop1_sample):
    """Todas las filas del fixture se mapean sin excepción."""
    records = []
    for row in secop1_sample:
        record = NormalizedHistoricalProcessRecord(row)
        records.append(record)
    assert len(records) == 20


def test_required_fields_present(secop1_sample):
    """Campos obligatorios mínimos están presentes en todas las filas mapeadas."""
    for row in secop1_sample:
        record = NormalizedHistoricalProcessRecord(row)
        assert record.source_dataset == "f789-7hwg"
        assert record.process_id, "process_id (uid) vacío"
        assert record.entity_name, "entity_name vacío"
        assert record.department, "department vacío"
        assert record.process_status, "process_status vacío"
        assert record.object_description, "object_description vacío"
        assert isinstance(record.estimated_value, float)


def test_no_secop2_data(secop1_sample):
    """f789-7hwg es solo SECOP I. No debe tener campos de SECOP II."""
    # Verificar que no hay campos típicos de SECOP II
    secop2_indicators = ["url_secop_ii", "community.secop.gov.co", "SECOPII"]
    for row in secop1_sample:
        for indicator in secop2_indicators:
            assert indicator not in str(row), (
                f"Indicador SECOP II encontrado en f789-7hwg: {indicator}"
            )


# ── Tests: Procesos sin contrato ──

def test_has_contract_derivation(secop1_sample):
    """has_contract se deriva correctamente de signature_date."""
    for row in secop1_sample:
        record = NormalizedHistoricalProcessRecord(row)
        if record.signature_date is not None:
            assert record.has_contract is True
        else:
            assert record.has_contract is False


def test_processes_without_contract_documented(secop1_sample):
    """Si hay procesos sin contrato en el fixture, se documentan."""
    records = [NormalizedHistoricalProcessRecord(r) for r in secop1_sample]
    without = [r for r in records if r.is_without_contract()]
    # No todos tienen que tener contrato — la auditoría muestra 8.5% sin firma
    # Este test solo verifica que el código maneja ambos casos
    for r in without:
        assert r.contract_value is None or r.contract_value == 0.0, (
            f"Proceso sin contrato no debería tener valor de contrato: {r.contract_value}"
        )
    # Si hay procesos sin contrato, documentarlo (no es un error)
    if without:
        pass  # OK, documentado en SCHEMA_AUDIT como 8.5%


# ── Tests: No tratar como contratos consolidados ──

def test_not_treated_as_consolidated_contracts(secop1_sample):
    """f789-7hwg no debe tratarse como dataset de contratos consolidados."""
    # f789-7hwg tiene campos de proceso (cuantia_proceso) y contrato (cuantia_contrato)
    # separados. Un dataset de contratos no tendría valor de proceso.
    for row in secop1_sample:
        record = NormalizedHistoricalProcessRecord(row)
        # Verificar que estimated_value (cuantia_proceso) existe
        assert isinstance(record.estimated_value, float)
        # Puede o no tener contract_value — eso es correcto


# ── Tests: Campos UNSPSC ──

def test_unspsc_fields_when_available(secop1_sample):
    """Campos UNSPSC son opcionales pero deben ser strings válidos cuando existen."""
    for row in secop1_sample:
        record = NormalizedHistoricalProcessRecord(row)
        for unspsc_val in [record.unspsc_group, record.unspsc_family, record.unspsc_class]:
            if unspsc_val is not None:
                assert isinstance(unspsc_val, str)
                assert unspsc_val.strip(), "UNSPSC value is empty string"


# ── Tests: Fechas ──

def test_dates_parseable(secop1_sample):
    """Todas las fechas presentes son parseables a date."""
    for row in secop1_sample:
        record = NormalizedHistoricalProcessRecord(row)
        for date_val, name in [
            (record.publication_date, "publication_date"),
            (record.signature_date, "signature_date"),
            (record.start_date, "start_date"),
            (record.end_date, "end_date"),
            (record.liquidation_date, "liquidation_date"),
        ]:
            if date_val is not None:
                assert isinstance(date_val, date), (
                    f"{name} no es date: {type(date_val)} = {date_val}"
                )


# ── Tests: URLs SECOP I ──

def test_urls_secop1_format(secop1_sample):
    """Las URLs apuntan a contratos.gov.co (SECOP I legacy)."""
    for row in secop1_sample:
        record = NormalizedHistoricalProcessRecord(row)
        if record.url:
            assert "contratos.gov.co" in record.url, (
                f"URL no apunta a SECOP I: {record.url}"
            )


# ── Tests: Valores numéricos ──

def test_numeric_values_parse(secop1_sample):
    """Valores numéricos se parsean correctamente."""
    for row in secop1_sample:
        record = NormalizedHistoricalProcessRecord(row)
        assert isinstance(record.estimated_value, float)
        if record.contract_value is not None:
            assert isinstance(record.contract_value, float)
        if record.contract_value_with_additions is not None:
            assert isinstance(record.contract_value_with_additions, float)

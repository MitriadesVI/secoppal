"""Tests for VALUE-SANITY-002A — Value anomaly detection for SECOP contracts."""

import pytest
from app.utils.value_sanity import detect_value_anomaly, RATIO_SUSPECT_THRESHOLD
from app.core.formatter import Formatter


# ── detect_value_anomaly unit tests ────────────────────────────────────

def test_detect_value_anomaly_luruaco_ratio_x1000():
    """Real case: Luruaco/CD-1028-2025 — valor_del_contrato=250B, valor_facturado=250M."""
    item = {
        "valor_del_contrato": 250_000_000_000.0,
        "valor_facturado": 250_000_000.0,
    }
    result = detect_value_anomaly(item, dataset_id="jbjy-vk9h")
    assert result is not None
    assert result["value_quality"] == "suspect"
    assert "difiere significativamente" in result["value_warning"]
    assert result["value_reference"] == 250_000_000.0


def test_no_anomaly_when_valor_facturado_missing():
    """If valor_facturado is missing, cannot compute ratio → no anomaly."""
    item = {"valor_del_contrato": 250_000_000_000.0}
    result = detect_value_anomaly(item, dataset_id="jbjy-vk9h")
    assert result is None


def test_no_anomaly_when_ratio_normal():
    """Normal contract with consistent values should not trigger."""
    item = {
        "valor_del_contrato": 29_866_666.0,
        "valor_facturado": 10_692_000.0,  # ratio ≈ 2.8
    }
    result = detect_value_anomaly(item, dataset_id="jbjy-vk9h")
    assert result is None


def test_no_anomaly_procesos_dataset():
    """Dataset Procesos is explicitly excluded."""
    item = {
        "valor_del_contrato": 250_000_000_000.0,
        "valor_facturado": 250_000_000.0,
    }
    result = detect_value_anomaly(item, dataset_id="p6dx-8zbt")
    assert result is None


def test_no_anomaly_when_valor_facturado_zero():
    """If valor_facturado is 0, division would be infinite → no anomaly."""
    item = {
        "valor_del_contrato": 250_000_000_000.0,
        "valor_facturado": 0.0,
    }
    result = detect_value_anomaly(item, dataset_id="jbjy-vk9h")
    assert result is None


def test_no_anomaly_when_valores_string():
    """Non-numeric fields should not trigger anomaly."""
    item = {
        "valor_del_contrato": "250000000",
        "valor_facturado": "250000",
    }
    result = detect_value_anomaly(item, dataset_id="jbjy-vk9h")
    assert result is None


# ── formatter integration tests ────────────────────────────────────────

class TestFormatterValueWarning:
    """Verify that suspect values produce visible warnings in output."""

    def test_formatter_to_rows_attaches_anomaly_fields(self):
        """to_rows() should add value_quality/warning/reference for suspect items."""
        fmt = Formatter(max_results=5)
        items = [
            {
                "nombre_entidad": "DEPARTAMENTO DEL ATLANTICO",
                "valor_del_contrato": 250_000_000_000.0,
                "valor_facturado": 250_000_000.0,
                "estado_contrato": "Terminado",
                "fecha_de_firma": "2025-05-01",
                "proveedor_adjudicado": "FUNDACION CULTURAL",
                "urlproceso": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.8089879",
                "referencia_del_contrato": "202501916",
            }
        ]
        rows = fmt.to_rows(items, dataset_id="jbjy-vk9h")
        assert len(rows) == 1
        row = rows[0]
        assert row.get("value_quality") == "suspect"
        assert "difiere" in row.get("value_warning", "")
        assert row.get("value_reference") == 250_000_000.0
        # valor_del_contrato should still be in the formatted valor string
        assert "250,000,000,000" in row["valor"]

    def test_raw_valor_del_contrato_not_modified(self):
        """The raw valor_del_contrato field is preserved — anomaly is metadata only."""
        item = {
            "valor_del_contrato": 250_000_000_000.0,
            "valor_facturado": 250_000_000.0,
        }
        original_vdc = item["valor_del_contrato"]
        detect_value_anomaly(item, dataset_id="jbjy-vk9h")
        assert item["valor_del_contrato"] == original_vdc
        assert item["valor_del_contrato"] == 250_000_000_000.0

    def test_formatter_shows_value_warning_whatsapp(self):
        """WhatsApp output includes per-card anomaly warning."""
        fmt = Formatter(max_results=3)
        items = [
            {
                "nombre_entidad": "DEPARTAMENTO DEL ATLANTICO",
                "valor_del_contrato": 250_000_000_000.0,
                "valor_facturado": 250_000_000.0,
                "estado_contrato": "Cerrado",
                "fecha_de_firma": "2025-03-01",
                "proveedor_adjudicado": "FUNDACION",
                "urlproceso": "https://community.secop.gov.co/...",
                "referencia_del_contrato": "202501916",
            }
        ]
        rows = fmt.to_rows(items, dataset_id="jbjy-vk9h")
        output = fmt.format_whatsapp(rows, dataset_id="jbjy-vk9h", total_count=1)
        assert "⚠️ valor atípico en datos abiertos" in output
        assert "valor facturado: $250,000,000" in output
        assert "⚠️ El mayor valor mostrado tiene inconsistencia de fuente" in output

    def test_formatter_shows_value_warning_telegram(self):
        """Telegram output includes per-card anomaly warning."""
        fmt = Formatter(max_results=3)
        items = [
            {
                "nombre_entidad": "DEPARTAMENTO DEL ATLANTICO",
                "valor_del_contrato": 250_000_000_000.0,
                "valor_facturado": 250_000_000.0,
                "estado_contrato": "Cerrado",
                "fecha_de_firma": "2025-03-01",
                "proveedor_adjudicado": "FUNDACION",
                "urlproceso": "https://community.secop.gov.co/...",
                "referencia_del_contrato": "202501916",
            }
        ]
        rows = fmt.to_rows(items, dataset_id="jbjy-vk9h")
        output = fmt.format_telegram(rows, dataset_id="jbjy-vk9h", total_count=1)
        assert "⚠️ valor atípico en datos abiertos" in output
        assert "⚠️ El mayor valor mostrado tiene inconsistencia de fuente" in output

    def test_formatter_no_warning_clean_values(self):
        """Clean values should not produce warnings."""
        fmt = Formatter(max_results=3)
        items = [
            {
                "entidad": "ALCALDIA MUNICIPIO DE DOSQUEBRADAS",
                "precio_base": 26_730_000.0,
                "estado_de_apertura_del_proceso": "Abierto",
                "fecha_de_publicacion_del": "2025-05-06",
                "urlproceso": "https://community.secop.gov.co/...",
                "referencia_del_proceso": "CD-1028-2025",
            }
        ]
        rows = fmt.to_rows(items, dataset_id="p6dx-8zbt")
        output = fmt.format_whatsapp(rows, dataset_id="p6dx-8zbt", total_count=1)
        assert "⚠️ valor atípico" not in output
        assert "inconsistencia de fuente" not in output

    def test_formatter_header_warning_only_for_first(self):
        """Header warning only appears when the FIRST result is suspect."""
        fmt = Formatter(max_results=3)
        items = [
            {  # First: suspect
                "nombre_entidad": "DEPARTAMENTO DEL ATLANTICO",
                "valor_del_contrato": 250_000_000_000.0,
                "valor_facturado": 250_000_000.0,
                "estado_contrato": "Cerrado",
                "fecha_de_firma": "2025-03-01",
                "proveedor_adjudicado": "FUNDACION",
                "urlproceso": "https://community.secop.gov.co/...",
                "referencia_del_contrato": "202501916",
            },
            {  # Second: clean
                "nombre_entidad": "OTRA ENTIDAD",
                "valor_del_contrato": 50_000_000.0,
                "valor_facturado": 50_000_000.0,
                "estado_contrato": "Abierto",
                "fecha_de_firma": "2025-04-01",
                "proveedor_adjudicado": "EMPRESA",
                "urlproceso": "https://community.secop.gov.co/...",
                "referencia_del_contrato": "OTHER-001",
            },
        ]
        rows = fmt.to_rows(items, dataset_id="jbjy-vk9h")
        output = fmt.format_whatsapp(rows, dataset_id="jbjy-vk9h", total_count=2)
        # Header warning present (first is suspect)
        assert "⚠️ El mayor valor mostrado tiene inconsistencia de fuente" in output
        # Per-card warning on first card
        output_before_second = output.split("OTRA ENTIDAD")[0]
        assert "⚠️ valor atípico en datos abiertos" in output_before_second
        # No warning on second card
        output_after_second = output.split("OTRA ENTIDAD")[1] if "OTRA ENTIDAD" in output else ""
        assert "⚠️ valor atípico" not in output_after_second or output_after_second == ""

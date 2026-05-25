"""Tests for secop_client._normalize_row URL repair (FIX-URL-FREEFORM-001)."""
from __future__ import annotations

from app.core.secop_client import SecopClient


class TestNormalizeRowUrl:
    """URL repair hierarchy: ID > reference > empty."""

    def test_url_from_id_when_original_is_login(self):
        """When urlproceso is 'Login', rebuild from id_del_proceso."""
        client = SecopClient(domain="www.datos.gov.co", app_token=None)
        row = {
            "urlproceso": "Login",
            "id_del_proceso": "CO1.NTC.2597221",
            "referencia_del_proceso": "SI- con espacios",
        }
        result = client._normalize_row("p6dx-8zbt", row)
        assert "CO1.NTC.2597221" in result["urlproceso"]
        assert "Login" not in result["urlproceso"]
        assert "SI-" not in result["urlproceso"]

    def test_url_from_id_when_original_is_empty(self):
        """When urlproceso is empty, rebuild from id_del_proceso."""
        client = SecopClient(domain="www.datos.gov.co", app_token=None)
        row = {
            "urlproceso": "",
            "id_del_proceso": "CO1.REQ.9912169",
            "referencia_del_proceso": "CD-08-2026-5534",
        }
        result = client._normalize_row("p6dx-8zbt", row)
        assert "CO1.REQ.9912169" in result["urlproceso"]

    def test_url_from_id_contrato_when_original_is_login(self):
        """When urlproceso is 'Login' on contratos, rebuild from id_contrato."""
        client = SecopClient(domain="www.datos.gov.co", app_token=None)
        row = {
            "urlproceso": "Login",
            "id_contrato": "CO1.PCCNTR.1000001",
            "referencia_del_contrato": "0070007831 copia",
        }
        result = client._normalize_row("jbjy-vk9h", row)
        assert "CO1.PCCNTR.1000001" in result["urlproceso"]

    def test_url_fallback_to_reference_when_no_id(self):
        """When no id_del_proceso, fallback to referencia (url-encoded)."""
        client = SecopClient(domain="www.datos.gov.co", app_token=None)
        row = {
            "urlproceso": "Login",
            "referencia_del_proceso": "CO 32 DE 2024 copia",
        }
        result = client._normalize_row("p6dx-8zbt", row)
        # Reference should be url-encoded (spaces → %20)
        assert "CO%2032%20DE%202024%20copia" in result["urlproceso"]

    def test_url_empty_when_no_id_or_reference(self):
        """When no id or reference, url should be empty."""
        client = SecopClient(domain="www.datos.gov.co", app_token=None)
        row = {
            "urlproceso": "Login",
        }
        result = client._normalize_row("p6dx-8zbt", row)
        assert result["urlproceso"] == ""

    def test_url_preserved_when_valid(self):
        """When urlproceso is valid, keep it untouched."""
        client = SecopClient(domain="www.datos.gov.co", app_token=None)
        original = "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.123"
        row = {
            "urlproceso": original,
            "id_del_proceso": "CO1.NTC.123",
        }
        result = client._normalize_row("p6dx-8zbt", row)
        assert result["urlproceso"] == original

    def test_money_fields_normalized_alongside_url(self):
        """URL repair doesn't break money normalization."""
        client = SecopClient(domain="www.datos.gov.co", app_token=None)
        row = {
            "urlproceso": "",
            "id_del_proceso": "CO1.NTC.123",
            "precio_base": "$1,500,000.99",
        }
        result = client._normalize_row("p6dx-8zbt", row)
        assert result["precio_base"] == 1500000.99
        assert "CO1.NTC.123" in result["urlproceso"]

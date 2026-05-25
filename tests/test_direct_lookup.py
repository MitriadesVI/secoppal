"""Tests for app.core.direct_lookup — Feature A1: direct process/contract lookup by ID or URL."""
from __future__ import annotations

import pytest

from app.core.direct_lookup import detect_direct_lookup, dataset_for_id, PREFIX_DATASET


class TestDetection:
    """detect_direct_lookup() — detection and extraction."""

    # ── Pure IDs ──────────────────────────────────────────────────────

    def test_detect_id_co1_ntc(self):
        assert detect_direct_lookup("CO1.NTC.2597221") == ("CO1.NTC.2597221", "id")

    def test_detect_id_co1_req(self):
        assert detect_direct_lookup("CO1.REQ.2577563") == ("CO1.REQ.2577563", "id")

    def test_detect_id_co1_bdos(self):
        assert detect_direct_lookup("CO1.BDOS.848814") == ("CO1.BDOS.848814", "id")

    def test_detect_id_co1_pccntr(self):
        assert detect_direct_lookup("CO1.PCCNTR.1000001") == ("CO1.PCCNTR.1000001", "id")

    def test_detect_id_co1_cons(self):
        assert detect_direct_lookup("CO1.CONS.123456") == ("CO1.CONS.123456", "id")

    # ── URLs ──────────────────────────────────────────────────────────

    def test_detect_url_noticeuid(self):
        url = "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.2597221"
        assert detect_direct_lookup(url) == ("CO1.NTC.2597221", "url")

    def test_detect_url_with_extra_params(self):
        url = (
            "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index"
            "?noticeUID=CO1.NTC.2597221&isFromPublicArea=True&isModal=true"
        )
        assert detect_direct_lookup(url) == ("CO1.NTC.2597221", "url")

    # ── ID embedded in natural text (non-strict detection) ────────────

    def test_detect_id_in_natural_text_prefix(self):
        assert detect_direct_lookup("muéstrame CO1.NTC.123") == ("CO1.NTC.123", "id")

    def test_detect_id_in_natural_text_suffix(self):
        assert detect_direct_lookup("info de CO1.NTC.123 ahora") == ("CO1.NTC.123", "id")

    def test_detect_id_in_natural_text_mid(self):
        assert detect_direct_lookup("el proceso CO1.NTC.123 que pediste") == ("CO1.NTC.123", "id")

    # ── Multiple IDs → no bypass ──────────────────────────────────────

    def test_detect_multiple_ids_no_bypass(self):
        assert detect_direct_lookup("compara CO1.NTC.1 y CO1.NTC.2") == (None, None)

    def test_detect_id_and_url_no_bypass(self):
        assert detect_direct_lookup(
            "mira CO1.NTC.123 y https://community.secop.gov.co/...?noticeUID=CO1.NTC.456"
        ) == (None, None)

    # ── Negatives ─────────────────────────────────────────────────────

    def test_no_detect_normal_query(self):
        assert detect_direct_lookup("mantenimiento vial en Bolívar") == (None, None)

    def test_no_detect_partial_id(self):
        assert detect_direct_lookup("CO1") == (None, None)
        assert detect_direct_lookup("NTC.2597221") == (None, None)

    def test_no_detect_other_url(self):
        assert detect_direct_lookup("https://google.com") == (None, None)

    def test_no_detect_empty(self):
        assert detect_direct_lookup("") == (None, None)


class TestPrefixMapping:
    """dataset_for_id() — prefix → dataset routing."""

    def test_ntc_routes_to_procesos(self):
        assert dataset_for_id("CO1.NTC.2597221") == "p6dx-8zbt"

    def test_req_routes_to_procesos(self):
        assert dataset_for_id("CO1.REQ.2577563") == "p6dx-8zbt"

    def test_bdos_routes_to_procesos(self):
        assert dataset_for_id("CO1.BDOS.848814") == "p6dx-8zbt"

    def test_cons_routes_to_procesos(self):
        assert dataset_for_id("CO1.CONS.123456") == "p6dx-8zbt"

    def test_pccntr_routes_to_contratos(self):
        assert dataset_for_id("CO1.PCCNTR.1000001") == "jbjy-vk9h"

    def test_unknown_prefix_returns_none(self):
        assert dataset_for_id("CO1.UNKNOWN.123") is None

    def test_invalid_id_returns_none(self):
        assert dataset_for_id("not a valid id") is None


class TestPrefixDataset:
    """PREFIX_DATASET constant — ensures mapping completeness."""

    def test_known_prefixes_both_datasets(self):
        """All known prefixes map to a valid dataset."""
        valid_datasets = {"p6dx-8zbt", "jbjy-vk9h"}
        for prefix, ds in PREFIX_DATASET.items():
            assert ds in valid_datasets, f"Prefix {prefix} maps to unknown dataset {ds}"

    def test_pccntr_is_only_contratos_prefix(self):
        """Only PCCNTR should map to contratos."""
        for prefix, ds in PREFIX_DATASET.items():
            if prefix == "PCCNTR":
                assert ds == "jbjy-vk9h"
            else:
                assert ds == "p6dx-8zbt", f"Non-PCCNTR prefix {prefix} mapped to {ds}"

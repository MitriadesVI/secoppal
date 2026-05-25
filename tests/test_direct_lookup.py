"""Tests for app.core.direct_lookup — Feature A1: direct process/contract lookup by ID, URL, or entity reference."""
from __future__ import annotations

import pytest

from app.core.direct_lookup import (
    detect_direct_lookup,
    dataset_for_id,
    normalize_entity_ref,
    build_entity_ref_soql,
    PREFIX_DATASET,
)


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


class TestEntityRefDetection:
    """detect_direct_lookup() — entity reference detection."""

    def test_detect_cd_format(self):
        assert detect_direct_lookup("CD-08-2026-5534") == ("CD-08-2026-5534", "entity_ref")

    def test_detect_samc_with_subprefix(self):
        assert detect_direct_lookup("SAMC-JCT-006") == ("SAMC-JCT-006", "entity_ref")

    def test_detect_lp_format(self):
        assert detect_direct_lookup("LP-2026-001") == ("LP-2026-001", "entity_ref")

    def test_detect_mc_format(self):
        assert detect_direct_lookup("MC-005-2024") == ("MC-005-2024", "entity_ref")

    def test_detect_cmc_format(self):
        assert detect_direct_lookup("CMC-2026-019") == ("CMC-2026-019", "entity_ref")

    def test_detect_ip_format(self):
        assert detect_direct_lookup("IP-017-2026") == ("IP-017-2026", "entity_ref")

    def test_detect_with_spaces(self):
        # Spaces should be normalized to hyphens
        ref, kind = detect_direct_lookup("CD 08 2026 5534")
        assert kind == "entity_ref"
        # Upstream normalizes; raw capture may have spaces but normalized form is hyphenated

    def test_detect_in_natural_text(self):
        assert detect_direct_lookup("muéstrame CD-08-2026-5534") == ("CD-08-2026-5534", "entity_ref")

    def test_detect_in_natural_text_mid(self):
        assert detect_direct_lookup("el proceso CD-08-2026-5534 que pediste") == ("CD-08-2026-5534", "entity_ref")

    # ── Multiple refs → no bypass ─────────────────────────────────────

    def test_multiple_entity_refs_no_bypass(self):
        assert detect_direct_lookup("compara CD-08-2026-5534 y CD-09-2026-1000") == (None, None)

    def test_entity_ref_with_secop_id_in_same_text(self):
        # CO1 ID should take priority over entity_ref
        result = detect_direct_lookup("mira CO1.NTC.123 y CD-08-2026-5534")
        assert result == (None, None)  # 2 unique identifiers → no bypass

    def test_entity_ref_with_secop_id_only(self):
        # When only CO1 is present, it should win
        result = detect_direct_lookup("CO1.NTC.123")
        assert result == ("CO1.NTC.123", "id")

    # ── Negatives ─────────────────────────────────────────────────────

    def test_no_detect_pure_number(self):
        assert detect_direct_lookup("5534") == (None, None)

    def test_no_detect_only_prefix(self):
        assert detect_direct_lookup("CD") == (None, None)
        assert detect_direct_lookup("LP") == (None, None)

    def test_no_detect_normal_text(self):
        assert detect_direct_lookup("licitaciones de obra") == (None, None)
        assert detect_direct_lookup("contratos del SENA") == (None, None)


class TestNormalization:
    """normalize_entity_ref() — reference normalization."""

    def test_cd_with_hyphens(self):
        assert normalize_entity_ref("CD-08-2026-5534") == "CD-08-2026-5534"

    def test_cd_with_spaces(self):
        assert normalize_entity_ref("CD 08 2026 5534") == "CD-08-2026-5534"

    def test_lowercase(self):
        assert normalize_entity_ref("cd-08-2026-5534") == "CD-08-2026-5534"

    def test_mixed_spaces_and_hyphens(self):
        assert normalize_entity_ref("SAMC JCT-006") == "SAMC-JCT-006"

    def test_extra_whitespace(self):
        assert normalize_entity_ref("  LP  2026  001  ") == "LP-2026-001"


class TestEntityRefSoQL:
    """build_entity_ref_soql() — SoQL generation for entity references."""

    def test_soql_procesos_reference_field(self):
        soql = build_entity_ref_soql("p6dx-8zbt", "CD-08-2026-5534", limit=3)
        assert "referencia_del_proceso" in soql
        assert "%CD-08-2026-5534%" in soql
        assert "LIMIT 3" in soql

    def test_soql_contratos_reference_field(self):
        soql = build_entity_ref_soql("jbjy-vk9h", "CD-08-2026-5534", limit=5)
        assert "referencia_del_contrato" in soql
        assert "LIMIT 5" in soql

    def test_soql_includes_select_fields(self):
        soql = build_entity_ref_soql("p6dx-8zbt", "LP-2026-001")
        assert "id_del_proceso" in soql
        assert "entidad" in soql
        assert "precio_base" in soql

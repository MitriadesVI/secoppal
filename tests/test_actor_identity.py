"""AP1 — identidad tipada de actor: invariantes y casos reales.

Los invariantes que este archivo congela (INV-ID, INV-ID-TYPE,
INV-NO-LENGTH, INV-DV, INV-PROVENANCE-SEPARATION, INV-PURE) están
definidos en docs/audits/2026-08-18_ACTOR_PROFILE_DIAGNOSTIC.md §17.1.
"""
from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

from app.core.actor_identity import (
    CHECK_DIGIT_WEIGHTS,
    IdentityKey,
    IdentityMatch,
    compute_check_digit,
    normalize_identity,
)


# ---------------------------------------------------------------------------
# Dígito de verificación (módulo 11).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("base", "expected"),
    [
        ("860034594", 1),   # FUNDACIÓN real: 860.034.594-1
        ("900123456", 8),   # el DV correcto es 8, NO el 7 del corpus de money
        ("114083513", 6),
        ("800197268", 4),
        ("830053105", 3),
    ],
)
def test_check_digit_follows_the_official_modulo_11_rule(base, expected):
    assert compute_check_digit(base) == expected


@pytest.mark.parametrize(
    "bad", [None, "", "abc", "860-034", 860034594, "1" * (len(CHECK_DIGIT_WEIGHTS) + 1)],
)
def test_check_digit_returns_none_instead_of_inventing_one(bad):
    """Un DV inventado sería peor que ninguno: devuelve None, no lanza."""
    assert compute_check_digit(bad) is None


# ---------------------------------------------------------------------------
# INV-ID — la misma identidad escrita distinto produce la misma llave.
# ---------------------------------------------------------------------------


_SAME_NIT_WRITTEN_FIVE_WAYS = [
    "860034594",        # RUP: limpio, sin DV
    "860034594-1",      # con guion
    "860.034.594-1",    # con puntos y DV
    "8,600,345,941",    # responsabilidad fiscal: comas y DV pegado
    "860034594 1",      # DV separado por espacio
]


@pytest.mark.parametrize("raw", _SAME_NIT_WRITTEN_FIVE_WAYS)
def test_inv_id_every_written_form_of_one_nit_yields_the_same_key(raw):
    match = normalize_identity(raw, document_type="NIT")
    assert match.key == IdentityKey(document_type="NIT", base_number="860034594")


def test_inv_id_all_five_forms_collapse_to_exactly_one_key():
    keys = {normalize_identity(r, document_type="NIT").key
            for r in _SAME_NIT_WRITTEN_FIVE_WAYS}
    assert len(keys) == 1


def test_nit_without_declared_type_is_still_recognised_via_check_digit():
    """Sin tipo declarado, las formas con DV siguen convergiendo."""
    keys = {normalize_identity(r).key for r in _SAME_NIT_WRITTEN_FIVE_WAYS[1:]}
    assert keys == {IdentityKey(document_type="NIT", base_number="860034594")}


# ---------------------------------------------------------------------------
# INV-DV — cuándo se separa el dígito de verificación, y cuándo no.
# ---------------------------------------------------------------------------


def test_inv_dv_explicit_separator_is_respected_without_revalidating():
    """La fuente ya declaró dónde termina el base; no se le contradice.

    ``900.123.456-7`` trae un DV que el módulo 11 no confirma (el correcto
    es 8), y aun así se respeta: sobrescribir una declaración explícita de
    la fuente con aritmética propia inventaría un documento distinto del
    que la fuente entregó. El corpus de money v2 ya congela esta forma.
    """
    match = normalize_identity("900.123.456-7")
    assert match.base_number == "900123456"
    assert match.check_digit == "7"


def test_inv_dv_bare_sequence_is_split_only_when_modulo_11_confirms_it():
    match = normalize_identity("8600345941")
    assert match.base_number == "860034594"
    assert match.check_digit == "1"


def test_inv_dv_bare_sequence_stays_whole_when_the_check_digit_fails():
    """Sin confirmación no se inventa una separación base/DV."""
    match = normalize_identity("9001234567")  # el DV de 900123456 es 8, no 7
    assert match.base_number == "9001234567"
    assert match.check_digit is None


def test_inv_dv_reason_codes_distinguish_an_explicit_dv_from_a_validated_one():
    """AP2 necesita saber si el DV vino declarado o si lo confirmó el módulo 11.

    Ambos casos infieren NIT y ambos son evidencia positiva, pero no son la
    misma evidencia: uno lo afirmó la fuente, al otro lo respalda la
    aritmética. La confianza no los separa, los códigos de razón sí.
    """
    explicit = normalize_identity("900.123.456-7")   # DV declarado, no confirmado
    validated = normalize_identity("8600345941")     # DV confirmado por módulo 11

    assert explicit.confidence == validated.confidence == "inferred_from_check_digit"
    assert "IDENTITY_CHECK_DIGIT_EXPLICIT" in explicit.reason_codes
    assert "IDENTITY_CHECK_DIGIT_VALIDATED" not in explicit.reason_codes
    assert "IDENTITY_CHECK_DIGIT_VALIDATED" in validated.reason_codes
    assert "IDENTITY_CHECK_DIGIT_EXPLICIT" not in validated.reason_codes


def test_inv_dv_a_real_cedula_is_never_mutilated_into_base_plus_dv():
    """1140835135 es una cédula: dv(114083513) == 6 != 5, así que no se parte.

    Es la otra cara de la regla anterior — la misma validación que rescata
    el NIT desnudo protege a la cédula de perder su último dígito.
    """
    match = normalize_identity("1140835135", document_type="CC")
    assert match.key == IdentityKey(document_type="CC", base_number="1140835135")
    assert match.check_digit is None


def test_missing_check_digit_is_not_evidence_of_a_cedula():
    """Una cédula no tiene DV inválido: simplemente no tiene DV."""
    match = normalize_identity("900264685")  # RUP limpio, sin DV
    assert match.document_type == "UNKNOWN"
    assert match.confidence == "undetermined"


# ---------------------------------------------------------------------------
# REGRESIÓN — "DV perdido sin separador" (INCIDENTAL_FINDING #2 de AP0).
# ---------------------------------------------------------------------------


def test_regression_bare_nit_with_dv_matches_the_same_nit_without_dv():
    """AP0 documentó que ``8600345941`` no coincidía con ``860034594``.

    En la capa pura ya coinciden. AP1 no toca el parser ni el routing: la
    corrección de esa ruta pertenece a AP2, que consumirá esta primitive.
    """
    with_dv = normalize_identity("8600345941", document_type="NIT")
    without_dv = normalize_identity("860034594", document_type="NIT")
    formatted = normalize_identity("860.034.594-1", document_type="NIT")

    assert with_dv.key == without_dv.key == formatted.key


# ---------------------------------------------------------------------------
# INV-ID-TYPE — el tipo documental separa identidades.
# ---------------------------------------------------------------------------


def test_inv_id_type_same_number_with_different_types_are_different_actors():
    assert IdentityKey("NIT", "123456789") != IdentityKey("CC", "123456789")


def test_inv_id_type_declared_types_do_not_merge_through_normalization():
    as_nit = normalize_identity("123456789", document_type="NIT")
    as_cc = normalize_identity("123456789", document_type="CC")
    assert as_nit.key != as_cc.key
    assert len({as_nit.key, as_cc.key}) == 2


def test_inv_id_type_unknown_does_not_collapse_into_a_typed_key():
    """UNKNOWN es un tipo propio, no un comodín que iguale a NIT o CC."""
    untyped = normalize_identity("900264685")
    typed = normalize_identity("900264685", document_type="NIT")
    assert untyped.key != typed.key


# ---------------------------------------------------------------------------
# INV-NO-LENGTH — la longitud nunca decide el tipo.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["7534386", "1140835135", "900264685", "12345678"])
def test_inv_no_length_bare_numbers_never_get_a_type_from_their_length(raw):
    match = normalize_identity(raw)
    assert match.confidence != "declared"
    if match.confidence == "inferred_from_check_digit":
        # La única inferencia admitida viene del DV, jamás del tamaño.
        assert match.check_digit is not None
    else:
        assert match.document_type == "UNKNOWN"


def test_inv_no_length_the_same_length_can_be_either_type_when_declared():
    """Diez dígitos son cédula o NIT según lo que declare la fuente."""
    assert normalize_identity("1140835135", document_type="CC").document_type == "CC"
    assert normalize_identity("1140835135", document_type="NIT").document_type == "NIT"


@pytest.mark.parametrize(
    "raw",
    [
        "1140835135",  # 10 dígitos, cédula real
        "9001234567",  # 10 dígitos, DV que no confirma
        "1234567890",  # 10 dígitos arbitrarios
        "123456789",   # 9 dígitos, la longitud "clásica" de NIT
        "1234567",     # 7 dígitos, la longitud "clásica" de cédula
    ],
)
def test_inv_no_length_no_digit_count_ever_produces_cc(raw):
    """Ninguna longitud, por sí sola, puede concluir que algo es cédula.

    CC solo aparece cuando la fuente lo declara. Si esta prueba falla, se
    introdujo una heurística de tamaño — el error que atribuye la actividad
    de una empresa a un ciudadano, y viceversa.
    """
    assert normalize_identity(raw).document_type != "CC"


def test_inv_no_length_two_numbers_of_equal_length_get_the_same_treatment():
    """La longitud no puede separar dos crudos que solo difieren en dígitos.

    Ambos tienen diez dígitos: si uno acabara clasificado por tamaño y el
    otro no, la longitud estaría actuando como discriminador de tipo.
    """
    cedula = normalize_identity("1140835135")
    other = normalize_identity("1234567890")
    assert cedula.document_type == other.document_type == "UNKNOWN"
    assert cedula.confidence == other.confidence == "undetermined"


# ---------------------------------------------------------------------------
# INV-PROVENANCE-SEPARATION — la procedencia no altera la igualdad.
# ---------------------------------------------------------------------------


def test_inv_provenance_different_sources_share_one_key():
    from_rup = normalize_identity("860034594", document_type="NIT", source="rup")
    from_fiscal = normalize_identity(
        "8,600,345,941", document_type="NIT", source="responsabilidad_fiscal",
    )
    assert from_rup.key == from_fiscal.key
    assert from_rup.source != from_fiscal.source


def test_inv_provenance_different_raw_values_share_one_key():
    a = normalize_identity("860.034.594-1", document_type="NIT")
    b = normalize_identity("860034594", document_type="NIT")
    assert a.key == b.key
    assert a.raw_value != b.raw_value


def test_inv_provenance_check_digit_is_not_part_of_the_key():
    with_dv = normalize_identity("860034594-1", document_type="NIT")
    without_dv = normalize_identity("860034594", document_type="NIT")
    assert with_dv.check_digit == "1" and without_dv.check_digit is None
    assert with_dv.key == without_dv.key


def test_inv_provenance_key_carries_only_type_and_base_number():
    """Estructural: si la llave ganara un campo de procedencia, esto rompe."""
    assert IdentityKey.__dataclass_fields__.keys() == {"document_type", "base_number"}


def test_key_is_hashable_and_usable_as_a_join_key():
    index = {normalize_identity("860.034.594-1", document_type="NIT").key: "FUNDACARIBE"}
    assert index[normalize_identity("8600345941", document_type="NIT").key] == "FUNDACARIBE"


# ---------------------------------------------------------------------------
# Tipo declarado por la fuente.
# ---------------------------------------------------------------------------


def test_declared_type_wins_over_check_digit_inference():
    """Una columna explícita es mejor evidencia que el módulo 11."""
    match = normalize_identity("8600345941", document_type="CC")
    assert match.document_type == "CC"
    assert match.confidence == "declared"


def test_declared_type_is_case_and_space_insensitive():
    assert normalize_identity("900264685", document_type=" nit ").document_type == "NIT"


def test_unrecognised_declared_type_is_not_guessed_at():
    """Un tipo desconocido se ignora como declaración y queda en las razones."""
    match = normalize_identity("900264685", document_type="PASAPORTE")
    assert match.document_type == "UNKNOWN"
    assert "IDENTITY_INVALID_TYPE" in match.reason_codes


def test_declared_unknown_falls_back_to_check_digit_evidence():
    match = normalize_identity("8600345941", document_type="UNKNOWN")
    assert match.document_type == "NIT"
    assert match.confidence == "inferred_from_check_digit"


# ---------------------------------------------------------------------------
# Entradas inservibles — se declara "no sé", no se fabrica una llave.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw", [None, "", "   ", 860034594, 860034594.0, True, b"860034594", ["860034594"]],
)
def test_unusable_input_yields_no_key(raw):
    match = normalize_identity(raw)
    assert match.key is None
    assert match.is_resolved is False
    assert match.confidence == "undetermined"
    assert match.base_number is None
    assert match.document_type == "UNKNOWN"


@pytest.mark.parametrize("sentinel", ["NO DEFINIDO", "no definido", "N/A", "SIN DATO", "-"])
def test_source_sentinels_never_become_an_actor(sentinel):
    """SECOP Integrado usa "NO DEFINIDO"; tratarlo como documento fusionaría
    en un solo actor a todos los registros sin dato."""
    match = normalize_identity(sentinel)
    assert match.key is None
    assert "IDENTITY_SENTINEL_DISCARDED" in match.reason_codes


@pytest.mark.parametrize("raw", ["abc", "N.I.T.", "---", "$1.500.000.000 COP"[:4]])
def test_text_without_digits_yields_no_key(raw):
    assert normalize_identity(raw).key is None


@pytest.mark.parametrize("raw", ["12", "1234", "123456789012345"])
def test_out_of_sanity_range_yields_no_key(raw):
    match = normalize_identity(raw)
    assert match.key is None
    assert "IDENTITY_OUT_OF_RANGE" in match.reason_codes


def test_range_guard_is_sanity_not_type_discrimination():
    """Un documento dentro del rango se acepta sin que el rango elija tipo."""
    match = normalize_identity("1234567", document_type="CC")
    assert match.key == IdentityKey("CC", "1234567")


def test_invalid_key_construction_is_rejected_loudly():
    with pytest.raises(ValueError):
        IdentityKey("NIT", "860.034.594")
    with pytest.raises(ValueError):
        IdentityKey("NIT", "")
    with pytest.raises(ValueError):
        IdentityKey("PASAPORTE", "860034594")


# ---------------------------------------------------------------------------
# Idempotencia y auditabilidad.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw", ["860034594", "860.034.594-1", "8600345941", "1140835135", "NO DEFINIDO", ""],
)
def test_normalizing_an_already_normalized_value_is_idempotent(raw):
    first = normalize_identity(raw, document_type="NIT")
    if first.key is None:
        return
    second = normalize_identity(first.base_number, document_type=first.document_type)
    assert second.key == first.key


def test_raw_value_is_preserved_verbatim_for_auditing():
    match = normalize_identity("  860.034.594-1  ", document_type="NIT")
    assert match.raw_value == "860.034.594-1"


def test_reason_codes_are_stable_upper_snake_case_and_deduplicated():
    match = normalize_identity("8600345941")
    assert match.reason_codes
    assert len(set(match.reason_codes)) == len(match.reason_codes)
    for code in match.reason_codes:
        assert code == code.upper()
        assert code.replace("_", "").isalnum()


def test_match_is_frozen():
    match = normalize_identity("860034594", document_type="NIT")
    with pytest.raises(Exception):
        match.key = None  # type: ignore[misc]


def test_public_surface_is_what_ap2_will_consume():
    assert isinstance(normalize_identity("860034594", document_type="NIT"), IdentityMatch)


# ---------------------------------------------------------------------------
# INV-PURE — sin red, base de datos, filesystem, reloj ni configuración.
# ---------------------------------------------------------------------------


_FORBIDDEN_IMPORT_TOKENS = (
    "socket", "http", "httpx", "requests", "sodapy", "sqlite3", "urllib",
    "datetime", "time", "os", "pathlib", "random", "app.",
)


def test_inv_pure_module_imports_only_stdlib_primitives():
    """El módulo no importa nada de ``app`` ni nada con efectos."""
    tree = ast.parse(Path("app/core/actor_identity.py").read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    assert sorted(imported) == ["__future__", "dataclasses", "re", "typing"], imported


def test_inv_pure_cold_import_pulls_in_no_app_or_io_modules():
    code = r'''import json, sys
import app.core.actor_identity
tokens = ("socket", "httpx", "requests", "sodapy", "sqlite3", "burr",
          "streamlit", "fastapi", "openai", "rapidfuzz")
print(json.dumps(sorted(
    name for name in sys.modules
    if any(t in name.casefold() for t in tokens)
)))
'''
    completed = subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True,
    )
    assert json.loads(completed.stdout) == []


def test_inv_pure_normalization_is_deterministic_across_calls():
    calls = [normalize_identity("8,600,345,941", document_type="NIT") for _ in range(5)]
    assert len({(c.key, c.check_digit, c.reason_codes) for c in calls}) == 1

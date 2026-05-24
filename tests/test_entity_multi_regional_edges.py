"""Test ENTITY-MULTI-REGIONAL idempotency and edge cases."""
from app.data.regional_entities import resolve_regional


def test_sena_antioquia_already_regional_is_idempotent():
    """If entidad_resolved is already a regional, resolve_regional returns (None, None)
    (no double-resolution). This ensures idempotency."""
    # "SENA REGIONAL ANTIOQUIA" should resolve to None because the stored name
    # is already a regional (not the central "SENA").
    regional, canonical = resolve_regional("SENA REGIONAL ANTIOQUIA", "Antioquia")
    # Should return (None, None) because the resolved entity IS the regional
    assert regional is None
    assert canonical is None


def test_icbf_regional_magdalena_is_idempotent():
    """ICBF REGIONAL MAGDALENA + Magdalena → (None, None) (already regional)."""
    regional, canonical = resolve_regional("ICBF REGIONAL MAGDALENA", "Magdalena")
    assert regional is None
    assert canonical is None


def test_central_entity_still_resolves():
    """Central entity names (not already regional) DO resolve."""
    regional, canonical = resolve_regional("SENA", "Antioquia")
    assert regional == "SENA REGIONAL ANTIOQUIA"
    assert canonical == "SENA"

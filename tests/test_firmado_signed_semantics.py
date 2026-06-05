"""FIRMADO-NO-ESTADO — "firmado" ⇔ fecha_de_firma, no un estado_contrato.

Bug (dogfood 2026-06-05): "que contratos ... se han firmado en mayo?" metía
estado=Aprobado vía ESTADO_CONTRATO_SYNONYMS, restringiendo a 184K de ~5.6M
contratos firmados → 9 resultados en vez de los firmados de mayo reales.

Validado contra SECOP: los estados con fecha_de_firma (~100%) son En ejecución,
Cerrado, Modificado, terminado, Aprobado, cedido, Suspendido, Prorrogado; los
sin firma (~0%) son Borrador, Cancelado, enviado Proveedor, En aprobación.
Por eso filtrar por fecha_de_firma captura exactamente los firmados — enumerar
estados es innecesario y frágil. Contradecía el invariante documentado en
estado_families.py e intent_vocabulary.py.
"""
from __future__ import annotations

from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter


def _plan(q: str) -> dict:
    return compute_query_plan(dict(QueryRouter().parse(q).params), q, enabled_rules=None).params


def _objeto(p: dict) -> list[str]:
    return [t for t in (p.get("objeto") or []) if isinstance(t, str)]


# ── EL CASO DE LA SESIÓN ──────────────────────────────────────────────────

def test_firmados_en_mayo_no_aplica_estado_filtra_fecha():
    p = _plan("que contratos de adulto mayor por mas de 200 millones se han firmado en mayo?")
    assert p.get("dataset") == "contratos"
    assert p.get("estado") is None          # ya NO mete Aprobado
    assert p.get("estado_field") is None
    assert p.get("fecha_desde") == "2026-05-01"  # "en mayo" → fecha_de_firma
    assert p.get("fecha_hasta") == "2026-05-31"
    assert p.get("valor_min") == 200_000_000
    assert _objeto(p) == ["adulto_mayor"]


def test_firmados_no_leak_al_objeto():
    """Ninguna variante de 'firmado' debe contaminar el objeto."""
    for q in (
        "contratos de adulto mayor firmados en mayo",
        "contratos de adulto mayor firmadas",
        "contratos de mantenimiento que se firmaron",
    ):
        p = _plan(q)
        obj = " ".join(_objeto(p))
        assert "firmad" not in obj, f"leak en {q!r}: objeto={_objeto(p)}"
        assert p.get("dataset") == "contratos"


# ── REGRESIÓN: estados explícitos siguen mapeando ─────────────────────────

def test_firmados_mas_estado_explicito_usa_el_estado():
    """'firmados cerrados' / 'firmados en ejecución' → gana el estado explícito."""
    cerrados = _plan("contratos firmados cerrados")
    assert cerrados.get("estado") == "Cerrado"

    ejec = _plan("contratos firmados que esten en ejecucion de mantenimiento")
    assert ejec.get("estado_family") == "contrato_activo"
    assert "En ejecución" in (ejec.get("estado_contrato") or [ejec.get("estado")])


def test_estados_contrato_explicitos_intactos():
    assert _plan("contratos en ejecucion de mantenimiento").get("estado") == "En ejecución"
    assert _plan("contratos cerrados").get("estado") == "Cerrado"
    assert _plan("contratos terminados").get("estado") == "terminado"

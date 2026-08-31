"""RADAR-SECOP-ACCESS-V2 — una oportunidad nunca queda sin vía a la fuente.

Dos afirmaciones distintas que estos tests mantienen separadas:

* "hay/no hay enlace público directo"  → es un hecho de los datos observados.
* "el proceso está en pre-publicación" → es un veredicto de etapa, y NO se
  deriva del prefijo del id. La evidencia está en
  ``docs/audits/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE.md``: los 9.096.877
  procesos de p6dx-8zbt llevan id ``CO1.REQ.*`` y el 98,97% trae vista pública.

Y la regla que hace segura la vía: el enlace directo se OBSERVA, nunca se
construye, y solo cuenta si es de SECOP y muestra un proceso.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from app.core.secop_access import (
    SECOP_PUBLIC_SEARCH_URL,
    resolve_secop_access,
)

_DIRECTA = (
    "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/"
    "Index?noticeUID=CO1.NTC.10541141"
)
_LOGIN = "https://community.secop.gov.co/STS/Users/Login/Index"


def _streamlit_source() -> str:
    return (Path(__file__).parents[1] / "streamlit_app.py").read_text()


# ── Caso A: hay enlace directo ──────────────────────────────────────────────


def test_observed_public_url_becomes_the_direct_link():
    acceso = resolve_secop_access(_DIRECTA, "ABC-123")
    assert acceso.kind == "direct"
    assert acceso.direct_url == _DIRECTA
    assert acceso.has_direct_link is True
    # La búsqueda sigue disponible aunque no se pinte: ninguna superficie tiene
    # que decidir por su cuenta qué hacer si el enlace directo falla.
    assert acceso.search_url == SECOP_PUBLIC_SEARCH_URL


def test_socrata_url_object_is_unwrapped_not_stringified():
    acceso = resolve_secop_access({"url": _DIRECTA, "description": "x"}, None)
    assert acceso.direct_url == _DIRECTA


# ── Caso B: no hay enlace directo, pero sí referencia ───────────────────────


def test_missing_link_with_reference_offers_the_official_search():
    acceso = resolve_secop_access(None, "ABC-123")
    assert acceso.kind == "search_with_reference"
    assert acceso.direct_url is None
    assert acceso.search_url == SECOP_PUBLIC_SEARCH_URL
    assert acceso.reference == "ABC-123"


def test_the_search_url_is_the_official_public_entry_point():
    """No es una ruta deducida: es el literal que SECOP publica él mismo en
    ``urlproceso`` para los registros sin vista pública propia."""
    assert SECOP_PUBLIC_SEARCH_URL == (
        "https://community.secop.gov.co/Public/Tendering/"
        "ContractNoticeManagement/Index"
        "?currentLanguage=es-CO&Page=login&Country=CO&SkinName=CCE"
    )


# ── Caso C: no hay ni enlace ni referencia ──────────────────────────────────


def test_without_link_or_reference_nothing_is_fabricated():
    acceso = resolve_secop_access(None, None)
    assert acceso.kind == "search"
    assert acceso.direct_url is None
    assert acceso.reference is None
    # La entrada genérica es más que un mensaje de error, y sigue siendo oficial.
    assert acceso.search_url == SECOP_PUBLIC_SEARCH_URL


@pytest.mark.parametrize("vacio", [None, "", "   ", {}, {"url": ""}, {"url": None}])
def test_empty_url_shapes_all_mean_the_same_absence(vacio):
    """Socrata entrega la columna de tipo ``url`` en varias formas; todas las
    vacías son el mismo hecho: no hay enlace."""
    assert resolve_secop_access(vacio, None).kind == "search"


@pytest.mark.parametrize("vacia", [None, "", "   "])
def test_blank_reference_is_absence_not_a_search_term(vacia):
    assert resolve_secop_access(None, vacia).reference is None


# ── CO1.REQ por sí solo no prueba nada ──────────────────────────────────────


def test_co1_req_id_alone_does_not_suppress_a_public_link():
    """El prefijo del id NO es la señal. Una fila CO1.REQ con URL pública —el
    98,97% del dataset— conserva su enlace directo."""
    acceso = resolve_secop_access(_DIRECTA, "CO1.REQ.2577563")
    assert acceso.kind == "direct"
    assert acceso.direct_url == _DIRECTA


def test_no_direct_url_is_fabricated_from_an_identifier():
    """§4: prohibido convertir CO1.REQ.123 en un OpportunityDetail."""
    acceso = resolve_secop_access(None, "CO1.REQ.123")
    assert acceso.direct_url is None
    assert "CO1.REQ.123" not in acceso.search_url
    assert "OpportunityDetail" not in acceso.search_url


def test_the_module_never_builds_a_detail_url():
    """La prohibición se verifica en el código, no solo en un caso de prueba.

    Se miran las cadenas EJECUTABLES y no los docstrings: que la prosa nombre
    ``OpportunityDetail`` para explicar por qué NO se enumeran rutas de ficha es
    lo contrario del defecto que este test busca.
    """
    arbol = ast.parse(
        (Path(__file__).parents[1] / "app/core/secop_access.py").read_text()
    )
    docstrings = {
        ast.get_docstring(node, clean=False)
        for node in ast.walk(arbol)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef))
    }
    literales = {
        node.value
        for node in ast.walk(arbol)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    } - docstrings
    assert literales, "no se encontraron cadenas ejecutables"
    for texto in literales:
        assert "OpportunityDetail" not in texto
        assert "noticeUID" not in texto


# ── Seguridad: lo que no puede llegar a un enlace de confianza ──────────────


@pytest.mark.parametrize(
    "hostil",
    [
        "javascript:alert(1)",
        "data:text/html,<script>alert(1)</script>",
        "{'url': 'https://community.secop.gov.co/x'}",
        "//community.secop.gov.co/x",
        "ftp://community.secop.gov.co/x",
        "https://evil.example.com/Public/Tendering/OpportunityDetail/Index",
        "https://community.secop.gov.co.evil.example.com/x",
        # urlsplit no puede leerlas: ausencia, nunca una excepción que tumbe
        # el render de la tarjeta.
        "https://community.secop.gov.co:puerto/x",
        "https://[::1/x",
    ],
)
def test_hostile_or_foreign_urls_never_become_a_direct_link(hostil):
    acceso = resolve_secop_access(hostil, "ABC-123")
    assert acceso.direct_url is None
    assert acceso.kind == "search_with_reference"


@pytest.mark.parametrize(
    "no_es_ficha",
    [
        _LOGIN,
        "https://login.secop.gov.co/Public/Tendering/OpportunityDetail/Index",
        SECOP_PUBLIC_SEARCH_URL,
    ],
)
def test_login_and_search_pages_are_not_a_process_view(no_es_ficha):
    """Viven en el host bueno y no muestran NINGÚN proceso: ofrecerlas como
    "Abrir en SECOP" prometería una ficha que la página no tiene."""
    assert resolve_secop_access(no_es_ficha, "ABC-123").direct_url is None


# ── El caso contractual: la URL que SECOP publica cuando no hay ficha ───────
#
# SECOP entrega literalmente esta URL en `urlproceso` para los 4.130 contratos
# de jbjy-vk9h sin vista pública. Es la trampa del bloque: vive en el host
# bueno, no dispara el filtro heredado `"Login" not in url` de `_safe_url`
# —el literal trae `Page=login` en minúscula— y aun así no muestra ningún
# contrato. Sin esta regla, los tres canales la rotularían como si fuera la
# ficha. Ver docs/audits/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE.md §5.


def test_the_official_search_url_is_never_its_own_direct_link():
    acceso = resolve_secop_access(SECOP_PUBLIC_SEARCH_URL, "ABC-123")
    assert acceso.kind == "search_with_reference"
    assert acceso.direct_url is None
    assert acceso.search_url == SECOP_PUBLIC_SEARCH_URL


def test_the_legacy_login_filter_alone_would_have_let_it_through():
    """Se documenta la trampa midiéndola, no describiéndola."""
    assert "Login" not in SECOP_PUBLIC_SEARCH_URL   # el filtro heredado falla
    assert "Page=login" in SECOP_PUBLIC_SEARCH_URL  # y este es el motivo
    assert resolve_secop_access(SECOP_PUBLIC_SEARCH_URL).direct_url is None


@pytest.mark.parametrize(
    "variante",
    [
        SECOP_PUBLIC_SEARCH_URL,
        SECOP_PUBLIC_SEARCH_URL.replace("Page=login", "page=LOGIN"),
        SECOP_PUBLIC_SEARCH_URL.replace("&Page=login", ""),
    ],
)
def test_the_contract_management_page_never_becomes_a_direct_link(variante):
    """Ni con otra capitalización ni sin el parámetro: sigue sin ser una ficha.

    La tercera variante NO lleva `Page=login`, así que la regla de query no la
    atrapa. La atrapa que tampoco es una ficha: no hay proceso que mostrar.
    """
    acceso = resolve_secop_access(variante, "ABC-123")
    assert acceso.direct_url is None, variante


# ── Wording: la etapa no se infiere del enlace ──────────────────────────────


def test_public_surfaces_no_longer_infer_prepublication_from_a_missing_link():
    """ADR-032 #4: la URL no determina publicación."""
    fuente = _streamlit_source()
    for inferencia in ("pre-publicación", "prepublicación", "requisición"):
        assert inferencia not in fuente, inferencia


def test_public_surfaces_say_what_is_actually_known():
    fuente = _streamlit_source()
    assert "Enlace directo no disponible en los datos observados." in fuente
    assert "Buscar en SECOP II" in fuente


def test_the_ui_imports_the_authority_instead_of_writing_the_url():
    """§6: fuente única. Ninguna superficie escribe la URL del buscador."""
    fuente = _streamlit_source()
    assert "from app.core.secop_access import resolve_secop_access" in fuente
    assert "ContractNoticeManagement" not in fuente


# ── Workspace legacy: un item viejo sin secop_url sigue siendo útil ─────────


def test_legacy_workspace_item_without_url_still_reaches_secop():
    """§10: sin migración obligatoria y sin fingir que se observó una URL."""
    metadata = {  # snapshot v1 guardado antes de este bloque
        "schema_version": 1,
        "source": "radar",
        "title": "ATENCION ADULTO MAYOR",
        "reference": "LP-004-2026",
    }
    acceso = resolve_secop_access(metadata.get("secop_url"),
                                  metadata.get("reference"))
    assert acceso.kind == "search_with_reference"
    assert acceso.reference == "LP-004-2026"
    assert acceso.search_url == SECOP_PUBLIC_SEARCH_URL
    # El snapshot NO se toca para fingir un enlace observado.
    assert "secop_url" not in metadata

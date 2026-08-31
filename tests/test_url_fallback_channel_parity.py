"""ADR-027 — paridad de canales para la vía a SECOP.

ADR-027 estableció el PRINCIPIO que sigue vigente: los tres canales dicen lo
mismo sobre cómo llegar a la fuente; ninguno omite la línea en silencio. Lo que
cambió es QUÉ dicen.

ADR-027 justificaba la nota con "CO1.REQ.* = requisición pre-publicación sin
link". El censo del 2026-08-31 falsificó esa causa: los 9.096.877 procesos de
p6dx-8zbt llevan ese prefijo y el 98,97% SÍ tiene vista pública
(``docs/audits/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE.md``). Así que estos tests
congelan dos cosas distintas:

1. **Paridad**: la misma decisión y la misma redacción en Streamlit, Telegram y
   WhatsApp, tomada por una sola autoridad (``app.core.secop_access``).
2. **Nada de inferencia de etapa**: sin enlace directo se ofrece una ACCIÓN —el
   buscador público oficial— y jamás un veredicto sobre si el proceso está
   publicado.
"""
from __future__ import annotations

import pytest

from app.core.formatter import Formatter
from app.core.secop_access import (
    SECOP_PUBLIC_SEARCH_URL,
    resolve_secop_access,
)

F = Formatter()
CANALES = ["telegram", "whatsapp"]
PROCESOS = "p6dx-8zbt"

_BUSCAR = "🔎 Buscar en SECOP II:"
_NTC = (
    "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/"
    "Index?noticeUID=CO1.NTC.10219520"
)
_AUTENTICACION = "https://community.secop.gov.co/STS/Users/Login/Index"


def _proceso(**overrides) -> dict:
    fila = {
        "nombre_del_procedimiento": "ATENCION ADULTO MAYOR",
        "entidad": "GOBERNACION X",
        "precio_base": 3_000_000_000,
        "estado_de_apertura_del_proceso": "Abierto",
        "fecha_de_publicacion_del": "2026-01-01",
        "urlproceso": "",
        "id_del_proceso": "CO1.REQ.999",
        "referencia_del_proceso": "LP-004-2026",
    }
    fila.update(overrides)
    return fila


def _mensaje(fila: dict, canal: str) -> str:
    return F.format_for_channel([fila], PROCESOS, canal)[0]


def _acceso_web(fila: dict):
    """Lo que decide la superficie web para esa misma fila.

    Se pasa por ``to_rows`` —el mismo camino que recorre la tarjeta de
    Streamlit— para que la comparación sea entre canales y no entre entradas.
    """
    row = F.to_rows([fila], PROCESOS)[0]
    return resolve_secop_access(
        row.get("url"),
        row.get("referencia_del_proceso") or row.get("referencia_del_contrato"),
    )


# ── Enlace directo observado ────────────────────────────────────────────────


@pytest.mark.parametrize("canal", CANALES)
def test_observed_public_url_is_a_direct_link_in_every_channel(canal):
    fila = _proceso(urlproceso={"url": _NTC})
    mensaje = _mensaje(fila, canal)
    assert _NTC in mensaje
    assert _BUSCAR not in mensaje


def test_the_web_agrees_that_it_is_a_direct_link():
    assert _acceso_web(_proceso(urlproceso={"url": _NTC})).kind == "direct"


# ── Sin enlace directo, con referencia ──────────────────────────────────────


@pytest.mark.parametrize("canal", CANALES)
def test_missing_link_offers_the_official_search_with_the_reference(canal):
    mensaje = _mensaje(_proceso(), canal)
    assert _BUSCAR in mensaje
    assert SECOP_PUBLIC_SEARCH_URL in mensaje
    assert "🏷️ Referencia: LP-004-2026" in mensaje


def test_the_web_agrees_and_carries_the_same_reference():
    acceso = _acceso_web(_proceso())
    assert acceso.kind == "search_with_reference"
    assert acceso.reference == "LP-004-2026"
    assert acceso.search_url == SECOP_PUBLIC_SEARCH_URL


# ── Sin enlace directo y sin referencia ─────────────────────────────────────


@pytest.mark.parametrize("canal", CANALES)
def test_without_reference_the_generic_search_is_still_offered(canal):
    mensaje = _mensaje(_proceso(referencia_del_proceso=""), canal)
    assert _BUSCAR in mensaje
    assert SECOP_PUBLIC_SEARCH_URL in mensaje
    assert "Referencia:" not in mensaje


def test_the_web_agrees_that_there_is_no_reference_to_show():
    assert _acceso_web(_proceso(referencia_del_proceso="")).kind == "search"


# ── La página de búsqueda contractual NO es una ficha ───────────────────────
#
# SECOP publica literalmente esta URL en `urlproceso` para los 4.130 contratos
# sin vista pública. Trae `Page=login` en minúscula, así que el filtro heredado
# `"Login" not in url` de `_safe_url` la deja pasar: sin la autoridad, los tres
# canales la rotularían como si fuera la ficha del contrato.


@pytest.mark.parametrize("canal", CANALES)
def test_the_contract_search_page_is_never_presented_as_a_process_view(canal):
    fila = _proceso(urlproceso={"url": SECOP_PUBLIC_SEARCH_URL})
    mensaje = _mensaje(fila, canal)
    assert _BUSCAR in mensaje
    assert "🏷️ Referencia: LP-004-2026" in mensaje
    # No se anuncia como enlace al proceso: la URL aparece bajo "Buscar".
    assert mensaje.count(SECOP_PUBLIC_SEARCH_URL) == 1
    assert f"🔗 {SECOP_PUBLIC_SEARCH_URL}" not in mensaje


def test_the_web_also_refuses_the_contract_search_page_as_a_direct_link():
    acceso = _acceso_web(_proceso(urlproceso={"url": SECOP_PUBLIC_SEARCH_URL}))
    assert acceso.kind == "search_with_reference"
    assert acceso.direct_url is None


# ── El prefijo del id no es criterio de presentación ────────────────────────


@pytest.mark.parametrize("canal", CANALES)
def test_co1_req_with_an_observed_url_keeps_its_direct_link(canal):
    """El 98,97% del dataset es exactamente este caso."""
    fila = _proceso(id_del_proceso="CO1.REQ.2577563", urlproceso={"url": _NTC})
    assert _NTC in _mensaje(fila, canal)


@pytest.mark.parametrize("canal", CANALES)
def test_co1_req_without_a_view_never_claims_prepublication(canal):
    fila = _proceso(id_del_proceso="CO1.REQ.999",
                    urlproceso={"url": _AUTENTICACION})
    mensaje = _mensaje(fila, canal)
    assert _BUSCAR in mensaje
    for inferencia in ("pre-publicación", "prepublicación", "requisición",
                       "Vista pública no disponible"):
        assert inferencia not in mensaje


@pytest.mark.parametrize("canal", CANALES)
@pytest.mark.parametrize("url", [_NTC, SECOP_PUBLIC_SEARCH_URL])
def test_the_id_prefix_is_not_a_presentation_criterion(canal, url):
    """Ante la misma url observada, el prefijo del id es irrelevante.

    Se parametriza con las dos urls que ``_safe_url`` deja pasar tal cual, para
    que lo medido sea la PRESENTACIÓN. Cuando la url llega vacía o es la
    pantalla de autenticación, ``_safe_url`` entra en su jerarquía heredada y sí
    ramifica por prefijo (construye desde un ``CO1.NTC.*``, no desde un
    ``CO1.REQ.*``); esa rama es anterior a este bloque, queda declarada como
    deuda de limpieza y en p6dx-8zbt es inalcanzable, porque el 100% de los ids
    son ``CO1.REQ.*``. El test siguiente es el que cierra la afirmación de
    fondo: la autoridad de presentación ni siquiera recibe el id.
    """
    entrada = {"urlproceso": {"url": url}}
    req = _mensaje(_proceso(id_del_proceso="CO1.REQ.999", **entrada), canal)
    ntc = _mensaje(_proceso(id_del_proceso="CO1.NTC.999", **entrada), canal)
    assert req == ntc


def test_the_access_authority_cannot_see_the_identifier():
    """La forma más fuerte de "el prefijo no decide": no está en la firma.

    ``resolve_secop_access`` recibe la url observada y la referencia. No hay
    ningún parámetro por el que un id pudiera entrar, así que no existe la
    posibilidad de que una regla futura vuelva a ramificar por ``CO1.REQ``.
    """
    import inspect

    parametros = set(inspect.signature(resolve_secop_access).parameters)
    assert parametros == {"observed_url", "reference"}


# ── Una sola autoridad: los formatters no recrean la política ───────────────


def test_the_formatter_delegates_instead_of_reimplementing_the_policy():
    fuente = Formatter._access_lines.__doc__ or ""
    assert "secop_access" in fuente
    import inspect

    from app.core import formatter as modulo

    codigo = inspect.getsource(modulo)
    # Ni host, ni /login, ni la URL del buscador viven aquí.
    assert "community.secop.gov.co/Public/Tendering/ContractNoticeManagement" not in codigo
    assert "Page=login" not in codigo
    assert "resolve_secop_access" in codigo


def test_every_text_channel_goes_through_the_same_helper():
    import inspect

    for metodo in (Formatter.format_telegram, Formatter.format_whatsapp,
                   Formatter._grouped_card_lines):
        assert "_access_lines" in inspect.getsource(metodo), metodo.__name__


# ── Paridad estricta: la misma decisión en los tres ─────────────────────────


@pytest.mark.parametrize(
    "url",
    ["", _NTC, _AUTENTICACION, SECOP_PUBLIC_SEARCH_URL, "javascript:alert(1)"],
)
def test_the_three_channels_take_the_same_decision(url):
    fila = _proceso(urlproceso={"url": url} if url else "")
    acceso = _acceso_web(fila)
    for canal in CANALES:
        mensaje = _mensaje(fila, canal)
        if acceso.has_direct_link:
            assert acceso.direct_url in mensaje
            assert _BUSCAR not in mensaje
        else:
            assert _BUSCAR in mensaje
            assert acceso.search_url in mensaje

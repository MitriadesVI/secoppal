"""Canarios congelados de BUYER_UNIT_INTEGRITY (Etapa 1, ADR-074).

Cuatro defectos reproducidos antes de tocar runtime. Los cuatro son la misma
familia: una BuyerUnit lógica que el gazetteer no representa como tal — o
porque sus alias humanos cuelgan de otra unidad, o porque quedó repartida en
varios canonicals, uno por dataset.

Estado: C1 y C2 (Puente Aranda) están **cerrados** y sus aserciones quedan como
guarda viva; C3 (MinJusticia) y C4 (Yopal/contratos) siguen congelados.

Qué NO se congela aquí, para no duplicar oráculos existentes:

* que ``ministerio de justicia`` resuelva a un canonical con ese nombre ya está
  congelado en ``tests/test_gazetteer_lote_01.py`` (xfail strict). Ese oráculo
  mira el *nombre*; el de aquí mira la *alcanzabilidad de filas*, que es el daño
  medido — 0 de 14.517 — y que un rename podría no arreglar.

Los nombres físicos publicados son constantes tomadas de la auditoría P0.1
(``docs/gazetteer/INSTITUTIONAL_BUYER_MODEL_P01.md`` §4). Se revalidan en vivo
antes de aplicar cualquier merge; estos tests corren offline a propósito.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import pytest

from app.core.buyer_source_names import project_buyer_names
from app.core.entity_resolver import EntityResolver

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
ALIASES_DB = _PROJECT_ROOT / "app" / "data" / "aliases_db.json"

PROCESOS = "p6dx-8zbt"
CONTRATOS = "jbjy-vk9h"

DISTRITO_NIT = "899999061"
CENAC_PUENTE_ARANDA_NIT = "830039548"

# Grafías que SECOP publica, por dataset (P0.1 §4).
PUBLICADO_MINJUSTICIA = {
    PROCESOS: "MINISTERIO DE JUSTICIA Y DEL DERECHO..",
    CONTRATOS: "MINISTERIO DE JUSTICIA Y DEL DERECHO.",
}
PUBLICADO_YOPAL = {
    PROCESOS: "MUNICIPIO DE YOPAL...",
    CONTRATOS: "MUNICIPIO DE YOPAL..",
}

MERGED_BUYER_UNITS = {
    "MUNICIPIO DE YOPAL": {
        "nit": "891855017",
        "aliases": ("alcaldia de yopal", "municipio de yopal"),
        "sources": PUBLICADO_YOPAL,
    },
    "MUNICIPIO DE SOACHA": {
        "nit": "800094755",
        "aliases": ("alcaldia de soacha", "municipio de soacha"),
        "sources": {
            PROCESOS: "MUNICIPIO DE SOACHA..",
            CONTRATOS: "MUNICIPIO DE SOACHA.",
        },
    },
    "FONDO FINANCIERO DISTRITAL DE SALUD": {
        "nit": "8002469532",
        "aliases": ("ffds", "fondo financiero distrital de salud"),
        "sources": {
            PROCESOS: "FONDO FINANCIERO DISTRITAL DE SALUD..",
            CONTRATOS: "FONDO FINANCIERO DISTRITAL DE SALUD.",
        },
    },
    "MINISTERIO DE JUSTICIA Y DEL DERECHO": {
        "nit": "900457461",
        "aliases": (
            "minjusticia",
            "ministerio de justicia",
            "ministerio de justicia y del derecho",
        ),
        "sources": PUBLICADO_MINJUSTICIA,
    },
    "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS": {
        "nit": "900126860",
        "aliases": ("uaesp", "unidad administrativa especial de servicios públicos"),
        "sources": {
            PROCESOS: "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS..",
            CONTRATOS: "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS.",
        },
    },
}

TARGET_DUPLICATE_CANONICALS = {
    "MUNICIPIO DE YOPAL...",
    "MUNICIPIO DE YOPAL..",
    "MUNICIPIO DE SOACHA..",
    "MUNICIPIO DE SOACHA.",
    "FONDO FINANCIERO DISTRITAL DE SALUD..",
    "FONDO FINANCIERO DISTRITAL DE SALUD.",
    "MINJUSTICIA..",
    "MINJUSTICIA.",
    "MINISTERIO DE JUSTICIA Y DEL DERECHO..",
    "MINISTERIO DE JUSTICIA Y DEL DERECHO.",
    "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS..",
    "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS.",
}

LOCALIDADES_BOGOTA = (
    "usaquen", "chapinero", "santa fe", "san cristobal", "usme", "tunjuelito",
    "bosa", "kennedy", "fontibon", "engativa", "suba", "barrios unidos",
    "teusaquillo", "los martires", "martires", "antonio narino", "puente aranda",
    "la candelaria", "candelaria", "rafael uribe uribe", "ciudad bolivar",
    "sumapaz",
)
_LOCALITY_ALIAS = re.compile(
    r"^(?:alcaldia\s+local\s+(?:de\s+)?|alcaldia\s+(?:de\s+)?|localidad\s+(?:de\s+)?)"
    r"(?P<loc>.+?)$"
)


def _fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text)
    return "".join(c for c in decomposed if unicodedata.category(c) != "Mn").lower()


@pytest.fixture(scope="module")
def records() -> list[dict]:
    return json.loads(ALIASES_DB.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


def _record(records: list[dict], official_name: str) -> dict | None:
    return next((r for r in records if r["official_name"] == official_name), None)


def _reaches(canonical: str, dataset_id: str, published: str) -> bool:
    """¿El filtro que el runtime construiría alcanza la grafía publicada?

    Replica las dos ramas de ``SoQLBuilder._entity_clause``: con binding de
    proyección la comparación es igualdad exacta; sin binding es un ``LIKE``
    con el canonical entre comodines. Ignora el plegado de acentos porque
    ninguna de las grafías en juego lleva tildes.
    """
    projected = project_buyer_names(canonical, dataset_id)
    if projected:
        return published in projected
    return canonical.upper() in published.upper()


# ── C1 — el hijack de Puente Aranda ────────────────────────────────────────


def test_locality_aliases_never_owned_by_a_foreign_nit(records: list[dict]) -> None:
    """Ningún NIT ajeno al Distrito puede poseer alias de una localidad de Bogotá.

    Ley en negativo a propósito: acredita sobre el NIT *conocido*, así que un
    canonical sin NIT declarado no la incumple y la alcaldía local real puede
    recibir los alias sin que el gazetteer tenga que inventarle identidad
    fiscal.
    """
    offenders: list[tuple[str, str, str]] = []
    for record in records:
        nit = record.get("nit")
        if not nit or str(nit) == DISTRITO_NIT:
            continue
        for alias in record.get("aliases", []):
            match = _LOCALITY_ALIAS.match(_fold(alias).strip())
            if match and match.group("loc").strip() in LOCALIDADES_BOGOTA:
                offenders.append((record["official_name"], str(nit), alias))
    assert offenders == [], (
        "alias de localidad de Bogotá en manos de un NIT ajeno al Distrito: "
        f"{offenders}"
    )


@pytest.mark.parametrize(
    "phrase",
    [
        "alcaldia local de puente aranda",
        "alcaldia local puente aranda",
        "localidad de puente aranda",
    ],
)
def test_puente_aranda_locality_resolves_to_the_local_mayoralty(
    resolver: EntityResolver, phrase: str,
) -> None:
    """La frase de la localidad debe llegar a la alcaldía local / FDL.

    Se asserta la *familia institucional* del canonical, no su string exacto:
    el fix puede adjudicar los alias a ``ALCALDIA LOCAL DE PUENTE ARANDA-FONDO
    DE DESARROLLO LOCAL`` o a cualquier otra grafía acreditada de esa unidad.
    """
    resolved = resolver.resolve_entidad(phrase).value
    assert resolved is not None, f"'{phrase}' no resolvió a ningún comprador"
    folded = _fold(resolved)
    assert "puente aranda" in folded, f"'{phrase}' resolvió fuera de la localidad: {resolved!r}"
    assert "alcaldia local" in folded or "fondo de desarrollo local" in folded, (
        f"'{phrase}' resolvió a '{resolved}', que no es la alcaldía local ni su FDL"
    )


# ── C2 — la grafía militar conserva su dueño militar ───────────────────────


@pytest.mark.parametrize("phrase", ["CENAC PUENTE ARANDA+", "cenac puente aranda"])
def test_cenac_puente_aranda_keeps_its_military_owner(
    resolver: EntityResolver, records: list[dict], phrase: str,
) -> None:
    """La central contable del Ejército es una BuyerUnit propia, no un duplicado.

    C1 y C2 se acreditan por separado y **no** se fusionan: comparten el token
    ``PUENTE ARANDA`` y nada más. Son NIT distintos y naturalezas distintas.
    """
    resolved = resolver.resolve_entidad(phrase).value
    assert resolved is not None, f"'{phrase}' no resolvió a ningún comprador"
    record = _record(records, resolved)
    assert record is not None, f"'{resolved}' no existe en el gazetteer"
    assert str(record.get("nit")) == CENAC_PUENTE_ARANDA_NIT, (
        f"'{phrase}' resolvió a '{resolved}' con NIT {record.get('nit')}; "
        f"la unidad acreditada es la del NIT {CENAC_PUENTE_ARANDA_NIT}"
    )


# ── C3 y C4 — unidades repartidas en varios canonicals ─────────────────────


@pytest.mark.parametrize(
    "phrase",
    ["minjusticia", "ministerio de justicia", "ministerio de justicia y del derecho"],
)
@pytest.mark.parametrize("dataset_id", [PROCESOS, CONTRATOS])
def test_minjusticia_reaches_its_published_rows(
    resolver: EntityResolver, phrase: str, dataset_id: str,
) -> None:
    """El filtro construido debe alcanzar la grafía publicada en cada dataset.

    Complementa al xfail de ``test_gazetteer_lote_01``: aquel exige que el
    canonical *se llame* Ministerio de Justicia; este exige que el filtro
    *devuelva filas*. Un rename del canonical satisface el primero y puede
    seguir incumpliendo el segundo.
    """
    resolved = resolver.resolve_entidad(phrase).value
    assert resolved is not None, f"'{phrase}' no resolvió a ningún comprador"
    published = PUBLICADO_MINJUSTICIA[dataset_id]
    assert _reaches(resolved, dataset_id, published), (
        f"'{phrase}' resolvió a '{resolved}', que no alcanza la grafía publicada "
        f"{published!r} en {dataset_id}"
    )


@pytest.mark.parametrize("phrase", ["alcaldia de yopal", "municipio de yopal"])
@pytest.mark.parametrize("dataset_id", [PROCESOS, CONTRATOS])
def test_yopal_reaches_both_published_spellings(
    resolver: EntityResolver, phrase: str, dataset_id: str,
) -> None:
    """Una BuyerUnit con dos grafías publicadas debe alcanzar las dos.

    Hoy el canonical es la grafía de procesos —tres puntos— y el ``LIKE`` con
    tres puntos no alcanza la de contratos, que tiene dos. La dirección importa:
    la grafía corta sí alcanza a la larga por substring, y por eso Soacha, FFDS
    y UAESP funcionan por accidente mientras Yopal no.

    El fix no puede arreglar contratos rompiendo procesos: por eso el lado que
    hoy pasa se asserta sin marca.
    """
    resolved = resolver.resolve_entidad(phrase).value
    assert resolved is not None, f"'{phrase}' no resolvió a ningún comprador"
    published = PUBLICADO_YOPAL[dataset_id]
    assert _reaches(resolved, dataset_id, published), (
        f"'{phrase}' resolvió a '{resolved}', que no alcanza la grafía publicada "
        f"{published!r} en {dataset_id}"
    )


# ── C5 — los cinco merges y sus bindings son un cambio atómico ──────────────


def test_verified_pairs_become_one_logical_buyer_unit_each(
    records: list[dict], resolver: EntityResolver,
) -> None:
    by_name = {record["official_name"]: record for record in records}
    assert not (TARGET_DUPLICATE_CANONICALS & set(by_name))

    for canonical, expected in MERGED_BUYER_UNITS.items():
        record = by_name[canonical]
        assert str(record.get("nit")) == expected["nit"]
        assert set(record.get("source_datasets") or []) == {"procesos", "contratos"}
        for alias in expected["aliases"]:
            assert resolver.resolve_entidad(alias).value == canonical


@pytest.mark.parametrize("canonical", sorted(MERGED_BUYER_UNITS))
@pytest.mark.parametrize("dataset_id", [PROCESOS, CONTRATOS])
def test_verified_buyer_unit_projects_exact_source_name(
    canonical: str, dataset_id: str,
) -> None:
    expected = MERGED_BUYER_UNITS[canonical]["sources"][dataset_id]
    assert project_buyer_names(canonical, dataset_id) == (expected,)


@pytest.mark.parametrize("canonical", sorted(MERGED_BUYER_UNITS))
@pytest.mark.parametrize("dataset_id", [PROCESOS, CONTRATOS])
def test_verified_buyer_unit_soql_uses_exact_source_name(
    canonical: str, dataset_id: str,
) -> None:
    from app.core.soql_builder import SoQLBuilder

    expected = MERGED_BUYER_UNITS[canonical]["sources"][dataset_id]
    query = SoQLBuilder().build_count(dataset_id, {"entidad_resolved": canonical})
    field = "entidad" if dataset_id == PROCESOS else "nombre_entidad"
    assert query == f"SELECT count(*) WHERE {field} = '{expected}'"

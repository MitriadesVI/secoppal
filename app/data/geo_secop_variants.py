"""Colloquial municipal names and the spellings SECOP really stores in ``ciudad``.

DIVIPOLA (``municipal_territories_v1.json``) indexes only the official DANE
name (``SANTIAGO DE CALI``), but users and SECOP both say ``Cali``. This module
is the curated bridge. It is deliberately NOT merged into the DIVIPOLA registry:
``MunicipalTerritoryRegistry.consume`` projects ``ciudad = <official>.title()``
with an equality predicate, so an alias that resolved to ``Santiago De Cali``
would match zero SECOP rows (SECOP stores ``Cali``). Legacy consumers keep their
behavior; only ``geo_authority`` reads these tables.

Evidence (SECOP II, datos.gov.co, captured 2026-09-30): every distinct
``(ciudad, departamento)`` of ``p6dx-8zbt`` (procesos) and ``jbjy-vk9h``
(contratos) with a defined city was compared with DIVIPOLA. 29 pairs did not
match the official name; each is covered below and frozen in
``docs/audits/m1_d_geo/secop_ciudad_evidence.json`` (test-checked).
"""
from __future__ import annotations

# alias (token key, see municipal_territory.key) -> DIVIPOLA codes. An alias MAY
# add a candidate to a key the registry already has (``san pedro``, ``manaure``):
# that is a real homonym, and the authority then asks instead of guessing.
USER_ALIASES: dict[str, tuple[str, ...]] = {
    # --- coloquiales que el DIVIPOLA oficial no indexa ---
    "cali": ("76001",),
    "cartagena": ("13001",),
    "cucuta": ("54001",),
    "bogota": ("11001",),
    "distrito capital": ("11001",),
    "tumaco": ("52835",),
    "buga": ("76111",),
    "mompos": ("13468",),
    "mompox": ("13468",),
    # --- grafías que SECOP escribe en `ciudad` (evidencia 2026-09-30) ---
    "santafe de antioquia": ("05042",),
    "leguizamo": ("86573",),
    "don matias": ("05237",),
    "mariquita": ("73443",),
    "san andres sotavento": ("23670",),
    "san vicente": ("05674",),
    "tolu viejo": ("70823",),
    "toluviejo": ("70823",),
    "san pedro": ("05664",),
    "cuaspud": ("52224",),
    "since": ("70742",),
    "piendamo": ("19548",),
    "purisima": ("23586",),
    "cerro san antonio": ("47161",),
    "san andres": ("05647", "88001"),
    "manaure": ("20443",),
    "chibolo": ("47170",),
    "sotara": ("19760",),
    "san juan de rio seco": ("25662",),
    "guican": ("15332",),
    "lopez": ("19418",),
    "belen de bajira": ("27493",),
}

# DANE classifies 88001 SAN ANDRES as MPIO_TIPO='ISLA' (not 'MUNICIPIO'), so the
# 1103-row registry omits it although SECOP stores 76 843 rows ``ciudad='San
# Andres'`` under the archipelago. It is the only supplement.
SUPPLEMENT_TERRITORIES: dict[str, tuple[str, str]] = {
    # divipola -> (official name, SECOP department value)
    "88001": ("SAN ANDRÉS", "San Andrés, Providencia y Santa Catalina"),
}

# DIVIPOLA code -> the ``ciudad`` spellings SECOP stores when they differ from
# the official name. Anything absent uses ``official.title()`` (verified).
SECOP_CITY_VALUES: dict[str, tuple[str, ...]] = {
    "76001": ("Cali",),
    "13001": ("Cartagena",),
    "54001": ("Cúcuta",),
    "11001": ("Bogotá", "Distrito Capital"),
    "52835": ("Tumaco",),
    "88001": ("San Andrés",),
    "05042": ("Santafé De Antioquia",),
    "86573": ("Leguízamo",),
    "05237": ("Don Matías",),
    "73443": ("Mariquita",),
    "23670": ("San Andrés Sotavento",),
    "05674": ("San Vicente",),
    "70823": ("Tolú Viejo",),
    "05664": ("San Pedro",),
    "52224": ("Cuaspud",),
    "70742": ("Sincé",),
    "19548": ("Piendamó",),
    "13468": ("Mompós",),
    "23586": ("Purísima",),
    "47161": ("Cerro San Antonio",),
    "05647": ("San Andrés",),
    "20443": ("Manaure",),
    "47170": ("Chibolo",),
    "19760": ("Sotara",),
    "25662": ("San Juan De Río Seco",),
    "15332": ("Güicán",),
    "19418": ("López",),
    "27493": ("Belén De Bajirá",),
}

# Names that denote the COUNTRY, never a department or a municipality
# (DIVIPOLA has a real municipality ``COLOMBIA`` in Huila, 41206).
COUNTRY_KEYS: frozenset[str] = frozenset({
    "colombia", "republica de colombia", "todo colombia", "toda colombia",
    "el pais", "todo el pais", "territorio colombiano",
})
HUILA_COLOMBIA_DIVIPOLA = "41206"

# Region aliases whose reading is not unique (the repo maps ``la costa`` to
# Caribe, but Colombia has two coasts). The authority asks.
AMBIGUOUS_REGION_ALIASES: dict[str, tuple[str, ...]] = {
    "la costa": ("Caribe", "Pacifica"),
}

# ``DEPARTAMENTOS`` (legacy table) files the CITY ``cucuta`` under the department
# Norte de Santander. A city is not a department: the authority ignores that key
# as a department mention (it stays a DIVIPOLA municipality, 54001). The legacy
# entry is reported as debt, not edited.
CITY_KEYS_FILED_AS_DEPARTMENT: frozenset[str] = frozenset({"cucuta"})

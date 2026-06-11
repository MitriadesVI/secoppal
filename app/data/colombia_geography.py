from __future__ import annotations

import unicodedata

from app.data.departamentos import DEPARTAMENTOS


REGION_BY_DEPARTMENT = {
    "AMAZONAS": "Amazonia",
    "ANTIOQUIA": "Andina",
    "ARAUCA": "Orinoquia",
    "ATLANTICO": "Caribe",
    "BOGOTA": "Andina",
    "BOLIVAR": "Caribe",
    "BOYACA": "Andina",
    "CALDAS": "Andina",
    "CAQUETA": "Amazonia",
    "CASANARE": "Orinoquia",
    "CAUCA": "Pacifica",
    "CESAR": "Caribe",
    "CHOCO": "Pacifica",
    "CORDOBA": "Caribe",
    "CUNDINAMARCA": "Andina",
    "GUAINIA": "Amazonia",
    "GUAVIARE": "Amazonia",
    "HUILA": "Andina",
    "LA GUAJIRA": "Caribe",
    "MAGDALENA": "Caribe",
    "META": "Orinoquia",
    "NARINO": "Pacifica",
    "NORTE DE SANTANDER": "Andina",
    "PUTUMAYO": "Amazonia",
    "QUINDIO": "Andina",
    "RISARALDA": "Andina",
    "SAN ANDRES": "Insular",
    "SANTANDER": "Andina",
    "SUCRE": "Caribe",
    "TOLIMA": "Andina",
    "VALLE DEL CAUCA": "Pacifica",
    "VAUPES": "Amazonia",
    "VICHADA": "Orinoquia",
}


def _norm(s: str) -> str:
    """Lowercase + strip accents — same normalization key DEPARTAMENTOS uses."""
    s = s.lower().strip()
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def _build_departamentos_by_region() -> dict[str, list[str]]:
    """Región canónica → lista de valores oficiales SECOP (con tilde).

    Deriva de REGION_BY_DEPARTMENT cruzando cada departamento contra
    DEPARTAMENTOS para obtener el valor EXACTO que SECOP espera ("Atlántico",
    no "ATLANTICO"). El cruce está verificado sin faltantes por
    test_colombia_geography; si una clave dejara de cruzar, ese test falla.
    """
    out: dict[str, list[str]] = {}
    for dept_key, region in REGION_BY_DEPARTMENT.items():
        oficial = DEPARTAMENTOS.get(_norm(dept_key))
        if oficial is None:
            continue
        bucket = out.setdefault(region, [])
        if oficial not in bucket:
            bucket.append(oficial)
    return {region: sorted(depts) for region, depts in out.items()}


# Región canónica → [valores oficiales SECOP]. Consumido por el builder
# (departamento IN (...)) cuando el usuario busca por zona en vez de depto.
DEPARTAMENTOS_BY_REGION: dict[str, list[str]] = _build_departamentos_by_region()


# Subregiones — agrupaciones de departamentos COMPLETOS. El filtro SECOP es a
# nivel departamento, así que solo caben subregiones que no parten un depto
# (queda fuera Magdalena Medio, Urabá, Catatumbo, etc., que son sub-departa-
# mentales). Viven aparte de la derivación canónica porque sus departamentos
# ya pertenecen a una de las 6 regiones (el Eje Cafetero está en la Andina).
EXTRA_ZONES: dict[str, list[str]] = {
    "Eje Cafetero": ["Caldas", "Quindío", "Risaralda"],
    "Santanderes": ["Norte de Santander", "Santander"],
    "Tolima Grande": ["Huila", "Tolima"],
}


# Vista única región/subregión → [departamentos oficiales] que consume el router.
ZONES_BY_NAME: dict[str, list[str]] = {**DEPARTAMENTOS_BY_REGION, **EXTRA_ZONES}


# Aliases coloquiales → región canónica. Claves normalizadas (lowercase, sin
# tildes) para el scan. OJO: "amazonas" es un DEPARTAMENTO, no va acá — la
# región se nombra "amazonia"/"region amazonica". Igual "san andres" queda
# como departamento (Insular solo por "insular"/"region insular").
REGION_ALIASES: dict[str, str] = {
    # Caribe
    "caribe": "Caribe",
    "region caribe": "Caribe",
    "costa caribe": "Caribe",
    "costa atlantica": "Caribe",
    "la costa": "Caribe",
    # Andina
    "andina": "Andina",
    "region andina": "Andina",
    "zona andina": "Andina",
    # Pacífica
    "pacifica": "Pacifica",
    "pacifico": "Pacifica",
    "region pacifica": "Pacifica",
    "costa pacifica": "Pacifica",
    "el pacifico": "Pacifica",
    # Orinoquía
    "orinoquia": "Orinoquia",
    "region orinoquia": "Orinoquia",
    "llanos": "Orinoquia",
    "los llanos": "Orinoquia",
    "llanos orientales": "Orinoquia",
    # Amazonía (NUNCA "amazonas" — ese es el departamento)
    "amazonia": "Amazonia",
    "la amazonia": "Amazonia",
    "region amazonica": "Amazonia",
    # Insular
    "insular": "Insular",
    "region insular": "Insular",
    # Subregiones (agrupaciones de departamentos completos → EXTRA_ZONES)
    "eje cafetero": "Eje Cafetero",
    "santanderes": "Santanderes",
    "los santanderes": "Santanderes",
    "tolima grande": "Tolima Grande",
}


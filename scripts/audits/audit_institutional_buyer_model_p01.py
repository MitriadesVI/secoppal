#!/usr/bin/env python3
"""Adjudica el modelo institucional de comprador y tria las 20 superficies P0.1.

Discovery read-only. No modifica ``aliases_db.json``, ``buyer_source_names.json``,
``normalize_text``, ``EntityResolver`` ni ningun archivo de runtime.

Cada adjudicacion declara un PREDICADO verificable contra las tres fuentes
vivas. El script no cree la adjudicacion: la comprueba. Si el predicado no se
reproduce, el caso degrada a ``REVIEW`` y el estado global baja a ``PARTIAL``.
La identidad nunca se decide por similitud.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import string
import subprocess
import sys
import time
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.generic_buyer import is_unqualified_buyer_name  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

ALIASES_DB = ROOT / "app/data/aliases_db.json"
P0_ARTIFACT = ROOT / "artifacts/audits/buyer_punctuation_surface_p0.json"
OUT = ROOT / "artifacts/audits/institutional_buyer_model_p01.json"

DIRECTORY, PROCESSES, CONTRACTS = "4ex9-j3n8", "p6dx-8zbt", "jbjy-vk9h"
RESOURCE = "https://www.datos.gov.co/resource/{}.json"
EXPECTED_HEAD = "7fa44a30fd92af6176ff9a85f73cd4f7696527e7"
OBSERVED_AT = "2026-09-02"

# NIT publicados para la administracion distrital de Bogota. Las cuatro grafias
# son la MISMA identidad fiscal: base, base+DV9, base+DV5 y base con cero guia.
# Que una identidad se publique de cuatro formas es, por si solo, motivo para
# no usar la cadena del NIT como clave de comprador.
BOGOTA_SHARED_NIT = ("899999061", "8999990619", "8999990615", "0899999061")

CANARIES = [
    ("A", "muestrame que ha publicado integracion social de bogota en adulto mayor en 2026",
     "BUYER_UNIT", "(Secretaría Distrital de Integración Social)"),
    ("B", "muestrame contratos recientes de la alcaldia de bogota por mas de 1000 millones",
     "BUYER_GROUP", "Administración Distrital de Bogotá"),
    ("C", "contratos en bogota por mas de 1000 millones",
     "TERRITORY", "Distrito Capital de Bogotá"),
    ("D", "contratos de la alcaldia local de chapinero",
     "BUYER_UNIT", "ALCALDÍA LOCAL DE CHAPINERO - FONDO DE DESARROLLO LOCAL"),
    ("E", "contratos de las alcaldias locales de bogota",
     "BUYER_GROUP", "Alcaldías Locales / Fondos de Desarrollo Local de Bogotá"),
]

# ---------------------------------------------------------------------------
# Adjudicacion de las 20 superficies P0.1.
#
# `predicate` es lo que el script comprueba en vivo:
#   NOT_PUBLISHED_AS_BUYER  la grafia no aparece como comprador en procesos ni
#                           contratos, y su NIT publica otro nombre.
#   PUBLISHED_SAME_NIT      la grafia SI se publica bajo el NIT atribuido.
#   DUPLICATE_PAIR          el NIT publica dos grafias, una por dataset, que
#                           difieren en exactamente un signo terminal.
#   PHANTOM_NIT             el NIT atribuido no tiene ni una fila transaccional.
# ---------------------------------------------------------------------------
ADJUDICATIONS: dict[str, dict[str, str]] = {
    "ALCALDIA  MUNICIPIO DE IBAGUE*|800133897|ALCALDIA DE IBAGUE -": {
        "cause": "REGISTRY_NOISE",
        "remediation": "NO_CHANGE",
        "predicate": "PHANTOM_NIT",
        "rationale": (
            "El NIT atribuido 800133897 no tiene una sola fila en procesos ni en "
            "contratos. Ibague publica bajo 800113389: los digitos estan "
            "transpuestos. La superficie no es un problema de puntuacion sino una "
            "atribucion de identidad sobre un NIT fantasma."
        ),
    },
    "ALCALDIA  MUNICIPIO DE IBAGUE*|800133897|ALCALDIA DE IBAGUe+": {
        "cause": "REGISTRY_NOISE",
        "remediation": "NO_CHANGE",
        "predicate": "PHANTOM_NIT",
        "rationale": "Misma fila fantasma que la anterior, otra grafia del directorio.",
    },
    "ALCALDIA DE PASTO|891280000|ALCALDIA*": {
        "cause": "OVERGENERIC_ALIAS",
        "remediation": "RESOLUTION_BLOCK",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "'ALCALDIA*' no identifica comprador alguno: hay 1.100+ alcaldias. "
            "El NIT 891280000 publica 'ALCALDIA DE PASTO' en ambos datasets. "
            "La guarda de comprador generico bloquea 'alcaldia' pero NO "
            "'alcaldia*': un signo terminal la deja pasar al fuzzy global."
        ),
    },
    "CENTRAL ADMINISTRATIVA Y CONTABLE  PUENTE ARANDA+|830039548|CENAC PUENTE ARANDA+": {
        "cause": "WRONG_ALIAS_OWNER",
        "remediation": "ADD_EXACT_ONLY",
        "predicate": "PUBLISHED_SAME_NIT",
        "rationale": (
            "Comprador real y activo: 2.967 procesos y 1.454 contratos bajo "
            "830039548. El resolver lo manda a CENAC PERSONAL, que es NIT "
            "900385079 — otra identidad legal. El canonical correcto ya existe "
            "con el mismo NIT. Unico caso del lote con dano transaccional por "
            "owner equivocado."
        ),
    },
    "EMPRESA SOCIAL DEL ESTADO BELLOSALUD|800174995|E.S.E BELLO SALUD": {
        "cause": "REGISTRY_NOISE",
        "remediation": "NO_CHANGE",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "Fila solo de directorio. El NIT publica 'EMPRESA SOCIAL DEL ESTADO "
            "BELLOSALUD'. El destino equivocado (Subred Norte) lo produce el "
            "fuzzy global sobre 'E.S.E', no la superficie."
        ),
    },
    "EMPRESA SOCIAL DEL ESTADO HOSPITAL BENJAMIN BARNEY GASCA|891380055|HOSPITAL BENJAMIN BARNEY GASCA E.S.E": {
        "cause": "SOURCE_NAME_VARIANT_SAME_BUYER_UNIT",
        "remediation": "ADD_EXACT_ONLY",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "Mismo NIT, mismo nombre institucional reordenado ('E.S.E' al final). "
            "Ningun otro owner reclama la cadena exacta. Hoy cae en HOSPITAL "
            "MILITAR CENTRAL por fuzzy(85). Alias exacto, sin scan."
        ),
    },
    "EMPRESA SOCIAL DEL ESTADO HOSPITAL LOCAL|846000253|E.S.E. HOSPITAL LOCAL PUERTO ASIS *": {
        "cause": "SOURCE_NAME_VARIANT_SAME_BUYER_UNIT",
        "remediation": "ADD_EXACT_ONLY",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "El nombre publicado del canonical ('EMPRESA SOCIAL DEL ESTADO "
            "HOSPITAL LOCAL') es mas generico que esta grafia, que anade el "
            "municipio. Mismo NIT. Alias exacto, sin scan: 'hospital local' "
            "desnudo no puede quedar habilitado."
        ),
    },
    "EMPRESA SOCIAL DEL ESTADO HOSPITAL SAGRADO CORAZÓN DE JESÚS|846000471|E.S.E  HOSPITAL SAGRADO CORAZoN DE JESuS": {
        "cause": "SOURCE_NAME_VARIANT_SAME_BUYER_UNIT",
        "remediation": "ADD_EXACT_ONLY",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "Variante mojibake de la grafia publicada. normalize_text pliega "
            "acentos, asi que colapsa a la misma clave que la forma acentuada: "
            "un solo alias exacto cubre ambas."
        ),
    },
    "EMPRESA SOCIAL DEL ESTADO HOSPITAL SAGRADO CORAZÓN DE JESÚS|846000471|E.S.E  HOSPITAL SAGRADO CORAZÓN DE JESÚS": {
        "cause": "SOURCE_NAME_VARIANT_SAME_BUYER_UNIT",
        "remediation": "SOURCE_NAME_BINDING",
        "predicate": "PUBLISHED_SAME_NIT",
        "rationale": (
            "Esta ES la grafia publicada en procesos (5.184 filas) para el NIT "
            "846000471, y NO coincide con el canonical. Caso de proyeccion "
            "ADR-073 ademas de alias exacto: hoy el canonical no alcanza sus "
            "propias filas por igualdad."
        ),
    },
    "ESE HOSPITAL PIO XII DE COLON|891201845|E.S.E": {
        "cause": "OVERGENERIC_ALIAS",
        "remediation": "RESOLUTION_BLOCK",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "'E.S.E' es una forma societaria, no un comprador. El NIT publica "
            "'ESE HOSPITAL PIO XII DE COLON'. Debe fallar cerrado."
        ),
    },
    "FONDO FINANCIERO DISTRITAL DE SALUD..|8002469532|FONDO FINANCIERO DISTRITAL DE SALUD.": {
        "cause": "DUPLICATE_CANONICAL_SAME_BUYER_UNIT",
        "remediation": "MERGE_DUPLICATE_BUYER_UNIT",
        "predicate": "DUPLICATE_PAIR",
        "rationale": (
            "Mismo NIT; procesos publica '..' y contratos publica '.'. Los dos "
            "canonicals tienen procedencia de dataset disjunta. Es una unidad "
            "compradora, no dos."
        ),
    },
    "GOBERNACIÓN DE LA GUAJIRA|892115015|SECRETARIA DE SALUD DEPARTAMENTAL+": {
        "cause": "SHARED_NIT_DISTINCT_BUYER_UNIT",
        "remediation": "KEEP_SEPARATE",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "Secretaria sectorial bajo la identidad fiscal del departamento. La "
            "Gobernacion publica bajo 8921150151 y el NIT base publica "
            "'ADMINISTRACION TEMPORAL PARA EL SECTOR AGUA POTABLE...': tres "
            "unidades, una identidad fiscal. El nombre no lleva territorio, asi "
            "que no puede resolverse solo."
        ),
    },
    "MINISTERIO DE JUSTICIA Y DEL DERECHO..|900457461|MINISTERIO DE JUSTICIA Y DEL DERECHO.": {
        "cause": "DUPLICATE_CANONICAL_SAME_BUYER_UNIT",
        "remediation": "MERGE_DUPLICATE_BUYER_UNIT",
        "predicate": "DUPLICATE_PAIR",
        "rationale": (
            "Mismo NIT 900457461, procedencia disjunta. Ademas existen dos "
            "canonicals sinteticos ('MINJUSTICIA.', 'MINJUSTICIA..') que SECOP "
            "no publica y que se quedaron con todos los alias humanos: cuatro "
            "canonicals para una unidad compradora."
        ),
    },
    "MUNICIPIO DE SOACHA..|800094755|ALCALDIA DE SOACHA*/*": {
        "cause": "REGISTRY_NOISE",
        "remediation": "NO_CHANGE",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": "Fila de directorio; el NIT publica 'MUNICIPIO DE SOACHA..' / '.'.",
    },
    "MUNICIPIO DE SOACHA..|800094755|MUNICIPIO DE SOACHA.": {
        "cause": "DUPLICATE_CANONICAL_SAME_BUYER_UNIT",
        "remediation": "MERGE_DUPLICATE_BUYER_UNIT",
        "predicate": "DUPLICATE_PAIR",
        "rationale": (
            "Mismo NIT, procedencia disjunta. Hoy no rompe consultas solo porque "
            "el alias resuelve a la grafia CORTA, que es substring de la larga."
        ),
    },
    "MUNICIPIO DE SOACHA..|8000947557|ALCALDIA DE SOACHA//": {
        "cause": "REGISTRY_NOISE",
        "remediation": "NO_CHANGE",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": "Fila de directorio sobre el NIT con DV; sin filas transaccionales.",
    },
    "MUNICIPIO DE SOACHA..|8000947557|SECRETARiA DE AMBIENTE, MINAS Y DESARROLLO RURAL": {
        "cause": "SHARED_NIT_DISTINCT_BUYER_UNIT",
        "remediation": "KEEP_SEPARATE",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "Secretaria sectorial de Soacha bajo la identidad fiscal del "
            "municipio. Unidad distinta del Municipio; el nombre no lleva "
            "territorio y no puede resolverse solo."
        ),
    },
    "MUNICIPIO DE VILLA DEL ROSARIO**|8905033730|AGENCIA NACIONAL DE CONTRATACION PUBLICA- COLOMBIA COMPRA EFICIENTE": {
        "cause": "REGISTRY_NOISE",
        "remediation": "NO_CHANGE",
        "predicate": "NOT_PUBLISHED_AS_BUYER",
        "rationale": (
            "Fila de directorio de CCE colgada del NIT de Villa del Rosario. CCE "
            "publica bajo 900514813. De paso acredita que 890503373 sostiene tres "
            "unidades compradoras: Municipio, Concejo y Personeria."
        ),
    },
    "MUNICIPIO DE YOPAL...|891855017|MUNICIPIO DE YOPAL..": {
        "cause": "DUPLICATE_CANONICAL_SAME_BUYER_UNIT",
        "remediation": "MERGE_DUPLICATE_BUYER_UNIT",
        "predicate": "DUPLICATE_PAIR",
        "rationale": (
            "Mismo NIT, procedencia disjunta. Aqui SI rompe: el alias resuelve a "
            "la grafia LARGA de procesos, que no es substring de la de contratos. "
            "'alcaldia de yopal' sobre contratos alcanza 0 de 19.039 filas."
        ),
    },
    "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS..|900126860|UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS.": {
        "cause": "DUPLICATE_CANONICAL_SAME_BUYER_UNIT",
        "remediation": "MERGE_DUPLICATE_BUYER_UNIT",
        "predicate": "DUPLICATE_PAIR",
        "rationale": "Mismo NIT, procedencia disjunta. UAESP del Distrito Capital.",
    },
}


def digits(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def is_punct(char: str) -> bool:
    return char in string.punctuation or unicodedata.category(char).startswith("P")


def differs_by_one_trailing_punct(long: str, short: str) -> bool:
    return len(long) == len(short) + 1 and long[:-1] == short and is_punct(long[-1])


class Socrata:
    def __init__(self, token: str | None) -> None:
        self.headers = {
            "Accept": "application/json",
            "User-Agent": "SECOPPAL-INSTITUTIONAL-BUYER-MODEL-P01/1.0",
        }
        if token:
            self.headers["X-App-Token"] = token

    def get(self, dataset: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        last: Exception | None = None
        for attempt in range(5):
            try:
                response = requests.get(
                    RESOURCE.format(dataset), params=params,
                    headers=self.headers, timeout=180,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("Socrata response is not a list")
                return payload
            except (requests.RequestException, ValueError) as exc:
                last = exc
                time.sleep(attempt + 1)
        assert last is not None
        raise last

    def buyers_by_nit(self, nits: list[str]) -> dict[str, dict[str, dict[str, int]]]:
        """{nit: {'processes'|'contracts': {physical_name: rows}}} para NITs dados."""
        out: dict[str, dict[str, dict[str, int]]] = defaultdict(
            lambda: {"processes": {}, "contracts": {}}
        )
        if not nits:
            return out
        clause = ",".join(f"'{n}'" for n in sorted(set(nits)))
        for dataset, field, tag in (
            (PROCESSES, "entidad", "processes"),
            (CONTRACTS, "nombre_entidad", "contracts"),
        ):
            rows = self.get(dataset, {
                "$select": f"{field},nit_entidad,count(1) as n",
                "$where": f"nit_entidad in ({clause})",
                "$group": f"{field},nit_entidad",
                "$limit": 2000,
            })
            for row in rows:
                out[str(row.get("nit_entidad"))][tag][row[field]] = int(row["n"])
        return out

    def buyers_by_exact_names(
        self, names: list[str],
    ) -> dict[str, dict[str, Any]]:
        """{physical_name: {'nits': [...], 'processes': n, 'contracts': n}}."""
        out: dict[str, dict[str, Any]] = {}
        for dataset, field, tag in (
            (PROCESSES, "entidad", "processes"),
            (CONTRACTS, "nombre_entidad", "contracts"),
        ):
            for start in range(0, len(names), 25):
                chunk = names[start:start + 25]
                clause = " OR ".join(
                    f"{field}='{name.replace(chr(39), chr(39) * 2)}'" for name in chunk
                )
                rows = self.get(dataset, {
                    "$select": f"{field},nit_entidad,count(1) as n",
                    "$where": clause,
                    "$group": f"{field},nit_entidad",
                    "$limit": 1000,
                })
                for row in rows:
                    entry = out.setdefault(
                        row[field], {"nits": [], "processes": 0, "contracts": 0}
                    )
                    nit = str(row.get("nit_entidad"))
                    if nit not in entry["nits"]:
                        entry["nits"].append(nit)
                    entry[tag] += int(row["n"])
        return out


# ---------------------------------------------------------------------------
# Clasificacion institucional de Bogota.
#
# La pertenencia se ACREDITA, no se deduce. Ninguna unidad entra a una clase
# distrital por estar en el departamento de Bogota ni por compartir un NIT.
# Las descentralizadas, empresas y ESE distritales se enumeran con el NIT que
# se verifico en vivo; lo que no esta acreditado NO es miembro, y se reporta
# como `NOT_BOGOTA_ADMINISTRATION` (residuo geografico), no como distrital.
# ---------------------------------------------------------------------------
ACCREDITED_DISTRICT: dict[str, tuple[str, str]] = {
    # nombre publicado -> (clase, NIT verificado en vivo)
    "FONDO FINANCIERO DISTRITAL DE SALUD..": ("DECENTRALIZED_DISTRICT_ENTITY", "8002469532"),
    "FONDO FINANCIERO DISTRITAL DE SALUD.": ("DECENTRALIZED_DISTRICT_ENTITY", "8002469532"),
    "UNIVERSIDAD DISTRITAL FRANCISCO JOSE DE CALDAS": ("DECENTRALIZED_DISTRICT_ENTITY", "899999230"),
    "IDRD - ENTIDAD OFICIAL.": ("DECENTRALIZED_DISTRICT_ENTITY", "860061099"),
    "IDARTES": ("DECENTRALIZED_DISTRICT_ENTITY", "900413030"),
    "INSTITUTO DISTRITAL PARA LA PROTECCIÓN DE LA NIÑEZ Y LA JUVENTUD - IDIPRON*":
        ("DECENTRALIZED_DISTRICT_ENTITY", "899999333"),
    "INSTITUTO DE DESARROLLO URBANO": ("DECENTRALIZED_DISTRICT_ENTITY", "899999081"),
    "Jardín Botánico José Celestino Mutis": ("DECENTRALIZED_DISTRICT_ENTITY", "860030197"),
    "INSTITUTO PARA LA ECONOMIA SOCIAL- IPES": ("DECENTRALIZED_DISTRICT_ENTITY", "899999446"),
    "CAJA DE LA VIVIENDA POPULAR//": ("DECENTRALIZED_DISTRICT_ENTITY", "899999074"),
    "INSTITUTO DISTRITAL DE LA PARTICIPACIÓN Y ACCIÓN COMUNAL":
        ("DECENTRALIZED_DISTRICT_ENTITY", "900127054"),
    "UNIDAD ADMINISTRATIVA ESPECIAL DE REHABILITACIÓN Y MANTENIMIENTO VIAL":
        ("DECENTRALIZED_DISTRICT_ENTITY", "900127032"),
    "UAECD": ("DECENTRALIZED_DISTRICT_ENTITY", "900127768"),
    "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS..":
        ("DECENTRALIZED_DISTRICT_ENTITY", "900126860"),
    "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS.":
        ("DECENTRALIZED_DISTRICT_ENTITY", "900126860"),
    "AGENCIA DISTRITAL PARA LA EDUCACIÓN SUPERIOR, LA CIENCIA Y LA TECNOLOGÍA, ATENEA":
        ("DECENTRALIZED_DISTRICT_ENTITY", "901508361"),
    "INSTITUTO  DISTRITAL DE CIENCIA, BIOTECNOLOGIA E INNOVACIÓN EN SALUD - IDCBIS":
        ("DECENTRALIZED_DISTRICT_ENTITY", "9010347905"),
    "INSTITUTO DISTRITAL DE PATRIMONIO CULTURAL": ("DECENTRALIZED_DISTRICT_ENTITY", "860506170"),
    "INSTITUTO DISTRITAL DE GESTIÓN DE RIESGOS Y CAMBIO CLIMÁTICO - IDIGER":
        ("DECENTRALIZED_DISTRICT_ENTITY", "800154275"),
    "INSTITUTO DISTRITAL DE PROTECCION Y BIENESTAR ANIMAL":
        ("DECENTRALIZED_DISTRICT_ENTITY", "901097324"),
    "CANAL CAPITAL": ("DISTRICT_COMPANY", "830012587"),
    "EMPRESA DE RENOVACION Y DESARROLLO URBANO DE BOGOTA D.C": ("DISTRICT_COMPANY", "830144890"),
    "METRO DE BOGOTA SA": ("DISTRICT_COMPANY", "901038962"),
    "LOTERIA DE BOGOTA": ("DISTRICT_COMPANY", "899999270"),
    "TERMINAL DE TRANSPORTE S.A": ("DISTRICT_COMPANY", "860052155"),
    "AGUAS DE BOGOTA S.A. E.S.P.": ("DISTRICT_COMPANY", "830128286"),
    "CAPITAL SALUD EPS-S S.A.S.": ("DISTRICT_COMPANY", "900298372"),
    "TRANSMILENIO S.A.": ("DISTRICT_COMPANY", "830063506"),
    "EMPRESA DE ACUEDUCTO Y ALCANTARILLADO DE BOGOTÁ - E.S.P.": ("DISTRICT_COMPANY", "899999094"),
    "SUBRED INTEGRADA DE SERVICIOS DE SALUD NORTE E.S.E. (OFICIAL)":
        ("DISTRICT_HEALTH_ENTITY", "900971006"),
    "SUBRED INTEGRADA DE SERVICIOS DE SALUD SUR E.S.E.**": ("DISTRICT_HEALTH_ENTITY", "900958564"),
    "SUBRED INTEGRADA DE SERVICIOS DE SALUD SUR OCCIDENTE ESE.":
        ("DISTRICT_HEALTH_ENTITY", "900959048"),
    "SUBRED INTEGRADA DE SERVICIO DE SALUD CENTRO ORIENTE E.S.E 1":
        ("DISTRICT_HEALTH_ENTITY", "900959051"),
    "CONTRALORIA DE BOGOTA": ("DISTRICT_CONTROL_OR_CORPORATE_BODY", "800245133"),
}


def _classify_shared_nit_unit(name: str) -> str:
    """Unidad que transacciona bajo la identidad fiscal distrital compartida."""
    upper = name.upper()
    if any(k in upper for k in ("ALCALDIA LOCAL", "ALCALDÍA LOCAL",
                                "FONDO DE DESARROLLO LOCAL")):
        return "LOCAL_MAYORALTY_FDL_CANDIDATE"
    if any(k in upper for k in ("PERSONER", "VEEDUR", "CONCEJO")):
        # Ministerio Publico distrital y corporacion politico-administrativa:
        # comparten el NIT del Distrito y NO son la administracion.
        return "DISTRICT_CONTROL_OR_CORPORATE_BODY"
    if "SUBRED" in upper or "POLICIA NACIONAL" in upper:
        return "REVIEW"
    return "ADMINISTRACION_CENTRAL_CANDIDATE"


def head_matches() -> tuple[str, bool]:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout.strip()
    return head, head == EXPECTED_HEAD


def run_canaries(resolver: EntityResolver) -> list[dict[str, Any]]:
    from scripts.audits.audit_isolation import isolated_runtime_stores

    results: list[dict[str, Any]] = []
    with isolated_runtime_stores(prefix="p01_canary_"):
        from app.core.query_router import QueryRouter
        router = QueryRouter()
        for key, utterance, expected_kind, expected_target in CANARIES:
            parsed = router.parse(utterance)
            params = parsed if isinstance(parsed, dict) else (
                getattr(parsed, "params", None) or vars(parsed)
            )
            resolved = params.get("entidad_resolved")
            observed_kind = (
                "BUYER_UNIT" if resolved
                else "TERRITORY" if params.get("departamento_resolved") and not params.get("entidad")
                else "FAIL_CLOSED" if params.get("entidad") and not resolved
                else "UNRESOLVED"
            )
            verdict = "PASS" if (
                observed_kind == expected_kind
                and (expected_kind != "BUYER_UNIT" or resolved == expected_target)
            ) else ("FAIL_CLOSED" if observed_kind == "FAIL_CLOSED" else "FAIL")
            results.append({
                "canary": key,
                "utterance": utterance,
                "expected_kind": expected_kind,
                "expected_target": expected_target,
                "observed_kind": observed_kind,
                "entidad_captured": params.get("entidad"),
                "entidad_resolved": resolved,
                "departamento_resolved": params.get("departamento_resolved"),
                "objeto": params.get("objeto"),
                "verdict": verdict,
            })
    return results


def guard_bypass_probe() -> list[dict[str, Any]]:
    """La guarda de comprador generico frente a un signo terminal."""
    out = []
    for probe in ("alcaldia", "ALCALDIA*", "ALCALDIA.", "gobernacion", "GOBERNACION+"):
        key = normalize_text(probe)
        out.append({
            "input": probe,
            "normalized": key,
            "blocked_as_unqualified": is_unqualified_buyer_name(key),
        })
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", default=None)
    parser.add_argument("--out", default=str(OUT))
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    client = Socrata(args.token or os.getenv("SECOP_APP_TOKEN"))
    resolver = EntityResolver(ALIASES_DB)

    head, head_ok = head_matches()
    p0 = json.loads(P0_ARTIFACT.read_text(encoding="utf-8"))
    cases = [
        c for c in p0["punctuated_surface_cases"]
        if c["recommendation"] in ("DO_NOT_ADD", "NEEDS_REVIEW")
    ]

    # ---- 1. Revalidacion de las 20 -------------------------------------
    nits = sorted({c["nit"] for c in cases})
    nit_variants = sorted({v for n in nits for v in ({n, n[:-1]} if len(n) >= 9 else {n})})
    published = client.buyers_by_nit(nit_variants)
    surfaces = sorted({c["physical_name"] for c in cases})
    surface_rows = client.buyers_by_exact_names(surfaces)

    triage: list[dict[str, Any]] = []
    for case in cases:
        nit = case["nit"]
        keys = [k for k in published if digits(k) in {digits(nit), digits(nit)[:-1], digits(nit) + ""}
                or digits(nit).startswith(digits(k)) or digits(k).startswith(digits(nit))]
        proc: dict[str, int] = {}
        cont: dict[str, int] = {}
        for k in keys:
            proc.update(published[k]["processes"])
            cont.update(published[k]["contracts"])
        physical = case["physical_name"]
        result = resolver.resolve_entidad(physical)
        adj = ADJUDICATIONS.get(case["surface_id"])
        predicate = adj["predicate"] if adj else "UNADJUDICATED"

        holds: bool
        if predicate == "PHANTOM_NIT":
            holds = not proc and not cont
        elif predicate == "NOT_PUBLISHED_AS_BUYER":
            holds = physical not in proc and physical not in cont and bool(proc or cont)
        elif predicate == "PUBLISHED_SAME_NIT":
            holds = physical in proc or physical in cont
        elif predicate == "DUPLICATE_PAIR":
            pn = max(proc, key=proc.get) if proc else None
            cn = max(cont, key=cont.get) if cont else None
            holds = bool(pn and cn and differs_by_one_trailing_punct(pn, cn))
        else:
            holds = False

        triage.append({
            "surface_id": case["surface_id"],
            "physical_name": physical,
            "attributed_canonical": case["canonical_name"],
            "nit": nit,
            "p0_recommendation": case["recommendation"],
            "p0_classification": case["current_classification"],
            "published_processes": proc,
            "published_contracts": cont,
            "surface_rows_live": surface_rows.get(physical),
            "resolver_owner": result.value,
            "resolver_method": result.method,
            "cause": adj["cause"] if adj and holds else "REVIEW",
            "remediation": adj["remediation"] if adj and holds else "REVIEW",
            "rationale": adj["rationale"] if adj else "sin adjudicacion declarada",
            "predicate": predicate,
            "predicate_holds": holds,
        })

    # ---- 2. Inventario de Bogota ---------------------------------------
    directory_shared = client.get(DIRECTORY, {
        "$select": "nombre_entidad,nit_entidad",
        "$where": "nit_entidad in ({}) AND es_entidad='Si'".format(
            ",".join(f"'{n}'" for n in BOGOTA_SHARED_NIT)),
        "$limit": 5000,
    })
    shared_nit_units: dict[str, dict[str, int]] = {}
    for dataset, field, tag in (
        (PROCESSES, "entidad", "processes"), (CONTRACTS, "nombre_entidad", "contracts"),
    ):
        rows = client.get(dataset, {
            "$select": f"{field},count(1) as n",
            "$where": "nit_entidad in ({})".format(
                ",".join(f"'{n}'" for n in BOGOTA_SHARED_NIT)),
            "$group": field, "$limit": 500,
        })
        for row in rows:
            shared_nit_units.setdefault(row[field], {"processes": 0, "contracts": 0})
            shared_nit_units[row[field]][tag] = int(row["n"])

    aliases = json.loads(ALIASES_DB.read_text(encoding="utf-8"))
    bogota_officials = sorted({
        e["official_name"] for e in aliases
        if str(e.get("departamento", "")).upper().replace("Á", "A")
        in ("DISTRITO CAPITAL DE BOGOTA", "BOGOTA D.C.", "BOGOTA")
    })
    # El universo de nombres a verificar es la union del gazetteer de Bogota y
    # la lista acreditada: varias empresas distritales (EAAB, Transmilenio,
    # Metro) no llevan `departamento` de Bogota en el gazetteer y quedarian
    # fuera del inventario por un campo administrativo, no por evidencia.
    scan_names = sorted(set(bogota_officials) | set(ACCREDITED_DISTRICT))
    decentralized = client.buyers_by_exact_names(scan_names)

    inventory: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for name, counts in shared_nit_units.items():
        inventory[_classify_shared_nit_unit(name)].append({
            "buyer_unit": name,
            "nits": ["899999061 (identidad fiscal distrital compartida)"],
            "membership_evidence": "SHARED_DISTRICT_FISCAL_IDENTITY + naturaleza de la unidad",
            **counts,
        })
    for name, info in decentralized.items():
        if name in shared_nit_units:
            continue
        accredited = ACCREDITED_DISTRICT.get(name)
        if accredited:
            cls, expected_nit = accredited
            observed = [digits(n) for n in info["nits"]]
            nit_ok = any(
                d == digits(expected_nit) or d == digits(expected_nit)[:-1]
                or digits(expected_nit).startswith(d)
                for d in observed
            )
            inventory[cls if nit_ok else "REVIEW"].append({
                "buyer_unit": name, "nits": info["nits"],
                "expected_nit": expected_nit, "nit_accredited_live": nit_ok,
                "membership_evidence": "ACCREDITED_DISTRICT_ENTITY",
                "processes": info["processes"], "contracts": info["contracts"],
            })
            continue
        inventory["NOT_BOGOTA_ADMINISTRATION"].append({
            "buyer_unit": name, "nits": info["nits"],
            "membership_evidence": (
                "residuo geografico: comprador con domicilio en Bogota sin "
                "acreditacion de pertenencia distrital; NO es miembro"
            ),
            "processes": info["processes"], "contracts": info["contracts"],
        })
    # Filas del directorio bajo el NIT distrital que nunca transaccionan: no se
    # pueden adjudicar como unidad compradora ni descartar como ruido sin una
    # revision individual.
    transacting = set(shared_nit_units)
    for name in sorted({str(r.get("nombre_entidad")) for r in directory_shared}):
        if name not in transacting:
            inventory["REVIEW_DIRECTORY_ONLY_SHARED_NIT"].append({
                "buyer_unit": name,
                "nits": ["899999061 (identidad fiscal distrital compartida)"],
                "membership_evidence": "solo directorio; 0 filas transaccionales",
                "processes": 0, "contracts": 0,
            })
    for bucket in inventory.values():
        bucket.sort(key=lambda x: -(x["processes"] + x["contracts"]))

    def volume(classes: tuple[str, ...]) -> dict[str, int]:
        units = [u for c in classes for u in inventory.get(c, [])]
        return {
            "buyer_units": len(units),
            "processes": sum(u["processes"] for u in units),
            "contracts": sum(u["contracts"] for u in units),
        }

    central = ("ADMINISTRACION_CENTRAL_CANDIDATE",)
    local = ("LOCAL_MAYORALTY_FDL_CANDIDATE",)
    broad = central + local + (
        "DECENTRALIZED_DISTRICT_ENTITY", "DISTRICT_COMPANY", "DISTRICT_HEALTH_ENTITY",
    )
    territory_total = client.get(PROCESSES, {
        "$select": "count(1) as n",
        "$where": "departamento_entidad='Distrito Capital de Bogotá'",
    })

    payload = {
        "audit": "INSTITUTIONAL_BUYER_MODEL_P01",
        "type": "DISCOVERY_AND_ADJUDICATION_READ_ONLY",
        "as_of": OBSERVED_AT,
        "expected_head": EXPECTED_HEAD,
        "observed_head": head,
        "head_matches": head_ok,
        "authorities": {
            "buyer_directory": DIRECTORY,
            "processes": PROCESSES,
            "contracts": CONTRACTS,
            "identity": "NIT declarado/publicado; nunca similitud",
            "alias_policy": "ADR-072",
            "source_projection": "ADR-073",
            "input_universe": "artifacts/audits/buyer_punctuation_surface_p0.json",
        },
        "institutional_model": {
            "TERRITORY": "campos geograficos; jamas implica comprador",
            "LEGAL_IDENTITY": "NIT; puede cubrir N unidades compradoras y ruido de registro",
            "BUYER_UNIT": "nombre de comprador publicado; identidad primaria de runtime",
            "BUYER_GROUP": "agrupacion declarada de BuyerUnits; nunca inferida de NIT ni geografia",
        },
        "canaries": run_canaries(resolver),
        "generic_buyer_guard_probe": guard_bypass_probe(),
        "triage_20": triage,
        "shared_nit_bogota": {
            "nit_spellings_observed": sorted(BOGOTA_SHARED_NIT),
            "directory_rows": len(directory_shared),
            "directory_distinct_names": len({r.get("nombre_entidad") for r in directory_shared}),
            "transacting_buyer_units": len(shared_nit_units),
            "processes": sum(u["processes"] for u in shared_nit_units.values()),
            "contracts": sum(u["contracts"] for u in shared_nit_units.values()),
        },
        "bogota_inventory": dict(inventory),
        "bogota_group_policies": {
            "A_CENTRAL_ONLY": volume(central),
            "B_CENTRAL_PLUS_LOCAL": volume(central + local),
            "C_BROAD_DISTRICT_ECOSYSTEM": volume(broad),
            "TERRITORY_BOGOTA_PROCESSES": int(territory_total[0]["n"]),
            "recommended_default": "B_CENTRAL_PLUS_LOCAL",
        },
        "runtime_changed": False,
        "aliases_changed": False,
        "source_bindings_changed": False,
    }

    resolved_cases = sum(1 for t in triage if t["predicate_holds"])
    payload["status"] = (
        "PASS" if head_ok and resolved_cases == len(triage) and len(triage) == 20
        else "PARTIAL"
    )
    payload["summary"] = {
        "input_cases": len(triage),
        "predicates_reproduced": resolved_cases,
        "by_cause": {
            cause: sum(1 for t in triage if t["cause"] == cause)
            for cause in sorted({t["cause"] for t in triage})
        },
        "by_remediation": {
            rem: sum(1 for t in triage if t["remediation"] == rem)
            for rem in sorted({t["remediation"] for t in triage})
        },
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(f"status={payload['status']} head_matches={head_ok} -> {out_path}")
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Auditor read-only R2C de universos institucionales de compradores.

Consulta únicamente los datasets públicos SECOP II, lee el gazetteer y emite
una fotografía reproducible. No importa ni modifica componentes de runtime.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

DB = ROOT / "app/data/aliases_db.json"
OUT = ROOT / "artifacts/audits/institutional_buyer_universe_r2c.json"
PROC_URL = "https://www.datos.gov.co/resource/p6dx-8zbt.json"
CONT_URL = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
RECENT_SINCE = "2025-09-01T00:00:00.000"


def E(group: str, name: str, search: str | None = None, queries: list[str] | None = None,
      role: str = "PHYSICAL_BUYER", official_scope: str | None = None) -> dict:
    source = {
        "CAR_CDS_CORE": "Ministerio de Ambiente — 33 CAR/CDS",
        "CORMAGDALENA_SEPARATE": "Función Pública — órganos autónomos / Ley 161 de 1994",
        "URBAN_ENVIRONMENTAL_AUTHORITIES": "Ministerio de Ambiente — 7 AAU",
        "PUBLIC_UNIVERSITIES_SECOP": "Ministerio de Educación / SUE",
        "DECENTRALIZED_INVENTORY": "Función Pública — Manual por sectores (muestra, no universo final)",
    }.get(group, "Función Pública — Manual de Estructura del Estado")
    return {"group": group, "logical_name": name, "search": search or name,
            "natural_queries": queries or [name], "buyer_concept": role,
            "official_scope": official_scope, "official_universe_source": source}


CAR = [
    ("Corporación Autónoma Regional del Alto Magdalena", "CAM"),
    ("Corporación Autónoma Regional de Cundinamarca", "CORPORACION AUTONOMA REGIONAL DE CUNDINAMARCA"),
    ("Corporación Autónoma Regional de Risaralda", "CARDER"),
    ("Corporación Autónoma Regional del Canal del Dique", "CARDIQUE"),
    ("Corporación Autónoma Regional de Sucre", "CARSUCRE"),
    ("Corporación Autónoma Regional de Santander", "CORPORACION AUTONOMA REGIONAL DE SANTANDER"),
    ("Corporación para el Desarrollo Sostenible del Norte y Oriente Amazónico", "CDA"),
    ("Corporación Autónoma Regional para la Defensa de la Meseta de Bucaramanga", "CDMB"),
    ("Corporación para el Desarrollo Sostenible del Chocó", "CODECHOCO"),
    ("Corporación para el Desarrollo Sostenible del Archipiélago de San Andrés", "CORALINA"),
    ("Corporación Autónoma Regional del Centro de Antioquia", "CORANTIOQUIA"),
    ("Corporación para el Desarrollo Sostenible del Área de Manejo Especial La Macarena", "CORMACARENA"),
    ("Corporación Autónoma Regional de las Cuencas de los Ríos Negro y Nare", "CORNARE"),
    ("Corporación Autónoma Regional del Magdalena", "CORPAMAG"),
    ("Corporación para el Desarrollo Sostenible del Sur de la Amazonía", "CORPOAMAZONIA"),
    ("Corporación Autónoma Regional de Boyacá", "CORPOBOYACA"),
    ("Corporación Autónoma Regional de Caldas", "CORPOCALDAS"),
    ("Corporación Autónoma Regional del Cesar", "CORPOCESAR"),
    ("Corporación Autónoma Regional de Chivor", "CORPOCHIVOR"),
    ("Corporación Autónoma Regional de La Guajira", "CORPOGUAJIRA"),
    ("Corporación Autónoma Regional del Guavio", "CORPOGUAVIO"),
    ("Corporación para el Desarrollo Sostenible de La Mojana y El San Jorge", "CORPOMOJANA"),
    ("Corporación Autónoma Regional de Nariño", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("Corporación Autónoma Regional de la Frontera Nororiental", "CORPONOR"),
    ("Corporación Autónoma Regional de la Orinoquía", "CORPORINOQUIA"),
    ("Corporación para el Desarrollo Sostenible del Urabá", "CORPOURABA"),
    ("Corporación Autónoma Regional del Tolima", "CORTOLIMA"),
    ("Corporación Autónoma Regional del Atlántico", "CORPORACION AUTONOMA REGIONAL DEL ATLANTICO"),
    ("Corporación Autónoma Regional del Cauca", "CORPORACION AUTONOMA REGIONAL DEL CAUCA"),
    ("Corporación Autónoma Regional del Quindío", "CORPORACION AUTONOMA REGIONAL DEL QUINDIO"),
    ("Corporación Autónoma Regional del Sur de Bolívar", "CSB"),
    ("Corporación Autónoma Regional del Valle del Cauca", "CVC"),
    ("Corporación Autónoma Regional de los Valles del Sinú y del San Jorge", "CVS"),
]

URBAN = [
    ("Secretaría Distrital de Ambiente de Bogotá", "SECRETARIA DISTRITAL DE AMBIENTE"),
    ("Departamento Administrativo de Gestión del Medio Ambiente de Cali", "DAGMA"),
    ("Área Metropolitana del Valle de Aburrá", "AREA METROPOLITANA DEL VALLE DE ABURRA"),
    ("Departamento Administrativo Distrital de Sostenibilidad Ambiental de Santa Marta", "DADSA"),
    ("Establecimiento Público Ambiental Barranquilla Verde", "BARRANQUILLA VERDE"),
    ("Establecimiento Público Ambiental de Cartagena", "EPA CARTAGENA"),
    ("Establecimiento Público Ambiental de Buenaventura", "EPA BUENAVENTURA"),
]

UNIVERSITIES = [
    "Universidad de la Amazonia", "Universidad de Antioquia", "Universidad del Atlántico",
    "Universidad de Caldas", "Universidad de Cartagena", "Universidad del Cauca",
    "Universidad Colegio Mayor de Cundinamarca", "Universidad de Córdoba",
    "Universidad de Cundinamarca", "Universidad Distrital Francisco José de Caldas",
    "Universidad Francisco de Paula Santander - Cúcuta",
    "Universidad Francisco de Paula Santander - Ocaña", "Universidad Industrial de Santander",
    "Universidad de La Guajira", "Universidad de los Llanos", "Universidad del Magdalena",
    "Universidad Militar Nueva Granada", "Universidad Nacional Abierta y a Distancia",
    "Universidad Nacional de Colombia", "Universidad de Nariño", "Universidad del Pacífico",
    "Universidad de Pamplona", "Universidad Pedagógica Nacional",
    "Universidad Pedagógica y Tecnológica de Colombia", "Universidad Popular del Cesar",
    "Universidad del Quindío", "Universidad de Sucre", "Universidad Surcolombiana",
    "Universidad Tecnológica del Chocó Diego Luis Córdoba", "Universidad Tecnológica de Pereira",
    "Universidad del Tolima", "Universidad del Valle", "Universidad Internacional del Trópico Americano",
    "Universidad Autónoma Indígena Intercultural",
]

NATIONAL_UNIVERSITIES = {
    "Universidad Nacional de Colombia", "Universidad Nacional Abierta y a Distancia",
    "Universidad del Cauca", "Universidad de Caldas", "Universidad de Córdoba",
    "Universidad Pedagógica Nacional", "Universidad Pedagógica y Tecnológica de Colombia",
    "Universidad Popular del Cesar", "Universidad Colegio Mayor de Cundinamarca",
    "Universidad Surcolombiana", "Universidad Tecnológica del Chocó Diego Luis Córdoba",
    "Universidad de los Llanos", "Universidad Tecnológica de Pereira", "Universidad de la Amazonia",
    "Universidad del Pacífico", "Universidad Militar Nueva Granada",
}


def universe() -> list[dict]:
    rows = [E("CAR_CDS_CORE", n, s, [s, n]) for n, s in CAR]
    rows.append(E("CORMAGDALENA_SEPARATE", "Corporación Autónoma Regional del Río Grande de la Magdalena",
                  "CORMAGDALENA", ["Cormagdalena", "Corporación Autónoma Regional del Río Grande de la Magdalena"]))
    rows += [E("URBAN_ENVIRONMENTAL_AUTHORITIES", n, s, [s, n]) for n, s in URBAN]
    judicial = [
        ("Corte Constitucional", None), ("Corte Suprema de Justicia", None),
        ("Consejo de Estado", None), ("Comisión Nacional de Disciplina Judicial", None),
        ("Consejo Superior de la Judicatura", None),
        ("Dirección Ejecutiva de Administración Judicial", "DIRECCION EJECUTIVA DE ADMINISTRACION JUDICIAL"),
        ("Fiscalía General de la Nación", "FISCALIA GENERAL DE LA NACION"),
        ("Instituto Nacional de Medicina Legal y Ciencias Forenses", None),
    ]
    rows += [E("JUDICIAL_CORE", n, s) for n, s in judicial]
    seccionales = [
        "Bogotá", "Armenia", "Bucaramanga", "Cali", "Cartagena", "Cúcuta", "Medellín",
        "Montería", "Neiva", "Pasto", "Pereira", "Popayán", "Santa Marta", "Sincelejo",
        "Tunja", "Valledupar", "Villavicencio", "Ibagué", "Riohacha", "Quibdó",
        "Cundinamarca - Amazonas",
    ]
    rows += [E("JUDICIAL_SECCIONAL_BUYERS",
               f"Dirección Seccional de Administración Judicial de {city}",
               f"DIRECCION SECCIONAL ADMINISTRACION JUDICIAL {city}") for city in seccionales]
    control = [
        ("Contraloría General de la República", "CGR NIVEL CENTRAL"),
        ("Auditoría General de la República", None),
        ("Procuraduría General de la Nación", None), ("Defensoría del Pueblo", None),
        ("Fondo de Bienestar Social de la Contraloría General de la República", None),
        ("Instituto de Estudios del Ministerio Público", None),
    ]
    rows += [E("CONTROL_NATIONAL", n, s) for n, s in control]
    rows += [E("LEGISLATIVE", "Senado de la República", "CONGRESO DE LA REPUBLICA HONORABLE SENADO"),
             E("LEGISLATIVE", "Cámara de Representantes")]
    rows += [E("ELECTORAL", "Registraduría Nacional del Estado Civil"),
             E("ELECTORAL", "Consejo Nacional Electoral"),
             E("ELECTORAL", "Fondo Rotatorio de la Registraduría Nacional del Estado Civil")]
    rows += [E("AUTONOMOUS_NATIONAL", "Banco de la República"),
             E("AUTONOMOUS_NATIONAL", "Comisión Nacional del Servicio Civil", "CNSC")]
    for n in UNIVERSITIES:
        search = ("UFPS OCAÑA" if n.endswith("Ocaña") else "UNITROPICO" if "Trópico" in n
                  else "UNAD" if n == "Universidad Nacional Abierta y a Distancia"
                  else "UPTC" if n == "Universidad Pedagógica y Tecnológica de Colombia"
                  else "UNIMAGDALENA" if n == "Universidad del Magdalena"
                  else "UNIQUINDIO" if n == "Universidad del Quindío" else n)
        rows.append(E("PUBLIC_UNIVERSITIES_SECOP", n, search,
                      official_scope="NATIONAL_MANUAL_16" if n in NATIONAL_UNIVERSITIES else "SUE_PUBLIC"))
    defense = [
        ("Ejército Nacional", "EJERCITO NACIONAL"), ("Armada de Colombia", "ARMADA NACIONAL"),
        ("Fuerza Aeroespacial Colombiana", None), ("Policía Nacional", "POLICIA NACIONAL"),
        ("Agencia Logística de las Fuerzas Militares", None), ("Hospital Militar Central", None),
        ("Fondo Rotatorio de la Policía Nacional", None), ("Caja de Retiro de las Fuerzas Militares", "CREMIL"),
        ("Caja de Sueldos de Retiro de la Policía Nacional", "CASUR"),
    ]
    rows += [E("DEFENSE_FORCE", n, s) for n, s in defense]
    decentralized = [
        ("Instituto Nacional de Vías", "INVIAS"), ("Agencia Nacional de Infraestructura", "ANI"),
        ("Aeronáutica Civil", "AERONAUTICA CIVIL"), ("Agencia Nacional de Seguridad Vial", "ANSV"),
        ("Superintendencia de Transporte", None), ("SENA", "SERVICIO NACIONAL DE APRENDIZAJE"),
        ("ICBF", "INSTITUTO COLOMBIANO DE BIENESTAR FAMILIAR"), ("Prosperidad Social", None),
        ("Autoridad Nacional de Licencias Ambientales", "ANLA"),
        ("Parques Nacionales Naturales de Colombia", None), ("IDEAM", "IDEAM"),
        ("Instituto Colombiano Agropecuario", "ICA"), ("Agencia de Desarrollo Rural", None),
        ("Agencia Nacional de Tierras", None), ("UPRA", "UNIDAD DE PLANIFICACION RURAL AGROPECUARIA"),
        ("AUNAP", None), ("Agencia Nacional de Minería", "ANM"),
        ("DIAN", "DIAN"),
        ("UGPP", "UNIDAD DE GESTION PENSIONAL Y PARAFISCALES"),
    ]
    rows += [E("DECENTRALIZED_INVENTORY", n, s) for n, s in decentralized]
    return rows


def fetch(url: str, params: dict) -> list[dict]:
    req = urllib.request.Request(url + "?" + urllib.parse.urlencode(params),
                                 headers={"User-Agent": "SECOPPAL-R2C-audit/1.0"})
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=25) as response:
                payload = json.load(response)
            return payload if isinstance(payload, list) else []
        except Exception as exc:
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    assert last_error is not None
    raise last_error


def key_tokens(value: str) -> set[str]:
    stop = {"de", "del", "la", "el", "y", "para", "las", "los", "nacional", "colombia"}
    return {t for t in normalize_text(value).split() if len(t) > 2 and t not in stop}


def score_norm(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c)).lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value).split())


def candidate_score(logical: str, search: str, source_name: str) -> float:
    a, b, s = score_norm(logical), score_norm(source_name), score_norm(search)
    if b == s:
        return 1.0
    logical_tokens, source_tokens = key_tokens(logical), key_tokens(source_name)
    overlap = len(logical_tokens & source_tokens) / max(1, len(logical_tokens | source_tokens))
    ratio = SequenceMatcher(None, a, b).ratio()
    search_bonus = 0.22 if len(s) >= 3 and s in b else 0.0
    return min(1.0, overlap * 0.55 + ratio * 0.35 + search_bonus)


def discover_one(row: dict) -> dict:
    found: dict[tuple[str, str], dict] = {}
    for dataset, url, field in (("procesos", PROC_URL, "entidad"),
                                ("contratos", CONT_URL, "nombre_entidad")):
        try:
            data = fetch(url, {"$q": row["search"], "$select": f"{field},nit_entidad",
                               "$limit": 100})
        except Exception as exc:  # keep uncertainty explicit
            row.setdefault("source_errors", []).append(f"{dataset}: {exc}")
            continue
        for item in data:
            name, nit = item.get(field), re.sub(r"\D", "", item.get("nit_entidad", ""))
            if not name or not nit:
                continue
            k = (nit, name)
            found.setdefault(k, {"nit": nit, "name": name, "datasets": set(), "score": 0.0})
            found[k]["datasets"].add(dataset)
            found[k]["score"] = candidate_score(row["logical_name"], row["search"], name)
    candidates = sorted(found.values(), key=lambda x: (x["score"], len(x["datasets"])), reverse=True)
    row["registry_candidates"] = [
        {**c, "datasets": sorted(c["datasets"]), "score": round(c["score"], 3)} for c in candidates[:8]
    ]
    best = candidates[0] if candidates and candidates[0]["score"] >= 0.52 else None
    row["nit"] = best["nit"] if best else None
    row["registry_match_confidence"] = (
        "HIGH" if best and best["score"] >= 0.68 else "REVIEW" if best else "NONE"
    )
    return row


def fetch_names_and_counts(nits: list[str], dataset: str, recent: bool) -> list[dict]:
    if not nits:
        return []
    url, field, dfield = ((PROC_URL, "entidad", "fecha_de_publicacion_del") if dataset == "procesos"
                          else (CONT_URL, "nombre_entidad", "fecha_de_firma"))
    quoted = ",".join(f'"{n}"' for n in nits)
    where = f"nit_entidad in ({quoted})"
    if recent:
        where += f' and {dfield} >= "{RECENT_SINCE}"'
    return fetch(url, {"$select": f"nit_entidad,{field},count(*) as rows", "$where": where,
                       "$group": f"nit_entidad,{field}", "$limit": 5000})


def attach_volume(rows: list[dict]) -> None:
    nits = sorted({r["nit"] for r in rows if r.get("nit")})
    evidence: dict[tuple[str, str, bool], list[dict]] = defaultdict(list)
    jobs = []
    for start in range(0, len(nits), 12):
        chunk = nits[start:start + 12]
        jobs.extend((chunk, dataset, recent) for dataset in ("procesos", "contratos")
                    for recent in (False, True))
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(fetch_names_and_counts, *job): job for job in jobs}
        for future in as_completed(futures):
            _, dataset, recent = futures[future]
            try:
                items = future.result()
            except Exception:
                items = []
            field = "entidad" if dataset == "procesos" else "nombre_entidad"
            for item in items:
                nit = re.sub(r"\D", "", item.get("nit_entidad", ""))
                evidence[(nit, dataset, recent)].append(
                    {"name": item.get(field), "rows": int(item.get("rows", 0))})
    for row in rows:
        nit = row.get("nit")
        for dataset in ("procesos", "contratos"):
            all_items = evidence[(nit, dataset, False)] if nit else []
            rec_items = evidence[(nit, dataset, True)] if nit else []
            if row["logical_name"] not in {"SENA", "ICBF", "Policía Nacional", "Ejército Nacional", "Armada de Colombia"}:
                selected = {normalize_text(c["name"]) for c in row.get("registry_candidates", [])
                            if c.get("nit") == nit and c.get("score", 0) >= 0.52}
                all_items = [x for x in all_items if normalize_text(x["name"]) in selected]
                rec_items = [x for x in rec_items if normalize_text(x["name"]) in selected]
            row[f"{dataset[:-1]}_source_names"] = [x["name"] for x in all_items]
            row[f"all_time_{dataset}"] = sum(x["rows"] for x in all_items) if nit else None
            row[f"recent_12m_{dataset}"] = sum(x["rows"] for x in rec_items) if nit else None
        pnames, cnames = row["proceso_source_names"], row["contrato_source_names"]
        distinct_names = set(map(normalize_text, pnames + cnames))
        if not pnames and not cnames:
            row["secop_identity_shape"] = "NO_SECOP_II_EVIDENCE"
        elif len(distinct_names) > 2:
            row["secop_identity_shape"] = "FRAGMENTED_BUYER"
        elif set(map(normalize_text, pnames)) != set(map(normalize_text, cnames)):
            row["secop_identity_shape"] = "SOURCE_NAME_SPLIT"
        else:
            row["secop_identity_shape"] = "SINGLE_PHYSICAL_NAME"


def audit_resolution(rows: list[dict]) -> None:
    resolver = EntityResolver(DB)
    db = json.loads(DB.read_text(encoding="utf-8"))
    by_name = {normalize_text(x["official_name"]): x for x in db}
    alias_owners: dict[str, list[str]] = defaultdict(list)
    for rec in db:
        for alias in rec.get("aliases", []):
            alias_owners[normalize_text(alias)].append(rec["official_name"])
    for row in rows:
        expected_names = set(row.get("proceso_source_names", []) + row.get("contrato_source_names", []))
        expected_norm = {normalize_text(x) for x in expected_names}
        existing = [by_name[x] for x in expected_norm if x in by_name]
        row["existing_gazetteer_records"] = [x["official_name"] for x in existing]
        row["existing_aliases"] = sorted({a for x in existing for a in x.get("aliases", [])})
        probes = []
        statuses = []
        for query in row["natural_queries"]:
            res = resolver.resolve_entidad(query)
            value = res.value
            correct = bool(value and normalize_text(value) in expected_norm)
            exact_support = value in alias_owners.get(normalize_text(query), [])
            if correct and res.method == "exact" and exact_support:
                status = "PASS_EXACT"
            elif correct and str(res.method).startswith("fuzzy"):
                status = "FUZZY_ONLY"
            elif correct:
                status = "PARTIAL"
            elif value:
                status = "WRONG_OWNER"
            elif row.get("secop_identity_shape") == "NO_SECOP_II_EVIDENCE":
                status = "NO_SECOP_II_EVIDENCE"
            else:
                status = "MISSING"
            statuses.append(status)
            probes.append({"query": query, "resolved_owner": value,
                           "resolution_method": res.method, "classification": status,
                           "exact_alias_owners": alias_owners.get(normalize_text(query), [])})
        precedence = ["WRONG_OWNER", "FUZZY_ONLY", "MISSING", "NO_SECOP_II_EVIDENCE", "PARTIAL", "PASS_EXACT"]
        if len(set(statuses)) > 1 and "PASS_EXACT" in statuses and not any(s in statuses for s in ("WRONG_OWNER", "FUZZY_ONLY")):
            final = "PARTIAL"
        else:
            final = next((s for s in precedence if s in statuses), "REVIEW")
        if row.get("registry_match_confidence") == "REVIEW" and final not in {"WRONG_OWNER", "FUZZY_ONLY"}:
            final = "REVIEW"
        row["resolution_probes"] = probes
        row["classification"] = final
        row["resolved_owner"] = probes[0]["resolved_owner"] if probes else None
        row["resolution_method"] = probes[0]["resolution_method"] if probes else None


def summarize(rows: list[dict]) -> dict:
    summaries = {}
    for group in sorted({r["group"] for r in rows}):
        subset = [r for r in rows if r["group"] == group]
        counts = Counter(r["classification"] for r in subset)
        n = len(subset)
        summaries[group] = {
            "universe_size": n,
            **{k.lower(): counts[k] for k in ("PASS_EXACT", "PARTIAL", "MISSING", "WRONG_OWNER",
                                              "FUZZY_ONLY", "AMBIGUOUS", "NO_SECOP_II_EVIDENCE", "REVIEW")},
            "coverage_precision_safe": round(counts["PASS_EXACT"] / n, 4) if n else None,
            "unsafe_resolution_rate": round((counts["WRONG_OWNER"] + counts["FUZZY_ONLY"]) / n, 4) if n else None,
            "all_time_processes": sum(r.get("all_time_procesos") or 0 for r in subset),
            "all_time_contracts": sum(r.get("all_time_contratos") or 0 for r in subset),
            "recent_12m_processes": sum(r.get("recent_12m_procesos") or 0 for r in subset),
            "recent_12m_contracts": sum(r.get("recent_12m_contratos") or 0 for r in subset),
            "unknown_volume_entities": sum(r.get("nit") is None for r in subset),
            "no_secop_evidence_entities": sum(
                r.get("secop_identity_shape") == "NO_SECOP_II_EVIDENCE" for r in subset),
        }
    return summaries


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()
    rows = universe()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(discover_one, dict(row)): i for i, row in enumerate(rows)}
        discovered = [None] * len(rows)
        for future in as_completed(futures):
            discovered[futures[future]] = future.result()
    rows = discovered
    attach_volume(rows)
    audit_resolution(rows)
    payload = {
        "audit": "INSTITUTIONAL-BUYER-UNIVERSE-R2C", "type": "discovery/read-only",
        "as_of": date.today().isoformat(), "recent_since": RECENT_SINCE[:10],
        "secop_authority": {"processes": "p6dx-8zbt", "contracts": "jbjy-vk9h"},
        "official_sources": [
            {"scope": "state_structure", "authority": "Función Pública — Manual de Estructura del Estado",
             "url": "https://www.funcionpublica.gov.co/eva/gestornormativo/manual-estado/index.php"},
            {"scope": "car_cds_33", "authority": "Ministerio de Ambiente",
             "url": "https://www.minambiente.gov.co/eleccion-car/"},
            {"scope": "urban_environmental_authorities_7", "authority": "Ministerio de Ambiente",
             "url": "https://www.minambiente.gov.co/wp-content/uploads/2023/07/DS-A-SCD-10_V3_copia_controlada.pdf"},
            {"scope": "public_universities", "authority": "Ministerio de Educación / SUE",
             "url": "https://www.mineducacion.gov.co/1759/w3-printer-89243.html"},
        ],
        "universe_reconciliation": {
            "car_core_count": 33,
            "cormagdalena_classification": "SPECIAL_NATIONAL_CORPORATE_ENTITY_EICE_OUTSIDE_CAR_CORE",
            "final_environmental_audit_universe": {"car_cds": 33, "cormagdalena_separate": 1,
                                                     "urban_environmental_authorities": 7},
            "university_official_universe": {"definition": "Función Pública national autonomous universities", "size": 16},
            "secop_public_university_universe": {"definition": "34 SUE public purchasing nodes", "size": 34},
        },
        "methodology": {
            "identity_order": ["official universe", "SECOP II", "NIT", "source physical name"],
            "volume_note": "Counts by NIT; processes and contracts remain separate.",
            "recent_date_fields": {"processes": "fecha_de_publicacion_del", "contracts": "fecha_de_firma"},
            "classification_precedence": ["WRONG_OWNER", "FUZZY_ONLY", "MISSING", "NO_SECOP_II_EVIDENCE", "PARTIAL", "PASS_EXACT"],
        },
        "buyer_group_candidates": [
            {"query": "Rama Judicial", "classification": "BUYER_GROUP_CANDIDATE"},
            {"query": "Congreso de la República", "classification": "BUYER_GROUP_CANDIDATE",
             "note": "No separate SECOP II physical root; Senate and Chamber buy separately."},
            {"query": "Fuerza Pública", "classification": "BUYER_GROUP_CANDIDATE"},
            {"query": "CAR de la Costa", "classification": "BUYER_GROUP_CANDIDATE"},
            {"query": "universidades públicas", "classification": "BUYER_GROUP_CANDIDATE"},
            {"query": "Gobernación de Cundinamarca", "classification": "BUYER_GROUP_CANDIDATE",
             "note": "R2A proved a fragmented physical root."},
        ],
        "product_assessment": {
            "CAR_CDS_CORE": {"coverage_gap": "HIGH", "unsafe_errors": "HIGH",
                              "identity_complexity": "LOW_MEDIUM", "expected_product_value": "HIGH", "priority": "P0"},
            "CONTROL_NATIONAL": {"coverage_gap": "HIGH", "unsafe_errors": "HIGH",
                                  "identity_complexity": "LOW", "expected_product_value": "HIGH", "priority": "P1"},
            "PUBLIC_UNIVERSITIES_SECOP": {"coverage_gap": "MEDIUM", "unsafe_errors": "HIGH",
                                           "identity_complexity": "HIGH", "expected_product_value": "HIGH", "priority": "P1"},
            "DEFENSE_FORCE": {"coverage_gap": "MEDIUM", "unsafe_errors": "HIGH",
                               "identity_complexity": "HIGH", "expected_product_value": "HIGH", "priority": "P2"},
            "JUDICIAL_CORE": {"coverage_gap": "HIGH", "unsafe_errors": "HIGH",
                               "identity_complexity": "HIGH", "expected_product_value": "MEDIUM", "priority": "P2"},
            "LEGISLATIVE": {"coverage_gap": "LOW", "unsafe_errors": "MEDIUM",
                             "identity_complexity": "LOW", "expected_product_value": "MEDIUM", "priority": "P2"},
            "ELECTORAL": {"coverage_gap": "HIGH", "unsafe_errors": "HIGH",
                           "identity_complexity": "MEDIUM", "expected_product_value": "MEDIUM", "priority": "P2"},
            "AUTONOMOUS_NATIONAL": {"coverage_gap": "NONE", "unsafe_errors": "NONE",
                                     "identity_complexity": "LOW", "expected_product_value": "MEDIUM", "priority": "DEFER"},
        },
        "decision": {"next_recommended": "R2D_CAR_CORE", "second_priority": "R2E_CONTROL_NATIONAL",
                     "third_priority": "R2F_PUBLIC_UNIVERSITY_PILOT"},
        "group_summary": summarize(rows), "entities": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload["group_summary"], ensure_ascii=False, indent=2))
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

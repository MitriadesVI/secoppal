"""A2 AUTHORITY HEALTH: ¿qué autoridades deterministas están sanas como «evidencia estructurada -> resolución»?

Uso (cwd = raíz del worktree, PYTHONPATH=.):
    python scripts/audits/m1_search/authority_probes.py [--out PATH] [--data-dir DIR]

Sin red (los sockets se bloquean), sin telemetría Burr, persistencia (feedback) redirigida a un tmpdir, sólo
entradas públicas (razones sociales de personas jurídicas, topónimos, frases genéricas). No modifica `app/`.

Qué mide, por autoridad (BUYER, ACTOR, GEOGRAPHY, TIME, MONEY, MODALITY, CONTRACT_UNIVERSE, STATE):

  * capa `authority`: la función pura que un host llamaría con el SPAN citado (no con el turno crudo);
  * capa `host_chain`: lo que el host Foundation actual hace con ese mismo span
    (`admit_proposal` -> `project_accepted` -> SoQL), con una propuesta de una sola condición construida a mano;
  * capa `aggregate`: barridos sobre el catálogo completo (consistencia de ownership, contaminación del directorio).

Cada sonda declara un ORACLE (`expected`) y se clasifica:

  PASS         el valor correcto, o la negativa correcta cuando la entrada es intrínsecamente insuficiente.
  DECLINE_OK   declinó (clarificación / sin match) y la entrada era insuficiente o el catálogo no tiene la entidad.
  FAIL_LOUD    declinó donde existía un valor correcto único (falso negativo: seguro, el usuario ve una pregunta).
  FAIL_SILENT  devolvió / ejecutó un valor equivocado sin señal (el grave).
  GATED        el valor equivocado existe en la autoridad pero otro control del host lo bloquea hoy.
  GAP          no existe capacidad llamable (o el host no la hospeda): se reporta, no se puntúa como error.
  INFO         observación sin oracle (política declarada, ambigüedad por diseño).

Las sondas FAIL_SILENT llevan `reachable_from_host` (True si el host Foundation actual las ejecuta tal cual).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path

# ── entorno hermético: ANTES de importar `app` ──────────────────────────────────────────────────────────────
REPO = Path.cwd()
TODAY = date(2026, 9, 30)
NOW = datetime.fromisoformat("2026-09-30T12:00:00-05:00")
DATA_DIR = Path(os.environ.get("SECOPPAL_DATA_DIR", "/Users/rodrigoortiz/Documents/secoppal/data"))
ACTOR_INDEX = Path(os.environ.get("SECOPPAL_ACTOR_INDEX", str(DATA_DIR / "actor_alias_index.sqlite")))
_SCRATCH = tempfile.mkdtemp(prefix="a2_authority_probes_")
os.environ.setdefault("SECOP_FEEDBACK_PATH", str(Path(_SCRATCH) / "feedback.jsonl"))
os.environ.setdefault("SECOP_FEEDBACK_DB_PATH", str(Path(_SCRATCH) / "secoppal.db"))
os.environ.setdefault("SECOP_ACTOR_ALIAS_INDEX_PATH", str(ACTOR_INDEX))
sys.path.insert(0, str(REPO))


def _no_network(*args, **kwargs):
    raise AssertionError("no network")


socket.socket.connect = _no_network  # type: ignore[assignment]
import burr.telemetry  # noqa: E402

burr.telemetry.g_telemetry_enabled = False

from app.core import contract_universe as CU  # noqa: E402
from app.core.actor_identity import compute_check_digit, normalize_identity  # noqa: E402
from app.core.actor_name import normalize_actor_name  # noqa: E402
from app.core.buyer_source_names import get_registry, project_buyer_names  # noqa: E402
from app.core.contract_type_resolver import resolve_contract_types  # noqa: E402
from app.core.contract_universe_intent import request as universe_request  # noqa: E402
from app.core.entity_resolver import (  # noqa: E402
    EntityResolver, department_mention_resolution, geo_mention_resolution,
)
from app.core.estado_families import read_process_state_mentions, resolve_estado  # noqa: E402
from app.core.foundation_admission import admit_proposal, state_value  # noqa: E402
from app.core.foundation_projection import ProjectionError, project_accepted  # noqa: E402
from app.core.foundation_value_direction import directions, operator_reason  # noqa: E402
from app.core.interpretation_contract import (  # noqa: E402
    AcceptanceStatus, ConditionExpression, InterpretationProposal, LogicOperator, Polarity,
    ProposedCondition, ProposedRequest, RawSpan,
)
from app.core.modality_resolver import resolve as resolve_modality  # noqa: E402
from app.core.money_admission import is_admissible_amount  # noqa: E402
from app.core.money_polarity import resolve_money_polarity  # noqa: E402
from app.core.municipal_territory import get_municipal_territories  # noqa: E402
from app.core.query_router import QueryRouter  # noqa: E402
from app.core.semantic_dimensions import SemanticDimension as D  # noqa: E402
from app.core.temporal_semantics import civil_today, resolve_temporal_scope  # noqa: E402
from app.core.workflow.actions.actor_resolution import resolve_actor_params  # noqa: E402
from app.data.actor_alias_resolver import ActorNameResolver  # noqa: E402
from app.data.colombia_geography import REGION_ALIASES, ZONES_BY_NAME  # noqa: E402
from app.data.departamentos import DEPARTAMENTOS  # noqa: E402
from app.utils.money import money_to_cop, normalize_text  # noqa: E402

POS, NEG = Polarity.POSITIVE, Polarity.NEGATIVE
PROBES: list[dict] = []
_SEQ: Counter = Counter()


# ── infraestructura de registro ─────────────────────────────────────────────────────────────────────────────
# Funciones que el host Foundation actual SÍ invoca (foundation_admission.py / foundation_projection.py). Un fallo
# silencioso en cualquier otra es LATENTE: no se ejecuta hoy, se expone cuando M1 la conecte.
HOST_CALLS = ("EntityResolver.resolve_entidad", "department_mention_resolution", "money_to_cop", "operator_reason",
              "modality_resolver.resolve", "contract_type_resolver.resolve_contract_types", "admit_proposal")


def rec(authority, layer, category, call, inp, expected, got, outcome, note="", silent=None, reachable=None,
        severity=None, producer_error=None):
    _SEQ[authority] += 1
    row = {
        "id": f"{authority}-{_SEQ[authority]:03d}", "authority": authority, "layer": layer, "category": category,
        "call": call, "input": inp, "expected": expected, "got": got, "outcome": outcome,
    }
    if note:
        row["note"] = note
    if outcome == "FAIL_SILENT":
        row["silent"] = True
        if reachable is None:
            reachable = layer == "host_chain" or call.startswith(HOST_CALLS)
        row["reachable_from_host"] = bool(reachable)
        if not reachable:
            row["note"] = (row.get("note", "") + " [LATENTE: el host Foundation actual no invoca esta función; se expone "
                           "al conectarla en M1]").strip()
        if producer_error is not None:
            row["requires_producer_error"] = producer_error
        row["severity"] = severity or "HIGH"
    PROBES.append(row)
    return row


def safe(authority, layer, category, call, inp, expected, fn, judge, note=""):
    """Ejecuta `fn()`, juzga con `judge(got) -> (outcome, note_extra, kwargs)`; una excepción es un hallazgo."""
    try:
        got = fn()
    except Exception as exc:  # una excepción de la autoridad ES un resultado
        return rec(authority, layer, category, call, inp, expected, {"EXCEPTION": repr(exc)[:300]},
                   "FAIL_LOUD", note="la autoridad lanzó una excepción")
    outcome, extra, kw = judge(got)
    return rec(authority, layer, category, call, inp, expected, got, outcome,
               note=" ".join(x for x in (note, extra) if x), **kw)


def jd(outcome, note="", **kw):
    return outcome, note, kw


# ── host chain: una propuesta de UNA condición (+ tema «aseo») por el host Foundation real ─────────────────────
_ER: EntityResolver | None = None


def entity_resolver() -> EntityResolver:
    global _ER
    if _ER is None:
        _ER = EntityResolver(Path("app/data/aliases_db.json"))
    return _ER


_TOPIC_RE = re.compile(r"\(unaccent\(UPPER\(\w+\)\) LIKE unaccent\(UPPER\('%aseo%'\)\)"
                       r"(?: OR unaccent\(UPPER\(\w+\)\) LIKE unaccent\(UPPER\('%aseo%'\)\))*\)")


def where_of(soql: str) -> str:
    """El WHERE aportado por la condición bajo prueba: sin universo contractual ni el tema «aseo» de relleno."""
    if " WHERE " not in soql:
        return ""
    w = re.sub(r"\s+ORDER BY.*$", "", soql.split(" WHERE ", 1)[1], flags=re.S)
    w = _TOPIC_RE.sub("", w.replace(CU.predicate(), ""))
    w = re.sub(r"(?:\s*AND\s*){2,}", " AND ", w)
    return re.sub(r"^(?:\s*AND\s*)+|(?:\s*AND\s*)+$", "", w).strip()[:260]


def host_chain(dim, op, pol, quote, *, processes=False):
    """Devuelve lo que el host hace con `quote` como condición `dim/op/pol`. Sin red, sin ejecutar SoQL."""
    lead = "procesos de aseo " if processes else "contratos de aseo "
    subject = "processes" if processes else "contracts"
    raw = f"{lead}{quote}"
    noun = lead.split()[0]
    topic_at, quote_at, noun_at = raw.index("aseo"), raw.index(quote, len(lead) - 1), raw.index(noun)
    conditions = (
        ProposedCondition("q1.c1", ("q1",), D.TOPIC, "contains", POS, "aseo",
                          (RawSpan(topic_at, topic_at + 4, "aseo"),)),
        ProposedCondition("q1.c2", ("q1",), dim, op, pol, quote,
                          (RawSpan(quote_at, quote_at + len(quote), quote),)),
    )
    expression = ConditionExpression(LogicOperator.AND, children=tuple(
        ConditionExpression(LogicOperator.ATOM, c.condition_id) for c in conditions))
    proposal = InterpretationProposal(
        hashlib.sha256(raw.encode()).hexdigest(), "a2-authority-probe", "1",
        (ProposedRequest("q1", "q1", "SEARCH", subject, (RawSpan(0, len(raw), raw),),
                         universe_claim=noun, universe_evidence=RawSpan(noun_at, noun_at + len(noun), noun),
                         condition_expression=expression),),
        conditions)
    accepted = admit_proposal(proposal, raw, entity_resolver=entity_resolver())
    out = {"status": accepted.status.value, "reason": accepted.reason,
           "resolved": [list(r.resolved_ids) for r in accepted.resolutions if r.target_id == "q1.c2"]}
    if accepted.status is AcceptanceStatus.READY:
        try:
            projected = project_accepted(accepted, reference_instant=NOW)
            soql = projected.manifest[0].soql if projected.manifest else ""
            out["projected"] = True
            out["where_fragment"] = where_of(soql)
        except ProjectionError as exc:
            out["projected"] = False
            out["projection_error"] = str(exc)
    return out


def hc_value(out):
    return out["resolved"][0] if out.get("resolved") else None


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# BUYER
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
_ADMITTED_METHODS = {"exact", "exact_official", "exact_normalized"}


def buyer_admitted(res) -> str | None:
    """Réplica EXACTA de la puerta de `foundation_admission.admit_proposal` (líneas 106-108)."""
    ok = res.method in _ADMITTED_METHODS or res.method.startswith("strict_")
    return res.value if ok else None


def probe_buyer():
    er = entity_resolver()
    reg = get_registry()
    A = "BUYER"
    # (texto, categoría, spec): spec = ("accept", {nombres}) | ("decline",) | ("accept_or_decline", {nombres})
    cases = [
        # -- nacionales / wrong-owner conocido
        ("INVIAS", "wrong_owner", ("accept", {"INVIAS"})),
        ("invías", "wrong_owner", ("accept", {"INVIAS"})),
        ("el INVIAS", "wrong_owner", ("accept", {"INVIAS"})),
        ("Instituto Nacional de Vías", "national", ("accept", {"INVIAS"})),
        ("ANI", "national", ("accept", {"ANI"})),
        ("Agencia Nacional de Infraestructura", "national", ("accept", {"ANI"})),
        # -- gobernaciones
        ("Gobernación del Huila", "gobernacion", ("accept", {"GOBERNACION DEL HUILA*"})),
        ("gobernacion del huila", "gobernacion", ("accept", {"GOBERNACION DEL HUILA*"})),
        ("Gobernación de Antioquia", "gobernacion", ("accept", {"DEPARTAMENTO DE ANTIOQUIA//"})),
        ("Gobernación del Atlántico", "gobernacion", ("accept", {"DEPARTAMENTO DEL ATLANTICO**"})),
        ("Gobernación de Santander", "gobernacion", ("accept", {"DEPARTAMENTO DE SANTANDER"})),
        ("Gobernación de Córdoba", "gobernacion", ("accept", {"DEPARTAMENTO DE CORDOBA"})),
        ("Gobernación de Bolívar", "gobernacion", ("accept", {"GOBERNACIÓN DE BOLÍVAR//"})),
        ("Gobernación del Meta", "gobernacion", ("accept", {"DEPARTAMENTO DEL META"})),
        ("Gobernación de Caldas", "gobernacion", ("accept", {"GOBERNACION DE CALDAS"})),
        ("Gobernación de Cundinamarca", "gobernacion", ("decline",)),
        ("Gobernación del Valle del Cauca", "gobernacion", ("decline",)),
        # -- ministerios
        ("Ministerio de Educación", "ministerio", ("accept", {"MINISTERIO DE EDUCACION NACIONAL (MEN)"})),
        ("Ministerio de Salud", "ministerio", ("accept", {"MINSALUD"})),
        ("Ministerio de Transporte", "ministerio", ("accept", {"MINISTERIO DE TRANSPORTE*"})),
        ("Ministerio de Defensa", "ministerio", ("accept", {"MINISTERIO DE DEFENSA NACIONAL"})),
        ("Ministerio de Hacienda", "ministerio", ("accept", {"MINISTERIO DE HACIENDA Y CREDITO PUBLICO"})),
        # -- universidades
        ("Universidad Nacional", "universidad", ("accept", {"UNIVERSIDAD NACIONAL DE COLOMBIA"})),
        ("Universidad de Antioquia", "universidad", ("accept", {"UNIVERSIDAD DE ANTIOQUIA"})),
        ("Universidad del Atlántico", "universidad", ("accept", {"UNIATLANTICO"})),
        ("Universidad Distrital", "universidad", ("accept", {"UNIVERSIDAD DISTRITAL FRANCISCO JOSE DE CALDAS"})),
        ("Universidad de Cartagena", "universidad", ("accept", {"Universidad de Cartagena"})),
        ("Universidad Pedagógica", "universidad", ("accept", {"UNIVERSIDAD PEDAGÓGICA NACIONAL"})),
        # -- SENA / ICBF: paraguas con N unidades físicas (un solo comprador «SENA» no existe en SECOP)
        ("SENA", "umbrella", ("decline",)),
        ("Servicio Nacional de Aprendizaje", "umbrella", ("decline",)),
        ("SENA Regional Antioquia", "umbrella", ("decline",)),
        ("SENA Atlántico", "umbrella", ("decline",)),
        ("ICBF", "umbrella", ("decline",)),
        ("Instituto Colombiano de Bienestar Familiar", "umbrella", ("decline",)),
        ("ICBF Bogotá", "umbrella", ("decline",)),
        ("ICBF Regional Atlántico", "umbrella", ("accept", {"ICBF REGIONAL ATLANTICO"})),
        # -- alcaldías
        ("Alcaldía de Barranquilla", "alcaldia", ("accept", {"DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"})),
        ("alcaldia de soledad", "alcaldia", ("accept", {"MUNICIPIO DE SOLEDAD"})),
        ("Alcaldía de Bogotá", "alcaldia", ("decline",)),
        ("Alcaldía de Medellín", "alcaldia", ("accept", {"DISTRITO ESPECIAL DE CIENCIA TECNOLOGIA E INNOVACION DE MEDELLIN"})),
        ("Alcaldía de Cali", "alcaldia", ("decline",)),
        ("Alcaldía de Cartagena", "alcaldia", ("accept", {"ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS"})),
        ("Alcaldía de Santa Marta", "alcaldia", ("accept", {"ALCALDÍA DISTRITAL DE SANTA MARTA"})),
        ("Alcaldía de Villavicencio", "alcaldia", ("accept", {"ALCALDIA MUNICIPIO DE VILLAVICENCIO"})),
        ("Alcaldía de San Gil", "alcaldia", ("decline",)),
        # -- hospitales / ESE (homónimos masivos)
        ("ESE Hospital San Rafael", "hospital", ("decline",)),
        ("Hospital San Rafael", "hospital", ("decline",)),
        ("Hospital Universitario del Valle", "hospital", ("decline",)),
        ("hospital", "generic", ("decline",)),
        ("ESE Hospital", "generic", ("decline",)),
        # -- genéricos
        ("Secretaría de Salud", "generic", ("decline",)),
        ("Gobernación", "generic", ("decline",)),
        ("Alcaldía", "generic", ("decline",)),
        ("Ministerio", "generic", ("decline",)),
        ("Universidad", "generic", ("decline",)),
        # -- otras entidades públicas
        ("Fondo Adaptación", "other", ("decline",)),
        ("Findeter", "other", ("accept", {"FINANCIERA DE DESARROLLO TERRITORIAL S.A."})),
        ("IDU", "other", ("accept", {"INSTITUTO DE DESARROLLO URBANO"})),
        ("EPM", "other", ("accept", {"EMPRESAS PUBLICAS DE MEDELLIN E.S.P."})),
        ("DIAN", "other", ("accept", {"DIRECCION DE IMPUESTOS Y ADUANAS NACIONALES*"})),
        ("IDRD", "wrong_owner", ("accept", {"IDRD - ENTIDAD OFICIAL."})),
        ("ITM", "wrong_owner", ("decline",)),
        ("INDER", "wrong_owner", ("decline",)),
        ("Contraloría General", "other", ("accept", {"CGR-NIVEL CENTRAL"})),
        ("Policía Nacional", "other", ("decline",)),
        ("Concejo de Bogotá", "other", ("decline",)),
    ]
    stats = Counter()
    for text, category, spec in cases:
        res = er.resolve_entidad(text)
        admitted = buyer_admitted(res)
        owners = sorted(er.owners_by_alias.get(normalize_text(text), set()))
        bound = {"procesos": bool(res.value and project_buyer_names(res.value, "p6dx-8zbt")),
                 "contratos": bool(res.value and project_buyer_names(res.value, "jbjy-vk9h"))}
        got = {"value": res.value, "method": res.method, "confidence": res.confidence,
               "like_value": res.like_value, "clarification_needed": res.clarification_needed,
               "alias_owner_count": len(owners), "admitted_by_host_gate": admitted,
               "source_binding": bound}
        kind, names = spec[0], (spec[1] if len(spec) > 1 else set())
        extra = ""
        if kind == "accept":
            expected = f"admitted value in {sorted(names)}"
            if admitted in names:
                outcome = "PASS"
                if not bound["contratos"]:
                    extra = "resuelve bien pero NO tiene binding de nombres físicos: la proyección fallaría (BUYER_SOURCE_PROJECTION_REQUIRED)."
            elif admitted is None:
                outcome = "FAIL_LOUD"
                extra = (f"existe la entidad; la autoridad devolvió method={res.method!r} value={res.value!r} y la "
                         "puerta del host lo descarta.")
            else:
                outcome = "FAIL_SILENT"
                extra = f"resuelve a {admitted!r} (owners del alias={len(owners)})."
        elif kind == "accept_or_decline":
            expected = f"admitted value in {sorted(names)} or decline"
            outcome = ("PASS" if admitted in names else "DECLINE_OK") if admitted in names or admitted is None else "FAIL_SILENT"
        else:
            expected = "decline (insufficient / no single owner): no admitted value"
            if admitted is None:
                outcome = "DECLINE_OK"
                if res.value is not None:
                    extra = (f"la autoridad devuelve {res.value!r} con method={res.method!r}/{res.confidence} (no exacto): "
                             "sólo la puerta de método del host lo contiene.")
                    outcome = "GATED"
            else:
                outcome = "FAIL_SILENT"
                extra = (f"admitido {admitted!r} method={res.method!r} confidence={res.confidence!r}; "
                         f"el alias tiene {len(owners)} owner(s) en el gazetteer.")
        stats[outcome] += 1
        row = rec(A, "authority", category, "EntityResolver.resolve_entidad", text, expected, got, outcome, note=extra,
                  producer_error=False)
        # además: lo que hace el host con ese mismo span (sólo las ambiguas y las silenciosas, para no inflar)
        if outcome == "FAIL_SILENT" or kind == "accept":
            chain = host_chain(D.BUYER, "is", POS, text)
            hv = hc_value(chain)
            if outcome == "FAIL_SILENT" and hv:
                if chain.get("projected"):
                    cn, cx = "FAIL_SILENT", f"el host lo EJECUTA: {chain.get('where_fragment')}"
                else:
                    cn, cx = "GATED", ("el host admite el valor equivocado y sólo lo frena la falta de binding de nombres "
                                       f"físicos ({chain.get('projection_error')}): protección accidental, no una guarda "
                                       "de identidad (en cuanto ese comprador reciba binding, se ejecuta).")
            elif outcome == "FAIL_SILENT":
                cn, cx = "PASS", "el host lo rechaza."
            elif chain.get("projected"):
                cn, cx = "PASS", f"el host lo ejecuta: {chain.get('where_fragment')}"
            else:
                cn, cx = "FAIL_LOUD", (f"resuelve bien pero el host no lo ejecuta ({chain.get('projection_error') or chain['status']}): "
                                       "falta el binding de nombres físicos.")
            rec(A, "host_chain", category, "admit_proposal+project_accepted", text,
                "host executes the right owner (accept) / never a wrong or umbrella owner (decline)", chain, cn, note=cx,
                reachable=bool(chain.get("projected")), producer_error=False)
    # ── los resolvers de ecosistema (SENA/ICBF) ya saben responder «paraguas»; el host Foundation no los consulta ──
    from app.core.icbf_resolver import resolve_icbf_reference
    from app.core.sena_resolver import resolve_sena_reference
    for text, fn, want_type in (("SENA", resolve_sena_reference, "sena_national"), ("ICBF", resolve_icbf_reference, "icbf_national"),
                                ("ICBF Regional Atlántico", resolve_icbf_reference, "icbf_regional")):
        r = fn(text)
        got = None if r is None else {"resolution_type": r.resolution_type, "official_names": len(r.official_names),
                                      "confidence": r.confidence}
        rec(A, "authority", "ecosystem_resolver", f"{fn.__module__.split('.')[-1]}.{fn.__name__}(span)", text,
            f"resolution_type={want_type}", got, "PASS" if got and got["resolution_type"] == want_type else "FAIL_LOUD",
            note="responde bien «paraguas» sobre el SPAN, pero `foundation_admission.py:106` sólo llama a `resolve_entidad`: "
                 "la capacidad existe y no está conectada.")
    # ── barridos agregados ──
    multi = {a: o for a, o in er.owners_by_alias.items() if len(o) > 1}
    methods = Counter()
    wrong = []
    for alias, owners in multi.items():
        res = er.resolve_entidad(alias)
        methods[res.method.split("(")[0]] += 1
        if res.method == "exact":
            wrong.append((alias, res.value, len(owners)))
    single = {a: next(iter(o)) for a, o in er.owners_by_alias.items() if len(o) == 1}
    s_methods = Counter()
    for alias, owner in single.items():
        res = er.resolve_entidad(alias)
        s_methods["owner_ok" if (res.method == "exact" and res.value == owner) else res.method.split("(")[0]] += 1
    names = [r["official_name"] for r in json.loads(Path("app/data/aliases_db.json").read_text(encoding="utf-8"))]
    bound = sum(1 for n in names if reg.binding_for(n))
    agg = {
        "gazetteer_records": len(names), "aliases_total": len(er.owners_by_alias),
        "multi_owner_aliases": len(multi), "multi_owner_resolution_methods": dict(methods),
        "multi_owner_aliases_resolved_silently_as_exact_high": len(wrong),
        "multi_owner_examples": [{"alias": a, "resolves_to": v, "owners": n} for a, v, n in wrong[:12]],
        "single_owner_aliases": len(single), "single_owner_resolution": dict(s_methods),
        "buyer_source_bindings": len(reg), "gazetteer_records_with_binding": bound,
        "gazetteer_records_without_binding": len(names) - bound,
    }
    rec(A, "aggregate", "ownership_sweep", "EntityResolver.resolve_entidad (all aliases)",
        "todos los aliases del gazetteer", "alias de varios owners nunca resuelven como `exact`/high a uno solo",
        agg, "FAIL_SILENT" if wrong else "PASS",
        note=(f"{len(wrong)}/{len(multi)} aliases con >1 owner resuelven method=exact confidence=high al último "
              "owner cargado (`alias_index[alias]=official` es last-writer-wins y el Tier 1 consulta alias_index, "
              "no owners_by_alias)."), severity="CRITICAL")
    rec(A, "aggregate", "ownership_sweep", "EntityResolver.resolve_entidad (single-owner aliases)",
        "aliases con un solo owner", "resuelven a su owner", {"single_owner_resolution": dict(s_methods)},
        "FAIL_LOUD" if s_methods.get("rewrite_unresolved") else "PASS",
        note=f"{s_methods.get('rewrite_unresolved', 0)} aliases propios resuelven `rewrite_unresolved` (falso negativo "
             "seguro; mismatch de familia institucional).")
    rec(A, "aggregate", "binding_coverage", "get_registry().binding_for", "official_name de cada ficha",
        "toda ficha resoluble tiene nombres físicos por dataset", {"with_binding": bound, "without_binding": len(names) - bound},
        "FAIL_LOUD" if bound < len(names) else "PASS",
        note="el host sólo ejecuta compradores con binding; el resto muere en BUYER_SOURCE_PROJECTION_REQUIRED.")
    return stats


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# ACTOR
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _id_str(identity):
    if identity is None:
        return None
    if identity.document_type == "CC":  # persona natural: nunca se vuelca el número
        return "CC:<persona natural, número omitido>"
    return f"{identity.document_type}:{identity.base_number}"


def alias_support(name: str) -> list[dict]:
    """Respaldo del alias exacto en el índice: tipo de documento y nº de observaciones (nunca el número)."""
    con = sqlite3.connect(f"file:{ACTOR_INDEX}?mode=ro", uri=True)
    try:
        rows = con.execute("SELECT document_type, occurrences, disposition FROM actor_aliases WHERE normalized_alias=?",
                           (normalize_actor_name(name),)).fetchall()
    finally:
        con.close()
    return [{"document_type": t, "occurrences": o, "disposition": d} for t, o, d in rows]


def probe_actor():
    A = "ACTOR"
    if not ACTOR_INDEX.is_file():
        rec(A, "authority", "environment", "ActorNameResolver", str(ACTOR_INDEX), "índice presente",
            {"index_missing": True}, "GAP", note="sin el índice SQLite ninguna sonda de nombre es válida (INDEX_UNAVAILABLE).")
        return
    ar = ActorNameResolver(ACTOR_INDEX)
    # (nombre, categoría, spec): ("nit", base) resuelve a ese NIT | ("decline",) | ("loud", base_esperado) existe pero no hay exacto
    cases = [
        ("Caja de Compensación Familiar Compensar", "positive", ("nit", "860066942")),
        ("Universidad de Antioquia", "positive", ("nit", "890980040")),
        ("Cafam", "positive", ("nit", "860013570")),
        ("Caracol Televisión S.A.", "positive", ("nit", "860025674")),
        ("Seguros del Estado S.A.", "positive", ("nit", "860009578")),
        ("Imprenta Nacional de Colombia", "positive", ("nit", "830001113")),
        ("Cruz Roja Colombiana", "positive", ("nit", "899999025")),
        ("Servientrega S.A.", "positive", ("nit", "860512330")),
        ("CRR Soluciones Integrales SAS", "legal_form_variant", ("nit", "901406206")),
        ("CRR Soluciones Integrales S.A.S.", "legal_form_variant", ("nit", "901406206")),
        ("Compensar", "brand_without_legal_form", ("loud", "860066942")),
        ("Colsubsidio", "brand_without_legal_form", ("loud", "860007336")),
        ("Caracol Television", "brand_without_legal_form", ("loud", "860025674")),
        ("Seguros del Estado", "brand_without_legal_form", ("loud", "860009578")),
        ("Universidad Libre", "homonym", ("decline",)),
        ("Banco Popular", "homonym", ("decline",)),
        ("Municipio de San Luis", "homonym", ("decline",)),
        ("Fundación Vida Silvestre", "homonym", ("decline",)),
        ("Federación Nacional de Cafeteros de Colombia", "homonym", ("decline",)),
        ("Unión Temporal", "generic_legal_form", ("decline",)),
        ("Consorcio Vial", "generic_legal_form", ("decline",)),
        ("Consorcio", "generic_legal_form", ("decline",)),
        ("Hospital", "generic_word", ("decline",)),
        ("salud", "generic_word", ("decline",)),
        ("construcción", "generic_word", ("decline",)),
        ("aseo", "generic_word", ("decline",)),
        ("vigilancia", "generic_word", ("decline",)),
        ("pavimentación", "generic_word", ("decline",)),
        ("Barranquilla", "toponym", ("decline",)),
        ("Huila", "toponym", ("decline",)),
        ("Cali", "toponym", ("decline",)),
        ("SENA", "buyer_name", ("decline",)),
        ("INVIAS", "buyer_name", ("decline",)),
        ("Empresa de Acueducto y Alcantarillado de Bogotá", "buyer_name", ("not_cc",)),
        ("XXXXX", "sentinel", ("decline",)),
        ("", "empty", ("decline",)),
    ]
    stats = Counter()
    for name, category, spec in cases:
        res = ar.resolve_exact(name)
        full = ar.resolve(name)
        got = {"status": res.status, "method": res.method, "identity": _id_str(res.identity),
               "candidates": len(res.candidates), "full_resolution_status": full.status,
               "full_resolution_candidates": len(full.candidates)}
        if res.status == "RESOLVED":
            got["alias_support"] = alias_support(name)
        kind = spec[0]
        extra = ""
        if kind == "nit":
            expected = f"RESOLVED NIT:{spec[1]}"
            if res.status == "RESOLVED" and res.identity and res.identity.base_number == spec[1]:
                outcome = "PASS"
            elif res.status == "RESOLVED":
                outcome, extra = "FAIL_SILENT", "resolvió a OTRA identidad."
            else:
                outcome = "FAIL_LOUD"
        elif kind == "loud":
            expected = f"existe NIT:{spec[1]}; exact lo declina"
            if res.status == "RESOLVED" and res.identity and res.identity.base_number == spec[1]:
                outcome = "PASS"
            elif res.status == "RESOLVED":
                outcome, extra = "FAIL_SILENT", "resolvió a OTRA identidad."
            else:
                outcome = "FAIL_LOUD"
                extra = (f"marca sin forma societaria: exact={res.status}; full={full.status} con {len(full.candidates)} "
                         "candidatos (sólo sugerencias, nunca autoridad).")
        elif kind == "not_cc":
            expected = "no resolver a una cédula (persona natural) para una entidad pública"
            if res.status == "RESOLVED" and res.identity and res.identity.document_type == "CC":
                outcome, extra = "FAIL_SILENT", "el directorio asocia el nombre de una ESP a una cédula de persona natural."
            else:
                outcome = "PASS"
        else:
            expected = "decline: no RESOLVED identity"
            if res.status == "RESOLVED":
                outcome = "FAIL_SILENT"
                extra = (f"RESOLVED {_id_str(res.identity)} method={res.method} (nombre genérico/topónimo/palabra común "
                         "con un único alias exacto en el directorio).")
            else:
                outcome = "DECLINE_OK"
        stats[outcome] += 1
        if outcome == "FAIL_SILENT" and category in {"generic_word", "toponym", "generic_legal_form"}:
            extra += " Requiere que el productor etiquete ese span como ACTOR (el directorio no lo impide)."
        rec(A, "authority", category, "ActorNameResolver.resolve_exact", name, expected, got, outcome, note=extra,
            severity=("MEDIUM" if category in {"generic_word", "toponym", "generic_legal_form"} else "HIGH")
            if outcome == "FAIL_SILENT" else None,
            producer_error=category in {"generic_word", "toponym", "generic_legal_form"})
    # ── documentos: normalize_identity es la única pieza pura sobre un span de documento ──
    dv = compute_check_digit("860066942")
    docs = [
        (f"860.066.942-{dv}", None, "document", "NIT:860066942 (validado por DV)", lambda m: m.key and m.key.base_number == "860066942" and m.key.document_type == "NIT" and "IDENTITY_CHECK_DIGIT_MISMATCH" not in m.reason_codes),
        ("860066942", None, "document", "UNKNOWN base 860066942 (sin DV: tipo indeterminado)", lambda m: m.key and m.key.document_type == "UNKNOWN"),
        ("860066942", "NIT", "document", "NIT declarado", lambda m: m.key and m.key.document_type == "NIT"),
        ("860.066.942-4", None, "document_wrong_dv", "NO debe quedar como NIT validado: el DV (4) es falso", lambda m: not (m.key and m.key.document_type == "NIT" and m.confidence == "inferred_from_check_digit")),
        ("NIT 860066942", None, "document_marker", "marcador ignorado por la función pura (el tipo lo declara quien llama)", lambda m: m.key and m.key.base_number == "860066942"),
        ("123", None, "document_range", "fuera de rango -> sin llave", lambda m: m.key is None),
        ("NO DEFINIDO", None, "document_sentinel", "centinela descartado", lambda m: m.key is None),
        ("3001234567", None, "document_shape", "forma de teléfono: NO debe ser una identidad plausible", lambda m: m.key is None),
        ("12345678", "CC", "document", "CC declarada", lambda m: m.key and m.key.document_type == "CC"),
    ]
    for raw, declared, category, expected, ok in docs:
        m = normalize_identity(raw, document_type=declared)
        got = {"key": _id_str(m.key) if m.key else None, "check_digit": m.check_digit, "confidence": m.confidence,
               "reason_codes": list(m.reason_codes)}
        good = bool(ok(m))
        if category == "document_marker":
            outcome = "INFO"
            extra = "normalize_identity no interpreta el marcador «NIT»: lo hace `QueryRouter._consume_actor_document` sobre texto crudo."
        elif good:
            outcome, extra = "PASS", ""
        elif category in {"document_wrong_dv", "document_shape"}:
            outcome = "FAIL_SILENT"
            extra = ("un DV declarado falso queda con confidence=inferred_from_check_digit (sólo el reason_code "
                     "IDENTITY_CHECK_DIGIT_MISMATCH lo delata)." if category == "document_wrong_dv" else
                     "`has_plausible_actor_document_digits` acepta 5-11 dígitos: un teléfono pasa como documento UNKNOWN.")
        else:
            outcome, extra = "FAIL_LOUD", ""
        stats[outcome] += 1
        rec(A, "authority", category, "normalize_identity", f"{raw!r} declared={declared!r}", expected, got, outcome,
            note=extra, severity="MEDIUM" if outcome == "FAIL_SILENT" else None)
    # ── el entry point nombrado en la misión: resolve_actor_params (legacy, parser-dependiente) ──
    qr = QueryRouter(entity_resolver())
    legacy = [
        ("contratos de Cafam", ("nit", "860013570")),
        ("contratos de Caja de Compensación Familiar Compensar", ("nit", "860066942")),
        ("contratos de Compensar", ("loud", "860066942")),
        ("contratos de codetec en 2026", ("loud", None)),
        ("contratos de Colsubsidio en 2025", ("loud", "860007336")),
        ("contratos de Seguros del Estado S.A. en 2025", ("nit", "860009578")),
        ("contratos de aseo", ("none", None)),
        ("proveedor Consorcio", ("decline", None)),
        ("contratos del NIT 860.066.942-7", ("doc", "860066942")),
        ("contratos de 860066942", ("doc", "860066942")),
    ]
    for raw, spec in legacy:
        parsed = dict(qr.parse(raw).params)
        out = resolve_actor_params(raw, parsed, ar)
        scope = (out or {}).get("actor_scope") or {}
        got = {"changed_params": out is not None, "actor_resolution": (out or {}).get("actor_resolution"),
               "actor_resolution_status": (out or {}).get("actor_resolution_status"),
               "document_type": scope.get("document_type"), "base_number": scope.get("base_number"),
               "contratista_filter": bool((out or {}).get("contratista")),
               "parser_objeto_if_unchanged": parsed.get("objeto") if out is None else None}
        kind, base = spec
        extra = ""
        resolved_base = scope.get("base_number")
        if kind in {"nit", "doc"}:
            expected = f"actor identity {base}"
            outcome = "PASS" if resolved_base == base else "FAIL_LOUD"
        elif kind == "loud":
            expected = "actor existe en el directorio sin forma societaria exacta" if base else "actor sin marcador"
            outcome = "PASS" if (base and resolved_base == base) else "FAIL_LOUD"
            extra = "queda como TEMA (`objeto`) en el parse: punto ciego declarado en el roadmap §deudas."
        elif kind == "decline":
            expected = "no resolver un nombre genérico a una identidad"
            outcome = "FAIL_SILENT" if resolved_base else "DECLINE_OK"
            extra = "«Consorcio» resuelve exact_unique_alias a un NIT concreto." if resolved_base else ""
        else:
            expected = "sin cambios (tema, no actor)"
            outcome = "PASS" if out is None else "FAIL_SILENT"
        stats[outcome] += 1
        rec(A, "authority", "legacy_entrypoint", "resolve_actor_params(raw_query, QueryRouter.parse, resolver)", raw,
            expected, got, outcome, note=extra, severity="HIGH" if outcome == "FAIL_SILENT" else None)
    # ── host: ACTOR no está hospedado ──
    for quote in ("Cafam", "Caja de Compensación Familiar Compensar"):
        chain = host_chain(D.ACTOR, "is", POS, quote)
        rec(A, "host_chain", "host_support", "admit_proposal", quote, "el host resuelve la dimensión ACTOR", chain, "GAP",
            note="foundation_admission.py no tiene rama ACTOR (UNRESOLVED_HOST_DIMENSION:ACTOR) y foundation_projection.py "
                 "no emite `contratista`.")
    # ── barridos agregados: contaminación del directorio ──
    geo_names = sorted({v for v in DEPARTAMENTOS.values()})
    muni = get_municipal_territories()
    muni_names = sorted({t.municipality for t in muni.by_code.values()})
    topic_words = ["aseo", "vigilancia", "alimentación", "pavimentación", "computadores", "software", "obra",
                   "interventoría", "tecnología", "mantenimiento", "transporte", "suministro", "papelería",
                   "construcción", "dotación", "salud", "educación", "cafetería", "hospital", "consorcio",
                   "consorcios", "contratos", "servicios", "ingeniería", "seguridad", "limpieza", "asesoría"]

    def sweep(words):
        c, cc, ex = Counter(), 0, []
        for w in words:
            r = ar.resolve_exact(w)
            c[r.status] += 1
            if r.status == "RESOLVED":
                if r.identity.document_type == "CC":
                    cc += 1
                elif len(ex) < 6:
                    ex.append({"word": w, "identity": _id_str(r.identity)})
        return {"n": len(words), "statuses": dict(c), "resolved_to_natural_person_cc": cc, "resolved_to_nit_examples": ex}

    agg = {"departments": sweep(geo_names), "municipalities_divipola": sweep(muni_names),
           "common_topic_words": sweep(topic_words)}
    resolved = (agg["departments"]["statuses"].get("RESOLVED", 0) + agg["municipalities_divipola"]["statuses"].get("RESOLVED", 0)
                + agg["common_topic_words"]["statuses"].get("RESOLVED", 0))
    rec(A, "aggregate", "directory_pollution", "ActorNameResolver.resolve_exact (barrido)",
        "departamentos, municipios DIVIPOLA y palabras temáticas comunes", "ninguno resuelve a una identidad", agg,
        "FAIL_SILENT" if resolved else "PASS",
        note=(f"{resolved} topónimos/palabras comunes resuelven RESOLVED a una identidad concreta (exact_unique_alias); "
              "incluidos alias de UNA sola observación (`minimum_total_occurrences=1` en build_policy). Requiere que el "
              "productor etiquete el span como ACTOR."),
        severity="MEDIUM", producer_error=True)
    rec(A, "aggregate", "environment", "ActorNameResolver(index_path)", str(ACTOR_INDEX),
        "índice resuelto por ruta absoluta", {"default_relative_path": "data/actor_alias_index.sqlite",
                                              "index_bytes": ACTOR_INDEX.stat().st_size},
        "INFO", note="el default de Settings es relativo (`data/…`): en un worktree sin data/ el resolver cae a "
                     "INDEX_UNAVAILABLE sin fallback; pasar la ruta absoluta (SECOP_ACTOR_ALIAS_INDEX_PATH).")
    return stats


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# GEOGRAPHY
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _region(token: str):
    key = normalize_text(token)
    name = REGION_ALIASES.get(key)
    return {"region": name, "departments": ZONES_BY_NAME.get(name) if name else None}


def probe_geography():
    A = "GEOGRAPHY"
    muni = get_municipal_territories()
    dept_ok = [("Antioquia", "Antioquia"), ("Atlántico", "Atlántico"), ("Huila", "Huila"),
               ("Bogotá", "Distrito Capital de Bogotá"), ("Bogotá D.C.", "Distrito Capital de Bogotá"),
               ("Valle del Cauca", "Valle del Cauca"), ("Norte de Santander", "Norte de Santander"),
               ("Santander", "Santander"), ("San Andrés y Providencia", "San Andrés, Providencia y Santa Catalina"),
               ("Guajira", "La Guajira"), ("atlantico", "Atlántico"), ("departamento de Córdoba", None)]
    for text, want in dept_ok:
        name = re.sub(r"^departamento\s+de\s+", "", normalize_text(text))  # réplica de foundation_admission.py:107
        for_got = department_mention_resolution(name)
        got = {"kind": for_got.kind, "canonical": for_got.canonical, "confidence": for_got.confidence}
        outcome = "PASS" if for_got.canonical == (want or "Córdoba") else "FAIL_LOUD"
        rec(A, "authority", "department", "department_mention_resolution", text, want or "Córdoba", got, outcome)
    for text in ("el Valle", "Urabá", "Magdalena Medio", "Santa Fe"):
        r = department_mention_resolution(text)
        rec(A, "authority", "department_decline", "department_mention_resolution", text, "decline (no es un departamento)",
            {"canonical": r.canonical}, "DECLINE_OK" if r.canonical is None else "FAIL_SILENT")
    # homónimos departamento / municipio: la autoridad es correcta SÓLO si el nivel viene tipado
    for text in ("Córdoba", "Nariño", "Boyacá", "San Andrés"):
        d = department_mention_resolution(text)
        m = muni.lookup(text)
        rec(A, "authority", "homonym_dept_vs_municipality", "department_mention_resolution + municipal.lookup", text,
            "el nivel (DEPARTMENT/MUNICIPALITY) debe llegar tipado; sin él, ambiguo",
            {"department": d.canonical, "municipalities": [(t.divipola, t.department_value) for t in m]},
            "INFO",
            note="`ProposedCondition` no tiene campo `level` (interpretation_contract.py:196): el host asume DEPARTAMENTO "
                 "(foundation_admission.py:101-104) aun cuando existe homónimo municipal.")
    # municipios (DIVIPOLA)
    uniq = [("Barranquilla", "08001"), ("Soledad", "08758"), ("Medellín", "05001"), ("Bucaramanga", "68001"),
            ("Villavicencio", "50001"), ("Santa Marta", "47001"), ("Pasto", "52001"), ("Yopal", "85001"), ("Chía", "25175"),
            ("Palmira", "76520")]
    for text, code in uniq:
        found = muni.lookup(text)
        outcome = "PASS" if len(found) == 1 and found[0].divipola == code else "FAIL_LOUD"
        rec(A, "authority", "municipality", "MunicipalTerritoryRegistry.lookup", text, f"único DIVIPOLA {code}",
            [(t.divipola, t.department_value) for t in found], outcome)
    for text in ("San Luis", "Concepción", "San Francisco", "La Unión", "Albán", "Rionegro", "Armenia", "Córdoba"):
        found = muni.lookup(text)
        rec(A, "authority", "municipal_homonym", "MunicipalTerritoryRegistry.lookup", text,
            "varios candidatos -> hay que desambiguar por departamento",
            [(t.divipola, t.department_value) for t in found],
            "PASS" if len(found) > 1 else "FAIL_SILENT",
            note="homónimos detectados correctamente (la autoridad municipal no elige)." if len(found) > 1 else "")
    disamb = muni.lookup("San Luis", "Tolima")
    rec(A, "authority", "municipal_homonym", "MunicipalTerritoryRegistry.lookup(name, department)", "San Luis, Tolima",
        "único 73678", [(t.divipola, t.department_value) for t in disamb],
        "PASS" if len(disamb) == 1 and disamb[0].divipola == "73678" else "FAIL_LOUD")
    for text, want in (("Cali", "76001"), ("Cartagena", "13001"), ("Tumaco", "52835"), ("Bogotá", "11001")):
        found = muni.lookup(text)
        gaz = geo_mention_resolution(text)
        rec(A, "authority", "municipal_colloquial_name", "MunicipalTerritoryRegistry.lookup", text,
            f"DIVIPOLA {want} por el nombre coloquial", {"lookup": [(t.divipola, t.department_value) for t in found],
                                                          "gazetteer_kind": gaz.kind, "gazetteer_canonical": gaz.canonical},
            "PASS" if len(found) == 1 and found[0].divipola == want else "FAIL_LOUD",
            note="el registro DIVIPOLA sólo indexa el nombre oficial (`aliases == (key(municipality),)` en "
                 "municipal_territory.py:57): «Cali»/«Cartagena»/«Tumaco» no resuelven; el gazetteer legacy sí los reconoce.")
    for text in ("Colombia",):
        found = muni.lookup(text)
        rec(A, "authority", "wrong_owner_toponym", "MunicipalTerritoryRegistry.lookup", text,
            "el país no es un municipio", [(t.divipola, t.department_value) for t in found],
            "FAIL_SILENT" if found else "PASS", severity="MEDIUM",
            note="existe el municipio «Colombia» (Huila): `consume('contratos en colombia')` lo resuelve status=resolved "
                 "(ciudad=Colombia, departamento=Huila).")
    for text in ("Armenia", "San Andrés", "Bogotá"):
        g = geo_mention_resolution(text)
        rec(A, "authority", "legacy_gazetteer_homonym", "geo_mention_resolution", text,
            "no devolver high-confidence para un homónimo", {"kind": g.kind, "canonical": g.canonical, "confidence": g.confidence},
            "FAIL_SILENT" if text in {"Armenia", "San Andrés"} and g.confidence == "high" else "INFO",
            severity="MEDIUM",
            note="GAZETTEER_CIUDADES es una segunda autoridad municipal con confianza alta y sin desambiguación." if g.confidence == "high" else "")
    # regiones
    for text, want in (("Caribe", "Caribe"), ("región caribe", "Caribe"), ("costa atlántica", "Caribe"), ("Eje Cafetero", "Eje Cafetero"),
                       ("Orinoquia", "Orinoquia"), ("los llanos", "Orinoquia"), ("Amazonia", "Amazonia"), ("Santanderes", "Santanderes")):
        r = _region(text)
        rec(A, "authority", "region", "REGION_ALIASES/ZONES_BY_NAME (datos, sin función)", text, f"región {want}", r,
            "PASS" if r["region"] == want else "FAIL_LOUD",
            note="no existe función pública `resolve_region`: son dicts de datos que el router lee con `scan_region`.")
    for text in ("Urabá", "Magdalena Medio", "la costa"):
        r = _region(text)
        rec(A, "authority", "region_decline", "REGION_ALIASES", text, "decline/aclarar (sub-departamental o ambiguo)", r,
            "DECLINE_OK" if r["region"] is None else "INFO",
            note="alias ambiguo: «la costa» se fija a Caribe (también puede ser Pacífica)." if r["region"] else "")
    rec(A, "authority", "homonym_region_dept", "REGION_ALIASES vs DEPARTAMENTOS", "Amazonas / Amazonia",
        "departamento y región no se confunden",
        {"amazonas_dept": department_mention_resolution("amazonas").canonical, "amazonas_region": _region("amazonas")["region"],
         "amazonia_region": _region("amazonia")["region"]},
        "PASS" if department_mention_resolution("amazonas").canonical == "Amazonas" and _region("amazonas")["region"] is None else "FAIL_SILENT")
    # catálogo: homónimos municipio/departamento y entre municipios
    by_name: dict[str, list] = {}
    for t in muni.by_code.values():
        by_name.setdefault(t.aliases[0], []).append(t)
    dept_keys = {normalize_text(v) for v in DEPARTAMENTOS.values()}
    rec(A, "aggregate", "catalog_shape", "get_municipal_territories()", "1103 municipios DIVIPOLA", "inventario de homonimia",
        {"municipalities": len(muni.by_code), "names_with_more_than_one_municipality": sum(1 for v in by_name.values() if len(v) > 1),
         "municipalities_in_homonym_groups": sum(len(v) for v in by_name.values() if len(v) > 1),
         "municipality_names_equal_to_a_department_name": sorted(k for k in by_name if k in {re.sub(r'\W+', ' ', d).strip() for d in dept_keys})},
        "INFO")
    # host
    host = [("Antioquia", "READY"), ("Norte de Santander", "READY"), ("Córdoba", "READY"), ("Bogotá", "READY"),
            ("Barranquilla", "NEEDS_CLARIFICATION"), ("Soledad", "NEEDS_CLARIFICATION"), ("Caribe", "NEEDS_CLARIFICATION"),
            ("Eje Cafetero", "NEEDS_CLARIFICATION"), ("municipio de Córdoba", "NEEDS_CLARIFICATION"),
            ("San Luis, Tolima", "NEEDS_CLARIFICATION")]
    for quote, _ in host:
        chain = host_chain(D.GEOGRAPHY, "is", POS, quote)
        value = hc_value(chain)
        if value:
            outcome, extra = ("INFO", "el host asume DEPARTAMENTO para cualquier GEOGRAPHY (sin `level`).") if quote in {"Córdoba", "Bogotá"} else ("PASS", "")
        else:
            outcome, extra = "GAP", "el host sólo hospeda departamentos (foundation_admission.py:101-104; projection `departamento_resolved`); municipio/región no tienen camino de admisión/proyección."
        rec(A, "host_chain", "host_support", "admit_proposal+project_accepted", quote, "READY sólo para departamentos",
            chain, outcome, note=extra)


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# TIME
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _t(text, today=TODAY):
    r = resolve_temporal_scope(text, today=today)
    return {"matched": r.matched, "desde": r.fecha_desde, "hasta": r.fecha_hasta, "precision": r.precision,
            "error_code": r.error_code, "phrase": r.phrase}


def probe_time():
    A = "TIME"
    Y = TODAY.year
    # (quote, esperado, categoría, nota)
    #   ("range", desde, hasta) | ("decline",) | ("resolvable",)  [hay que resolver pero el rango exacto es política]
    table = [
        ("2025", ("range", "2025-01-01", "2025-12-31"), "year", ""),
        ("en 2025", ("range", "2025-01-01", "2025-12-31"), "year", ""),
        ("durante 2024", ("range", "2024-01-01", "2024-12-31"), "year", ""),
        ("marzo de 2025", ("range", "2025-03-01", "2025-03-31"), "month", ""),
        ("sep 2025", ("range", "2025-09-01", "2025-09-30"), "month", ""),
        ("septiembre del 2025", ("range", "2025-09-01", "2025-09-30"), "month_year_connector",
         "«del 2025» (variante frecuente) pierde el año y usa el año civil actual."),
        ("en marzo", ("range", f"{Y}-03-01", f"{Y}-03-31"), "month_without_year", "política ADR-065: mes sin año = año civil actual"),
        ("desde 2023", ("range", "2023-01-01", None), "open_bound", ""),
        ("hasta 2024", ("range", None, "2024-12-31"), "open_bound", ""),
        ("desde marzo de 2025", ("range", "2025-03-01", None), "open_bound", ""),
        ("hasta marzo", ("range", None, f"{Y}-03-31"), "open_bound", ""),
        ("entre enero y marzo de 2025", ("range", "2025-01-01", "2025-03-31"), "range", ""),
        ("entre 2023 y 2025", ("range", "2023-01-01", "2025-12-31"), "range", ""),
        ("2023-2025", ("range", "2023-01-01", "2025-12-31"), "range", ""),
        ("enero 2024 a diciembre 2024", ("range", "2024-01-01", "2024-12-31"), "range", ""),
        ("del 1 al 15 de marzo de 2025", ("range", "2025-03-01", "2025-03-15"), "day_range",
         "el rango de días con mes único se lee como el día 15 (exact_day_re gana a day_range_re)."),
        ("entre el 1 de marzo y el 30 de junio", ("decline",), "range_without_year", "pide año (missing_range_year)"),
        ("30 de febrero de 2025", ("decline",), "invalid_date", ""),
        ("diciembre a marzo", ("decline",), "reverse_range", ""),
        ("este mes", ("range", "2026-09-01", "2026-09-30"), "relative", ""),
        ("el mes pasado", ("range", "2026-08-01", "2026-08-31"), "relative", ""),
        ("este año", ("range", "2026-01-01", "2026-12-31"), "relative", ""),
        ("el año pasado", ("range", "2025-01-01", "2025-12-31"), "relative", ""),
        ("esta semana", ("range", "2026-09-28", "2026-10-04"), "relative", ""),
        ("la semana pasada", ("range", "2026-09-21", "2026-09-27"), "relative", ""),
        ("hace 3 meses", ("range", "2026-06-01", "2026-06-30"), "relative", ""),
        ("últimos 30 días", ("range", "2026-08-31", "2026-09-30"), "relative", ""),
        ("hoy", ("range", "2026-09-30", "2026-09-30"), "relative", ""),
        ("ayer", ("range", "2026-09-29", "2026-09-29"), "relative", ""),
        ("últimos 6 meses", ("resolvable",), "relative_missing", "sin patrón: matched=False (el host debe tratarlo como no resuelto)"),
        ("los últimos 3 años", ("resolvable",), "relative_missing", ""),
        ("hace 2 años", ("resolvable",), "relative_missing", ""),
        ("la última semana", ("resolvable",), "relative_missing", ""),
        ("después de 2022", ("range", "2023-01-01", None), "operator_word",
         "la autoridad lee el span, no el comparador: «de 2022» casa closed_year y devuelve el año 2022 completo."),
        ("antes de 2024", ("range", None, "2023-12-31"), "operator_word", ""),
        ("a partir de 2023", ("range", "2023-01-01", None), "operator_word", ""),
        ("posteriores a 2022", ("range", "2023-01-01", None), "operator_word", ""),
        ("de 2024 a la fecha", ("range", "2024-01-01", None), "operator_word", ""),
        ("desde el año pasado", ("range", "2025-01-01", None), "operator_word", ""),
        ("antes de marzo", ("range", None, f"{Y}-02-28"), "operator_word", ""),
        ("primer trimestre de 2025", ("range", "2025-01-01", "2025-03-31"), "quarter",
         "el rama `unresolved` de trimestre/semestre es inalcanzable cuando hay año (closed_year gana)."),
        ("segundo semestre de 2025", ("range", "2025-07-01", "2025-12-31"), "quarter", ""),
        ("último año", ("decline",), "unsupported", ""),
        ("2025 millones", ("decline",), "money_collision", "protección contra monto"),
        ("1999", ("decline",), "out_of_window", "fuera de 20xx: no matchea (loud)"),
        ("dos mil veinticinco", ("decline",), "verbal_year", ""),
        ("próximo mes", ("decline",), "future_relative", ""),
    ]
    for quote, spec, category, note in table:
        got = _t(quote)
        kind = spec[0]
        resolved = got["matched"] and not got["error_code"]
        if kind == "range":
            expected = f"{spec[1]} .. {spec[2]}"
            if resolved and (got["desde"], got["hasta"]) == (spec[1], spec[2]):
                outcome = "PASS"
            elif resolved:
                outcome = "FAIL_SILENT"
            else:
                outcome = "FAIL_LOUD"
        elif kind == "resolvable":
            expected = "debe resolverse a un rango"
            outcome = "PASS" if resolved else "FAIL_LOUD"
        else:
            expected = "decline"
            outcome = "DECLINE_OK" if not resolved else "FAIL_SILENT"
        rec(A, "authority", category, "resolve_temporal_scope(quote, today=2026-09-30)", quote, expected, got, outcome, note=note)
    # reloj inyectado: aritmética de calendario sin tocar el reloj del sistema
    clocks = [
        ("este mes", date(2026, 1, 15), ("2026-01-01", "2026-01-31")),
        ("el mes pasado", date(2026, 1, 15), ("2025-12-01", "2025-12-31")),
        ("hace 3 meses", date(2026, 2, 10), ("2025-11-01", "2025-11-30")),
        ("este mes", date(2024, 2, 29), ("2024-02-01", "2024-02-29")),
        ("esta semana", date(2026, 3, 1), ("2026-02-23", "2026-03-01")),
        ("últimos 7 días", date(2026, 3, 3), ("2026-02-24", "2026-03-03")),
    ]
    for quote, today, want in clocks:
        got = _t(quote, today)
        rec(A, "authority", "injected_clock", f"resolve_temporal_scope(today={today})", quote, f"{want[0]} .. {want[1]}", got,
            "PASS" if (got["desde"], got["hasta"]) == want else "FAIL_SILENT")
    probe = resolve_temporal_scope("este mes")  # sin `today`
    rec(A, "authority", "clock_default", "resolve_temporal_scope(quote) sin today", "este mes",
        "el host siempre inyecta el reloj", {"default_uses_system_clock": True, "civil_today": civil_today().isoformat(),
                                             "this_month_start": probe.fecha_desde},
        "INFO", note="`civil_today(None)` lee `date.today()` (temporal_semantics.py:73): no determinista si el host olvida inyectar.")
    # host chain
    hc = [
        (D.TIME, "during_year", POS, "en 2025", "PASS"),
        (D.TIME, "during_year", POS, "antes de 2024", None),
        (D.TIME, "during_year", POS, "después de 2022", None),
        (D.TIME, "during_year", POS, "desde 2023", None),
        (D.TIME, "during_year", POS, "primer trimestre de 2025", None),
        (D.TIME, "during_year", POS, "en 2025 y 2026", "DECLINE_OK"),
        (D.TIME, "during_month", POS, "en marzo de 2025", "GAP"),
        (D.TIME, "between_dates", POS, "entre 2023 y 2025", "GAP"),
        (D.TIME, "between_dates", POS, "desde 2023", "GAP"),
        (D.TIME, "relative_period", POS, "este mes", "GAP"),
        (D.TIME, "during_year", NEG, "en 2024", "GAP"),
    ]
    for dim, op, pol, quote, want in hc:
        chain = host_chain(dim, op, pol, quote)
        value = hc_value(chain)
        if want == "PASS":
            outcome, note = ("PASS" if value == ["2025"] else "FAIL_SILENT"), ""
        elif want == "DECLINE_OK":
            outcome, note = ("DECLINE_OK" if not value else "FAIL_SILENT"), ""
        elif want == "GAP":
            outcome, note = ("GAP" if not (chain.get("projected")) else "INFO"), (
                "el host sólo admite `during_year` (foundation_admission.py:109-111: una regex de 4 dígitos) y la proyección sólo "
                "emite fecha_desde/fecha_hasta_exclusive para ese operador.")
        else:  # el operador `during_year` con un comparador distinto: ejecuta el año citado
            outcome = "FAIL_SILENT" if chain.get("projected") else "DECLINE_OK"
            note = ("el host acepta CUALQUIER cita con exactamente un año para `during_year` (no verifica el operador contra "
                    "la cita, a diferencia de VALUE/operator_reason): ejecuta el año completo citado.")
        if outcome == "FAIL_SILENT":
            note += (" El prompt v4.2 (regla Y1) manda citar SÓLO el año: el comparador («antes de», «desde») no es evidencia "
                     "citable, así que el host no puede ni verlo; con el vocabulario actual no hay forma de expresarlo.")
        rec(A, "host_chain", "host_support" if want in {"GAP", "PASS", "DECLINE_OK"} else "operator_vs_evidence",
            "admit_proposal+project_accepted", f"{op}/{pol.value}: {quote}", want or "el host no debe ejecutar un año que la cita no significa",
            chain, outcome, note=note, producer_error=False)


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# MONEY
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
def probe_money():
    A = "MONEY"
    values = [
        ("500 millones", 500_000_000), ("500 millones de pesos", 500_000_000), ("1 millón", 1_000_000),
        ("un millón", 1_000_000), ("mil millones", 1_000_000_000), ("1.5 mil millones", 1_500_000_000),
        ("2 mil millones", 2_000_000_000), ("tres mil millones", 3_000_000_000), ("1,5 millones", 1_500_000),
        ("$500.000.000", 500_000_000), ("500,000,000", 500_000_000), ("200 palos", 200_000_000),
        ("cien millones", 100_000_000), ("quinientos millones", 500_000_000), ("mil quinientos millones", 1_500_000_000),
        ("2 billones", 2_000_000_000_000), ("500 mil pesos", 500_000), ("10 millones de pesos", 10_000_000),
        ("100 SMMLV", None), ("50 salarios mínimos", None), ("un millón y medio", None), ("medio millón", None),
        ("entre 100 y 500 millones", None), ("100 millones y 200 millones", None), ("-5 millones", None),
    ]
    for span, want in values:
        got_v = money_to_cop(span)
        got = {"cop": got_v, "is_admissible_amount": is_admissible_amount(got_v, span)}
        if want is not None:
            expected, outcome = str(want), ("PASS" if got_v == want else ("FAIL_LOUD" if got_v is None else "FAIL_SILENT"))
            extra = ""
        else:
            expected = "decline (None)"
            outcome = "DECLINE_OK" if got_v is None else "GATED"
            extra = ""
            if got_v is not None:
                extra = ("money_to_cop devuelve un valor para un span que no es un monto simple "
                         "(toma el primer operando / descarta el signo); el host lo contiene sólo porque exige un "
                         "comparador compatible (`operator_reason`) antes de convertir.")
        rec(A, "authority", "amount", "money_to_cop", span, expected, got, outcome, note=extra)
    # operador vs. evidencia (foundation_value_direction.operator_reason)
    ops = [
        ("greater_than", "más de 300 millones", "ok"), ("less_than", "menos de 300 millones", "ok"),
        ("greater_than", "superior a 300 millones", "ok"), ("less_than", "inferior a 300 millones", "ok"),
        ("greater_than", "por encima de 300 millones", "ok"), ("less_than", "por debajo de 300 millones", "ok"),
        ("greater_than", "mayores a 1 millón", "ok"),
        ("greater_than", "menos de 300 millones", "clarify"), ("less_than", "más de 300 millones", "clarify"),
        ("greater_than", "entre 100 y 500 millones", "clarify"), ("greater_than", "300 millones", "clarify"),
        ("greater_than", "más de 300 millones o menos de 100", "clarify"),
        ("greater_than", "no menos de 300 millones", "clarify"), ("less_than", "no más de 300 millones", "clarify"),
        ("greater_than", "que no superen 300 millones", "clarify"), ("greater_than", "no mayor a 300 millones", "clarify"),
        ("greater_than", "que no excedan 300 millones", "clarify"), ("greater_than", "sin superar 300 millones", "clarify"),
        ("greater_than", "no sobrepasen 300 millones", "clarify"),
        ("greater_than_or_equal", "menos de 300 millones", "clarify"), ("less_than_or_equal", "más de 300 millones", "clarify"),
        ("greater_than_or_equal", "al menos 300 millones", "ok"), ("less_than_or_equal", "hasta 300 millones", "ok"),
    ]
    for op, text, want in ops:
        reason = operator_reason(op, text)
        got = {"reason": reason, "directions": list(directions(text))}
        latent = op in {"greater_than_or_equal", "less_than_or_equal"}
        if want == "ok":
            outcome = "PASS" if reason is None else "FAIL_LOUD"
        else:
            outcome = "PASS" if reason is not None else "FAIL_SILENT"
        extra = ""
        if outcome == "FAIL_SILENT":
            if latent:
                extra = ("`operator_reason` devuelve None sin leer la cita para cualquier operador fuera de {greater_than, "
                         "less_than} (foundation_value_direction.py:58-59): al activar los inclusivos (D4) la guarda queda ciega.")
            else:
                extra = ("la guarda de dirección no lee la negación del comparador: el operador contradice la cita y "
                         "se acepta (el vocabulario `_MARKERS` sólo conoce «no más de/no menos de»).")
        rec(A, "authority", "operator_vs_evidence", "operator_reason", f"{op}: {text}", "accept" if want == "ok" else "clarify",
            got, outcome, note=extra, reachable=not latent, severity="HIGH" if outcome == "FAIL_SILENT" else None,
            producer_error=True)
    # vocabularios divergentes: money_polarity (léxico del router) vs value_direction (guarda del host)
    for text in ("mínimo 300 millones", "máximo 2 mil millones", "que superen 300 millones", "excedan 300 millones", "rebasen 300 millones"):
        pol = resolve_money_polarity(text)
        rec(A, "authority", "vocabulary_divergence", "money_polarity vs foundation_value_direction", text,
            "los dos vocabularios coinciden",
            {"money_polarity": [d.relation.value for d in pol], "value_direction": list(directions(text))},
            "INFO" if (bool(pol) == bool(directions(text))) else "FAIL_LOUD",
            note="dos autoridades léxicas para el mismo comparador" if bool(pol) != bool(directions(text)) else "")
    # host
    hc = [
        (D.VALUE, "greater_than", POS, "más de 300 millones", "valor_del_contrato > 300000000"),
        (D.VALUE, "less_than", POS, "menos de 300 millones", "valor_del_contrato < 300000000"),
        (D.VALUE, "greater_than", POS, "más de 5", "bare"),
        (D.VALUE, "greater_than", POS, "más de 500 mil", "below_floor"),
        (D.VALUE, "greater_than", POS, "que no superen 300 millones", "inversion"),
        (D.VALUE, "greater_than", POS, "no mayor a 300 millones", "inversion"),
        (D.VALUE, "greater_than", POS, "menos de 300 millones", "mismatch"),
        (D.VALUE, "greater_than_or_equal", POS, "al menos 300 millones", "inclusive"),
        (D.VALUE, "less_than_or_equal", POS, "hasta 300 millones", "inclusive"),
        (D.VALUE, "greater_than", POS, "más de 100 SMMLV", "smmlv"),
        (D.VALUE, "greater_than", POS, "más de un millón y medio", "fraction"),
    ]
    for dim, op, pol, quote, kind in hc:
        chain = host_chain(dim, op, pol, quote)
        value, executed = hc_value(chain), bool(chain.get("projected"))
        extra = ""
        if kind in {"valor_del_contrato > 300000000", "valor_del_contrato < 300000000"}:
            ok = executed and kind.split()[-1] in chain.get("where_fragment", "")
            outcome = "PASS" if ok else "FAIL_SILENT"
        elif kind == "bare":
            outcome = "FAIL_SILENT" if executed else "DECLINE_OK"
            extra = ("el host admite cualquier cifra >= 0 sin unidad (foundation_admission.py:117-118 `amount >= 0`); el piso "
                     "anti-ruido `is_admissible_amount` (money_admission.py:75) existe y no se usa.")
        elif kind == "below_floor":
            outcome = "INFO"
            extra = ("cifra sin unidad bajo el piso anti-ruido del legacy (1.000.000): Foundation la ejecuta tal cual "
                     "(valor_del_contrato > 500000); plausible como pesos, pero es una diferencia deliberada a decidir.")
        elif kind == "inversion":
            outcome = "FAIL_SILENT" if executed else "DECLINE_OK"
            extra = ("el host ejecuta el valor con la dirección contraria a la cita (cota inferior en vez de superior); requiere "
                     "que el productor etiquete la negación como greater_than.")
        elif kind == "mismatch":
            outcome = "DECLINE_OK" if not executed else "FAIL_SILENT"
        elif kind == "inclusive":
            outcome, extra = "GAP", ("el host no proyecta inclusivos (`foundation_projection.py` sólo `valor_{min,max}_exclusive`) "
                                     "aunque la decisión D4 del contrato M1.0 manda proyectarlos como >=/<=.")
        else:
            outcome = "DECLINE_OK" if not executed else "FAIL_SILENT"
        rec(A, "host_chain", "host_support" if kind not in {"bare", "inversion", "mismatch"} else "operator_vs_evidence",
            "admit_proposal+project_accepted", f"{op}: {quote}", "ver categoría", chain, outcome, note=extra,
            severity="MEDIUM", producer_error=True)


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# MODALITY
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _mod(text, dataset="procesos"):
    r = resolve_modality(text, dataset)
    return {"status": r.status.value,
            "includes": [{"id": m.target_id, "strength": m.strength.value, "confidence": m.confidence.value,
                          "family": m.retrieval_family, "procedure_ids": list(m.procedure_ids)} for m in r.includes],
            "excludes": [m.target_id for m in r.excludes], "issues": [i.code for i in r.issues],
            "assumption_codes": list(r.assumption_codes), "clarification_code": r.clarification_code}


def probe_modality():
    A = "MODALITY"
    # (texto, esperado, categoría) esperado: ("include", {ids}) | ("exclude", {ids}) | ("none",) | ("colloquial",) | ("decline",)
    table = [
        ("contratación directa", ("include", {"contratacion_directa"}), "positive"),
        ("licitación pública", ("include", {"licitacion_publica"}), "positive"),
        ("selección abreviada", ("include", {"seleccion_abreviada"}), "positive"),
        ("selección abreviada de menor cuantía", ("include", {"seleccion_abreviada_menor_cuantia"}), "positive"),
        ("concurso de méritos", ("include", {"concurso_de_meritos"}), "positive"),
        ("mínima cuantía", ("include", {"minima_cuantia"}), "positive"),
        ("menor cuantía", ("include", {"seleccion_abreviada_menor_cuantia"}), "positive"),
        ("régimen especial", ("include", {"regimen_especial"}), "positive"),
        ("subasta inversa", ("include", {"seleccion_abreviada_subasta_inversa"}), "positive"),
        ("acuerdo marco de precios", ("include", {"acuerdo_marco"}), "positive"),
        ("subasta", ("decline",), "ambiguous"),
        ("no por contratación directa", ("exclude", {"contratacion_directa"}), "negation"),
        ("sin licitación pública", ("exclude", {"licitacion_publica"}), "negation"),
        ("excepto licitación pública", ("exclude", {"licitacion_publica"}), "negation"),
        ("que no sean de contratación directa", ("exclude", {"contratacion_directa"}), "negation"),
        ("licitación", ("colloquial",), "O6_colloquial"),
        ("licitaciones", ("colloquial",), "O6_colloquial"),
        ("convenio", ("none",), "not_a_modality"),
        ("convocatoria pública", ("none",), "not_a_modality"),
        ("contratos directos", ("include", {"contratacion_directa"}), "colloquial_synonym"),
        ("a dedo", ("decline",), "slang"),
        ("directa o licitación", ("decline",), "or_composition"),
        ("licitación o contratación directa", ("decline",), "or_composition"),
    ]
    for text, spec, category in table:
        got = _mod(text)
        inc = {m["id"] for m in got["includes"]}
        exc = set(got["excludes"])
        kind = spec[0]
        extra = ""
        if kind == "include":
            expected = f"include {sorted(spec[1])}"
            outcome = "PASS" if inc == spec[1] and not exc else ("FAIL_LOUD" if not inc and not exc else "FAIL_SILENT")
        elif kind == "exclude":
            expected = f"exclude {sorted(spec[1])}"
            outcome = "PASS" if exc == spec[1] and not inc else ("FAIL_LOUD" if not inc and not exc else "FAIL_SILENT")
        elif kind == "none":
            expected = "no_match (no es modalidad)"
            outcome = "PASS" if not inc and not exc else "FAIL_SILENT"
        elif kind == "colloquial":
            expected = "O6: coloquial NO es modalidad -> el resolver debe marcarlo (assumption/strength=colloquial)"
            flagged = got["status"] == "resolved_with_assumption" and all(m["strength"] == "colloquial" for m in got["includes"])
            outcome = "PASS" if (flagged or (not inc and not exc)) else "FAIL_SILENT"
            extra = "la autoridad lo señala (resolved_with_assumption + strength=colloquial); la decisión de no ejecutarlo es del host." if flagged else ""
        else:
            expected = "decline / ambiguous / sin conversión parcial"
            partial = bool(inc or exc) and category == "or_composition" and len(inc) < 2
            outcome = "FAIL_SILENT" if partial else ("DECLINE_OK" if (not inc and not exc) or got["status"] in {"ambiguous"} or len(inc) > 1 else "FAIL_SILENT")
            if partial:
                extra = "OR con un miembro no reconocido: el resolver devuelve sólo el otro miembro, sin issue (pérdida silenciosa de una rama)."
            elif len(inc) > 1:
                extra = "devuelve ambos miembros (OR no ejecutable hoy: el host lo rechaza)."
        rec(A, "authority", category, "modality_resolver.resolve(text, dataset='procesos')", text, expected, got, outcome,
            note=extra, producer_error=True, severity="MEDIUM")
    # el dataset no cambia la identidad lingüística
    for text in ("contratación directa", "licitación pública", "mínima cuantía"):
        a, b = _mod(text, "procesos"), _mod(text, "contratos")
        rec(A, "authority", "dataset_consistency", "resolve(text, procesos|contratos)", text, "mismo target en ambos datasets",
            {"procesos": [m["id"] for m in a["includes"]], "contratos": [m["id"] for m in b["includes"]]},
            "PASS" if [m["id"] for m in a["includes"]] == [m["id"] for m in b["includes"]] else "FAIL_LOUD")
    # host
    hc = [
        (D.MODALITY, "family", POS, "contratación directa", "positive"),
        (D.MODALITY, "family", POS, "licitación pública", "positive"),
        (D.MODALITY, "family", POS, "licitaciones", "O6"),
        (D.MODALITY, "family", POS, "licitación", "O6"),
        (D.MODALITY, "family", POS, "no por contratación directa", "polarity_inversion"),
        (D.MODALITY, "family", POS, "sin licitación pública", "polarity_inversion"),
        (D.MODALITY, "family", POS, "directa o licitación", "or_drop"),
        (D.MODALITY, "family", POS, "licitación o contratación directa", "or_decline"),
        (D.MODALITY, "family", NEG, "contratación directa", "negative_support"),
        (D.MODALITY, "is", POS, "contratación directa", "literal_vs_family"),
        (D.MODALITY, "family", POS, "convenio", "not_a_modality"),
    ]
    for dim, op, pol, quote, kind in hc:
        chain = host_chain(dim, op, pol, quote)
        executed = bool(chain.get("projected"))
        extra = ""
        if kind == "positive":
            outcome = "PASS" if executed else "FAIL_LOUD"
        elif kind == "O6":
            outcome = "FAIL_SILENT" if executed else "DECLINE_OK"
            extra = ("el host sólo exige `not issues and len(matches)==1` (foundation_admission.py:122-124); ignora "
                     "`status=resolved_with_assumption` y `strength=colloquial`: «licitación(es)» se ejecuta como "
                     "licitación pública, contra O6/DA-2bis.")
        elif kind == "polarity_inversion":
            outcome = "FAIL_SILENT" if executed else "DECLINE_OK"
            extra = ("el host mezcla includes+excludes del resolver y NO compara la polaridad del match con la de la "
                     "condición: una cita con negación embebida se ejecuta con polaridad POSITIVA (inversión).")
        elif kind == "or_drop":
            outcome = "FAIL_SILENT" if executed else "DECLINE_OK"
            extra = "la rama «directa» se pierde en silencio y se ejecuta sólo licitación pública."
        elif kind == "or_decline":
            outcome = "DECLINE_OK" if not executed else "FAIL_SILENT"
        elif kind == "negative_support":
            outcome = "GAP"
            extra = "modalidad negativa: admitida pero la proyección la rechaza (UNSUPPORTED_MODALITY_POLARITY): no hospedada."
        elif kind == "literal_vs_family":
            outcome = "DECLINE_OK" if not executed else "FAIL_SILENT"
            extra = "operator=is con una familia de 2 procedimientos -> LITERAL_MODALITY_REQUIRES_ONE_PROCEDURE (loud)."
        else:
            outcome = "DECLINE_OK" if not executed else "FAIL_SILENT"
        if outcome == "FAIL_SILENT":
            extra += (" Defensa en profundidad: el prompt v4.2 (M3/M4/E1) impide que el productor lo proponga así; el host "
                      "no lo verifica por su cuenta.")
        rec(A, "host_chain", kind, "admit_proposal+project_accepted", f"{op}/{pol.value}: {quote}", "ver categoría", chain,
            outcome, note=extra, severity="MEDIUM" if outcome == "FAIL_SILENT" else None, producer_error=True)


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# CONTRACT_UNIVERSE (+ tipo/naturaleza)
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
def probe_contract_universe():
    A = "CONTRACT_UNIVERSE"
    rows = [
        ({"estado_contrato": "En ejecución"}, True), ({"estado_contrato": "en ejecucion"}, True),
        ({"estado_contrato": "terminado"}, True), ({"estado_contrato": "Terminado"}, True),
        ({"estado_contrato": "cedido"}, True), ({"estado_contrato": "Suspendido"}, True),
        ({"estado_contrato": " Modificado "}, True), ({"estado_contrato": "Prorrogado"}, True),
        ({"estado_contrato": "Cerrado"}, True),
        ({"estado_contrato": "Aprobado", "fecha_de_firma": "2025-03-01T00:00:00.000"}, True),
        ({"estado_contrato": "Aprobado"}, False), ({"estado_contrato": "Cancelado"}, False),
        ({"estado_contrato": "Borrador"}, False), ({"estado_contrato": "enviado Proveedor"}, False),
        ({"estado_contrato": "En aprobación"}, False), ({"estado_contrato": "Liquidado"}, False),
        ({"estado_contrato": "No Definido"}, False), ({"estado_contrato": None}, False), ({}, False),
    ]
    for row, want in rows:
        got = CU.eligible(row)
        rec(A, "authority", "universe_eligibility", "contract_universe.eligible(row)", json.dumps(row, ensure_ascii=False),
            str(want), {"eligible": got}, "PASS" if got == want else "FAIL_SILENT")
    got = CU.eligible({"estado_contrato": "Aprobado", "fecha_de_firma": "1900-01-01T00:00:00.000"})
    rec(A, "authority", "universe_eligibility", "contract_universe.eligible(row)", "Aprobado + fecha_de_firma 1900-01-01",
        "una fecha centinela no debería acreditar firma", {"eligible": got}, "INFO",
        note="`observed_date_is_usable` valida el calendario, no la plausibilidad: 1900-01-01 cuenta como firma (política V1.1: «es una fecha»).")
    stamps = [({"dataset": "contratos"}, "celebrated"), ({"dataset": "jbjy-vk9h"}, "celebrated"),
              ({"dataset": "procesos"}, None), ({"dataset": "jbjy-vk9h", "intent_type": "direct_lookup"}, "source_records")]
    for p, want in stamps:
        got = CU.stamp(p)
        rec(A, "authority", "universe_stamp", "contract_universe.stamp(params)", json.dumps(p), str(want),
            {"contract_universe": got.get("contract_universe")}, "PASS" if got.get("contract_universe") == want else "FAIL_SILENT")
    try:
        CU.universe_from_params({"contract_universe_version": "x"})
        got, outcome = {"raised": False}, "FAIL_SILENT"
    except ValueError as exc:
        got, outcome = {"raised": True, "message": str(exc)}, "PASS"
    rec(A, "authority", "universe_version", "contract_universe.universe_from_params", "version distinta",
        "rechaza (ValueError)", got, outcome)
    for p, want in (({"dataset": "contratos", "estado_contrato": ["Cancelado"]}, True),
                    ({"dataset": "contratos", "estado_contrato": ["cancelado"]}, True),
                    ({"dataset": "contratos", "estado_family": "no_procede"}, True),
                    ({"dataset": "contratos", "estado_contrato": ["Suspendido"]}, False),
                    ({"dataset": "procesos", "estado_del_procedimiento": ["Cancelado"]}, False)):
        got = bool(CU.conflict_message(p))
        rec(A, "authority", "state_vs_universe_conflict", "contract_universe.conflict_message", json.dumps(p, ensure_ascii=False),
            f"conflicto={want}", {"conflict": got}, "PASS" if got == want else "FAIL_SILENT")
    # petición explícita de registros (lee TEXTO del turno, no un span)
    reqs = [
        ("contratos de aseo", None), ("todos los registros del dataset", ("source_records", None, False)),
        ("contratos cancelados", ("source_records", "Cancelado", False)), ("contratos en borrador", ("source_records", "Borrador", False)),
        ("contratos enviados al proveedor", ("source_records", "enviado Proveedor", False)),
        ("registros cancelados de aseo en Cali", ("source_records", "Cancelado", False)),
        ("contratos cancelados y borradores", ("source_records", None, True)),
        ("contratos cancelados y procesos cancelados", ("source_records", "Cancelado", True)),
        ("contratos celebrados", ("celebrated", None, False)), ("procesos cancelados", None),
        ("oportunidades canceladas", None), ("contratos que no estén cancelados", None), ("contratos no cancelados", None),
        ("contratos sin cancelar", None), ("contratos suspendidos", None), ("contratos terminados", None),
        ("contratos que el proveedor canceló", None),
    ]
    for text, want in reqs:
        r = universe_request(text)
        got = None if r is None else (r.universe.value, r.state, r.conflict)
        rec(A, "authority", "explicit_record_request", "contract_universe_intent.request(texto del turno)", text,
            str(want), got, "PASS" if got == want else "FAIL_SILENT",
            note="función de TEXTO crudo (regex + split_clauses): no es llamable desde evidencia estructurada." if text == "contratos cancelados" else "")
    # host: SOURCE_RECORDS no se hospeda
    for quote in ("cancelados", "en borrador"):
        chain = host_chain(D.STATE, "is", POS, quote)
        rec(A, "host_chain", "host_support", "admit_proposal", f"STATE is: {quote}", "el host hospeda el universo SOURCE_RECORDS",
            chain, "GAP", note="la proyección fija siempre CELEBRATED; un estado excluido no tiene camino (loud: clarificación).")
    # ── tipo / naturaleza ──
    types = [
        ("contrato de prestación de servicios", ("prestacion_servicios",)), ("prestación de servicios", ("prestacion_servicios",)),
        ("contratos de obra", ("obra",)), ("contrato de suministro", ("suministro",)), ("contrato de compraventa", ("compraventa",)),
        ("contrato de arrendamiento", ("arrendamiento",)), ("contrato de arrendamiento de inmuebles", ("arrendamiento_inmuebles",)),
        ("convenio", ("convenio",)), ("convenios", ("convenio",)),
        ("convenio interadministrativo", ("convenio_interadministrativo",)), ("convenio de asociación", ("convenio_asociacion",)),
        ("convenio solidario", ("convenio_solidario",)), ("contrato interadministrativo", ("contrato_interadministrativo",)),
        ("convenios y contratos de obra", ("obra", "convenio")),
        # sustantivo desnudo = TEMA por diseño («suministro de papelería»): el patrón exige «contrato(s) de …»
        ("suministro", ()), ("suministros", ()), ("compraventa", ()), ("arrendamiento", ()), ("obra", ()),
        # tipos reales de SECOP (secop_field_values.json) sin patrón
        ("obra pública", ("obra",)), ("contrato de consultoría", ("consultoria",)), ("contrato de interventoría", ("interventoria",)),
        ("contrato de comodato", ("comodato",)), ("contrato de seguros", ("seguros",)), ("contrato de concesión", ("concesion",)),
    ]
    for text, want in types:
        r = resolve_contract_types(text)
        got = {"include_ids": list(r.include_ids), "classifications": list(r.classifications), "residual": r.residual_text}
        got_ids, want_ids = tuple(sorted(r.include_ids)), tuple(sorted(want))
        extra = ""
        if got_ids == want_ids:
            outcome = "PASS"
            if not want:
                extra = "sustantivo desnudo = tema (contrato §4 / prompt v4.2 T4: «obra(s)» es TOPIC)."
            elif r.residual_text.strip():
                extra = "residuo no consumido: " + repr(r.residual_text)
        elif not got_ids:
            outcome = "FAIL_LOUD"
            extra = ("tipo real de SECOP no enumerado en CONTRACT_TYPE_CLASSIFICATION (contract_type_resolver.py:20-33)."
                     if text != "obra pública" else
                     "el contrato M1.0 §4 lista «obra pública» como tipo; el prompt v4.2 (T4/K1) manda «obra(s)» a TOPIC: "
                     "CONTRACT_GAP a decidir.")
        else:
            outcome = "FAIL_SILENT"
        rec(A, "authority", "contract_type", "contract_type_resolver.resolve_contract_types", text, str(list(want)), got,
            outcome, note=extra)
    # cobertura del vocabulario de tipos contra los valores reales de SECOP (jbjy-vk9h): ¿qué parte tiene patrón?
    values = json.loads(Path("app/data/secop_field_values.json").read_text(encoding="utf-8"))["jbjy-vk9h"]["fields"]["tipo_de_contrato"]
    phrase = {"Prestación de servicios": "contrato de prestación de servicios", "Suministros": "contrato de suministro",
              "Compraventa": "contrato de compraventa", "Obra": "contrato de obra",
              "Arrendamiento de inmuebles": "contrato de arrendamiento de inmuebles",
              "Arrendamiento de muebles": "contrato de arrendamiento de muebles"}
    total = sum(v["count"] for v in values)
    covered = sum(v["count"] for v in values if v["value"] in phrase and resolve_contract_types(phrase[v["value"]]).include_ids)
    rec(A, "aggregate", "contract_type_coverage", "contract_type_resolver (vs tipo_de_contrato de SECOP)", "valores reales de jbjy-vk9h",
        "cada valor de tipo con volumen tiene un patrón",
        {"distinct_values": len(values), "values_with_pattern": sum(1 for v in values if v["value"] in phrase),
         "row_share_with_pattern": round(covered / total, 3),
         "without_pattern_top": [v["value"] for v in values if v["value"] not in phrase][:10]},
        "FAIL_LOUD", note=f"cobertura por filas alta ({covered / total:.0%}) pero {len(values) - len(phrase)} tipos sin patrón "
                          "(Consultoría, Interventoría, Seguros, Comodato, Concesión…; «Otro» y «Decreto 092 de 2017» no son "
                          "naturaleza nombrable): el usuario recibe una aclaración, nunca un valor equivocado.")
    r = resolve_contract_types("contratos de prestación de servicios profesionales")
    chain = host_chain(D.CONTRACT_TYPE_OR_NATURE, "is", POS, "prestación de servicios profesionales")
    rec(A, "host_chain", "residual_dropped", "admit_proposal+project_accepted", "prestación de servicios profesionales",
        "el host no descarta la cola «profesionales» sin decirlo",
        {"resolver_residual": r.residual_text, "host": chain}, "FAIL_SILENT" if chain.get("projected") else "DECLINE_OK",
        note="el host usa `include_ids[0]` y descarta `residual_text` (foundation_admission.py:138-139): ensancha el filtro "
             "a todo el tipo.", severity="LOW")


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# STATE
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
def _state(token, dataset):
    r = resolve_estado(token, dataset)
    return None if r is None else {"family": r["family"], "kind": r["kind"], "filters": r["filters"]}


def probe_state():
    A = "STATE"
    # (token, dataset, esperado, categoría, nota)   esperado: ("exact", campo, [valores]) | ("family", familia) | ("none",)
    table = [
        ("publicados", "procesos", ("exact", ["Publicado"]), "exact_vs_family", ""),
        ("Publicado", "procesos", ("exact", ["Publicado"]), "exact_vs_family", "case-insensitive"),
        ("abiertos", "procesos", ("family", "oferta_abierta"), "exact_vs_family", "ADR-067/068: «abiertos» = familia (funnel), no el literal Abierto"),
        ("vigentes", "procesos", ("family", "oferta_abierta"), "liveness", ""),
        ("vigentes", "contratos", ("family", "contrato_activo"), "liveness", "dataset-aware"),
        ("cancelados", "procesos", ("exact", ["Cancelado"]), "exact_vs_family", ""),
        ("suspendidos", "procesos", ("exact", ["Suspendido"]), "exact_vs_family", ""),
        ("suspendidos", "contratos", ("exact", ["Suspendido"]), "exact_vs_family", ""),
        ("seleccionados", "procesos", ("exact", ["Seleccionado"]), "exact_vs_family", ""),
        ("borradores", "procesos", ("exact", ["Borrador"]), "exact_vs_family", ""),
        ("terminados", "contratos", ("exact", ["terminado"]), "exact_vs_family", "literal en minúscula (SECOP)"),
        ("cerrados", "contratos", ("exact", ["Cerrado"]), "exact_vs_family", ""),
        ("cedidos", "contratos", ("exact", ["cedido"]), "exact_vs_family", ""),
        ("en ejecucion", "contratos", ("family", "contrato_activo"), "derived", "ADR-003: En ejecución + Modificado + Prorrogado"),
        ("en ejecución", "contratos", ("family", "contrato_activo"), "accent", "la cita llega con tilde: el resolver sólo pliega mayúsculas"),
        ("en evaluación", "procesos", ("family", "en_evaluacion"), "accent", ""),
        ("adjudicados", "procesos", ("exact", ["Seleccionado"]), "lexicon_conflict",
         "ESTADO_PROCEDIMIENTO_SYNONYMS mapea adjudicado -> Seleccionado; NATURAL_TO_FAMILY lo manda a la familia en_evaluacion "
         "(Evaluación+Seleccionado+Aprobado+En aprobación): ensancha a estados previos a la adjudicación."),
        ("liquidados", "contratos", ("exact", ["Cerrado"]), "lexicon_conflict", "ESTADO_CONTRATO_SYNONYMS: liquidado -> Cerrado"),
        ("en curso", "contratos", ("family", "contrato_activo"), "lexicon_conflict", "ESTADO_CONTRATO_SYNONYMS: en curso -> En ejecución"),
        ("finalizados", "contratos", ("exact", ["terminado"]), "lexicon_conflict", "ESTADO_CONTRATO_SYNONYMS: finalizado -> terminado"),
        ("publicados", "contratos", ("none",), "wrong_dataset", "estado de proceso sobre contratos: no resuelve (correcto)"),
        ("terminados", "procesos", ("none",), "wrong_dataset", "estado de contrato sobre procesos: no resuelve (correcto)"),
        ("desiertos", "procesos", ("info",), "approximation", "«desierto» no es un literal de SECOP: se proyecta a la familia no_procede (Cancelado+Suspendido) sin marcarla aproximada"),
        ("adjudicados", "contratos", ("info",), "approximation", "la familia en_evaluacion en contratos son estados FUERA del universo celebrado (Borrador/enviado Proveedor/En aprobación/Aprobado)"),
        ("recibiendo ofertas", "procesos", ("family", "oferta_abierta"), "paraphrase", ""),
        ("por vencer", "procesos", ("none",), "unsupported", "no es un estado"),
    ]
    for token, dataset, spec, category, note in table:
        got = _state(token, dataset)
        kind = spec[0]
        if kind == "exact":
            expected = f"exact {spec[1]}"
            ok = got and list(got["filters"].values())[0] == spec[1] and (dataset == "contratos" or got["kind"] == "exact")
            outcome = "PASS" if ok else ("FAIL_LOUD" if got is None else "FAIL_SILENT")
        elif kind == "family":
            expected = f"family {spec[1]}"
            ok = got and got["family"] == spec[1]
            outcome = "PASS" if ok else ("FAIL_LOUD" if got is None else "FAIL_SILENT")
        elif kind == "none":
            expected = "no resuelve"
            outcome = "PASS" if got is None else "FAIL_SILENT"
        else:
            expected, outcome = "observación", "INFO"
        rec(A, "authority", category, f"estado_families.resolve_estado(token, {dataset!r})", token, expected, got, outcome, note=note,
            severity="MEDIUM" if outcome == "FAIL_SILENT" else None)
    # el reconocimiento léxico de varias menciones (state_vocabulary) vs resolve_estado
    for text in ("cerrados", "procesos cancelados y adjudicados"):
        got = [(m.surface, m.reading) for m in read_process_state_mentions(text)]
        rec(A, "authority", "vocabulary_divergence", "estado_families.read_process_state_mentions", text,
            "coincide con resolve_estado", {"mentions": got, "resolve_estado_procesos": _state(text.split()[0], "procesos")},
            "INFO", note="`cerrados` es Cerrado para el reconocedor y None para resolve_estado(procesos): vocabularios separados." if text == "cerrados" else "")
    # el host NO usa resolve_estado: re-implementa tres palabras (state_value)
    for quote, op, processes in (("publicados", "is", True), ("abiertos", "family", True), ("cancelados", "is", True),
                                 ("terminados", "is", False), ("cerrados", "is", False), ("en ejecución", "is", False),
                                 ("vigentes", "family", True), ("terminados anticipadamente", "is", False)):
        chain = host_chain(D.STATE, op, POS, quote, processes=processes)
        executed = bool(chain.get("projected"))
        authority = _state(quote.replace("ó", "o"), "procesos" if processes else "contratos")
        if executed and quote == "terminados anticipadamente":
            outcome, extra = "FAIL_SILENT", "el host ejecuta estado_contrato='terminado' para una cita que dice otra cosa (la regex `\\bterminad` ignora el resto)."
        elif executed:
            outcome, extra = "PASS", ""
        elif authority:
            outcome, extra = "GAP", ("la autoridad `resolve_estado` sí resuelve esta cita; el host usa su propia `state_value` de 3 "
                                     "patrones (foundation_admission.py:30-39) y la declina: capacidad existente no conectada.")
        else:
            outcome, extra = "DECLINE_OK", ""
        rec(A, "host_chain", "host_support", "admit_proposal+project_accepted", f"{'procesos' if processes else 'contratos'} {op}: {quote}",
            "ver nota", {**chain, "state_value_host": state_value.__name__, "resolve_estado": authority}, outcome, note=extra,
            severity="MEDIUM" if outcome == "FAIL_SILENT" else None)


# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
# veredictos y salida
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
ENTRY_POINTS = {
    "BUYER": [
        {"call": "EntityResolver.resolve_entidad(user_input, departamento=None) -> ResolutionResult(value, method, confidence, like_value, clarification_needed)",
         "where": "app/core/entity_resolver.py:488", "pure": "sí sobre el span (índice cargado en __init__ desde aliases_db.json; sin red, sin reloj)",
         "returns_method_confidence": True},
        {"call": "buyer_source_names.project_buyer_names(canonical, dataset_id) -> tuple[str,...] | None",
         "where": "app/core/buyer_source_names.py:342", "pure": "singleton de módulo de sólo lectura (sidecar JSON)",
         "returns_method_confidence": False},
    ],
    "ACTOR": [
        {"call": "ActorNameResolver.resolve_exact(name) / resolve(name) -> ActorResolutionResult(status, identity, method, candidates)",
         "where": "app/data/actor_alias_resolver.py:87 / :78", "pure": "determinista pero lee SQLite por llamada (artefacto de 660 MB, ruta absoluta obligatoria)",
         "returns_method_confidence": True},
        {"call": "actor_identity.normalize_identity(raw, document_type=None) -> IdentityMatch",
         "where": "app/core/actor_identity.py:414", "pure": "sí", "returns_method_confidence": True},
        {"call": "resolve_actor_params(raw_query, parsed_params, actor_name_resolver, negation_requests=()) -> dict | None",
         "where": "app/core/workflow/actions/actor_resolution.py:109",
         "pure": "NO sobre evidencia estructurada: exige el texto crudo y el parse de QueryRouter, instancia QueryRouter() por dentro "
                 "(actor_resolution.py:87) y lee el flag global `actor_profile_enabled()` (get_settings)", "returns_method_confidence": True},
    ],
    "GEOGRAPHY": [
        {"call": "department_mention_resolution(token) -> GeoMention(kind, canonical, confidence)", "where": "app/core/entity_resolver.py:129",
         "pure": "sí", "returns_method_confidence": True},
        {"call": "get_municipal_territories().lookup(name, department=None) -> tuple[MunicipalTerritory,...]",
         "where": "app/core/municipal_territory.py:79", "pure": "sí (singleton lru_cache de sólo lectura); devuelve candidatos, no decide",
         "returns_method_confidence": False},
        {"call": "REGION_ALIASES / ZONES_BY_NAME (dicts)", "where": "app/data/colombia_geography.py:97,87",
         "pure": "datos, sin función `resolve_region` pública", "returns_method_confidence": False},
        {"call": "MunicipalTerritoryRegistry.consume(text)", "where": "app/core/municipal_territory.py:101", "pure": "NO: lee texto crudo (`en X`)",
         "returns_method_confidence": False},
    ],
    "TIME": [
        {"call": "resolve_temporal_scope(text, *, today=None, protected_spans=()) -> TemporalResolution(matched, fecha_desde, fecha_hasta, precision, error_code)",
         "where": "app/core/temporal_semantics.py:136", "pure": "sí con `today` inyectado (el default lee date.today(), :73); opera sobre el span",
         "returns_method_confidence": True},
    ],
    "MONEY": [
        {"call": "money_to_cop(raw_text) -> int | None", "where": "app/utils/money.py:83", "pure": "sí", "returns_method_confidence": False},
        {"call": "operator_reason(operator, text) -> str | None  (guarda dirección operador vs cita)", "where": "app/core/foundation_value_direction.py:57-59",
         "pure": "sí", "returns_method_confidence": False},
        {"call": "is_admissible_amount(amount, span) -> bool (piso anti-ruido)", "where": "app/core/money_admission.py:75", "pure": "sí; el host no lo llama",
         "returns_method_confidence": False},
    ],
    "MODALITY": [
        {"call": "modality_resolver.resolve(text, dataset=None) -> ModalityResolution(status, includes, excludes, issues, assumption_codes)",
         "where": "app/core/modality_resolver.py:403", "pure": "sí (singleton de catálogo/lexicón YAML)", "returns_method_confidence": True},
    ],
    "CONTRACT_UNIVERSE": [
        {"call": "contract_universe.eligible/stamp/predicate/conflict_message/universe_from_params", "where": "app/core/contract_universe.py:34-90",
         "pure": "sí (sobre fila / params)", "returns_method_confidence": False},
        {"call": "contract_universe_intent.request(text) -> UniverseRequest | None", "where": "app/core/contract_universe_intent.py:39",
         "pure": "NO sobre evidencia: lee el turno crudo (regex, split_clauses, source_occurrence)", "returns_method_confidence": False},
        {"call": "contract_type_resolver.resolve_contract_types(text) -> ContractTypeInterpretation", "where": "app/core/contract_type_resolver.py:118",
         "pure": "sí sobre el span (regex)", "returns_method_confidence": False},
    ],
    "STATE": [
        {"call": "estado_families.resolve_estado(token, dataset) -> {family, filters, kind} | None", "where": "app/core/estado_families.py:392",
         "pure": "sí; exige token ya plegado (sin tilde)", "returns_method_confidence": True},
        {"call": "foundation_admission.state_value(condition) -> str | None  (lo que el host usa HOY)", "where": "app/core/foundation_admission.py:30",
         "pure": "sí, pero sólo 3 patrones (abiertos/publicados/terminados)", "returns_method_confidence": False},
    ],
}

VERDICTS = {
    "BUYER": ("FIX_REQUIRED",
              "Tier 1 de resolve_entidad (entity_resolver.py:540-541) consulta alias_index —last-writer-wins, entity_resolver.py:851— en vez de owners_by_alias: 136/137 aliases con varios "
              "owners resuelven exact/high a uno solo (INVIAS->ANI, SENA/ICBF->una regional, IDRD, ITM, INDER, hospitales homónimos).",
              ["app/core/entity_resolver.py (Tier 1: usar owners_by_alias; >1 owner = AMBIGUOUS/clarify)",
               "app/data/aliases_db.json (alias `invias`/`el invias`/`el instituto nacional de vias` duplicados en la ficha ANI)",
               "app/data/buyer_source_names.json (517 fichas del gazetteer sin binding de nombres físicos; sólo 579/1096 ejecutables)"],
              "S-M: 1 función + 1 dato; el cuello de ejecución es la cobertura de bindings (dato)."),
    "ACTOR": ("FIX_REQUIRED",
              "El directorio resuelve RESOLVED a una identidad concreta para topónimos y palabras comunes (Barranquilla, Huila, «Hospital», "
              "«salud», «Consorcio») incluso a cédulas de persona natural; DV declarado falso no demota el NIT; el host no hospeda ACTOR.",
              ["app/data/actor_alias_resolver.py (guarda de nombres genéricos/topónimos/forma societaria sola)",
               "app/data/actor_alias_index.py + reconstrucción del índice (excluir aliases = topónimo/genérico)",
               "app/core/actor_identity.py (DV mismatch no debe quedar inferred_from_check_digit)",
               "app/core/foundation_admission.py + foundation_projection.py (rama ACTOR -> `contratista`)"],
              "M: 3-4 archivos + rebuild del índice; el host ACTOR es trabajo nuevo, no arreglo."),
    "GEOGRAPHY": ("FIX_REQUIRED",
                  "Departamento OK; MUNICIPIO sin autoridad que decida (lookup devuelve candidatos) y sin alias coloquiales (Cali/Cartagena/Tumaco); "
                  "REGION sólo dicts; `consume` resuelve «en Colombia» a un municipio del Huila; el host sólo hospeda departamentos y no recibe `level`.",
                  ["app/core/municipal_territory.py + app/data/municipal_territories_v1.json (alias coloquiales, política de homónimos, stoplist `Colombia`)",
                   "app/core/interpretation_contract.py (campo `level` en ProposedCondition) + prompt v5",
                   "app/core/foundation_admission.py + foundation_projection.py (MUNICIPALITY/REGION)"],
                  "M: 3-4 archivos; es principalmente trabajo nuevo de host (MUNICIPALITY/REGION no existen como autoridad llamable)."),
    "TIME": ("FIX_REQUIRED",
             "resolve_temporal_scope es determinista y respeta el reloj inyectado, pero ignora el comparador: «después de/antes de/a partir de/de 2024 a la fecha» "
             "devuelven el año completo; «del 1 al 15 de marzo» devuelve el día 15; «primer trimestre/semestre de 2025» devuelve el año; «del 2025» pierde el año. "
             "El host sólo admite during_year y acepta cualquier cita con un año.",
             ["app/core/temporal_semantics.py (comparadores, día-rango de un solo mes, trimestre/semestre, `del <año>`, últimos N meses/años)",
              "app/core/foundation_admission.py (TIME: usar resolve_temporal_scope y verificar operador vs cita)",
              "app/core/foundation_projection.py (during_month / between_dates / relative_period con reloj inyectado)"],
             "M: 3 archivos."),
    "MONEY": ("FIX_REQUIRED",
              "money_to_cop es sólido para escalas es-CO; pero la guarda de dirección no lee negación («que no superen», «no mayor a» pasan con greater_than = inversión ejecutada), "
              "ignora los inclusivos (None para cualquier otro operador), el host admite cifras sin unidad (más de 5) y no proyecta >=/<=.",
              ["app/core/foundation_value_direction.py (negación + vocabulario unificado con money_polarity + operadores inclusivos)",
               "app/core/foundation_admission.py (piso is_admissible_amount)",
               "app/core/foundation_projection.py (valor_min/valor_max inclusivos)"],
              "S-M: 3 archivos."),
    "MODALITY": ("FIX_REQUIRED",
                 "El resolver es sano y señala lo coloquial (resolved_with_assumption). El HOST lo ignora: «licitaciones» se ejecuta como licitación pública (O6), "
                 "«no por contratación directa» con POSITIVE se ejecuta como directa (inversión) y «directa o licitación» pierde una rama.",
                 ["app/core/foundation_admission.py (rama MODALITY: exigir status RESOLVED, strength != colloquial, polaridad del match == polaridad de la condición, 1 target)",
                  "app/core/foundation_projection.py (modalidad negativa)"],
                 "S: 1-2 archivos; el resolver no se toca."),
    "CONTRACT_UNIVERSE": ("HEALTHY",
                          "Las primitivas puras (eligible, stamp, predicate, conflict_message, version guard) pasan todas las sondas y el tipo/naturaleza resuelve lo enumerado "
                          "sin errores silenciosos graves. Huecos: tipos desnudos («suministro», «obra pública», «consultoría») no se reconocen (loud), el host descarta la cola "
                          "residual del tipo, y SOURCE_RECORDS no es hospedable (el universo se decide leyendo texto).",
                          ["app/core/contract_type_resolver.py (patrones desnudos / obra pública / consultoría / interventoría)",
                           "app/core/foundation_admission.py (residual_text no vacío -> clarificar)"],
                          "XS-S: 1-2 archivos."),
    "STATE": ("FIX_REQUIRED",
              "resolve_estado es correcto para los literales, pero (a) exige token sin tilde («en ejecución» -> None), (b) «adjudicados» ensancha a estados previos a la adjudicación "
              "contra su propio léxico, (c) tres catálogos de vocabulario divergen, y (d) el host no la usa: re-implementa 3 palabras (state_value) y ejecuta «terminados anticipadamente» como Terminado.",
              ["app/core/estado_families.py (plegar tildes en resolve_estado; unificar adjudicado)",
               "app/core/foundation_admission.py (state_value -> resolve_estado + verificar cita completa)"],
              "S: 2 archivos."),
}

MISSING_FOR_FOUNDATION = {
    "BUYER": ["Una función pura `resolve_buyer(span) -> BuyerResolution{status: RESOLVED|AMBIGUOUS|UNRESOLVED, owners, canonical}` que respete owners_by_alias.",
              "Paraguas (SENA, ICBF, Alcaldía de Bogotá/Cali): `buyer_groups`/ecosistema como resolución explícita, no una unidad al azar.",
              "Bindings de nombres físicos para las 517 fichas sin binding (hoy sólo 579/1096 ejecutan)."],
    "ACTOR": ["El host no tiene rama ACTOR (ni admisión ni proyección `contratista`).",
              "Guarda de genericidad en el directorio y en el resolver; `resolve_actor_params` no es llamable con evidencia estructurada.",
              "Entrada para documento citado (NIT/CC con o sin marcador) sin pasar por QueryRouter."],
    "GEOGRAPHY": ["MUNICIPALITY no tiene autoridad llamable que decida (sólo candidatos) ni alias coloquiales; REGION no tiene función pública.",
                  "`ProposedCondition` no transporta `level`; el host asume DEPARTAMENTO.",
                  "Proyección municipal/región (`ciudad`, `departamentos_in`) no existe en foundation_projection.py."],
    "TIME": ["TIME no admite during_month, between_dates ni relative_period en el host (sólo during_year por regex).",
             "La autoridad no acepta el comparador como parámetro: necesita `resolve_temporal_scope(span, operator, today)`."],
    "MONEY": ["Operadores inclusivos (>=, <=) no se proyectan; `operator_reason` no los verifica.", "Piso de ruido y negación del comparador."],
    "MODALITY": ["El host debe consumir `status/strength/polarity` del resolver, no sólo `len(matches)==1`.",
                 "Modalidad negativa y OR no se proyectan (GAP)."],
    "CONTRACT_UNIVERSE": ["`SOURCE_RECORDS` como decisión pura sobre la IR (estado excluido + CONTRACTS) en lugar de leer texto."],
    "STATE": ["Conectar `resolve_estado` al host y ampliar STATE más allá de abiertos/publicados/terminados (cerrados, en ejecución, cancelados, vigentes)."],
}


def summarize():
    by_auth: dict[str, dict] = {}
    for p in PROBES:
        a = by_auth.setdefault(p["authority"], {"probes": 0, "outcomes": Counter(), "by_layer": {}, "silent_reachable": [], "silent_other": []})
        a["probes"] += 1
        a["outcomes"][p["outcome"]] += 1
        a["by_layer"].setdefault(p["layer"], Counter())[p["outcome"]] += 1
        if p["outcome"] == "FAIL_SILENT":
            (a["silent_reachable"] if p.get("reachable_from_host") else a["silent_other"]).append(
                {"id": p["id"], "input": p["input"], "layer": p["layer"], "severity": p.get("severity")})
    out = {}
    for name, a in by_auth.items():
        verdict, why, files, size = VERDICTS[name]
        out[name] = {
            "verdict": verdict, "why": why, "fix_files": files, "fix_size": size,
            "missing_for_foundation": MISSING_FOR_FOUNDATION[name], "entry_points": ENTRY_POINTS[name],
            "probes": a["probes"], "outcomes": dict(a["outcomes"]),
            "by_layer": {k: dict(v) for k, v in a["by_layer"].items()},
            "silent_failures_reachable_from_host": a["silent_reachable"], "silent_failures_not_reachable": a["silent_other"],
        }
    return out


def repo_head():
    try:
        gitfile = (REPO / ".git").read_text().strip()
        gitdir = Path(gitfile.split("gitdir:")[1].strip()) if gitfile.startswith("gitdir:") else REPO / ".git"
        head = (gitdir / "HEAD").read_text().strip()
        if head.startswith("ref:"):
            ref = head.split()[1]
            common = gitdir
            for candidate in (gitdir / ref, gitdir.parent.parent / ref):
                if candidate.is_file():
                    return {"ref": ref, "sha": candidate.read_text().strip()[:12]}
            return {"ref": ref, "sha": None}
        return {"ref": "detached", "sha": head[:12]}
    except Exception:
        return {"ref": "unknown", "sha": None}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="docs/audits/m1_search_foundation/a2_authority_probes.json")
    args = parser.parse_args()
    for fn in (probe_buyer, probe_actor, probe_geography, probe_time, probe_money, probe_modality,
               probe_contract_universe, probe_state):
        fn()
    summary = summarize()
    thin = {a: sum(1 for p in PROBES if p["authority"] == a and p["layer"] != "aggregate")
            for a in summary if sum(1 for p in PROBES if p["authority"] == a and p["layer"] != "aggregate") < 15}
    if thin:  # gate de la misión: >= 15 sondas ejecutadas por autoridad
        raise SystemExit(f"sondas insuficientes: {thin}")
    payload = {
        "meta": {"mission": "M1_SEARCH_FOUNDATION_V1 PHASE A", "lane": "A2 AUTHORITY HEALTH", "clock": TODAY.isoformat(),
                 "reference_instant": NOW.isoformat(), "repo": repo_head(), "network": "blocked (socket.connect raises)",
                 "burr_telemetry": False, "actor_index": str(ACTOR_INDEX),
                 "feedback_persistence": "redirected to a tmpdir (SECOP_FEEDBACK_PATH/SECOP_FEEDBACK_DB_PATH)",
                 "outcome_legend": {"PASS": "valor correcto / negativa correcta", "DECLINE_OK": "declinó con razón",
                                    "FAIL_LOUD": "declinó donde existía un valor único (seguro)",
                                    "FAIL_SILENT": "valor equivocado sin señal (grave)", "GATED": "equivocado en la autoridad, contenido por otro control del host",
                                    "GAP": "capacidad ausente / no hospedada", "INFO": "observación sin oracle"},
                 "total_probes": len(PROBES)},
        "verdicts": summary, "probes": PROBES,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False, default=str) + "\n", encoding="utf-8")
    print(f"probes={len(PROBES)} -> {out}")
    for name, v in summary.items():
        print(f"{name:18} {v['verdict']:12} probes={v['probes']:3} {v['outcomes']} silent_reachable={len(v['silent_failures_reachable_from_host'])}")


if __name__ == "__main__":
    main()

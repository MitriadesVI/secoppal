"""Differential shadow: legacy ``QueryRouter`` params vs an accepted Foundation reading.

A pure parity instrument for the legacy -> Foundation migration. ``compare`` projects
(a) ``QueryRouter.parse(raw, today=...).params`` and (b) the resolutions of a READY
``InterpretationAcceptance`` onto one vocabulary of dimensions and says, per dimension,
``AGREE | ONLY_LEGACY | ONLY_FOUNDATION | DIFFER`` with the normalized values of both sides.

Laws of this module
-------------------
* No network, no mutation of either input, no reading of ``raw`` to decide anything
  (it only yields a short sha256 digest); the result is JSON-serializable and ordered
  deterministically; ``compare`` never raises (an internal failure is a ``SHADOW_ERROR``).
* Polarity counts: a negated TOPIC is never the TOPIC. A value this module cannot
  normalize is carried raw and can only ``DIFFER``, never ``AGREE``.
* Not wired into runtime; the ledger stage ``SHADOW`` is integrated afterwards.

Inventory (verified by running ``QueryRouter().parse`` on 83 queries, 2026-09-30; fixture tests/fixtures/legacy_shadow_inventory.json)
----------------------------------------------------------------------------------
The dimension keys are ``SemanticDimension`` values plus the shadow-only
``UNIVERSE``, ``ORDER`` and ``GEOGRAPHY_MUNICIPALITY``. ``LEGACY_KEY_INVENTORY`` is the
machine-readable form of this table.

====================== ================================================== ==========================================
dimension              legacy params keys                                 Foundation side
====================== ================================================== ==========================================
UNIVERSE               dataset, dataset_scope (dataset_explicit and       request resolution (host-universe)
                       contract_universe[_version]: notes only)
RETRIEVAL_INTENT       intent_type                                        REQUESTED_UNIVERSE condition
TOPIC                  objeto (tokens; nested list = OR group),           TOPIC conditions (resolved phrase);
                       objeto_phrases; negative: objeto_exclude           polarity from the condition
BUYER                  entidad_resolved, entidad_like, entidad,           BUYER conditions (canonical name)
                       buyer_group
GEOGRAPHY              departamento_resolved, departamentos_in, region    GEOGRAPHY conditions (department)
GEOGRAPHY_MUNICIPALITY ciudad_resolved, ciudad, municipal_territory       (Foundation resolves departments only)
TIME                   fecha_desde, fecha_hasta, fecha_hasta_exclusive,   TIME condition, operator during_year
                       _temporal_error, _temporal_clarification
VALUE                  valor_min, valor_max, valor_min_exclusive,         VALUE conditions greater_than/less_than
                       valor_max_exclusive, money_clarification
STATE                  estado_del_procedimiento, estado_contrato,         STATE conditions (family or literal)
                       estado_de_apertura_del_proceso, estado,
                       estado_family (only when no state list)
MODALITY               modalidad_include_procedure_ids (positive),        MODALITY conditions (procedure ids)
                       modalidad_exclude_procedure_ids (negative)
CONTRACT_TYPE_OR_NATURE contract_type_include, contract_type_exclude      CONTRACT_TYPE_OR_NATURE conditions
ACTOR                  actor_scope (base_number | observed_name),         ACTOR conditions (none READY today)
                       contratista
ORDER                  ordering_signal                                    (Foundation has no ORDER yet)
====================== ================================================== ==========================================

Where legacy hides a negation: only ``objeto_exclude`` and ``modalidad_exclude_*`` survive in
params. A negated buyer, city, department, state or time is *popped* (``negation_policy``) and
lives only in ``ParsedQuery.negation_requests`` (not an input here); the shadow then shows the
Foundation negative term as ``ONLY_FOUNDATION``. Worse, «sin contar Antioquia» leaves a POSITIVE
``departamento_resolved`` in legacy, which the shadow reports as ``DIFFER`` by polarity.

Doubtful normalization decisions (all conservative)
---------------------------------------------------
* TOPIC: tokens are folded (lowercase, no accents, ``_`` -> space) and the function words
  de/del/la/el/los/las are dropped on both sides; a phrase and the legacy AND-ed token bag are
  equal when their token sets are equal (order and contiguity are NOT compared). A negative
  phrase of several tokens (``not_phrase``) never equals the legacy token-wise ``none_of``.
* VALUE: ``valor_min`` is ``>=`` in the legacy SoQL and Foundation ``greater_than`` is ``>``;
  the shadow compares direction and amount and keeps ``inclusive`` out of the key, noting
  ``BOUND_INCLUSIVITY_DIFFERS`` when an AGREE hides it.
* STATE: both sides become the set of folded states; Foundation ``oferta_abierta`` expands to
  the process funnel of ``ESTADO_FAMILIES``. The ``estado_de_apertura_del_proceso=Abierto`` that
  Foundation projection adds is a projection detail and is not compared.
* TIME: both sides become a half-open interval [from, to_exclusive).
* Opportunity default state (legacy ``estado_*`` filled by ``intent_type``) shows as
  ``ONLY_LEGACY`` STATE; Foundation's O1B policy is listed in ``foundation_policies`` only.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum

from app.core.estado_families import ESTADO_FAMILIES
from app.core.interpretation_contract import (
    AcceptanceStatus, ConditionRole, InterpretationAcceptance, Polarity, ResolutionDecision,
)
from app.core.semantic_dimensions import SemanticDimension
from app.core.soql_builder import SoQLBuilder
from app.utils.money import normalize_text

SHADOW_VERSION = "1"

UNIVERSE = "UNIVERSE"
ORDER = "ORDER"
GEOGRAPHY_MUNICIPALITY = "GEOGRAPHY_MUNICIPALITY"
DIMENSION_ORDER = (
    UNIVERSE, "RETRIEVAL_INTENT", "TOPIC", "BUYER", "GEOGRAPHY", GEOGRAPHY_MUNICIPALITY, "TIME",
    "VALUE", "STATE", "MODALITY", "CONTRACT_TYPE_OR_NATURE", "ACTOR", ORDER, "CARDINALITY",
    "CONTROL_LANGUAGE", "UNSUPPORTED",
)

LEGACY_KEY_INVENTORY: dict[str, tuple[str, ...]] = {
    UNIVERSE: ("dataset", "dataset_scope"),
    "RETRIEVAL_INTENT": ("intent_type",),
    "TOPIC": ("objeto", "objeto_phrases", "objeto_exclude"),
    "BUYER": ("entidad_resolved", "entidad_like", "entidad", "buyer_group"),
    "GEOGRAPHY": ("departamento_resolved", "departamentos_in", "region"),
    GEOGRAPHY_MUNICIPALITY: ("ciudad_resolved", "ciudad", "municipal_territory"),
    "TIME": ("fecha_desde", "fecha_hasta", "fecha_hasta_exclusive",
             "_temporal_error", "_temporal_clarification"),
    "VALUE": ("valor_min", "valor_max", "valor_min_exclusive", "valor_max_exclusive",
              "money_clarification"),
    "STATE": ("estado_del_procedimiento", "estado_contrato", "estado_de_apertura_del_proceso",
              "estado", "estado_family"),
    "MODALITY": ("modalidad_include_procedure_ids", "modalidad_exclude_procedure_ids"),
    "CONTRACT_TYPE_OR_NATURE": ("contract_type_include", "contract_type_exclude"),
    "ACTOR": ("actor_scope", "contratista"),
    ORDER: ("ordering_signal",),
}
# Keys observed in params that carry no dimension of their own: provenance, notes or control.
LEGACY_IGNORED_KEYS = frozenset({
    "dataset_explicit", "contract_universe", "contract_universe_version", "departamento",
    "departamento_resolution", "entidad_resolution", "region_resolution", "money_metadata",
    "actor_resolution", "actor_conversation_operation", "actor_conversation_construction",
    "actor_conversation_probe", "limit", "buyer_group_error", "_temporal_replace_scope",
    "_temporal_anaphora", "estado_field",
})
_MAPPED_KEYS = frozenset(key for keys in LEGACY_KEY_INVENTORY.values() for key in keys)

_FUNCTION_WORDS = frozenset({"de", "del", "la", "el", "los", "las"})
_NON_KEY_FIELDS = frozenset({"inclusive"})
_SAFE_REASON = re.compile(r"[A-Za-z0-9_:.\-]{1,120}\Z")
_DATASET_NAMES = {SoQLBuilder.PROCESOS_DATASET: "procesos", SoQLBuilder.CONTRATOS_DATASET: "contratos"}
_EXPECTED_FAILURES = (ArithmeticError, LookupError, TypeError, ValueError, AttributeError, RecursionError)
_STATE_LISTS = ("estado_del_procedimiento", "estado_contrato", "estado_de_apertura_del_proceso")


class Verdict(str, Enum):
    AGREE = "AGREE"
    ONLY_LEGACY = "ONLY_LEGACY"
    ONLY_FOUNDATION = "ONLY_FOUNDATION"
    DIFFER = "DIFFER"


class ShadowStatus(str, Enum):
    COMPARED = "COMPARED"
    FOUNDATION_NOT_READY = "FOUNDATION_NOT_READY"
    FOUNDATION_MULTI_REQUEST = "FOUNDATION_MULTI_REQUEST"
    LEGACY_PARAMS_INVALID = "LEGACY_PARAMS_INVALID"
    SHADOW_ERROR = "SHADOW_ERROR"


@dataclass(frozen=True, slots=True)
class DimensionDiff:
    dimension: str
    verdict: Verdict
    legacy: tuple[dict, ...]
    foundation: tuple[dict, ...]
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {"dimension": self.dimension, "verdict": self.verdict.value,
                "legacy": [dict(t) for t in self.legacy],
                "foundation": [dict(t) for t in self.foundation], "notes": list(self.notes)}


@dataclass(frozen=True, slots=True)
class ShadowDiff:
    status: ShadowStatus
    raw_digest: str | None
    today: str | None
    reason: str = ""
    foundation_status: str | None = None
    dimensions: tuple[DimensionDiff, ...] = ()
    foundation_policies: tuple[str, ...] = ()
    unmapped_legacy_keys: tuple[str, ...] = ()
    version: str = SHADOW_VERSION

    @property
    def is_parity(self) -> bool:
        return (self.status is ShadowStatus.COMPARED
                and all(d.verdict is Verdict.AGREE for d in self.dimensions))

    def summary(self) -> dict:
        counts = {verdict.value: 0 for verdict in Verdict}
        for item in self.dimensions:
            counts[item.verdict.value] += 1
        return counts

    def to_dict(self) -> dict:
        return {"version": self.version, "status": self.status.value, "reason": self.reason,
                "raw_digest": self.raw_digest, "today": self.today,
                "foundation_status": self.foundation_status, "summary": self.summary(),
                "is_parity": self.is_parity,
                "dimensions": [item.to_dict() for item in self.dimensions],
                "foundation_policies": list(self.foundation_policies),
                "unmapped_legacy_keys": list(self.unmapped_legacy_keys)}


# ── small pure helpers ────────────────────────────────────────────────────────

def _fold(value) -> str:
    return re.sub(r"\s+", " ", normalize_text(str(value))).strip()


def _tokens(value) -> list[str]:
    folded = _fold(value).replace("_", " ")
    return [t for t in re.findall(r"[a-z0-9]+", folded) if t not in _FUNCTION_WORDS]


def _items(value) -> list:
    if value is None:
        return []
    return list(value) if isinstance(value, (list, tuple, set, frozenset)) else [value]


def _term(polarity: str, **value) -> dict:
    return {"polarity": polarity, "value": value}


def _pos(**value) -> dict:
    return _term(Polarity.POSITIVE.value, **value)


def _neg(**value) -> dict:
    return _term(Polarity.NEGATIVE.value, **value)


def _safe(value, depth: int = 0):
    """A JSON-safe, deterministic copy; never raises."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if value == value and value not in (float("inf"), float("-inf")) else str(value)
    if depth > 4:
        return "<deep>"
    if isinstance(value, Mapping):
        return {str(k): _safe(v, depth + 1) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_safe(v, depth + 1) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_safe(v, depth + 1) for v in value), key=lambda v: json.dumps(v, sort_keys=True))
    return str(value)[:200]


def _sort_terms(terms: list[dict]) -> tuple[dict, ...]:
    unique = {json.dumps(t, sort_keys=True): t for t in terms}
    return tuple(unique[key] for key in sorted(unique))


def _key(term: dict) -> str:
    value = {k: v for k, v in term["value"].items() if k not in _NON_KEY_FIELDS}
    return json.dumps({"p": term["polarity"], "v": value}, sort_keys=True)


def _iso(value) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


# ── legacy side ───────────────────────────────────────────────────────────────

def _legacy_universe(p, notes):
    scope = [s for s in _items(p.get("dataset_scope")) if isinstance(s, str)]
    names = scope or ([p["dataset"]] if isinstance(p.get("dataset"), str) else [])
    if names and p.get("dataset_explicit") is False:
        notes.append("LEGACY_DATASET_DEFAULTED")
    return [_pos(dataset=_fold(n)) for n in names]


def _legacy_intent(p, notes):
    intent = p.get("intent_type")
    return [_pos(intent=_fold(intent))] if intent not in (None, "") else []


def _legacy_topic(p, notes):
    terms, bag = [], set()
    for item in (*_items(p.get("objeto")), *_items(p.get("objeto_phrases"))):
        if isinstance(item, (list, tuple)):
            terms.append(_pos(any_of=sorted({t for s in item for t in _tokens(s)})))
        else:
            bag.update(_tokens(item))
    if bag:
        terms.append(_pos(all_of=sorted(bag)))
    excluded = set()
    for item in _items(p.get("objeto_exclude")):
        excluded.update(t for s in _items(item) for t in _tokens(s))
    if excluded:
        terms.append(_neg(none_of=sorted(excluded)))
    return terms


def _legacy_buyer(p, notes):
    if p.get("entidad_resolved"):
        return [_pos(canonical=_fold(p["entidad_resolved"]))]
    if p.get("entidad_like"):
        return [_pos(like=_fold(p["entidad_like"]))]
    if p.get("entidad"):
        return [_pos(unresolved_mention=_fold(p["entidad"]))]
    if p.get("buyer_group"):
        return [_pos(buyer_group=_fold(p["buyer_group"]))]
    return []


def _legacy_geography(p, notes):
    terms = []
    if p.get("departamento_resolved"):
        terms.append(_pos(department=_fold(p["departamento_resolved"])))
        if (p.get("ciudad") or p.get("ciudad_resolved")) and not p.get("departamento"):
            notes.append("DEPARTMENT_DERIVED_FROM_MUNICIPALITY")
    depts = sorted({_fold(d) for d in _items(p.get("departamentos_in"))})
    if depts:
        terms.append(_pos(departments=depts))
    if p.get("region"):
        terms.append(_pos(region=_fold(p["region"])))
    return terms


def _legacy_municipality(p, notes):
    territory = p.get("municipal_territory")
    resolved = (territory.get("municipality") if isinstance(territory, Mapping)
                and territory.get("status") == "resolved" else None)
    name = p.get("ciudad_resolved") or p.get("ciudad") or resolved
    return [_pos(municipality=_fold(name))] if name else []


def _interval(start, end_exclusive) -> dict:
    return {"from": start.isoformat() if start else None,
            "to_exclusive": end_exclusive.isoformat() if end_exclusive else None}


def _legacy_time(p, notes):
    terms = []
    if p.get("_temporal_error") or p.get("_temporal_clarification"):
        terms.append(_pos(unresolved="temporal_issue"))
    raw = [p.get("fecha_desde"), p.get("fecha_hasta"), p.get("fecha_hasta_exclusive")]
    if all(v in (None, "") for v in raw):
        return terms
    start, end, end_ex = (_iso(v) if v not in (None, "") else None for v in raw)
    if any(v not in (None, "") and parsed is None for v, parsed in zip(raw, (start, end, end_ex))):
        return [*terms, _pos(unparsed=_safe(raw))]
    if end_ex is None and end is not None:
        end_ex = end + timedelta(days=1)
    return [*terms, _pos(**_interval(start, end_ex))]


def _amount(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value == int(value):
        return int(value)
    if isinstance(value, str) and re.fullmatch(r"\d+", value.strip()):
        return int(value.strip())
    return None


def _legacy_value(p, notes):
    terms = []
    if p.get("money_clarification"):
        terms.append(_pos(unresolved="money_clarification"))
    for key, bound, inclusive in (("valor_min", "min", True), ("valor_max", "max", True),
                                  ("valor_min_exclusive", "min", False),
                                  ("valor_max_exclusive", "max", False)):
        if p.get(key) is None:
            continue
        amount = _amount(p[key])
        terms.append(_pos(bound=bound, amount=amount, inclusive=inclusive) if amount is not None
                     else _pos(unparsed=_safe(p[key]), key=key))
    return terms


def _legacy_state(p, notes):
    states = {_fold(s) for key in _STATE_LISTS for s in _items(p.get(key))}
    if not states and p.get("estado"):
        states = {_fold(p["estado"])}
    if states:
        return [_pos(states=sorted(states))]
    if p.get("estado_family"):
        return [_pos(family=_fold(p["estado_family"]))]
    return []


def _legacy_modality(p, notes):
    terms = []
    for key, make in (("modalidad_include_procedure_ids", _pos),
                      ("modalidad_exclude_procedure_ids", _neg)):
        ids = sorted({_fold(i) for i in _items(p.get(key))})
        if ids:
            terms.append(make(procedure_ids=ids))
    return terms


def _legacy_contract_type(p, notes):
    terms = []
    for key, make in (("contract_type_include", _pos), ("contract_type_exclude", _neg)):
        ids = sorted({_fold(i) for i in _items(p.get(key))})
        if ids:
            terms.append(make(types=ids))
    return terms


def _actor_term(identifier) -> dict:
    text = str(identifier)
    if re.fullmatch(r"[\d.\-\s]+", text) and re.search(r"\d", text):
        return _pos(nit=re.sub(r"\D", "", text))
    return _pos(name=_fold(text))


def _legacy_actor(p, notes):
    scope = p.get("actor_scope")
    if isinstance(scope, Mapping):
        if scope.get("base_number"):
            return [_actor_term(scope["base_number"])]
        if scope.get("observed_name"):
            return [_pos(name=_fold(scope["observed_name"]))]
    return [_actor_term(i) for i in _items(p.get("contratista")) if str(i).strip()]


def _legacy_order(p, notes):
    signal = p.get("ordering_signal")
    return [_pos(signal=_fold(signal))] if signal not in (None, "") else []


_LEGACY_PROJECTORS = {
    UNIVERSE: _legacy_universe, "RETRIEVAL_INTENT": _legacy_intent, "TOPIC": _legacy_topic,
    "BUYER": _legacy_buyer, "GEOGRAPHY": _legacy_geography,
    GEOGRAPHY_MUNICIPALITY: _legacy_municipality, "TIME": _legacy_time, "VALUE": _legacy_value,
    "STATE": _legacy_state, "MODALITY": _legacy_modality,
    "CONTRACT_TYPE_OR_NATURE": _legacy_contract_type, "ACTOR": _legacy_actor, ORDER: _legacy_order,
}


def _legacy_side(params: Mapping) -> tuple[dict, dict]:
    terms, notes = {}, {}
    for dimension, projector in _LEGACY_PROJECTORS.items():
        dimension_notes: list[str] = []
        try:
            projected = projector(params, dimension_notes)
        except (TypeError, ValueError, AttributeError, KeyError) as exc:
            keys = LEGACY_KEY_INVENTORY[dimension]
            projected = [_pos(unnormalizable=type(exc).__name__,
                              raw=_safe({k: params[k] for k in keys if k in params}))]
        if projected:
            terms[dimension] = projected
            notes[dimension] = dimension_notes
    return terms, notes


# ── Foundation side ───────────────────────────────────────────────────────────

def _resolved(acceptance, target_id) -> tuple[str, ...] | None:
    for item in acceptance.resolutions:
        if item.target_id == target_id and item.decision is ResolutionDecision.RESOLVED:
            return item.resolved_ids
    return None


def _foundation_value(condition, ids):
    """The normalized value of one resolved condition (never reads the evidence text)."""
    dimension, operator = condition.dimension, condition.operator
    if ids is None or not ids:
        return {"unresolved": operator}
    if dimension is SemanticDimension.GEOGRAPHY:
        return {"department": _fold(ids[0])} if len(ids) == 1 else {"departments": sorted(map(_fold, ids))}
    if dimension is SemanticDimension.BUYER:
        return {"canonical": _fold(ids[0])} if len(ids) == 1 else {"resolved": sorted(map(_fold, ids))}
    if dimension is SemanticDimension.MODALITY:
        return {"procedure_ids": sorted(map(_fold, ids))}
    if dimension is SemanticDimension.CONTRACT_TYPE_OR_NATURE:
        return {"types": sorted(map(_fold, ids))}
    if dimension is SemanticDimension.TIME:
        if operator == "during_year" and len(ids) == 1 and re.fullmatch(r"(?:19|20)\d{2}", ids[0]):
            year = int(ids[0])
            return _interval(date(year, 1, 1), date(year + 1, 1, 1))
    elif dimension is SemanticDimension.VALUE:
        bound = {"greater_than": "min", "less_than": "max"}.get(operator)
        amount = _amount(ids[0]) if len(ids) == 1 else None
        if bound is not None and amount is not None:
            return {"bound": bound, "amount": amount, "inclusive": False}
    elif dimension is SemanticDimension.STATE:
        if len(ids) == 1 and operator == "family" and ids[0] == "oferta_abierta":
            funnel = ESTADO_FAMILIES["oferta_abierta"]["procesos"]["estado_del_procedimiento"]
            return {"states": sorted({_fold(s) for s in funnel})}
        if len(ids) == 1 and operator != "family":
            return {"states": [_fold(ids[0])]}
    elif dimension is SemanticDimension.ACTOR:
        if len(ids) == 1:
            return _actor_term(ids[0])["value"]
    return {"operator": operator, "resolved": sorted(map(str, ids))}


def _foundation_topic(entries) -> list[dict]:
    """Conjunctive TOPIC conditions merge like the legacy token bag; the rest stay apart."""
    terms, positive, negative = [], set(), set()
    for polarity, tokens, nonconjunctive in entries:
        if nonconjunctive:
            terms.append(_term(polarity, nonconjunctive={"all_of": tokens}))
        elif polarity == Polarity.POSITIVE.value:
            positive.update(tokens)
        elif len(tokens) == 1:
            negative.update(tokens)
        else:
            terms.append(_neg(not_phrase=tokens))
    if positive:
        terms.append(_pos(all_of=sorted(positive)))
    if negative:
        terms.append(_neg(none_of=sorted(negative)))
    return terms


def _foundation_side(acceptance: InterpretationAcceptance) -> tuple[dict, tuple[str, ...]]:
    proposal = acceptance.proposal
    terms: dict[str, list[dict]] = {}
    topics: list[tuple] = []
    for request in proposal.requests:
        ids = _resolved(acceptance, request.request_id) or ()
        for dataset in ids:
            terms.setdefault(UNIVERSE, []).append(_pos(dataset=_DATASET_NAMES.get(dataset, _fold(dataset))))
    expressions = {r.request_id: r.condition_expression for r in proposal.requests}
    for condition in proposal.conditions:
        polarity = condition.polarity.value
        if condition.role is ConditionRole.REQUESTED_UNIVERSE:
            terms.setdefault("RETRIEVAL_INTENT", []).append(_pos(intent="opportunity_search"))
            continue
        ids = _resolved(acceptance, condition.condition_id)
        nonconjunctive = any(
            expressions[r] is not None and expressions[r].has_nonconjunctive_atom(condition.condition_id)
            for r in condition.request_ids if r in expressions)
        if condition.dimension is SemanticDimension.TOPIC:
            tokens = sorted({t for i in ids or () for t in _tokens(i)})
            if tokens:
                topics.append((polarity, tokens, nonconjunctive))
            else:
                terms.setdefault("TOPIC", []).append(_term(polarity, unresolved=condition.operator))
            continue
        value = _foundation_value(condition, ids)
        if nonconjunctive:
            value = {"nonconjunctive": value}
        terms.setdefault(condition.dimension.value, []).append(_term(polarity, **value))
    if topics:
        terms.setdefault("TOPIC", []).extend(_foundation_topic(topics))
    policies = tuple(sorted({f"{p.policy_id}@{p.version}" for p in acceptance.policies}))
    return terms, policies


# ── comparison ────────────────────────────────────────────────────────────────

def _verdict(legacy: tuple[dict, ...], foundation: tuple[dict, ...]) -> Verdict:
    if legacy and not foundation:
        return Verdict.ONLY_LEGACY
    if foundation and not legacy:
        return Verdict.ONLY_FOUNDATION
    if {_key(t) for t in legacy} == {_key(t) for t in foundation}:
        return Verdict.AGREE
    return Verdict.DIFFER


def _inclusivity(terms) -> dict:
    return {_key(t): t["value"].get("inclusive") for t in terms}


def _dimension_diffs(legacy, legacy_notes, foundation) -> tuple[DimensionDiff, ...]:
    names = [d for d in DIMENSION_ORDER if d in legacy or d in foundation]
    names += sorted((set(legacy) | set(foundation)) - set(DIMENSION_ORDER))
    diffs = []
    for name in names:
        left = _sort_terms(legacy.get(name, []))
        right = _sort_terms(foundation.get(name, []))
        verdict = _verdict(left, right)
        notes = list(legacy_notes.get(name, []))
        if (verdict is Verdict.AGREE and name == "VALUE"
                and _inclusivity(left) != _inclusivity(right)):
            notes.append("BOUND_INCLUSIVITY_DIFFERS")
        diffs.append(DimensionDiff(name, verdict, left, right, tuple(sorted(set(notes)))))
    return tuple(diffs)


def _digest(raw) -> str | None:
    if not isinstance(raw, str):
        return None
    return hashlib.sha256(raw.encode("utf-8", errors="surrogatepass")).hexdigest()[:16]


def _today(today) -> str | None:
    return today.isoformat() if isinstance(today, date) else None


def _unmapped(params: Mapping) -> tuple[str, ...]:
    return tuple(sorted(str(k) for k in params if k not in _MAPPED_KEYS and k not in LEGACY_IGNORED_KEYS))


def compare(raw, today, legacy_params, acceptance) -> ShadowDiff:
    """Per-dimension parity of legacy params vs a READY Foundation acceptance. Never raises."""
    digest, day = _digest(raw), _today(today)
    try:
        if not isinstance(legacy_params, Mapping):
            return ShadowDiff(ShadowStatus.LEGACY_PARAMS_INVALID, digest, day, "LEGACY_PARAMS_NOT_A_MAPPING")
        foundation_status = (acceptance.status.value
                             if isinstance(acceptance, InterpretationAcceptance) else None)
        if (not isinstance(acceptance, InterpretationAcceptance)
                or acceptance.status is not AcceptanceStatus.READY or acceptance.proposal is None):
            reason = "NO_ACCEPTANCE" if acceptance is None else str(getattr(acceptance, "reason", ""))
            return ShadowDiff(ShadowStatus.FOUNDATION_NOT_READY, digest, day,
                              reason if _SAFE_REASON.fullmatch(reason) else "UNSPECIFIED",
                              foundation_status)
        if len(acceptance.proposal.requests) != 1:
            return ShadowDiff(ShadowStatus.FOUNDATION_MULTI_REQUEST, digest, day, "NOT_COMPARABLE",
                              foundation_status)
        legacy, legacy_notes = _legacy_side(legacy_params)
        foundation, policies = _foundation_side(acceptance)
        return ShadowDiff(ShadowStatus.COMPARED, digest, day, "", foundation_status,
                          _dimension_diffs(legacy, legacy_notes, foundation), policies,
                          _unmapped(legacy_params))
    except _EXPECTED_FAILURES as exc:  # the instrument must not break the turn; the failure is the result
        return ShadowDiff(ShadowStatus.SHADOW_ERROR, digest, day, type(exc).__name__)

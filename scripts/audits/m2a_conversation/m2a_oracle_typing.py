"""M2A-8a0 oracle, part 2: typing of ``value_hint`` and the ``m2a-accepted/1`` / ``m2a-patch/1`` dicts.

A hint becomes the closed ``ExecutableValue`` of its dimension, deterministically and without guessing:

* opaque dimensions (TOPIC, BUYER, ACTOR, GEOGRAPHY, MODALITY, CONTRACT_TYPE_OR_NATURE) carry the hint as their
  identity (ACTOR: a synthetic document hashed from it; a REGION: one synthetic member hashed from it), so the same hint
  gives the same value and distinct hints give distinct values;
* quantified dimensions are normalised to their canonical content: VALUE to ``amount_cop`` and comparator (the unit goes
  to ``Provenance.unit``), TIME to its half-open interval, ORDER and CARDINALITY to their enumerations. Two hints that
  denote the same canonical content give the same value: that is what a canonical value is. A SMMLV amount is converted
  with the SEALED catalogue of D10 (``SMMLV_COP_BY_YEAR``, read through the sealed checker) and the single civil year of the
  IR it belongs to (§15.5); the conversion is the contract's own rule, and ``Provenance`` keeps the unit, the amount said
  and the year used. A relative window is typed only when its hint states the window explicitly;
* a hint that cannot be typed without guessing raises ``Untypeable`` and the case becomes ``NO_MEDIBLE(<motive>)``: a
  VALUE hint must hold exactly one number and any unit word in it must agree with the unit the Gold gives; a SMMLV amount
  needs a single catalogued year; a relative window without an explicit ``(start..end)`` is not an interval;
* a CLEAR names the whole dimension unless the Gold gives it a polarity, and then only that polarity's slot.

Where a datum is typed twice (an op and the resulting IR) both typings must coincide. Only the closed value types are
used (they are not M2a); the dicts are built by hand from the shapes of contract §3 and §5.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audits.m2a_conversation.m2a_oracle_gold import checker, sha  # noqa: E402

from app.core.foundation_authority_values import (  # noqa: E402 (closed types: not M2a)
    MAX_TOP_N, STATE_CLASSES, ActorValue, BuyerKind, BuyerValue, CardinalityBasis, CardinalityValue, Comparator,
    ContractTypeValue, GeoValue, ModalityValue, MoneyValue, OrderDirection, OrderKey, OrderValue, StateValue, TimeValue,
    TopicValue,
)
from app.core.foundation_geo_level import GeoLevel  # noqa: E402

UNIT_SCALE = {"PESOS": 1, "MILES": 10**3, "MILLONES": 10**6, "MIL_MILLONES": 10**9, "BILLONES": 10**12}
COMPARATOR_OF = {"greater_than": Comparator.GT, "greater_or_equal": Comparator.GTE,
                 "less_than": Comparator.LT, "less_or_equal": Comparator.LTE}
OPPOSITE = {Comparator.GT: Comparator.LTE, Comparator.GTE: Comparator.LT,
            Comparator.LT: Comparator.GTE, Comparator.LTE: Comparator.GT}
LOWER = frozenset({Comparator.GT, Comparator.GTE})
POLARIZED = ("TOPIC", "BUYER", "ACTOR", "GEOGRAPHY", "TIME", "STATE", "MODALITY", "CONTRACT_TYPE_OR_NATURE")
MONTHS = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
          "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}
AUTHORITY = "oracle.synthetic@1"


class Untypeable(Exception):
    """A piece of the Gold cannot be typed or determined without guessing: the case is ``NO_MEDIBLE(motive)``."""

    def __init__(self, motive: str):
        super().__init__(motive)
        self.motive = motive


# ---- hints -----------------------------------------------------------------------------------------------------------
def nfc(text) -> str:
    if type(text) is not str or not text.strip():
        raise Untypeable("HINT_EMPTY")
    return unicodedata.normalize("NFC", text).strip()


def fold(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text.lower()) if unicodedata.category(c) != "Mn")


def _next_month(year: int, month: int) -> date:
    return date(year + (month == 12), month % 12 + 1, 1)


def _time_label(start: date | None, end: date | None) -> str:
    if start is None:
        return "until"
    if end is None:
        return "from"
    if (start.month, start.day) == (1, 1) and end == date(start.year + 1, 1, 1):
        return "year"
    if start.day == 1 and end == _next_month(start.year, start.month):
        return "month"
    return "range"


def _time_value(operator: str, hint, excluded: bool) -> tuple[str, ...]:
    text = nfc(hint)
    start = end = None
    try:
        if operator == "during_year":
            m = re.fullmatch(r"(\d{4})", text)
            if not m:
                raise Untypeable("TIME_YEAR_HINT")
            start, end = date(int(m.group(1)), 1, 1), date(int(m.group(1)) + 1, 1, 1)
        elif operator == "during_month":
            iso, spoken = re.fullmatch(r"(\d{4})-(\d{2})", text), re.fullmatch(r"([a-z]+)\s+de\s+(\d{4})", fold(text))
            if iso:
                year, month = int(iso.group(1)), int(iso.group(2))
            elif spoken and spoken.group(1) in MONTHS:
                year, month = int(spoken.group(2)), MONTHS[spoken.group(1)]
            else:
                raise Untypeable("TIME_MONTH_HINT")
            start, end = date(year, month, 1), _next_month(year, month)
        elif operator == "between_dates":
            m = re.fullmatch(r"(\d{4}-\d{2}-\d{2})?\.\.(\d{4}-\d{2}-\d{2})?", text)
            if not m or not (m.group(1) or m.group(2)):
                raise Untypeable("TIME_RANGE_HINT")
            start = date.fromisoformat(m.group(1)) if m.group(1) else None
            end = date.fromisoformat(m.group(2)) + timedelta(days=1) if m.group(2) else None  # the end day is inclusive
        elif operator == "relative_period":  # a window the hint states: «últimos 3 meses (2026-07-01..2026-10-01)»
            m = re.search(r"\((\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})\)\s*$", text)
            if not m:
                raise Untypeable("TIME_RELATIVE_WINDOW_NOT_EXPLICIT")
            start = date.fromisoformat(m.group(1))
            end = date.fromisoformat(m.group(2)) + timedelta(days=1)  # the notation of between_dates: the end day is inclusive
        else:
            raise Untypeable(f"TIME_OPERATOR:{operator}")
        return TimeValue(start, end, _time_label(start, end), excluded).encode()
    except ValueError as error:  # impossible date, empty interval (ContractError is a ValueError)
        raise Untypeable("TIME_INVALID_INTERVAL") from error


def _unit_word(folded: str) -> str | None:
    for pattern, unit in ((r"mil\s+millones", "MIL_MILLONES"), (r"billon", "BILLONES"), (r"millon", "MILLONES"),
                          (r"\bmil\b", "MILES"), (r"smmlv", "SMMLV"), (r"\bpesos?\b", "PESOS")):
        if re.search(pattern, folded):
            return unit
    return None


def _number(hint, unit: str) -> Decimal:
    """The single number of a VALUE hint, in the unit said (never a guess among several, never an ambiguous ``2.5``)."""
    text = nfc(hint)
    numbers = re.findall(r"\d[\d.]*(?:,\d+)?", text)
    if len(numbers) != 1:  # the first number of «de 50 a 100 millones» would be a guess
        raise Untypeable("VALUE_NUMBER_COUNT")
    word = _unit_word(fold(text))
    if word is not None and word != unit:
        raise Untypeable("VALUE_UNIT_WORD_DISAGREES")
    whole, comma, frac = numbers[0].partition(",")
    if "." in whole and not re.fullmatch(r"\d{1,3}(?:\.\d{3})+", whole):
        raise Untypeable("VALUE_NUMBER_AMBIGUOUS")  # «2.5» could be 2,5 or 2500: not guessed
    try:
        return Decimal(whole.replace(".", "") + ("." + frac if comma else ""))
    except InvalidOperation as error:
        raise Untypeable("VALUE_NO_NUMBER") from error


def _amount(hint, unit: str, year: int | None = None) -> int:
    """``amount_cop`` of a VALUE hint: the scale of its unit; for SMMLV (§15.5) the sealed catalogue of D10 at ``year``."""
    if unit == "SMMLV":
        if year is None:
            raise Untypeable("SMMLV_YEAR_NOT_SINGLE")  # no single civil year in the IR: the clock is not in the Gold
        scale = checker().SMMLV_COP_BY_YEAR.get(year)
        if scale is None:
            raise Untypeable("SMMLV_YEAR_NOT_CATALOGUED")  # the catalogue is closed: nothing is extrapolated
    else:
        scale = UNIT_SCALE.get(unit or "")
        if scale is None:
            raise Untypeable("VALUE_UNIT_NOT_PESO_SCALE")
    amount = _number(hint, unit) * scale
    if amount != amount.to_integral_value():
        raise Untypeable("VALUE_NOT_WHOLE_PESOS")
    return int(amount)


def _civil_year(value) -> int | None:
    """The single civil year a typed ``time@1`` interval lies in, or None (several years, or an open bound)."""
    try:
        start = date.fromisoformat(next(i for i in value if i.startswith("start=")).partition("=")[2])
        end = date.fromisoformat(next(i for i in value if i.startswith("end_exclusive=")).partition("=")[2])
    except (StopIteration, ValueError):
        return None
    return start.year if (end - timedelta(days=1)).year == start.year else None


def ir_year(ir: dict | None) -> int | None:
    """The single year of an IR for a SMMLV conversion (§15.5): its ONE positive TIME condition, when it lies in one civil
    year. No TIME, several TIME conditions or several years give None (the clock is not in the Gold)."""
    times = [tcond("TIME", c["operator"], c["polarity"], c["level"], c["unit"], c["value_hint"])
             for c in (ir or {"conditions": []})["conditions"] if c["dimension"] == "TIME" and c["polarity"] == "POSITIVE"]
    return _civil_year(times[0]["value"]) if len(times) == 1 else None


def money_amount(value) -> int:
    """``amount_cop`` of an encoded ``money@1`` value."""
    return int(next(i for i in value if i.startswith("amount_cop=")).partition("=")[2])


def _opaque(text: str) -> str:
    return sha(unicodedata.normalize("NFC", text))


def type_value(dimension: str, operator: str, polarity: str, level: str | None, unit: str | None, hint,
               year: int | None = None) -> tuple[str, ...]:
    """The ``ExecutableValue.encode()`` of a Gold condition or op (contract §2, §3.2). ``year``: the single civil year of the
    IR the condition belongs to, only for the conversion of a SMMLV amount (§15.5)."""
    excluded = polarity == "NEGATIVE"
    if dimension == "TOPIC":
        return TopicValue(nfc(hint), operator == "whole_token_folded", excluded).encode()
    if dimension == "BUYER":
        name = nfc(hint)
        return BuyerValue(name, BuyerKind.UNIT, (name,), None, excluded).encode()
    if dimension == "ACTOR":
        name = nfc(hint)
        return ActorValue("H" + _opaque(name)[:15].upper(), "ORACLE", name, excluded).encode()
    if dimension == "GEOGRAPHY":
        if level not in ("DEPARTMENT", "MUNICIPALITY", "REGION"):
            raise Untypeable(f"GEO_LEVEL:{level}")
        department = "oracle-department" if level == "MUNICIPALITY" else None
        # A REGION needs its members (E-GEO's closed list, §19 DEP-REGION: LECTURA). The Gold carries none and compose never
        # reads them, so the typed value has ONE synthetic member derived from the hint, as an ACTOR has a synthetic
        # document (``LG-REGION-MEMBERS-SYNTHETIC``): the same hint gives the same value, distinct hints distinct values.
        members = (f"oracle-member-{_opaque(nfc(hint))[:12]}",) if level == "REGION" else ()
        return GeoValue(GeoLevel(level), nfc(hint), department, None, members, excluded).encode()
    if dimension == "TIME":
        return _time_value(operator, hint, excluded)
    if dimension == "VALUE":
        comparator = COMPARATOR_OF.get(operator)
        if comparator is None:
            raise Untypeable(f"VALUE_OPERATOR:{operator}")
        if polarity == "NEGATIVE":  # the comparator absorbs the negation (contract §2): «no más de X» is <= X
            comparator = OPPOSITE[comparator]
        return MoneyValue(_amount(hint, unit or "", year), comparator).encode()
    if dimension == "STATE":
        name = nfc(hint)
        if name not in STATE_CLASSES:
            raise Untypeable("STATE_CLASS")
        return StateValue(name, (), excluded).encode()
    if dimension == "MODALITY":
        name = nfc(hint)
        if operator == "family":
            return ModalityValue((), f"oracle-{_opaque(name)[:12]}@1", excluded).encode()
        return ModalityValue((name,), None, excluded).encode()
    if dimension == "CONTRACT_TYPE_OR_NATURE":
        return ContractTypeValue((nfc(hint),), excluded).encode()
    raise Untypeable(f"DIMENSION:{dimension}")


def _order_value(operator: str) -> tuple[str, ...]:
    key, _, direction = operator.partition("_")
    try:
        return OrderValue(OrderKey(key), OrderDirection(direction)).encode()
    except ValueError as error:
        raise Untypeable(f"ORDER_OPERATOR:{operator}") from error


def _cardinality_value(n_requested) -> tuple[str, ...]:
    """The Gold keeps ``(n, n_requested)`` and no basis: SINGULAR iff the figure is 1, a documented total function."""
    try:
        figure = int(n_requested)
        basis = CardinalityBasis.SINGULAR if figure == 1 else CardinalityBasis.FIGURE
        return CardinalityValue(min(figure, MAX_TOP_N), basis, figure).encode()
    except (TypeError, ValueError) as error:
        raise Untypeable("CARDINALITY_FIGURE") from error


def slot_key(dimension: str, value: tuple[str, ...]) -> str:
    """The slot a typed value occupies (contract §4.2): VALUE by its comparator, polarized dimensions by ``excluded``."""
    if dimension == "VALUE":
        comparator = Comparator(next(i for i in value if i.startswith("comparator=")).partition("=")[2])
        return "VALUE/lower" if comparator in LOWER else "VALUE/upper"
    if dimension in POLARIZED:
        excluded = next(i for i in value if i.startswith("excluded=")).partition("=")[2] == "1"
        return f"{dimension}/{'NEGATIVE' if excluded else 'POSITIVE'}"
    return dimension


# ---- typed conditions and the accepted / patch dicts ------------------------------------------------------------------
def tcond(dimension, operator, polarity, level, unit, hint, *, ods=None, unit_source=None, n_requested=None,
          year_source=None, year=None) -> dict:
    """A typed condition (a plain dict): the identity every comparison of the oracle is made on."""
    if dimension == "ORDER":
        value = _order_value(operator)
    elif dimension == "CARDINALITY":
        value = _cardinality_value(n_requested)
    else:
        value = type_value(dimension, operator, polarity, level, unit, hint, year)
    slot = slot_key(dimension, value)
    smmlv = dimension == "VALUE" and unit == "SMMLV"  # §3.2 Provenance: the amount said and the year the conversion used
    return {"dimension": dimension, "operator": operator, "slot": slot,
            "polarity": "NEGATIVE" if slot.endswith("/NEGATIVE") else "POSITIVE",
            "level": level if dimension == "GEOGRAPHY" else None, "value": list(value),
            "unit": unit if dimension == "VALUE" else None, "ods": ods if dimension == "ORDER" else None,
            "unit_source": (unit_source or "STATED") if dimension == "VALUE" else None,
            "year_source": year_source if dimension == "TIME" else None,
            "source_amount": format(_number(hint, unit).normalize(), "f") if smmlv else None,
            "year": year if smmlv else None}


def sem_key(c: dict) -> tuple:
    """Semantic identity of a typed condition (what «the same condition» means when carrying)."""
    return (c["dimension"], c["operator"], c["polarity"], c["level"], tuple(c["value"]), c["unit"], c["ods"])


def ir_conditions(ir: dict) -> list[dict]:
    """Typed conditions of a Gold IR, each with its Gold id (ORDER and CARDINALITY are conditions in contract §2)."""
    if ir.get("logic") is not None:
        raise Untypeable("IR_LOGIC")
    out = []
    year = ir_year(ir) if any(c["dimension"] == "VALUE" and c["unit"] == "SMMLV" for c in ir["conditions"]) else None
    for c in ir["conditions"]:
        if c["dimension"] == "SOURCE":
            raise Untypeable("IR_SOURCE_CONDITION")  # IR v2 keeps it in source_evidence, it is not a slot
        out.append({**tcond(c["dimension"], c["operator"], c["polarity"], c["level"], c["unit"], c["value_hint"], year=year),
                    "id": c["id"]})
    if ir.get("order"):
        out.append({**tcond("ORDER", ir["order"]["operator"], "POSITIVE", None, None, None,
                            ods=ir["order"].get("direction_source")), "id": "order"})
    if ir.get("cardinality"):
        ca = ir["cardinality"]
        if ca["n"] != min(int(ca["n_requested"]), MAX_TOP_N):
            raise Untypeable("CARDINALITY_FIGURE")
        out.append({**tcond("CARDINALITY", "top_n", "POSITIVE", None, None, None, n_requested=ca["n_requested"]),
                    "id": "cardinality"})
    return out


_UNIVERSE = {"CONTRACTS": ("contracts",), "PROCESSES": ("processes",), "BOTH": ("processes", "contracts")}
_KIND = {"RECORDS": "records", "OPPORTUNITIES": "opportunities"}


def requests_of(ir: dict) -> list[tuple[str, str]]:
    subjects, kind = _UNIVERSE.get(ir["universe"]), _KIND.get(ir["view"])
    if subjects is None or kind is None:
        raise Untypeable("UNIVERSE_VIEW")  # UNSPECIFIED + Product View is the F wave's: it never reaches here
    return [(subject, kind) for subject in subjects]


def origin(trace: str, raw: str, i: int) -> dict:
    return {"kind": "SPAN", "trace_id": trace, "raw_sha256": raw, "span": [i, i + 1], "event_id": None}


def condition_dict(t: dict, cid: str, request_ids: list[str], org: dict) -> dict:
    return {"condition_id": cid, "request_ids": sorted(request_ids), "slot": t["slot"], "dimension": t["dimension"],
            "operator": t["operator"], "polarity": t["polarity"], "level": t["level"], "value": list(t["value"]),
            "authority": AUTHORITY, "receipt_rules": [], "policies": [], "origin": org,
            "provenance": {"unit": t["unit"], "source_amount": t.get("source_amount"), "unit_source": t["unit_source"],
                           "year": t.get("year"),
                           "year_source": t.get("year_source"), "buyer_scope": None, "order_direction_source": t["ods"],
                           "inherited_from": None}}


def dataset_of(subject: str) -> str:
    """The dataset a request projects follows its SUBJECT, not its kind (contract §3.2: the one the subject projects);
    the oracle's placeholder stands for the host table: the real ids are the reducer's (see ``LIMITED_GUARANTEES``)."""
    return f"oracle.dataset.{subject}"


def request_dicts(pairs, trace: str, raw: str) -> list[dict]:
    return [{"request_id": f"r{i}", "scope_id": "s1", "subject": subject, "universe_kind": kind,
             "universe_origin": origin(trace, raw, 0), "view": None, "dataset": dataset_of(subject)}
            for i, (subject, kind) in enumerate(pairs, 1)]


def accepted_dict(gid: str, ir: dict) -> dict:
    """The base: the prior IR of the Gold as an ``m2a-accepted/1`` dict (origins are synthetic digests, never text)."""
    pairs = requests_of(ir)
    rids = [f"r{i}" for i in range(1, len(pairs) + 1)]
    conditions = [condition_dict(t, t["id"], rids, origin("t0", sha(f"{gid}|prior|{t['id']}"), 0))
                  for t in ir_conditions(ir)]
    return {"schema": "m2a-accepted/1", "requests": request_dicts(pairs, "t0", sha(f"{gid}|prior")),
            "conditions": conditions, "logic": None, "policies": [], "base_trace_id": None, "root_trace_id": "t0"}


# ---- typing the ops of a label ----------------------------------------------------------------------------------------
_OP_NAME = {"SET_UNIVERSE": "REPLACE_UNIVERSE"}
_OP_RANK = {"CLEAR": 0, "SET": 1, "SET_ORDER": 1, "REPLACE_UNIVERSE": 1, "KEEP": 2}


def _slot_of_directive(op: dict) -> str:
    """Slot key of a CLEAR/KEEP op. KEEP names a real slot. CLEAR names the whole dimension (contract §5.2) unless the
    Gold gives a polarity to a polarized dimension: then it is that polarity's slot, never the other one."""
    slot = op["slot"]
    if slot in ("VALUE.lower", "VALUE.upper"):
        return "VALUE/" + slot.partition(".")[2]
    if op["op"] == "KEEP":
        if slot in POLARIZED:
            return f"{slot}/{op['polarity']}"
        if slot == "VALUE":
            raise Untypeable("KEEP_DIMENSION_WIDE")
    if op["op"] == "CLEAR" and slot in POLARIZED and op.get("polarity") in ("POSITIVE", "NEGATIVE"):
        return f"{slot}/{op['polarity']}"
    return slot


def typed_ops(label: dict) -> list[dict]:
    """The label's ops as typed ops in a canonical order (a patch is an unordered atomic set; contract §5.1)."""
    res = label["resulting_ir"]
    res_conds = ir_conditions(res) if res else []
    typed = []
    for op in label["ops"]:
        kind = op["op"]
        if kind in ("ANSWER", "CANCEL_PENDING"):
            raise Untypeable("PENDING_OP")
        entry = {"op": _OP_NAME.get(kind, kind), "slot": None, "cond": None, "universe": None}
        if kind in ("CLEAR", "KEEP"):
            entry["slot"] = _slot_of_directive(op)
        elif kind == "SET_UNIVERSE":
            hint = fold(str(op["value_hint"] or ""))
            named = hint.split()[:1]
            subject = next((s for s in ("contracts", "processes") if named and named[0].startswith(s[:4])), None)
            kind_named = "opportunities" if re.search(r"oportunidad|opportunit", hint) else "records"
            if subject is None:
                raise Untypeable("UNIVERSE_TARGET")
            if res:  # an executed turn: the typed result must name the same universe the hint names
                pair = requests_of(res)
                if pair != [(subject, kind_named)]:
                    raise Untypeable("UNIVERSE_TARGET_KIND" if pair and pair[0][0] == subject else "UNIVERSE_TARGET")
            entry.update(slot="UNIVERSE", universe=[subject, kind_named])
        elif kind in ("SET", "SET_ORDER"):
            entry["cond"] = _typed_set_op(op, res, res_conds)
            entry["slot"] = entry["cond"]["slot"]
        else:
            raise Untypeable(f"OP:{kind}")
        typed.append(entry)
    typed.sort(key=lambda e: (_OP_RANK[e["op"]], e["slot"], e["op"]))
    return typed


def _typed_set_op(op: dict, res: dict | None, res_conds: list[dict]) -> dict:
    """The condition a SET/SET_ORDER op writes, typed from the op and CHECKED against the typed resulting IR."""
    dim = "VALUE" if op["slot"].startswith("VALUE") else op["slot"]
    if dim == "VALUE":
        side = "VALUE/" + op["slot"].partition(".")[2]
        on_side = [c for c in res_conds if c["slot"] == side]
        if len(on_side) != 1:  # the op carries no unit: the typed result must hold exactly one bound on that side
            raise Untypeable("VALUE_UNIT_FROM_RESULT")
        t = tcond("VALUE", op["operator"], op["polarity"], None, on_side[0]["unit"], op["value_hint"],
                  unit_source=(op["inherited"] or {}).get("unit_source"), year=ir_year(res))
    elif dim == "ORDER":
        ods = res["order"].get("direction_source") if res and res.get("order") else None
        t = tcond("ORDER", op["operator"], "POSITIVE", None, None, None, ods=ods)
    elif dim == "CARDINALITY":
        t = tcond("CARDINALITY", "top_n", "POSITIVE", None, None, None, n_requested=nfc(str(op["value_hint"])))
    else:
        if dim == "GEOGRAPHY" and op.get("level") is None:
            raise Untypeable("GEO_LEVEL_MISSING")
        t = tcond(dim, op["operator"], op["polarity"], op.get("level"), None, op["value_hint"],
                  year_source=(op["inherited"] or {}).get("year_source"))
    if res is not None and not any(sem_key(t) == sem_key(c) for c in res_conds if c["slot"] == t["slot"]):
        raise Untypeable("OP_DISAGREES_WITH_RESULT")
    return t


def fresh_ops(res: dict) -> list[dict]:
    """P of a fresh acceptance read as the patch it would be (a universe plus one SET per own condition), so that every
    case has the SAME input shape: the shape never says what §7.2 concludes."""
    pairs = requests_of(res)
    if len(pairs) != 1:
        raise Untypeable("FRESH_DUAL_UNIVERSE")
    ops = [{"op": "REPLACE_UNIVERSE", "slot": "UNIVERSE", "cond": None, "universe": list(pairs[0])}]
    ops += [{"op": "SET_ORDER" if t["dimension"] == "ORDER" else "SET", "slot": t["slot"], "cond": t, "universe": None}
            for t in ir_conditions(res)]
    return sorted(ops, key=lambda e: (_OP_RANK[e["op"]], e["slot"], e["op"]))


def patch_dict(raw: str, typed: list[dict], rids: list[str]) -> dict:
    """The ``m2a-patch/1`` dict of typed ops: the same shape for every case."""
    if not typed:
        raise Untypeable("EMPTY_PATCH")
    ops = []
    for i, e in enumerate(typed):
        org = origin("t1", raw, i)
        cond = None if e["cond"] is None else condition_dict(e["cond"], f"p{i + 1}", rids, org)
        universe = None if e["universe"] is None else {"subject": e["universe"][0], "universe_kind": e["universe"][1]}
        ops.append({"op": e["op"], "slot": e["slot"], "condition": cond, "universe": universe, "origin": org})
    return {"schema": "m2a-patch/1", "ops": ops, "origin_kind": "TEXT"}

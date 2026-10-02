"""M2A contract amendment R3: the representability sweep (INV-M2A-25, the major invariant).

User criterion (2026-10-02): «no existe una respuesta representable para un estado que sus propios tipos permiten
alcanzar». This module sweeps every invariant the M2a types impose on an effective and every response the contract
prescribes. The inventory is GENERATED from the code, never listed by hand:

* invariant codes: the AST of ``AcceptedInterpretation.__post_init__`` (plus the module helpers it calls) and of
  ``AcceptedRequest.__post_init__`` in ``app/core/foundation_accepted.py``;
* slots: ``SLOT_DIMENSIONS`` / ``POLARIZED_SLOT_DIMENSIONS`` / ``ValueSide`` plus UNIVERSE (contract §4.2);
* op × slot: every ``PatchOpKind`` × slot is CONSTRUCTED as a ``PatchOp`` (the type decides, not a table);
* responses: the ``DeltaRefusal`` list and its A.3 table are parsed from contract §6, the §7.1 row 4b motives from
  §7.1, annex A.1/A.3 from the annex; a CLARIFY always leaves a ``PendingClarification`` whose ``reason`` the type checks.

Two sides, kept apart on purpose:

* CONTRACT side (amended, phase B1): every row of ``SWEEP`` is answered by the contract; the pins read the contract
  text. No row is HUECO any more.
* TYPE side (``app/core/foundation_accepted.py``, exclusive owner M2A-3, phase 1c after the r3 re-seal): what the type
  still lacks is listed in ``TYPE_GAPS`` and runs as a STRICT xfail with its reason. It is a declared red, never a
  green: the day M2A-3 lands an item, its xfail turns into an XPASS that fails until the marker is removed.

A new invariant in the types fails ``test_every_type_invariant_is_swept`` until it is swept.
``python -m tests.test_m2a_contract_r3_sweep`` prints the tables.
"""
from __future__ import annotations

import ast
import hashlib
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.core.foundation_accepted as fa
from app.core.foundation_authority_values import (
    ActorValue, BuyerKind, BuyerValue, CardinalityBasis, CardinalityValue, Comparator, ContractTypeValue, GeoValue,
    ModalityValue, MoneyValue, OrderDirection, OrderKey, OrderValue, StateValue, TimeValue, TopicValue,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Operation, Subject
from app.core.interpretation_contract import (
    POLARIZED_SLOT_DIMENSIONS, SLOT_DIMENSIONS, ConditionExpression, ContractError, DirectiveKind,
    InterpretationProposal, LogicOperator, Polarity, ProposedCondition, ProposedJob, ProposedRequest,
    ProposedSlotDirective, RawSpan, ValueSide,
)
from app.core.semantic_dimensions import SemanticDimension as D

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md"
SOURCE = ROOT / "app/core/foundation_accepted.py"
RAW = "a" * 64

# ---- builders (real types; values as in tests/test_foundation_accepted_contract.py) -------------------------------
SAMPLE = {
    D.TOPIC: (TopicValue("aseo", False, False), TopicValue("aseo", False, True)),
    D.BUYER: (BuyerValue("ICBF", BuyerKind.UNIT, ("ICBF",), None, False),
              BuyerValue("ICBF", BuyerKind.UNIT, ("ICBF",), None, True)),
    D.ACTOR: (ActorValue("900123456", "NIT", "Acme", False), ActorValue("900123456", "NIT", "Acme", True)),
    D.GEOGRAPHY: (GeoValue(GeoLevel.DEPARTMENT, "VALLE", None, "76", (), False),
                  GeoValue(GeoLevel.DEPARTMENT, "VALLE", None, "76", (), True)),
    D.TIME: (TimeValue(date(2025, 1, 1), date(2026, 1, 1), "year", False),
             TimeValue(date(2025, 1, 1), date(2026, 1, 1), "year", True)),
    D.VALUE: (MoneyValue(500_000_000, Comparator.GTE), MoneyValue(900_000_000, Comparator.LTE)),
    D.STATE: (StateValue("open", (), False), StateValue("open", (), True)),
    D.MODALITY: (ModalityValue(("minima_cuantia",), None, False), ModalityValue(("minima_cuantia",), None, True)),
    D.CONTRACT_TYPE_OR_NATURE: (ContractTypeValue(("obra",), False), ContractTypeValue(("obra",), True)),
    D.ORDER: (OrderValue(OrderKey.VALUE, OrderDirection.DESC), OrderValue(OrderKey.DATE, OrderDirection.DESC)),
    D.CARDINALITY: (CardinalityValue(10, CardinalityBasis.FIGURE, 10),),
}
OPERATOR = {D.TOPIC: "contains", D.BUYER: "is", D.ACTOR: "is", D.GEOGRAPHY: "is", D.TIME: "during_year",
            D.STATE: "is", D.MODALITY: "is", D.CONTRACT_TYPE_OR_NATURE: "is", D.ORDER: "value_desc",
            D.CARDINALITY: "top_n"}


def _origin(start: int = 0) -> fa.ConditionOrigin:
    return fa.ConditionOrigin(fa.OriginKind.SPAN, "t-turn", RAW, (start, start + 1))


def cond(cid: str, dimension: D, value=None, *, rids=("r1",), start: int = 0) -> fa.AcceptedCondition:
    value = SAMPLE[dimension][0] if value is None else value
    encoded = value.encode()
    if dimension is D.VALUE:
        operator, polarity = fa.VALUE_OPERATOR_OF[value.comparator], Polarity.POSITIVE
    else:
        operator = "date_desc" if value == SAMPLE[D.ORDER][1] else OPERATOR[dimension]
        polarity = Polarity.NEGATIVE if getattr(value, "excluded", False) else Polarity.POSITIVE
    return fa.AcceptedCondition(cid, tuple(rids), fa.slot_of(dimension, encoded), dimension, operator, polarity,
                                value.level if dimension is D.GEOGRAPHY else None, encoded,
                                f"{dimension.value.lower()}@1", (), (), _origin(start), fa.Provenance())


def req(rid: str = "r1", subject: Subject = Subject.CONTRACTS, kind=fa.UniverseKind.RECORDS) -> fa.AcceptedRequest:
    return fa.AcceptedRequest(rid, "s1", subject, kind, _origin(), None, f"ds-{subject.value}")


DUAL = (req("r1", Subject.PROCESSES), req("r2", Subject.CONTRACTS))


def effective(conditions, requests=(None,), logic=None) -> fa.AcceptedInterpretation:
    requests = tuple(req() if r is None else r for r in requests)
    return fa.AcceptedInterpretation(requests, tuple(conditions), logic, (), None, "root")


def refusal(build) -> str | None:
    """The ``ContractError`` code a construction raises, or None when the type admits it."""
    try:
        build()
    except ContractError as error:
        return str(error)
    return None


def atom(cid: str) -> ConditionExpression:
    return ConditionExpression(LogicOperator.ATOM, cid)


def group(operator: LogicOperator, *children) -> ConditionExpression:
    return ConditionExpression(operator, None, tuple(children))


def proposal(raw: str, job: Operation, conditions: tuple[ProposedCondition, ...],
             requests: tuple[ProposedRequest, ...] | None = None) -> InterpretationProposal:
    """A wire (IR v2) proposal: what the producer may hand to M2a for P."""
    if requests is None:
        expression = atom(conditions[0].condition_id) if len(conditions) == 1 else group(
            LogicOperator.AND, *(atom(c.condition_id) for c in conditions))
        requests = (ProposedRequest("r1", "r1", Subject.UNSPECIFIED, (RawSpan(0, len(raw), raw),),
                                    condition_expression=expression),)
    return InterpretationProposal(hashlib.sha256(raw.encode()).hexdigest(), "sweep", "1", requests, conditions,
                                  (), (), (ProposedJob("j", job, tuple(r.request_id for r in requests),
                                                       (RawSpan(0, 1, raw[0]),)),))


def wire(cid: str, dimension: D, operator: str, quote: str, raw: str, *, rid: str = "r1",
         **extra) -> ProposedCondition:
    at = raw.index(quote)
    return ProposedCondition(cid, (rid,), dimension, operator, Polarity.POSITIVE, quote,
                             (RawSpan(at, at + len(quote), quote),), **extra)


def pending(reason: str, candidates=(), answer_slots=frozenset()) -> fa.PendingClarification:
    now = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
    return fa.PendingClarification("p1", "t-turn", now, now + timedelta(minutes=30), 1, None, None, None, None,
                                   reason, tuple(candidates), frozenset(answer_slots))


ORDER_SLOT, CARDINALITY_SLOT = fa.Slot(D.ORDER), fa.Slot(D.CARDINALITY)


def r31_candidate() -> fa.CandidatePatch:
    """Contract §6.7 (R3-1, D-1 option 1): C1 «quitar orden y límite»."""
    ops = (fa.PatchOp(fa.PatchOpKind.CLEAR, ORDER_SLOT, None, None, _origin(0)),
           fa.PatchOp(fa.PatchOpKind.CLEAR, CARDINALITY_SLOT, None, None, _origin(2)))
    return fa.CandidatePatch("k1", "remove_order_and_limit", fa.SemanticPatch(ops, fa.PatchOriginKind.TEXT))


def r31_set_order_candidate(value: OrderValue, cid: str = "k2") -> fa.CandidatePatch:
    """Contract §6.7 (R3-1, D-1 option 1): one ``SET_ORDER`` per alternative order criterion (resolved at ask time)."""
    condition = cond(f"o-{cid}", D.ORDER, value)
    op = fa.PatchOp(fa.PatchOpKind.SET_ORDER, ORDER_SLOT, condition, None, condition.origin)
    return fa.CandidatePatch(cid, f"order_{value.key.value}", fa.SemanticPatch((op,), fa.PatchOriginKind.TEXT))


def r31_candidates(*, provisional: bool = False) -> tuple[fa.CandidatePatch, ...]:
    """Anchor ordered by VALUE desc on CONTRACTS: the alternative key is DATE (desc, as the authority defaults it).
    Without E-ORDER (provisional, §6.7/§19) the pending carries C1 only."""
    return (r31_candidate(),) if provisional else (r31_candidate(), r31_set_order_candidate(SAMPLE[D.ORDER][1]))


def selects(candidates: tuple[fa.CandidatePatch, ...], answer: fa.AcceptedCondition) -> int:
    """§8.3 row 4 (sealed text): how many candidates the answer selects, comparing by encoded value."""
    return sum(any(op.condition is not None and op.slot == answer.slot and op.condition.value == answer.value
                   for op in candidate.patch.ops) for candidate in candidates)


# ---- generated inventory -------------------------------------------------------------------------------------------
_SHAPE_HELPERS = frozenset({"_fail", "_records", "_text", "_opt_text", "_instance", "_texts", "_sha", "_int",
                            "_utc", "_flag", "_opt", "_keys", "_schema", "_list", "_enum_of"})


def _single_slot_keys() -> list[str]:
    keys = []
    for dimension in sorted(fa._SINGLE_SLOT, key=lambda d: d.value):
        slots = ([fa.Slot(dimension, side=s) for s in ValueSide] if dimension is D.VALUE else [fa.Slot(dimension)])
        keys += [slot.key.replace("/", "_").upper() for slot in slots]
    return keys


def invariant_codes() -> dict[str, str]:
    """``{code: constructor}`` for every semantic ``_fail`` code an effective can hit (``INVALID_<label>`` shape
    guards of the generic helpers are excluded: a well-typed ``compose`` input never reaches them)."""
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}

    def codes(function: ast.FunctionDef, seen: set[str]) -> set[str]:
        out: set[str] = set()
        for node in ast.walk(function):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
                continue
            name, args = node.func.id, node.args
            if name == "_fail" and args and isinstance(args[0], ast.Constant):
                out.add(args[0].value)
            elif name == "_fail" and args and isinstance(args[0], ast.JoinedStr):
                head = "".join(v.value for v in args[0].values if isinstance(v, ast.Constant))
                out |= {head + key for key in _single_slot_keys()} if head == "TWO_CONDITIONS_IN_" else {head}
            elif name == "_records" and any(k.arg == "empty" for k in node.keywords):
                out.add(f"EMPTY_{args[2].value}")
            elif name in functions and name not in _SHAPE_HELPERS and name not in seen:
                seen.add(name)
                out |= codes(functions[name], seen)
        return out

    out = {}
    for owner in ("AcceptedInterpretation", "AcceptedRequest"):
        init = next(m for m in classes[owner].body if isinstance(m, ast.FunctionDef) and m.name == "__post_init__")
        out.update({code: owner for code in codes(init, set())})
    return out


def all_slots() -> list[fa.Slot]:
    slots = []
    for dimension in sorted(SLOT_DIMENSIONS, key=lambda d: d.value):
        if dimension in POLARIZED_SLOT_DIMENSIONS:
            slots += [fa.Slot(dimension, p) for p in Polarity] + [fa.Slot(dimension)]
        elif dimension is D.VALUE:
            slots += [fa.Slot(dimension, side=s) for s in ValueSide] + [fa.Slot(dimension)]
        else:
            slots.append(fa.Slot(dimension))
    return slots + [fa.UNIVERSE_SLOT]


def _condition_for(slot: fa.Slot) -> fa.AcceptedCondition | None:
    if slot.is_universe:
        return None
    if slot.dimension is D.VALUE:
        return cond("c", D.VALUE, SAMPLE[D.VALUE][1 if slot.side is ValueSide.UPPER else 0])
    negative = slot.polarity is Polarity.NEGATIVE and len(SAMPLE[slot.dimension]) > 1
    return cond("c", slot.dimension, SAMPLE[slot.dimension][1 if negative else 0])


def op_slot_matrix() -> dict[tuple[str, str], str]:
    """``{(op, slot key): "OK" | ContractError code}`` by constructing each ``PatchOp``."""
    out = {}
    for op in fa.PatchOpKind:
        for slot in all_slots():
            writes = op in (fa.PatchOpKind.SET, fa.PatchOpKind.SET_ORDER)
            condition = _condition_for(slot) if writes else None
            origin = condition.origin if condition is not None else _origin()
            universe = (fa.UniverseTarget(Subject.PROCESSES, fa.UniverseKind.RECORDS)
                        if op is fa.PatchOpKind.REPLACE_UNIVERSE else None)
            out[(op.value, slot.key)] = refusal(lambda: fa.PatchOp(op, slot, condition, universe, origin)) or "OK"
    return out


# ---- the amended contract, read from its text -----------------------------------------------------------------------
def _contract() -> str:
    return CONTRACT.read_text(encoding="utf-8")


def delta_refusal_codes() -> list[str]:
    """The closed ``DeltaRefusal`` list as contract §6 writes it."""
    block = re.search(r"Códigos de rechazo \(`DeltaRefusal`, cerrados\):(.*?)\.\s", _contract(), re.S).group(1)
    return re.findall(r"`([^`]+)`", block)


def refusal_table() -> dict[str, list[str]]:
    """Contract §6 (R3-2): ``DeltaRefusal`` → the A.3 codes it publishes. «cualquier otro» lists its codes itself."""
    block = _contract().split("| `DeltaRefusal` | `CLARIFY` (A.3) |", 1)[1].split("\n\n", 1)[0]
    code = re.compile(r"`([A-Z_]+(?::<[a-zA-Z]+>)?)`")  # an A.3 code; a bare `<DIM>` in a comment is not one
    out: dict[str, list[str]] = {}
    for line in block.splitlines()[2:]:
        left, right = line.strip("|").split("|", 1)
        sources, targets = code.findall(left), code.findall(right)
        if sources:
            out.update({source: targets for source in sources})
        else:  # «cualquier otro (estructural, sin autoridad)»: each listed code publishes itself
            out.update({target: [target] for target in targets})
    return out


def annex_a3() -> dict[str, list[str]]:
    text = _contract().split("**A.3 Códigos de razón:**", 1)[1].split("Retirados:", 1)[0]
    return {m.group(1): re.findall(r"`([^`]+)`", m.group(2))
            for m in re.finditer(r"^\| `([A-Z_]+)` \| (.*) \|$", text, re.M)}


def annex_a1() -> list[str]:
    text = _contract().split("**A.1 `TurnScope`:**", 1)[1].split("**A.2", 1)[0]
    return re.findall(r"`([^`]+)`", text)


def row_4b_motives() -> dict[str, str]:
    """§7.1 fila 4b (D-6): ``{motivo: dueño}`` of the closed list."""
    block = _contract().split("| Motivo (cerrado) | P… | Dueño |", 1)[1].split("\n\n", 1)[0]
    return {re.search(r"`([A-Z_]+)`", cells[0]).group(1): cells[2].strip()
            for cells in (line.strip("|").split("|") for line in block.splitlines()[2:])}


def dim_bullet() -> str:
    """A.3 «Parámetros (R3)»: the bullet that defines the domain of ``<DIM>`` (and only that bullet)."""
    params = _contract().split("Parámetros (R3):", 1)[1].split("\n\n", 2)[1]
    return next(item for item in params.split("\n- ") if item.lstrip("- ").startswith("`<DIM>`"))


def dim_domain() -> set[str]:
    """A.3 (R3-3): the domain of ``<DIM>`` as the contract states it: the slot dimensions plus UNIVERSE, never a
    dimension that is not a slot."""
    bullet = dim_bullet()
    named = set(re.findall(r"`([A-Z_]+)`", bullet.split("Nunca", 1)[0]))
    excluded = set(re.findall(r"`([A-Z_]+)`", bullet.split("Nunca", 1)[1].split(".", 1)[0]))
    assert "once dimensiones de ranura" in bullet and excluded == {"UNSUPPORTED", "RETRIEVAL_INTENT",
                                                                     "CONTROL_LANGUAGE"}
    return ({d.value for d in SLOT_DIMENSIONS} | named) - excluded


# ---- the sweep (verdicts are analysis; rows are tied to generated codes, ops and probes) ---------------------------
@dataclass(frozen=True)
class Row:
    rid: str
    codes: tuple[str, ...]  # type-invariant codes the row answers for (empty: a type-valid state)
    state: str
    ops: str  # which op × slot can drive it
    reachable: str
    response: str  # the contract's answer after R3
    verdict: str  # CUBIERTO | DEPENDENCIA | NO_ALCANZABLE (HUECO: none left on the contract side)
    evidence: str
    proposal: str = ""
    type_gaps: tuple[str, ...] = ()  # TYPE_GAPS ids the type still lacks (declared red until M2A-3 1c)


BRANCH_M2A3 = "rama M2A-3 30d073a4..f30e91ff, no en la base fa0b6de8"
# Codes present on only ONE of the two lines that meet at integration (base fa0b6de8 / M2A-3 branch): a swept code may
# be absent from the source under test only if it is listed here. Every other swept code must exist in the types.
BASE_ONLY = frozenset({"UNSORTED_POLICY_CONDITION_IDS"})  # replaced by MULTI_CONDITION_POLICY in 0de2b9a2
MOTIVES = ("MULTI_REQUEST", "UNSUPPORTED_CONDITION", "UNSUPPORTED_SOURCE", "INVALID_PROPOSAL", "NO_ACTION")
SWEEP = (
    Row("T01", ("EMPTY_REQUESTS", "DUPLICATE_REQUEST_ID", "TOO_MANY_REQUESTS", "INVALID_DUAL_ANCHOR"),
        "efectiva sin peticiones, con ids repetidos, >2 o dual inválida", "REPLACE_UNIVERSE/UNIVERSE", "NO",
        "—", "NO_ALCANZABLE",
        "UniverseTarget es un único (sujeto, clase) y §5.2 deja la petición nombrada; ningún op crea ni quita "
        "peticiones (PatchOp no tiene campo de petición)"),
    Row("T02", ("DUAL_ANCHOR_CONDITIONS_DIFFER",), "ancla dual con una condición nueva en una sola petición",
        "SET, SET_ORDER sobre ancla dual", "sólo si compose acotara la condición", "—", "CUBIERTO",
        "§5.1: PatchOp y Slot no llevan petición; compose da a la condición nueva todas las peticiones de la base "
        "(sonda P-dual-set). Una P que reparte condiciones por petición es MULTI: §7.1 fila 4b (S07)"),
    Row("T03", ("FOREIGN_CONDITION_REQUEST",), "dual → una petición sin estrechar request_ids",
        "REPLACE_UNIVERSE con ancla dual", "SÍ, si compose no estrecha", "EXECUTE + UNIVERSE_CHANGED", "CUBIERTO",
        "§6.3 declara la excepción de request_ids (sondas P-dual-replace y P-dual-replace-narrowed)", "P-1"),
    Row("T04", ("DUPLICATE_CONDITION_ID", "LOGIC_ATOMS_MISMATCH", "UNSORTED_POLICY_CONDITION_IDS"),
        "ids, átomos de lógica y forma canónica", "todos", "sólo por defecto de construcción", "—", "CUBIERTO",
        "obligación de construcción de compose (§4.3 determinismo; forma canónica de 1a.1)"),
    Row("T05", ("TWO_CONDITIONS_IN_ORDER",), "dos ORDER en una petición", "SET_ORDER/ORDER", "NO", "—",
        "NO_ALCANZABLE",
        "SET_ORDER sustituye; dos SET_ORDER en un parche no caben: la P los rechaza (IR v2 MULTIPLE_ORDER) y un "
        "evento lleva una op; si ocurriera, DELTA_SLOT_CONFLICT (R01)"),
    Row("T06", ("TWO_CONDITIONS_IN_CARDINALITY",), "dos CARDINALITY en una petición", "SET/CARDINALITY", "NO",
        "—", "NO_ALCANZABLE", "SET sustituye; IR v2 MULTIPLE_CARDINALITY rechaza la P"),
    Row("T07", ("TWO_CONDITIONS_IN_VALUE_LOWER", "TWO_CONDITIONS_IN_VALUE_UPPER"),
        "dos cotas VALUE del mismo lado", "SET/VALUE.lower ×2 o SET/VALUE.upper ×2 en un parche",
        "SÍ (la P con dos cotas del mismo lado es IR v2 válida: sonda P-two-lower-wire)",
        "DELTA_SLOT_CONFLICT → CLARIFY DELTA_SLOT_CONFLICT", "CUBIERTO", "§6.2 y tabla de §6", "R3-2",
        ("A3-DELTA_SLOT_CONFLICT",)),
    Row("T08", ("CARDINALITY_WITHOUT_ORDER",), "(a) CLEAR(ORDER) con CARDINALITY arrastrada",
        "CLEAR/ORDER (directiva CLEAR del wire 5.1 o evento «Quitar»)", "SÍ (sonda P-a)",
        "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER → CLARIFY homónimo; pendiente de D-1", "CUBIERTO",
        "§6.7 decidido sobre el estado; controles P-a-SF023 y P-a-set-order no disparan", "R3-1",
        ("A3-DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER", "PENDING-R3-1")),
    Row("T09", ("CARDINALITY_WITHOUT_ORDER",), "(b) SET CARDINALITY sin ORDER en base ni SET_ORDER",
        "SET/CARDINALITY", "NO hoy: la P es IR v2 inválida (sonda P-b-wire); ningún evento fija CARDINALITY",
        "si llega a ser alcanzable: el mismo estado de (a), §6.7", "NO_ALCANZABLE",
        "§6.7 se decide sobre el estado (D-2): cubre esta vía sin otro re-sellado", "R3-1"),
    Row("T10", ("NONCONJUNCTIVE_ORDER",), "ORDER dentro de OR/NOT", "SET_ORDER; CLEAR/SET/KEEP sobre átomos de un OR/NOT",
        "SÍ (objetivos dentro de OR/NOT)", "TARGET_NOT_ADDRESSABLE → CLARIFY TARGET_NOT_ADDRESSABLE", "CUBIERTO",
        "§4.2; SET_ORDER añade un átomo del AND de primer nivel (sonda P-order-or)"),
    Row("T11", ("FOREIGN_POLICY_CONDITION",), f"política que cita una condición quitada ({BRANCH_M2A3})",
        "CLEAR/SET de una condición citada", "SÍ",
        "OP_NOT_ACCREDITED:<DIM> → UNRESOLVED_VALUE:<DIM>", "CUBIERTO",
        "§6.6: la política se va con su condición o se re-decide; sin dueño, fail-closed con la dimensión de la "
        "condición citada. Excepción O1B: cita RETRIEVAL_INTENT (no es ranura) → S01/S02"),
    Row("T12", ("CONFLICTING_POLICY_EFFECT", "DUPLICATE_POLICY", "UNSORTED_POLICIES", "NONCANONICAL_LOGIC",
                "MULTI_CONDITION_POLICY", "NONCANONICAL_CONDITION_ORDER", "SLOT_WITHOUT_RANK"),
        f"forma canónica de políticas, condiciones y lógica ({BRANCH_M2A3})", "todos",
        "sólo por defecto de construcción", "—", "CUBIERTO",
        "canonical_interpretation es el único constructor (M2A3_CANONICAL_FORM.md, decisiones D-CANON-1/2 del "
        "usuario); main emite políticas de una condición y compose nunca las fusiona"),
    Row("T13", ("VIEW_TYPE_UNAVAILABLE", "VIEW_REQUIRED"), "universo no dicho / vista derivada (F9)",
        "REPLACE_UNIVERSE; recálculo de vista", "NO hoy (UniverseTarget prohíbe unspecified; la base no puede serlo)",
        "—", "DEPENDENCIA", "ola F (HostViewDecision), declarada en §19 «vista derivada»"),
    Row("S01", (), "REPLACE_UNIVERSE a oportunidades (SF075): la CAPACIDAD", "REPLACE_UNIVERSE/UNIVERSE", "SÍ",
        "rojo declarado (§19)", "DEPENDENCIA",
        "O1B/opportunity-explicit-state no representables y TIME explícito sobre oportunidades "
        "(foundation_projection.py:235-237); dueño FOUNDATION_O1B_EXPLICIT_TIME_PARITY (+ M2A-1b)", "R3-3"),
    Row("S02", (), "REPLACE_UNIVERSE a oportunidades: el RECHAZO fail-closed", "REPLACE_UNIVERSE/UNIVERSE",
        "SÍ (el tipo admite la efectiva: sonda P-c)", "OP_NOT_ACCREDITED:UNIVERSE → CLARIFY UNRESOLVED_VALUE:UNIVERSE",
        "CUBIERTO", "A.3: <DIM> recorre las claves de dimensión de las ranuras de §4.2, incluida UNIVERSE; §19",
        "R3-3", ("DIM-UNIVERSE",)),
    Row("S03", (), "primer turno o búsqueda nueva que pregunta por conflicto de universo (H7)", "— (P sin base)",
        "SÍ (foundation_runtime.py:290-292 HOST_UNIVERSE_RULE_CONFLICT)", "→ CLARIFY UNRESOLVED_VALUE:UNIVERSE",
        "CUBIERTO", "A.3 (R3-3); la reducción M1 → UNRESOLVED_VALUE:<DIM> es convención del esquema Gold 2.1 (O-2)",
        "R3-3", ("DIM-UNIVERSE",)),
    Row("S04", (), "F16 fail-closed: negativo sobre varios positivos o valor contenido",
        "SET/<DIM>/NEGATIVE con ≥2 positivos en la ranura", "SÍ (el tipo admite la efectiva: sonda P-f16)",
        "OP_NOT_ACCREDITED:<DIM> → CLARIFY UNRESOLVED_VALUE:<DIM>", "CUBIERTO", "§6.5 F16 (D-3: sin código nuevo)",
        "R3-4"),
    Row("S05", (), "REPLACE_UNIVERSE con STATE/MODALITY arrastrado inexistente en el destino", "REPLACE_UNIVERSE",
        "SÍ", "H8 → STATE_NOT_IN_UNIVERSE", "CUBIERTO", "§6 H8 (SF079); MODALITY depende de E-MODALITY con el mismo código"),
    Row("S06", (), "CLEAR que deja la efectiva sin condiciones / CLEAR de ranura vacía", "CLEAR", "SÍ",
        "H11 EMPTY_SEARCH_AFTER_DELTA / H12 INFORM CLEAR_ON_EMPTY_SLOT", "CUBIERTO",
        "con otra op, el CLEAR vacío no cambia nada (OQ-5 X8, tensión viva declarada, no hueco de representación)"),
    Row("S07", (), "P no admitida con ancla o pendiente vigente (MULTI, condición UNSUPPORTED libre, otra fuente, "
        "propuesta inválida, sin acción)", "— (antes de componer)",
        "SÍ (sondas P-d6-multi-wire y P-d6-unsupported-wire: la P es IR v2 válida)",
        "OUT_UNSUPPORTED_PROPOSAL → INFORM UNSUPPORTED_PROPOSAL:<motivo>", "CUBIERTO",
        "§7.1 fila 4b, lista cerrada con dueño por motivo (D-6, marcada para revisión: Anexo D.2)", "D-6",
        ("SCOPE-OUT_UNSUPPORTED_PROPOSAL", "SCOPE-RESOLUTION-INFORM", *(f"A3-UNSUPPORTED_PROPOSAL:{m}" for m in MOTIVES))),
    Row("S08", (), "pendiente de R3-1 (D-1 opción 1)", "— (pendiente)", "SÍ (consecuencia de T08)",
        "C1 «quitar orden y límite» + un SET_ORDER por criterio alternativo (sin el vigente); answer_slots = "
        "{ORDER, CARDINALITY}; «déjalo como estaba» = CANCEL_PENDING; sin E-ORDER, sólo C1", "CUBIERTO",
        "§6.7 y §19; §8.3 fila 4 sellada: «por fecha» selecciona uno, «dame 20» ninguno (fila 7); sondas "
        "P-d1-option1/P-d1-provisional; {ORDER} literal = CANDIDATE_OUTSIDE_ANSWER_SLOTS (P-d1-literal)", "R3-1",
        ("PENDING-R3-1",)),
    Row("R01", (), "rechazo DELTA_SLOT_CONFLICT (dos SET en una ranura; KEEP+CLEAR; dos REPLACE_UNIVERSE)",
        "SET×2, KEEP+CLEAR, REPLACE_UNIVERSE×2", "SÍ", "→ CLARIFY DELTA_SLOT_CONFLICT", "CUBIERTO",
        "tabla de §6; incluye la unión de valores de G-FU-09 (§19)", "R3-2", ("A3-DELTA_SLOT_CONFLICT",)),
    Row("R02", (), "rechazo KEEP_ON_EMPTY_SLOT", "KEEP", "SÍ (directiva KEEP del wire 5.1: sonda P-keep)",
        "→ CLARIFY KEEP_ON_EMPTY_SLOT", "CUBIERTO", "tabla de §6", "R3-2", ("A3-KEEP_ON_EMPTY_SLOT",)),
    Row("R03", (), "rechazo TARGET_NOT_ADDRESSABLE", "CLEAR/SET/KEEP", "SÍ", "→ CLARIFY TARGET_NOT_ADDRESSABLE",
        "CUBIERTO", "homónimo en A.3"),
    Row("R04", (), "rechazo OP_NOT_ACCREDITED:<DIM> sobre una dimensión de ranura", "SET/SET_ORDER", "SÍ",
        "→ CLARIFY UNRESOLVED_VALUE:<DIM>", "CUBIERTO", "11 dimensiones de ranura admitidas por reason_code_allowed"),
    Row("R05", (), "rechazo BASE_INVALID", "todos", "defensivo (§7.1 fila 1 decide antes)",
        "→ CLARIFY NO_ANCHOR / ANCHOR_UNREADABLE", "CUBIERTO", "tabla de §6; §3.8 y §7.1 fila 1"),
    Row("F01", (), "G-FU-05 paginar ancla dual / página frente a top-N", "evento de página (M2A-6)", "SÍ (M2A-6)",
        "CLARIFY, código propio diferido al dueño; forma representable: UNRESOLVED_VALUE:<DIM>", "DEPENDENCIA",
        "§19 declara «Salida: regla y código de razón»; SF148 (FRONTIER) usa UNRESOLVED_VALUE:UNIVERSE"),
    Row("F02", (), "G-FU-07 territorio contenido / comprador nacional", "SET GEOGRAPHY/BUYER", "sólo con E-GEO/E-BUYER",
        "código de la autoridad (UNRESOLVED_VALUE:<GEOGRAPHY|BUYER>)", "DEPENDENCIA", "detección aguas abajo"),
    Row("F03", (), "G-FU-04 filas/página sin result_set_ref", "protocolo", "SÍ (M2A-6)",
        "INFORM, código diferido (A.3 lo declara)", "CUBIERTO",
        "INFORM no deja pendiente ni se persiste su razón; el gate es «INFORM y 0 llamadas» (§9.1)"),
)

PROBE_EXPECTED = {
    "P-a": "CARDINALITY_WITHOUT_ORDER", "P-a-SF023": None, "P-a-set-order": None,
    "P-b": "CARDINALITY_WITHOUT_ORDER", "P-b-wire": "CARDINALITY_WITHOUT_ORDER", "P-b-wire-rank": None,
    "P-two-lower": "TWO_CONDITIONS_IN_VALUE_LOWER", "P-two-lower-wire": None,
    "P-dual-set": "DUAL_ANCHOR_CONDITIONS_DIFFER", "P-dual-replace": "FOREIGN_CONDITION_REQUEST",
    "P-dual-replace-narrowed": None, "P-order-or": "NONCONJUNCTIVE_ORDER", "P-order-and": None,
    "P-c": None, "P-c-target": None, "P-f16": None, "P-keep": None, "P-clear-order": None,
    "P-d1-literal": "CANDIDATE_OUTSIDE_ANSWER_SLOTS", "P-d1-option1": None, "P-d1-provisional": None,
    "P-d6-multi-wire": None, "P-d6-unsupported-wire": None,
}


def probes() -> dict[str, str | None]:
    topic, order, card = cond("c1", D.TOPIC), cond("c2", D.ORDER), cond("c3", D.CARDINALITY)
    lower = cond("c4", D.VALUE, SAMPLE[D.VALUE][0])
    lower2 = cond("c5", D.VALUE, MoneyValue(800_000_000, Comparator.GT))
    shared = cond("c1", D.TOPIC, rids=("r1", "r2"))
    raw_b, raw_v = "dame solo 10", "mayor a 50 millones y mayor a 80 millones"
    raw_m = "procesos de aseo y contratos de vigilancia"
    cafe = cond("c6", D.TOPIC, TopicValue("cafeteria", False, False))
    multi = (ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (RawSpan(0, 17, raw_m[:17]),),
                             condition_expression=atom("t1")),
             ProposedRequest("r2", "s2", Subject.UNSPECIFIED, (RawSpan(18, len(raw_m), raw_m[18:]),),
                             condition_expression=atom("t2")))
    return {
        "P-a": refusal(lambda: effective((topic, card))),  # base {c1,c2,c3} − CLEAR(ORDER), c3 carried as is
        "P-a-SF023": refusal(lambda: effective((topic,))),  # CLEAR(ORDER) + CLEAR(CARDINALITY)
        "P-a-set-order": refusal(lambda: effective((topic, cond("c7", D.ORDER, SAMPLE[D.ORDER][1]), card))),
        "P-b": refusal(lambda: effective((topic, card))),  # base {c1} + SET CARDINALITY
        "P-b-wire": refusal(lambda: proposal(raw_b, Operation.FOLLOWUP_DELTA,
                                             (wire("n", D.CARDINALITY, "top_n", "10", raw_b),))),
        "P-b-wire-rank": refusal(lambda: proposal(raw_b, Operation.RANK_SUPPLIERS,
                                                  (wire("n", D.CARDINALITY, "top_n", "10", raw_b),))),
        "P-two-lower": refusal(lambda: effective((topic, lower, lower2))),
        "P-two-lower-wire": refusal(lambda: proposal(raw_v, Operation.FOLLOWUP_DELTA, (
            wire("v1", D.VALUE, "greater_than", "mayor a 50 millones", raw_v),
            wire("v2", D.VALUE, "greater_than", "mayor a 80 millones", raw_v)))),
        "P-dual-set": refusal(lambda: effective((shared, cond("c8", D.TIME, rids=("r1",))), DUAL)),
        "P-dual-replace": refusal(lambda: effective((shared,), (DUAL[1],))),
        "P-dual-replace-narrowed": refusal(lambda: effective((cond("c1", D.TOPIC, rids=("r2",)),), (DUAL[1],))),
        "P-order-or": refusal(lambda: effective((topic, order), logic=group(LogicOperator.OR, atom("c1"),
                                                                             atom("c2")))),
        "P-order-and": refusal(lambda: effective((topic, cafe, order), logic=group(
            LogicOperator.AND, group(LogicOperator.OR, atom("c1"), atom("c6")), atom("c2")))),
        "P-c": refusal(lambda: effective((topic,), (req("r1", Subject.PROCESSES, fa.UniverseKind.OPPORTUNITIES),))),
        "P-c-target": refusal(lambda: fa.UniverseTarget(Subject.PROCESSES, fa.UniverseKind.OPPORTUNITIES)),
        "P-f16": refusal(lambda: effective((topic, cafe, cond("c9", D.TOPIC, SAMPLE[D.TOPIC][1])))),
        "P-keep": refusal(lambda: ProposedSlotDirective("d1", DirectiveKind.KEEP, (RawSpan(0, 1, "x"),), D.BUYER,
                                                        Polarity.POSITIVE)),
        "P-clear-order": refusal(lambda: ProposedSlotDirective("d2", DirectiveKind.CLEAR, (RawSpan(0, 1, "x"),),
                                                               D.ORDER)),
        # D-1: the answer_slots check is isolated with a reason the type already admits.
        "P-d1-literal": refusal(lambda: pending("TARGET_NOT_ADDRESSABLE", (r31_candidate(),), {ORDER_SLOT})),
        "P-d1-option1": refusal(lambda: pending("TARGET_NOT_ADDRESSABLE", r31_candidates(),
                                                {ORDER_SLOT, CARDINALITY_SLOT})),
        "P-d1-provisional": refusal(lambda: pending("TARGET_NOT_ADDRESSABLE", r31_candidates(provisional=True),
                                                    {ORDER_SLOT, CARDINALITY_SLOT})),
        # D-6: the non-admitted proposals are valid wire records, so they reach M2a with an anchor.
        "P-d6-multi-wire": refusal(lambda: proposal(raw_m, Operation.FOLLOWUP_DELTA, (
            wire("t1", D.TOPIC, "contains", "aseo", raw_m, rid="r1"),
            wire("t2", D.TOPIC, "contains", "vigilancia", raw_m, rid="r2")), multi)),
        "P-d6-unsupported-wire": refusal(lambda: proposal(raw_b, Operation.FOLLOWUP_DELTA, (
            wire("u", D.UNSUPPORTED, "is", "solo", raw_b, unsupported_label="exclusive_scope"),))),
    }


def representable(code: str) -> bool:
    return refusal(lambda: pending(code)) is None


# Facts of the type that hold now AND must keep holding after M2A-3 1c (they are not gaps).
TYPE_ADMITS = {
    "TARGET_NOT_ADDRESSABLE": True, "NO_ANCHOR": True, "ANCHOR_UNREADABLE": True,
    "STATE_NOT_IN_UNIVERSE": True, "EMPTY_SEARCH_AFTER_DELTA": True, "ANSWER_UNRESOLVED": True,
    **{f"UNRESOLVED_VALUE:{d.value}": True for d in sorted(SLOT_DIMENSIONS, key=lambda d: d.value)},
    **{f"UNRESOLVED_VALUE:{d.value}": False for d in (D.RETRIEVAL_INTENT, D.UNSUPPORTED, D.CONTROL_LANGUAGE)},
    "UNRESOLVED_VALUE:WHATEVER": False, "DELTA_CONTRADICTS_CARRIED": False,
}

# What the amended contract requires and the type (app/core/foundation_accepted.py) still lacks. Exclusive owner:
# M2A-3, phase 1c, after the r3 re-seal. Each item is a STRICT xfail: declared red, never a false green.
TYPE_GAPS = {
    "A3-DELTA_SLOT_CONFLICT": ("CLARIFY_CODES sin DELTA_SLOT_CONFLICT (R3-2)",
                               lambda: representable("DELTA_SLOT_CONFLICT")),
    "A3-KEEP_ON_EMPTY_SLOT": ("CLARIFY_CODES sin KEEP_ON_EMPTY_SLOT (R3-2)",
                              lambda: representable("KEEP_ON_EMPTY_SLOT")),
    "A3-DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER": (
        "CLARIFY_CODES sin DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER (R3-1)",
        lambda: representable("DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER")),
    "DIM-UNIVERSE": ("reason_code_allowed: <DIM> sin UNIVERSE (R3-3)",
                     lambda: representable("UNRESOLVED_VALUE:UNIVERSE")),
    "PENDING-R3-1": ("la pendiente de R3-1 (C1 + SET_ORDER alternativos, answer_slots {ORDER, CARDINALITY}) no se "
                     "construye sin su código (R3-1, D-1 opción 1)",
                     lambda: refusal(lambda: pending("DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER", r31_candidates(),
                                                     {ORDER_SLOT, CARDINALITY_SLOT})) is None),
    "SCOPE-OUT_UNSUPPORTED_PROPOSAL": ("TurnScope sin OUT_UNSUPPORTED_PROPOSAL (D-6)",
                                       lambda: "OUT_UNSUPPORTED_PROPOSAL" in {s.value for s in fa.TurnScope}),
    "SCOPE-RESOLUTION-INFORM": ("OUT_SCOPE_RESOLUTION sin OUT_UNSUPPORTED_PROPOSAL → INFORM (D-6)",
                                lambda: any(s.value == "OUT_UNSUPPORTED_PROPOSAL" and r is fa.TurnResolution.INFORM
                                            for s, r in fa.OUT_SCOPE_RESOLUTION.items())),
    **{f"A3-UNSUPPORTED_PROPOSAL:{m}": (
        f"reason_code_allowed(INFORM) sin UNSUPPORTED_PROPOSAL:{m} (D-6)",
        lambda m=m: fa.reason_code_allowed(f"UNSUPPORTED_PROPOSAL:{m}", fa.TurnResolution.INFORM))
       for m in MOTIVES},
    "A3-CONTRACT-EQUALS-TYPE": ("A.3/A.1 del contrato enmendado ≠ constantes del tipo",
                                lambda: _contract_equals_type()),
}


def _contract_equals_type() -> bool:
    annex = annex_a3()
    return (set(annex["CLARIFY"]) - {"UNRESOLVED_VALUE:<DIM>"} == fa.CLARIFY_CODES
            and set(annex["INFORM"]) - {"OPERATION_CHANGE:<op>", "UNSUPPORTED_PROPOSAL:<motivo>"} == fa.INFORM_CODES
            and set(annex_a1()) == {s.value for s in fa.TurnScope})


# ---- pins: generated inventory --------------------------------------------------------------------------------------
def test_every_type_invariant_is_swept():
    swept = {code for row in SWEEP for code in row.codes}
    generated = invariant_codes()
    assert set(generated) <= swept, f"unswept invariants: {sorted(set(generated) - swept)}"
    branch_only = {code for row in SWEEP if BRANCH_M2A3 in row.state for code in row.codes}
    assert swept - set(generated) <= branch_only | BASE_ONLY  # only a one-line code may be absent


def test_the_op_slot_matrix_is_the_one_swept():
    matrix = op_slot_matrix()
    admitted = Counter(op for (op, _), result in matrix.items() if result == "OK")
    assert len(all_slots()) == 30
    assert admitted == {"SET": 19, "CLEAR": 29, "KEEP": 21, "SET_ORDER": 1, "REPLACE_UNIVERSE": 1}
    assert matrix[("SET", "ORDER")] == "ORDER_NEEDS_SET_ORDER"
    assert matrix[("CLEAR", "UNIVERSE")] == "UNIVERSE_NEEDS_REPLACE_UNIVERSE"
    assert matrix[("KEEP", "TOPIC")] == "DIMENSION_WIDE_SLOT_OUTSIDE_CLEAR"


def test_each_probe_reaches_exactly_the_swept_state():
    assert probes() == PROBE_EXPECTED


# ---- pins: the amended contract (INV-M2A-25, contract side) ---------------------------------------------------------
def test_no_row_is_left_without_a_contract_answer():
    assert all(row.verdict in {"CUBIERTO", "DEPENDENCIA", "NO_ALCANZABLE"} for row in SWEEP)
    assert len({row.rid for row in SWEEP}) == len(SWEEP)
    referenced = {gap for row in SWEEP for gap in row.type_gaps}
    assert referenced <= set(TYPE_GAPS) and set(TYPE_GAPS) - referenced == {"A3-CONTRACT-EQUALS-TYPE"}


def test_every_refusal_publishes_an_a3_code_in_the_contract():
    refusals = delta_refusal_codes()
    assert refusals == ["DELTA_SLOT_CONFLICT", "TARGET_NOT_ADDRESSABLE", "KEEP_ON_EMPTY_SLOT",
                        "OP_NOT_ACCREDITED:<DIM>", "BASE_INVALID", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"]
    table, clarify = refusal_table(), set(annex_a3()["CLARIFY"])
    assert set(table) == set(refusals)
    for code, targets in table.items():
        assert targets and set(targets) <= clarify, (code, targets)
    assert table["OP_NOT_ACCREDITED:<DIM>"] == ["UNRESOLVED_VALUE:<DIM>"]
    assert table["BASE_INVALID"] == ["NO_ANCHOR", "ANCHOR_UNREADABLE"]
    assert all(table[c] == [c] for c in ("DELTA_SLOT_CONFLICT", "TARGET_NOT_ADDRESSABLE", "KEEP_ON_EMPTY_SLOT",
                                         "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"))


def test_the_contract_keeps_every_code_the_type_already_has():
    annex = annex_a3()
    assert fa.CLARIFY_CODES <= set(annex["CLARIFY"])
    assert set(annex["REASK"]) == fa.REASK_CODES
    assert fa.INFORM_CODES <= set(annex["INFORM"])
    assert {s.value for s in fa.TurnScope} <= set(annex_a1())


def test_d6_and_dim_are_closed_lists_in_the_contract():
    assert "OUT_UNSUPPORTED_PROPOSAL" in annex_a1()
    assert "UNSUPPORTED_PROPOSAL:<motivo>" in annex_a3()["INFORM"]
    motives = row_4b_motives()
    assert tuple(motives) == MOTIVES and all(owner and owner != "—" for owner in motives.values())
    assert motives["MULTI_REQUEST"] == "M2b"
    row = re.search(r"^\| 4b \| `OUT_UNSUPPORTED_PROPOSAL` \|.*\| `INFORM UNSUPPORTED_PROPOSAL:<motivo>` \|$",
                    _contract(), re.M)
    assert row is not None
    assert dim_domain() == {d.value for d in SLOT_DIMENSIONS} | {"UNIVERSE"}
    assert "### 20.4 Congelación tras R3" in _contract()
    assert "INV-M2A-25" in _contract()


def test_type_facts_that_hold_before_and_after_m2a3_1c():
    assert {code: representable(code) for code in TYPE_ADMITS} == TYPE_ADMITS
    # every <DIM> the contract allows, except UNIVERSE (a TYPE_GAPS item), is already admitted by the type
    assert all(representable(f"UNRESOLVED_VALUE:{dim}") for dim in dim_domain() - {"UNIVERSE"})


ROW4_SEALED = ("| 4 | Todas las condiciones de P caen en `answer_slots` y su valor selecciona **exactamente un** candidato "
               "(comparado por valor codificado), o no hay candidatos | `PENDING_ANSWER`: `compose(frame, patch)` |")
RETIRED_CLAUSE = "que ningún candidato fija con valor (R3, D-1)"  # B1 draft, retired by D-1 option 1 (Anexo D.2)


def test_d1_option1_is_in_the_contract_and_section_8_3_is_untouched():
    text = _contract()
    assert ROW4_SEALED in text.splitlines() and RETIRED_CLAUSE not in text
    section = text.split("**CARDINALITY sin ORDER (R3-1).**", 1)[1].split("Códigos de rechazo", 1)[0]
    for clause in ("**C1** «quitar orden y límite»: `CLEAR(ORDER)` + `CLEAR(CARDINALITY)`",
                   "un `SET_ORDER` por cada criterio de orden alternativo", "**salvo la vigente**",
                   "`answer_slots = {ORDER, CARDINALITY}`", "`REASK ANSWER_UNRESOLVED`", "`CANCEL_PENDING`",
                   "**sólo C1**"):
        assert clause in section, clause
    e_order = next(line for line in text.splitlines() if line.startswith("| follow-ups con ORDER/CARDINALITY"))
    assert "**sólo C1**" in e_order
    assert "### D.2 Puntos de revisión del usuario: RESUELTOS (2026-10-02)" in text


def test_d1_option1_answers_select_by_encoded_value():
    candidates = r31_candidates()
    by_date = cond("a1", D.ORDER, SAMPLE[D.ORDER][1])  # «por fecha»: the authority resolves date (desc)
    twenty = cond("a2", D.CARDINALITY, CardinalityValue(20, CardinalityBasis.FIGURE, 20))  # «dame 20»
    current = cond("a3", D.ORDER, SAMPLE[D.ORDER][0])  # the criterion in force is never a candidate
    assert selects(candidates, by_date) == 1  # row 4: exactly one ⇒ PENDING_ANSWER
    assert selects(candidates, twenty) == 0  # row 7: REASK ANSWER_UNRESOLVED, never «top 20 by the old order»
    assert selects(candidates, current) == 0
    assert selects(r31_candidates(provisional=True), by_date) == 0  # without E-ORDER: C1 only


# ---- the type side: declared red until M2A-3 phase 1c (strict xfail; never a false green) -------------------------
@pytest.mark.parametrize("gap", [
    pytest.param(gap, id=gap, marks=pytest.mark.xfail(
        strict=True, reason=f"tipo: {TYPE_GAPS[gap][0]}; dueño M2A-3, fase 1c tras el re-sellado r3"))
    for gap in TYPE_GAPS])
def test_the_type_implements_the_amended_contract(gap):
    assert TYPE_GAPS[gap][1]()


# ---- rendering --------------------------------------------------------------------------------------------------------
def render() -> str:
    def cell(text: str) -> str:
        return text.replace("|", "\\|")

    generated = invariant_codes()
    lines = ["### Invariantes generados del AST", "", "| Código | Constructor | Fila |", "|---|---|---|"]
    for code in sorted(generated):
        rows = ", ".join(r.rid for r in SWEEP if code in r.codes)
        lines.append(f"| `{code}` | `{generated[code]}` | {rows} |")
    lines += ["", "### Op × ranura (construido con `PatchOp`)", "", "| Op | Ranuras admitidas | Rechazos del tipo |",
              "|---|---|---|"]
    matrix = op_slot_matrix()
    for op in fa.PatchOpKind:
        ok = [s for (o, s), r in matrix.items() if o == op.value and r == "OK"]
        bad = Counter(r for (o, _), r in matrix.items() if o == op.value and r != "OK")
        lines.append(f"| `{op.value}` | {len(ok)}: {', '.join(ok)} | "
                     + ", ".join(f"`{code}` ×{n}" for code, n in sorted(bad.items())) + " |")
    lines += ["", "### Barrido (contrato enmendado)", "", "| Fila | Estado | Op × ranura | ¿Alcanzable? | Respuesta del "
              "contrato | Veredicto | Evidencia | Origen | Falta en el tipo |", "|---|---|---|---|---|---|---|---|---|"]
    for r in SWEEP:
        state = r.state + (f" ({', '.join(f'`{c}`' for c in r.codes)})" if r.codes else "")
        lines.append("| " + " | ".join(cell(x) for x in (r.rid, state, r.ops, r.reachable, r.response, r.verdict,
                                                           r.evidence, r.proposal, ", ".join(r.type_gaps))) + " |")
    count = Counter(r.verdict for r in SWEEP)
    lines += ["", "Totales: " + ", ".join(f"{k} {count[k]}" for k in ("CUBIERTO", "HUECO", "DEPENDENCIA",
                                                                        "NO_ALCANZABLE"))]
    lines += ["", "### Lado del tipo: rojo declarado (xfail estricto) hasta M2A-3 fase 1c", "",
              "| Id | Falta | ¿Ya en el tipo? |", "|---|---|---|"]
    lines += [f"| `{gap}` | {cell(text)} | {'sí' if check() else 'no'} |" for gap, (text, check) in TYPE_GAPS.items()]
    lines += ["", "### Sondas", "", "| Sonda | Resultado del tipo |", "|---|---|"]
    lines += [f"| {name} | {'admitido' if code is None else f'`{code}`'} |" for name, code in probes().items()]
    return "\n".join(lines)


if __name__ == "__main__":
    print(render())

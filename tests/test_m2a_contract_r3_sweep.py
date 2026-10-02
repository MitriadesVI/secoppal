"""M2A contract amendment R3, phase A: the representability sweep (a proposal; nothing here changes the contract).

User criterion (2026-10-02): «no existe una respuesta representable para un estado que sus propios tipos permiten
alcanzar». This module sweeps every invariant the M2a types impose on an effective and every refusal the contract
prescribes. The inventory is GENERATED from the code, never listed by hand:

* invariant codes: the AST of ``AcceptedInterpretation.__post_init__`` (plus the module helpers it calls) and of
  ``AcceptedRequest.__post_init__`` in ``app/core/foundation_accepted.py``;
* slots: ``SLOT_DIMENSIONS`` / ``POLARIZED_SLOT_DIMENSIONS`` / ``ValueSide`` plus UNIVERSE (contract §4.2);
* op × slot: every ``PatchOpKind`` × slot is CONSTRUCTED as a ``PatchOp`` (the type decides, not a table);
* refusals: the ``DeltaRefusal`` list is parsed from contract §6; annex A.3 is read from the contract and checked
  against ``reason_code_allowed``; a CLARIFY always leaves a ``PendingClarification`` whose ``reason`` the type checks.

``SWEEP`` carries one verdict per row (CUBIERTO / HUECO / DEPENDENCIA / NO_ALCANZABLE) with its evidence; every
reachable violating state has a PROBE built with the real types. The pins fix TODAY's contract: the HUECO rows and
``REPRESENTABLE_TODAY`` flip deliberately when the R3 amendment lands (phase B), never silently. A new invariant in
the types fails ``test_every_type_invariant_is_swept`` until it is swept.

``python -m tests.test_m2a_contract_r3_sweep`` prints the tables of
``docs/audits/m2a_conversation/M2A_CONTRACT_ENM_R3_PROPOSAL.md``.
"""
from __future__ import annotations

import ast
import hashlib
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

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


def proposal(raw: str, job: Operation, conditions: tuple[ProposedCondition, ...]) -> InterpretationProposal:
    """A wire (IR v2) proposal of one UNSPECIFIED request: what the producer may hand to M2a for P."""
    expression = atom(conditions[0].condition_id) if len(conditions) == 1 else group(
        LogicOperator.AND, *(atom(c.condition_id) for c in conditions))
    request = ProposedRequest("r1", "r1", Subject.UNSPECIFIED, (RawSpan(0, len(raw), raw),),
                              condition_expression=expression)
    return InterpretationProposal(hashlib.sha256(raw.encode()).hexdigest(), "sweep", "1", (request,), conditions,
                                  (), (), (ProposedJob("j", job, ("r1",), (RawSpan(0, 1, raw[0]),)),))


def wire(cid: str, dimension: D, operator: str, quote: str, raw: str) -> ProposedCondition:
    at = raw.index(quote)
    return ProposedCondition(cid, ("r1",), dimension, operator, Polarity.POSITIVE, quote,
                             (RawSpan(at, at + len(quote), quote),))


def pending(reason: str) -> fa.PendingClarification:
    now = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
    return fa.PendingClarification("p1", "t-turn", now, now + timedelta(minutes=30), 1, None, None, None, None,
                                   reason, (), frozenset())


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


def delta_refusal_codes() -> list[str]:
    """The closed ``DeltaRefusal`` list as contract §6 writes it."""
    text = CONTRACT.read_text(encoding="utf-8")
    block = re.search(r"Códigos de rechazo \(`DeltaRefusal`, cerrados\):(.*?)\.\s", text, re.S).group(1)
    return re.findall(r"`([^`]+)`", block)


def annex_a3() -> dict[str, list[str]]:
    text = CONTRACT.read_text(encoding="utf-8").split("**A.3 Códigos de razón:**", 1)[1].split("Retirados:", 1)[0]
    return {m.group(1): re.findall(r"`([^`]+)`", m.group(2))
            for m in re.finditer(r"^\| `([A-Z_]+)` \| (.*) \|$", text, re.M)}


# ---- the sweep (verdicts are analysis; rows are tied to generated codes, ops and probes) ---------------------------
@dataclass(frozen=True)
class Row:
    rid: str
    codes: tuple[str, ...]  # type-invariant codes the row answers for (empty: a type-valid state)
    state: str
    ops: str  # which op × slot can drive it
    reachable: str
    response: str  # DeltaRefusal → annex A.3, today
    verdict: str  # CUBIERTO | HUECO | DEPENDENCIA | NO_ALCANZABLE
    evidence: str
    proposal: str = ""


BRANCH_M2A3 = "rama M2A-3 30d073a4..f30e91ff, no en la base fa0b6de8"
# Codes present on only ONE of the two lines that meet at integration (base fa0b6de8 / M2A-3 branch): a swept code may
# be absent from the source under test only if it is listed here. Every other swept code must exist in the types.
BASE_ONLY = frozenset({"UNSORTED_POLICY_CONDITION_IDS"})  # replaced by MULTI_CONDITION_POLICY in 0de2b9a2
SWEEP = (
    Row("T01", ("EMPTY_REQUESTS", "DUPLICATE_REQUEST_ID", "TOO_MANY_REQUESTS", "INVALID_DUAL_ANCHOR"),
        "efectiva sin peticiones, con ids repetidos, >2 o dual inválida", "REPLACE_UNIVERSE/UNIVERSE", "NO",
        "—", "NO_ALCANZABLE",
        "UniverseTarget es un único (sujeto, clase) y §5.2 deja la petición nombrada; ningún op crea ni quita "
        "peticiones (PatchOp no tiene campo de petición)"),
    Row("T02", ("DUAL_ANCHOR_CONDITIONS_DIFFER",), "ancla dual con una condición nueva en una sola petición",
        "SET, SET_ORDER sobre ancla dual", "sólo si compose acotara la condición", "—", "CUBIERTO",
        "§5.1: PatchOp y Slot no llevan petición; compose da a la condición nueva todas las peticiones de la base "
        "(sonda P-dual-set). Una P que reparte condiciones por petición es MULTI (§1.2): ver O-3"),
    Row("T03", ("FOREIGN_CONDITION_REQUEST",), "dual → una petición sin estrechar request_ids",
        "REPLACE_UNIVERSE con ancla dual", "SÍ, si se aplica §6.3 al pie de la letra",
        "EXECUTE + UNIVERSE_CHANGED", "CUBIERTO",
        "§5.2 «se queda con el nombrado» exige reescribir request_ids de lo arrastrado (sonda P-dual-replace); "
        "§6.3/INV-M2A-02 lo prohíben literalmente («byte-idéntico»). M2A-3 ya aplica la excepción R5", "P-1"),
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
        "SÍ (la P con dos cotas del mismo lado es IR v2 válida: sonda P-two-lower)",
        "DELTA_SLOT_CONFLICT → ninguno", "HUECO",
        "§6.2 rechaza con DELTA_SLOT_CONFLICT; §6 manda «CLARIFY con el código de la autoridad», que no existe para "
        "un rechazo estructural; reason_code_allowed lo rechaza", "R3-2"),
    Row("T08", ("CARDINALITY_WITHOUT_ORDER",), "(a) CLEAR(ORDER) con CARDINALITY arrastrada",
        "CLEAR/ORDER (directiva CLEAR del wire 5.1 o evento «Quitar»)", "SÍ (sonda P-a)", "ninguno → ninguno",
        "HUECO",
        "ningún DeltaRefusal lo nombra y A.3 no tiene código; conservar CARDINALITY la inventa sin ORDER, quitarla "
        "viola INV-M2A-16 (STOP-2 de M2A-3)", "R3-1"),
    Row("T09", ("CARDINALITY_WITHOUT_ORDER",), "(b) SET CARDINALITY sin ORDER en base ni SET_ORDER",
        "SET/CARDINALITY", "NO hoy: la P es IR v2 inválida (sonda P-b-wire); ningún evento fija CARDINALITY",
        "—", "NO_ALCANZABLE",
        "condicional: si M2A-5 relaja CARDINALITY_WITHOUT_ORDER para FOLLOWUP_DELTA, es el mismo estado de (a); "
        "R3-1 formulada sobre el estado lo cubre sin re-sellar (D-2)", "R3-1"),
    Row("T10", ("NONCONJUNCTIVE_ORDER",), "ORDER dentro de OR/NOT", "SET_ORDER; CLEAR/SET/KEEP sobre átomos de un OR/NOT",
        "SÍ (objetivos dentro de OR/NOT)", "TARGET_NOT_ADDRESSABLE → TARGET_NOT_ADDRESSABLE", "CUBIERTO",
        "§4.2; SET_ORDER añade un átomo del AND de primer nivel (sonda P-order-or)"),
    Row("T11", ("FOREIGN_POLICY_CONDITION",), f"política que cita una condición quitada ({BRANCH_M2A3})",
        "CLEAR/SET de una condición citada", "SÍ",
        "OP_NOT_ACCREDITED:<DIM> → UNRESOLVED_VALUE:<DIM>", "CUBIERTO",
        "§6.6: la política se va con su condición o se re-decide; sin dueño, fail-closed con la dimensión de la "
        "condición citada (ranura). Excepción O1B: cita RETRIEVAL_INTENT (no es ranura) → S02"),
    Row("T12", ("CONFLICTING_POLICY_EFFECT", "DUPLICATE_POLICY", "UNSORTED_POLICIES", "NONCANONICAL_LOGIC",
                "MULTI_CONDITION_POLICY", "NONCANONICAL_CONDITION_ORDER", "SLOT_WITHOUT_RANK"),
        f"forma canónica de políticas, condiciones y lógica ({BRANCH_M2A3})", "todos",
        "sólo por defecto de construcción", "—", "CUBIERTO",
        "canonical_interpretation es el único constructor (M2A3_CANONICAL_FORM.md, decisiones D-CANON-1/2 del "
        "usuario); main emite políticas de una condición y compose nunca las fusiona"),
    Row("T13", ("VIEW_TYPE_UNAVAILABLE", "VIEW_REQUIRED"), "universo no dicho / vista derivada (F9)",
        "REPLACE_UNIVERSE; recálculo de vista", "NO hoy (UniverseTarget prohíbe unspecified; la base no puede serlo)",
        "—", "DEPENDENCIA", "ola F (HostViewDecision), ya declarada en §19 «vista derivada»"),
    Row("S01", (), "REPLACE_UNIVERSE a oportunidades (SF075): la CAPACIDAD", "REPLACE_UNIVERSE/UNIVERSE", "SÍ",
        "—", "DEPENDENCIA",
        "re-decidir O1B/opportunity-explicit-state (no representables: citan RETRIEVAL_INTENT) y paridad de TIME "
        "explícito (foundation_projection.py:235-237 UNSUPPORTED_OPPORTUNITY_OVERRIDE); dueño "
        "FOUNDATION_O1B_EXPLICIT_TIME_PARITY (+ M2A-1b)", "registro §19"),
    Row("S02", (), "REPLACE_UNIVERSE a oportunidades: el RECHAZO fail-closed", "REPLACE_UNIVERSE/UNIVERSE",
        "SÍ (el tipo admite la efectiva: sonda P-c)", "OP_NOT_ACCREDITED:UNIVERSE → UNRESOLVED_VALUE:UNIVERSE (rechazado)",
        "HUECO",
        "<DIM> sólo recorre SLOT_DIMENSIONS; UNIVERSE es ranura (§4.2) pero no dimensión: la pendiente no se puede "
        "construir (UNKNOWN_PENDING_REASON)", "R3-3"),
    Row("S03", (), "primer turno o búsqueda nueva que pregunta por conflicto de universo (H7)", "— (P sin base)",
        "SÍ (foundation_runtime.py:290-292 HOST_UNIVERSE_RULE_CONFLICT)", "→ UNRESOLVED_VALUE:UNIVERSE (rechazado)",
        "HUECO", "§7.2 (c) y primer turno dejan una pendiente; la reducción M1 → UNRESOLVED_VALUE:<DIM> es la del "
        "esquema Gold 2.1 (O-2) y para el universo no es representable", "R3-3"),
    Row("S04", (), "F16 fail-closed: negativo sobre varios positivos o valor contenido",
        "SET/<DIM>/NEGATIVE con ≥2 positivos en la ranura", "SÍ (el tipo admite la efectiva: sonda P-f16)",
        "«CLARIFY (G-FU-09)» sin código", "HUECO", "§6.5 prescribe CLARIFY sin código; A.3 no tiene ninguno",
        "R3-4"),
    Row("S05", (), "REPLACE_UNIVERSE con STATE/MODALITY arrastrado inexistente en el destino", "REPLACE_UNIVERSE",
        "SÍ", "H8 → STATE_NOT_IN_UNIVERSE", "CUBIERTO", "§6 H8 (SF079); MODALITY depende de E-MODALITY con el mismo código"),
    Row("S06", (), "CLEAR que deja la efectiva sin condiciones / CLEAR de ranura vacía", "CLEAR", "SÍ",
        "H11 EMPTY_SEARCH_AFTER_DELTA / H12 INFORM CLEAR_ON_EMPTY_SLOT", "CUBIERTO",
        "con otra op, el CLEAR vacío no cambia nada (OQ-5 X8, tensión viva declarada, no hueco de representación)"),
    Row("R01", (), "rechazo DELTA_SLOT_CONFLICT (dos SET en una ranura; KEEP+CLEAR; dos REPLACE_UNIVERSE)",
        "SET×2, KEEP+CLEAR, REPLACE_UNIVERSE×2", "SÍ", "DELTA_SLOT_CONFLICT → ninguno", "HUECO",
        "incluye la unión de valores de G-FU-09 y «procesos y contratos» sobre un ancla simple", "R3-2"),
    Row("R02", (), "rechazo KEEP_ON_EMPTY_SLOT", "KEEP", "SÍ (directiva KEEP del wire 5.1: sonda P-keep)",
        "KEEP_ON_EMPTY_SLOT → ninguno", "HUECO", "§6.1 lo rechaza; A.3 no tiene código", "R3-2"),
    Row("R03", (), "rechazo TARGET_NOT_ADDRESSABLE", "CLEAR/SET/KEEP", "SÍ", "→ TARGET_NOT_ADDRESSABLE",
        "CUBIERTO", "homónimo en A.3"),
    Row("R04", (), "rechazo OP_NOT_ACCREDITED:<DIM> sobre una dimensión de ranura", "SET/SET_ORDER", "SÍ",
        "→ UNRESOLVED_VALUE:<DIM>", "CUBIERTO", "11 dimensiones de ranura admitidas por reason_code_allowed"),
    Row("R05", (), "rechazo BASE_INVALID", "todos", "defensivo (§7.1 fila 1 decide antes)",
        "→ NO_ANCHOR / ANCHOR_UNREADABLE", "CUBIERTO", "§3.8 y §7.1 fila 1"),
    Row("F01", (), "G-FU-05 paginar ancla dual / página frente a top-N", "evento de página (M2A-6)", "SÍ (M2A-6)",
        "CLARIFY, código diferido al dueño", "DEPENDENCIA",
        "§19 ya declara «Salida: regla y código de razón»; con R3-3 existe forma representable "
        "(SF148, FRONTIER, ya usa UNRESOLVED_VALUE:UNIVERSE)"),
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
}


def probes() -> dict[str, str | None]:
    topic, order, card = cond("c1", D.TOPIC), cond("c2", D.ORDER), cond("c3", D.CARDINALITY)
    lower = cond("c4", D.VALUE, SAMPLE[D.VALUE][0])
    lower2 = cond("c5", D.VALUE, MoneyValue(800_000_000, Comparator.GT))
    shared = cond("c1", D.TOPIC, rids=("r1", "r2"))
    raw_b, raw_v = "dame solo 10", "mayor a 50 millones y mayor a 80 millones"
    cafe = cond("c6", D.TOPIC, TopicValue("cafeteria", False, False))
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
    }


# Annex A.3 representability today: a CLARIFY code exists iff ``PendingClarification`` accepts it as ``reason``.
REPRESENTABLE_TODAY = {
    "TARGET_NOT_ADDRESSABLE": True, "NO_ANCHOR": True, "ANCHOR_UNREADABLE": True,
    "STATE_NOT_IN_UNIVERSE": True, "EMPTY_SEARCH_AFTER_DELTA": True,
    **{f"UNRESOLVED_VALUE:{d.value}": True for d in sorted(SLOT_DIMENSIONS, key=lambda d: d.value)},
    "UNRESOLVED_VALUE:UNIVERSE": False, "DELTA_SLOT_CONFLICT": False, "KEEP_ON_EMPTY_SLOT": False,
    "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER": False,
    **{f"UNRESOLVED_VALUE:{d.value}": False for d in (D.RETRIEVAL_INTENT, D.UNSUPPORTED, D.CONTROL_LANGUAGE)},
}


def representable(code: str) -> bool:
    return refusal(lambda: pending(code)) is None


# ---- pins -----------------------------------------------------------------------------------------------------------
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


def test_the_refusal_list_and_annex_a3_are_the_ones_swept():
    assert delta_refusal_codes() == ["DELTA_SLOT_CONFLICT", "TARGET_NOT_ADDRESSABLE", "KEEP_ON_EMPTY_SLOT",
                                     "OP_NOT_ACCREDITED:<DIM>", "BASE_INVALID"]
    annex = annex_a3()
    assert set(annex["CLARIFY"]) - {"UNRESOLVED_VALUE:<DIM>"} == fa.CLARIFY_CODES
    assert set(annex["REASK"]) == fa.REASK_CODES
    assert set(annex["INFORM"]) - {"OPERATION_CHANGE:<op>"} == fa.INFORM_CODES


def test_representability_is_today_what_the_sweep_says():
    assert {code: representable(code) for code in REPRESENTABLE_TODAY} == REPRESENTABLE_TODAY
    assert all(row.verdict in {"CUBIERTO", "HUECO", "DEPENDENCIA", "NO_ALCANZABLE"} for row in SWEEP)
    assert len({row.rid for row in SWEEP}) == len(SWEEP)


# ---- rendering for the proposal document -----------------------------------------------------------------------------
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
    lines += ["", "### Barrido", "", "| Fila | Estado | Op × ranura | ¿Alcanzable? | Respuesta hoy (DeltaRefusal → A.3) "
              "| Veredicto | Evidencia | Propuesta |", "|---|---|---|---|---|---|---|---|"]
    for r in SWEEP:
        state = r.state + (f" ({', '.join(f'`{c}`' for c in r.codes)})" if r.codes else "")
        lines.append("| " + " | ".join(cell(x) for x in (r.rid, state, r.ops, r.reachable, r.response, r.verdict,
                                                           r.evidence, r.proposal)) + " |")
    count = Counter(r.verdict for r in SWEEP)
    lines += ["", "Totales: " + ", ".join(f"{k} {count[k]}" for k in ("CUBIERTO", "HUECO", "DEPENDENCIA",
                                                                        "NO_ALCANZABLE"))]
    lines += ["", "### Sondas", "", "| Sonda | Resultado del tipo |", "|---|---|"]
    lines += [f"| {name} | {'admitido' if code is None else f'`{code}`'} |" for name, code in probes().items()]
    lines += ["", "### Representabilidad hoy (`PendingClarification.reason`)", "", "| Código | ¿Representable? |",
              "|---|---|"]
    lines += [f"| `{code}` | {'sí' if ok else 'NO'} |" for code, ok in REPRESENTABLE_TODAY.items()]
    return "\n".join(lines)


if __name__ == "__main__":
    print(render())

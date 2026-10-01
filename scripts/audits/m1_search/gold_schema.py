"""Validator for the M1 SEARCH Gold v1 (docs/audits/m1_search_foundation/M1_0_SEARCH_CONTRACT.md §8).

Usage: python scripts/audits/m1_search/gold_schema.py FILE.jsonl [FILE.jsonl ...]
       python scripts/audits/m1_search/gold_schema.py --selftest     # cases of the v1.7/v1.8/v1.9 host-policy rules + lexicon drift
Exit 0 when every record is valid; otherwise prints one line per error and exits 1.
Stdlib only, so it runs from any worktree without the app.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys

SCHEMA_VERSION = "m1-search-gold/1.9"
# Contract v1.7 (P-ACT-2): a PROCESSES request with an ACTOR condition (either polarity) is executed by the
# HOST on CONTRACTS and announced. Request level, because a MULTI record can carry other requests that do
# not qualify. The field is absent (or null) everywhere else.
REDIRECT_FIELD = "host_universe_redirect"
ACTOR_PROCESSES_REDIRECT = {"executed_universe": "CONTRACTS", "notice": "ACTOR_PROCESSES_TO_CONTRACTS"}
# Contract v1.8 (ACTOR x STATE, universe NOT cited): ACTOR + an awarded state executes on CONTRACTS as an
# ANNOUNCED APPROXIMATION ("adjudicada" is not "contrato firmado").
ACTOR_AWARDED_REDIRECT = {"executed_universe": "CONTRACTS", "notice": "ACTOR_AWARDED_TO_CONTRACTS",
                          "effect": "APPROXIMATION"}
# Contract v1.9: PROCESSES cited + ACTOR + awarded keeps the v1.7 notice (the universe WAS cited and changed) and
# gains the declared approximation effect. Without a STATE the v1.7 object above stays byte-identical (N073).
ACTOR_PROCESSES_AWARDED_REDIRECT = {**ACTOR_PROCESSES_REDIRECT, "effect": "APPROXIMATION"}
PREAWARD_REASON = "ACTOR_STATE_INCOMPATIBLE"
# Contract v1.9: UNSPECIFIED + positive ACTOR + TIME and no STATE executes on CONTRACTS (ACTOR beats PV1) and the
# host announces that TIME is applied to the SIGNATURE date. Request-level closed notice (not a universe redirect:
# the universe is not changed, so `product_view` already carries CONTRACTS).
NOTICE_FIELD = "host_notice"
ACTOR_TIME_NOTICE = "ACTOR_TIME_ON_SIGNATURE_DATE"

# ---- Contract v1.9: a state is classified by the DATASET WHERE IT EXISTS, never by the host family. The process
# lifecycle is another universe: a process state is NEVER translated into a contract state, and a state that exists
# in contracts is applied as a contract state. Derived from app/data/estados.py (VALID_ESTADOS_PROCEDIMIENTO,
# VALID_ESTADOS_APERTURA, VALID_ESTADOS_CONTRATO) and app/core/estado_families.py (PROCESS_ONLY_OUTCOME_TOKENS);
# `--selftest` fails if the lexicon drifts from this table.
PROCESS_ONLY_PRE_AWARD = "PROCESS_ONLY_PRE_AWARD"      # exists only in processes, before the award: no awardee yet
PROCESS_ONLY_NO_AWARDEE = "PROCESS_ONLY_NO_AWARDEE"    # process outcome that never has an awardee (desierto, revocado)
AWARDED = "AWARDED"                                    # process state "Seleccionado" (colloquial "adjudicado")
CONTRACT_STATE = "CONTRACT_STATE"                      # exists as a state of CONTRACTS
PENDING = "PENDING"                                    # no rule adjudicated: the validator refuses to label it
STATE_CLASSES = (PROCESS_ONLY_PRE_AWARD, PROCESS_ONLY_NO_AWARDEE, AWARDED, CONTRACT_STATE, PENDING)

# hint -> (literal in estado_de_apertura/estado_del_procedimiento, literal in estado_contrato, phase in the PROCESS
# lifecycle, decision). Phase is only meaningful for a process literal: "pre_award" | "awarded" | "no_awardee" | None.
# decision = PENDING marks a user decision (2026-10-01) that overrides the existence rule.
# Literals marked "~" are not Socrata literals: the contract lexicon maps them by synonym or by definition
# ("liquidado" -> Cerrado; "celebrado"/"firmado" is the contract universe itself, `contract_universe` in the host).
_STATE_LEXICON = {
    "open":                  ("Abierto", None, "pre_award", None),
    "published":             ("Publicado", None, "pre_award", None),
    "in_evaluation":         ("Evaluación", None, "pre_award", None),
    "awarded":               ("Seleccionado", None, "awarded", None),
    "draft":                 ("Borrador", "Borrador", "pre_award", None),
    "cancelled":             ("Cancelado", "Cancelado", None, None),
    "terminated":            (None, "terminado", None, None),
    "in_execution":          (None, "En ejecución", None, None),
    "liquidated":            (None, "~Cerrado", None, None),
    "signed":                (None, "~contract_universe", None, None),
    "closed":                ("Cerrado", "Cerrado", None, PENDING),               # double reading (apertura / contrato)
    "other:seleccionado":    ("Seleccionado", None, None, PENDING),               # ambiguous with awarded (host: Sprint 5)
    "other:desierto":        ("~desierto", None, "no_awardee", None),
    "other:revocado":        ("~revocado", None, "no_awardee", None),
    "other:aprobado":        ("Aprobado", "Aprobado", None, None),
    "other:en_aprobacion":   ("En aprobación", "En aprobación", None, None),
    "other:suspendido":      ("Suspendido", "Suspendido", None, None),
    "other:cedido":          (None, "cedido", None, None),
    "other:prorrogado":      (None, "Prorrogado", None, None),
    "other:modificado":      (None, "Modificado", None, None),
    "other:enviado_proveedor": (None, "enviado Proveedor", None, None),
}
# Spelling variants already present in the sealed Gold; same state, same class. Anything else under `other:` is PENDING.
STATE_ALIASES = {"other:celebrado": "signed", "other:liquidado": "liquidated", "other:borrador": "draft",
                 "other:en_evaluacion": "in_evaluation"}
_PHASE_CLASS = {"pre_award": PROCESS_ONLY_PRE_AWARD, "awarded": AWARDED, "no_awardee": PROCESS_ONLY_NO_AWARDEE}


def _fold_hint(hint: str) -> str:
    import unicodedata
    text = unicodedata.normalize("NFKD", str(hint or "")).encode("ascii", "ignore").decode().lower().strip()
    return re.sub(r"\s+", "_", text)


def _canonical_hint(hint) -> str | None:
    folded = _fold_hint(hint)
    if folded in _STATE_LEXICON:
        return folded
    return STATE_ALIASES.get(folded)


def state_class(hint, universe: str = "UNSPECIFIED") -> str:
    """Class of ONE state value for a request whose universe is `universe` (UNSPECIFIED or PROCESSES).

    UNSPECIFIED: existence decides. A state that exists in contracts is CONTRACT_STATE (also `draft`, `cancelled`,
      `aprobado`, `suspendido`); a process-only state keeps its process phase (pre-award / awarded / no awardee).
    PROCESSES (universe cited): the state is read in the PROCESS lifecycle and is never translated to a contract
      state: only an explicit process phase classifies it; every other state is PENDING.
    Unknown hints and the user-decided ambiguous ones (`closed`, `other:seleccionado`) are PENDING."""
    canon = _canonical_hint(hint)
    if canon is None:
        return PENDING
    process_literal, contract_literal, phase, decision = _STATE_LEXICON[canon]
    if decision:
        return decision
    if universe == "PROCESSES":
        return _PHASE_CLASS.get(phase, PENDING) if process_literal else PENDING
    if contract_literal:
        return CONTRACT_STATE
    return _PHASE_CLASS.get(phase, PENDING)


def has_actor(request: dict, polarity: str | None = None) -> bool:
    return any(isinstance(c, dict) and c.get("dimension") == "ACTOR" and polarity in (None, c.get("polarity"))
               for c in request.get("conditions") or [])


def _conditions(request: dict, dimension: str) -> list[dict]:
    return [c for c in request.get("conditions") or [] if isinstance(c, dict) and c.get("dimension") == dimension]


def needs_redirect(request: dict) -> bool:
    """True when contract v1.7 P-ACT-2 applies: cited universe PROCESSES + any ACTOR condition."""
    return request.get("universe") == "PROCESSES" and has_actor(request)


def actor_state_policy(request: dict) -> str | None:
    """Contract v1.9 class of a request with an ACTOR and a STATE (universe UNSPECIFIED or PROCESSES).

    One of STATE_CLASSES, or None when there is no ACTOR, no STATE, or the universe is CONTRACTS (explicit universe).
    - every STATE positive and of the same class                     -> that class
    - any negated STATE or ACTOR, mixed classes, unknown/ambiguous   -> PENDING (no rule: never labeled silently)
    - UNSPECIFIED + AWARDED/CONTRACT_STATE + TIME                    -> PENDING (the date semantics is not adjudicated)
    Pre-award and no-awardee states are clarified even with TIME (nothing is executed)."""
    universe = request.get("universe")
    if universe not in {"UNSPECIFIED", "PROCESSES"} or not has_actor(request):
        return None
    states = _conditions(request, "STATE")
    if not states:
        return None
    if not has_actor(request, "POSITIVE") or has_actor(request, "NEGATIVE") \
            or any(c.get("polarity") != "POSITIVE" for c in states):
        return PENDING
    classes = {state_class(c.get("value_hint"), universe) for c in states}
    if len(classes) != 1:
        return PENDING
    klass = classes.pop()
    if universe == "UNSPECIFIED" and klass in {AWARDED, CONTRACT_STATE} and _conditions(request, "TIME"):
        return PENDING
    return klass


def actor_time_applies(request: dict) -> bool:
    """Contract v1.9: UNSPECIFIED + positive ACTOR + TIME and no STATE -> CONTRACTS, TIME on the signature date."""
    return (request.get("universe") == "UNSPECIFIED" and has_actor(request, "POSITIVE")
            and not _conditions(request, "STATE") and bool(_conditions(request, "TIME")))


def required_redirect(request: dict) -> dict | None:
    """The exact `host_universe_redirect` object this request must carry (None: it must carry none)."""
    policy = actor_state_policy(request)
    if request.get("universe") == "PROCESSES":
        if policy == AWARDED:
            return ACTOR_PROCESSES_AWARDED_REDIRECT
        return ACTOR_PROCESSES_REDIRECT if needs_redirect(request) and policy is None else None
    return ACTOR_AWARDED_REDIRECT if policy == AWARDED else None


def clarifies(policy: str | None) -> bool:
    """Classes whose request is never executed: a process that has not been awarded (or never will be) has no awardee."""
    return policy in {PROCESS_ONLY_PRE_AWARD, PROCESS_ONLY_NO_AWARDEE}


SLICES = {"REAL", "SYNTHETIC", "ADVERSARIAL", "MINIMAL_PAIR", "NEIGHBOR"}
TURN_CONTEXTS = {"SELF_CONTAINED", "FOLLOWUP_FRAGMENT", "NEEDS_HISTORY"}
OPERATIONS = {"SEARCH", "COUNT", "SUM_VALUE", "GROUP_BY", "RANK_SUPPLIERS", "PROFILE_BUYER",
              "PROFILE_ACTOR", "FOLLOW", "EXPORT", "PROTOCOL", "FOLLOWUP_DELTA", "OUT_OF_DOMAIN",
              "MULTI"}
ASPECT_OPERATIONS = {"COUNT", "SUM_VALUE", "GROUP_BY"}
ASPECTS = {"TOTAL", "BY_MODALITY", "BY_YEAR", "BY_BUYER", "BY_SUPPLIER", "BY_STATE", "BY_TYPE"}
UNIVERSES = {"PROCESSES", "CONTRACTS", "UNSPECIFIED"}
VIEWS = {"RECORDS", "OPPORTUNITIES"}
PRODUCT_VIEWS = {"CURRENT_OPPORTUNITIES", "HISTORICAL_PROCESSES", "CONTRACTS"}
DISPOSITIONS = {"EXECUTE", "CLARIFY", "DECLINE"}
POLARITIES = {"POSITIVE", "NEGATIVE"}
OPERATORS = {
    "TOPIC": {"contains", "whole_token_folded"},
    "BUYER": {"is"},
    "ACTOR": {"is"},
    "GEOGRAPHY": {"is"},
    "TIME": {"during_year", "during_month", "between_dates", "relative_period"},
    "VALUE": {"greater_than", "greater_or_equal", "less_than", "less_or_equal"},
    "STATE": {"is", "family"},
    "MODALITY": {"family", "is"},
    "CONTRACT_TYPE_OR_NATURE": {"is", "family"},
    "ORDER": {"value_desc", "value_asc", "date_desc", "date_asc"},
    "SOURCE": {"is"},
    "UNSUPPORTED": None,  # free short label
}
GEO_LEVELS = {"DEPARTMENT", "MUNICIPALITY", "REGION", "UNKNOWN"}
STATE_HINT = re.compile(r"(open|published|closed|awarded|terminated|in_execution|cancelled|draft|in_evaluation|signed|liquidated|other:.+)\Z")
LOGIC_TOKEN = re.compile(r"AND|OR|NOT|[A-Za-z0-9_.-]+|\(|\)|,")


def _parse_logic(text: str) -> tuple[str, list[str]]:
    """Return (normalized, atoms); raises ValueError on a malformed expression."""
    tokens = [t for t in LOGIC_TOKEN.findall(text.replace(" ", ""))]
    if "".join(tokens) != text.replace(" ", ""):
        raise ValueError("LOGIC_BAD_CHARS")
    pos = 0
    atoms: list[str] = []

    def node() -> None:
        nonlocal pos
        if pos >= len(tokens):
            raise ValueError("LOGIC_TRUNCATED")
        tok = tokens[pos]
        pos += 1
        if tok in {"AND", "OR", "NOT"}:
            if pos >= len(tokens) or tokens[pos] != "(":
                raise ValueError("LOGIC_EXPECTED_PAREN")
            pos += 1
            count = 0
            while True:
                node()
                count += 1
                if pos >= len(tokens):
                    raise ValueError("LOGIC_TRUNCATED")
                if tokens[pos] == ",":
                    pos += 1
                    continue
                if tokens[pos] == ")":
                    pos += 1
                    break
                raise ValueError("LOGIC_UNEXPECTED_TOKEN")
            if tok == "NOT" and count != 1:
                raise ValueError("LOGIC_NOT_ARITY")
            if tok in {"AND", "OR"} and count < 1:
                raise ValueError("LOGIC_GROUP_ARITY")
        elif tok in {"(", ")", ","}:
            raise ValueError("LOGIC_UNEXPECTED_TOKEN")
        else:
            atoms.append(tok)

    node()
    if pos != len(tokens):
        raise ValueError("LOGIC_TRAILING")
    return text, atoms


def _check_requests(requests, text, where, errors, search=True) -> set[str]:
    universes = set()
    if not isinstance(requests, list) or not requests:
        errors.append(f"{where}: REQUESTS_REQUIRED")
        return universes
    ids = set()
    for request in requests:
        rid = request.get("request_id")
        if not rid or rid in ids:
            errors.append(f"{where}: BAD_OR_DUPLICATE_REQUEST_ID {rid}")
        ids.add(rid)
        universe = request.get("universe")
        universes.add(universe)
        if universe not in UNIVERSES:
            errors.append(f"{where}/{rid}: BAD_UNIVERSE {universe}")
        view = request.get("view")
        if view not in VIEWS:
            errors.append(f"{where}/{rid}: BAD_VIEW {view}")
        if view == "OPPORTUNITIES" and universe != "PROCESSES":
            errors.append(f"{where}/{rid}: OPPORTUNITIES_ONLY_ON_PROCESSES")
        quote = request.get("universe_quote")
        if universe == "UNSPECIFIED":
            if quote is not None:
                errors.append(f"{where}/{rid}: UNSPECIFIED_HAS_UNIVERSE_QUOTE")
        elif not quote or (text is not None and quote not in text):
            errors.append(f"{where}/{rid}: UNIVERSE_QUOTE_NOT_IN_TEXT {quote!r}")
        conditions = request.get("conditions", [])
        cids = []
        for cond in conditions:
            cid = cond.get("id")
            cids.append(cid)
            dim = cond.get("dimension")
            if dim not in OPERATORS:
                errors.append(f"{where}/{rid}/{cid}: BAD_DIMENSION {dim}")
                continue
            allowed = OPERATORS[dim]
            op = cond.get("operator")
            if allowed is not None and op not in allowed:
                errors.append(f"{where}/{rid}/{cid}: BAD_OPERATOR {dim}.{op}")
            if allowed is None and (not isinstance(op, str) or not op.strip()):
                errors.append(f"{where}/{rid}/{cid}: UNSUPPORTED_NEEDS_LABEL")
            if cond.get("polarity") not in POLARITIES:
                errors.append(f"{where}/{rid}/{cid}: BAD_POLARITY")
            q = cond.get("quote")
            if not q or (text is not None and q not in text):
                errors.append(f"{where}/{rid}/{cid}: QUOTE_NOT_IN_TEXT {q!r}")
            if dim == "GEOGRAPHY" and cond.get("level") not in GEO_LEVELS:
                errors.append(f"{where}/{rid}/{cid}: GEOGRAPHY_LEVEL_REQUIRED")
            if dim != "GEOGRAPHY" and cond.get("level") not in (None, ""):
                errors.append(f"{where}/{rid}/{cid}: LEVEL_ONLY_FOR_GEOGRAPHY")
            if dim == "STATE" and not STATE_HINT.match(str(cond.get("value_hint") or "")):
                errors.append(f"{where}/{rid}/{cid}: BAD_STATE_HINT {cond.get('value_hint')!r}")
            if dim == "TOPIC" and op == "whole_token_folded" and cond.get("polarity") == "POSITIVE":
                if q and q.strip().lower() not in {"obra", "obras"}:
                    errors.append(f"{where}/{rid}/{cid}: WHOLE_TOKEN_ONLY_FOR_OBRA")
        if len(set(cids)) != len(cids):
            errors.append(f"{where}/{rid}: DUPLICATE_CONDITION_ID")
        logic = request.get("logic")
        if conditions:
            try:
                _, atoms = _parse_logic(logic or "")
            except ValueError as exc:
                errors.append(f"{where}/{rid}: {exc} {logic!r}")
            else:
                if sorted(atoms) != sorted(cids):
                    errors.append(f"{where}/{rid}: LOGIC_ATOMS_MISMATCH {logic!r}")
        elif logic not in (None, ""):
            errors.append(f"{where}/{rid}: LOGIC_WITHOUT_CONDITIONS")
        errors.extend(f"{where}/{rid}: {e}" for e in _redirect_errors(request, search) + _notice_errors(request, search))
    return universes


def _redirect_errors(request: dict, search: bool = True) -> list[str]:
    """Contract v1.7/v1.9 host-policy fields of one request: required, forbidden or malformed (one error max).
    `search=False` (non-SEARCH operations, normalized to UNSPECIFIED by C7) skips the ACTOR x STATE policy."""
    present = request.get(REDIRECT_FIELD) is not None
    got = request.get(REDIRECT_FIELD)
    policy = actor_state_policy(request) if search else None
    if policy == PENDING or (not search and needs_redirect(request) and _conditions(request, "STATE")):
        return ["ACTOR_STATE_PENDING_ADJUDICATION"]
    if clarifies(policy):
        return ["REDIRECT_FORBIDDEN_FOR_ACTOR_PREAWARD"] if present else []
    expected = required_redirect(request) if search else (ACTOR_PROCESSES_REDIRECT if needs_redirect(request) else None)
    if expected is None:
        return ["REDIRECT_ONLY_FOR_QUALIFYING_REQUEST"] if present else []
    awarded = expected in (ACTOR_AWARDED_REDIRECT, ACTOR_PROCESSES_AWARDED_REDIRECT)
    if not present:
        return ["REDIRECT_REQUIRED_FOR_ACTOR_AWARDED" if awarded else "REDIRECT_REQUIRED_FOR_ACTOR_ON_PROCESSES"]
    if got == expected:
        return []
    if awarded and isinstance(got, dict):  # the approximation must be announced AND declared: name what is missing
        if "notice" not in got:
            return ["REDIRECT_NOTICE_REQUIRED"]
        if "effect" not in got:
            return ["REDIRECT_EFFECT_REQUIRED"]
    return [f"BAD_REDIRECT {got!r}"]


def _notice_errors(request: dict, search: bool = True) -> list[str]:
    """Contract v1.9 TIME notice: required exactly on UNSPECIFIED + positive ACTOR + TIME without STATE."""
    got = request.get(NOTICE_FIELD)
    if search and actor_time_applies(request):
        if got is None:
            return ["NOTICE_REQUIRED_FOR_ACTOR_TIME"]
        return [] if got == ACTOR_TIME_NOTICE else [f"BAD_NOTICE {got!r}"]
    return ["NOTICE_ONLY_FOR_ACTOR_TIME"] if got is not None else []


def _search_requests(record: dict) -> list[dict]:
    if record.get("operation") == "SEARCH":
        return list(record.get("requests") or [])
    return []


def _policy_record_errors(record: dict, gid: str) -> list[str]:
    """v1.8/v1.9 record-level coherence (SEARCH records): view and disposition follow the policy."""
    errors: list[str] = []
    requests = _search_requests(record)
    unspecified = [q for q in requests if q.get("universe") == "UNSPECIFIED"]
    policies = {actor_state_policy(q) for q in requests}
    if AWARDED in policies:
        if any(actor_state_policy(q) == AWARDED for q in unspecified) and record.get("product_view") != "CONTRACTS":
            errors.append(f"{gid}: AWARDED_PRODUCT_VIEW_MUST_BE_CONTRACTS")
        if record.get("expected_disposition") != "EXECUTE":
            errors.append(f"{gid}: AWARDED_MUST_EXECUTE")
    if CONTRACT_STATE in policies:
        if record.get("product_view") != "CONTRACTS":
            errors.append(f"{gid}: CONTRACT_STATE_PRODUCT_VIEW_MUST_BE_CONTRACTS")
        if record.get("expected_disposition") != "EXECUTE":
            errors.append(f"{gid}: CONTRACT_STATE_MUST_EXECUTE")
    if any(actor_time_applies(q) for q in requests):
        if record.get("product_view") != "CONTRACTS":
            errors.append(f"{gid}: ACTOR_TIME_PRODUCT_VIEW_MUST_BE_CONTRACTS")
        if record.get("expected_disposition") != "EXECUTE":
            errors.append(f"{gid}: ACTOR_TIME_MUST_EXECUTE")
    if any(clarifies(p) for p in policies):
        if (record.get("expected_disposition"), record.get("disposition_reason")) != ("CLARIFY", PREAWARD_REASON):
            errors.append(f"{gid}: PREAWARD_MUST_CLARIFY_{PREAWARD_REASON}")
    elif record.get("disposition_reason") == PREAWARD_REASON:
        errors.append(f"{gid}: REASON_{PREAWARD_REASON}_WITHOUT_PREAWARD")
    return errors


def validate(record: dict) -> list[str]:
    errors: list[str] = []
    gid = record.get("gold_id") or "?"
    if record.get("slice") not in SLICES:
        errors.append(f"{gid}: BAD_SLICE")
    text = record.get("text")
    sha = record.get("text_sha256")
    if text is not None:
        if hashlib.sha256(text.encode("utf-8")).hexdigest() != sha:
            errors.append(f"{gid}: TEXT_SHA_MISMATCH")
    elif not sha:
        errors.append(f"{gid}: TEXT_OR_SHA_REQUIRED")
    if record.get("turn_context") not in TURN_CONTEXTS:
        errors.append(f"{gid}: BAD_TURN_CONTEXT")
    op = record.get("operation")
    if op not in OPERATIONS:
        errors.append(f"{gid}: BAD_OPERATION {op}")
    aspect = record.get("aspect")
    if op in ASPECT_OPERATIONS:
        if not (aspect in ASPECTS or (isinstance(aspect, str) and aspect.startswith("OTHER:"))):
            errors.append(f"{gid}: ASPECT_REQUIRED {aspect}")
    elif aspect is not None and op != "MULTI":
        errors.append(f"{gid}: ASPECT_ONLY_FOR_ANALYTIC")
    universes: set[str] = set()
    if op == "SEARCH":
        universes |= _check_requests(record.get("requests"), text, gid, errors)
    elif op == "MULTI":
        jobs = record.get("jobs")
        if not isinstance(jobs, list) or len(jobs) < 2:
            errors.append(f"{gid}: MULTI_NEEDS_JOBS")
        else:
            for at, job in enumerate(jobs):
                if job.get("operation") not in OPERATIONS - {"MULTI"}:
                    errors.append(f"{gid}/job{at}: BAD_JOB_OPERATION")
                if job.get("operation") == "SEARCH":
                    universes |= _check_requests(job.get("requests"), text, f"{gid}/job{at}", errors)
    elif record.get("requests"):
        # Non-SEARCH operations may still carry conditions (e.g. RANK_SUPPLIERS of a buyer).
        universes |= _check_requests(record.get("requests"), text, gid, errors, search=False)
    pv = record.get("product_view")
    if op not in {"SEARCH", "MULTI"}:
        universes = set()  # contract v1.2 B1: product_view only exists for SEARCH requests
    if "UNSPECIFIED" in universes:
        if pv not in PRODUCT_VIEWS:
            errors.append(f"{gid}: PRODUCT_VIEW_REQUIRED")
    elif pv is not None:
        errors.append(f"{gid}: PRODUCT_VIEW_ONLY_FOR_UNSPECIFIED")
    disp = record.get("expected_disposition")
    if disp not in DISPOSITIONS:
        errors.append(f"{gid}: BAD_DISPOSITION")
    if disp in {"CLARIFY", "DECLINE"} and not record.get("disposition_reason"):
        errors.append(f"{gid}: DISPOSITION_REASON_REQUIRED")
    if record.get("ambiguity") not in {"CLEAR", "AMBIGUOUS"}:
        errors.append(f"{gid}: BAD_AMBIGUITY")
    if record.get("ambiguity") == "AMBIGUOUS" and not record.get("alternatives"):
        errors.append(f"{gid}: AMBIGUOUS_NEEDS_ALTERNATIVES")
    if not record.get("labeler"):
        errors.append(f"{gid}: LABELER_REQUIRED")
    errors.extend(_policy_record_errors(record, gid))
    return errors


def main(paths: list[str]) -> int:
    all_errors: list[str] = []
    seen: dict[str, str] = {}
    total = 0
    for path in paths:
        with open(path, encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                total += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    all_errors.append(f"{path}:{line_no}: BAD_JSON {exc}")
                    continue
                gid = record.get("gold_id")
                if gid in seen:
                    all_errors.append(f"{path}:{line_no}: DUPLICATE_GOLD_ID {gid} (first in {seen[gid]})")
                seen[gid] = f"{path}:{line_no}"
                all_errors.extend(f"{path}:{line_no}: {e}" for e in validate(record))
    for error in all_errors:
        print(error)
    print(f"records={total} errors={len(all_errors)}", file=sys.stderr)
    return 1 if all_errors else 0


def lexicon_drift() -> list[str]:
    """Problems between `_STATE_LEXICON` and the real lexicon (app/data/estados.py, app/core/estado_families.py).
    Empty list = the table still says what the host lexicon says; `None`-free so a missing file is reported."""
    import importlib.util
    from pathlib import Path
    root = Path(__file__).resolve().parents[3]
    estados = root / "app" / "data" / "estados.py"
    families = root / "app" / "core" / "estado_families.py"
    if not estados.exists() or not families.exists():
        return ["LEXICON_SOURCE_MISSING"]
    spec = importlib.util.spec_from_file_location("_estados_lexicon", estados)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    proc_real = set(mod.VALID_ESTADOS_PROCEDIMIENTO) | set(mod.VALID_ESTADOS_APERTURA)
    contract_real = set(mod.VALID_ESTADOS_CONTRATO)
    problems: list[str] = []
    claimed_proc, claimed_contract = set(), set()
    for hint, (proc, contract, phase, decision) in _STATE_LEXICON.items():
        if not STATE_HINT.match(hint):
            problems.append(f"HINT_NOT_CANONICAL {hint}")
        if proc and not proc.startswith("~"):
            claimed_proc.add(proc)
            if proc not in proc_real:
                problems.append(f"PROCESS_LITERAL_NOT_IN_LEXICON {hint} {proc}")
        if contract and not contract.startswith("~"):
            claimed_contract.add(contract)
            if contract not in contract_real:
                problems.append(f"CONTRACT_LITERAL_NOT_IN_LEXICON {hint} {contract}")
        if proc and contract is None and not proc.startswith("~") and proc in contract_real:
            problems.append(f"PROCESS_ONLY_BUT_EXISTS_IN_CONTRACTS {hint} {proc}")
        if phase and not proc:
            problems.append(f"PHASE_WITHOUT_PROCESS_LITERAL {hint}")
    problems += [f"PROCESS_LITERAL_UNCOVERED {x}" for x in sorted(proc_real - claimed_proc)]
    problems += [f"CONTRACT_LITERAL_UNCOVERED {x}" for x in sorted(contract_real - claimed_contract)]
    if mod.ESTADO_CONTRATO_SYNONYMS.get("liquidado") != "Cerrado":
        problems.append("LIQUIDATED_NO_LONGER_MAPS_TO_CERRADO")
    source = families.read_text(encoding="utf-8")
    outcomes = re.search(r"PROCESS_ONLY_OUTCOME_TOKENS[^=]*=\s*\((.*?)\)", source, re.S)
    for word in ("desierto", "revocado"):
        if not outcomes or f'"{word}"' not in outcomes.group(1):
            problems.append(f"PROCESS_ONLY_OUTCOME_MISSING {word}")
    return problems


def selftest() -> int:
    """Positive and negative cases of the v1.7 (P-ACT-2), v1.8 and v1.9 (state by dataset of existence) host-policy rules."""
    import copy

    def cond(cid, dim, polarity="POSITIVE", hint="X SAS", op="is"):
        return {"id": cid, "dimension": dim, "operator": op, "polarity": polarity, "quote": "X SAS",
                "value_hint": hint, "level": None}

    def record(universe, dim, view="RECORDS", polarity="POSITIVE", redirect="ABSENT", jobs=False, state=None,
               state_polarity="POSITIVE", pv="DEFAULT", disposition="EXECUTE", reason=None, actor_polarity=None,
               time=None, notice="ABSENT", states=None, second_actor=None):
        conds = [cond("c1", dim, actor_polarity or polarity)]
        if second_actor:  # a second ACTOR condition with the given polarity (mixed polarities)
            conds.append(cond("c2", "ACTOR", second_actor, hint="Y SAS"))
        for hint in ([state] if state else []) + list(states or []):
            conds.append(cond(f"c{len(conds) + 1}", "STATE", state_polarity, hint=hint))
        if time:
            conds.append(cond(f"c{len(conds) + 1}", "TIME", time, hint="2024", op="during_year"))
        request = {"request_id": "q1", "universe": universe, "view": view,
                   "universe_quote": None if universe == "UNSPECIFIED" else "procesos",
                   "conditions": conds, "logic": "c1" if len(conds) == 1 else "AND(" + ",".join(c["id"] for c in conds) + ")"}
        if redirect != "ABSENT":
            request[REDIRECT_FIELD] = copy.deepcopy(redirect)
        if notice != "ABSENT":
            request[NOTICE_FIELD] = notice
        text = "procesos de X SAS"
        base = {"gold_id": "T", "slice": "SYNTHETIC", "text": text,
                "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(), "turn_context": "SELF_CONTAINED",
                "aspect": None, "expected_disposition": disposition, "disposition_reason": reason,
                "ambiguity": "CLEAR", "labeler": "t",
                "product_view": ("CONTRACTS" if universe == "UNSPECIFIED" else None) if pv == "DEFAULT" else pv}
        if jobs:
            return {**base, "operation": "MULTI", "requests": [], "jobs": [
                {"operation": "SEARCH", "requests": [request]}, {"operation": "FOLLOW", "requests": []}]}
        return {**base, "operation": "SEARCH", "requests": [request]}

    ok = ACTOR_PROCESSES_REDIRECT
    aw = ACTOR_AWARDED_REDIRECT
    paw = ACTOR_PROCESSES_AWARDED_REDIRECT
    clarify = {"disposition": "CLARIFY", "reason": PREAWARD_REASON}
    tn = ACTOR_TIME_NOTICE
    PENDING_CODE = "ACTOR_STATE_PENDING_ADJUDICATION"
    cases = [  # (label, record, expected error code(s) or None). A tuple = every code, and no other error.
        ("valid: PROCESSES+ACTOR with redirect", record("PROCESSES", "ACTOR", redirect=ok), None),
        ("valid: PROCESSES+ACTOR NEGATIVE with redirect", record("PROCESSES", "ACTOR", polarity="NEGATIVE", redirect=ok), None),
        ("valid: OPPORTUNITIES+ACTOR with redirect", record("PROCESSES", "ACTOR", view="OPPORTUNITIES", redirect=ok), None),
        ("valid: BUYER over PROCESSES has no field", record("PROCESSES", "BUYER"), None),
        ("valid: null on a non-qualifying request", record("CONTRACTS", "ACTOR", redirect=None), None),
        ("NEG1 missing: PROCESSES+ACTOR", record("PROCESSES", "ACTOR"), "REDIRECT_REQUIRED_FOR_ACTOR_ON_PROCESSES"),
        ("NEG1 missing: explicit null", record("PROCESSES", "ACTOR", redirect=None), "REDIRECT_REQUIRED_FOR_ACTOR_ON_PROCESSES"),
        ("NEG1 missing: NEGATIVE actor", record("PROCESSES", "ACTOR", polarity="NEGATIVE"), "REDIRECT_REQUIRED_FOR_ACTOR_ON_PROCESSES"),
        ("NEG1 missing: OPPORTUNITIES view", record("PROCESSES", "ACTOR", view="OPPORTUNITIES"), "REDIRECT_REQUIRED_FOR_ACTOR_ON_PROCESSES"),
        ("NEG1 missing: inside a MULTI job", record("PROCESSES", "ACTOR", jobs=True), "REDIRECT_REQUIRED_FOR_ACTOR_ON_PROCESSES"),
        ("NEG2 surplus: BUYER over PROCESSES", record("PROCESSES", "BUYER", redirect=ok), "REDIRECT_ONLY_FOR_QUALIFYING_REQUEST"),
        ("NEG2 surplus: ACTOR over CONTRACTS", record("CONTRACTS", "ACTOR", redirect=ok), "REDIRECT_ONLY_FOR_QUALIFYING_REQUEST"),
        ("NEG2 surplus: ACTOR UNSPECIFIED", record("UNSPECIFIED", "ACTOR", redirect=ok), "REDIRECT_ONLY_FOR_QUALIFYING_REQUEST"),
        ("NEG3 malformed: wrong universe", record("PROCESSES", "ACTOR", redirect={**ok, "executed_universe": "PROCESSES"}), "BAD_REDIRECT"),
        ("NEG3 malformed: wrong notice", record("PROCESSES", "ACTOR", redirect={**ok, "notice": "OTHER"}), "BAD_REDIRECT"),
        ("NEG3 malformed: missing key", record("PROCESSES", "ACTOR", redirect={"executed_universe": "CONTRACTS"}), "BAD_REDIRECT"),
        ("NEG3 malformed: extra key", record("PROCESSES", "ACTOR", redirect={**ok, "why": "x"}), "BAD_REDIRECT"),
        ("NEG3 malformed: string instead of object", record("PROCESSES", "ACTOR", redirect="CONTRACTS"), "BAD_REDIRECT"),
        # ---- v1.8: ACTOR x STATE, universe not cited (awarded / pre-award)
        ("v1.8 valid: ACTOR+awarded -> CONTRACTS, notice+APPROXIMATION", record("UNSPECIFIED", "ACTOR", state="awarded", redirect=aw), None),
        ("v1.8 valid: ACTOR+published asks, no redirect", record("UNSPECIFIED", "ACTOR", state="published", **clarify), None),
        ("v1.8 valid: ACTOR+open asks", record("UNSPECIFIED", "ACTOR", state="open", **clarify), None),
        ("v1.8 valid: ACTOR+in_evaluation asks", record("UNSPECIFIED", "ACTOR", state="in_evaluation", **clarify), None),
        ("v1.8 valid: ACTOR alone (PV3) executes, no redirect", record("UNSPECIFIED", "ACTOR"), None),
        ("v1.8 valid: awarded without ACTOR keeps PV2", record("UNSPECIFIED", "BUYER", state="awarded", pv="HISTORICAL_PROCESSES"), None),
        ("v1.8 valid: explicit CONTRACTS ACTOR+awarded has no redirect", record("CONTRACTS", "ACTOR", state="awarded"), None),
        ("v1.8 NEG missing effect", record("UNSPECIFIED", "ACTOR", state="awarded", redirect={k: v for k, v in aw.items() if k != "effect"}), "REDIRECT_EFFECT_REQUIRED"),
        ("v1.8 NEG missing notice", record("UNSPECIFIED", "ACTOR", state="awarded", redirect={k: v for k, v in aw.items() if k != "notice"}), "REDIRECT_NOTICE_REQUIRED"),
        ("v1.8 NEG v1.7 object reused for awarded", record("UNSPECIFIED", "ACTOR", state="awarded", redirect=ok), "REDIRECT_EFFECT_REQUIRED"),
        ("v1.8 NEG missing redirect (STATE_ALWAYS_WINS shape)", record("UNSPECIFIED", "ACTOR", state="awarded"), "REDIRECT_REQUIRED_FOR_ACTOR_AWARDED"),
        ("v1.8 NEG explicit null redirect", record("UNSPECIFIED", "ACTOR", state="awarded", redirect=None), "REDIRECT_REQUIRED_FOR_ACTOR_AWARDED"),
        ("v1.8 NEG effect value outside the closed set", record("UNSPECIFIED", "ACTOR", state="awarded", redirect={**aw, "effect": "EXACT"}), "BAD_REDIRECT"),
        ("v1.8 NEG extra key in redirect", record("UNSPECIFIED", "ACTOR", state="awarded", redirect={**aw, "why": "x"}), "BAD_REDIRECT"),
        ("v1.8 NEG awarded but product_view = processes", record("UNSPECIFIED", "ACTOR", state="awarded", redirect=aw, pv="HISTORICAL_PROCESSES"), "AWARDED_PRODUCT_VIEW_MUST_BE_CONTRACTS"),
        ("v1.8 NEG awarded but not EXECUTE", record("UNSPECIFIED", "ACTOR", state="awarded", redirect=aw, disposition="CLARIFY", reason="X"), "AWARDED_MUST_EXECUTE"),
        ("v1.8 NEG redirect on pre-award", record("UNSPECIFIED", "ACTOR", state="published", redirect=aw, **clarify), "REDIRECT_FORBIDDEN_FOR_ACTOR_PREAWARD"),
        ("v1.8 NEG pre-award executes (ACTOR_ALWAYS_WINS shape)", record("UNSPECIFIED", "ACTOR", state="published"), "PREAWARD_MUST_CLARIFY"),
        ("v1.8 NEG pre-award clarified with another reason", record("UNSPECIFIED", "ACTOR", state="open", disposition="CLARIFY", reason="OTHER"), "PREAWARD_MUST_CLARIFY"),
        ("v1.8 NEG pre-award declined", record("UNSPECIFIED", "ACTOR", state="in_evaluation", disposition="DECLINE", reason=PREAWARD_REASON), "PREAWARD_MUST_CLARIFY"),
        ("v1.8 NEG reason used without a pre-award state", record("UNSPECIFIED", "ACTOR", **clarify), f"REASON_{PREAWARD_REASON}_WITHOUT_PREAWARD"),
        ("v1.8 NEG redirect on explicit CONTRACTS ACTOR+awarded", record("CONTRACTS", "ACTOR", state="awarded", redirect=aw), "REDIRECT_ONLY_FOR_QUALIFYING_REQUEST"),
        ("v1.8 NEG redirect on ACTOR alone", record("UNSPECIFIED", "ACTOR", redirect=aw), "REDIRECT_ONLY_FOR_QUALIFYING_REQUEST"),
        # ---- v1.9: states by dataset of existence
        ("v1.9 valid: ACTOR+draft is a CONTRACT state: executes on CONTRACTS, no redirect (P181)", record("UNSPECIFIED", "ACTOR", state="draft"), None),
        ("v1.9 valid: ACTOR+cancelled executes as a contract state", record("UNSPECIFIED", "ACTOR", state="cancelled"), None),
        ("v1.9 valid: ACTOR+terminated executes as a contract state", record("UNSPECIFIED", "ACTOR", state="terminated"), None),
        ("v1.9 valid: ACTOR+in_execution executes as a contract state", record("UNSPECIFIED", "ACTOR", state="in_execution"), None),
        ("v1.9 valid: ACTOR+liquidated executes as a contract state", record("UNSPECIFIED", "ACTOR", state="liquidated"), None),
        ("v1.9 valid: ACTOR+signed executes as a contract state", record("UNSPECIFIED", "ACTOR", state="signed"), None),
        ("v1.9 valid: ACTOR+other:aprobado / suspendido / cedido execute", record("UNSPECIFIED", "ACTOR", states=["other:aprobado", "other:suspendido", "other:cedido"]), None),
        ("v1.9 valid: spelling variant other:borrador is draft", record("UNSPECIFIED", "ACTOR", state="other:borrador"), None),
        ("v1.9 valid: ACTOR+desierto asks (no awardee)", record("UNSPECIFIED", "ACTOR", state="other:desierto", **clarify), None),
        ("v1.9 valid: ACTOR+revocado asks (no awardee)", record("UNSPECIFIED", "ACTOR", state="other:revocado", **clarify), None),
        ("v1.9 valid: ACTOR+published+TIME asks (nothing executes, no notice)", record("UNSPECIFIED", "ACTOR", state="published", time="POSITIVE", **clarify), None),
        ("v1.9 valid: PROCESSES+ACTOR+draft asks, no redirect, no translation", record("PROCESSES", "ACTOR", state="draft", pv=None, **clarify), None),
        ("v1.9 valid: PROCESSES+ACTOR+published asks", record("PROCESSES", "ACTOR", state="published", pv=None, **clarify), None),
        ("v1.9 valid: PROCESSES+ACTOR+desierto asks", record("PROCESSES", "ACTOR", state="other:desierto", pv=None, **clarify), None),
        ("v1.9 valid: PROCESSES+ACTOR+awarded -> v1.7 notice + APPROXIMATION", record("PROCESSES", "ACTOR", state="awarded", redirect=paw), None),
        ("v1.9 valid: PROCESSES+ACTOR (no state) keeps the v1.7 object byte-identical", record("PROCESSES", "ACTOR", redirect=ok), None),
        ("v1.9 valid: ACTOR+TIME -> CONTRACTS + TIME on the signature date notice", record("UNSPECIFIED", "ACTOR", time="POSITIVE", notice=tn), None),
        ("v1.9 NEG STATE_UNIVERSE_TRANSLATION: processes in draft executed on CONTRACTS", record("PROCESSES", "ACTOR", state="draft", pv=None), "PREAWARD_MUST_CLARIFY"),
        ("v1.9 NEG STATE_UNIVERSE_TRANSLATION: ... with the v1.7 redirect", record("PROCESSES", "ACTOR", state="draft", pv=None, redirect=ok, **clarify), "REDIRECT_FORBIDDEN_FOR_ACTOR_PREAWARD"),
        ("v1.9 NEG PROCESSES+ACTOR+published executed", record("PROCESSES", "ACTOR", state="published", pv=None), "PREAWARD_MUST_CLARIFY"),
        ("v1.9 NEG CONTRACT_STATE_CLARIFIED: cancelled clarified instead of executed", record("UNSPECIFIED", "ACTOR", state="cancelled", **clarify), ("CONTRACT_STATE_MUST_EXECUTE", f"REASON_{PREAWARD_REASON}_WITHOUT_PREAWARD")),
        ("v1.9 NEG CONTRACT_STATE_CLARIFIED: terminated clarified with another reason", record("UNSPECIFIED", "ACTOR", state="terminated", disposition="CLARIFY", reason="OTHER"), "CONTRACT_STATE_MUST_EXECUTE"),
        ("v1.9 NEG draft still asking (v1.8 shape, P181 before)", record("UNSPECIFIED", "ACTOR", state="draft", **clarify), ("CONTRACT_STATE_MUST_EXECUTE", f"REASON_{PREAWARD_REASON}_WITHOUT_PREAWARD")),
        ("v1.9 NEG contract state sent to processes (product_view)", record("UNSPECIFIED", "ACTOR", state="terminated", pv="HISTORICAL_PROCESSES"), "CONTRACT_STATE_PRODUCT_VIEW_MUST_BE_CONTRACTS"),
        ("v1.9 NEG contract state with an approximation redirect", record("UNSPECIFIED", "ACTOR", state="draft", redirect=aw), "REDIRECT_ONLY_FOR_QUALIFYING_REQUEST"),
        ("v1.9 NEG DESERTED_EXECUTES: ACTOR+desierto executed", record("UNSPECIFIED", "ACTOR", state="other:desierto"), "PREAWARD_MUST_CLARIFY"),
        ("v1.9 NEG PROCESSES_AWARDED_WITHOUT_APPROXIMATION (v1.7 object only)", record("PROCESSES", "ACTOR", state="awarded", redirect=ok), "REDIRECT_EFFECT_REQUIRED"),
        ("v1.9 NEG PROCESSES+ACTOR+awarded without any redirect", record("PROCESSES", "ACTOR", state="awarded"), "REDIRECT_REQUIRED_FOR_ACTOR_AWARDED"),
        ("v1.9 NEG PROCESSES+ACTOR+awarded with the UNSPECIFIED notice", record("PROCESSES", "ACTOR", state="awarded", redirect=aw), "BAD_REDIRECT"),
        ("v1.9 NEG PROCESSES+ACTOR+awarded missing notice", record("PROCESSES", "ACTOR", state="awarded", redirect={"executed_universe": "CONTRACTS", "effect": "APPROXIMATION"}), "REDIRECT_NOTICE_REQUIRED"),
        ("v1.9 NEG TIME_BEATS_ACTOR: ACTOR+year on processes (PV1 shape)", record("UNSPECIFIED", "ACTOR", time="POSITIVE", notice=tn, pv="HISTORICAL_PROCESSES"), "ACTOR_TIME_PRODUCT_VIEW_MUST_BE_CONTRACTS"),
        ("v1.9 NEG ACTOR+TIME without the signature-date notice", record("UNSPECIFIED", "ACTOR", time="POSITIVE"), "NOTICE_REQUIRED_FOR_ACTOR_TIME"),
        ("v1.9 NEG ACTOR+TIME with another notice", record("UNSPECIFIED", "ACTOR", time="POSITIVE", notice="OTHER"), "BAD_NOTICE"),
        ("v1.9 NEG ACTOR+TIME not executed", record("UNSPECIFIED", "ACTOR", time="POSITIVE", notice=tn, disposition="CLARIFY", reason="X"), "ACTOR_TIME_MUST_EXECUTE"),
        ("v1.9 NEG time notice on ACTOR alone", record("UNSPECIFIED", "ACTOR", notice=tn), "NOTICE_ONLY_FOR_ACTOR_TIME"),
        ("v1.9 NEG time notice on explicit CONTRACTS", record("CONTRACTS", "ACTOR", time="POSITIVE", notice=tn), "NOTICE_ONLY_FOR_ACTOR_TIME"),
        ("v1.9 NEG time notice on PROCESSES+ACTOR (v1.7 owns it)", record("PROCESSES", "ACTOR", time="POSITIVE", notice=tn, redirect=ok), "NOTICE_ONLY_FOR_ACTOR_TIME"),
        # ---- pending: no rule adjudicated, the validator refuses to label it
        ("pending: closed (double reading apertura/contrato)", record("UNSPECIFIED", "ACTOR", state="closed"), PENDING_CODE),
        ("pending: other:seleccionado", record("UNSPECIFIED", "ACTOR", state="other:seleccionado"), PENDING_CODE),
        ("pending: unknown other:* state", record("UNSPECIFIED", "ACTOR", state="other:en_curso"), PENDING_CODE),
        ("pending: negated state with ACTOR", record("UNSPECIFIED", "ACTOR", state="awarded", state_polarity="NEGATIVE"), PENDING_CODE),
        ("pending: positive and NEGATIVE ACTOR + STATE (mixed polarities)", record("UNSPECIFIED", "ACTOR", state="terminated", second_actor="NEGATIVE"), PENDING_CODE),
        ("pending: NEGATIVE ACTOR + STATE", record("UNSPECIFIED", "ACTOR", state="terminated", actor_polarity="NEGATIVE"), PENDING_CODE),
        ("pending: mixed awarded + published", record("UNSPECIFIED", "ACTOR", states=["awarded", "published"]), PENDING_CODE),
        ("pending: mixed contract state + pre-award", record("UNSPECIFIED", "ACTOR", states=["terminated", "open"]), PENDING_CODE),
        ("pending: awarded + TIME (date semantics not adjudicated)", record("UNSPECIFIED", "ACTOR", state="awarded", time="POSITIVE"), PENDING_CODE),
        ("pending: contract state + TIME", record("UNSPECIFIED", "ACTOR", state="terminated", time="POSITIVE"), PENDING_CODE),
        ("pending: PROCESSES+ACTOR+cancelled (process state, not pre-award)", record("PROCESSES", "ACTOR", state="cancelled", redirect=ok), PENDING_CODE),
        ("pending: PROCESSES+ACTOR+terminated (contract-only state)", record("PROCESSES", "ACTOR", state="terminated", redirect=ok), PENDING_CODE),
        ("pending: PROCESSES+ACTOR+closed", record("PROCESSES", "ACTOR", state="closed", redirect=ok), PENDING_CODE),
        ("pending: PROCESSES+NEGATIVE ACTOR+published", record("PROCESSES", "ACTOR", state="published", actor_polarity="NEGATIVE", redirect=ok), PENDING_CODE),
    ]
    failures = 0
    for label, rec, code in cases:
        errors = validate(rec)
        if code is None:
            good = not errors
        else:
            codes = (code,) if isinstance(code, str) else code
            good = len(errors) == len(codes) and all(any(c in e for e in errors) for c in codes)
        failures += not good
        print(("ok   " if good else "FAIL ") + label + ("" if good else f" -> {errors}"))
    table = [  # (hint, universe, class): the classification by dataset of existence (contract v1.9)
        ("open", "UNSPECIFIED", PROCESS_ONLY_PRE_AWARD), ("published", "UNSPECIFIED", PROCESS_ONLY_PRE_AWARD),
        ("in_evaluation", "UNSPECIFIED", PROCESS_ONLY_PRE_AWARD), ("other:desierto", "UNSPECIFIED", PROCESS_ONLY_NO_AWARDEE),
        ("other:revocado", "UNSPECIFIED", PROCESS_ONLY_NO_AWARDEE), ("awarded", "UNSPECIFIED", AWARDED),
        ("draft", "UNSPECIFIED", CONTRACT_STATE), ("cancelled", "UNSPECIFIED", CONTRACT_STATE),
        ("terminated", "UNSPECIFIED", CONTRACT_STATE), ("in_execution", "UNSPECIFIED", CONTRACT_STATE),
        ("liquidated", "UNSPECIFIED", CONTRACT_STATE), ("signed", "UNSPECIFIED", CONTRACT_STATE),
        ("other:Aprobado", "UNSPECIFIED", CONTRACT_STATE), ("other:en aprobación", "UNSPECIFIED", CONTRACT_STATE),
        ("other:suspendido", "UNSPECIFIED", CONTRACT_STATE), ("other:prorrogado", "UNSPECIFIED", CONTRACT_STATE),
        ("other:modificado", "UNSPECIFIED", CONTRACT_STATE), ("other:enviado proveedor", "UNSPECIFIED", CONTRACT_STATE),
        ("other:celebrado", "UNSPECIFIED", CONTRACT_STATE), ("other:liquidado", "UNSPECIFIED", CONTRACT_STATE),
        ("other:en_evaluacion", "UNSPECIFIED", PROCESS_ONLY_PRE_AWARD), ("closed", "UNSPECIFIED", PENDING),
        ("other:seleccionado", "UNSPECIFIED", PENDING), ("other:en_curso", "UNSPECIFIED", PENDING), ("", "UNSPECIFIED", PENDING),
        ("draft", "PROCESSES", PROCESS_ONLY_PRE_AWARD), ("open", "PROCESSES", PROCESS_ONLY_PRE_AWARD),
        ("awarded", "PROCESSES", AWARDED), ("other:desierto", "PROCESSES", PROCESS_ONLY_NO_AWARDEE),
        ("cancelled", "PROCESSES", PENDING), ("terminated", "PROCESSES", PENDING), ("in_execution", "PROCESSES", PENDING),
        ("other:aprobado", "PROCESSES", PENDING), ("closed", "PROCESSES", PENDING),
    ]
    for hint, universe, expected in table:
        got = state_class(hint, universe)
        good = got == expected
        failures += not good
        print(("ok   " if good else "FAIL ") + f"class {universe}:{hint!r} = {expected}" + ("" if good else f" -> {got}"))
    drift = lexicon_drift()
    failures += bool(drift)
    print(("ok   " if not drift else "FAIL ") + "lexicon table matches app/data/estados.py + estado_families.py"
          + ("" if not drift else f" -> {drift}"))
    total = len(cases) + len(table) + 1
    print(f"selftest: {total - failures}/{total} passed", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--selftest"]:
        sys.exit(selftest())
    sys.exit(main(sys.argv[1:]))

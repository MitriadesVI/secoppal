"""M1 PHASE A / A3: one-pass capture of ADMISSION_GATE, PRODUCER, HOST and LEGACY per gold_id.

Run from the worktree root (cwd) with PYTHONPATH=<worktree>:

    python scripts/audits/m1_search/capture_foundation_legacy.py --stage all

Stages (each one resumable and independently re-runnable):
  det        gate + legacy parse/plan on a frozen, empty source (no network at all)
  producer   REAL Foundation producer calls (DeepSeek only; every entry, admitted or not)
  host       deterministic replay of decode -> H7 -> admit_proposal -> project_accepted (no network)
  merge      captures.jsonl (one record per gold_id, normalized legacy dims, per-condition host rows)
  stability  second producer pass over a fixed 40-entry sample and canonical-proposal diff
  all        det, producer, host, merge

Safety: sockets are denied except the host of the configured DeepSeek base_url (datos.gov.co and
every other host raise); Burr telemetry is off; every persistence path of Settings and the
workflow is redirected under --out; the reference date is fixed (2026-09-30). The output holds
real query text: it stays outside git. SECOP is never called (legacy runs on a frozen source).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

ENV_FILE = Path("/Users/rodrigoortiz/Documents/secoppal/.env")
ACTOR_INDEX = Path("/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite")
GOLD_INPUTS = Path("/Users/rodrigoortiz/Documents/tools/secoppal-diagnostics/m1_search_gold/inputs")
DEFAULT_OUT = Path("/Users/rodrigoortiz/Documents/tools/secoppal-diagnostics/m1_search_gold/captures")
INPUT_FILES = ("real_a.jsonl", "real_b.jsonl", "synthetic.jsonl")
TODAY = date(2026, 9, 30)
REFERENCE_INSTANT = datetime(2026, 9, 30, 12, 0, tzinfo=ZoneInfo("America/Bogota"))
MAX_CONCURRENCY = 4
STABILITY_SAMPLE = {"REAL": 30, "SYNTHETIC": 10}
PROVIDER_HOST = urlparse("https://api.deepseek.com/v1").hostname  # = LLMHandler default base_url


# --------------------------------------------------------------------------- sockets
class SocketPolicy:
    """Deny every connection except the resolved addresses of the allowed hosts."""

    def __init__(self, allowed_hosts):
        self.allowed_hosts = set(allowed_hosts)
        self.allowed_ips: set[str] = set()
        self.blocked: list[str] = []
        self._getaddrinfo = socket.getaddrinfo
        self._connect, self._connect_ex = socket.socket.connect, socket.socket.connect_ex

    def install(self) -> "SocketPolicy":
        for host in self.allowed_hosts:
            self._learn(self._getaddrinfo(host, 443))
        policy = self

        def getaddrinfo(host, *a, **k):
            if host not in policy.allowed_hosts:
                policy.blocked.append(f"DNS:{host}")
                raise AssertionError(f"BLOCKED_DNS:{host}")
            result = policy._getaddrinfo(host, *a, **k)
            policy._learn(result)
            return result

        def guarded(original):
            def call(sock, address, *a):
                ip = address[0] if isinstance(address, tuple) else str(address)
                if ip not in policy.allowed_ips:
                    policy.blocked.append(f"CONNECT:{ip}")
                    raise AssertionError(f"BLOCKED_CONNECT:{ip}")
                return original(sock, address, *a)
            return call

        socket.getaddrinfo = getaddrinfo
        socket.socket.connect = guarded(self._connect)
        socket.socket.connect_ex = guarded(self._connect_ex)
        return self

    def _learn(self, infos) -> None:
        self.allowed_ips.update(info[4][0] for info in infos)


def lock_down(allow_provider: bool) -> SocketPolicy:
    policy = SocketPolicy({PROVIDER_HOST} if allow_provider else set()).install()
    import burr.telemetry
    burr.telemetry.g_telemetry_enabled = False
    os.environ["BURR_DISABLED"] = "true"
    return policy


# --------------------------------------------------------------------------- inputs / io
def load_inputs() -> list[dict]:
    rows = []
    for name in INPUT_FILES:
        with open(GOLD_INPUTS / name, encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle if line.strip())
    ids = [r["gold_id"] for r in rows]
    assert len(ids) == len(set(ids)), "duplicate gold_id in inputs"
    return rows


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_jsonl(path: Path, rows) -> None:
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    tmp.replace(path)


def shrink(value, depth=0):
    """JSON-safe copy with bounded lists/strings (candidate lists can be large)."""
    if isinstance(value, dict):
        return {str(k): shrink(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [shrink(v, depth + 1) for v in list(value)[:12]]
        return items + ([f"...+{len(value) - 12}"] if len(value) > 12 else [])
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value[:600] if isinstance(value, str) else value
    return str(value)[:200]


def env_setup(out: Path) -> Path:
    run = out / "run"
    for sub in ("conversations_legacy", "conversations_foundation"):
        (run / sub).mkdir(parents=True, exist_ok=True)
    os.environ["CONVERSATION_STORE_DIR"] = str(run / "conversations_default_unused")
    return run


def build_flow(run: Path, tag: str, *, with_key: bool, foundation: bool):
    from app.config import Settings
    from app.core.conversation_store import ConversationStore
    from app.core.orchestrator import SecopalWorkflow

    common = dict(
        secoppal_natural_alpha_mode="off", secoppal_a2_suppliers_binding_enabled=True,
        secoppal_contract_activity_binding_enabled=True, secoppal_natural_follow_binding_enabled=True,
        secoppal_foundation_mode="foundation_v3" if foundation else "legacy",
        secop_feedback_path=run / f"{tag}_traces.jsonl", secop_feedback_db_path=run / f"{tag}_feedback.db",
        secop_actor_alias_index_path=ACTOR_INDEX,
    )
    if with_key:
        settings = Settings(_env_file=str(ENV_FILE), **common)  # normal load of .env; paths redirected
        assert settings.deepseek_api_key, "DEEPSEEK_API_KEY missing"
    else:
        settings = Settings(_env_file=None, deepseek_api_key=None, **common)
    flow = SecopalWorkflow(settings)
    flow.conv_store = ConversationStore(run / f"conversations_{tag}")
    return flow


# --------------------------------------------------------------------------- stage: det
def owner_view(flow, text: str) -> dict:
    from app.core import foundation_route_owner as fro
    from app.core.foundation_runtime import first_search_ineligibility
    from app.core.workflow.context import HandlerContext

    ctx = HandlerContext(user_query=text, channel="streamlit", today=TODAY)
    reason = first_search_ineligibility(flow, ctx, None)
    parsed = fro._parse(flow, ctx)
    owner = fro.specialized_owner(flow, ctx, parsed)
    p = parsed.params
    return {"eligible": reason is None, "ineligibility_code": reason.value if reason else None,
            "specialized_owner": owner, "admitted": reason is None and owner is None,
            "actor_parse": bool(p.get("actor_scope") or p.get("actor_pending") or p.get("contratista")),
            "parse_params": shrink(p)}


def legacy_view(flow, src, text: str) -> dict:
    from tests.test_natural_alpha_contract import IDENTITY

    before = len(src.calls)
    started = time.perf_counter()
    result = flow.run_query(text, channel="streamlit", user_identity=IDENTITY, today=TODAY)
    calls = src.calls[before:]
    return {
        "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        "route_reason": result.get("route_reason"), "continuation_owner": result.get("_continuation_owner"),
        "needs_clarification": result.get("needs_clarification"), "needs_llm": result.get("needs_llm"),
        "analytical_intent": result.get("analytical_intent"), "intent_type": result.get("intent_type"),
        "dataset_id": result.get("dataset_id"), "outcome": result.get("outcome"),
        "clarification_reason": result.get("clarification_reason"),
        "resolved_params": shrink(result.get("resolved_params") or {}),
        "parsed_params": shrink(result.get("parsed_params") or {}),
        "soql_calls": [{"operation": op, "dataset": ds, "soql": soql} for op, ds, soql in calls[:2]],
        "n_source_calls": len(calls),
    }


def stage_det(out: Path, rows: list[dict]) -> None:
    from tests.test_foundation_route_ownership import RecordingSource

    policy = lock_down(allow_provider=False)
    run = env_setup(out)
    flow = build_flow(run, "legacy", with_key=False, foundation=False)
    src = RecordingSource()
    flow.secop_client, flow.source = src.client, src
    done = {r["gold_id"]: r for r in read_jsonl(out / "gate_legacy.jsonl")}
    results, started = [], time.time()
    for at, row in enumerate(rows):
        if row["gold_id"] in done:
            results.append(done[row["gold_id"]])
            continue
        record = {"gold_id": row["gold_id"], "text_sha256": row["text_sha256"]}
        for key, fn in (("gate", lambda: owner_view(flow, row["text"])),
                        ("legacy", lambda: legacy_view(flow, src, row["text"]))):
            try:
                record[key] = fn()
            except Exception as exc:  # noqa: BLE001 - captured, never silent
                record[key] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
        results.append(record)
        if at % 50 == 0:
            print(f"det {at}/{len(rows)} {time.time() - started:.0f}s", flush=True)
            write_jsonl(out / "gate_legacy.jsonl", results + [done[r["gold_id"]] for r in rows[at + 1:]
                                                               if r["gold_id"] in done])
    write_jsonl(out / "gate_legacy.jsonl", results)
    print("det done", len(results), "blocked sockets:", policy.blocked)


# --------------------------------------------------------------------------- stage: producer
def _is_transport_error(exc: Exception) -> bool:
    import openai
    return isinstance(exc, (openai.APIConnectionError, openai.InternalServerError, openai.RateLimitError))


def produce_one(flow, row: dict) -> dict:
    """One REAL product-producer call (same provider closure, prompt, temperature 0), raw kept."""
    from app.core.foundation_producers import LinguisticProducer, configured_producer
    from app.core.interpretation_contract import InterpretationAcceptance

    inner = configured_producer(flow).provider
    rec: dict = {"gold_id": row["gold_id"], "text_sha256": row["text_sha256"], "attempts": [],
                 "raw_content": None, "model": None}

    def provider(system, payload):
        for attempt in (1, 2):
            started = time.perf_counter()
            try:
                reply = inner(system, payload)
            except Exception as exc:  # noqa: BLE001
                latency = round((time.perf_counter() - started) * 1000, 1)
                rec["attempts"].append({"latency_ms": latency, "error": type(exc).__name__,
                                        "status_code": getattr(exc, "status_code", None)})
                if attempt == 1 and _is_transport_error(exc):
                    continue
                raise
            rec["attempts"].append({"latency_ms": round((time.perf_counter() - started) * 1000, 1),
                                    "error": None})
            rec["raw_content"], rec["model"] = reply.content, reply.model
            return reply

    producer = LinguisticProducer(provider)
    result = producer.produce(row["text"])
    if isinstance(result, InterpretationAcceptance):
        rec.update(status=result.status.value, reason=result.reason)
    else:
        rec.update(status="PROPOSAL", reason=None)
    rec["latency_ms"] = rec["attempts"][-1]["latency_ms"] if rec["attempts"] else None
    rec["total_latency_ms"] = round(sum(a["latency_ms"] for a in rec["attempts"]), 1)
    rec["provider_calls"] = len(rec["attempts"])
    return rec


def run_producer(out: Path, rows: list[dict], target: str) -> None:
    policy = lock_down(allow_provider=True)
    run = env_setup(out)
    flow = build_flow(run, "foundation", with_key=True, foundation=True)
    path = out / target
    done = {r["gold_id"]: r for r in read_jsonl(path)}
    todo = [r for r in rows if r["gold_id"] not in done]
    print(f"producer: {len(done)} done, {len(todo)} pending -> {target}", flush=True)
    started = time.time()
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as pool:
        for at, rec in enumerate(pool.map(lambda r: produce_one(flow, r), todo), 1):
            done[rec["gold_id"]] = rec
            if at % 25 == 0 or at == len(todo):
                print(f"producer {at}/{len(todo)} {time.time() - started:.0f}s", flush=True)
                write_jsonl(path, [done[r["gold_id"]] for r in rows if r["gold_id"] in done])
    write_jsonl(path, [done[r["gold_id"]] for r in rows if r["gold_id"] in done])
    print("producer done; blocked sockets:", policy.blocked)


def stability_sample(rows: list[dict]) -> list[dict]:
    chosen = []
    for slice_name, count in STABILITY_SAMPLE.items():
        pool = sorted((r for r in rows if r["slice"] == slice_name),
                      key=lambda r: hashlib.sha256(("m1-a3-stability:" + r["gold_id"]).encode()).hexdigest())
        chosen.extend(pool[:count])
    return chosen


# --------------------------------------------------------------------------- stage: host (replay)
def _expr_str(node) -> str | None:
    if node is None:
        return None
    if node.operator.value == "ATOM":
        return node.condition_id
    return f"{node.operator.value}({','.join(_expr_str(c) for c in node.children)})"


def describe(proposal) -> dict:
    """Structured, JSON-safe view of a decoded proposal (ids kept, for joining per condition)."""
    return {
        "requests": [{"request_id": r.request_id, "scope_id": r.scope_id, "operation": r.operation,
                      "subject": r.subject, "universe_quote": r.universe_claim, "source_claim": r.source_claim,
                      "expr": _expr_str(r.condition_expression)} for r in proposal.requests],
        "conditions": [{"id": c.condition_id, "request_ids": list(c.request_ids),
                        "dimension": c.dimension.value, "operator": c.operator, "polarity": c.polarity.value,
                        "role": c.role.value, "quotes": [s.quote for s in c.evidence],
                        "unsupported_label": c.unsupported_label} for c in proposal.conditions],
        "relations": [{"kind": r.kind.value, "members": list(r.members)} for r in proposal.relations],
    }


def _canon_expr(node, key_of) -> str | None:
    if node is None:
        return None
    if node.operator.value == "ATOM":
        return key_of[node.condition_id]
    return f"{node.operator.value}({','.join(sorted(_canon_expr(c, key_of) for c in node.children))})"


def canonical(proposal) -> dict:
    """Id-free, order-free form: two runs that mean the same thing compare equal."""
    requests = sorted(proposal.requests, key=lambda r: (r.subject, r.universe_claim or "", r.operation))
    idx = {r.request_id: at for at, r in enumerate(requests)}
    key_of = {}
    for c in proposal.conditions:
        key_of[c.condition_id] = "|".join((
            c.dimension.value, c.operator, c.polarity.value, c.role.value,
            ";".join(sorted(q.casefold() for q in (s.quote for s in c.evidence))),
            ",".join(str(i) for i in sorted(idx[x] for x in c.request_ids))))
    return {
        "requests": [{"subject": r.subject, "operation": r.operation, "universe": (r.universe_claim or "").casefold(),
                      "source": r.source_claim, "expr": _canon_expr(r.condition_expression, key_of)}
                     for r in requests],
        "conditions": sorted(key_of.values()),
        "relations": sorted(f"{r.kind.value}:{sorted(idx[m] for m in r.members)}" for r in proposal.relations),
    }


def _prune(node, remove: set):
    from app.core.interpretation_contract import ConditionExpression, LogicOperator

    if node is None:
        return None
    if node.operator is LogicOperator.ATOM:
        return None if node.condition_id in remove else node
    kids = tuple(k for k in (_prune(c, remove) for c in node.children) if k is not None)
    if not kids:
        return None
    if node.operator is LogicOperator.NOT:
        return ConditionExpression(LogicOperator.NOT, None, kids[:1])
    return kids[0] if len(kids) == 1 else ConditionExpression(node.operator, None, kids)


def without(proposal, remove: set):
    conditions = tuple(c for c in proposal.conditions if c.condition_id not in remove)
    requests = tuple(replace(r, condition_expression=_prune(r.condition_expression, remove))
                     for r in proposal.requests)
    return replace(proposal, requests=requests, conditions=conditions)


def only(proposal, keep: set):
    from app.core.interpretation_contract import ConditionRole

    drop = {c.condition_id for c in proposal.conditions
            if c.condition_id not in keep and c.role is not ConditionRole.REQUESTED_UNIVERSE}
    return without(proposal, drop)


_DIM_REASONS = (  # ProjectionError reason fragment -> dimensions whose condition raises it
    (("UNSUPPORTED_PROCESS_STATE", "UNSUPPORTED_CONTRACT_STATE", "UNSAFE_STATE_COMPLEMENT"), {"STATE"}),
    (("MODALITY",), {"MODALITY"}), (("BUYER_SOURCE_PROJECTION_REQUIRED",), {"BUYER"}),
    (("UNACCREDITED_TERRITORY",), {"GEOGRAPHY"}), (("INVALID_HOST_YEAR",), {"TIME"}),
    (("INVALID_HOST_AMOUNT",), {"VALUE"}), (("UNSUPPORTED_OPPORTUNITY_OVERRIDE",), {"STATE", "TIME"}),
)


def _candidates(reason: str, proposal) -> list[str]:
    ids = {c.condition_id for c in proposal.conditions}
    for prefix in ("UNSUPPORTED_CONDITION:", "MISSING_HOST_POLICY:", "MISSING_HOST_RESOLUTION:",
                   "EMPTY_HOST_RESOLUTION:", "MULTIPLE_HOST_RESOLUTIONS:"):
        if reason.startswith(prefix):
            cid = reason[len(prefix):].split(":")[0]
            return [cid] if cid in ids else []
    for fragments, dims in _DIM_REASONS:
        if any(f in reason for f in fragments):
            return [c.condition_id for c in proposal.conditions if c.dimension.value in dims
                    and c.role.value == "FILTER"]
    return []


def _project(accepted, flow):
    from app.core.foundation_projection import ProjectionError, project_accepted

    try:
        return project_accepted(accepted, reference_instant=REFERENCE_INSTANT, builder=flow.soql_builder), None
    except (ProjectionError, ValueError) as exc:
        return None, str(exc)


def projection_culprits(flow, text, proposal, host_bad: set) -> dict:
    """Which conditions the projection cannot compile, found by removing one culprit at a time."""
    from app.core.foundation_admission import admit_proposal
    from app.core.interpretation_contract import AcceptanceStatus

    current, culprits, level = without(proposal, host_bad), [], None
    for _ in range(len(proposal.conditions) + 2):
        if not any(c.role.value == "FILTER" for c in current.conditions):
            break  # nothing left to project: a global-query refusal here would be an artifact
        accepted = admit_proposal(current, text, entity_resolver=flow.entity_resolver)
        if accepted.status is not AcceptanceStatus.READY:
            level = f"HOST_RESIDUAL:{accepted.status.value}:{accepted.reason}"
            break
        outcome, error = _project(accepted, flow)
        if error is None and outcome.status is AcceptanceStatus.READY:
            break
        reason = error if error is not None else outcome.reason
        cands = _candidates(reason, current) if error is not None else []
        if not cands:
            level = reason
            break
        pick = cands  # default: all of the dimension
        if len(cands) > 1:  # exact: the first candidate whose removal changes the verdict
            for cid in cands:
                trial = admit_proposal(without(current, {cid}), text, entity_resolver=flow.entity_resolver)
                if trial.status is not AcceptanceStatus.READY:
                    continue
                t_out, t_err = _project(trial, flow)
                if t_err != reason:
                    pick = [cid]
                    break
        culprits.extend({"id": cid, "reason": reason} for cid in pick)
        current = without(current, set(pick))
    return {"culprits": culprits, "request_level": level}


def host_replay(flow, row: dict, prod: dict) -> dict:
    from app.core.foundation_admission import admit_proposal
    from app.core.foundation_producers import LinguisticProducer, ProviderReply
    from app.core.foundation_runtime import host_universe
    from app.core.interpretation_contract import (
        AcceptanceStatus, ConditionRole, InterpretationAcceptance)

    text = row["text"]
    out: dict = {"gold_id": row["gold_id"], "producer_status": prod["status"]}
    if prod["status"] != "PROPOSAL":
        out.update(ran=False, final_status=prod["status"], final_reason=prod.get("reason"))
        return out
    proposal = LinguisticProducer(lambda s, p: ProviderReply(prod["raw_content"], prod.get("model"))).produce(text)
    if isinstance(proposal, InterpretationAcceptance):  # the decoder disagrees with the stored status
        out.update(ran=False, final_status=proposal.status.value, final_reason=proposal.reason)
        return out
    out["ran"] = True
    out["proposal_raw"] = describe(proposal)
    out["canonical"] = canonical(proposal)
    universe = host_universe(proposal)
    out["h7"] = {"conflict": universe.conflict, "decisions": [d.to_dict() for d in universe.decisions]}
    eff = universe.proposal
    out["proposal"] = describe(eff)
    if universe.conflict:
        accepted = InterpretationAcceptance(eff, AcceptanceStatus.NEEDS_CLARIFICATION, reason=universe.conflict)
    else:
        accepted = admit_proposal(eff, text, entity_resolver=flow.entity_resolver)
    out["admission"] = {"status": accepted.status.value, "reason": accepted.reason}
    iso = {}
    for c in eff.conditions:
        if c.role is ConditionRole.REQUESTED_UNIVERSE:
            continue
        r = admit_proposal(only(eff, {c.condition_id}), text, entity_resolver=flow.entity_resolver)
        iso[c.condition_id] = {"status": r.status.value, "reason": r.reason}
    out["admission_by_condition"] = iso
    host_bad = {cid for cid, r in iso.items() if r["status"] != "READY"}
    final = accepted
    if accepted.status is AcceptanceStatus.READY:
        outcome, error = _project(accepted, flow)
        if error is not None:
            out["projection"] = {"status": "UNSUPPORTED", "reason": error, "queries": []}
            final = InterpretationAcceptance(eff, AcceptanceStatus.UNSUPPORTED, reason=error)
        else:
            out["projection"] = {
                "status": outcome.status.value, "reason": outcome.reason,
                "datasets": sorted({m.dataset for m in outcome.manifest}),
                "queries": [{"request_id": m.request_id, "dataset": m.dataset, "soql": m.soql}
                            for m in outcome.manifest if m.operation == "query"],
                "dispositions": [{"condition_id": d.condition_id, "disposition": d.disposition.value,
                                  "ref": d.clause_or_policy, "unknown": d.unknown_treatment}
                                 for call in outcome.calls[:1] for d in call.dispositions]}
            if outcome.status is not AcceptanceStatus.READY:
                final = InterpretationAcceptance(eff, outcome.status, reason=outcome.reason)
    else:
        out["projection"] = {"status": "NOT_REACHED", "reason": None, "queries": []}
    out["projection_diagnosis"] = projection_culprits(flow, text, eff, host_bad)
    out["final_status"], out["final_reason"] = final.status.value, final.reason
    return out


def stage_host(out: Path, rows: list[dict]) -> None:
    lock_down(allow_provider=False)
    run = env_setup(out)
    flow = build_flow(run, "foundation_host", with_key=True, foundation=True)
    prods = {r["gold_id"]: r for r in read_jsonl(out / "producer_raw.jsonl")}
    results = []
    for row in rows:
        prod = prods.get(row["gold_id"])
        if prod is None:
            continue
        try:
            results.append(host_replay(flow, row, prod))
        except Exception as exc:  # noqa: BLE001
            results.append({"gold_id": row["gold_id"], "ran": False, "final_status": "HOST_REPLAY_ERROR",
                            "final_reason": f"{type(exc).__name__}: {exc}"[:300]})
    write_jsonl(out / "host.jsonl", results)
    print("host replay done", len(results))


# --------------------------------------------------------------------------- main
STAGES = ("det", "producer", "host", "merge", "stability", "all")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stage", choices=STAGES, default="all")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--limit", type=int, default=None, help="first N entries (smoke tests only)")
    args = ap.parse_args(argv)
    sys.path.insert(0, str(Path.cwd()))
    args.out.mkdir(parents=True, exist_ok=True)
    rows = load_inputs()[: args.limit]
    stage = args.stage
    if stage in ("det", "all"):
        stage_det(args.out, rows)
    if stage in ("producer", "all"):
        run_producer(args.out, rows, "producer_raw.jsonl")
    if stage in ("host", "all"):
        stage_host(args.out, rows)
    if stage in ("merge", "all"):
        stage_merge(args.out, rows)
    if stage == "stability":
        run_stability(args.out, rows)
    return 0


# --------------------------------------------------------------------------- stage: merge
_STATE_CANON = (  # folded legacy/host state word -> Gold STATE value_hint
    ("oferta_abierta", "open"), ("abiert", "open"), ("publicad", "published"), ("terminad", "terminated"),
    ("cerrad", "closed"), ("adjudicad", "awarded"), ("seleccionad", "awarded"), ("en ejecucion", "in_execution"),
    ("contrato_activo", "in_execution"), ("cancelad", "cancelled"), ("descartad", "cancelled"),
    ("en_evaluacion", "other:evaluation"), ("evaluacion", "other:evaluation"),
    ("borrador", "other:draft"), ("aprobad", "other:approved"),
)
_ORDER_CANON = {"valor_desc": "value_desc", "valor_asc": "value_asc", "fecha_desc": "date_desc",
                "fecha_asc": "date_asc"}


def _fold(value) -> str:
    import unicodedata
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode().lower()
    return " ".join(text.split())


def _state_canon(value) -> str:
    folded = _fold(value)
    return next((canon for needle, canon in _STATE_CANON if needle in folded), f"other:{folded}")


def _time_condition(fd, fh) -> dict:
    import calendar
    if fd and fh:
        try:
            d0, d1 = date.fromisoformat(fd), date.fromisoformat(fh)
        except ValueError:
            return {"operator": "between_dates", "value": f"{fd}..{fh}"}
        if d0 == date(d0.year, 1, 1) and d1 == date(d0.year, 12, 31):
            return {"operator": "during_year", "value": str(d0.year)}
        if (d0.day == 1 and (d0.year, d0.month) == (d1.year, d1.month)
                and d1.day == calendar.monthrange(d1.year, d1.month)[1]):
            return {"operator": "during_month", "value": f"{d0.year}-{d0.month:02d}"}
    return {"operator": "between_dates", "value": f"{fd or ''}..{fh or ''}"}


def legacy_dims(lg: dict) -> dict:
    """Normalize what legacy actually resolved (resolved_params) into Gold-comparable conditions."""
    params = lg.get("resolved_params") or {}
    conds: list[dict] = []

    def add(dimension, operator, value, key, polarity="POSITIVE", **extra):
        conds.append({"dimension": dimension, "operator": operator, "polarity": polarity,
                      "value": value, "key": key, **extra})

    for key, polarity in (("objeto", "POSITIVE"), ("objeto_exclude", "NEGATIVE")):
        tokens = params.get(key)
        if tokens:
            flat = (str(t) if not isinstance(t, (list, tuple)) else " ".join(map(str, t)) for t in tokens) \
                if isinstance(tokens, (list, tuple)) else [str(tokens)]
            add("TOPIC", "contains", " ".join(flat), key, polarity)
    buyer = params.get("entidad_resolved") or params.get("entidad") or params.get("entidad_like")
    if buyer:
        add("BUYER", "is", str(buyer), "entidad_resolved" if params.get("entidad_resolved") else "entidad")
    elif params.get("entity_ecosystem_label"):
        add("BUYER", "is", str(params["entity_ecosystem_label"]), "entity_ecosystem", family=True)
    scope = params.get("actor_scope")
    pending = params.get("actor_pending")
    if scope or pending or params.get("contratista"):
        name = ((scope or {}).get("raw_value") or (pending or {}).get("actor_name")
                or (params.get("contratista") or [None])[0])
        add("ACTOR", "is", str(name), "actor_scope" if scope else "actor_pending" if pending else "contratista",
            resolved=bool(scope), status=params.get("actor_resolution_status"))
    if params.get("region") or params.get("departamentos_in"):
        add("GEOGRAPHY", "is", str(params.get("region") or params.get("departamentos_in")), "region", level="REGION")
    elif params.get("departamento_resolved"):
        add("GEOGRAPHY", "is", str(params["departamento_resolved"]), "departamento_resolved", level="DEPARTMENT")
    municipal = params.get("municipal_territory") or {}
    if params.get("ciudad") or municipal.get("status") in {"resolved", "ambiguous"}:
        add("GEOGRAPHY", "is", str(params.get("ciudad") or municipal.get("mention")), "municipal_territory",
            level="MUNICIPALITY", status=municipal.get("status"))
    if params.get("fecha_desde") or params.get("fecha_hasta"):
        add("TIME", **_time_condition(params.get("fecha_desde"), params.get("fecha_hasta")), key="fecha")
    if params.get("valor_min") is not None:
        add("VALUE", "greater_or_equal", str(params["valor_min"]), "valor_min", inclusive_bound=True)
    if params.get("valor_max") is not None:
        add("VALUE", "less_or_equal", str(params["valor_max"]), "valor_max", inclusive_bound=True)
    if params.get("estado_family"):
        add("STATE", "family", _state_canon(params["estado_family"]), "estado_family", raw=params["estado_family"])
    elif params.get("estado"):
        add("STATE", "is", _state_canon(params["estado"]), "estado", raw=params["estado"])
    elif params.get("estado_contrato"):
        add("STATE", "family", _state_canon(params["estado_contrato"][0]), "estado_contrato",
            raw=params["estado_contrato"])
    for key, polarity in (("modalidad_include_procedure_ids", "POSITIVE"), ("modalidad_exclude_procedure_ids", "NEGATIVE")):
        if params.get(key):
            add("MODALITY", "family", ",".join(params[key]), key, polarity)
    for key, polarity in (("contract_type_include", "POSITIVE"), ("contract_type_exclude", "NEGATIVE")):
        if params.get(key):
            add("CONTRACT_TYPE_OR_NATURE", "is", ",".join(params[key]), key, polarity)
    if params.get("ordering_signal"):
        add("ORDER", _ORDER_CANON.get(params["ordering_signal"], params["ordering_signal"]),
            str(params["ordering_signal"]), "ordering_signal")
    scope_ds = params.get("dataset_scope")
    universe = ["PROCESSES", "CONTRACTS"] if scope_ds else (
        ["PROCESSES"] if params.get("dataset") == "procesos" else
        ["CONTRACTS"] if params.get("dataset") == "contratos" else [])
    route = lg.get("route_reason") or ""
    owner = lg.get("continuation_owner") or ""
    handled = ("ENTITY_PROFILE" if route.startswith("entity_profile") else
               "ANALYTICS" if route.startswith("analytics") else
               "ACTOR_PROFILE" if route == "actor_profile" else
               "CLARIFY" if lg.get("needs_clarification") else "SEARCH")
    return {"conditions": conds, "universe": universe,
            "view": "OPPORTUNITIES" if params.get("intent_type") == "opportunity_search" else "RECORDS",
            "handled_as": handled, "owner": owner, "route_reason": route,
            "needs_clarification": bool(lg.get("needs_clarification")),
            "clarification_reason": lg.get("clarification_reason") or None,
            "uses_llm_route": "llm" in route, "has_where": any("WHERE" in c["soql"] for c in lg.get("soql_calls", []))}


def foundation_view(host: dict | None, prod: dict | None, gate: dict) -> dict:
    """Per-condition Foundation rows: what was proposed, how the host judged it alone, and the projection."""
    view = {"producer_status": (prod or {}).get("status", "NOT_RUN"), "producer_reason": (prod or {}).get("reason"),
            "latency_ms": (prod or {}).get("latency_ms"), "provider_calls": (prod or {}).get("provider_calls"),
            "total_latency_ms": (prod or {}).get("total_latency_ms"),
            "attempt_errors": [a["error"] for a in (prod or {}).get("attempts", []) if a.get("error")],
            "model": (prod or {}).get("model"),
            "gate_admitted": gate.get("admitted"), "final_status": (host or {}).get("final_status"),
            "final_reason": (host or {}).get("final_reason"), "conditions": [], "requests": [], "relations": []}
    if not host or not host.get("ran"):
        return view
    diagnosis = host.get("projection_diagnosis") or {}
    proj_bad = {c["id"]: c["reason"] for c in diagnosis.get("culprits", [])}
    iso = host.get("admission_by_condition", {})
    decisions = {d["request_id"]: d for d in host.get("h7", {}).get("decisions", [])}
    for cond in host["proposal"]["conditions"]:
        if cond["role"] == "REQUESTED_UNIVERSE":
            continue
        host_row = iso.get(cond["id"], {})
        view["conditions"].append({**cond, "host_status": host_row.get("status"), "host_reason": host_row.get("reason"),
                                   "projection_reason": proj_bad.get(cond["id"])})
    for request in host["proposal"]["requests"]:
        decision = decisions.get(request["request_id"], {})
        view["requests"].append({**request, "producer_kind": decision.get("producer_kind"),
                                 "effective_kind": decision.get("effective_kind"),
                                 "h7_authority": decision.get("authority"), "h7_rules": decision.get("rules")})
    view["relations"] = host["proposal"]["relations"]
    view.update(admission=host["admission"], h7_conflict=host["h7"]["conflict"],
                projection_status=host["projection"]["status"], projection_reason=host["projection"]["reason"],
                projection_request_level=diagnosis.get("request_level"),
                datasets=host["projection"].get("datasets"),
                queries=[q["soql"] for q in host["projection"].get("queries", [])])
    return view


def stage_merge(out: Path, rows: list[dict]) -> None:
    gl = {r["gold_id"]: r for r in read_jsonl(out / "gate_legacy.jsonl")}
    prods = {r["gold_id"]: r for r in read_jsonl(out / "producer_raw.jsonl")}
    hosts = {r["gold_id"]: r for r in read_jsonl(out / "host.jsonl")}
    merged = []
    for row in rows:
        gid = row["gold_id"]
        base = gl.get(gid, {})
        gate, legacy = base.get("gate", {}), base.get("legacy", {})
        merged.append({
            "gold_id": gid, "slice": row["slice"], "text_sha256": row["text_sha256"], "text": row["text"],
            "freq": row.get("freq"), "gate": {k: v for k, v in gate.items() if k != "parse_params"},
            "legacy": {**({"error": legacy["error"]} if "error" in legacy else {}),
                       "dims": legacy_dims(legacy) if legacy and "error" not in legacy else None,
                       "soql": [c["soql"] for c in legacy.get("soql_calls", [])[:1]]},
            "foundation": foundation_view(hosts.get(gid), prods.get(gid), gate),
        })
    write_jsonl(out / "captures.jsonl", merged)
    print("merge done", len(merged))


# --------------------------------------------------------------------------- stage: stability
def run_stability(out: Path, rows: list[dict]) -> None:
    sample = stability_sample(rows)
    run_producer(out, sample, "producer_raw_rerun.jsonl")
    lock_down(allow_provider=False)
    run = env_setup(out)
    flow = build_flow(run, "foundation_host", with_key=True, foundation=True)
    first = {r["gold_id"]: r for r in read_jsonl(out / "producer_raw.jsonl")}
    second = {r["gold_id"]: r for r in read_jsonl(out / "producer_raw_rerun.jsonl")}
    report = {"sample_ids": [r["gold_id"] for r in sample], "sample": STABILITY_SAMPLE, "compared": 0,
              "status_changed": [], "raw_identical": 0, "canonical_changed": [], "outcome_changed": [],
              "latency_ms": {"first": [], "second": []}}
    by_id = {r["gold_id"]: r for r in sample}
    for gid in report["sample_ids"]:
        a, b = first.get(gid), second.get(gid)
        if not a or not b:
            continue
        report["compared"] += 1
        report["latency_ms"]["first"].append(a.get("latency_ms"))
        report["latency_ms"]["second"].append(b.get("latency_ms"))
        report["raw_identical"] += a.get("raw_content") == b.get("raw_content")
        if a["status"] != b["status"]:
            report["status_changed"].append({"gold_id": gid, "first": a["status"], "second": b["status"]})
            continue
        ha, hb = host_replay(flow, by_id[gid], a), host_replay(flow, by_id[gid], b)
        if ha.get("canonical") != hb.get("canonical"):
            report["canonical_changed"].append(gid)
        if (ha.get("final_status"), ha.get("final_reason")) != (hb.get("final_status"), hb.get("final_reason")):
            report["outcome_changed"].append({"gold_id": gid, "first": [ha.get("final_status"), ha.get("final_reason")],
                                              "second": [hb.get("final_status"), hb.get("final_reason")]})
    report["n_canonical_changed"] = len(report["canonical_changed"])
    report["n_status_changed"] = len(report["status_changed"])
    report["n_outcome_changed"] = len(report["outcome_changed"])
    (out / "stability.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print("stability:", {k: report[k] for k in ("compared", "raw_identical", "n_canonical_changed",
                                                "n_status_changed", "n_outcome_changed")})


if __name__ == "__main__":
    sys.exit(main())

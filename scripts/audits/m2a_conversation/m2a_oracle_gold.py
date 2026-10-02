"""M2A-8a0 oracle, part 1: how the sealed Gold is read.

The seal, the contract and the valid vocabulary are INPUTS verified at run time, never copies kept in the oracle:

* ``verify_seal`` runs ``m2a_gold_seal verify`` (the seal binds the Gold, the partition, the contract and the checker by
  sha256) before any case is built; a broken seal stops the oracle. The verdict is cached only while the files the seal
  binds keep the same size and modification time;
* the reading rules and the vocabulary of reason codes and effects are the sealed checker's
  (``check_m2a_gold_contract``: ``derive_effects``, ``key``, ``side``, ``REASONS``, ``EFFECTS``), which the seal binds;
* the ids, the DEV/TEST partition and the FRONTIER ids come from the seal.

So when the Gold is re-sealed (a new case, a new code) the oracle is regenerated without touching any code.

User decisions about applicability live in ``APPLICABILITY_DECISIONS.json`` and are accepted only when each one cites a
RECORD that exists, is pinned by sha256 and states the decision on one line (decision id, Gold id and owner): a decision
that nobody recorded is refused by ``load_decisions`` (and so by ``load_cases`` and ``audit``).

Nothing here knows the reducer: the oracle is independent of M2A-3 and imports no registered M2a module.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
GOLD_DIR = ROOT / "docs/audits/m2a_conversation/gold"
SEAL_PATH = GOLD_DIR / "M2A_GOLD_SEAL.json"
SCRIPTS_DIR = Path(__file__).resolve().parent
DECISIONS_PATH = ROOT / "docs/audits/m2a_conversation/oracle/APPLICABILITY_DECISIONS.json"
DECISIONS_SCHEMA = "m2a-oracle-applicability-decisions/2"
DECISION_FIELDS = {"owner", "decided_by", "date", "record"}
RECORD_FIELDS = {"doc", "decision_id", "doc_sha256"}


class OracleError(Exception):
    """The Gold, the seal or a decision is not what the oracle reads: a defect, never a case outcome."""


_CHECKER = None


def checker():
    """The sealed ``check_m2a_gold_contract``: its rules ARE the reading of the Gold (derived effects, key, side)."""
    global _CHECKER
    if _CHECKER is None:
        path = Path(__file__).resolve().parent / "check_m2a_gold_contract.py"
        spec = importlib.util.spec_from_file_location("check_m2a_gold_contract", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _CHECKER = module
    return _CHECKER


_SEAL_MODULE = None
_VERIFIED: dict[tuple, bool] = {}


def seal_module():
    global _SEAL_MODULE
    if _SEAL_MODULE is None:
        spec = importlib.util.spec_from_file_location("m2a_gold_seal", SCRIPTS_DIR / "m2a_gold_seal.py")
        _SEAL_MODULE = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_SEAL_MODULE)
    return _SEAL_MODULE


def _fingerprint(real_dir: Path | None) -> tuple:
    """Size and modification time of every file the seal binds: the cache dies with any of them."""
    sealed = seal()
    paths = [SEAL_PATH, *(ROOT / p for p in (*sealed["public_files"], *sealed["interpreting_rules"]))]
    if real_dir is not None:
        paths += [Path(real_dir) / n for n in ("M2A_GOLD_real.jsonl", "FOLLOWUP_FRONTIER_real.jsonl")]
    return tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) if p.exists() else (str(p), None, None) for p in paths)


def verify_seal(real_dir: Path | None = None) -> None:
    """Run ``m2a_gold_seal verify`` (with the private REAL directory when given); a broken seal is a defect."""
    key = (str(real_dir), _fingerprint(real_dir))
    if _VERIFIED.get(key):
        return
    argv, out = ["m2a_gold_seal.py", "verify", *(["--real-dir", str(real_dir)] if real_dir else [])], io.StringIO()
    saved = sys.argv
    try:
        sys.argv = argv
        with contextlib.redirect_stdout(out):
            code = seal_module().main()
    finally:
        sys.argv = saved
    if code != 0:
        raise OracleError(f"the Gold seal does not verify: {out.getvalue().strip()}")
    _VERIFIED[key] = True


def invalidate_seal_cache() -> None:
    _VERIFIED.clear()


def check_reason(reason: str | None) -> None:
    """A ``disposition_reason`` must be in the sealed checker's vocabulary (its codes and parameterised prefixes)."""
    chk = checker()
    if reason is None:
        return
    if reason in chk.RETIRED_REASONS or not (reason in chk.REASONS or reason.startswith(tuple(chk.REASON_PREFIXES))):
        raise OracleError(f"disposition_reason {reason!r} is not in the sealed vocabulary")


def _record_states(decision_id: str, gold_id: str, owner: str, text: str) -> bool:
    """Some line of the record carries the decision id, the Gold id and the owner as whole tokens."""
    def has(token: str, line: str) -> bool:
        return re.search(rf"(?<![\w.-]){re.escape(token)}(?![\w.-])", line) is not None
    return any(has(decision_id, ln) and has(gold_id, ln) and has(owner, ln) for ln in text.splitlines())


def load_decisions(known_ids: set[str] | None = None) -> dict[str, dict]:
    """The user's decisions about applicability, each checked against a pinned record that states it."""
    return verify_decisions(json.loads(DECISIONS_PATH.read_text(encoding="utf-8")), known_ids)


def verify_decisions(data: dict, known_ids: set[str] | None = None) -> dict[str, dict]:
    if data.get("schema") != DECISIONS_SCHEMA or set(data) != {"schema", "decisions"}:
        raise OracleError("APPLICABILITY_DECISIONS.json: unknown schema or keys")
    known = known_ids if known_ids is not None else {
        i for source in ("REAL", "SYNTHETIC") for i in seal()["slices"]["M2A_GOLD"][source]["ids"]}
    for gid, decision in data["decisions"].items():
        if gid not in known:
            raise OracleError(f"decision for {gid}: not an M2A_GOLD id of the seal")
        record = decision.get("record") if isinstance(decision, dict) else None
        if set(decision) != DECISION_FIELDS or not isinstance(record, dict) or set(record) != RECORD_FIELDS or not all(
                type(v) is str and v.strip() for v in (*(decision[k] for k in DECISION_FIELDS - {"record"}),
                                                      *record.values())):
            raise OracleError(f"decision for {gid}: needs owner, decided_by, date and a record "
                              f"{{doc, decision_id, doc_sha256}}, all non-empty")
        doc = (ROOT / record["doc"]).resolve()
        if ROOT not in doc.parents or doc == DECISIONS_PATH.resolve() or not doc.is_file():
            raise OracleError(f"decision for {gid}: record {record['doc']!r} is not an existing document of the repo "
                              f"(the decisions file cannot be its own record)")
        raw = doc.read_bytes()
        if hashlib.sha256(raw).hexdigest() != record["doc_sha256"]:
            raise OracleError(f"decision for {gid}: record {record['doc']!r} does not match its pinned sha256")
        if not _record_states(record["decision_id"], gid, decision["owner"], raw.decode("utf-8")):
            raise OracleError(f"decision for {gid}: record {record['doc']!r} does not state decision "
                              f"{record['decision_id']!r} for {gid} with owner {decision['owner']!r}")
    return data["decisions"]


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canon(value) -> str:
    """The canonical JSON the seal and ``_digest`` use."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def digest(value) -> str:
    return sha(canon(value))


def seal() -> dict:
    return json.loads(SEAL_PATH.read_text(encoding="utf-8"))


def gold_records(real_dir: Path | None, gold_dir: Path = GOLD_DIR) -> list[tuple[str, dict]]:
    """``(source, record)`` for every ``M2A_GOLD`` record, checked against the seal; FRONTIER never enters."""
    chk, sealed = checker(), seal()
    sources = {"SYNTHETIC": gold_dir / "M2A_GOLD_synthetic.jsonl"}
    if real_dir is not None:
        sources["REAL"] = Path(real_dir) / "M2A_GOLD_real.jsonl"
    frontier = {r["gold_id"] for s in ("REAL", "SYNTHETIC") for r in sealed["slices"]["FOLLOWUP_FRONTIER"][s]["records"]}
    out: list[tuple[str, dict]] = []
    for source, path in sources.items():
        records = chk.load(path)
        ids = {r["gold_id"] for r in records}
        if ids != set(sealed["slices"]["M2A_GOLD"][source]["ids"]):
            raise OracleError(f"{source}: the Gold ids differ from the seal")
        if ids & frontier:
            raise OracleError(f"{source}: FOLLOWUP_FRONTIER ids inside M2A_GOLD: {sorted(ids & frontier)}")
        out += [(source, r) for r in records]
    return out


def partition_of(sealed: dict) -> dict[str, str]:
    """``{gold_id: "DEV" | "TEST"}`` exactly as the seal froze it."""
    return {i: side for side in ("DEV", "TEST") for ids in sealed["partition"][side].values() for i in ids}

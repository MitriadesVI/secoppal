"""M2A-8a0 oracle, part 1: how the sealed Gold is read.

The seal, the contract and the valid vocabulary are INPUTS verified at run time, never copies kept in the oracle:

* ``verify_seal`` runs ``m2a_gold_seal verify`` (the seal binds the Gold, the partition, the contract and the checker by
  sha256) before any case is built; a broken seal stops the oracle. The verdict is cached only while the files the seal
  binds keep the same size and modification time;
* the reading rules and the vocabulary of reason codes and effects are the sealed checker's
  (``check_m2a_gold_contract``: ``derive_effects``, ``key``, ``side``, ``REASONS``, ``EFFECTS``), which the seal binds;
* the ids, the DEV/TEST partition and the FRONTIER ids come from the seal.

So when the Gold is re-sealed (a new case, a new code) the oracle is regenerated without touching any code.

Dependencies have ONE authority (U19): contract section 19, through the sealed extraction ``CONTRACT_S19_DEPENDENCIES.json``
(schema ``m2a-contract-s19/2``). ``s19()`` reads it only after the seal verifies and the file matches the sha256 the seal
binds, and refuses an extraction that is missing, of another schema, with an owner or an impact outside its own closed
lists, or inconsistent with itself. There is no local override and no recorded user decision in the oracle.

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
S19_PATH = GOLD_DIR / "CONTRACT_S19_DEPENDENCIES.json"
S19_KEY = str(S19_PATH.relative_to(ROOT))  # how the seal names it
S19_SCHEMA = "m2a-contract-s19/2"
KNOWN_IMPACTS = ("LECTURA", "COMPOSICION")  # the only two impacts the oracle knows how to treat (U19)


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
    except (OSError, ValueError) as error:  # a file the seal binds is missing or unreadable: the seal does not verify
        raise OracleError(f"the Gold seal does not verify: {error}") from error
    finally:
        sys.argv = saved
    if code != 0:
        raise OracleError(f"the Gold seal does not verify: {out.getvalue().strip()}")
    _VERIFIED[key] = True


def invalidate_seal_cache() -> None:
    _VERIFIED.clear()


def host_rules() -> dict[str, tuple[str | None, str | None]]:
    """``{rule: (disposition, code)}`` of contract §6 step 4 (H1...H12), through the sealed checker's own contract parser."""
    chk = checker()
    return {rule: (disposition, code) for rule, disposition, code in chk.contract_text.host_rules(chk._CONTRACT)}


def contract_slot_order() -> tuple[str, ...]:
    """The slots in the order of the rows of contract §4.2 (each polarized dimension POSITIVE then NEGATIVE, VALUE lower then
    upper): the tie-break between refusal causes of one precedence. Read through the sealed checker's contract parser."""
    chk = checker()
    text = chk._CONTRACT
    rows = chk.contract_text.table(chk.contract_text.section(text, "### 4.2 Ranuras", "### 4.3 Identidad"),
                                   "| Ranura | Contenido |")
    names = {"TOPIC", "BUYER", "ACTOR", "GEOGRAPHY", "TIME", "STATE", "MODALITY", "CONTRACT_TYPE_OR_NATURE", "VALUE", "ORDER",
             "CARDINALITY", "UNIVERSE"}
    dimensions: list[str] = []
    for row in rows:
        for d in re.findall(r"[A-Z][A-Z_]+", row[0]):
            if d in names and d not in dimensions:
                dimensions.append(d)
    if set(dimensions) != names:
        raise OracleError(f"the slot table of §4.2 does not name the dimensions the oracle knows: {sorted(set(dimensions))}")
    single = {"VALUE": ("VALUE/lower", "VALUE/upper"), "ORDER": ("ORDER",), "CARDINALITY": ("CARDINALITY",),
              "UNIVERSE": ("UNIVERSE",)}
    return tuple(s for d in dimensions for s in single.get(d, (f"{d}/POSITIVE", f"{d}/NEGATIVE")))


def contract_comparable_units() -> tuple[frozenset[str], str]:
    """§15.2 (A8): the units of the scale of pesos, comparable among themselves, and the one unit comparable only with itself."""
    chk = checker()
    body = chk.contract_text.section(chk._CONTRACT, "### 15.2 A8.1", "### 15.3")
    found = re.search(r"unidades de escala de pesos \(([^)]*)\)", body)
    if found is None or "`SMMLV`" not in body:
        raise OracleError("§15.2 does not say which units are comparable in the shape the oracle reads")
    return frozenset(re.findall(r"`([A-Z_]+)`", found.group(1))), "SMMLV"


def contract_precedence() -> tuple[str, ...]:
    """§6 step 3b: the closed precedence of refusal causes, highest first (the sealed checker's own reading)."""
    return tuple(code.partition(":")[0] for code in checker().PRECEDENCE)


def check_reason(reason: str | None, disposition: str | None = None) -> None:
    """A ``disposition_reason`` must be in the sealed checker's vocabulary: its codes, its parameterised heads WITH a tail
    in the domain the contract declares, and (given the disposition) the pair annex A.3 allows."""
    chk = checker()
    if reason is None:
        return
    if reason in chk.RETIRED_REASONS:
        raise OracleError(f"disposition_reason {reason!r} is retired")
    if reason in chk.REASONS:
        shape = reason
    elif reason.startswith(tuple(chk.REASON_PREFIXES)):
        head, _, tail = reason.partition(":")
        if tail not in chk.REASON_TAILS.get(head + ":", set()):
            raise OracleError(f"disposition_reason {reason!r}: the tail is outside the domain the contract declares")
        shape = head + ":" + {"UNRESOLVED_VALUE": "<DIM>", "OPERATION_CHANGE": "<op>",
                              "UNSUPPORTED_PROPOSAL": "<motivo>"}.get(head, "?")
    else:
        raise OracleError(f"disposition_reason {reason!r} is not in the sealed vocabulary")
    if disposition in ("EXECUTE", "NEW_SEARCH"):
        raise OracleError(f"{disposition} carries no reason (got {reason!r})")
    if disposition in chk.PAIRS and shape not in chk.PAIRS[disposition]:
        raise OracleError(f"disposition_reason {reason!r} is not valid with {disposition} (annex A.3 pairs)")


_S19: dict[tuple, dict] = {}


def s19(real_dir: Path | None = None) -> dict:
    """The sealed extraction of contract section 19, verified at run time (never a copy kept in the oracle).

    Fails (``OracleError``) when the file is missing, does not match the sha256 the seal binds, has another schema, lists an
    impact the oracle does not know or an owner outside its own closed list, names a case the sealed Gold does not have, or
    contradicts itself (a case's impact or owners are not the roll-up of its dependencies)."""
    verify_seal(real_dir)
    if not S19_PATH.is_file():
        raise OracleError("the section 19 extraction is missing: nothing else may decide a dependency (U19)")
    raw, sealed = S19_PATH.read_bytes(), seal()
    entry = sealed["public_files"].get(S19_KEY)
    digest_now = hashlib.sha256(raw).hexdigest()
    if entry is None or entry["sha256"] != digest_now:
        raise OracleError("the section 19 extraction does not match the sha256 the seal binds")
    if digest_now in _S19:
        return _S19[digest_now]
    data = json.loads(raw.decode("utf-8"))
    if data.get("schema") != S19_SCHEMA:
        raise OracleError(f"the section 19 extraction has schema {data.get('schema')!r}, not {S19_SCHEMA!r}")
    contract = sealed["interpreting_rules"].get(data["source"]["contract"])
    if contract is None or contract != data["source"]["contract_sha256"]:
        raise OracleError("the section 19 extraction was made from a contract the seal does not bind")
    if tuple(data["impacts"]) != KNOWN_IMPACTS:
        raise OracleError(f"the section 19 extraction declares impacts {data['impacts']}: the oracle knows {KNOWN_IMPACTS}")
    owners = set(data["owners"])
    rows = {d["dep_id"]: d for d in data["dependencies"]}
    gold_ids = {i for source in ("REAL", "SYNTHETIC") for i in sealed["slices"]["M2A_GOLD"][source]["ids"]}
    for dep in data["dependencies"]:
        if dep["impacto_m2a3"] not in KNOWN_IMPACTS:
            raise OracleError(f"{dep['dep_id']}: unknown impact {dep['impacto_m2a3']!r}")
        if set(dep["owners"]) - owners or not dep["owners"]:
            raise OracleError(f"{dep['dep_id']}: owner outside the closed list of the extraction: "
                              f"{sorted(set(dep['owners']) - owners)}")
    if set(data["case_dependencies"]) - gold_ids:
        raise OracleError("the section 19 extraction names cases the sealed M2A_GOLD does not have")
    for case, deps in data["case_dependencies"].items():
        unknown = [d for d in deps if d not in rows]
        if unknown:
            raise OracleError(f"{case}: dependencies {unknown} are not rows of the extraction")
        impact = "COMPOSICION" if any(rows[d]["impacto_m2a3"] == "COMPOSICION" for d in deps) else "LECTURA"
        if data["case_impacto_m2a3"].get(case) != impact or data["case_owners"].get(case) != sorted(
                {o for d in deps for o in rows[d]["owners"]}):
            raise OracleError(f"{case}: impact or owners are not the roll-up of its dependencies")
    if set(data["case_impacto_m2a3"]) != set(data["case_dependencies"]) or set(data["case_owners"]) != set(
            data["case_dependencies"]):
        raise OracleError("the section 19 extraction indexes different cases per field")
    _S19[digest_now] = data
    return data


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

"""M2A-0 privacy gate: no REAL utterance or personal datum from the private Gold reaches a public tree.

Reads the private REAL material (outside git) and a public tree; prints only counts, file names and token lengths —
never the matched text. Blocking classes:
  - FULL_UTTERANCE: a complete REAL user utterance of >= 5 tokens appears verbatim (word-bounded);
  - ACTOR: a REAL ACTOR value (natural person or contractor) appears;
  - DOCUMENT: a 6–11 digit number from REAL records appears;
  - EMAIL: any e-mail address appears.
Reported, not blocking: complete REAL utterances of <= 4 tokens (generic follow-ups such as the product-decision
examples), which carry no identifying content.

Usage: python scripts/audits/m2a_conversation/privacy_scan_public_tree.py <private_dir> <public_dir> [--json out.json]
Exit 1 if any blocking hit.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import sys
import unicodedata
from pathlib import Path

TEXT_KEYS = {"followup_text", "text", "prior_text", "query", "raw_text", "user_text", "message", "user_query",
             "prior_query"}


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", s)).strip().lower()


def collect(private: Path) -> tuple[set[str], set[str], set[str]]:
    texts, actors, docs = set(), set(), set()
    for f in private.rglob("*.jsonl"):
        for line in f.read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            stack = [(rec, None)]
            while stack:
                x, k = stack.pop()
                if isinstance(x, dict):
                    if x.get("dimension") == "ACTOR" or x.get("slot") == "ACTOR":
                        v = norm(str(x.get("value_hint") or ""))
                        if len(v) >= 5:
                            actors.add(v)
                    stack.extend((v, kk) for kk, v in x.items())
                elif isinstance(x, list):
                    stack.extend((v, k) for v in x)
                elif isinstance(x, str):
                    docs.update(re.findall(r"\b\d{6,11}\b", x))
                    if k in TEXT_KEYS:
                        t = norm(x).strip(" ?¿!¡.")
                        if len(t.split()) >= 3:
                            texts.add(t)
    return texts, actors, docs


def scan(private: Path, public: Path) -> dict:
    texts, actors, docs = collect(private)
    out: dict = {"private_utterances": len(texts), "private_actor_values": len(actors), "private_numbers": len(docs),
                 "public_files": 0, "blocking": collections.defaultdict(list), "generic_short": collections.defaultdict(list)}
    for f in sorted(p for p in public.rglob("*") if p.is_file()):
        out["public_files"] += 1
        body = norm(f.read_text(encoding="utf-8", errors="ignore"))
        rel = str(f.relative_to(public))
        for t in texts:
            if re.search(r"(?<!\w)" + re.escape(t) + r"(?!\w)", body):
                n = len(t.split())
                (out["blocking"] if n >= 5 else out["generic_short"])[rel].append(f"FULL_UTTERANCE:{n}tok")
        for a in actors:
            if a in body:
                out["blocking"][rel].append("ACTOR")
        for d in docs:
            if re.search(r"(?<!\d)" + d + r"(?!\d)", body):
                out["blocking"][rel].append("DOCUMENT")
        if re.search(r"[\w.+-]+@[A-Za-z][\w-]*\.[A-Za-z]{2,}", body):
            out["blocking"][rel].append("EMAIL")
    out["blocking"] = dict(out["blocking"])
    out["generic_short"] = dict(out["generic_short"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("private", type=Path)
    ap.add_argument("public", type=Path)
    ap.add_argument("--json", type=Path)
    a = ap.parse_args()
    out = scan(a.private, a.public)
    text = json.dumps(out, ensure_ascii=False, indent=1, sort_keys=True)
    if a.json:
        a.json.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 1 if out["blocking"] else 0


if __name__ == "__main__":
    sys.exit(main())

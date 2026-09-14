"""Pre-Alpha smoke evaluation; live is explicit, replay has zero network calls."""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.natural_alpha.provider import DeepSeekResponsesProvider  # noqa: E402
from experiments.natural_alpha_v1.evaluation import evaluate, load_frozen  # noqa: E402
from experiments.natural_alpha_v1.replay import load  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--live", action="store_true")
    source.add_argument("--replay", type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("output already exists; preserve the previous run")
    cases, gold, manifest = load_frozen()
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if args.live:
        # Freeze must be committed before a model sees the smoke corpus.
        names = [*manifest["sha256"], "experiments/natural_alpha_v1/manifest.json"]
        for name in names:
            committed = subprocess.check_output(["git", "show", f"HEAD:{name}"], cwd=ROOT)
            if committed != (ROOT / name).read_bytes():
                raise ValueError(f"PRE_LIVE_COMMIT_REQUIRED:{name}")
        key = os.getenv("DEEPSEEK_API_KEY")
        if not key and args.env_file:
            from dotenv import dotenv_values
            key = dotenv_values(args.env_file).get("DEEPSEEK_API_KEY")
        if not key:
            print("PROVIDER_NOT_RUN: no credential available")
            return 2
        provider = DeepSeekResponsesProvider(api_key=key, timeout_seconds=manifest["timeout_seconds"])
    else:
        provider = load(args.replay)
    report = evaluate(cases, gold, provider,
                      progress=lambda key, status: print(f"{key}: {status}", flush=True))
    if args.replay:
        provider.assert_consumed()
    report.update(source="live" if args.live else "replay", candidate_sha=head, frozen_manifest=manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps({"provider_calls": report["provider_calls"], "metrics": report["metrics"],
                      "stopped_reason": report["stopped_reason"]}, ensure_ascii=False))
    return 0 if report["metrics"]["gates_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

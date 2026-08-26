"""Read-only evidence harness; delegates to the unchanged isolated HCA runner.

Run with --out PATH, optionally --corpus PATH. No assertions/oracles are changed.
Observes run_query results and counts calls to the runner's existing SECOP stubs.
No production rows, network calls, or runtime-file edits are introduced.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
from scripts.audits import run_conversation_holistic_audit as runner
from scripts.audits.audit_isolation import isolated_runtime_stores


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    ap.add_argument('--corpus', default=str(ROOT / 'tests/fixtures/audits/conversation_holistic_v1.yaml'))
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    captures = []
    with isolated_runtime_stores() as stores:
        from app.core.orchestrator import SecopalWorkflow
        original_run = SecopalWorkflow.run_query

        def observed_run(self, user_query, *positional, **kwargs):
            calls = []
            originals = {name: getattr(self.secop_client, name) for name in ('query', 'count', 'aggregate')}
            for name, original in originals.items():
                def observe(*a, _name=name, _original=original, **kw):
                    calls.append({'method': _name, 'args': a, 'kwargs': kw})
                    return _original(*a, **kw)
                setattr(self.secop_client, name, observe)
            try:
                result = original_run(self, user_query, *positional, **kwargs)
            finally:
                for name, original in originals.items():
                    setattr(self.secop_client, name, original)
            captures.append({
                'case': kwargs.get('chat_id', '').removeprefix('hca1-'),
                'query': user_query,
                'source_calls': calls,
                'result_keys': sorted(result),
                'result': {k: result.get(k) for k in (
                    'parsed_params', 'resolved_params', 'query_plan', 'needs_clarification',
                    'clarification_reason', 'followup', 'followup_intent_type',
                    'followup_mutation', 'soql_query', 'response', 'route_reason',
                    'result_order_guarantee', 'outcome', 'actor_pending',
                )},
            })
            return result

        SecopalWorkflow.run_query = observed_run
        try:
            runner.run_audit(argparse.Namespace(corpus=args.corpus, only=None), out, stores)
        finally:
            SecopalWorkflow.run_query = original_run
    with (out / 'runtime_capture.jsonl').open('w') as stream:
        for row in captures:
            stream.write(json.dumps(row, ensure_ascii=False, default=str) + '\n')


if __name__ == '__main__':
    main()

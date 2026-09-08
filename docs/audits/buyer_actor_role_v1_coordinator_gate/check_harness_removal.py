"""Bounded removal control on a temporary export; never edit the candidate."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
PYTHON = '/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python'
TARGETS = [
    'tests/test_actor_conversation_corpus.py::test_all_expanded_sequences_mutate_real_query_frames',
    'tests/test_actor_profile_render.py::test_a_specific_aggregate_question_still_gets_its_specific_answer',
]


def main():
    destination = OUT / 'harness_removal_clone'
    destination.mkdir(exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='buyer-harness-removal-') as temporary:
        root = Path(temporary) / 'candidate'
        # APFS copy-on-write keeps this bounded on a nearly full local disk.
        subprocess.run(['cp', '-cR', str(ROOT), str(root)], check=True)
        # The reused runner needs historical Git refs for preservation, read-only.
        (root / '.git').write_text((ROOT / '.git').read_text())
        helper = root / 'tests/buyer_actor_harness_source.py'
        original = helper.read_bytes()
        summary = []
        for label in ('baseline', 'removed'):
            if label == 'removed':
                helper.write_text(original.decode().replace(
                    '        sources.append((flow, conn))',
                    "        for method in ('aggregate', 'with_timeout'):\n"
                    "            monkeypatch.setattr(flow.secop_client, method,\n"
                    "                getattr(type(flow.secop_client), method).__get__(flow.secop_client))\n"
                    '        sources.append((flow, conn))'))
            command = [PYTHON, str(OUT.relative_to(ROOT) / 'run_gate.py'),
                       'focal', *TARGETS, '--run-id', label]
            env = dict(os.environ, BUYER_GATE_STRICT='1')
            result = subprocess.run(command, cwd=root, env=env, capture_output=True)
            (destination / (label + '.runner.log')).write_bytes(result.stdout + result.stderr)
            evidence = root / OUT.relative_to(ROOT)
            denied = json.loads((evidence / f'focal_{label}_focused_external.json').read_text())['denied']
            summary.append({'label': label, 'command': command, 'exit': result.returncode,
                            'denied_nodeids': sorted({r['nodeid'] for r in denied}),
                            'attempt_count': len(denied)})
            for path in evidence.glob(f'focal_{label}*'):
                if path.is_dir():
                    shutil.copytree(path, destination / path.name)
                else:
                    shutil.copyfile(path, destination / path.name)
        helper.write_bytes(original)
        restored = hashlib.sha256(helper.read_bytes()).hexdigest() == hashlib.sha256(original).hexdigest()
        passed = (summary[0]['exit'] == 0 and not summary[0]['attempt_count']
                  and summary[1]['exit'] != 0 and len(summary[1]['denied_nodeids']) == 2 and restored)
        (destination / 'result.json').write_text(json.dumps(
            {'runs': summary, 'helper_restored': restored, 'pass': passed}, indent=2) + '\n')
        print(json.dumps({'pass': passed, 'runs': summary}, indent=2))  # noqa: T201
        return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())

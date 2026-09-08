"""Frozen file hashes and rule/path/source parity; no repinning historical evidence."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
BASE = 'af3d0c7a26182fc26599a4b764ec60fe600b17f8'
CANDIDATE = 'af8f0a3407102dcb807089e87e470d754b835d61'


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    files = git('ls-tree', '-r', '--name-only', BASE).decode().splitlines()
    allowed = set(git('diff', '--name-only', BASE, '--', 'app').decode().splitlines())
    preservation = {}
    for name in files:
        if name not in allowed:
            frozen = git('show', f'{BASE}:{name}')
            current = (ROOT / name).read_bytes()
            preservation[name] = dict(frozen=sha(frozen), current=sha(current), preserved=frozen == current)
    assert all(item['preserved'] for item in preservation.values())
    audit_delta = git('diff', '--name-status', CANDIDATE, BASE).decode().splitlines()
    assert all(line.startswith('A\t') for line in audit_delta)
    (OUT / 'preservation.json').write_text(json.dumps(dict(base=BASE, candidate=CANDIDATE,
        audit_delta=audit_delta, modified_runtime=sorted(allowed), files=preservation), indent=2) + '\n')
    baseline = json.loads((ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent/semgrep_final.json').read_text())
    current = json.loads((OUT / 'semgrep_final.json').read_text())
    def identity(item, frozen):
        text = git('show', f'{BASE}:{item["path"]}').decode() if frozen else (ROOT / item['path']).read_text()
        fragment = '\n'.join(text.splitlines()[item['start']['line'] - 1:item['end']['line']])
        return (item['check_id'], item['path'], fragment)
    before, after = {identity(x, True) for x in baseline['results']}, {identity(x, False) for x in current['results']}
    (OUT / 'semgrep_delta.json').write_text(json.dumps(dict(baseline=sorted(before), current=sorted(after),
        new=sorted(after-before), removed=sorted(before-after)), indent=2) + '\n')
    assert not after-before
    sys.stdout.write(f'PRESERVED={len(preservation)}; SEMGREP_BASE={len(before)} CURRENT={len(after)} NEW={len(after-before)}\n')


if __name__ == '__main__':
    main()

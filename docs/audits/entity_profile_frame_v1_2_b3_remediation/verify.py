"""Verify scope against frozen Git blobs, literal logs and a fresh baseline scan."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
BASE = 'a7e12ebf037f962d356bb34130e180e97f97aa0e'
CANDIDATE = '5d715ba9e7d67aa05f703d2a280fdf9e2efde3b1'
BIN = Path('/Users/rodrigoortiz/Documents/secoppal/.venv/bin')
CHANGED = {'app/core/workflow/publication_io.py', 'app/core/entity_profile/frame_sections.py',
           'app/core/entity_profile/render.py'}
NEW = {'app/core/entity_profile/supplier_rankings.py'}
PREFIX = 'docs/audits/entity_profile_frame_v1_2_b3_remediation/'


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def preserve():
    subprocess.run(['git', 'merge-base', '--is-ancestor', CANDIDATE, BASE], cwd=ROOT, check=True)
    delta = git('diff', '--name-status', CANDIDATE, BASE).decode().splitlines()
    assert all(line.startswith('A\tdocs/audits/entity_profile_frame_v1_2_b2_independent/') for line in delta)
    files = git('ls-tree', '-rz', '--name-only', BASE).decode().split('\0')
    files = [p for p in files if p and p not in CHANGED]
    request = ''.join(f'{BASE}:{p}\n' for p in files).encode()
    batch = subprocess.run(['git', 'cat-file', '--batch'], input=request, cwd=ROOT, capture_output=True, check=True).stdout
    offset, hashes = 0, {}
    for filename in files:
        end = batch.index(b'\n', offset)
        size = int(batch[offset:end].split()[-1])
        raw = batch[end + 1:end + 1 + size]
        offset = end + size + 2
        assert (ROOT/filename).read_bytes() == raw, filename
        hashes[filename] = hashlib.sha256(raw).hexdigest()
    current = git('diff', '--name-only', BASE, 'HEAD').decode().splitlines()
    assert all(p in CHANGED | NEW or p.startswith(PREFIX) for p in current), current
    for filename, expected in json.loads((OUT/'original_sha256.json').read_text()).items():
        assert hashlib.sha256((ROOT/filename).read_bytes()).hexdigest() == expected, filename
    (OUT/'preservation.json').write_text(json.dumps(dict(base=BASE, candidate=CANDIDATE,
        head=git('rev-parse', 'HEAD').decode().strip(), preserved=hashes, changed=current), indent=2)+'\n')


def scan_base():
    with tempfile.TemporaryDirectory(prefix='secoppal-b3-verify-') as directory:
        root = Path(directory)
        archive = root/'base.tar'
        subprocess.run(['git', 'archive', '--output', str(archive), BASE], cwd=ROOT, check=True)
        subprocess.run(['tar', '-xf', str(archive), '-C', str(root)], check=True)
        archive.unlink()
        subprocess.run(['git', 'init', '-q'], cwd=root, check=True)
        env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(root), str(root/'tests'),
                   str(root/'scripts/audits/entity_profile_frame_v1_2_b_independent')]),
                   PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false')
        commands = [('semgrep_base', [str(BIN/'semgrep'), '--config', '.semgrep/secoppal-laws.yml',
                     'app/', '--metrics=off', '--json', '--output', str(OUT/'semgrep_base.json')]),
                    ('clock_base', [str(BIN/'python'), '-m', 'pytest', '-q',
                     'tests/test_entity_profile_frame_integration_contract.py::test_a_relative_period_does_not_get_reinterpreted_on_reload'])]
        for name, args in commands:
            result = subprocess.run(args, cwd=root, env=env, text=True, capture_output=True, check=False)
            (OUT/(name+'.txt')).write_text(shlex.join(args)+'\n'+result.stdout+result.stderr+f'\nEXIT_CODE={result.returncode}\n')
        def key(item, source):
            fragment = '\n'.join((source/item['path']).read_text().splitlines()[item['start']['line']-1:item['end']['line']])
            return item['check_id'], item['path'], fragment
        before = {key(item, root) for item in json.loads((OUT/'semgrep_base.json').read_text())['results']}
        after = {key(item, ROOT) for item in json.loads((OUT/'semgrep.json').read_text())['results']}
        delta = dict(baseline=sorted(before), current=sorted(after), added=sorted(after-before), removed=sorted(before-after))
        (OUT/'semgrep_delta.json').write_text(json.dumps(delta, indent=2)+'\n')
        assert not delta['added']


def archive_logs():
    manifest = {}
    for path in OUT.iterdir():
        if path.suffix not in ('.txt', '.xml'):
            continue
        raw = path.read_bytes()
        zipped = path.with_name(path.name + '.gz')
        zipped.write_bytes(gzip.compress(raw, mtime=0))
        assert gzip.decompress(zipped.read_bytes()) == raw
        manifest[path.name] = dict(archive=zipped.name, uncompressed_bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        path.unlink()
    previous = OUT/'literal_log_manifest.json'
    old = json.loads(previous.read_text()) if previous.exists() else {}
    previous.write_text(json.dumps(old | manifest, indent=2)+'\n')


if __name__ == '__main__':
    import sys
    {'preserve': preserve, 'base': scan_base, 'archive': archive_logs}[sys.argv[1]]()

"""Read every frozen artifact and verify preservation without rewriting evidence."""
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent'
CANDIDATE = 'af8f0a3407102dcb807089e87e470d754b835d61'
TESTED = '64f9836edf96a89051ffc4eb21b0b3397091228c'
BASE = '708ea3b3f609af4d9d3abe471f6963ee23513d7a'
RUNTIME_B = '1b2a85f89e5d556f1de392e11b9da437da6540d1'


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    for first, last in ((RUNTIME_B, BASE), (BASE, TESTED), (TESTED, CANDIDATE)):
        subprocess.run(['git', 'merge-base', '--is-ancestor', first, last], cwd=ROOT, check=True)
    assert not git('diff', '--name-only', TESTED, CANDIDATE, '--', 'app', 'tests', 'streamlit_app.py')
    files = git('ls-tree', '-rz', CANDIDATE).split(b'\0')
    hashes, changed = {}, []
    for entry in files:
        if not entry:
            continue
        meta, raw_path = entry.split(b'\t', 1)
        path = raw_path.decode()
        mode, kind, blob = meta.split()
        if kind != b'blob':
            continue
        content = (ROOT / path).read_bytes()
        hashes[path] = sha(content)
        current_blob = hashlib.sha1(b'blob ' + str(len(content)).encode() + b'\0' + content).hexdigest()
        if current_blob != blob.decode():
            changed.append(path)
    assert not changed, changed
    author = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_remediation'
    originals = json.loads((author / 'original_integrity.json').read_text())
    assert all(hashes[path] == digest for path, digest in originals['sha256'].items())
    assert not git('diff', '--name-only', BASE, CANDIDATE, '--', *originals['sha256'])
    assert not git('diff', '--name-only', RUNTIME_B, BASE, '--', 'app', 'streamlit_app.py')
    protected = ['AGENTS.md', 'docs/orquestacion/CARRILES.md', 'docs/decisions', 'docs/decisions_log.md',
                 'docs/architecture/ENTITY_PROFILE_FRAME_ARCHITECTURE_V1.md', 'app/config.py',
                 '.semgrep', 'ruff.toml', 'Makefile', 'data']
    assert not git('diff', '--name-only', BASE, CANDIDATE, '--', *protected)
    manifest = json.loads((author / 'literal_log_manifest.json').read_text())
    for entry in manifest.values():
        raw = gzip.decompress((author / entry['archive']).read_bytes())
        assert sha(raw) == entry['sha256'] and len(raw) == entry['bytes']
    inventory = {}
    for path in sorted(author.iterdir()):
        raw = path.read_bytes()
        content = gzip.decompress(raw) if path.suffix == '.gz' else raw
        text = content.decode()
        item = dict(bytes=len(raw), decoded_bytes=len(content), sha256=sha(raw))
        if path.suffix == '.json':
            parsed = json.loads(text)
            item['keys'] = list(parsed) if isinstance(parsed, dict) else ['list']
            if isinstance(parsed, dict) and 'mutations' in parsed:
                item['mutations'] = [dict(id=m['id'], verdict=m['verdict'],
                    baseline=m.get('baseline_exit'), restored=m.get('restored'),
                    evidence=[line for line in m.get('output', '').splitlines()
                              if re.search(r'^(FAILED|E |ERROR)|passed|failed', line)])
                    for m in parsed['mutations']]
        elif path.name.endswith('.log.gz'):
            item['outcomes'] = [line for line in text.splitlines() if re.search(
                r'^(FAILED|E |ERROR)|passed|failed|findings|DETECTED|INCONCLUSIVE|restored', line)]
        inventory[path.name] = item
    (OUT / 'candidate_sha256.json').write_text(json.dumps(hashes, indent=2) + '\n')
    (OUT / 'artifact_inventory.json').write_text(json.dumps(inventory, indent=2, ensure_ascii=False) + '\n')
    report = dict(candidate=CANDIDATE, tested=TESTED, base=BASE, runtime_b=RUNTIME_B,
                  ancestry=True, tested_runtime_and_tests_identical=True,
                  all_candidate_files=len(hashes), changed=changed,
                  original_bytes_match_base=True, protected_authorities_unchanged=True,
                  author_originals_verified=originals['files'], literal_archives_verified=len(manifest))
    (OUT / 'integrity.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()

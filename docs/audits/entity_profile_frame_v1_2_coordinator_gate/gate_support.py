"""Gate environment, preservation and semantic Semgrep comparison."""
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
BASE = '616826231d5d2bf25c47640e5dcf4f1bbc491326'
RUNTIME = '4edc248e60610efdbeebb3cb8010bc7b59445cad'
BIN = Path('/Users/rodrigoortiz/Documents/secoppal/.venv/bin')
ACTIVE = OUT / 'active'


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def write(name, data, *, destination=OUT):
    (destination / name).write_text(json.dumps(data, indent=2, ensure_ascii=False) + '\n')


def environment(scratch, name, network, *, output=OUT):
    env = dict(os.environ)
    for key in list(env):
        if any(token in key for token in ('API_KEY', 'TOKEN', 'PASSWORD', 'SECRET')):
            env.pop(key)
    for key in ('PYTEST_ADDOPTS', 'PYTEST_PLUGINS', 'FRAME_REVIEW_OUTPUT', 'AUDIT_FLAG_LOG',
                'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        env.pop(key, None)
    paths = [ROOT, ROOT / 'tests', OUT, *[ACTIVE / group for group in
             ('b2_independent', 'b3_remediation', 'b3_independent')],
             ROOT / 'scripts/audits/entity_profile_frame_v1_2_b_independent']
    env.update(PYTHONPATH=os.pathsep.join(map(str, paths)), PYTHONDONTWRITEBYTECODE='1',
               SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false',
               SECOP_FEEDBACK_PATH=str(scratch / 'feedback.jsonl'),
               SECOP_FEEDBACK_DB_PATH=str(scratch / 'feedback.sqlite'),
               CONVERSATION_STORE_DIR=str(scratch / 'conversations'),
               SECOP_ACTOR_ALIAS_INDEX_PATH=str(scratch / 'missing-index.sqlite'),
               COORDINATOR_OUTPUT=str(output), COORDINATOR_COMPONENT=name,
               COORDINATOR_NETWORK=network, HYPOTHESIS_STORAGE_DIRECTORY=str(scratch / 'hypothesis'),
               FRAME_REVIEW_OUTPUT=str(output / 'frame_observations'))
    (output / 'frame_observations').mkdir(exist_ok=True)
    return env


def preservation():
    paths = git('ls-tree', '-r', '--name-only', BASE, 'app', 'streamlit_app.py', 'docs/audits').splitlines()
    hashes = {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in paths}
    originals = json.loads((OUT / 'reconciliation.json').read_text())['files']
    failures = []
    for row in originals:
        source = ROOT / row.get('archive', row['original'])
        if hashlib.sha256(source.read_bytes()).hexdigest() != row['sha256']:
            failures.append(str(source))
    return {'hashes': hashes, 'original_failures': failures}


def semgrep_delta(base, current, base_root):
    def findings(data, root):
        rows = []
        for item in data.get('results', []):
            path = item['path']
            if Path(path).is_absolute():
                path = str(Path(path).relative_to(root))
            lines = (root / path).read_text().splitlines()
            fragment = '\n'.join(lines[item['start']['line']-1:item['end']['line']]).strip()
            rows.append({'rule': item['check_id'], 'path': path, 'fragment': fragment,
                         'line': item['start']['line']})
        return rows
    before, after = findings(base, base_root), findings(current, ROOT)
    def key(row):
        return row['rule'], row['path'], row['fragment']
    old = Counter(map(key, before))
    new = Counter(map(key, after)) - old
    moved = [{'before': b, 'after': a} for b in before for a in after
             if key(b) == key(a) and b['line'] != a['line']]
    errors = base.get('errors', []) + current.get('errors', [])
    complete = bool(base.get('paths', {}).get('scanned')) and bool(current.get('paths', {}).get('scanned')) and not errors
    return {'baseline': before, 'candidate': after, 'new': [list(k) for k in new.elements()],
            'relocations': moved, 'scanner_errors': errors, 'complete': complete,
            'pass': complete and not new, 'baseline_is_red': bool(before)}


def code_snapshot():
    paths = git('ls-files').splitlines()
    return {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in paths
            if Path(p).suffix in ('.py', '.toml', '.ini', '.yml', '.yaml')
            or p in ('Makefile', 'AGENTS.md')}

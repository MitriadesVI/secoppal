"""Frozen blobs, author log checksums, baseline scan and clock counterfactual."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from audit_output import emit
import tempfile

from audit_runner import ROOT, OUT, ENV, BIN, CANDIDATE

BASE = 'af3d0c7a26182fc26599a4b764ec60fe600b17f8'
TESTED = 'aaa9168b75a5633469af507a1cf7c1c2e2c1ce00'
RUNTIME_B1 = 'af8f0a3407102dcb807089e87e470d754b835d61'


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    integrity = {'candidate': CANDIDATE, 'tested': TESTED, 'base': BASE, 'runtime_b1': RUNTIME_B1}
    for ancestor in (BASE, TESTED, RUNTIME_B1):
        subprocess.run(['git', 'merge-base', '--is-ancestor', ancestor, CANDIDATE], cwd=ROOT, check=True)
    final_delta = git('diff', '--name-only', TESTED, CANDIDATE).decode().splitlines()
    assert all(p.startswith('docs/audits/entity_profile_frame_v1_2_b2_remediation/') for p in final_delta)
    integrity['tested_to_final'] = final_delta
    integrity['trees'] = {ref: {p: git('rev-parse', f'{ref}:{p}').decode().strip() for p in ('app', 'tests', 'streamlit_app.py')} for ref in (TESTED, CANDIDATE)}
    assert integrity['trees'][TESTED] == integrity['trees'][CANDIDATE]
    preserved = {}
    changed_app = set(git('diff', '--name-only', BASE, CANDIDATE, '--', 'app').decode().splitlines())
    for name in git('ls-tree', '-r', '--name-only', BASE).decode().splitlines():
        if name in changed_app:
            continue
        frozen = git('show', f'{BASE}:{name}')
        current = (ROOT/name).read_bytes()
        assert current == frozen, name
        preserved[name] = sha(current)
    integrity['base_preserved_files'] = preserved
    inventory = {}
    for directory in ('entity_profile_frame_v1_2_b2_remediation', 'entity_profile_frame_v1_2_b1_independent'):
        folder = ROOT/'docs/audits'/directory
        manifest = json.loads((folder/'literal_log_manifest.json').read_text())
        for name, expected in manifest.items():
            raw = gzip.decompress((folder/expected.get('archive', name)).read_bytes())
            assert len(raw) == expected.get('uncompressed_bytes', expected.get('bytes')) and sha(raw) == expected['sha256'], name
            inventory[f'{directory}/{name}'] = dict(sha256=sha(raw), bytes=len(raw), summaries=[s for s in raw.decode().splitlines() if ' passed' in s or 'failed,' in s or 'EXIT_CODE' in s][-5:])
    integrity['verified_literal_logs'] = inventory
    (OUT/'integrity.json').write_text(json.dumps(integrity, indent=2) + '\n')
    with tempfile.TemporaryDirectory(prefix='secoppal-b2-base-audit-') as temp:
        root = Path(temp)
        archive = root/'base.tar'
        subprocess.run(['git', 'archive', '--output', str(archive), BASE], cwd=ROOT, check=True)
        subprocess.run(['tar', '-xf', str(archive), '-C', str(root)], check=True)
        archive.unlink()
        # Local throwaway root makes Semgrep path exclusions comparable.
        subprocess.run(['git', 'init', '-q'], cwd=root, check=True)
        env = dict(ENV, PYTHONPATH=str(root/'scripts/audits/entity_profile_frame_v1_2_b_independent'))
        clock = [str(BIN/'python'), '-m', 'pytest', '-q', 'tests/test_entity_profile_frame_integration_contract.py::test_a_relative_period_does_not_get_reinterpreted_on_reload']
        scan = [str(BIN/'semgrep'), '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json', '--output', str(OUT/'semgrep_base.json')]
        for name, args in [('clock_base', clock), ('semgrep_base', scan)]:
            result = subprocess.run(args, cwd=root, env=env, capture_output=True, text=True, check=False)
            (OUT/(name+'.txt')).write_text(repr(args)+'\n'+result.stdout+result.stderr+f'\nEXIT_CODE={result.returncode}\n')
            emit(name, result.returncode, flush=True)
        base_findings = json.loads((OUT/'semgrep_base.json').read_text())['results']
        current_findings = json.loads((OUT/'semgrep.json').read_text())['results']
        def identity(item, source):
            fragment = '\n'.join((source/item['path']).read_text().splitlines()[item['start']['line']-1:item['end']['line']])
            return item['check_id'], item['path'], fragment
        before = {identity(x, root) for x in base_findings}
        after = {identity(x, ROOT) for x in current_findings}
        delta = dict(baseline=sorted(before), current=sorted(after), added=sorted(after-before), removed=sorted(before-after))
        (OUT/'semgrep_delta.json').write_text(json.dumps(delta, indent=2)+'\n')
        assert not delta['added']
    emit('INTEGRITY', len(preserved), 'literal_logs', len(inventory), 'SEMGREP', len(before), len(after), len(after-before), flush=True)


if __name__ == '__main__':
    main()

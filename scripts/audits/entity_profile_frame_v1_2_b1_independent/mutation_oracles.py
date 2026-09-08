"""Adjudicate the two label-only kills with additional product assertions."""
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent'
TEST = 'tests/test_v12b1_independent_mutation_oracles.py'
SHA = 'af8f0a3407102dcb807089e87e470d754b835d61'


def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.glob('app/**/*.py')}


def main():
    campaign = runpy.run_path(str(ROOT / 'scripts/audits/entity_profile_frame_v1_2_b/mutation_campaign.py'))
    mutations = [item for item in campaign['MUTATIONS'] if item.mutation_id.startswith(('M2-', 'M3-'))]
    entries = []
    with tempfile.TemporaryDirectory(prefix='secoppal-b1-independent-oracles-') as temporary:
        root = Path(temporary)
        archive = root / 'source.tar'
        with archive.open('wb') as output:
            subprocess.run(['git', 'archive', SHA], cwd=ROOT, stdout=output, check=True)
        with tarfile.open(archive) as source:
            source.extractall(root, filter='data')
        shutil.copyfile(ROOT / TEST, root / TEST)
        before = hashes(root)
        env = dict(os.environ, PYTHONPATH=str(root / 'scripts/audits/entity_profile_frame_v1_2_b_independent'),
                   PYTEST_ADDOPTS='-p offline_guard', PYTHONDONTWRITEBYTECODE='1')
        for item in mutations:
            selector = 'corrupt_queue' if item.mutation_id.startswith('M2') else 'profile_seed'
            args = [sys.executable, '-B', '-m', 'pytest', '-q', '-s', '--tb=short', TEST, '-k', selector]
            baseline = subprocess.run(args, cwd=root, env=env, capture_output=True, text=True, check=False, timeout=30)
            assert baseline.returncode == 0
            path = root / item.path
            original = path.read_bytes()
            assert original.decode().count(item.old) == 1
            try:
                path.write_text(original.decode().replace(item.old, item.new, 1))
                mutant = subprocess.run(args, cwd=root, env=env, capture_output=True, text=True, check=False, timeout=30)
            finally:
                path.write_bytes(original)
            entries.append(dict(id=item.mutation_id, baseline_exit=baseline.returncode, mutant_exit=mutant.returncode,
                baseline=baseline.stdout + baseline.stderr, mutant=mutant.stdout + mutant.stderr,
                restored=before == hashes(root)))
            assert entries[-1]['restored']
    (OUT / 'mutation_product_oracles.json').write_text(json.dumps(dict(sha=SHA, entries=entries), indent=2, ensure_ascii=False) + '\n')
    for entry in entries:
        print(entry['id'], entry['baseline_exit'], entry['mutant_exit'], entry['restored'])


if __name__ == '__main__':
    main()

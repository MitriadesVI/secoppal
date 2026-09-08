"""B.3 ports of nine B.2 mutations plus four required residual mutations."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
SHA = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
AUTHOR = ROOT / 'docs/audits/entity_profile_frame_v1_2_b2_remediation/mutate.py'
spec = importlib.util.spec_from_file_location('b2_mutations', AUTHOR)
author = importlib.util.module_from_spec(spec)
spec.loader.exec_module(author)
MUTATIONS = list(author.MUTATIONS)
name, path, _, replacement, targets = MUTATIONS[-1]
MUTATIONS[-1] = (name, path, "if by_value and section_ranking(profile.suppliers, 'value').accredited:", replacement, targets)
PUB = 'app/core/workflow/publication_io.py'
OWN = 'docs/audits/entity_profile_frame_v1_2_b3_remediation/'
MUTATIONS += [
    ('B3-lost-prefix-nonconfirmation', PUB, 'blocked_prefix = barrier.exists()', 'blocked_prefix = False',
     [OWN + 'test_publication_ports.py::test_failed_prefix_port[True-True]']),
    ('B3-physical-length-only-recovery', PUB,
     '''                    if blocked_prefix:
                        # The offset delimits our append, NOT a confirmed prefix.
                        # Preserve the earlier barrier's rejection in our own
                        # suffix when that barrier cannot be recreated. The
                        # strict envelope reader rejects this incomplete record.
                        handle.seek(offset)
                        handle.write(b'\\n{"publication_incomplete":true}\\n')
''', '', [OWN + 'test_publication_chains.py::test_integrated_three_failures_reload_capabilities_and_recovery[fsync-False]']),
    ('B3-global-value-warning', 'app/core/entity_profile/frame_sections.py',
     '        warnings.extend(ranking_warnings(ranking, decision))',
     '''        warnings.extend(ranking_warnings(ranking, decision))
        if available and not decision.amounts_valid:
            warnings.append('El ranking por valor no está acreditado.')''',
     [OWN + 'test_supplier_ports.py::test_ranking_claim_and_published_warning_are_consistent[count-overview]',
      OWN + 'test_supplier_ports.py::test_ranking_claim_and_published_warning_are_consistent[count-suppliers]']),
    ('B3-render-reaccredits', 'app/core/entity_profile/render.py',
     "if by_value and section_ranking(profile.suppliers, 'value').accredited:",
     'if by_value and all(item.total_contract_value is not None for item in by_value):',
     [OWN + 'test_ranking_matrix.py::test_render_consumes_the_assembler_decision']),
]


def run(root, name, targets):
    args = [sys.executable, '-m', 'pytest', '-q', '--tb=short', *targets]
    paths = [root, root/'tests', root/'scripts/audits/entity_profile_frame_v1_2_b_independent',
             root/'docs/audits/entity_profile_frame_v1_2_b2_independent', root/OWN]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(map(str, paths)), PYTEST_ADDOPTS='-p offline_guard',
               SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false', PYTHONDONTWRITEBYTECODE='1')
    try:
        result = subprocess.run(args, cwd=root, env=env, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=90, check=False)
        output, code = result.stdout, result.returncode
    except subprocess.TimeoutExpired as exc:
        output, code = str(exc), 'TIMEOUT'
    (OUT / (name + '.txt')).write_text(shlex.join(args) + '\n' + output + f'\nEXIT_CODE={code}\n')
    return code, output


def main():
    selected = set(sys.argv[1:])
    results = json.loads((OUT/'mutations.json').read_text())['results'] if selected else []
    with tempfile.TemporaryDirectory(prefix='secoppal-b3-mutations-') as temp:
        root = Path(temp)
        archive = root / 'source.tar'
        subprocess.run(['git', 'archive', '--output', str(archive), SHA], cwd=ROOT, check=True)
        subprocess.run(['tar', '-xf', str(archive), '-C', str(root)], check=True)
        archive.unlink()
        hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in (root/'app').rglob('*') if p.is_file()}
        for name, filename, anchor, replacement, targets in MUTATIONS:
            if selected and name not in selected:
                continue
            path = root / filename
            before = path.read_bytes()
            baseline, _ = run(root, name + '-control', targets)
            assert baseline == 0, (name, 'control must pass')
            assert anchor in before.decode(), (name, 'anchor absent')
            try:
                path.write_text(before.decode().replace(anchor, replacement))
                for cache in root.rglob('__pycache__'):
                    shutil.rmtree(cache)
                code, output = run(root, name + '-mutant', targets)
            finally:
                path.write_bytes(before)
            restored = all(hashlib.sha256(p.read_bytes()).hexdigest() == digest for p, digest in hashes.items())
            assert restored
            invalid = any(s in output for s in ('ImportError', 'INTERNALERROR', 'ERROR collecting', 'ERROR at setup', 'TIMEOUT'))
            detected = code == 1 and not invalid and any(s in output for s in ('AssertionError', 'E   assert', 'Failed: DID NOT RAISE'))
            results = [r for r in results if r['name'] != name]
            results.append(dict(head=SHA, name=name, file=filename, anchor=anchor, replacement=replacement, targets=targets,
                                baseline=baseline, mutant=code, result='BEHAVIOR_DETECTED' if detected else 'REVIEW',
                                restored=restored, sha256=hashlib.sha256(before).hexdigest()))
            (OUT/'mutations.json').write_text(json.dumps(dict(head=SHA, results=results), indent=2)+'\n')
            sys.stdout.write(f'{name} {results[-1]["result"]}\n')
            sys.stdout.flush()


if __name__ == '__main__':
    main()

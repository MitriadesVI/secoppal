"""Behavioral mutation probes on a temporary export of the tested commit."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
SHA = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
MUTATIONS = [
    ('F4-partial-recovery', 'app/core/workflow/publication_io.py',
     "handle.write(separator + line + '\\n')", "handle.write(line + '\\n')",
     ['tests/test_v12b2_storage.py::test_explicit_publication_after_partial_append_is_reloadable']),
    ('F4-confirmation-recovery', 'app/core/workflow/publication_io.py',
     '            _arm(barrier)', '            pass',
     ['tests/test_v12b2_storage.py::test_barrier_failures_never_authorize_failed_turn[confirm_after-True]']),
    ('F9-buyer-snapshot', 'app/core/entity_profile/record_validation.py',
     "if plan.get('buyer_scope') != frame.buyer_scope.to_dict():", 'if False:',
     ['tests/test_v12b2_integrity.py::test_buyer_snapshot_rejected_even_with_recomputed_digest']),
    ('F4-reader-fence', 'app/core/conversation_store.py',
     'if barrier_path(path).exists():', 'if False:',
     ['tests/test_v12b1_independent_storage.py::test_failed_io_never_authorizes_the_unpublished_generation[envelope_fsync]',
      'tests/test_v12b1_independent_storage.py::test_append_boundary_is_fail_closed_after_write[False]']),
    ('F5-multiplicity', 'app/core/entity_profile/record_validation.py',
     "len(jobs) != len(plan['jobs']) or len(record.calls) != len(jobs)", "len(jobs) != len(plan['jobs'])",
     ['tests/test_v12b1_independent_records.py::test_individual_record_damage_rejects_before_io[call_duplicate]',
      'tests/test_v12b1_independent_records.py::test_record_call_membership_preserves_multiplicity']),
    ('F9-buyer-consumers', 'app/core/entity_profile/record_validation.py',
     'if plan.buyer_scope != frame.buyer_scope:', 'if False:',
     ['tests/test_v12b1_independent_records.py::test_foreign_buyer_with_reused_ids_is_rejected']),
    ('supplier-unsafe-float', 'app/core/entity_profile/aspects/suppliers.py',
     'total_contract_value=amount.value,', 'total_contract_value=float(row.get("total_contract_value")),',
     ['tests/test_v12b2_suppliers.py::test_mixed_supplier_amounts_survive_reload_and_followup[overview-Infinity-INVALID]',
      'tests/test_v12b2_suppliers.py::test_mixed_supplier_amounts_survive_reload_and_followup[suppliers-1e999-REPRESENTATION_OVERFLOW]']),
    ('supplier-false-record-accreditation', 'app/core/entity_profile/execution_record.py',
     "if supplier_amount(metric.value).status != 'VALID':", 'if False:',
     ['tests/test_v12b2_suppliers.py::test_mixed_supplier_amounts_survive_reload_and_followup[suppliers-1e999-REPRESENTATION_OVERFLOW]']),
    ('supplier-value-ranking', 'app/core/entity_profile/render.py',
     'if by_value and all(item.total_contract_value is not None for item in by_value):', 'if by_value:',
     ['tests/test_v12b2_suppliers.py::test_mixed_supplier_amounts_survive_reload_and_followup[overview-Infinity-INVALID]']),
]


def run(root, name, targets):
    args = [sys.executable, '-m', 'pytest', '-q', '--tb=short', *targets]
    env = dict(os.environ, PYTHONPATH=str(root / 'scripts/audits/entity_profile_frame_v1_2_b_independent'),
               PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false',
               PYTHONDONTWRITEBYTECODE='1')
    try:
        result = subprocess.run(args, cwd=root, env=env, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=90, check=False)
        output, code = result.stdout, result.returncode
    except subprocess.TimeoutExpired as exc:
        output, code = str(exc), 'TIMEOUT'
    (OUT / (name + '.txt')).write_text(repr(args) + '\n' + output + f'\nEXIT_CODE={code}\n')
    return code, output


def main():
    results = []
    with tempfile.TemporaryDirectory(prefix='secoppal-b2-mutations-') as temp:
        root = Path(temp)
        archive = root / 'source.tar'
        subprocess.run(['git', 'archive', '--output', str(archive), SHA], cwd=ROOT, check=True)
        subprocess.run(['tar', '-xf', str(archive), '-C', str(root)], check=True)
        archive.unlink()
        app_hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in (root / 'app').rglob('*') if p.is_file()}
        for name, filename, anchor, replacement, targets in MUTATIONS:
            path = root / filename
            before = path.read_bytes()
            baseline, _ = run(root, name + '-control', targets)
            assert baseline == 0, (name, 'baseline must be green')
            source = before.decode()
            assert anchor in source, (name, 'anchor absent')
            path.write_text(source.replace(anchor, replacement))
            for cache in root.rglob('__pycache__'):
                shutil.rmtree(cache)
            try:
                code, output = run(root, name + '-mutant', targets)
            finally:
                path.write_bytes(before)
            restored = all(hashlib.sha256(p.read_bytes()).hexdigest() == digest
                           for p, digest in app_hashes.items())
            invalid = any(word in output for word in ('ImportError', 'INTERNALERROR', 'ERROR collecting'))
            detected = code == 1 and not invalid and any(signal in output for signal in
                ('AssertionError', 'E   assert', 'Failed: DID NOT RAISE'))
            results.append(dict(name=name, file=filename, anchor=anchor, replacement=replacement,
                                targets=targets, baseline=baseline, mutant=code,
                                result='BEHAVIOR_DETECTED' if detected else 'REVIEW', restored=restored,
                                sha256=hashlib.sha256(before).hexdigest()))
            sys.stdout.write(f"{name} {results[-1]['result']}\n")
            sys.stdout.flush()
    (OUT / 'mutations.json').write_text(json.dumps({'head': SHA, 'results': results}, indent=2) + '\n')


if __name__ == '__main__':
    main()

"""Six disposable functional mutations plus frozen-candidate red reproduction."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'docs/audits/contract_universe_v1_1_remediation/mutations.json'
CASES = {
    'global_lexical_match': 'tests/test_contract_universe_remediation.py::test_object_borradores_is_not_state',
    'params_fallback': 'tests/test_contract_universe_remediation.py::test_materialized_fields_are_not_consent',
    'global_record_terms': 'tests/test_contract_universe_remediation.py::test_published_description_not_rewritten',
    'first_match_wins': 'tests/test_contract_universe_remediation.py::test_two_states_not_silently_partial',
    'dual_process_veto': 'tests/test_contract_universe_remediation_consumers.py::test_dual_raw_explicit_records_not_silently_dropped',
    'aprobado_without_date': 'tests/test_contract_universe_remediation.py::test_independent_sql_matrix',
}


def run(root, targets):
    command = [sys.executable, '-m', 'pytest', '-p', 'tests.contract_universe_offline_guard',
               '-p', 'tests.contract_universe_sql_dates', *targets, '-q', '--tb=short']
    result = subprocess.run(command, cwd=root, env={**os.environ, 'BURR_TELEMETRY_ENABLED': 'false'},
                            capture_output=True, text=True, timeout=90)
    return dict(command=command, exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)


def main():
    evidence = {'baseline': run(ROOT, list(CASES.values())), 'mutations': {}}
    assert evidence['baseline']['exit_code'] == 0, evidence['baseline']
    with tempfile.TemporaryDirectory(prefix='cu-v1-1-mutations-') as temp:
        copy = Path(temp)
        for name in ('app', 'tests', 'data'):
            shutil.copytree(ROOT / name, copy / name, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
        for name in ('pyproject.toml', 'pytest.ini'):
            if (ROOT / name).exists():
                shutil.copyfile(ROOT / name, copy / name)
        for name, target in CASES.items():
            relative = ('app/core/contract_universe.py' if name == 'aprobado_without_date' else
                        'app/core/entity_profile/render.py' if name == 'global_record_terms' else
                        'app/core/contract_universe_intent.py')
            path = copy / relative
            original = path.read_text()
            if name == 'global_lexical_match':
                mutant = original.replace('    normalized = normalize_with_offsets(text)', '''    normalized = normalize_with_offsets(text)
    hit = re.search(r"\\bborrador(?:es)?\\b", normalized.text)
    if hit:
        return UniverseRequest(ContractUniverse.SOURCE_RECORDS, normalized.to_original(hit.span()), 'Borrador')''')
            elif name == 'params_fallback':
                mutant = original.replace('    clean = stamp(clean)', '''    if intent is None and clean.get('dataset') == 'contratos':
        clean['contract_universe'] = 'source_records'
    clean = stamp(clean)''')
            elif name == 'global_record_terms':
                mutant = original.replace('    return response\n', "    return response.replace('contratos', 'registros')\n")
            elif name == 'first_match_wins':
                mutant = original.replace('            # Multiple states require clarification',
                                          '            states = states[:1]\n            # Multiple states require clarification')
            elif name == 'dual_process_veto':
                mutant = original.replace('    normalized = normalize_with_offsets(text)', '''    normalized = normalize_with_offsets(text)
    if re.search(r"\\bprocesos?\\b", normalized.text):
        return None''')
            else:
                mutant = original.replace('date_extract_y(fecha_de_firma) IS NOT NULL', '1=1')
            assert mutant != original
            path.write_text(mutant)
            frame_path = copy / 'app/core/entity_profile/frame_response.py'
            frame_original = frame_path.read_text()
            if name == 'global_record_terms':
                frame_path.write_text(frame_original.replace('    return response\n',
                                                            "    return response.replace('contratos', 'registros')\n"))
            result = run(copy, [target])
            frame_path.write_text(frame_original)
            result['functional_kill'] = (result['exit_code'] == 1 and ('E   assert ' in result['stdout'] or 'AssertionError' in result['stdout'])
                                        and 'ERROR ' not in result['stdout']
                                        and 'ImportError' not in result['stdout'])
            evidence['mutations'][name] = result
            path.write_text(original)
        # Our scenario ports on the exact old runtime; expected differences include adjudication.
        for path in (copy / 'app').rglob('*.py'):
            relative = str(path.relative_to(copy))
            old = subprocess.run(['git', 'show', f'067b39770592640b5aef3898cbc99d8f82c59e0f:{relative}'],
                                 cwd=ROOT, capture_output=True)
            if old.returncode == 0:
                path.write_bytes(old.stdout)
        evidence['owned_ports_on_frozen_candidate'] = run(copy, list(CASES.values()))
        generator = (copy / 'tests/contract_universe_independent_base_generator.py').read_text()
        generator = generator.replace('assert frame.frame.schema_version == 1', 'assert frame.frame.schema_version == 2')
        generator = generator.replace("    store = ConversationStore", "    from app.core.contract_universe import stamp\n    params = stamp(params)\n    store = ConversationStore")
        generator = generator.replace('c52d7eda355f44278164d4cb9803a199eec83486',
                                      '067b39770592640b5aef3898cbc99d8f82c59e0f')
        (copy / 'tests/test_generate_v1_artifacts.py').write_text(generator)
        os.environ['CU_BASE_FIXTURE'] = str(OUT.with_name('v1_serialized.json'))
        evidence['v1_artifact_generation'] = run(copy, ['tests/test_generate_v1_artifacts.py'])
    OUT.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + '\n')
    print({name: result['functional_kill'] for name, result in evidence['mutations'].items()})
    assert all(result['functional_kill'] for result in evidence['mutations'].values())


if __name__ == '__main__':
    main()

"""F2.1 focal validation driver: every gate, with its literal exit code and output."""
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PY = '/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python'
VENV = '/Users/rodrigoortiz/Documents/secoppal/.venv/bin'
GUARDS = ['-p', 'tests.contract_universe_offline_guard', '-p', 'tests.contract_universe_sql_dates']
OUT = ROOT / 'docs/audits/contract_universe_v1_1_f2_1/validation.json'

FROZEN = ['tests/test_contract_universe_independent.py',
          'tests/test_contract_universe_independent_consumers.py',
          'tests/test_contract_universe_independent_legacy.py',
          'tests/test_contract_universe_independent_safety.py',
          'tests/test_module_size_limits.py']
AUDITOR = ['tests/test_cu_v11_audit_ports.py', 'tests/test_cu_v11_audit_f1_f5.py',
           'tests/test_cu_v11_audit_aprobado.py', 'tests/test_cu_v11_audit_persistence.py',
           'tests/test_cu_v11_audit_regressions.py', 'tests/test_cu_v11_audit_properties.py']
REMEDIATION = ['tests/test_contract_universe_remediation.py',
               'tests/test_contract_universe_remediation_consumers.py',
               'tests/test_contract_universe_remediation_properties.py',
               'tests/test_contract_universe_remediation_legacy.py']
AUTHOR = """test_contract_universe_product test_contract_universe_consumers
test_contract_universe_properties test_soql_builder test_soql_builder_phrase test_observer
test_observer_partial test_observer_signals test_analytical_queries test_actor_profile
test_actor_profile_scope test_actor_profile_render test_actor_profile_scope_conversation
test_contract_invariants test_truth_layer_contract_signature test_entity_profile_frame_b_semantics
test_entity_profile_frame_b_admission test_entity_profile_frame_catalog_parity
test_module_size_limits test_entity_profile_frame_core test_entity_profile_frame_migration
test_entity_profile_frame_properties test_entity_profile_semantic_corpus
test_buyer_actor_independent_product test_buyer_actor_independent_boundaries
test_entity_profile_source_plan test_radar_builder test_radar_runner test_query_router
test_estado_families_integrity test_pagination_buffer test_pagination_buffer_lifecycle
test_export_xlsx test_truth_layer_public_parity test_entity_profile_frame_b_integration
test_entity_profile_frame_b_publication test_entity_profile_frame_evidence
test_entity_profile_frame_b_properties test_entity_profile_frame_independent_contracts
test_entity_profile_workflow test_entity_profile_conversation_scope
test_entity_profile_queries""".split()

COMMANDS = {
    'ruff': [f'{VENV}/ruff', 'check', 'app/', 'tests/', 'scripts/'],
    'semgrep': [f'{VENV}/semgrep', '--config', '.semgrep/secoppal-laws.yml', 'app/',
                '--metrics=off'],
    'f2_1_symmetry': [PY, '-m', 'pytest', *GUARDS,
                      'tests/test_contract_universe_f2_1_symmetry.py', '-q', '--tb=short'],
    'f2_1_completeness': [PY, '-m', 'pytest', *GUARDS,
                          'tests/test_contract_universe_f2_1_completeness.py',
                          '-q', '--tb=short'],
    'auditor_findings_now_green': [PY, '-m', 'pytest', *GUARDS,
                                   'tests/test_cu_v11_audit_findings.py', '-q', '--tb=short'],
    'auditor_rest': [PY, '-m', 'pytest', *GUARDS, *AUDITOR, '-q', '--tb=short'],
    'remediation_85': [PY, '-m', 'pytest', *GUARDS, *REMEDIATION, '-q', '--tb=short'],
    'author_targets': [PY, '-m', 'pytest', *GUARDS,
                       *[f'tests/{name}.py' for name in AUTHOR], '-q', '--tb=short'],
    'frozen_43_literal': [PY, '-m', 'pytest', *GUARDS, *FROZEN, '-q', '--tb=no'],
    'frozen_43_with_v1_aprobado_shim': [PY, '-m', 'pytest', *GUARDS,
                                        '-p', 'tests.cu_v1_policy_shim', *FROZEN,
                                        '-q', '--tb=no'],
    'corpus': [PY, 'scripts/run_query_corpus.py', '--mode', 'all'],
    'lint_core': ['make', 'lint-core'],
    'diff_check': ['git', 'diff', '--check'],
}


def main() -> None:
    results = {}
    for name, command in COMMANDS.items():
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        results[name] = {
            'command': command,
            'exit_code': completed.returncode,
            'stdout_tail': completed.stdout.strip().splitlines()[-8:],
            'stderr_tail': completed.stderr.strip().splitlines()[-4:],
        }
        last = (completed.stdout.strip().splitlines() or [''])[-1]
        print(f'{name:34s} exit={completed.returncode}  {last[:78]}')
    OUT.write_text(json.dumps(results, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()

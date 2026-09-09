"""Repeat the bounded gate, keeping commands and literal results; never global suite."""
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'docs/audits/contract_universe_v1_1_remediation'
BIN = Path(sys.executable).parent


def main():
    original = json.loads((ROOT / 'docs/audits/contract_universe_v1/final_validation.json').read_text())
    author = original['commands']['pytest']['command']
    author[0] = sys.executable
    author[3:3] = ['-p', 'tests.contract_universe_sql_dates']
    author[author.index('-q'):author.index('-q')] = [
        'tests/test_entity_profile_workflow.py', 'tests/test_entity_profile_conversation_scope.py',
        'tests/test_entity_profile_queries.py',
    ]
    pytest = [sys.executable, '-m', 'pytest', '-p', 'tests.contract_universe_offline_guard',
              '-p', 'tests.contract_universe_sql_dates']
    commands = {
        'ruff': [str(BIN / 'ruff'), 'check', 'app/', 'tests/', 'scripts/'],
        'semgrep': [str(BIN / 'semgrep'), '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json'],
        'remediation': [*pytest, 'tests/test_contract_universe_remediation.py',
                        'tests/test_contract_universe_remediation_consumers.py',
                        'tests/test_contract_universe_remediation_properties.py',
                        'tests/test_contract_universe_remediation_legacy.py', '-q', '--tb=short'],
        'independent': [*pytest, 'tests/test_contract_universe_independent.py',
                        'tests/test_contract_universe_independent_consumers.py',
                        'tests/test_contract_universe_independent_legacy.py',
                        'tests/test_contract_universe_independent_safety.py',
                        'tests/test_module_size_limits.py', '-q', '--tb=short'],
        'author': author,
        'corpus': [sys.executable, 'scripts/run_query_corpus.py', '--mode', 'all'],
        'lint-core': ['make', 'lint-core'],
        'diff-check': ['git', 'diff', '--check'],
    }
    evidence = {'tested_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                'commands': {}}
    for name, command in commands.items():
        env = {**os.environ, 'BURR_TELEMETRY_ENABLED': 'false'}
        if name in ('remediation', 'independent'):
            trace = OUT / f'{name}_trace.jsonl'
            trace.write_text('')
            env['CU_INDEPENDENT_TRACE'] = str(trace)
        result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=240)
        evidence['commands'][name] = dict(command=command, exit_code=result.returncode,
                                          stdout=result.stdout, stderr=result.stderr)
        (OUT / 'validation.json').write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + '\n')
        summary = result.stderr[-120:] if name == 'semgrep' else result.stdout[-250:]
        print(name, result.returncode, summary.strip(), flush=True)


if __name__ == '__main__':
    main()

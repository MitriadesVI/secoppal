"""Disposable copies; a kill requires a green control and a product assertion."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'docs/audits/contract_universe_v1/mutations.json'
TESTS = {
    'count': 'tests/test_contract_universe_consumers.py::test_count_analytics_observer_and_rankings',
    'frame': 'tests/test_contract_universe_product.py::test_frame_effective_plan_and_reload',
    'limit': 'tests/test_contract_universe_product.py::test_seeds_before_limit',
    'reload': 'tests/test_contract_universe_consumers.py::test_records_universe_survives_reload_values',
}


def run(path, tests):
    command = [sys.executable, '-m', 'pytest', '-p', 'tests.contract_universe_offline_guard', *tests, '-q', '--tb=short']
    result = subprocess.run(command, cwd=path, env={**os.environ, 'BURR_TELEMETRY_ENABLED': 'false'},
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
    return dict(command=command, exit_code=result.returncode, output=result.stdout)


def main():
    evidence = {'baseline': run(ROOT, list(TESTS.values())), 'mutations': {}}
    if evidence['baseline']['exit_code']:
        OUT.write_text(json.dumps(evidence, indent=2) + '\n')
        raise SystemExit('Baseline is not green; no mutation claim')
    with tempfile.TemporaryDirectory(prefix='contract-universe-mutations-') as temp:
        copy = Path(temp)
        for name in ('app', 'tests', 'data'):
            shutil.copytree(ROOT / name, copy / name, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
        for name in ('pyproject.toml', 'pytest.ini'):
            if (ROOT / name).exists():
                shutil.copyfile(ROOT / name, copy / name)
        for name, test in TESTS.items():
            path = copy / ('app/core/entity_profile/frame_predicates.py' if name == 'frame'
                           else 'app/core/query_frame.py' if name == 'reload'
                           else 'app/core/soql_builder.py')
            original = path.read_text()
            if name == 'frame':
                mutant = original.replace('clauses.append(universe_clause)', 'pass  # mutation: unfiltered Frame')
            elif name == 'reload':
                mutant = original.replace('    return params\n',
                                          '    params.pop("contract_universe", None)\n    params.pop("contract_universe_version", None)\n    return params\n')
            else:
                a = original.index('    def build_count(') if name == 'count' else original.index('    def build(')
                pos = original.index('self._build_where(spec, params, allow_global=allow_global)', a)
                mutant = original[:pos] + original[pos:].replace(
                    'self._build_where(spec, params, allow_global=allow_global)',
                    'self._build_where(spec, {**params, "contract_universe": "source_records"}, allow_global=allow_global)', 1)
            assert mutant != original
            path.write_text(mutant)
            helper = copy / 'app/core/workflow/helpers.py'
            helper_original = helper.read_text()
            if name == 'limit':
                helper.write_text(helper_original.replace(
                    '    query_ms = _elapsed_ms(query_start)',
                    '    from app.core.contract_universe import eligible\n    results = [row for row in results if eligible(row)]\n    query_ms = _elapsed_ms(query_start)'))
            result = run(copy, [test])
            result['killed_by_product'] = (result['exit_code'] == 1 and ('AssertionError' in result['output'] or 'E   assert ' in result['output'])
                                           and 'ERROR ' not in result['output'] and 'ImportError' not in result['output'])
            evidence['mutations'][name] = result
            path.write_text(original)
            helper.write_text(helper_original)
        # Red reproduction at the integrated base; keep the exact new synthetic tests.
        tracked = subprocess.check_output(['git', 'diff', 'c52d7eda355f44278164d4cb9803a199eec83486', '--diff-filter=M', '--name-only', '--', 'app'], cwd=ROOT, text=True).splitlines()
        for relative in tracked:
            content = subprocess.check_output(['git', 'show', 'c52d7eda355f44278164d4cb9803a199eec83486:' + relative], cwd=ROOT)
            (copy / relative).write_bytes(content)
        evidence['integrated_base_red'] = run(copy, [TESTS['limit']])
    OUT.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: v['killed_by_product'] for k, v in evidence['mutations'].items()}))
    if not all(v['killed_by_product'] for v in evidence['mutations'].values()):
        raise SystemExit(1)


if __name__ == '__main__':
    main()

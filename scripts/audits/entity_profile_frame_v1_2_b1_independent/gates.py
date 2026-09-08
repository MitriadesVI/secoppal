"""Literal instrument outputs and identity-based Semgrep reconciliation."""
import ast
import json
import os
from pathlib import Path
import subprocess
import textwrap

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent'
VENV = '/Users/rodrigoortiz/Documents/secoppal/.venv/bin/'
BASE = '1b2a85f89e5d556f1de392e11b9da437da6540d1'


def run(args, *, env=None):
    return subprocess.run(args, cwd=ROOT, capture_output=True, text=True, env=env, check=False)


def findings(payload, old):
    result = []
    for entry in payload['results']:
        path = entry['path']
        source = subprocess.check_output(['git', 'show', BASE + ':' + path], cwd=ROOT, text=True) if old else (ROOT / path).read_text()
        start, end = entry['start']['line'], entry['end']['line']
        result.append(dict(rule=entry['check_id'], path=path, start=start, end=end,
                           fragment='\n'.join(source.splitlines()[start - 1:end])))
    return result


def main():
    commands = [[VENV + 'python', '--version'], [VENV + 'ruff', '--version'], [VENV + 'semgrep', '--version'],
                [VENV + 'python', '-c', 'from importlib.metadata import version; print("Hypothesis", version("hypothesis")); print("Burr", version("burr"))'],
                ['codegraph', '--version'], ['codegraph', 'status']]
    env = dict(os.environ, PATH='/opt/homebrew/opt/node@22/bin:' + os.environ['PATH'])
    with (OUT / 'preflight.log').open('w') as log:
        for command in commands:
            result = run(command, env=env)
            log.write(repr(command) + '\n' + result.stdout + result.stderr + f'EXIT_CODE={result.returncode}\n')
        log.write('Codegraph MCP: not loaded; CLI index: Not initialized; fallback: rg and manual review.\n')
    for name, command in [('ruff_final', [VENV + 'ruff', 'check', 'app/', 'tests/', 'scripts/']),
                          ('semgrep_final', [VENV + 'semgrep', '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off']),
                          ('lint_core', ['make', 'lint-core']),
                          ('diff_check', ['git', 'diff', '--check', '708ea3b3f609af4d9d3abe471f6963ee23513d7a..HEAD'])]:
        result = run(command)
        (OUT / (name + '.log')).write_text(repr(command) + '\n' + result.stdout + result.stderr + f'EXIT_CODE={result.returncode}\n')
        print(name, result.returncode, flush=True)
    scan = run([VENV + 'semgrep', '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json'])
    (OUT / 'semgrep_final.json').write_text(scan.stdout)
    assert scan.returncode == 0
    before = findings(json.loads((OUT / 'semgrep_base.json').read_text()), True)
    remaining = findings(json.loads(scan.stdout), False)
    pairs = []
    for old in before:
        same = next((item for item in remaining if (item['rule'], item['path'], item['fragment']) ==
                     (old['rule'], old['path'], old['fragment'])), None)
        if same:
            reason = 'identical fragment; line displacement recorded'
        else:
            candidates = [item for item in remaining if (item['rule'], item['path']) == (old['rule'], old['path'])]
            assert len(candidates) == 1
            same = candidates[0]

            def handlers(fragment):
                return [ast.dump(node) for node in ast.walk(ast.parse(textwrap.dedent(fragment))) if isinstance(node, ast.ExceptHandler)]

            assert handlers(old['fragment']) == handlers(same['fragment'])
            reason = 'same except AST; pagination try now consumes admitted buffer; no added handler or suppression'
        remaining.remove(same)
        pairs.append(dict(before=old, after=same, adjudication=reason))
    assert not remaining
    report = dict(baseline=len(before), candidate=len(pairs), new_findings=len(remaining), pairs=pairs)
    (OUT / 'semgrep_comparison.json').write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    print('semgrep baseline', len(before), 'delta', len(remaining))
    result = run(['rg', '-n', 'coordinate_turn|stage_continuity|prepare_generation|publish_turn|validate_record|validate_frame_plan|validate_tabular|export_params', 'app', 'tests', 'streamlit_app.py'])
    (OUT / 'consumers.txt').write_text(result.stdout)


if __name__ == '__main__':
    main()

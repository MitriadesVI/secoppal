"""Historical replay and mutations only in disposable Git exports."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'docs/audits/buyer_actor_role_arbitration_v1_independent'
PYTHON = '/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python'
CANDIDATE = 'e06c60482756e2d4238052818117568f9cd4e946'
BASE = '52a6bcc68efc50e06ba9aa0f60ff8d30f8cf1386'
HISTORY = '7868664757b28bed16c583ea50327d669f1501e3'
PRODUCT = 'tests/test_buyer_actor_independent_product.py'
BOUNDARY = 'tests/test_buyer_actor_independent_boundaries.py'


def export(sha, directory):
    archive = subprocess.check_output(['git', 'archive', sha], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(directory, filter='data')
    for path in (ROOT / 'tests').glob('*buyer_actor_independent*.py'):
        shutil.copy2(path, directory / 'tests' / path.name)


def run(directory, label, targets):
    env = dict(os.environ)
    with tempfile.TemporaryDirectory(prefix='buyer-experiment-state-') as state:
        env.update(SECOP_FEEDBACK_PATH=state + '/feedback.jsonl',
                   SECOP_FEEDBACK_DB_PATH=state + '/feedback.sqlite',
                   CONVERSATION_STORE_DIR=state + '/conversations',
                   SECOP_ACTOR_ALIAS_INDEX_PATH=state + '/missing.sqlite',
                   SECOPPAL_ACTOR_PROFILE_ENABLED='true',
                   SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false',
                   BURR_TELEMETRY_ENABLED='false',
                   BUYER_INDEPENDENT_EVIDENCE=str(OUT / label))
        command = [PYTHON, '-m', 'pytest', '-p', 'tests.buyer_actor_independent_guard',
                   *targets, '-q', '--tb=short', '-ra']
        result = subprocess.run(command, cwd=directory, env=env, capture_output=True)
    (OUT / (label + '.stdout.txt')).write_bytes(result.stdout)
    (OUT / (label + '.stderr.txt')).write_bytes(result.stderr)
    record = dict(command=command, cwd=str(directory), exit=result.returncode,
                  summary=result.stdout.decode(errors='replace').splitlines()[-1:])
    (OUT / (label + '.command.json')).write_text(json.dumps(record, indent=2) + '\n')
    print(label, record['exit'], record['summary'], flush=True)
    return record


def mutations():
    cases = [
        ('verbal', {'app/core/actor_intent.py': (
            'if buyer_owns_complete_mention(mention, params):', 'if False:')},
         [PRODUCT + '::test_seed_universe_and_reload', '-k', 'barranquilla and 4']),
        ('bare', {'app/core/actor_arbitration.py': (
            'if buyer_owns_complete_mention(candidate, params):', 'if False:')},
         [PRODUCT + '::test_seed_universe_and_reload', '-k', 'cartagena and 4']),
        ('global-veto', {'app/core/buyer_actor_role.py': (
            '    resolution = params.get("entidad_resolution")',
            '    return bool(params.get("entidad_resolved"))\n    resolution = params.get("entidad_resolution")')},
         [BOUNDARY + '::test_partial_or_fuzzy_buyer_cannot_veto_whole_mention']),
        ('context', {
            'app/core/workflow/actions/parsing.py': (
                'if is_complete_buyer_search(user_query, parsed_params):', 'if False:'),
            'app/core/workflow/external_admission.py': (
                'if not profile and is_complete_buyer_search(query, params):', 'if False:')},
         [PRODUCT + '::test_new_search_after_actor']),
    ]
    report = []
    for label, changes, targets in cases:
        with tempfile.TemporaryDirectory(prefix='buyer-mutation-' + label + '-') as temporary:
            root = Path(temporary)
            export(CANDIDATE, root)
            baseline = run(root, 'mutation-' + label + '-baseline', targets)
            if baseline['exit']:
                report.append(dict(mutation=label, baseline=baseline, status='INVALID_BASELINE'))
                continue
            original = {p: (root / p).read_bytes() for p in changes}
            for path, (needle, replacement) in changes.items():
                text = (root / path).read_text()
                assert text.count(needle) == 1, (path, text.count(needle))
                (root / path).write_text(text.replace(needle, replacement))
            mutant = run(root, 'mutation-' + label + '-mutant', targets)
            for path, data in original.items():
                (root / path).write_bytes(data)
            restored = {path: hashlib.sha256((root / path).read_bytes()).hexdigest()
                        for path in changes}
            assert all(restored[p] == hashlib.sha256(data).hexdigest() for p, data in original.items())
            report.append(dict(mutation=label, baseline=baseline, mutant=mutant,
                               restored_sha256=restored))
        (OUT / 'mutations.json').write_text(json.dumps(report, indent=2) + '\n')


def history():
    for label, sha in [('base', BASE), ('pre-frame', HISTORY)]:
        with tempfile.TemporaryDirectory(prefix='buyer-history-' + label + '-') as temporary:
            root = Path(temporary)
            export(sha, root)
            targets = [PRODUCT + '::test_seed_universe_and_reload']
            if label == 'pre-frame':
                targets.extend(['-k', 'off'])
            run(root, label + '-seeds', targets)
            if label == 'base':
                run(root, 'base-negation', [BOUNDARY + '::test_short_negation_characterization'])
                run(root, 'base-dependent', [PRODUCT + '::test_dependent_actor_refinement'])
                # Semgrep path exclusions need the same repository-root semantics.
                subprocess.run(['git', 'init', '-q', str(root)], check=True)
                top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], cwd=root)
                assert Path(top.decode().strip()) == root.resolve()
                subprocess.run(['git', 'add', 'app', '.semgrep'], cwd=root, check=True)
                command = ['/Users/rodrigoortiz/Documents/secoppal/.venv/bin/semgrep',
                           '--config', '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json']
                result = subprocess.run(command, cwd=root, capture_output=True)
                (OUT / 'semgrep-base.stdout.txt').write_bytes(result.stdout)
                (OUT / 'semgrep-base.stderr.txt').write_bytes(result.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['mutations', 'history'])
    args = parser.parse_args()
    mutations() if args.mode == 'mutations' else history()

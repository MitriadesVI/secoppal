"""Sequential gate: every required component contributes to the final exit."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import tempfile
import time

from gate_support import ACTIVE, BASE, BIN, OUT, ROOT, code_snapshot, environment, git, preservation, semgrep_delta, write

BLOCKS = {'global': ['tests'], **{
    group: [str(path.relative_to(ROOT)) for path in sorted((ACTIVE / group).glob('test_*.py'))]
    for group in ('b2_independent', 'b3_remediation', 'b3_independent')
}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['collect', 'focal', 'full'])
    parser.add_argument('targets', nargs='*')
    parser.add_argument('--run-id', default='')
    args = parser.parse_args()
    if args.mode == 'full' and args.run_id:
        raise SystemExit('A full rerun requires an explicit candidate review, not a new label')
    prefix = args.mode + ('_' + args.run_id if args.run_id else '')
    if (OUT / f'{prefix}_results.json').exists():
        raise SystemExit('This run exists; preserve it and choose a separately reviewed candidate before rerun.')
    rows = []
    before = preservation()
    write(f'{prefix}_hashes_before.json', before)
    head = git('rev-parse', 'HEAD')
    frozen_code = code_snapshot()
    write(f'{prefix}_refs.json', {'GATE_CODE_HEAD': head, 'MAIN_BEFORE': git('rev-parse', 'main'),
          'interval': git('log', '--format=%H %s', 'main..HEAD'),
          'initial_frame': False, 'blocks': BLOCKS})
    with tempfile.TemporaryDirectory(prefix='secoppal-coordinator-') as temp:
        scratch = Path(temp)

        def run(name, command, *, network='offline', cwd=ROOT, scanner=False):
            name = prefix + '_' + name
            env = environment(scratch, name, network)
            started = time.monotonic()
            log = OUT / f'{name}.log'
            with log.open('w') as output:
                output.write(shlex.join(command) + '\n')
                output.flush()
                result = subprocess.run(command, cwd=cwd, env=env, stdout=output,
                                        stderr=subprocess.STDOUT, check=False)
                output.write(f'\nEXIT_CODE={result.returncode}\n')
            row = {'component': name, 'command': command, 'sha': head,
                   'duration': time.monotonic()-started, 'exit': result.returncode,
                   'pass': result.returncode == 0, 'network': network,
                   'initial_frame': False, 'log': str(log.relative_to(ROOT))}
            if scanner:
                row['pass'] = result.returncode in (0, 1)
            rows.append(row)
            write(f'{prefix}_results.json', rows)
            print(json.dumps(row), flush=True)  # noqa: T201
            return row

        def pytest(name, targets, collect=False):
            command = [str(BIN / 'python'), '-m', 'pytest', '-p', 'gate_plugin', '-q',
                       '-o', 'cache_dir=' + str(scratch / 'pytest_cache'), *targets]
            if name != 'global':
                command += ['-p', 'offline_guard']
            if collect:
                command += ['--collect-only']
            else:
                command += ['--tb=short', '--junitxml=' + str(OUT / f'{prefix}_{name}.xml')]
            return run(name, command, network='secop_reads' if name == 'global' else 'offline')

        if args.mode == 'focal':
            pytest('focused', args.targets)
        elif args.mode == 'collect':
            for name, targets in BLOCKS.items():
                pytest(name, targets, collect=True)
        else:
            if git('status', '--porcelain', '--untracked-files=no'):
                raise SystemExit('Tracked candidate must be clean before full gate')
            run('ruff', [str(BIN / 'ruff'), 'check', 'app/', 'tests/', 'scripts/'])
            run('ruff_complement', [str(BIN / 'ruff'), 'check', str(OUT.relative_to(ROOT))])
            base_root = scratch / 'semgrep_base'
            base_root.mkdir()
            archive = scratch / 'base.tar'
            with archive.open('wb') as output:
                subprocess.run(['git', 'archive', BASE, 'app', '.semgrep'], cwd=ROOT, stdout=output, check=True)
            with tarfile.open(archive) as source:
                source.extractall(base_root, filter='data')
            subprocess.run(['git', 'init', '-q', str(base_root)], check=True)
            subprocess.run(['git', 'add', 'app', '.semgrep'], cwd=base_root, check=True)
            scans = []
            for label, cwd in [('base', base_root), ('candidate', ROOT)]:
                scans.append(run('semgrep_' + label, [str(BIN / 'semgrep'), '--config',
                    '.semgrep/secoppal-laws.yml', 'app/', '--metrics=off', '--json',
                    '--output', str(OUT / f'semgrep_{label}.json')], cwd=cwd, scanner=True))
            delta = semgrep_delta(json.loads((OUT/'semgrep_base.json').read_text()),
                                  json.loads((OUT/'semgrep_candidate.json').read_text()), base_root)
            delta['pass'] = delta['pass'] and all(row['pass'] for row in scans)
            write('semgrep_delta.json', delta)
            rows.append({'component': 'semgrep_evaluation', 'pass': delta['pass']})
            for name, targets in BLOCKS.items():
                pytest(name, targets)
            run('query_corpus', [str(BIN / 'python'), 'scripts/run_query_corpus.py', '--mode', 'all', '--show-pass'])
            run('lint_core', ['make', 'lint-core'])
            run('diff_check', ['git', 'diff', '--check', BASE + '..HEAD'])
            # Module-size and semantic corpus are part of the global collection.
        after = preservation()
        write(f'{prefix}_hashes_after.json', after)
        rows.append({'component': 'preservation', 'pass': before == after and not after['original_failures']})
        rows.append({'component': 'code_frozen', 'pass': head == git('rev-parse', 'HEAD') and frozen_code == code_snapshot()})
        write(f'{prefix}_postflight.json', {'MAIN_AFTER': git('rev-parse', 'main'),
              'HEAD': git('rev-parse', 'HEAD'), 'tracked_status': git('status', '--porcelain', '-uno')})
        write(f'{prefix}_results.json', rows)
    return 0 if all(row['pass'] for row in rows) else 1


if __name__ == '__main__':
    sys.exit(main())

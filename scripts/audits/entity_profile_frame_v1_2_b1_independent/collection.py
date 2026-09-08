"""Collect only documented targets and reconcile node IDs without summing overlaps."""
import ast
import json
import os
from pathlib import Path
import runpy
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent'


def main():
    env = dict(os.environ, PYTHONPATH=str(ROOT / 'scripts/audits/entity_profile_frame_v1_2_b_independent'),
               PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false')
    collections = {}
    targets = ['author_4', 'author_5', 'author_6', 'author_7', 'author_8', 'author_9', 'originals', 'acceptance', 'own_final']
    for target in targets:
        first = (OUT / (target + '.log')).read_text().splitlines()[0]
        args = ast.literal_eval(first) if first.startswith('[') else shlex.split(first)
        args = [arg for arg in args if arg != '-q']
        args.extend(['--collect-only', '-q'])
        result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, check=True)
        collected = [line.strip() for line in result.stdout.splitlines() if line.startswith('tests/') and '::' in line]
        assert collected and len(collected) == len(set(collected))
        collections[target] = collected
    ports = runpy.run_path(str(ROOT / 'scripts/audits/entity_profile_frame_v1_2_b1_remediation/acceptance.py'))['PORTS']
    replaced = [node for node in collections['originals'] if any(node.startswith(port) for port in ports)]
    assert len(replaced) == 9
    original_failures = [line.split(' - ')[0].removeprefix('FAILED ') for line in (OUT / 'originals.log').read_text().splitlines() if line.startswith('FAILED tests/')]
    contractual = [line.split(' - ')[0].removeprefix('FAILED ') for line in (OUT / 'author_5.log').read_text().splitlines() if line.startswith('FAILED tests/')]
    controls = sorted(set(replaced) - set(original_failures))
    assert len(controls) == 4 and len(contractual) == 2
    observational = [node for node in collections['originals'] if 'base_comparison' in node]
    assert len(observational) == 5
    actual_failures = [line.split(' - ')[0].removeprefix('FAILED ') for line in (OUT / 'own_final.log').read_text().splitlines() if line.startswith('FAILED tests/')]
    union = sorted(set().union(*(set(v) for v in collections.values())))
    report = dict(candidate='af8f0a3407102dcb807089e87e470d754b835d61', targets=collections,
        counts={key: len(value) for key, value in collections.items()}, unique_collected=len(union),
        original_directly_current=sorted(set(collections['originals']) - set(replaced) - set(observational)),
        historical_mounts_replaced=replaced, four_green_controls_executed=controls,
        contractual_expectations_replaced=contractual, observational_probes=observational,
        real_failures_open=actual_failures,
        note='Union is a collection inventory, not a count of passing product guarantees. Targets overlap.')
    (OUT / 'test_reconciliation.json').write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    print(report['counts'], 'unique', len(union))
    print('HISTORICAL PORTS', replaced, 'GREEN CONTROLS', controls)


if __name__ == '__main__':
    main()

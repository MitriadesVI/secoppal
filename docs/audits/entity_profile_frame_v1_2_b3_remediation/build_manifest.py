"""Explicit reconciliation proposal and replayable examples; no active test edits."""
import ast
import gzip
import json
from pathlib import Path
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]
HIST = ROOT/'docs/audits/entity_profile_frame_v1_2_b2_independent'


def read(name):
    path = OUT/name
    return path.read_text() if path.exists() else gzip.decompress(path.with_name(name+'.gz').read_bytes()).decode()


def nodes(name):
    return {node.attrib['classname'].replace('.', '/')+'.py::'+node.attrib['name']:
            ('FAILED' if node.find('failure') is not None else 'ERROR' if node.find('error') is not None
             else 'SKIPPED' if node.find('skipped') is not None else 'PASSED')
            for node in ET.fromstring(read(name+'.xml')).iter('testcase')}


def main():
    names = ['baseline', 'after_originals', 'own_verified', 'final_ports', 'acceptance', 'historical_current']
    names += [f'regressions_{i}' for i in range(4, 10)]
    targets = {name: nodes(name) for name in names}
    old = json.loads((HIST/'reconciliation_manifest.json').read_text())
    red = [node for node, status in targets['baseline'].items() if status == 'FAILED']
    assert len(red) == 4 and all(targets['after_originals'][node] == 'FAILED' for node in red)
    ports = {}
    for node in red:
        if 'test_publication_sequences.py' in node:
            port = node.replace('entity_profile_frame_v1_2_b2_independent/test_publication_sequences.py::test_rollback_of_retry_never_promotes_an_earlier_failed_turn',
                                'entity_profile_frame_v1_2_b3_remediation/test_publication_ports.py::test_failed_prefix_port')
            change = 'Terminal recovery suffix preserves blocked prefix; original asserts byte equality.'
        else:
            port = node.replace('entity_profile_frame_v1_2_b2_independent/test_supplier_topologies.py',
                                'entity_profile_frame_v1_2_b3_remediation/test_supplier_ports.py')
            change = 'Count-only damage warns within count TOP; value TOP remains accredited per B.3 product decision.'
        assert targets['own_verified'][port] == 'PASSED'
        ports[node] = dict(port=port, original_after='MECHANICAL_EXPECTATION_CHANGED', change=change)
    for item in old['residual_acceptance']:
        assert targets['acceptance'][item['nodeid']] == 'PASSED'
    for node in old['previous_45_controls']:
        assert targets['acceptance'][node] == 'PASSED'
    inventory = sorted({node for target in targets.values() for node in target})
    manifest = dict(active_collection_changed=False, historical_proposal_preserved=old['mappings'],
                    inherited_clock_debt=old['inherited_clock_debt'], ports=ports,
                    preserved_observations=old['new_independent_observations'],
                    targets=targets, unique_executed_inventory=inventory,
                    note='Overlapping targets and ports are not independent guarantees. Initial and final originals stay separate from acceptance.')
    (OUT/'reconciliation_proposal.json').write_text(json.dumps(manifest,indent=2)+'\n')
    # Copy actual observed complete responses and records, without another run
    # or constructed example. Exact original logs remain independently hashed.
    examples = {}
    log = read('own_verified.txt')
    for focus in ('overview', 'suppliers'):
        for damage in ('None', 'count', 'value', 'both'):
            tag = f'INDEPENDENT_TOP5 {focus} {damage} '
            start = log.index(tag) + len(tag)
            record_start = log.index(" {'plan_digest':", start)
            record_end = log.index('\n', record_start)
            examples[f'{focus}-{damage}'] = dict(response=log[start:record_start],
                execution_record=ast.literal_eval(log[record_start + 1:record_end]))
    (OUT/'response_record_examples.json').write_text(json.dumps(examples, ensure_ascii=False, indent=2)+'\n')


if __name__ == '__main__':
    main()

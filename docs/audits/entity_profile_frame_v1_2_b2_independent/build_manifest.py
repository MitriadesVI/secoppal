"""Exact node outcomes and proposed reconciliation; never changes active collection."""
import hashlib
import gzip
import json
from pathlib import Path
import xml.etree.ElementTree as ET

from audit_runner import ROOT, OUT, CANDIDATE
from audit_output import emit


def read_target(name):
    path = OUT/(name+'.xml')
    root = ET.fromstring(path.read_bytes() if path.exists() else gzip.decompress(path.with_suffix('.xml.gz').read_bytes()))
    results = {}
    for case in root.findall('.//testcase'):
        node = case.attrib['classname'].replace('.', '/')+'.py::'+case.attrib['name']
        status = 'FAILED' if case.find('failure') is not None else 'ERROR' if case.find('error') is not None else 'XFAILED' if case.find('skipped') is not None else 'PASSED'
        results[node] = status
    return results


def main():
    names = ['acceptance', 'historical_current', 'originals', 'own_verified', *[f'regressions_{i}' for i in range(4, 10)]]
    targets = {name: read_target(name) for name in names}
    original = json.loads((ROOT/'docs/audits/entity_profile_frame_v1_2_b2_remediation/test_reconciliation_proposed.json').read_text())
    residual = [item['nodeid'] for item in original['residual_acceptance']]
    assert len(residual) == 11 and all(targets['acceptance'][node] == 'PASSED' for node in residual)
    five = [n for n in targets['acceptance'] if n.startswith('tests/test_v12b1_independent_')]
    controls = [node for node in five if node not in residual]
    assert len(five) == 56 and len(controls) == 45
    assert all(targets['acceptance'][n] == 'PASSED' for n in controls)
    assert all(targets['historical_current'][n] == 'PASSED' for n in original['original_directly_current']+original['four_green_controls_executed'])
    assert {n for n,s in targets['originals'].items() if s == 'FAILED'} == set(original['historical_mounts_replaced'])-set(original['four_green_controls_executed'])
    clock = 'tests/test_entity_profile_frame_integration_contract.py::test_a_relative_period_does_not_get_reinterpreted_on_reload'
    assert {n for n,s in targets['regressions_5'].items() if s == 'FAILED'} == {*original['contractual_expectations_replaced'], clock}
    own_failed = [n for n,s in targets['own_verified'].items() if s == 'FAILED']
    assert len(own_failed) == 4
    files = ['tests/test_v12b_independent_publication.py', 'tests/test_v12b_independent_boundaries.py',
             'tests/test_entity_profile_frame_b_publication.py', 'tests/test_entity_profile_frame_b_semantics.py']
    def matching(prefixes):
        return sorted({n for target in targets.values() for n in target if any(n.startswith(p) for p in prefixes)})
    maps = [
        dict(historical=matching(['tests/test_v12b_independent_publication.py::test_reservation_serializes_execution_not_only_append']),
             replacements=matching(['tests/test_v12b1_remediation_acceptance.py::test_serial_execution_reserves_the_next_revision',
                 'tests/test_v12b1_remediation_acceptance.py::test_other_chat_executes_while_first_chat_is_paused',
                 'tests/test_v12b1_process_ports.py::test_real_workflows_serialize_across_processes_and_reload']),
             keep_controls=[n for n in original['four_green_controls_executed'] if 'serializes' in n]),
        dict(historical=matching(['tests/test_v12b_independent_publication.py::test_two_processes_both_enter_when_fcntl_is_missing']),
             replacements=matching(['tests/test_v12b1_process_ports.py::test_without_fcntl_every_process_refuses_before_entry'])),
        dict(historical=matching(['tests/test_v12b_independent_boundaries.py::test_current_actor_api_token_reaches_actor_handler']),
             replacements=matching(['tests/test_v12b1_actor_port.py::test_actor_direct_control_with_own_unconsumed_token',
                 'tests/test_v12b1_actor_port.py::test_actor_api_publishes_and_rejects_consumed_token']),
             keep_controls=[n for n in original['four_green_controls_executed'] if 'actor' in n]),
        dict(historical=[original['contractual_expectations_replaced'][0]], replacements=matching([
            'tests/test_v12b_independent_continuity.py::test_cross_references_are_validated_on_real_reload[catalog_version]',
            'tests/test_v12b1_independent_records.py::test_individual_record_damage_rejects_before_io[catalog]'])),
        dict(historical=[original['contractual_expectations_replaced'][1]], replacements=matching([
            'tests/test_v12b1_remediation_acceptance.py::test_current_record_damage_blocks_before_calls[focus]',
            'tests/test_v12b1_remediation_acceptance.py::test_current_record_damage_blocks_before_calls[expected_sections]',
            'tests/test_v12b1_independent_records.py::test_valid_publications_remain_reloadable'])),
    ]
    assert all(item['historical'] and item['replacements'] for item in maps)
    observatory = [n for n in targets['own_verified'] if 'test_recovery_error_observes' in n]
    manifest = dict(candidate=CANDIDATE, base='af3d0c7a26182fc26599a4b764ec60fe600b17f8', runtime_b1=original['candidate'], active_collection_changed=False,
        targets=targets, counts={name:{s:list(result.values()).count(s) for s in ('PASSED','FAILED','ERROR','XFAILED')} for name,result in targets.items()},
        unique_executed_node_inventory=len({n for target in targets.values() for n in target}),
        note='Targets overlap. Union is an inventory, not passing independent guarantees. Observational probes excluded from safety claims.',
        residual_acceptance=[dict(nodeid=n,result=targets['acceptance'][n],action='KEEP_ACTIVE',evidence='acceptance.xml.gz') for n in residual],
        previous_45_controls=controls, original_directly_current=original['original_directly_current'],
        four_green_controls=original['four_green_controls_executed'], mappings=maps,
        preserve_historical_files={p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in files},
        new_b2_author_active_nodes=[n for n in targets['acceptance'] if n.startswith('tests/test_v12b2_')],
        new_independent_active_nodes=[n for n in targets['own_verified'] if n not in observatory],
        new_independent_observations=observatory, previous_observational_probes=original['observational_probes'],
        real_failures_open=own_failed, inherited_clock_debt=dict(nodeid=clock,result='FAILED_ON_CANDIDATE_AND_BASE',action='KEEP_ACTIVE_PENDING_COORDINATOR_TEST_HARNESS_CORRECTION',evidence='clock_base.txt.gz'),
        old_mutations=dict(behavioral=25,route_reason_only=2,repeated_in_this_audit=False),
        new_mutations=json.loads((OUT/'mutations.json').read_text()))
    (OUT/'reconciliation_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    emit('NODES',manifest['unique_executed_node_inventory'],'RESIDUAL',len(residual),'CONTROLS',len(controls),'OWN_FAILURES',len(own_failed))
    for name,result in manifest['counts'].items():
        emit(name,result)


if __name__ == '__main__':
    main()

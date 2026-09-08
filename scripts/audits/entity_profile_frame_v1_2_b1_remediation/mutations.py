"""Behavioral mutations in disposable exports; never alter a user's worktree."""
from dataclasses import replace
import hashlib
import json
import os
import runpy
import re
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_remediation'
original_campaign = runpy.run_path(str(ROOT / "scripts/audits/entity_profile_frame_v1_2_b/mutation_campaign.py"))
MUTATIONS, Mutation = original_campaign["MUTATIONS"], original_campaign["Mutation"]

A = 'tests/test_v12b1_remediation_acceptance.py::'
I = 'tests/test_v12b_independent_'
T = 'app/core/workflow/turn_coordination.py'


def mutation(name, path, old, new, *targets):
    return Mutation(name, name, path, old, new, targets)


ADDITIONS = (
    mutation('B1-F1-preserve', T,
             '    if admitted.route is AdmissionRoute.CLARIFY and admitted.contract and reservation:',
             '    if False:',
             I + 'continuity.py::test_clarification_preserves_the_profile_it_offers_to_continue'),
    mutation('B1-F2-reformulate', 'app/core/entity_profile/frame_turn.py',
             '        if replacement:', '        if False:',
             I + 'continuity.py::test_full_reformulation_exits_a_pending_without_alternatives'),
    mutation('B1-F3-actor-operation', 'app/core/workflow/turn_coordination.py',
             'actor_candidate_selection=ctx.actor_candidate_selection,', 'actor_candidate_selection=None,',
             'tests/test_v12b1_actor_port.py::test_actor_api_publishes_and_rejects_consumed_token'),
    mutation('B1-F3-external-pending', 'app/core/workflow/admission.py',
             '    if external is not None and not seed_resolved_params:', '    if False:',
             I + 'continuity.py::test_explicit_external_commands_keep_their_handler'),
    mutation('B1-F4-serial-execution', T, '    with lock:', '    with nullcontext():',
             'tests/test_v12b1_process_ports.py::test_real_workflows_serialize_across_processes_and_reload'),
    mutation('B1-F4-reset-generation', 'app/core/conversation_store.py',
             ' or read_epoch(path) != reservation.epoch', '',
             I + 'publication.py::test_reset_invalidates_an_already_reserved_initial_turn'),
    mutation('B1-F4-unpublished-history', 'app/core/workflow/continuity_staging.py',
             "    generation = active_generation(store, chat_id)\n", '    generation = None if store.load_envelope(chat_id).usable else active_generation(store, chat_id)\n',
             I + 'publication.py::test_failed_delegated_publication_cannot_replace_legacy_authority'),
    mutation('B1-F5-joint-reload', 'app/core/workflow/admission.py',
             '        validate_envelope(envelope, state, contract)', '        pass',
             I + 'continuity.py::test_cross_references_are_validated_on_real_reload'),
    mutation('B1-F6-buffer-reference', 'app/core/workflow/tabular_capability.py',
             '    if groups[name] != capability(buffer, generation) or buffer.is_expired():', '    if False:',
             I + 'boundaries.py::test_pagination_consumes_the_published_capability',
             I + 'boundaries.py::test_real_sidecar_expiry_does_not_silently_repeat_a_search'),
    mutation('B1-F6-missing-ui-ref', 'app/core/workflow/admission_rules.py',
             '    if owner is not ContinuationOwner.TABULAR or not _matches(reference, contract, conversation_revision):',
             '    if reference is not None and not _matches(reference, contract, conversation_revision):',
             I + 'boundaries.py::test_ui_seed_requires_current_result_reference'),
    mutation('B1-F7-activation', 'app/core/workflow/admission.py',
             '        migrated = migrate_current_legacy(deps.conv_store, chat_id)', '        migrated = None',
             A + 'test_activation_migrates_equivalent_legacy_and_roundtrips'),
    mutation('B1-F7-rollback', T,
             '        if read and read.status is not EnvelopeStatus.EMPTY:', '        if False:',
             I + 'boundaries.py::test_rollback_does_not_reactivate_a_preprofile_tabular_cursor'),
    mutation('B1-F8-no-fcntl', 'app/core/store_lock.py',
             '        raise RuntimeError("cross-process conversation locking requires fcntl") from exc', '        return',
             'tests/test_v12b1_process_ports.py::test_without_fcntl_every_process_refuses_before_entry'),
    mutation('B1-F9-foreign-frame', 'app/core/entity_profile/frame_response.py',
             '    validate_frame_plan(frame, plan)', '    pass',
             I + 'execution.py::test_renderer_rejects_a_frame_other_than_the_authorized_revision',
             A + 'test_assembler_refuses_foreign_revision'),
    mutation('B1-F10-save-current-owner', 'app/core/workflow/tabular_capability.py',
             "    if contract.owner.value != 'TABULAR' or not contract.export_ref:\n        return None\n    history = store.get_history(chat, last_n=1)\n    if len(history) != 1 or history[0].turn_id != contract.export_ref:",
             "    history = store.get_history(chat, last_n=1)\n    if len(history) != 1:",
             I + 'boundaries.py::test_radar_save_cannot_export_an_older_tabular_search_over_profile'),
)


def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.glob('app/**/*.py')}


def run(root, targets):
    env = dict(os.environ, PYTHONPATH=str(root / 'scripts/audits/entity_profile_frame_v1_2_b_independent'),
               PYTEST_ADDOPTS='-p offline_guard', SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED='false',
               PYTHONDONTWRITEBYTECODE='1')
    return subprocess.run([sys.executable, '-B', '-m', 'pytest', '-q', '--tb=short', *targets],
                          cwd=root, env=env, capture_output=True, text=True, timeout=60)


def main():
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    mutations = []
    for item in MUTATIONS:
        if item.mutation_id.startswith('M7-'):
            item = replace(item, path=T, old='    elif admitted.route is AdmissionRoute.PROFILE:', new='    elif False:')
        if item.mutation_id.startswith('M9-'):
            item = replace(item, kills=(*item.kills, A + 'test_nonfinite_metric_remains_an_honest_response'))
        mutations.append(item)
    mutations.extend(ADDITIONS)
    only = os.getenv("B1_MUTATION_ONLY")
    if only:
        mutations = [item for item in mutations if item.mutation_id == only]
        if not mutations:
            raise ValueError("unknown mutation")
    with tempfile.TemporaryDirectory(prefix='secoppal-b1-mutations-') as temp:
        root = Path(temp)
        archive = root / 'source.tar'
        with archive.open('wb') as handle:
            subprocess.run(['git', 'archive', sha], cwd=ROOT, stdout=handle, check=True)
        with tarfile.open(archive) as handle:
            handle.extractall(root, filter='data')
        before = hashes(root)
        report = {'sha': sha, 'mutations': []}
        for item in mutations:
            baseline = run(root, item.kills)
            entry = {'id': item.mutation_id, 'path': item.path, 'targets': item.kills,
                     'baseline_exit': baseline.returncode, 'baseline_output': baseline.stdout + baseline.stderr}
            path = root / item.path
            original = path.read_bytes()
            if baseline.returncode:
                entry['verdict'] = 'BASELINE_FAILED'
            elif item.old not in original.decode():
                entry['verdict'] = 'ANCHOR_NOT_FOUND'
            else:
                try:
                    path.write_text(original.decode().replace(item.old, item.new, 1))
                    process = run(root, item.kills)
                    output = process.stdout + process.stderr
                    entry.update(exit=process.returncode, output=output)
                    bad = ('ImportError', 'ModuleNotFoundError', 'ERROR collecting', 'INTERNALERROR',
                           'handshake failed', 'BrokenBarrierError', 'TimeoutExpired')
                    entry['verdict'] = ('SURVIVED' if process.returncode == 0 else
                                        'INCONCLUSIVE' if any(word in output for word in bad) else
                                        'DETECTED' if 'AssertionError' in output or 'DID NOT RAISE' in output or re.search(r'^E +assert ', output, re.M) else 'INCONCLUSIVE')
                except subprocess.TimeoutExpired:
                    entry['verdict'] = 'INCONCLUSIVE_TIMEOUT'
                finally:
                    path.write_bytes(original)
                entry['restored'] = path.read_bytes() == original
            report['mutations'].append(entry)
            print(item.mutation_id, entry['verdict'], flush=True)
        report['runtime_restored'] = before == hashes(root)
        report['summary'] = {value: sum(m['verdict'] == value for m in report['mutations'])
                             for value in sorted({m['verdict'] for m in report['mutations']})}
        (OUT / ('mutations_m9.json' if only else 'mutations.json')).write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
        print(report['summary'], 'restored', report['runtime_restored'], flush=True)


if __name__ == '__main__':
    main()

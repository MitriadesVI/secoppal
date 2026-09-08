"""Hash and provenance evidence; does not import application settings."""
from pathlib import Path
import hashlib
import json
import subprocess

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b_independent'
BASE = '4c86d5bafaa3296b73c6523465b33efa4311910a'
CANDIDATE = '1b2a85f89e5d556f1de392e11b9da437da6540d1'


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def main():
    frozen = json.loads((OUT / 'candidate_hashes.json').read_text())
    changed = [p for p, h in frozen.items() if hashlib.sha256((ROOT / p).read_bytes()).hexdigest() != h]
    originals = {}
    for p in ['tests/test_entity_profile_v1_1_reaudit.py', 'tests/test_entity_profile_v1_1_reaudit_evidence.py']:
        originals[p] = hashlib.sha256(git('show', f'e024753:{p}')).hexdigest()
    (OUT / 'integrity.json').write_text(json.dumps({'candidate': CANDIDATE, 'tracked_candidate_files_checked': len(frozen),
         'changed_candidate_files': changed, 'original_adversarial_sha': 'e024753a7a3e26a80aa0c07359078544f45f4ee5',
         'original_adversarial_hashes': originals, 'runtime_diff_since_author_tested_head': git('diff', '--name-only', '11399da', CANDIDATE, '--', 'app/', 'streamlit_app.py').decode(),
         'a2_r3d_existing_test_diff': git('diff', '--name-status', BASE, CANDIDATE, '--', 'tests/test_v12a1_independent_counterexamples.py', 'tests/test_entity_profile_r3d_postposed_source.py').decode()}, indent=2) + '\n')
    findings = {}
    for label in ['base', 'candidate']:
        lines = (OUT / f'semgrep_{label}.log').read_text().splitlines()
        raw = next(json.loads(line) for line in lines if line.startswith('{') and '"results"' in line)
        entries = []
        for item in raw['results']:
            path = item['path']
            start, end = item['start']['line'], item['end']['line']
            sha = BASE if label == 'base' else CANDIDATE
            fragment = '\n'.join(git('show', f'{sha}:{path}').decode().splitlines()[start - 1:end])
            entries.append({'rule': item['check_id'], 'path': path, 'start': start, 'end': end, 'fragment': fragment})
        findings[label] = sorted(entries, key=lambda x: (x['path'], x['start'], x['rule']))
    findings['identical'] = findings['base'] == findings['candidate']
    (OUT / 'semgrep_comparison.json').write_text(json.dumps(findings, indent=2) + '\n')
    test = 'tests/test_actor_hardening_v2_1_remediation.py'
    (OUT / 'xfail_comparison.json').write_text(json.dumps({'test_file_identical': git('show', f'{BASE}:{test}') == git('show', f'{CANDIDATE}:{test}'),
      'base_sha256': hashlib.sha256(git('show', f'{BASE}:{test}')).hexdigest(), 'candidate_sha256': hashlib.sha256(git('show', f'{CANDIDATE}:{test}')).hexdigest()}, indent=2) + '\n')
    print('candidate files changed:', changed, 'semgrep identical:', findings['identical'])


if __name__ == '__main__':
    main()

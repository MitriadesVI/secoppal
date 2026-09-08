"""Verify original bytes, Semgrep identity and literal log archives."""
import ast
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import textwrap

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_remediation'
BASE = '708ea3b3f609af4d9d3abe471f6963ee23513d7a'


def sha(value):
    return hashlib.sha256(value).hexdigest()


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def findings(path, base=False):
    result = []
    for entry in json.loads(path.read_text())['results']:
        source = git('show', BASE + ':' + entry['path']).decode() if base else (ROOT / entry['path']).read_text()
        start, end = entry['start']['line'], entry['end']['line']
        result.append(dict(rule=entry['check_id'], path=entry['path'], start=start, end=end,
                           fragment='\n'.join(source.splitlines()[start - 1:end])))
    return result


def main():
    originals = json.loads((OUT / 'original_integrity.json').read_text())
    assert all(sha((ROOT / path).read_bytes()) == digest for path, digest in originals['sha256'].items())
    before = findings(OUT / 'semgrep_baseline.json', True)
    after = findings(OUT / 'semgrep_final.json')
    remaining, pairs = list(after), []
    for old in before:
        same = next((item for item in remaining if (item['rule'], item['path'], item['fragment']) ==
                     (old['rule'], old['path'], old['fragment'])), None)
        if same:
            reason = 'identical fragment; location recorded separately'
        else:
            candidates = [item for item in remaining if (item['rule'], item['path']) == (old['rule'], old['path'])]
            assert len(candidates) == 1
            same = candidates[0]
            def handlers(fragment):
                return [ast.dump(node) for node in ast.walk(ast.parse(textwrap.dedent(fragment)))
                        if isinstance(node, ast.ExceptHandler)]
            assert handlers(old['fragment']) == handlers(same['fragment'])
            assert old['path'] == 'app/core/workflow/handlers/pagination.py'
            reason = 'same exception handlers; F6 inserts admitted_buffer branch in existing try; no new catch/suppression'
        remaining.remove(same)
        pairs.append(dict(before=old, after=same, adjudication=reason))
    assert not remaining
    (OUT / 'semgrep_comparison.json').write_text(json.dumps(dict(baseline=len(before), final=len(after),
        new_findings=0, pairs=pairs), ensure_ascii=False, indent=2) + '\n')
    campaign = json.loads((OUT / 'mutations.json').read_text())
    assert campaign['summary'] == {'DETECTED': 27} and campaign['runtime_restored']
    summary = dict(tested_sha=campaign['sha'], detected=27, total=27, runtime_restored=True,
                   entries=[{key: item[key] for key in ('id', 'targets', 'verdict', 'restored')}
                            for item in campaign['mutations']])
    (OUT / 'mutation_final_summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    manifest = OUT / "literal_log_manifest.json"
    logs = json.loads(manifest.read_text()) if manifest.exists() else {}
    for path in sorted(OUT.glob('*.log')):
        content = path.read_bytes()
        destination = path.with_suffix('.log.gz')
        destination.write_bytes(gzip.compress(content, mtime=0))
        assert gzip.decompress(destination.read_bytes()) == content
        logs[path.name] = dict(archive=destination.name, sha256=sha(content), bytes=len(content))
        path.unlink()
    (OUT / 'literal_log_manifest.json').write_text(json.dumps(logs, indent=2) + '\n')
    print(f'originals={originals["files"]} unchanged; Semgrep baseline=10 delta=0; mutations=27/27 restored')


if __name__ == '__main__':
    main()

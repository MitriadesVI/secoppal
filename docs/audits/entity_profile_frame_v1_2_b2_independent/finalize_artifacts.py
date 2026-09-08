"""Preserve literal evidence and verify that every candidate byte remains untouched."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess

from audit_runner import ROOT, OUT, CANDIDATE
from audit_output import emit


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    frozen = json.loads((OUT/'candidate_sha256.json').read_text())
    mismatches = [p for p, digest in frozen.items() if sha((ROOT/p).read_bytes()) != digest]
    assert not mismatches, mismatches
    changed = subprocess.check_output(['git','diff','--name-only',CANDIDATE], cwd=ROOT, text=True).splitlines()
    assert all(p.startswith(str(OUT.relative_to(ROOT))+'/') for p in changed)
    (OUT/'preservation_final.json').write_text(json.dumps(dict(candidate=CANDIDATE,
        candidate_files=len(frozen),candidate_manifest_sha256=sha((OUT/'candidate_sha256.json').read_bytes()),
        mismatches=mismatches,only_audit_changes=True),indent=2)+'\n')
    manifest_path = OUT/'literal_log_manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for path in sorted([*OUT.glob('*.txt'), *OUT.glob('*.xml')]):
        data = path.read_bytes()
        archive = path.with_suffix(path.suffix+'.gz')
        archive.write_bytes(gzip.compress(data, mtime=0))
        assert gzip.decompress(archive.read_bytes()) == data
        manifest[archive.name] = dict(uncompressed_bytes=len(data),sha256=sha(data))
        path.unlink()
    for name, entry in manifest.items():
        data = gzip.decompress((OUT/name).read_bytes())
        assert sha(data) == entry['sha256'] and len(data) == entry['uncompressed_bytes']
    manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
    (OUT/'artifact_sha256.json').write_text(json.dumps({str(p.relative_to(OUT)):sha(p.read_bytes())
        for p in sorted(OUT.rglob('*')) if p.is_file() and '__pycache__' not in p.parts
        and p.name != 'artifact_sha256.json'},indent=2)+'\n')
    emit('PRESERVED',len(frozen),'LOGS',len(manifest),'MISMATCHES',len(mismatches))


if __name__ == '__main__':
    main()

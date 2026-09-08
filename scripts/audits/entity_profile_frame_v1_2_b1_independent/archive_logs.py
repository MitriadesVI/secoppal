"""Lossless literal output archives, restricted to this independent audit."""
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'docs/audits/entity_profile_frame_v1_2_b1_independent'


def main():
    manifest_path = OUT / 'literal_log_manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for path in sorted(OUT.glob('*.log')):
        raw = path.read_bytes()
        archive = path.with_suffix('.log.gz')
        archive.write_bytes(gzip.compress(raw, mtime=0))
        assert gzip.decompress(archive.read_bytes()) == raw
        manifest[path.name] = dict(archive=archive.name, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        path.unlink()
    for record in manifest.values():
        raw = gzip.decompress((OUT / record['archive']).read_bytes())
        assert len(raw) == record['bytes'] and hashlib.sha256(raw).hexdigest() == record['sha256']
    manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
    print(len(manifest), 'literal archives verified')


if __name__ == '__main__':
    main()

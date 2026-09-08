"""Lossless log packaging with uncompressed byte hashes; no historical edits."""
import gzip
import hashlib
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent


def main():
    existing = OUT / 'literal_log_manifest.json'
    manifest = json.loads(existing.read_text()) if existing.exists() else {}
    for path in sorted(OUT.rglob('*.txt')):
        data = path.read_bytes()
        zipped = path.with_suffix(path.suffix + '.gz')
        zipped.write_bytes(gzip.compress(data, mtime=0))
        assert gzip.decompress(zipped.read_bytes()) == data
        manifest[str(zipped.relative_to(OUT))] = {
            'uncompressed_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
        path.unlink()
    (OUT / 'literal_log_manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()

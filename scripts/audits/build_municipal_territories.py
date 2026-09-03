"""Generate the runtime municipal territory authority from the frozen M0 DANE capture."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.entity_types import name_tokens

SOURCE = ROOT / 'artifacts/audits/municipal_m0_v1/sources'
TARGET = ROOT / 'app/data/municipal_territories_v1.json'


def build():
    manifest = json.loads((SOURCE / 'manifest.json').read_text())
    raw = gzip.decompress((SOURCE / 'divipola.json.gz').read_bytes())
    expected = next(s['uncompressed_sha256'] for s in manifest['snapshots'] if s['file'] == 'divipola.json.gz')
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError('Frozen DIVIPOLA capture hash mismatch')
    territories = sorted([
        dict(divipola=a['MPIO_CDPMP'], municipality=a['MPIO_CNMBRE'],
             department=a['DPTO_CNMBRE'], aliases=[' '.join(name_tokens(a['MPIO_CNMBRE']))])
        for row in json.loads(raw)['features']
        if (a := row['attributes'])['MPIO_TIPO'] == 'MUNICIPIO'
    ], key=lambda r: r['divipola'])
    if len(territories) != 1103 or len({r['divipola'] for r in territories}) != 1103:
        raise ValueError('M0 municipal denominator changed')
    result = dict(schema_version=1, version='DANE_DIVIPOLA_MGN_2025_M0_2026-09-03',
                  provenance=dict(source_url=manifest['divipola_source'], captured_at=manifest['as_of'],
                                  snapshot='artifacts/audits/municipal_m0_v1/sources/divipola.json.gz',
                                  uncompressed_sha256=expected, classification='MPIO_TIPO == MUNICIPIO'),
                  territories=territories)
    # One record per line: compact runtime artifact, reviewable diffs.
    header = json.dumps({k: v for k, v in result.items() if k != 'territories'}, ensure_ascii=False, indent=2)
    return (header[:-2] + ',\n  "territories": [\n' + ',\n'.join(
        '    ' + json.dumps(r, ensure_ascii=False) for r in territories
    ) + '\n  ]\n}\n').encode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    raw = build()
    if args.check:
        if TARGET.read_bytes() != raw:
            raise SystemExit('Runtime artifact differs from frozen DIVIPOLA')
    else:
        TARGET.write_bytes(raw)
    print(f'PASS: 1103 municipal territories; {len(raw)} bytes')


if __name__ == '__main__':
    main()

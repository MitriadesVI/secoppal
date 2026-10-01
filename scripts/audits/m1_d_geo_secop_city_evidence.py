"""Freeze SECOP's real (ciudad, departamento) pairs for the M1-D-GEO authority.

Read-only: two GROUP BY queries on the public datasets (procesos p6dx-8zbt,
contratos jbjy-vk9h). Writes ``docs/audits/m1_d_geo/secop_ciudad_evidence.json``.
The unit tests read that file and never touch the network.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.core.geo_authority import resolve_geography, secop_city_values  # noqa: E402
from app.core.municipal_territory import get_municipal_territories, key  # noqa: E402

OUT = ROOT / 'docs/audits/m1_d_geo/secop_ciudad_evidence.json'
DATASETS = {'p6dx-8zbt': ('ciudad_entidad', 'departamento_entidad'),
            'jbjy-vk9h': ('ciudad', 'departamento')}


def fetch(dataset: str, city: str, dept: str) -> list[dict]:
    query = urlencode({'$select': f'{city},{dept},count(*) as n', '$group': f'{city},{dept}',
                       '$order': 'n desc', '$limit': '20000'})
    with urlopen(f'https://www.datos.gov.co/resource/{dataset}.json?{query}', timeout=120) as resp:
        return json.load(resp)


def main(captured_at: str) -> None:
    pairs: dict[tuple[str, str], dict] = {}
    for dataset, (city, dept) in DATASETS.items():
        for row in fetch(dataset, city, dept):
            slot = pairs.setdefault((row[city], row[dept]), {'rows': {}})
            slot['rows'][dataset] = int(row['n'])
    registry = get_municipal_territories()
    out, official_rows, authority_rows, total = [], 0, 0, 0
    for (city, dept), slot in sorted(pairs.items()):
        if key(city) in ('no definido', ''):
            continue
        rows = sum(slot['rows'].values())
        total += rows
        official = bool(registry.lookup(city, dept))
        official_rows += rows if official else 0
        res = resolve_geography(city, 'MUNICIPALITY', 'POSITIVE', department_hint=dept)
        ok = res.decision == 'RESOLVED' and any(
            key(city) == key(v) for v in secop_city_values(res.municipality_divipola))
        authority_rows += rows if ok else 0
        out.append({'ciudad': city, 'departamento': dept, 'rows': slot['rows'],
                    'official_name_matches': official})
    OUT.write_text(json.dumps({
        'captured_at': captured_at, 'datasets': DATASETS,
        'rows_with_defined_city': total,
        'rows_matched_by_official_divipola': official_rows,
        'rows_matched_by_geo_authority': authority_rows,
        'pairs': out}, ensure_ascii=False, indent=1) + '\n', encoding='utf-8')
    print(f'pairs={len(out)} rows={total} official={official_rows} authority={authority_rows}')


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '2026-09-30')

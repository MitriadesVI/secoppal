"""Round 2 reconciliation: per-case outcomes, skips/xfails by provenance, flags."""
import json
from collections import Counter
from pathlib import Path
import sys

OUT = Path(__file__).resolve().parent
COMPONENTS = ('global', 'b2_independent', 'b3_remediation', 'b3_independent',
              'buyer_acceptance', 'buyer_local')


def load(name):
    """Large component reports are stored gzipped; gate_storage.json keeps the
    SHA256 of the original bytes so the archive is verifiable, not just smaller."""
    path = OUT / f'full_{name}.json'
    if path.exists():
        return json.loads(path.read_text())
    archive = path.with_suffix('.json.gz')
    if archive.exists():
        import gzip
        return json.loads(gzip.decompress(archive.read_bytes()))
    return None


def main():
    report = {'components': {}, 'skips': [], 'xfails': [], 'deselected': [],
              'unexpected': [], 'flags_summary': {}}
    for name in COMPONENTS:
        data = load(name)
        if data is None:
            report['components'][name] = {'status': 'MISSING — component produced no report'}
            continue
        calls = [r for r in data['results'] if r['when'] == 'call']
        phases = Counter((r['when'], r['outcome']) for r in data['results'])
        entry = {'collected': len(data['collection']), 'exitstatus': data.get('exitstatus'),
                 'phases': {f'{w}:{o}': n for (w, o), n in sorted(phases.items())},
                 'network_total': len(data['network']),
                 'network_denied': sum(1 for r in data['network'] if not r['allowed']),
                 'deselected': len(data['deselected'])}
        for row in data['results']:
            if row['outcome'] == 'skipped' and row.get('xfail'):
                report['xfails'].append({'component': name, 'nodeid': row['nodeid'],
                                         'reason': row['xfail']})
            elif row['outcome'] == 'skipped':
                # A skip raised inside the body reports at 'call', not 'setup':
                # counting only setup would hide most of them.
                report['skips'].append({'component': name, 'nodeid': row['nodeid'],
                                        'when': row['when'],
                                        'detail': (row.get('detail') or '')[:300]})
            if row['outcome'] == 'failed':
                report['unexpected'].append({'component': name, 'nodeid': row['nodeid'],
                                             'when': row['when'],
                                             'detail': (row.get('detail') or '')[:600]})
        report['deselected'].extend({'component': name, 'nodeid': n} for n in data['deselected'])
        flags = data.get('flags', {})
        entry['frame_true_before_call'] = sum(1 for v in flags.values() if v.get('before_call'))
        entry['frame_false_before_call'] = sum(1 for v in flags.values() if not v.get('before_call'))
        report['components'][name] = entry
        external = OUT / f'full_{name}_external.json'
        if external.exists():
            ext = json.loads(external.read_text())
            actor = Counter(v['actor'] for v in ext['flags'].values())
            frame = Counter(v['frame'] for v in ext['flags'].values())
            report['flags_summary'][name] = {'actor': dict(actor), 'frame': dict(frame),
                                             'denied': len(ext['denied'])}
    report['totals'] = {
        'skips': len(report['skips']), 'xfails': len(report['xfails']),
        'deselected': len(report['deselected']), 'unexpected': len(report['unexpected'])}
    (OUT / 'reconciliation_round2.json').write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    summary = {'components': report['components'], 'totals': report['totals'],
               'flags': report['flags_summary']}
    print(json.dumps(summary, indent=2, ensure_ascii=False))  # noqa: T201
    return 0 if not report['unexpected'] else 1


if __name__ == '__main__':
    sys.exit(main())

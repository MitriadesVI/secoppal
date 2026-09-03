"""Read-only capacity probe; candidate sets are not production memberships."""
from concurrent.futures import ThreadPoolExecutor
import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
from app.core.icbf_resolver import resolve_icbf_reference
from app.core.result_identity import result_identity
from app.core.socrata_transport.soda3 import Soda3HttpTransport
from app.core.soql_builder import SoQLBuilder

OUT = ROOT / 'artifacts/audits/buyer_group_execution_v1'
inventory = json.loads((ROOT / 'artifacts/audits/institutional_buyer_model_p01.json').read_text())['bogota_inventory']
central = [r['buyer_unit'] for r in inventory['ADMINISTRACION_CENTRAL_CANDIDATE']]
local = [r['buyer_unit'] for r in inventory['LOCAL_MAYORALTY_FDL_CANDIDATE']]
groups = {'icbf_33': resolve_icbf_reference('icbf').official_names,
          'central_19_source_names': central, 'local_20': local,
          'executive_39_source_names': central + local}

def quote(value):
    return "'" + value.replace("'", "''") + "'"

def probe(spec):
    group, dataset, names = spec
    buyer, identity, date = ('entidad', 'id_del_proceso', 'fecha_de_publicacion_del') if dataset == 'p6dx-8zbt' else ('nombre_entidad', 'id_contrato', 'fecha_de_firma')
    transport = Soda3HttpTransport(domain='www.datos.gov.co', app_token=os.getenv('SECOP_APP_TOKEN'))
    report = {'group': group, 'dataset': dataset, 'candidate_source_names': names,
              'membership_status': 'DISCOVERY_ONLY_NOT_RUNTIME_MEMBERSHIP',
              'as_of': datetime.now(UTC).isoformat(), 'timeout_seconds': 15,
              'window': ['2026-08-01', '2026-08-08'], 'calls': [], 'status': 'PARTIAL'}
    path = OUT / f'{group}-{dataset}.json'
    def save():
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    def query(soql, tag, ordered=False):
        start = time.monotonic()
        event = {'tag': tag, 'soql': soql, 'ordering': 'total' if ordered else 'no_preserve'}
        try:
            rows = transport.query(dataset, soql, timeout_seconds=15,
                                   ordering_specifier=event['ordering'])
            event.update(status='PASS', rows=len(rows), sha256=hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest())
            return rows
        except Exception as exc:
            event.update(status='ERROR', error_type=type(exc).__name__, error=str(exc)[:500])
            raise
        finally:
            event['latency_seconds'] = round(time.monotonic() - start, 3)
            report['calls'].append(event)
            save()
    def where(subset):
        return '(' + ' OR '.join(f'{buyer} = {quote(n)}' for n in subset) + f") AND {date} >= '2026-08-01T00:00:00' AND {date} < '2026-08-08T00:00:00'"
    spec = SoQLBuilder.SPECS[dataset]
    select = ':id AS physical_id,' + ','.join(spec.select_fields)
    order = f'{date} DESC,{spec.value} DESC,{identity} DESC,:id DESC'
    try:
        clause = where(names)
        count_sql = f'SELECT count(*) AS physical_rows,count(distinct {identity}) AS unique_ids WHERE {clause}'
        counts = query(count_sql, 'or_count')[0]
        report['counts_before'] = counts
        # Wider counts/pages measure the actual production timeout budget.
        # These are first-observed and repeat requests, not a claim of a
        # controlled cold cache. The complete materialization below remains
        # explicitly scoped to one week.
        for window in ('all_time', '2026'):
            broad = '(' + ' OR '.join(f'{buyer} = {quote(n)}' for n in names) + ')'
            if window == '2026':
                broad += f" AND {date} >= '2026-01-01T00:00:00' AND {date} < '2027-01-01T00:00:00'"
            for repeat in range(3):
                query(f'SELECT count(*) AS physical_rows,count(distinct {identity}) AS unique_ids WHERE {broad}', f'{window}_count_{repeat}')
                query(f'SELECT {select} WHERE {broad} ORDER BY {order} LIMIT 500 OFFSET 500', f'{window}_page_{repeat}', True)
        # A single globally ordered OR and the disjoint exact-name batches
        # must see the same number of physical rows. No NIT-based identity.
        batch_counts = []
        for i in range(0, len(names), 10):
            batch_counts.append(query(f'SELECT count(*) AS physical_rows WHERE {where(names[i:i+10])}', 'batch_count')[0])
        report['batch_physical_rows'] = sum(int(row['physical_rows']) for row in batch_counts)
        rows = []
        offset = 0
        expected = int(counts['physical_rows'])
        if expected > 20000:
            report['reason'] = 'WINDOW_EXCEEDS_MATERIALIZATION_BUDGET'
            return report
        while True:
            page = query(f'SELECT {select} WHERE {clause} ORDER BY {order} LIMIT 500 OFFSET {offset}', 'or_page', True)
            rows.extend(page)
            offset += len(page)
            if len(page) < 500:
                break
            if offset > expected + 500:
                raise ValueError('source changed during pagination')
        wider = query(f'SELECT {select} WHERE {clause} ORDER BY {order} LIMIT 1000', 'page_boundary_replay', True)
        after = query(count_sql, 'or_count_after')[0]
        stable = [result_identity(row, dataset) for row in rows]
        # Signature inspected before running; keeps the sole runtime authority.
        report.update(materialized_physical_rows=len(rows),
                      materialized_unique_ids=len({r.stable_key for r in stable if r.stable_key}),
                      unstable_rows=sum(r.stable_key is None for r in stable),
                      unique_physical_ids=len({r['physical_id'] for r in rows}),
                      wrong_names=sorted({r[buyer] for r in rows} - set(names)),
                      page_boundary_equal=rows[:1000] == wider,
                      counts_after=after)
        report['status'] = 'PASS' if (
            counts == after and len(rows) == expected == report['batch_physical_rows']
            and report['materialized_unique_ids'] == int(counts['unique_ids'])
            and report['unique_physical_ids'] == len(rows)
            and not report['unstable_rows'] and not report['wrong_names']
            and report['page_boundary_equal']
        ) else 'PARTIAL'
    except Exception as exc:
        report['error_type'] = type(exc).__name__
        report['error'] = str(exc)[:500]
    finally:
        transport.close()
        save()
    print(group, dataset, report['status'], flush=True)
    return report

def main():
    global OUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUT)
    args = parser.parse_args()
    OUT = args.output.resolve()
    OUT.mkdir(parents=True, exist_ok=True)
    load_dotenv(ROOT / '.env')
    specs = [(group, ds, names) for group, names in groups.items()
             for ds in ('p6dx-8zbt', 'jbjy-vk9h')]
    with ThreadPoolExecutor(max_workers=2) as pool:
        reports = list(pool.map(probe, specs))
    summary = {
        'audit': 'BUYER_GROUP_EXECUTION_STRATEGY_V1',
        'status': 'PASS' if all(r['status'] == 'PASS' for r in reports) else 'PARTIAL',
        'strategy': 'EXACT_OR_SINGLE_GLOBAL_ORDER',
        'full_materialization_scope': ['2026-08-01', '2026-08-08'],
        'wide_windows': ['all_time', '2026'],
        'source': 'artifacts/audits/institutional_buyer_model_p01.json',
        'membership_caveat': '19/39 count physical source names; Salud is a two-dataset source pair, not two accredited BuyerUnits.',
        'cases': [{k:r.get(k) for k in ('group','dataset','status','materialized_physical_rows','materialized_unique_ids','page_boundary_equal')} for r in reports],
        'runtime_changed': False,
    }
    (OUT / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary['status'] == 'PASS' else 1

if __name__ == '__main__':
    raise SystemExit(main())

"""Three bounded live profiles plus exact-WHERE point checks; no Settings/persistence."""

from dataclasses import asdict
from datetime import date, datetime, timezone
from decimal import Decimal
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
import httpx
from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.aspects.common import build_where
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.secop_client import SecopClient
from app.core.socrata_transport.soda3 import Soda3HttpTransport
from app.core.soql_builder import SoQLBuilder

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
OUT = args.output
if OUT.exists():
    parser.error('output already exists; preserve previous observations')
receipts = []
lock = threading.Lock()


class RecordingTransport(Soda3HttpTransport):
    def _post(self, dataset_id, soql, timeout_seconds, ordering_specifier):
        row = {
            'dataset_id': dataset_id,
            'soql': soql,
            'timeout_seconds': timeout_seconds,
            'ordering_specifier': str(ordering_specifier),
            'observed_at': datetime.now(timezone.utc).isoformat(),
        }
        start = time.perf_counter()
        try:
            payload = super()._post(dataset_id, soql, timeout_seconds, ordering_specifier)
            row['status'] = 'SUCCESS'
            row['payload'] = payload
            row['payload_sha256'] = hashlib.sha256(
                json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()
            return payload
        except Exception as exc:
            row['status'] = type(exc).__name__
            row['error'] = str(exc)
            raise
        finally:
            row['wall_ms'] = (time.perf_counter() - start) * 1000
            with lock:
                receipts.append(row)


router = QueryRouter(EntityResolver(ROOT / 'app/data/aliases_db.json'))
builder = SoQLBuilder()
report = {
    'observed_at': datetime.now(timezone.utc).isoformat(),
    'transport': 'SODA3',
    'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
    'settings_constructed': False,
    'period': '2026',
    'is_sla': False,
    'canaries': [],
    'receipts': receipts,
}


def point_check(client, turn, profile):
    checks = []
    by_id = {c.call_id: c for c in profile.source_calls.calls}
    for dataset, call_id in [('p6dx-8zbt', 'process_activity'), ('jbjy-vk9h', 'contract_values')]:
        spec, where = build_where(builder, dataset, turn.params)
        profile_where = by_id[call_id].soql.split(' WHERE ', 1)[1].split(' GROUP BY ', 1)[0]
        assert profile_where == where
        select = 'count(*) AS exact_count'
        if dataset == 'jbjy-vk9h':
            select += f', sum({spec.value}) AS exact_value'
        soql = f'SELECT {select} WHERE {where}'
        item = {'dataset': dataset, 'same_where': True, 'soql': soql}
        try:
            rows = client.aggregate(dataset, soql)
            row = rows[0]
            count = int(row['exact_count'])
            profile_count = profile.activity.fact(
                'process_count' if dataset == 'p6dx-8zbt' else 'contract_count'
            )
            item.update(
                point_count=count,
                profile_count=profile_count,
                exact_count_match=count == profile_count,
            )
            if dataset == 'jbjy-vk9h':
                raw = row['exact_value']
                actual = profile.values.fact('total_contract_value')
                item.update(
                    point_value=str(raw),
                    profile_value=str(actual),
                    exact_value_match=actual is not None
                    and Decimal(str(raw)) == Decimal(str(actual)),
                )
            item['status'] = (
                'PASS'
                if item['exact_count_match'] and item.get('exact_value_match', True)
                else 'NO_MATCH_OR_UNAVAILABLE'
            )
        except Exception as exc:
            item.update(status='UNAVAILABLE', error_type=type(exc).__name__, error=str(exc))
        checks.append(item)
    return checks


with httpx.Client() as http_client:
    transport = RecordingTransport(
        domain='www.datos.gov.co', app_token=None, http_client=http_client
    )
    client = SecopClient(
        domain='www.datos.gov.co', app_token=None, timeout=8, max_retries=1, transport=transport
    )
    for name, query in [
        ('BARANOA', 'analiza la alcaldía de Baranoa en 2026'),
        ('BOGOTA', 'perfil de la alcaldía mayor de Bogotá en 2026'),
        ('BARRANQUILLA', 'analiza la alcaldía de Barranquilla en 2026'),
    ]:
        parsed = router.parse(query, today=date(2026, 9, 6)).params
        turn = classify_entity_profile_turn(query, parsed)
        assert turn.scope and turn.identity and turn.focus == 'overview'
        before = len(receipts)
        start = time.perf_counter()
        profile = build_entity_profile(
            scope=turn.scope,
            identity=turn.identity,
            params=turn.params,
            focus=turn.focus,
            secop_client=client,
            soql_builder=builder,
        )
        wall = (time.perf_counter() - start) * 1000
        item = {
            'name': name,
            'query': query,
            'scope': asdict(turn.scope),
            'identity': asdict(turn.identity),
            'wall_ms': wall,
            'calls': profile.source_calls.calls_total,
            'http_calls': len(receipts) - before,
            'source_calls': [asdict(c) for c in profile.source_calls.calls],
            'sections': {
                k: getattr(profile, k).status.value
                for k in ['activity', 'values', 'modalities', 'suppliers', 'temporal', 'recent']
            },
            'process_count': profile.activity.fact('process_count'),
            'contract_count': profile.activity.fact('contract_count'),
            'contract_value': profile.values.fact('total_contract_value'),
            'partial': profile.partial,
            'response': render_entity_profile(profile),
        }
        assert item['calls'] <= 6 and item['http_calls'] <= 6
        if name in {'BARANOA', 'BOGOTA'}:
            item['consistency'] = point_check(client, turn, profile)
        report['canaries'].append(item)
        OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + '\n')
report['http_calls_total'] = len(receipts)
assert len(receipts) <= 22
OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + '\n')
sys.stdout.write(json.dumps({"record": str(OUT), "http_calls": len(receipts)}) + "\n")

"""Small SQL-evaluated source, reusing the corrected Buyer-Actor transport."""
import re
import socket
import threading
import json
import os
from pathlib import Path
import traceback
import pytest
from tests.buyer_actor_independent_support import attach_source


@pytest.fixture
def offline_universe(monkeypatch):
    monkeypatch.setenv("BURR_TELEMETRY_ENABLED", "false")
    import burr.telemetry
    monkeypatch.setattr(burr.telemetry, "g_telemetry_enabled", False)
    attempts = []
    def forbidden(*args, **kwargs):
        attempts.append(''.join(traceback.format_stack(limit=18)))
        raise AssertionError('Contract Universe acceptance is offline')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    yield attempts
    assert not attempts, attempts


def source(flow, monkeypatch, buyer, minimum):
    conn = attach_source(flow, monkeypatch, buyer, minimum)
    from tests.contract_universe_sql_dates import register_dates
    register_dates(conn)
    template = dict(conn.execute('SELECT * FROM contracts LIMIT 1').fetchone())
    conn.execute('DELETE FROM contracts')
    fields = flow.soql_builder.SPECS['p6dx-8zbt'].select_fields
    conn.execute('CREATE TABLE processes (' + ','.join(field + ' TEXT' for field in fields) + ')')
    original_query = flow.secop_client.query
    def execute(dataset, sql, *args, **kwargs):
        if dataset != 'p6dx-8zbt':
            return original_query(dataset, sql, *args, **kwargs)
        flow.audit_trace['sql'].append({'dataset': dataset, 'soql': sql})
        try:
            evaluated = re.sub(r'\bWHERE\b', 'FROM processes WHERE', sql, count=1, flags=re.I)
            return [dict(row) for row in conn.execute(evaluated)]
        except Exception as exc:
            flow.audit_trace['errors'].append(repr(exc))
            raise
    source_lock = threading.RLock()
    def query(dataset, sql, *args, **kwargs):
        with source_lock:
            return execute(dataset, sql, *args, **kwargs)
    def count(*args, **kwargs):
        rows = query(*args, **kwargs)
        return int(next(iter(rows[0].values())))
    monkeypatch.setattr(flow.secop_client, 'count', count)
    monkeypatch.setattr(flow.secop_client, 'query', query)
    monkeypatch.setattr(flow.secop_client, 'aggregate', query)
    conn.create_function('upper', 1, lambda x: x.upper() if x is not None else None)
    states = ['En ejecución', 'terminado', 'Cerrado', 'Suspendido', 'Modificado',
              'Prorrogado', 'cedido', 'Aprobado']
    rows = []
    for i, state in enumerate(states):
        rows.append(dict(template, id_contrato=f'VALID-{i}', referencia_del_contrato=f'V-{i}',
                         estado_contrato=state, valor_del_contrato=minimum + 100,
                         fecha_de_firma=(None if i in (1, 2) else '2025-03-01T00:00:00.000'
                                         if i == 6 else '2026-03-01T00:00:00.000')))
    for i in range(55):
        rows.append(dict(template, id_contrato=f'EXCLUDED-{i:03}', referencia_del_contrato=f'E-{i}',
                         estado_contrato=['Cancelado', 'Borrador', 'enviado Proveedor',
                                          'En aprobación', None, 'DESCONOCIDO'][i % 6],
                         valor_del_contrato=999_000_000_000,
                         fecha_de_firma=None if (i // 6) % 2 else '2026-09-01T00:00:00.000',
                         proveedor_adjudicado='EXCLUDED HUGE', documento_proveedor='88888888'))
    rows.extend([dict(rows[0], id_contrato='WRONG-BUYER', nombre_entidad='WRONG BUYER'),
                 dict(rows[0], id_contrato='WRONG-VALUE', valor_del_contrato=1)])
    conn.executemany('INSERT INTO contracts VALUES (' + ','.join('?' for _ in template) + ')',
                     [list(row.values()) for row in rows])
    original_run = flow.run_query
    def observed_run(*args, **kwargs):
        result = original_run(*args, **kwargs)
        destination = os.environ.get('CONTRACT_UNIVERSE_EVIDENCE')
        if destination:
            path = Path(destination)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('a') as stream:
                stream.write(json.dumps({'query': args[0] if args else kwargs.get('query'),
                                         'result': result, 'source': flow.audit_trace},
                                        ensure_ascii=False, default=str) + '\n')
        return result
    monkeypatch.setattr(flow, 'run_query', observed_run)
    return conn

"""Synthetic SQL boundary and an external attempt ledger for two legacy tests."""
import socket
import traceback

import pytest

from tests.buyer_actor_independent_support import attach_source, persist


@pytest.fixture
def actor_harness_source(monkeypatch, request):
    attempts, sources = [], []

    def forbidden(*args, **kwargs):
        attempts.append(''.join(traceback.format_stack(limit=30)))
        raise AssertionError('External connection in synthetic actor test')

    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)

    def install(flow, actors):
        flow.audit_trace = {'sql': [], 'errors': []}
        conn = attach_source(flow, monkeypatch)
        template = dict(conn.execute('SELECT * FROM contracts LIMIT 1').fetchone())
        conn.execute('DELETE FROM contracts')
        for rank, actor in enumerate(actors, 1):
            for year, value in ((2025, 600_000_000), (2026, 700_000_000)):
                row = dict(template, id_contrato=f'HARNESS-{rank}-{year}',
                           referencia_del_contrato=f'H-{rank}-{year}',
                           proveedor_adjudicado=actor['name'],
                           documento_proveedor=actor.get('base_number', f'90{rank:07d}'),
                           tipodocproveedor=actor.get('document_type', 'NIT'),
                           valor_del_contrato=value, fecha_de_firma=f'{year}-03-01T00:00:00.000')
                conn.execute('INSERT INTO contracts VALUES (' + ','.join('?' for _ in row) + ')',
                             list(row.values()))
        sources.append((flow, conn))
        return flow

    yield install
    errors = [error for flow, _ in sources for error in flow.audit_trace['errors']]
    persist(request.node.name, {'attempts': attempts,
                               'sources': [flow.audit_trace for flow, _ in sources]})
    for _, conn in sources:
        conn.close()
    assert not attempts, attempts
    assert not errors, errors

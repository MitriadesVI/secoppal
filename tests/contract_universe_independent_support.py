"""Independent explicit matrix and recording; only external source is simulated."""
import json
import os
from pathlib import Path

from tests.contract_universe_support import source
from tests.buyer_actor_independent_support import index_at, workflow

ADMITTED = ('En ejecución', 'Cerrado', 'Modificado', 'terminado', 'cedido',
            'Suspendido', 'Prorrogado')
EXCLUDED = ('Cancelado', 'Borrador', 'enviado Proveedor', 'En aprobación',
            None, '', 'ESTADO NUEVO')
DESCRIPTION = 'elaboración de borradores de documentos y contratos de consultoría'


def record(name, value):
    destination = os.environ.get('CU_INDEPENDENT_TRACE')
    if destination:
        with Path(destination).open('a') as stream:
            stream.write(json.dumps(dict(case=name, evidence=value), ensure_ascii=False,
                                    default=str) + '\n')


def setup(root, monkeypatch, buyer, minimum, frame=False, aliases=()):
    flow = workflow(root, monkeypatch, index_at(root, list(aliases)), frame)
    conn = source(flow, monkeypatch, buyer, minimum)
    template = dict(conn.execute('SELECT * FROM contracts LIMIT 1').fetchone())
    conn.execute('DELETE FROM contracts')
    rows = []
    # Aprobado is measured separately, never used to infer the acceptance oracle.
    for i, state in enumerate((*ADMITTED, 'Aprobado')):
        for suffix, date in [('dated', '2026-03-01T00:00:00.000'), ('absent', None),
                             ('invalid', '2026-02-30')]:
            rows.append(dict(template, id_contrato=f'A-{i}-{suffix}', estado_contrato=state,
                             valor_del_contrato=minimum + 100, fecha_de_firma=date,
                             objeto_del_contrato=DESCRIPTION, descripcion_del_proceso=DESCRIPTION))
    for i in range(70):
        rows.append(dict(template, id_contrato=f'X-{i:03}', estado_contrato=EXCLUDED[i % 7],
                         valor_del_contrato=900_000_000_000,
                         fecha_de_firma='2026-08-01' if (i // 7) % 2 else None,
                         objeto_del_contrato=DESCRIPTION, descripcion_del_proceso=DESCRIPTION))
    rows.extend([dict(rows[0], id_contrato='OTHER-BUYER', nombre_entidad='OTHER BUYER'),
                 dict(rows[0], id_contrato='LOW-VALUE', valor_del_contrato=1)])
    conn.executemany('INSERT INTO contracts VALUES (' + ','.join('?' for _ in template) + ')',
                     [list(row.values()) for row in rows])
    return flow, conn


def capture(flow, name, result):
    record(name, dict(result=result, source=flow.audit_trace))


def ids(result):
    return {r['id_contrato'] for r in result.get('results', [])}

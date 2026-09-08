"""Independent workflow fixture with a SQLite-evaluated synthetic source."""
import json
import os
from datetime import date
from pathlib import Path
import re
import sqlite3
import threading
import unicodedata

from app.config import Settings, actor_profile_enabled, get_settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.data.actor_alias_index import build_actor_alias_index

CARTAGENA = 'ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS'
BARRANQUILLA = 'DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA'
SEEDS = [
    ('contratos de alcaldia de cartagena por mas de 500 millones',
     'alcaldia de cartagena', CARTAGENA, 500_000_000),
    ('que ha contratado alcaldia de barranquilla por mas de 2000 millones',
     'alcaldia de barranquilla', BARRANQUILLA, 2_000_000_000),
]


def alias(name, number='71000001', kind='CC', frequency=2):
    return dict(proveedor_adjudicado=name, documento_proveedor=number,
                tipodocproveedor=kind, occurrences=frequency,
                first_seen='2024-01-01', last_seen='2026-01-01')


def index_at(root, rows, mode=1):
    path = root / 'independent-aliases.sqlite'
    if mode == 'absent':
        return path
    if mode == 'incompatible':
        with sqlite3.connect(path) as conn:
            conn.execute('CREATE TABLE not_an_actor_index (x TEXT)')
    else:
        build_actor_alias_index(rows, path, source_dataset='independent-synthetic',
                               built_at='2026-09-08T12:00:00+00:00')
    return path


def persist(label, trace):
    destination = os.environ.get('BUYER_INDEPENDENT_EVIDENCE')
    if destination:
        path = Path(destination)
        path.mkdir(parents=True, exist_ok=True)
        (path / (label + '.json')).write_text(json.dumps(trace, ensure_ascii=False,
                                                       indent=2, default=str) + '\n')


def workflow(root, monkeypatch, index, frame):
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', str(frame).lower())
    monkeypatch.setenv('SECOPPAL_ACTOR_PROFILE_ENABLED', 'true')
    get_settings.cache_clear()
    settings = Settings(secop_actor_alias_index_path=index, deepseek_api_key=None,
                        secop_feedback_path=root / 'feedback.jsonl',
                        secop_feedback_db_path=root / 'feedback.sqlite',
                        secop_feedback_sqlite_enabled=False)
    assert actor_profile_enabled() is True
    from app import config
    reader = getattr(config, 'entity_profile_frame_enabled', lambda: False)
    assert reader() is frame
    flow = SecopalWorkflow(settings)
    flow.conv_store = ConversationStore(root / 'conversations')
    monkeypatch.setattr(flow.narrator, 'narrate_with_grounding', lambda **kwargs: None)
    flow.audit_trace = {'flags': {'actor_reader': actor_profile_enabled(),
                                 'frame_reader': reader(),
                                 'settings_actor': settings.secoppal_actor_profile_enabled},
                        'sql': [], 'errors': [], 'resolver': []}
    for method in ('resolve', 'resolve_exact'):
        original = getattr(flow.actor_name_resolver, method)

        def call(mention, *args, _method=method, _original=original, **kwargs):
            result = _original(mention, *args, **kwargs)
            flow.audit_trace['resolver'].append([_method, mention, str(result.status)])
            return result

        monkeypatch.setattr(flow.actor_name_resolver, method, call)
    return flow


def attach_source(flow, monkeypatch, buyer=CARTAGENA, minimum=500_000_000,
                  supplier='FUNDACARIBE'):
    columns = ('id_contrato referencia_del_contrato nombre_entidad departamento ciudad '
               'objeto_del_contrato descripcion_del_proceso tipo_de_contrato '
               'modalidad_de_contratacion valor_del_contrato valor_facturado estado_contrato '
               'fecha_de_firma proveedor_adjudicado documento_proveedor urlproceso tipodocproveedor').split()
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    lock = threading.RLock()
    conn.row_factory = sqlite3.Row
    conn.create_function('unaccent', 1, lambda x: ''.join(
        c for c in unicodedata.normalize('NFD', str(x)) if not unicodedata.combining(c)))
    conn.create_function('date_trunc_y', 1, lambda x: str(x)[:4] + '-01-01T00:00:00.000')
    conn.execute('CREATE TABLE contracts (' + ','.join(
        col + (' INTEGER' if col.startswith('valor_') else ' TEXT') for col in columns) + ')')
    rows = []
    for i in range(65):
        rows.append((f'INDEPENDENT-{i:03}', f'REF-{i}', buyer if i != 60 else 'WRONG BUYER',
                     'Bolívar', 'Cartagena', 'software audit', 'software audit',
                     'Prestacion de servicios', 'Contratacion directa',
                     minimum + 1 if i != 61 else 1, 0, 'Firmado',
                     '2026-03-01T00:00:00.000' if i != 62 else '2025-03-01T00:00:00.000',
                     supplier, '71000001' if i != 63 else '71000999',
                     'https://example.invalid/audit', 'CC'))
    conn.executemany('INSERT INTO contracts VALUES (' + ','.join('?' for _ in columns) + ')', rows)

    def execute(dataset, soql, *args, **kwargs):
        flow.audit_trace['sql'].append({'dataset': dataset, 'soql': soql})
        try:
            assert dataset in ('jbjy-vk9h', 'contratos'), dataset
            query = re.sub(r'\bWHERE\b', 'FROM contracts WHERE', soql, count=1, flags=re.I)
            if 'FROM contracts' not in query:
                raise AssertionError('Source query without WHERE')
            with lock:
                result = [dict(row) for row in conn.execute(query)]
            flow.audit_trace.setdefault('source_rows', []).append(result)
            return result
        except Exception as exc:
            flow.audit_trace['errors'].append(repr(exc))
            raise

    monkeypatch.setattr(flow.secop_client, 'query', execute)
    monkeypatch.setattr(flow.secop_client, 'aggregate', execute)
    monkeypatch.setattr(flow.secop_client, 'with_timeout', lambda *args, **kwargs: flow.secop_client)
    monkeypatch.setattr(flow.secop_client, 'derived_for_timeout', lambda *args, **kwargs: flow.secop_client)
    def count(*args, **kwargs):
        rows = execute(*args, **kwargs)
        return int(next(iter(rows[0].values())))

    monkeypatch.setattr(flow.secop_client, 'count', count)
    return conn


def say(flow, text, chat='audit'):
    result = flow.run_query(text, chat_id=chat, today=date(2026, 9, 8))
    flow.audit_trace.update(query=text, result=result)
    assert not flow.audit_trace['errors'], flow.audit_trace
    return result


def assert_buyer(flow, result, buyer, amount):
    params = result['resolved_params']
    assert params.get('dataset') == 'contratos', result
    assert params.get('entidad_resolved') == buyer, result
    assert params.get('valor_min') == amount, result
    assert not any(params.get(k) for k in (
        'actor_scope', 'actor_pending', 'contratista', 'actor_resolution',
        'actor_resolution_method')), result
    assert not result.get('needs_clarification'), result
    assert buyer in result['response'], result
    assert result.get('rows'), result
    assert flow.audit_trace['sql'] or result.get('soql_query'), result
    for call in flow.audit_trace['sql']:
        where = call['soql'].split(' WHERE ', 1)[-1].split(' ORDER BY ', 1)[0]
        assert buyer in where and str(amount) in where, where
        assert 'documento_proveedor' not in where.lower(), where
        assert 'contratado' not in where.lower(), where
    for row in result['results']:
        assert row['nombre_entidad'] == buyer
        assert int(row['valor_del_contrato']) >= amount

"""Each independent TOP keeps its own evidence across assembly/publication/reload."""
from dataclasses import asdict, replace
from decimal import Decimal

from hypothesis import given, settings, strategies as st
import pytest
from audit_output import emit

from app.core.entity_profile import frame_response
from app.core.entity_profile.execution_record import ExecutionRecord
from app.core.entity_profile.models import SectionStatus
from app.core.entity_profile.render import profile_fact_lines
from app.core.entity_profile.supplier_rankings import accredit_ranking, section_ranking
from app.core.entity_profile.aspects.suppliers import parse_suppliers
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


QUERY = 'analiza contratos de alcaldia de baranoa en agosto'
CASES = [
    ('valid', 'valid'), ('invalid', 'valid'), ('valid', 'invalid'), ('invalid', 'invalid'),
    ('missing', 'valid'), ('valid', 'missing'), ('missing', 'missing'),
    ('absent', 'valid'), ('valid', 'absent'),
    ('error', 'valid'), ('valid', 'error'), ('error', 'error'),
    ('empty', 'valid'), ('valid', 'empty'), ('empty', 'empty'),
    ('bad_count', 'valid'), ('valid', 'bad_count'),
]


def row(document, name, amount='700000000', count='3'):
    return dict(documento_proveedor=document, tipodocproveedor='NIT', supplier_name=name,
                contract_count=count, total_contract_value=amount)


class MatrixClient(WorkflowClient):
    def __init__(self, count_mode, value_mode, overlap):
        super().__init__()
        self.modes = dict(count=count_mode, value=value_mode)
        self.overlap = overlap
        self.returned = {}

    def aggregate(self, dataset, sql):
        if 'GROUP BY documento_proveedor' not in sql:
            return super().aggregate(dataset, sql)
        self._record('aggregate', dataset, sql)
        ranking = 'count' if 'ORDER BY contract_count DESC' in sql else 'value'
        mode = self.modes[ranking]
        if mode == 'error':
            raise TimeoutError('offline matrix: selected supplier query failed')
        if mode == 'empty':
            self.returned[ranking] = []
            return []
        rows = [row('900123456' if ranking == 'count' else '800123456',
                    'PROVEEDOR CANTIDAD' if ranking == 'count' else 'PROVEEDOR VALOR'),
                row('700123456' if ranking == 'count' or self.overlap else '600123456',
                    'PROVEEDOR COMPARTIDO' if self.overlap else 'PROVEEDOR SEGUNDO', '500000000', '1')]
        # In overlapping TOPs damage the SAME identity in only one query.
        damaged = rows[-1]
        if mode == 'invalid':
            damaged['total_contract_value'] = 'Infinity'
        elif mode == 'missing':
            damaged['total_contract_value'] = None
        elif mode == 'absent':
            damaged.pop('total_contract_value')
        elif mode == 'bad_count':
            damaged['contract_count'] = 'Infinity'
        self.returned[ranking] = rows
        return rows


@pytest.mark.parametrize('focus', ['overview', 'suppliers'])
@pytest.mark.parametrize('overlap', [False, True], ids=['disjoint', 'overlap'])
@pytest.mark.parametrize('count_mode,value_mode', CASES)
def test_independent_ranking_matrix(frame_v12, monkeypatch, focus, overlap, count_mode, value_mode):
    chat = Conversation('b3-ranking-matrix')
    assert chat.say(QUERY).result['continuation_published']
    client = MatrixClient(count_mode, value_mode, overlap)
    profiles = []
    original = frame_response.build_profile_sections

    def observe(*args, **kwargs):
        result = original(*args, **kwargs)
        profiles.append(result)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(frame_response, 'build_profile_sections', observe)
        result = chat.say('proveedores' if focus == 'suppliers' else QUERY, client=client)
    assert result.result['continuation_published'] and not result.clarifies
    assert result.response.startswith('ALCALDIA MUNICIPAL DE BARANOA — SECOP II Contratos')
    assert len(result.calls) == (2 if focus == 'suppliers' else 4)
    assert all('BARANOA' in sql and '2026-08' in sql for sql in result.sql)
    profile = profiles[-1]
    section = profile.suppliers
    record = chat.execution_record()
    assert ExecutionRecord.from_dict(record).to_dict() == record
    assert chat.published().envelope.response == result.response
    for ranking, mode in client.modes.items():
        decision = section_ranking(section, ranking)
        key = 'top_by_contract_count' if ranking == 'count' else 'top_by_contract_value'
        rows = section.fact(key)
        assert decision.available == (mode != 'error')
        assert decision.empty == (mode == 'empty')
        valid_metric = mode != 'bad_count' if ranking == 'count' else mode not in ('invalid', 'missing', 'absent')
        accredited = mode != 'error' and valid_metric
        assert decision.accredited == accredited
        assert decision.amounts_valid == (mode not in ('error', 'invalid', 'missing', 'absent'))
        label = 'número de contratos' if ranking == 'count' else 'valor'
        claim = f'Principal contratista por {label}:' if focus == 'overview' else (
            'Principales contratistas por número de contratos' if ranking == 'count'
            else 'Principales contratistas por valor contratado')
        assert (claim in result.response) == (accredited and mode != 'empty')
        assert not (claim in result.response and f'ranking por {label} no está acreditado' in result.response)
        call = next(call for call in record['calls'] if call['call_id'] == 'supplier_' + ranking)
        assert call['attempted'] and call['status'] == ('TIMEOUT' if mode == 'error' else 'SUCCESS')
        if mode in ('empty', 'error'):
            assert not call['metrics']
            assert rows == (() if mode == 'empty' else None)
            if focus == 'suppliers' and count_mode == value_mode == 'error':
                assert 'Ninguna de las fuentes respondió' in result.response
                assert 'No interpreto ese silencio como ausencia de actividad' in result.response
            else:
                assert (f'No se obtuvo el TOP por {"cantidad" if ranking == "count" else "valor"}' in result.response) == (mode == 'error')
            continue
        assert len(rows) == 2
        assert [item.actor_identity for item in rows] == [
            'NIT:' + ('60012345' if raw['documento_proveedor'] == '600123456' else raw['documento_proveedor'])
            for raw in client.returned[ranking]]
        assert rows[0].total_contract_value == 700000000
        assert rows[-1].total_contract_value == (None if mode in ('invalid', 'missing', 'absent') else 500000000)
        amounts = [m for m in call['metrics'] if m['name'] == 'total_contract_value']
        assert sum(m['status'] == 'VALID' for m in amounts) == (1 if mode in ('invalid', 'missing', 'absent') else 2)
        assert all(m['value'] is None for m in amounts if m['status'] != 'VALID')
        assert all(Decimal(m['value']).is_finite() for m in amounts if m['status'] == 'VALID')
        counts = [m for m in call['metrics'] if m['name'] == 'contract_count']
        assert sum(m['status'] == 'VALID' for m in counts) == (1 if mode == 'bad_count' else 2)
    expected = (SectionStatus.UNAVAILABLE if count_mode == value_mode == 'error' else
                SectionStatus.SUPPORTED if all(mode in ('valid', 'empty') for mode in client.modes.values()) else SectionStatus.PARTIAL)
    assert section.status == expected
    if focus == 'overview':
        assert '4 contratos' in result.response and profile.values.fact('total_contract_value') is not None
    emit('B3_RANKING_MATRIX', focus, overlap, client.modes, result.response, asdict(section), record)
    next_turn = chat.say('recientes')
    assert next_turn.result['continuation_published'] and not next_turn.clarifies
    assert all('BARANOA' in sql and '2026-08' in sql for sql in next_turn.sql)


def test_render_consumes_the_assembler_decision(frame_v12, monkeypatch):
    profiles = []
    original = frame_response.build_profile_sections

    def observe(*args, **kwargs):
        profile = original(*args, **kwargs)
        profiles.append(profile)
        return profile

    with monkeypatch.context() as patch:
        patch.setattr(frame_response, 'build_profile_sections', observe)
        Conversation('b3-render-consumer').say(QUERY)
    profile = profiles[-1]
    assert 'Principal contratista por valor:' in '\n'.join(profile_fact_lines(profile))
    # Consumer contract: a renderer is not allowed to re-accredit facts after
    # the assembler supplies a negative decision, even if numbers look valid.
    facts = tuple(replace(fact, value=replace(fact.value, accredited=False))
                  if fact.key == 'value_ranking_accreditation' else fact for fact in profile.suppliers.facts)
    denied = replace(profile, suppliers=replace(profile.suppliers, facts=facts))
    assert 'Principal contratista por valor:' not in '\n'.join(profile_fact_lines(denied))
    focused = '\n'.join(profile_fact_lines(replace(denied, focus='suppliers')))
    assert 'Principales contratistas por valor contratado' not in focused
    assert 'ranking por valor no acreditado' in focused


@settings(max_examples=40, derandomize=True, deadline=None)
@given(st.lists(st.one_of(st.none(), st.sampled_from(['Infinity', 'NaN', '1e999', '', '7', '0'])), min_size=1, max_size=5))
def test_amount_damage_never_invalidates_valid_count_order(amounts):
    parsed = parse_suppliers([row(str(900123450 + i), 'supplier', amount) for i, amount in enumerate(amounts)])
    count = accredit_ranking('count', parsed, available=True)
    value = accredit_ranking('value', parsed, available=True)
    assert count.accredited and count.counts_valid
    assert value.accredited == all(amount in ('7', '0') for amount in amounts)

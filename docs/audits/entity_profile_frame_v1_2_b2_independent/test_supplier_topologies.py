"""Independent TOP5 sets: both calls need not contain the same suppliers."""
from decimal import Decimal

import pytest
from audit_output import emit

from app.core.entity_profile import frame_response
from app.core.entity_profile.models import SectionStatus
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


class IndependentRankings(WorkflowClient):
    def __init__(self, bad_ranking=None, valid_amount='700000000'):
        super().__init__()
        self.bad_ranking = bad_ranking
        self.valid_amount = valid_amount

    def aggregate(self, dataset, sql):
        rows = super().aggregate(dataset, sql)
        if 'GROUP BY documento_proveedor' not in sql:
            return rows
        ranking = 'count' if 'ORDER BY contract_count DESC' in sql else 'value'
        bad = self.bad_ranking in (ranking, 'both')
        return [dict(documento_proveedor='900123456' if ranking == 'count' else '800123456',
                     tipodocproveedor='NIT', supplier_name='PROVEEDOR CANTIDAD' if ranking == 'count' else 'PROVEEDOR VALOR',
                     contract_count='3' if ranking == 'count' else '1',
                     total_contract_value='Infinity' if bad else self.valid_amount)]


@pytest.mark.parametrize('focus', ['overview', 'suppliers'])
@pytest.mark.parametrize('bad_ranking', [None, 'count', 'value', 'both'])
def test_ranking_claim_and_published_warning_are_consistent(frame_v12, monkeypatch, focus, bad_ranking):
    chat = Conversation('audit-independent-top5')
    assert chat.say('analiza contratos de alcaldia de baranoa en agosto').result['continuation_published']
    original = frame_response.build_profile_sections
    profiles = []

    def observe(*args, **kwargs):
        profile = original(*args, **kwargs)
        profiles.append(profile)
        return profile

    with monkeypatch.context() as patch:
        patch.setattr(frame_response, 'build_profile_sections', observe)
        result = chat.say('proveedores' if focus == 'suppliers' else 'analiza contratos de alcaldia de baranoa en agosto',
                          client=IndependentRankings(bad_ranking))
    assert result.result['continuation_published'] and not result.clarifies
    assert len(result.calls) == (2 if focus == 'suppliers' else 4)
    assert profiles
    profile = profiles[-1]
    assert profile.suppliers.fact('top_by_contract_count')[0].actor_identity == 'NIT:900123456'
    assert profile.suppliers.fact('top_by_contract_value')[0].actor_identity == 'NIT:800123456'
    record = chat.execution_record()
    amounts = [m for call in record['calls'] if call['call_id'].startswith('supplier_') for m in call['metrics'] if m['name'] == 'total_contract_value']
    assert sum(m['status'] == 'VALID' for m in amounts) == (2 if bad_ranking is None else 0 if bad_ranking == 'both' else 1)
    assert all(m['value'] is None for m in amounts if m['status'] != 'VALID')
    next_turn = chat.say('recientes')
    assert next_turn.result['continuation_published'] and not next_turn.clarifies
    assert all('BARANOA' in sql and '2026-08' in sql for sql in next_turn.sql)
    emit('INDEPENDENT_TOP5', focus, bad_ranking, result.response, record)
    warning = 'ranking por valor no está acreditado' in result.response
    positive_claim = ('Principal contratista por valor:' in result.response or
                      'Principales contratistas por valor contratado' in result.response)
    assert not (warning and positive_claim), 'One published response denies and affirms accreditation of the value ranking'
    if bad_ranking is None:
        assert profile.suppliers.status == SectionStatus.SUPPORTED and positive_claim and not warning
    else:
        assert profile.suppliers.status == SectionStatus.PARTIAL and warning
    assert 'Principal contratista por número de contratos:' in result.response if focus == 'overview' else 'Principales contratistas por número de contratos' in result.response


@pytest.mark.parametrize('raw', ['0', '-0', '1234.56', '-1234.56', '1e100', '1e308'])
@pytest.mark.parametrize('focus', ['overview', 'suppliers'])
def test_supported_finite_amounts_keep_response_record_and_universe(frame_v12, raw, focus):
    chat = Conversation('audit-normal-amounts')
    chat.say('analiza contratos de alcaldia de baranoa en agosto')
    result = chat.say('proveedores' if focus == 'suppliers' else 'analiza contratos de alcaldia de baranoa en agosto',
                      client=IndependentRankings(valid_amount=raw))
    assert result.result['continuation_published'] and not result.clarifies
    assert len(result.calls) == (2 if focus == 'suppliers' else 4)
    assert 'ranking por valor no' not in result.response
    record = chat.execution_record()
    amounts = [m for call in record['calls'] if call['call_id'].startswith('supplier_') for m in call['metrics'] if m['name'] == 'total_contract_value']
    assert len(amounts) == 2 and all(m['status'] == 'VALID' and Decimal(m['value']) == Decimal(raw) for m in amounts)
    assert not chat.say('recientes').clarifies

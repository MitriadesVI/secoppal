"""B2-I-SUP: one accreditation decision per TOP governs every consumer."""
import pytest

from app.core.entity_profile import frame_response
from app.core.entity_profile.models import SectionStatus
from app.core.entity_profile.supplier_rankings import accredit_ranking, section_ranking
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient

WARN_VALUE = 'El ranking por valor no está acreditado.'
WARN_COUNT = 'El ranking por número de contratos no está acreditado.'
CLAIM_VALUE = 'Principal contratista por valor:'
CLAIM_COUNT = 'Principal contratista por número de contratos:'
LIST_VALUE = 'Principales contratistas por valor contratado'
LIST_COUNT = 'Principales contratistas por número de contratos'
DENIED_VALUE = 'ranking por valor no acreditado'
DENIED_COUNT = 'ranking por número de contratos no acreditado'


def row(document, name, count, amount):
    return dict(documento_proveedor=document, tipodocproveedor='NIT', supplier_name=name,
                contract_count=count, total_contract_value=amount)


class Matrix(WorkflowClient):
    """Each TOP answers on its own: disjoint or overlapping, damaged or not."""

    def __init__(self, count_state='valid', value_state='valid', overlap=False):
        super().__init__()
        self.count_state, self.value_state, self.overlap = count_state, value_state, overlap

    def _rows(self, ranking):
        state = self.count_state if ranking == 'count' else self.value_state
        if state == 'query_failure':
            raise TimeoutError(f'audit: {ranking} query failed')
        if state == 'empty':
            return []
        shared = '900111111'
        document = shared if self.overlap else ('900123456' if ranking == 'count' else '800123456')
        name = 'PROVEEDOR COMPARTIDO' if self.overlap else (
            'PROVEEDOR CANTIDAD' if ranking == 'count' else 'PROVEEDOR VALOR')
        count = 'no-es-entero' if state == 'bad_counts' else ('3' if ranking == 'count' else '1')
        amount = 'Infinity' if state == 'bad_amounts' else (
            '700000000' if ranking == 'count' else '900000000')
        rows = [row(document, name, count, amount)]
        if self.overlap:
            rows.append(row('800222222', 'PROVEEDOR PROPIO', '2', '100000000'))
        return rows

    def aggregate(self, dataset, sql):
        rows = super().aggregate(dataset, sql)
        if 'GROUP BY documento_proveedor' not in sql:
            return rows
        return self._rows('count' if 'ORDER BY contract_count DESC' in sql else 'value')


def profile_for(chat, focus, client, monkeypatch):
    original = frame_response.build_profile_sections
    seen = []

    def observe(*args, **kwargs):
        profile = original(*args, **kwargs)
        seen.append(profile)
        return profile

    with monkeypatch.context() as patch:
        patch.setattr(frame_response, 'build_profile_sections', observe)
        result = chat.say('proveedores' if focus == 'suppliers'
                          else 'analiza contratos de alcaldia de baranoa en agosto', client=client)
    assert seen, 'the Frame assembler must have run'
    return result, seen[-1]


STATES = ['valid', 'bad_amounts', 'bad_counts', 'query_failure', 'empty']


@pytest.mark.parametrize('overlap', [False, True])
@pytest.mark.parametrize('focus', ['overview', 'suppliers'])
@pytest.mark.parametrize('value_state', STATES)
@pytest.mark.parametrize('count_state', STATES)
def test_each_ranking_is_accredited_by_its_own_query(
        frame_v12, monkeypatch, count_state, value_state, focus, overlap):
    chat = Conversation('audit-independent-matrix')
    assert chat.say('analiza contratos de alcaldia de baranoa en agosto').result['continuation_published']
    client = Matrix(count_state, value_state, overlap)
    result, profile = profile_for(chat, focus, client, monkeypatch)
    assert result.result['continuation_published'] and not result.clarifies

    count = section_ranking(profile.suppliers, 'count')
    value = section_ranking(profile.suppliers, 'value')

    # The decision follows its own query, never the other one.
    assert count.available is (count_state != 'query_failure')
    assert value.available is (value_state != 'query_failure')
    # Each ranking is accredited by the metric it actually publishes: the count
    # TOP prints counts, the value TOP prints amounts. Damage to the metric the
    # other TOP needs never invalidates this one.
    assert count.accredited is (count_state in ('valid', 'bad_amounts', 'empty'))
    assert value.accredited is (value_state in ('valid', 'bad_counts', 'empty'))
    assert value.amounts_valid is (value_state in ('valid', 'bad_counts', 'empty'))
    assert count.counts_valid is (count_state in ('valid', 'bad_amounts', 'empty'))

    response = result.response
    claim_value = CLAIM_VALUE in response or LIST_VALUE in response
    claim_count = CLAIM_COUNT in response or LIST_COUNT in response

    # Never affirm and deny the same ranking in one published response.
    assert not (DENIED_VALUE in response and claim_value)
    assert not (DENIED_COUNT in response and claim_count)
    assert not (WARN_VALUE in response and claim_value)
    assert not (WARN_COUNT in response and claim_count)

    # A positive claim requires that ranking's own accreditation and rows.
    if claim_value:
        assert value.accredited and value_state in ('valid', 'bad_counts')
    if claim_count:
        assert count.accredited and count_state in ('valid', 'bad_amounts')

    # Damage is reported against the query that actually carries it.
    assert ('montos de proveedores no acreditables en el TOP por cantidad' in response) \
        is (count_state == 'bad_amounts')
    assert ('montos de proveedores no acreditables en el TOP por valor' in response) \
        is (value_state == 'bad_amounts')
    assert ('conteos de proveedores no acreditables en el TOP por cantidad' in response) \
        is (count_state == 'bad_counts')
    assert ('conteos de proveedores no acreditables en el TOP por valor' in response) \
        is (value_state == 'bad_counts')

    # A damaged count TOP does not invalidate an independent, valid value TOP.
    if count_state in ('bad_amounts', 'bad_counts') and value_state == 'valid':
        assert value.accredited and claim_value
    if value_state in ('bad_amounts', 'bad_counts') and count_state == 'valid':
        assert count.accredited and claim_count

    # Section state, and the turn after a reload, survive intact.
    healthy = count_state in ('valid', 'empty') and value_state in ('valid', 'empty')
    dead = count_state == 'query_failure' and value_state == 'query_failure'
    assert profile.suppliers.status == (
        SectionStatus.UNAVAILABLE if dead else
        SectionStatus.SUPPORTED if healthy else SectionStatus.PARTIAL)
    # What each list prints must match the metric that accredits it.
    for line in response.splitlines():
        if line.startswith('• ') and ' contratos' in line and 'COP' not in line:
            assert 'no-es-entero' not in line
    after = chat.say('recientes')
    assert after.result['continuation_published'] and not after.clarifies
    assert all('BARANOA' in sql and '2026-08' in sql for sql in after.sql)


@pytest.mark.parametrize('focus', ['overview', 'suppliers'])
def test_no_amount_is_borrowed_across_the_two_queries(frame_v12, monkeypatch, focus):
    """Overlapping identities: a bad amount in one TOP is never filled from the other."""
    chat = Conversation('audit-independent-crossfill')
    chat.say('analiza contratos de alcaldia de baranoa en agosto')
    client = Matrix('bad_amounts', 'valid', overlap=True)
    result, profile = profile_for(chat, focus, client, monkeypatch)
    by_count = profile.suppliers.fact('top_by_contract_count')
    by_value = profile.suppliers.fact('top_by_contract_value')
    shared = [s for s in by_count if s.document == '900111111']
    assert shared and all(s.total_contract_value is None for s in shared), \
        'the damaged amount was filled from the other query'
    assert all(s.total_contract_value is not None for s in by_value)
    assert {s.actor_identity for s in by_count} == {s.actor_identity for s in by_value}
    assert section_ranking(profile.suppliers, 'value').accredited
    assert not section_ranking(profile.suppliers, 'count').amounts_valid
    assert len(result.calls) == (2 if focus == 'suppliers' else 4)


def test_the_frame_route_always_supplies_the_explicit_decision(frame_v12, monkeypatch):
    """The legacy recompute path must never be reached on the new route."""
    chat = Conversation('audit-independent-authority')
    chat.say('analiza contratos de alcaldia de baranoa en agosto')
    calls = []
    original = accredit_ranking
    import app.core.entity_profile.supplier_rankings as module

    def counted(*args, **kwargs):
        calls.append(kwargs.get('available'))
        return original(*args, **kwargs)

    monkeypatch.setattr(module, 'accredit_ranking', counted)
    _, profile = profile_for(chat, 'suppliers', Matrix('bad_amounts', 'valid'), monkeypatch)
    for ranking in ('count', 'value'):
        fact = profile.suppliers.fact(f'{ranking}_ranking_accreditation')
        assert fact is not None and type(fact).__name__ == 'RankingAccreditation'
        assert section_ranking(profile.suppliers, ranking) is fact, \
            'the renderer must consume the assembler decision, not recompute it'


def test_an_empty_success_is_not_an_invented_ranking(frame_v12, monkeypatch):
    chat = Conversation('audit-independent-empty')
    chat.say('analiza contratos de alcaldia de baranoa en agosto')
    result, profile = profile_for(chat, 'suppliers', Matrix('empty', 'empty'), monkeypatch)
    for ranking in ('count', 'value'):
        decision = section_ranking(profile.suppliers, ranking)
        assert decision.available and decision.empty and decision.accredited
    assert CLAIM_VALUE not in result.response and CLAIM_COUNT not in result.response
    assert profile.suppliers.status == SectionStatus.SUPPORTED
    assert 'no se obtuvo' not in result.response.lower()

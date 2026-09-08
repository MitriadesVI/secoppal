"""Amounts never erase supplier identity/count, source facts or publication."""
from decimal import Decimal
import math

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_profile.aspects.suppliers import parse_suppliers
from app.core.entity_profile.frame_compiler import compile_profile
from app.core.entity_profile.frame_response import build_profile_sections
from app.core.entity_profile.frame_runner import execute_plan
from app.core.entity_profile.models import SectionStatus
from app.core.entity_profile.supplier_numbers import supplier_amount
from app.core.soql_builder import SoQLBuilder
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


BAD_VALUES = [('Infinity', 'INVALID'), ('-Infinity', 'INVALID'), ('NaN', 'INVALID'),
              ('1e999', 'REPRESENTATION_OVERFLOW'), (None, 'UNAVAILABLE'),
              ('', 'UNAVAILABLE'), ('broken', 'INVALID'), ('1e-999', 'REPRESENTATION_UNDERFLOW')]


class MixedSuppliers(WorkflowClient):
    def __init__(self, raw):
        super().__init__()
        self.raw = raw

    def aggregate(self, dataset, sql):
        rows = super().aggregate(dataset, sql)
        if 'GROUP BY documento_proveedor' in sql:
            return [
                {'documento_proveedor': '900123456', 'tipodocproveedor': 'NIT',
                 'supplier_name': 'PROVEEDOR A', 'contract_count': '3', 'total_contract_value': self.raw},
                {'documento_proveedor': '800123456', 'tipodocproveedor': 'NIT',
                 'supplier_name': 'PROVEEDOR B', 'contract_count': '1', 'total_contract_value': '700000000'},
            ]
        return rows


@pytest.mark.parametrize('raw,status', BAD_VALUES)
@pytest.mark.parametrize('focus', ['overview', 'suppliers', 'values'])
def test_mixed_supplier_amounts_survive_reload_and_followup(frame_v12, raw, status, focus):
    chat = Conversation('b2-supplier')
    first = chat.say('analiza contratos de alcaldia de baranoa en agosto')
    assert first.result['continuation_published']
    query = {'overview': 'analiza contratos de alcaldia de baranoa en agosto',
             'suppliers': 'proveedores', 'values': 'montos'}[focus]
    error, result = None, None
    try:
        result = chat.say(query, client=MixedSuppliers(raw))
    except (OverflowError, ValueError) as exc:
        error = str(exc)
    assert error is None, f'supplier data destroyed response and facts: {error}'
    assert result and not result.clarifies and result.result['continuation_published']
    assert len(result.calls) == {'overview': 4, 'suppliers': 2, 'values': 1}[focus]
    builder = SoQLBuilder()
    frame = chat.profile_state().confirmed_frame
    plan = compile_profile(frame, soql_builder=builder).plan
    profile = build_profile_sections(frame, plan, execute_plan(plan, MixedSuppliers(raw)), builder)
    if focus != 'values':
        assert profile.suppliers.status is SectionStatus.PARTIAL
        for key in ('top_by_contract_count', 'top_by_contract_value'):
            a, b = profile.suppliers.fact(key)
            assert a.contract_count == 3 and b.contract_count == 1
            assert a.actor_identity == 'NIT:900123456' and b.actor_identity == 'NIT:800123456'
            assert a.total_contract_value is None and a.total_contract_value_status == status
            assert b.total_contract_value == 700000000 and b.total_contract_value_status == 'VALID'
        assert 'ranking por valor no' in result.response.lower()
        assert 'Principal contratista por valor:' not in result.response
        assert 'PROVEEDOR A' in result.response and '3 contratos' in result.response
        if focus == 'suppliers':
            assert 'PROVEEDOR B' in result.response and 'monto no acreditable' in result.response
        amounts = [m for c in chat.execution_record()['calls'] if c['call_id'].startswith('supplier_')
                   for m in c['metrics'] if m['name'] == 'total_contract_value']
        assert sum(m['status'] == 'VALID' for m in amounts) == 2
        assert all(m['value'] is None for m in amounts if m['status'] != 'VALID')
    if focus == 'overview':
        assert profile.activity.fact('contract_count') == 4 and '4 contratos' in result.response
    if focus == 'values':
        control = Conversation('b2-values-control')
        control.say('analiza contratos de alcaldia de baranoa en agosto')
        assert result.response == control.say('montos').response
    following = chat.say('recientes')
    assert following.calls and following.result['continuation_published'] and not following.clarifies
    assert all('BARANOA' in sql and '2026-08' in sql for sql in following.sql)
    print('SUPPLIER_RESULT', focus, repr(raw), result.response, chat.execution_record())


@settings(max_examples=60, derandomize=True, deadline=None)
@given(st.integers(min_value=-10**15, max_value=10**15))
def test_valid_supplier_amount_parity(value):
    amount = supplier_amount(str(value))
    assert amount.status == 'VALID' and amount.value == float(value)
    rows = [{'documento_proveedor': '900123456', 'tipodocproveedor': 'NIT',
             'contract_count': '3', 'total_contract_value': str(value)}]
    assert parse_suppliers(rows)[0].total_contract_value == float(value)


@settings(max_examples=30, derandomize=True, deadline=None)
@given(st.integers(min_value=309, max_value=2000), st.sampled_from(['', '-']))
def test_finite_decimal_overflow_is_not_a_valid_float(exponent, sign):
    raw = f'{sign}1e{exponent}'
    assert Decimal(raw).is_finite() and not math.isfinite(float(Decimal(raw)))
    amount = supplier_amount(raw)
    assert amount.status == 'REPRESENTATION_OVERFLOW' and amount.value is None

"""Bounded supplier defect reproduction, runnable on pre-B.1 exports too."""
import traceback

import pytest

from app.config import get_settings
from test_entity_profile_workflow import WorkflowClient, _workflow


@pytest.mark.parametrize('focus', ['overview', 'suppliers', 'values'])
@pytest.mark.parametrize('value', ['Infinity', '1e999'])
def test_supplier_invalid_value_keeps_a_product_response(monkeypatch, focus, value):
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'true')
    get_settings.cache_clear()
    chat = 'b1-independent-supplier'
    first, _ = _workflow()
    first.run_query('analiza contratos de alcaldia de baranoa en agosto', chat_id=chat)

    class SupplierValue(WorkflowClient):
        def aggregate(self, dataset, sql):
            rows = super().aggregate(dataset, sql)
            if 'GROUP BY documento_proveedor' in sql:
                return [{**row, 'total_contract_value': value} for row in rows]
            return rows

    flow, client = _workflow(SupplierValue())
    query = {'overview': 'analiza contratos de alcaldia de baranoa en agosto',
             'suppliers': 'proveedores', 'values': 'montos'}[focus]
    error, frames, result = None, [], None
    try:
        result = flow.run_query(query, chat_id=chat)
    except OverflowError as exc:
        error = str(exc)
        frames = [(frame.name, frame.filename, frame.lineno) for frame in traceback.extract_tb(exc.__traceback__)]
    finally:
        get_settings.cache_clear()
    print('SUPPLIER_OVERFLOW', focus, value, error, 'FRAMES', frames, 'CALLS', client.calls,
          'RESPONSE_RETURNED', result is not None)
    assert error is None, 'Supplier input aborted the complete product response: ' + str(error)
    assert result['analytical_intent'] == 'entity_profile' and result['response']
    assert 'Infinity' not in result['response']

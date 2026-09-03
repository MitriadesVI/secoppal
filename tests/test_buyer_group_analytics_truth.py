"""Aggregate rows are physical counts, and the trace is the executed aggregate."""
from unittest.mock import MagicMock

from burr.core import State

from app.core.analytics.execution import ExecutionAnalyzer
from app.core.analytics.ranking import RankingAnalyzer
from app.core.buyer_groups import get_groups
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.execution import execute_query


def params():
    return {'buyer_group':get_groups()['bogota_executive'].reference(),'dataset':'contratos'}


def test_ranking_counts_are_physical_and_explicitly_limited_to_returned_rows():
    response=RankingAnalyzer().format([{'nombre_entidad':'A','cnt':'3'}],params(),'jbjy-vk9h')
    assert '3 filas físicas' in response
    assert 'ranking mostrado' in response
    assert '3 contratos' not in response


def test_execution_counts_are_physical_rows():
    response=ExecutionAnalyzer().format([{'contratado':100,'pagado':25,'total_count':'3'}],params(),'jbjy-vk9h')
    assert '3 filas físicas' in response and '3 contratos' not in response


def test_analytic_trace_is_exactly_the_aggregate_sent_to_secop():
    client=MagicMock()
    client.with_timeout.return_value=client
    client.aggregate.return_value=[{'nombre_entidad':'A','cnt':'3'}]
    state=State({'resolved_params':params(),'dataset_id':'jbjy-vk9h','soql_query':'UNEXECUTED SEARCH',
                 'user_query':'ranking de entidades de la alcaldia de bogota','timings_ms':{}})
    result=execute_query(state,client,SoQLBuilder())
    assert result['soql_query']==client.aggregate.call_args.args[1]
    assert 'GROUP BY nombre_entidad' in result['soql_query']
    client.query.assert_not_called()

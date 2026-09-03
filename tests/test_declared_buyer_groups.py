"""Declared groups preserve membership, explicit units and physical truth."""
from copy import deepcopy
import json
from pathlib import Path

import pytest
from burr.core import State

from app.config import Settings
from app.core.buyer_groups import BuyerGroupError, get_groups, group_for_params, parse_groups
from app.core.entity_resolver import EntityResolver
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.core.query_plan import compute_query_plan
from app.core.query_frame import frame_from_params, params_from_frame
from app.core.followup_engine import FollowupClassifier
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.planning import resolve_entities
from app.core.conversation_store import ConversationStore


@pytest.mark.parametrize("phrase,key,n", [
    ("alcaldia de bogota","bogota_executive",38),
    ("administracion central de bogota","bogota_central",18),
    ("alcaldias locales de bogota","bogota_local_mayoralties",20),
    ("icbf","icbf_national",33),
])
@pytest.mark.parametrize("dataset", ["procesos","contratos"])
def test_parse_plan_and_exact_projection(phrase,key,n,dataset):
    query=f"{dataset} de la {phrase} en 2026"
    params=QueryRouter().parse(query).params
    plan=compute_query_plan(params,query)
    assert plan.should_execute
    group=group_for_params(plan.params)
    assert group.group_id==key and len(group.members)==n
    state=resolve_entities(State({"parsed_params":plan.params,"user_query":query,"query_plan":{"should_execute":True}}),EntityResolver(Path('app/data/aliases_db.json')))
    assert not state.get("needs_clarification")
    result=state["resolved_params"]
    assert not result.get("entidad_in_candidates")
    ds=SoQLBuilder.dataset_id_for(dataset)
    sql=SoQLBuilder().build(ds,result)
    names=group.names_for(ds)
    assert len(names)==n
    for name in names: assert "= '"+name.replace("'","''")+"'" in sql
    assert ":id DESC" in sql and "LIKE" not in sql


@pytest.mark.parametrize("query", [
    "contratos de la alcaldia de bogota secretaria distrital de integracion social en 2026",
    "contratos de la alcaldia de bogota alcaldia local de chapinero en 2026",
    "contratos del icbf regional amazonas en 2026",
    "contratos de icbf sede nacional en 2026",
    "contratos de la secretaria general de la alcaldia mayor de bogota en 2026",
    "contratos de la alcaldia de bogota integracion social en adulto mayor en 2026",
    "contratos en bogota en 2026",
])
def test_explicit_unit_or_geography_does_not_become_a_group(query):
    params=QueryRouter().parse(query).params
    assert not params.get("buyer_group")
    if 'integracion social' in query:
        assert params['entidad_resolved']=='(Secretaría Distrital de Integración Social)'


def test_stale_or_forged_membership_fails_closed():
    for ref in ({"id":"bogota_executive","version":"old"},
                {"id":"bogota_executive","version":"2026-09-03.v1","members":["ONE"]}):
        with pytest.raises(BuyerGroupError):
            SoQLBuilder().build('p6dx-8zbt',{'buyer_group':ref})


def test_registry_rejects_unaccredited_and_duplicate_members():
    payload=json.loads(Path('app/data/buyer_groups.json').read_text())
    known={m['canonical_name'] for g in payload['groups'] for m in g['members']}
    broken=deepcopy(payload)
    broken['groups'][0]['members'].append(broken['groups'][0]['members'][0])
    with pytest.raises(BuyerGroupError): parse_groups(broken,known)
    with pytest.raises(BuyerGroupError): parse_groups(payload,set())


def test_group_is_conversational_scope_not_raw_residue():
    params={'buyer_group':get_groups()['bogota_executive'].reference(),'dataset':'contratos'}
    frame=frame_from_params(params)
    assert 'buyer_group' in frame.scope and 'buyer_group' not in frame.raw_params
    assert params_from_frame(frame)['buyer_group']==params['buyer_group']
    assert FollowupClassifier._scope_differs({'buyer_group':get_groups()['bogota_central'].reference()},frame.scope)


def test_workflow_declares_members_and_keeps_shared_nit_contracts_separate():
    workflow=SecopalWorkflow(Settings(deepseek_api_key=None))
    calls=[]
    rows=[{'id_contrato':cid,'nombre_entidad':name,'nit_entidad':'899999061',
           'objeto_del_contrato':'Servicio','valor_del_contrato':'1000','fecha_de_firma':'2026-01-01'}
          for cid,name in [('C1','SECRETARIA DISTRITAL DE HACIENDA'),('C2','SECRETARIA DE EDUCACION DEL DISTRITO')]]
    def query(dataset,soql,**kwargs):
        calls.append(soql)
        return deepcopy(rows)
    workflow.secop_client.query=query
    workflow.secop_client.count=lambda *a,**k: 2
    workflow.secop_client.aggregate=lambda *a,**k: []
    workflow.narrator.narrate_with_grounding=lambda **k: None
    result=workflow.run_query('contratos de la alcaldia de bogota en 2026')
    assert result['soql_query'] in calls and not result['needs_clarification']
    assert len(result['rows'])==2
    assert len(result['buyer_group']['members'])==38
    assert 'Alcance declarado' in result['response'] and 'filas físicas' in result['response']
    assert result['total_count_metric']=='physical_rows'


def test_pagination_year_and_scope_switch_preserve_only_applicable_membership(tmp_path, monkeypatch):
    import re
    monkeypatch.setattr('app.core.workflow.actions.execution.observe_universe_fn',lambda *a,**k:None)
    workflow=SecopalWorkflow(Settings(deepseek_api_key=None))
    workflow.conv_store=ConversationStore(base_path=tmp_path/'conv')
    rows=[{'id_contrato':f'C{i:03}', 'nombre_entidad':'SECRETARIA DISTRITAL DE HACIENDA',
           'objeto_del_contrato':'Servicio', 'valor_del_contrato':str(1000+i),
           'fecha_de_firma':'2026-01-01'} for i in range(80)]
    calls=[]
    def query(ds,sql,**kwargs):
        calls.append(sql)
        off=re.search(r'OFFSET (\d+)',sql); limit=re.search(r'LIMIT (\d+)',sql)
        start=int(off[1]) if off else 0
        return deepcopy(rows[start:start+int(limit[1])])
    workflow.secop_client.query=query
    workflow.secop_client.count=lambda *a,**k:len(rows)
    workflow.secop_client.aggregate=lambda *a,**k:[]
    workflow.narrator.narrate_with_grounding=lambda **k:None
    first=workflow.run_query('contratos de la alcaldia de bogota en 2026',chat_id='declared_group')
    second=workflow.run_query('muestrame mas',chat_id='declared_group')
    assert first['buyer_group']==second['buyer_group']
    assert not ({r['id_contrato'] for r in first['rows']} & {r['id_contrato'] for r in second['rows']})
    year=workflow.run_query('y en 2025',chat_id='declared_group')
    assert year['buyer_group']==first['buyer_group']
    other=workflow.run_query('ahora en antioquia',chat_id='declared_group')
    assert not other.get('buyer_group') and not other['resolved_params'].get('buyer_group')
    assert 'SECRETARIA DISTRITAL DE HACIENDA' not in other['soql_query']


def test_timeout_and_empty_group_do_not_relax_filters(monkeypatch):
    from unittest.mock import MagicMock
    monkeypatch.setattr('app.core.workflow.actions.execution.observe_universe_fn',lambda *a,**k:None)
    for timeout in (False,True):
        workflow=SecopalWorkflow(Settings(deepseek_api_key=None))
        query=MagicMock(side_effect=TimeoutError('source timeout')) if timeout else MagicMock(return_value=[])
        workflow.secop_client.query=query
        workflow.secop_client.count=lambda *a,**k:0
        workflow.secop_client.aggregate=lambda *a,**k:[]
        result=workflow.run_query('contratos de la alcaldia de bogota en 2026')
        assert query.call_count==1
        assert not result.get('fallback_used')
        assert "2026-01-01" in result['soql_query']

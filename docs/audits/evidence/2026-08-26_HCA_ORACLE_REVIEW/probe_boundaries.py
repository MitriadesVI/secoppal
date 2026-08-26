"""Read-only parser probes, not tests and not an alternative execution runner."""
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
from scripts.audits.audit_isolation import isolated_runtime_stores

with isolated_runtime_stores():
    from app.core.query_router import QueryRouter
    from app.utils.money import money_to_cop
    from app.core.estado_families import detect_estado_family

    router=QueryRouter()
    queries=[
        'contratos de papeleria por mas de 100 mil pesos',
        'contratos de papeleria por mas de 1 millon',
        'contratos de papeleria por mas de 1 millones',
        'contratos de papeleria por mas de 100000 pesos',
        'contratos de papeleria por mas de 1000000 pesos',
        'contratos en ejecucion de la alcaldia de medellin',
        'contratos activos de la alcaldia de medellin',
    ]
    result={
        'money_primitive': {s:money_to_cop(s) for s in ('100 mil pesos','1 millon','1 millones')},
        'unit_pattern_accepts_millon': bool(re.fullmatch(r'millones?', 'millon')),
        'state_family_direct': detect_estado_family('en ejecucion', 'contratos'),
        'parser_probes': [{'query':q, 'params':router.parse(q).params} for q in queries],
    }
    print(json.dumps(result,ensure_ascii=False,indent=2,default=str))

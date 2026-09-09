"""Separate derived universe metadata from accepted turn intent in traces."""
from app.core.contract_universe_intent import request


def derived_universe_fields(params: dict, text: str) -> set[str]:
    derived = {'contract_universe_version'} & params.keys()
    if 'contract_universe' in params:
        decision = request(text)
        if decision is None or decision.universe.value != params['contract_universe']:
            derived.add('contract_universe')
    return derived

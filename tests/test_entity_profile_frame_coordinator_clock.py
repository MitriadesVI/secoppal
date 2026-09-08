"""The helper's civil reference is injected, including after a later reload."""
from datetime import date
import json

import pytest

from app.core.entity_profile.frame import EntityProfileFrame
from app.core.entity_profile.frame_compiler import compile_profile
from app.core.entity_profile.frame_values import SourceKind
from app.core.soql_builder import SoQLBuilder
from entity_profile_frame_harness import BUYER, observe


@pytest.mark.parametrize('confirmed_on,reloaded_on', [
    (date(2026, 9, 7), date(2027, 2, 1)),
    (date(2027, 10, 3), date(2028, 4, 12)),
])
def test_injected_clock_survives_later_helper_turn_and_reload(confirmed_on, reloaded_on):
    first = observe(f'analiza contratos firmados de {BUYER} en agosto', today=confirmed_on)
    assert first.ready
    period = first.frame.source_predicates[SourceKind.CONTRACTS].temporal.period
    assert period.civil_reference == confirmed_on.isoformat()
    revived = EntityProfileFrame.from_dict(json.loads(json.dumps(first.frame.to_dict())))
    later = observe('proveedores', base=revived, today=reloaded_on, turn_id='later')
    assert later.ready
    assert later.frame.source_predicates[SourceKind.CONTRACTS].temporal.period == period
    recompiled = compile_profile(revived, soql_builder=SoQLBuilder())
    assert recompiled.plan.source_plans[SourceKind.CONTRACTS].fingerprint == first.decision.plan.source_plans[SourceKind.CONTRACTS].fingerprint

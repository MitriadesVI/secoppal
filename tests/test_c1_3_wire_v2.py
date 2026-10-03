"""Synthetic C1.3 wire/profile authority tests: no Gold, provider or source traffic."""
from __future__ import annotations

import ast
from copy import deepcopy
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from app.core.foundation_decoder_v2 import decode_proposal_v2
from app.core.foundation_operations import Aspect, Operation, Subject
from app.core.foundation_producers import PROFILES, LinguisticProducer, decode_proposal, host_candidates
from app.core.interpretation_contract import AcceptanceStatus, ContractError, InterpretationProposal
from app.core.semantic_dimensions import SemanticDimension
from app.core.foundation_operator_table import OPERATORS
from tests.test_foundation_prompts import RAW, documented


def wire(arm='B', subject='contracts', operation='SEARCH'):
    candidates = host_candidates(RAW)
    evidence = {'candidate_ids': [next(c['candidate_id'] for c in candidates if c['quote'] == RAW)]} if arm == 'B' else {
        'evidence': [{'start': 0, 'end': len(RAW), 'quote': RAW}]}
    universe = next(c for c in candidates if c['quote'] == 'contratos')
    request = dict(request_id='r1', scope_id='r1', subject=subject, **evidence)
    if subject != 'unspecified':
        request['universe_kind'] = 'records'
        request.update({'universe_evidence_candidate_ids': [universe['candidate_id']]} if arm == 'B' else {
            'universe_evidence': {k: universe[k] for k in ('start', 'end', 'quote')}})
    job = dict(job_id='j1', operation=operation, request_ids=['r1'])
    if operation != 'SEARCH':
        job.update(evidence)
    if operation in ('COUNT', 'SUM_VALUE'):
        job['aspect'] = 'TOTAL'
    if operation == 'GROUP_BY':
        job['aspect'] = 'BY_YEAR'
    return dict(jobs=[job], requests=[request], conditions=[], relations=[], omissions=[])


def decode(payload, arm='B'):
    return decode_proposal_v2(payload, RAW, arm=arm, candidates=host_candidates(RAW))


def invalid(payload, reason=None):
    calls = []
    producer = LinguisticProducer(lambda system, request: calls.append(request) or payload, profile='foundation_v5')
    result = producer.produce(RAW)
    assert result.status is AcceptanceStatus.INVALID_PROPOSAL and result.proposal is None
    assert len(calls) == producer.receipt.provider_calls == 1
    if reason:
        assert result.reason == reason
    return result


@pytest.mark.parametrize('arm', ['B', 'C'])
def test_documented_v2_shape_decodes_without_lifting(arm):
    proposal = decode(wire(arm), arm)
    assert isinstance(proposal, InterpretationProposal)
    assert proposal.producer_version == '5'
    assert proposal.jobs[0].operation is Operation.SEARCH
    assert proposal.requests[0].subject is Subject.CONTRACTS


@pytest.mark.parametrize('profile,payload', [
    ('foundation_v3', wire()), ('foundation_v5', documented('B')),
    ('foundation_v3', {'jobs': [], 'requests': [], 'conditions': []}),
    ('foundation_v5', {'requests': [], 'conditions': []}),
])
def test_wires_crossed_or_missing_jobs_never_sniff_or_return_a_proposal(profile, payload):
    result = LinguisticProducer(lambda system, request: payload, profile=profile).produce(RAW)
    assert result.status is AcceptanceStatus.INVALID_PROPOSAL and result.proposal is None


def test_profiles_are_closed_and_v3_default_and_receipt_remain_frozen():
    assert set(PROFILES) == {'foundation_v3', 'foundation_v5'}
    with pytest.raises(TypeError):
        PROFILES['foreign'] = PROFILES['foundation_v3']
    with pytest.raises(ContractError, match='INVALID_PRODUCER_PROFILE'):
        LinguisticProducer(lambda system, request: {}, profile='foreign')
    default = LinguisticProducer(lambda system, request: documented('B'))
    explicit = LinguisticProducer(lambda system, request: documented('B'), profile='foundation_v3')
    assert default.produce(RAW).to_dict() == explicit.produce(RAW).to_dict()
    assert default.receipt.producer_version == '4.2'
    assert default.receipt.prompt_sha256 == 'a5867d40cb0027c1ebd32714b61efc9d97bfc82730afaee1e9d58007214340f4'


def test_v5_profile_pairs_its_prompt_decoder_and_telemetry():
    seen = []
    producer = LinguisticProducer(lambda system, request: seen.append((system, request)) or wire(), profile='foundation_v5')
    result = producer.produce(RAW)
    assert isinstance(result, InterpretationProposal) and result.producer_version == '5'
    assert seen[0][0] == PROFILES['foundation_v5'].prompt()
    assert seen[0][1]['host_candidates'] == host_candidates(RAW)
    assert producer.receipt.prompt_sha256 == PROFILES['foundation_v5'].prompt_sha256
    assert producer.receipt.producer_version == '5'


def test_d1_empty_is_valid_but_requests_require_jobs_and_nonsearch_can_have_no_requests():
    assert decode({'jobs': [], 'requests': [], 'conditions': []}).jobs == ()
    payload = wire()
    payload['jobs'] = []
    invalid(payload, 'MISSING_JOBS')
    payload = wire(operation='COUNT')
    payload['jobs'][0]['request_ids'] = []
    payload['requests'] = []
    assert decode(payload).jobs[0].aspect is Aspect.TOTAL


@pytest.mark.parametrize('operation', [o.value for o in Operation])
def test_v2_declares_its_whole_operation_vocabulary(operation):
    assert decode(wire(operation=operation)).jobs[0].operation.value == operation


@pytest.mark.parametrize('operation', ['search', 'MULTI', 'UNKNOWN', None, 1])
def test_d3_operation_is_never_inferred(operation):
    payload = wire()
    payload['jobs'][0]['operation'] = operation
    invalid(payload, 'INVALID_OPERATION')


@pytest.mark.parametrize('subject', ['oportunidades', 'CONTRACTS', 'UNKNOWN', None, 1])
def test_d3_subject_is_never_inferred(subject):
    payload = wire()
    payload['requests'][0]['subject'] = subject
    invalid(payload, 'INVALID_SUBJECT')


def test_unspecified_has_no_universe_default_or_claim():
    result = decode(wire(subject='unspecified'))
    assert result.requests[0].universe_evidence is None
    assert result.requests[0].universe_claim is None
    payload = wire(subject='unspecified')
    payload['requests'][0]['universe_kind'] = 'records'
    invalid(payload, 'UNIVERSE_KIND_ON_UNSPECIFIED')
    payload = wire()
    del payload['requests'][0]['universe_kind']
    invalid(payload, 'MISSING_UNIVERSE_KIND')


def test_v2_does_not_default_a_geographic_level_or_count_aspect():
    payload = wire()
    payload['conditions'] = [dict(condition_id='geo', request_ids=['r1'], dimension='GEOGRAPHY',
                                  operator='is', polarity='POSITIVE', candidate_ids=payload['requests'][0]['candidate_ids'])]
    payload['requests'][0]['condition_expression'] = {'operator': 'ATOM', 'condition_id': 'geo'}
    invalid(payload, 'GEOGRAPHY_LEVEL_REQUIRED')
    payload['conditions'][0]['level'] = 'MUNICIPALITY'
    assert decode(payload).conditions[0].level.value == 'MUNICIPALITY'
    payload = wire(operation='COUNT')
    del payload['jobs'][0]['aspect']
    assert decode(payload).jobs[0].aspect is None
    payload['jobs'][0]['aspect'] = 'BY_YEAR'
    invalid(payload, 'COUNT_ASPECT_NOT_TOTAL')


def test_universe_kind_expansion_is_grounded_and_never_mutates_the_payload():
    payload = wire()
    payload['requests'][0]['universe_kind'] = 'opportunities'
    before = deepcopy(payload)
    result = decode(payload)
    assert result.conditions[0].dimension is SemanticDimension.RETRIEVAL_INTENT
    assert result.conditions[0].evidence == (result.requests[0].universe_evidence,)
    assert payload == before


def test_unsupported_is_explicit_and_an_invalid_time_pair_cannot_be_rescued():
    payload = wire()
    payload['conditions'] = [dict(condition_id='c', request_ids=['r1'], dimension='UNSUPPORTED',
                                  operator='unlisted', polarity='POSITIVE', unsupported_label='unsupported',
                                  candidate_ids=payload['requests'][0]['candidate_ids'])]
    payload['requests'][0]['condition_expression'] = {'operator': 'ATOM', 'condition_id': 'c'}
    assert decode(payload).conditions[0].unsupported_label == 'unsupported'
    payload['conditions'][0].update(dimension='TIME', operator='before')
    del payload['conditions'][0]['unsupported_label']
    invalid(payload, 'OPERATOR_OUTSIDE_DIMENSION')


@given(st.sampled_from(['jobs', 'requests']), st.text(min_size=1).filter(lambda x: x not in {'job_id', 'request_id', 'operation', 'subject', 'scope_id', 'request_ids', 'candidate_ids', 'universe_kind', 'universe_evidence_candidate_ids', 'condition_expression', 'aspect', 'aspect_label', 'referent_evidence_candidate_ids', 'source_evidence_candidate_ids'}))
@settings(max_examples=50, deadline=None)
def test_property_foreign_executable_fields_never_survive(section, key):
    payload = wire()
    payload[section][0][key] = {'params': 'untrusted'}
    invalid(payload, 'INVALID_LINGUISTIC_RECORD_KEYS')


@given(st.sampled_from(list(OPERATORS)), st.text(min_size=1))
@settings(max_examples=60, deadline=None)
def test_property_operator_outside_the_closed_dimension_never_yields_a_proposal(dimension, operator):
    if operator in OPERATORS[dimension]:
        return
    payload = wire()
    condition = dict(condition_id='c', request_ids=['r1'], dimension=dimension.value, operator=operator,
                     polarity='POSITIVE', candidate_ids=payload['requests'][0]['candidate_ids'])
    if dimension is SemanticDimension.GEOGRAPHY:
        condition['level'] = 'UNKNOWN'
    payload['conditions'] = [condition]
    payload['requests'][0]['condition_expression'] = {'operator': 'ATOM', 'condition_id': 'c'}
    result = invalid(payload)
    assert result.proposal is None


@pytest.mark.parametrize('section,key', [('requests', 'subject'), ('jobs', 'operation')])
def test_missing_v2_semantics_are_not_supplied_by_lift_or_prompt(section, key):
    payload = wire()
    del payload[section][0][key]
    invalid(payload, 'INVALID_LINGUISTIC_RECORD_KEYS')


def test_d1_empty_interpretation_preserves_the_host_no_action_outcome():
    from app.core.foundation_admission import admit_proposal
    result = admit_proposal(decode({'jobs': [], 'requests': [], 'conditions': []}), RAW, entity_resolver=None)
    assert result.status is AcceptanceStatus.NO_ACTION and result.reason == 'NO_REQUEST'


@pytest.mark.parametrize('value', ['foreign', [], [['foreign']], None])
def test_foreign_or_malformed_evidence_never_yields_a_proposal(value):
    payload = wire()
    payload['requests'][0]['candidate_ids'] = value
    invalid(payload)


def test_unspecified_cannot_carry_universe_evidence_and_referent_is_single():
    payload = wire(subject='unspecified')
    payload['requests'][0]['universe_evidence_candidate_ids'] = payload['requests'][0]['candidate_ids']
    invalid(payload, 'UNSPECIFIED_WITH_UNIVERSE_EVIDENCE')
    payload = wire()
    payload['jobs'][0]['referent_evidence_candidate_ids'] = payload['requests'][0]['candidate_ids'] * 2
    invalid(payload, 'SINGLE_EVIDENCE_REQUIRED')


@pytest.fixture(autouse=True)
def no_source_calls(monkeypatch):
    from app.core.secop_client import SecopClient
    calls = []
    def forbidden(*args, **kwargs):
        calls.append(args)
        raise AssertionError('wire/profile processing cannot call a source')
    for method in ('query', 'count', 'aggregate'):
        monkeypatch.setattr(SecopClient, method, forbidden)
    yield
    assert calls == []


def test_v2_does_not_import_the_v1_lift():
    path = Path(__file__).resolve().parents[1] / 'app/core/foundation_decoder_v2.py'
    imports = [n.module for n in ast.walk(ast.parse(path.read_bytes())) if isinstance(n, ast.ImportFrom)]
    assert 'app.core.foundation_lift_v1' not in imports

"""Product invariants separate from diagnostic route names for M2/M3."""
import pytest

from app.core.conversation_store import ConversationStore
from entity_profile_frame_b_harness import BUYER, Conversation, frame_v12  # noqa: F401


@pytest.mark.parametrize('case', ['corrupt_queue', 'profile_seed'])
def test_continuation_rejection_prevents_product_effects(frame_v12, case):
    chat = Conversation('b1-independent-oracle')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    before = chat.profile_state().to_dict()
    if case == 'corrupt_queue':
        path = ConversationStore()._envelope_path_for(chat.chat_id)
        with path.open('a') as output:
            output.write('{not json')
        result = chat.say('mas')
    else:
        result = chat.say('Estado: activo', seed_resolved_params={
            'entidad_resolved': 'ALCALDIA MUNICIPAL DE BARANOA', 'estado': 'activo'})
    assert result.clarifies and not result.calls and not result.result['rows']
    if case == 'profile_seed':
        assert chat.profile_state().to_dict() == before
    print('PRODUCT_ORACLE', case, result.route, result.response, len(result.calls))

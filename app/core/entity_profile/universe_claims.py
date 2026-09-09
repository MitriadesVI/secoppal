"""One bounded universe occurrence, carried through the existing claim/patch path."""
from app.core.contract_universe_intent import request
from app.core.entity_profile.claims import ClaimDimension, SemanticClaim, UnresolvedIssue
from app.core.entity_profile.frame_values import SourceKind


def add_universe_claims(text, normalized, sources, claims, issues, turn_id, next_id):
    intent = request(text)
    if intent is None:
        return
    if intent.conflict:
        issues.append(UnresolvedIssue('CONTRACT_UNIVERSE_CONFLICT',
                                      'Aclara si deseas contratos celebrados o registros preparatorios/cancelados.'))
    span = intent.span if intent.span != (0, 0) else (0, len(text))
    if not sources:
        sources[0] = SemanticClaim(
            id=next_id('source'), turn_id=turn_id, span=span,
            dimension=ClaimDimension.SOURCE, canonical_value=SourceKind.CONTRACTS,
            authority='contract_universe_intent', authority_version='1',
        )
    claims.append(SemanticClaim(
        id=next_id('universe'), turn_id=turn_id, span=span,
        dimension=ClaimDimension.CONTRACT_UNIVERSE, canonical_value=intent.universe.value,
        authority='contract_universe_intent', authority_version='1',
    ))

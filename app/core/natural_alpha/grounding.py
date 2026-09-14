"""Ground exact raw code-point intervals. No normalization or interpretation."""
import json

from .models import GroundedJob, Proposal


def ground_exact(raw: str, quote: str, occurrence: int) -> tuple[int, int]:
    if type(raw) is not str or type(quote) is not str or not quote:
        raise ValueError("INVALID_EVIDENCE")
    if type(occurrence) is not int or occurrence < 1:
        raise ValueError("INVALID_OCCURRENCE")
    start = -1
    for _ in range(occurrence):
        start = raw.find(quote, start + 1)
        if start < 0:
            raise ValueError("UNGROUNDED_EVIDENCE")
    return start, start + len(quote)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def validate_and_ground(raw: str, payload: str) -> tuple[GroundedJob, ...]:
    if type(payload) is not str or len(payload) > 20_000:
        raise ValueError("INVALID_PAYLOAD")
    document = json.loads(payload, object_pairs_hook=_unique_object)
    proposal = Proposal.model_validate(document)
    return tuple(
        GroundedJob(job.action, job.aspect, *ground_exact(raw, job.evidence_quote, job.occurrence))
        for job in proposal.jobs
    )

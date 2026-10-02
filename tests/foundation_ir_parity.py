"""The observable outcome of one proposal through H7 -> admission -> projection (INV-C0-2).

The same function produced the pinned fixture on the IR v1 base and runs on the IR v2: a proposal that
``lift_v1`` carries must give the same status, reason, resolutions, SoQL, clauses and dispositions. Only
the interpretation/plan hashes may change, and they are not part of the dump.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.foundation_admission import admit_proposal
from app.core.foundation_projection import ProjectionError, project_accepted
from app.core.foundation_runtime import host_universe
from app.core.interpretation_contract import AcceptanceStatus, InterpretationAcceptance

INSTANT = datetime(2026, 9, 30, 12, 0, tzinfo=ZoneInfo("America/Bogota"))


def dump_acceptance(accepted) -> dict:
    return {
        "status": accepted.status.value, "reason": accepted.reason,
        "resolutions": [[r.target_id, r.decision.value, r.authority, list(r.resolved_ids)]
                        for r in accepted.resolutions],
        "policies": sorted([p.policy_id, p.version, list(p.condition_ids), p.effect.value]
                           for p in accepted.policies),
    }


def dump_outcome(proposal, raw: str, *, resolver, builder) -> dict:
    record: dict = {}
    if isinstance(proposal, InterpretationAcceptance):
        return {"stage": "decode", "final_status": proposal.status.value, "final_reason": proposal.reason}
    universe = host_universe(proposal)
    record["h7"] = {"conflict": universe.conflict, "decisions": [d.to_dict() for d in universe.decisions]}
    if universe.conflict:
        accepted = InterpretationAcceptance(universe.proposal, AcceptanceStatus.NEEDS_CLARIFICATION,
                                            reason=universe.conflict)
    else:
        accepted = admit_proposal(universe.proposal, raw, entity_resolver=resolver)
    record["admission"] = dump_acceptance(accepted)
    final = (accepted.status.value, accepted.reason)
    if accepted.status is AcceptanceStatus.READY:
        try:
            outcome = project_accepted(accepted, reference_instant=INSTANT, builder=builder)
        except (ProjectionError, ValueError) as exc:
            record["projection"] = {"error": str(exc)}
            final = ("UNSUPPORTED", str(exc))
        else:
            record["projection"] = {
                "status": outcome.status.value, "reason": outcome.reason,
                "manifest": [{"request_id": m.request_id, "scope_id": m.scope_id, "operation": m.operation,
                              "dataset": m.dataset, "soql": m.soql,
                              "clauses": [[c.clause, list(c.condition_ids), list(c.policy_ids)] for c in m.clauses],
                              "policies": [[p.policy_ref, p.effect.value] for p in m.policies],
                              "local_filter": m.local_filter.receipt() if m.local_filter else None}
                             for m in outcome.manifest],
                "dispositions": [[d.condition_id, list(d.request_ids), d.disposition.value, d.effect.value,
                                  d.operator, d.polarity.value, d.unknown_treatment, d.clause_or_policy,
                                  d.role.value]
                                 for call in outcome.calls for d in call.dispositions],
            }
            if outcome.status is not AcceptanceStatus.READY:
                final = (outcome.status.value, outcome.reason)
    record.update(stage="host", final_status=final[0], final_reason=final[1])
    return record

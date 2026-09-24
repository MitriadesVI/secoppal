"""Alpha FOLLOW/LIST and FOLLOW/RUN → the Radar commands that already exist.

Natural phrasings («qué búsquedas tengo guardadas», «hay novedades en PAE
Caribe?») never match the literal radar grammar, so they used to fall to the
tabular search as a topic. The job pair only chooses the command; the runtime is
the radar handler's own:

- LIST takes no parameter: it reads the owner's radars.
- RUN's only input, the radar, is accredited by the owner's store: exactly one
  of their search radars' names must occur literally in the turn. With none or
  several, it runs nothing and answers with the owner's radars. The synthetic
  followed-processes watchlist is not a search radar and has its own runtime:
  it is never a RUN candidate (running its empty frame counts a permanent
  failure against it, and three of those pause the watchlist).

``None`` keeps the turn on the legacy chain (nothing bound, or no radar store).
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.core.capability_binding.registry import FOLLOW_LIST, FOLLOW_RUN
from app.core.radar.commands import radar_owner
from app.core.radar.name_mentions import radars_named_in
from app.core.radar.store import FOLLOWS_FREQUENCY
from app.core.workflow.handlers.natural_alpha import attach_observation
from app.core.workflow.handlers.radar import _do_list, _do_run

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


def handle_bound_follow(
    deps: "WorkflowDeps", ctx: "HandlerContext", capability, observation,
) -> dict | None:
    if capability not in (FOLLOW_LIST, FOLLOW_RUN):
        return None
    if not ctx.chat_id or deps.radar_store is None:
        return None
    store = deps.radar_store
    owner = radar_owner(ctx.user_identity, ctx.chat_id)
    if capability == FOLLOW_LIST:
        result = _do_list(store, owner)
    else:
        result = _run_named(deps, store, owner, ctx.user_query)
    # The literal radar commands leave no trace; a bound turn does, so the
    # dogfood can see that Alpha, not the grammar, opened the command.
    attach_observation(result, observation)
    result["trace_id"] = deps.feedback.log_trace(
        ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
    )
    return result


def _run_named(deps, store, owner, text: str) -> dict:
    radars = store.list_radars(owner)
    searches = [radar for radar in radars if radar.frequency != FOLLOWS_FREQUENCY]
    named = radars_named_in(text, searches)
    if len(named) == 1:
        return _do_run(deps, store, owner, named[0].name)
    listing = _do_list(store, owner)
    if not radars:
        return listing
    lead = (
        "Tu mensaje nombra varios de tus radares y no voy a elegir por ti."
        if named else
        "No reconocí cuál de tus radares quieres revisar."
    )
    listing["response"] = f"{lead}\n\n{listing['response']}"
    listing["route_reason"] = "radar_run_name_ambiguous" if named else "radar_run_name_required"
    listing["needs_clarification"] = True
    return listing

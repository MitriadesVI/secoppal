"""Workflow handlers — early-return paths and refinement bypass.

Each handler implements one branch of the chain of responsibility that
``SecopalWorkflow.run_query`` walks before the Burr pipeline runs:

- ``refinement``   — bypass parser when the UI sends pre-resolved
                    params (refinement bar chip).

Future handlers (Paso 7) will add: reset, pagination, suggestion
selection, direct lookup, and the normal-flow wrapper.

Contract
--------
Every handler is a free function with the signature::

    def handle_<name>(deps: WorkflowDeps, ctx: HandlerContext, **extras) -> dict | None

Returns the full result dict when this handler claims the request,
``None`` when the next handler in the chain should try.

Like ``workflow/actions/``, this package has no re-exports in
``__init__`` — callers import from the specific submodule so the chain
order stays visible at the call site.
"""

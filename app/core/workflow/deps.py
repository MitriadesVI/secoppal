"""WorkflowDeps — composition root for SecopalWorkflow collaborators.

This dataclass groups the ten objects that ``SecopalWorkflow`` constructs
at startup, so future handlers (Paso 5) can receive a single ``deps``
argument instead of accepting the whole workflow instance.

Why a property-built snapshot (not a stored instance)?
------------------------------------------------------
The existing test suite mutates workflow attributes directly:

    wf.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    wf.secop_client = make_mock_client(...)

If we stored ``self.deps`` as an instance attribute, those reassignments
would leave ``self.deps.conv_store`` pointing to the old object. To keep
the public API identical, ``SecopalWorkflow.deps`` is a property that
constructs a fresh ``WorkflowDeps`` on every access. The cost is one
small dataclass construction per ``run_query`` call — negligible next to
the SECOP HTTP round-trip — and the upside is zero breaking changes for
the rest of the refactor.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.entity_resolver import EntityResolver
from app.core.feedback import FeedbackStore
from app.core.formatter import Formatter
from app.core.llm_handler import LLMHandler
from app.core.narrator import NarratorHandler
from app.core.query_router import QueryRouter
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder


@dataclass
class WorkflowDeps:
    """Snapshot of the collaborators a handler needs.

    Constructed via ``SecopalWorkflow.deps`` (a property) so it always
    reflects the workflow's current attributes, even after tests reassign
    ``wf.conv_store`` or ``wf.secop_client``.
    """

    settings: Settings
    entity_resolver: EntityResolver
    query_router: QueryRouter
    llm_handler: LLMHandler
    soql_builder: SoQLBuilder
    secop_client: SecopClient
    formatter: Formatter
    feedback: FeedbackStore
    conv_store: ConversationStore
    narrator: NarratorHandler

from __future__ import annotations

from functools import lru_cache

from app.config import get_settings
from app.core.orchestrator import SecopalWorkflow
from app.utils.logging import configure_logging


@lru_cache(maxsize=1)
def get_workflow() -> SecopalWorkflow:
    settings = get_settings()
    configure_logging(settings.log_level)
    return SecopalWorkflow(settings)


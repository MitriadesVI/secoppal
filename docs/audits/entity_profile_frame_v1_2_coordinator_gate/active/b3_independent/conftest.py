"""Independent audit persistence; every artefact stays under tmp_path."""
import pytest


@pytest.fixture(autouse=True)
def isolated_audit_persistence(monkeypatch, tmp_path):
    from app.config import get_settings
    monkeypatch.setenv('SECOP_FEEDBACK_PATH', str(tmp_path / 'feedback.jsonl'))
    monkeypatch.setenv('SECOP_FEEDBACK_DB_PATH', str(tmp_path / 'feedback.sqlite'))
    monkeypatch.setenv('CONVERSATION_STORE_DIR', str(tmp_path / 'conversations'))
    monkeypatch.setenv('SECOP_ACTOR_ALIAS_INDEX_PATH', str(tmp_path / 'missing-index.sqlite'))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()

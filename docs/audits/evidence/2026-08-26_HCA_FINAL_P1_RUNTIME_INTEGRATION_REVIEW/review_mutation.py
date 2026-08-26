"""Temporarily remove only the derived-state persistence fix in this process."""
def pytest_configure(config):
    import app.core.conversation_store as module
    module.strip_derived_execution_state = dict

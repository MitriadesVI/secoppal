"""B1 — SLASH-COMMAND-NOT-DETECTED-001: variantes con slash como reset."""

import pytest

from app.core.conversation_store import is_reset_command


@pytest.mark.parametrize("cmd", ["/new", "/reset", "/nueva", "/limpiar", "/clear", "/restart"])
def test_slash_commands_recognized(cmd):
    assert is_reset_command(cmd) is True


def test_existing_reset_still_works():
    assert is_reset_command("reset") is True
    assert is_reset_command("nueva") is True
    assert is_reset_command("limpiar") is True


def test_query_with_slash_in_middle_not_reset():
    assert is_reset_command("buscar /tarifa") is False

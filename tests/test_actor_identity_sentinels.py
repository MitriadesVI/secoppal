from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from app.core.actor_identity import (
    REASON_SENTINEL_DISCARDED,
    actor_document_sentinels,
    is_actor_document_sentinel,
    normalize_identity,
)
from app.core.actor_identity_soql import exclude_actor_document_sentinels


@settings(derandomize=True)
@given(
    sentinel=st.sampled_from(actor_document_sentinels()),
    lower=st.booleans(),
    padding=st.sampled_from(['', ' ', '\t', '\n', '\u00a0']),
)
def test_helper_and_normalizer_share_existing_authority(sentinel, lower, padding):
    raw = padding + (sentinel.lower() if lower else sentinel) + padding
    assert is_actor_document_sentinel(raw)
    match = normalize_identity(raw, document_type='NIT')
    assert match.key is None
    assert REASON_SENTINEL_DISCARDED in match.reason_codes


@pytest.mark.parametrize('value', [None, 12345678, '', ' ', 'unknown', '12', '900123456', '860.034.594-1', 'NO  DEFINIDO', 'SIN INFORMACIO\u0301N'])
def test_helper_does_not_expand_authority(value):
    assert not is_actor_document_sentinel(value)


@pytest.mark.parametrize('kind', ['CC', 'NIT'])
def test_valid_typed_document_unchanged(kind):
    match = normalize_identity('900123456', document_type=kind)
    assert (match.key.document_type, match.key.base_number) == (kind, '900123456')


def test_soql_escapes_authority_literals(monkeypatch):
    monkeypatch.setattr('app.core.actor_identity_soql.actor_document_sentinels', lambda: ("A'B",))
    assert exclude_actor_document_sentinels('documento_proveedor') == "upper(trim(documento_proveedor)) NOT IN ('A''B')"


@pytest.mark.parametrize('column', ['documento); DELETE', 'UPPER(documento)', "a'b", 'table.column', ''])
def test_soql_rejects_non_column_expression(column):
    with pytest.raises(ValueError):
        exclude_actor_document_sentinels(column)

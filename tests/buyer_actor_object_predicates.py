"""Inspect field/literal comparisons emitted by the contractual object builder.

This bounded test reader supports LIKE/equality and the builder's unary wrappers.
Boolean grouping and sibling predicates have no authority over field ownership.
SQL string literals are tokenized atomically, including escaped apostrophes.
"""
import re

from tests.buyer_actor_sql_dialect import unaccent

_TOKEN = re.compile(r"'(?:''|[^'])*'|[a-zA-Z_][a-zA-Z_0-9]*|<>|!=|[^\s]")
_FIELDS = {'objeto_del_contrato', 'descripcion_del_proceso'}
_WRAPPERS = {'unaccent', 'upper', 'lower', 'trim'}


def _operand(tokens, offset):
    if offset >= len(tokens):
        return None, offset
    token = tokens[offset]
    if token.lower() in _WRAPPERS and tokens[offset + 1:offset + 2] == ['(']:
        value, end = _operand(tokens, offset + 2)
        if tokens[end:end + 1] == [')']:
            return value, end + 1
        return None, offset
    if token.startswith("'"):
        return ('literal', token[1:-1].replace("''", "'")), offset + 1
    if token.lower() in _FIELDS:
        return ('field', token.lower()), offset + 1
    return None, offset


def assert_no_request_language_in_object_predicates(soql):
    tokens = _TOKEN.findall(soql)
    where = next((i for i, token in enumerate(tokens) if token.upper() == 'WHERE'), None)
    if where is None:
        return
    for offset in range(where + 1, len(tokens)):
        left, end = _operand(tokens, offset)
        if left is None:
            continue
        if tokens[end:end + 1] and tokens[end].upper() == 'NOT':
            end += 1
        if end >= len(tokens) or tokens[end].upper() not in {'LIKE', '=', '!=', '<>'}:
            continue
        right, _ = _operand(tokens, end + 1)
        if right is None or left[0] == right[0]:
            continue
        field, value = (left[1], right[1]) if left[0] == 'field' else (right[1], left[1])
        assert not any(term in unaccent(value).upper() for term in ('CONTRATADO', 'ALCALDIA')), (
            f'request language as topic: {field} {value!r}')

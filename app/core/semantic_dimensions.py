"""Shared names for deterministic query dimensions.

This is intentionally a vocabulary, not an ontology.  It lets structured
resolvers declare what they consumed so their spans cannot later be inferred
again as thematic object terms.
"""
from __future__ import annotations

from enum import Enum


class SemanticDimension(str, Enum):
    TOPIC = "TOPIC"
    MODALITY = "MODALITY"
    CONTRACT_TYPE_OR_NATURE = "CONTRACT_TYPE_OR_NATURE"
    ACTOR = "ACTOR"
    BUYER = "BUYER"
    GEOGRAPHY = "GEOGRAPHY"
    TIME = "TIME"
    VALUE = "VALUE"
    STATE = "STATE"
    CONTROL_LANGUAGE = "CONTROL_LANGUAGE"

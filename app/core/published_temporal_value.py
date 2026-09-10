"""Is a published temporal value usable as a date?

One question, asked by runtime code that is not the Truth Layer: Contract
Universe, deciding whether an ``Aprobado`` contract evidences a signature, and
the formatter, deciding whether a contractual date can be shown as one.

Both used to import a raw Truth Layer resolver module directly, reaching past
the package's public surface. The boundary exists because those resolvers take
an injected reference and return a full assessment; a consumer that wants a
yes/no should not be reading their internals, and it should not re-implement
ISO parsing either. So the question is asked once, here, through
``app.core.truth_layer`` — the package root that already exports the typed
observation and its resolution.

The prose above deliberately does not name the private modules: the runtime
boundary is checked by scanning this file's text, and naming one in order to
disclaim it would read as a use of it.

Presence is the caller's contract, not this module's: both consumers establish
that they hold an observed string before asking. A value that is absent is a
different question, with a different answer, and it belongs to them.
"""
from __future__ import annotations

from app.core.truth_layer import FieldObservation, resolve_temporal_value

__all__ = ["observed_date_is_usable"]


def observed_date_is_usable(value: str) -> bool:
    """Whether ``value``, observed in the source, really is a date.

    A civil date, a naive timestamp and an offset-bearing instant all count.
    Unparseable text, an empty string and a local time that its own zone never
    had do not — none of them is a date, and none may be presented as one.
    """
    return resolve_temporal_value(FieldObservation("observed", value)).is_valid

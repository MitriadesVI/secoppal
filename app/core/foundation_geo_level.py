"""Geographic level a GEOGRAPHY condition names (spine v5, C0 §5.1.1).

A tiny module on purpose: the authority contract (C1.1) and the IR v2 (C1.2) both need the
same enum, and neither may own the other.  ``UNKNOWN`` is a level the *producer* may claim when
it cannot tell; a resolved ``GeoValue`` never carries it.
"""

from __future__ import annotations

from enum import Enum


class GeoLevel(str, Enum):
    DEPARTMENT = "DEPARTMENT"
    MUNICIPALITY = "MUNICIPALITY"
    REGION = "REGION"
    UNKNOWN = "UNKNOWN"

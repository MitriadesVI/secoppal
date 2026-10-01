"""The datasets of an ``AuthorityContext`` by logical name (M1-D-STATE/MOD).

STATE and MODALITY literals are per dataset (``procesos`` and ``contratos`` have different columns and
different spellings).  The context carries Socrata dataset ids; the catalogs speak the logical names.
An id the catalogs do not know is dropped here, and the authority that needs a dataset it cannot see
clarifies: it never invents one.
"""

from __future__ import annotations

from app.core.foundation_authority_contract import AuthorityContext
from app.core.soql_builder import SoQLBuilder

PROCESSES = "procesos"
CONTRACTS = "contratos"

_LOGICAL = {
    SoQLBuilder.PROCESOS_DATASET: PROCESSES, SoQLBuilder.CONTRATOS_DATASET: CONTRACTS,
    PROCESSES: PROCESSES, CONTRACTS: CONTRACTS,
}


def logical_datasets(ctx: AuthorityContext) -> dict[str, str]:
    """``{context dataset id: logical name}`` for the datasets the catalogs know, sorted by id."""
    return {name: _LOGICAL[name] for name in sorted(ctx.datasets) if name in _LOGICAL}

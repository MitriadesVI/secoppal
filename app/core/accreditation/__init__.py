"""Pre-legacy accreditation contracts (plan V2 5-7).

The LLM accredits the JOB; the host accredits HOW to run it. This package holds
only the two contracts that let a turn travel that way — the credential and the
declarative capability spec — plus the registry that matches them. Authorities,
residual accounting and admission changes live elsewhere and arrive later.
"""

from .credential import NATURAL_ALPHA, CapabilityCredential, from_grounded_job
from .registry import (
    CONTRACTS_UNIVERSE,
    SUPPLIERS,
    capability_spec,
    declared_capabilities,
    spec_for_credential,
)
from .spec import CapabilitySpec, Cardinality, Dimension, Owner

__all__ = [
    "CONTRACTS_UNIVERSE",
    "NATURAL_ALPHA",
    "SUPPLIERS",
    "CapabilityCredential",
    "CapabilitySpec",
    "Cardinality",
    "Dimension",
    "Owner",
    "capability_spec",
    "declared_capabilities",
    "from_grounded_job",
    "spec_for_credential",
]

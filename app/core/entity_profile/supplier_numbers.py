"""Supplier amount accreditation at the existing float presentation boundary."""
from dataclasses import dataclass
import math

from app.core.entity_profile.frame_evidence import accredit_metric


@dataclass(frozen=True, slots=True)
class SupplierAmount:
    value: float | None
    status: str


def supplier_amount(raw: object) -> SupplierAmount:
    metric = accredit_metric('total_contract_value', raw)
    if metric.value is None:
        return SupplierAmount(None, metric.status.value)
    value = float(metric.value)
    if not math.isfinite(value):
        return SupplierAmount(None, 'REPRESENTATION_OVERFLOW')
    if value == 0 and metric.value != 0:
        return SupplierAmount(None, 'REPRESENTATION_UNDERFLOW')
    return SupplierAmount(value, 'VALID')

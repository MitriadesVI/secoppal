"""Value sanity checks for SECOP contract data — detect anomalous source values.

Detects data-entry errors in Socrata without silently altering the raw data.
The canonical example: ``valor_del_contrato`` inflated x1000 vs ``valor_facturado``
(250.000.000.000 vs 250.000.000 in Luruaco/CD-1028-2025).

Does NOT modify any field. Returns diagnostic metadata only.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# ── Constants ──────────────────────────────────────────────────────────
RATIO_SUSPECT_THRESHOLD = 100
# Above this ratio (valor_del_contrato / valor_facturado), the contract
# value is flagged as suspect. Chosen after observing real x1000 errors:
# - Luruaco: 250_000_000_000 / 250_000_000 = 1000
# - DANE: 9_645_115_773_936 / 19_689_256 ≈ 489_867
# A ratio of 100 catches both clear x1000 errors and extreme outliers.


def detect_value_anomaly(item: dict, dataset_id: str) -> dict | None:
    """Check a normalized SECOP item for value data anomalies.

    Only operates on the Contratos dataset (jbjy-vk9h). For Procesos,
    the ``precio_base`` field has not shown the same data-entry issues.

    Returns a dict with metadata if anomalous, ``None`` if clean.
    The dict keys are designed to be merged into the display row:

    * ``value_quality`` — ``"suspect"``
    * ``value_warning`` — human-readable diagnostic
    * ``value_reference`` — the alternate value that appears correct

    """
    if dataset_id != "jbjy-vk9h":
        return None

    vdc = item.get("valor_del_contrato")
    vf = item.get("valor_facturado")

    # Both must exist and be comparable numbers
    if not isinstance(vdc, (int, float)) or not isinstance(vf, (int, float)):
        return None
    if vf <= 0:
        return None
    if vdc <= 0:
        return None

    ratio = vdc / vf
    if ratio < RATIO_SUSPECT_THRESHOLD:
        return None

    return {
        "value_quality": "suspect",
        "value_warning": (
            "valor_del_contrato difiere significativamente de valor_facturado"
        ),
        "value_reference": vf,
    }

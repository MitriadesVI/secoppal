"""Catálogo anual autoritativo del salario mínimo mensual legal colombiano.

No hace I/O ni actualización automática. Un año nuevo exige agregar el acto
oficial correspondiente y sus pruebas antes de habilitar conversiones.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


BOGOTA_TZ = ZoneInfo("America/Bogota")

# 2024 — Decreto 2292 de 2023, artículo 1.
# https://www.suin-juriscol.gov.co/viewDocument.asp?ruta=Decretos/30050726
# 2025 — Decreto 1572 de 2024, artículo 1.
# https://www.suin-juriscol.gov.co/viewDocument.asp?id=30054293
# 2026 — Decreto 1469 de 2025, artículo 1; el mismo valor fue fijado
# transitoriamente por el Decreto 159 de 2026 tras la medida cautelar.
# https://www.suin-juriscol.gov.co/viewDocument.asp?id=30055940
# https://www.suin-juriscol.gov.co/viewDocument.asp?id=30056106
SMMLV_COP_BY_YEAR: dict[int, int] = {
    2024: 1_300_000,
    2025: 1_423_500,
    2026: 1_750_905,
}


def current_colombian_year(now: datetime | None = None) -> int:
    """Año civil actual según un reloj explícito de America/Bogota."""
    instant = now or datetime.now(BOGOTA_TZ)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=BOGOTA_TZ)
    return instant.astimezone(BOGOTA_TZ).year


def smmlv_cop(year: int) -> int | None:
    """Valor COP de un SMMLV para ``year`` o None si no está catalogado."""
    return SMMLV_COP_BY_YEAR.get(year)

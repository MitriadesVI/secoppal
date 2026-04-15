from __future__ import annotations

import json
from pathlib import Path

from app.config import get_settings
from app.core.secop_client import SecopClient

OUTPUT_PATH = Path("scripts/top_entities.json")


def main() -> None:
    settings = get_settings()
    client = SecopClient(
        domain=settings.datos_gov_domain,
        app_token=settings.secop_app_token,
        timeout=settings.secop_timeout_seconds,
    )

    processes_query = (
        "SELECT entidad, COUNT(*) AS total "
        "GROUP BY entidad ORDER BY total DESC LIMIT 500"
    )
    contracts_query = (
        "SELECT nombre_entidad, COUNT(*) AS total "
        "GROUP BY nombre_entidad ORDER BY total DESC LIMIT 500"
    )

    payload = {
        "procesos": client.query("p6dx-8zbt", processes_query),
        "contratos": client.query("jbjy-vk9h", contracts_query),
    }

    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Saved top entities to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()


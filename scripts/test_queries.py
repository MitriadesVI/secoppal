from __future__ import annotations

from app.config import get_settings
from app.core.orchestrator import SecopalWorkflow

QUERIES = [
    "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas",
    "procesos de la gobernacion del Atlantico",
    "contratos del SENA en Bogota mayores a 200 millones",
]


def main() -> None:
    workflow = SecopalWorkflow(get_settings())
    for query in QUERIES:
        result = workflow.run_query(query, channel="streamlit")
        print("=" * 80)
        print(query)
        print("-" * 80)
        print(result["response"])
        print(result["soql_query"])
        print(result["parsed_params"])
        print(result["resolved_params"])


if __name__ == "__main__":
    main()


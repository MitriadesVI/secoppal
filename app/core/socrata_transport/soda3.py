"""Transporte SODA3 — el transporte productivo default.

Implementa el contrato exacto que el discovery validó y congeló en
``scripts/audits/soda3_probe.py``, ``soda3_runtime_canary.py`` y
``soda3_h4_policy_replay.py``:

    POST https://{domain}/api/v3/views/{dataset_id}/query.json
    {
      "query": "<SoQL literal>",
      "timeout": <segundos>,
      "includeSynthetic": false,
      "orderingSpecifier": "total" | "no_preserve"
    }

No se inventa ninguna variante del protocolo. El SoQL viaja **literal**: este
módulo no lo reescribe, no lo normaliza, no lo inspecciona y no decide su
política de ordenamiento — la recibe de la ruta que la conoce.

Usa ``httpx``, que ya es dependencia del proyecto. Cada request lleva su
timeout explícito (INV-SODA-06): no hay mutación de estado compartido.
"""
from __future__ import annotations

from typing import Any

import httpx

from app.core.socrata_transport.base import (
    TRANSPORT_SODA3,
    Soda3OrderingSpecifier,
)
from app.core.socrata_transport.errors import (
    Soda3ContractViolationError,
    Soda3DecodeError,
    Soda3NetworkError,
    Soda3TimeoutError,
    classify_http_status,
)

#: Statuses que el discovery aceptó como respuesta válida.
_OK_STATUSES = (200, 202)

_USER_AGENT = "SECOPPAL-SODA3-transport-v1"


class Soda3HttpTransport:
    """Cliente HTTP síncrono contra ``/api/v3/views/{dataset}/query``.

    Sin estado de timeout: el presupuesto llega por llamada, de modo que los
    clientes derivados de ``SecopClient`` pueden compartir esta instancia (y
    su pool de conexiones) sin pisarse.
    """

    name = TRANSPORT_SODA3

    def __init__(
        self,
        *,
        domain: str,
        app_token: str | None,
        http_client: Any | None = None,
    ) -> None:
        self.domain = domain
        self.app_token = app_token
        self._http_client = http_client
        self._owns_http_client = http_client is None

    # ── Las tres formas remotas ──────────────────────────────────────────
    # Separadas para que cada ruta declare su política de forma consciente.
    # Ninguna la deduce del texto del SoQL.

    def query(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> list[dict]:
        return self._post(dataset_id, soql, timeout_seconds, ordering_specifier)

    def count(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> list[dict]:
        return self._post(dataset_id, soql, timeout_seconds, ordering_specifier)

    def aggregate(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> list[dict]:
        return self._post(dataset_id, soql, timeout_seconds, ordering_specifier)

    # ── Transporte ───────────────────────────────────────────────────────

    @property
    def http_client(self):
        """Cliente ``httpx`` perezoso: construirlo no debe abrir recursos."""
        if self._http_client is None:
            self._http_client = httpx.Client(headers=self._headers())
        return self._http_client

    def close(self) -> None:
        if self._owns_http_client and self._http_client is not None:
            self._http_client.close()
            self._http_client = None

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": _USER_AGENT}
        if self.app_token:
            headers["X-App-Token"] = self.app_token
        return headers

    def url_for(self, dataset_id: str) -> str:
        return f"https://{self.domain}/api/v3/views/{dataset_id}/query.json"

    @staticmethod
    def payload_for(
        soql: str,
        timeout_seconds: float,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> dict:
        """El payload congelado por el discovery, sin variantes.

        El SoQL se copia literal. ``timeout`` es el presupuesto en segundos
        que el runtime ya calculó; se pasa entero y con piso 1 porque un
        presupuesto de 0 le pediría al backend que no ejecute nada.
        """
        return {
            "query": soql,
            "timeout": max(1, int(timeout_seconds)),
            "includeSynthetic": False,
            "orderingSpecifier": Soda3OrderingSpecifier(ordering_specifier).value,
        }

    def _post(
        self,
        dataset_id: str,
        soql: str,
        timeout_seconds: float,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> list[dict]:
        payload = self.payload_for(soql, timeout_seconds, ordering_specifier)
        try:
            response = self.http_client.post(
                self.url_for(dataset_id),
                json=payload,
                timeout=timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            raise Soda3TimeoutError(
                f"SODA3 request exceeded its budget on dataset {dataset_id}",
                dataset_id=dataset_id, retryable=True,
            ) from exc
        except httpx.HTTPError as exc:
            raise Soda3NetworkError(
                f"SODA3 network failure on dataset {dataset_id}: {type(exc).__name__}",
                dataset_id=dataset_id, retryable=True,
            ) from exc

        status = response.status_code
        if status not in _OK_STATUSES:
            error_cls = classify_http_status(status)
            # Un fallo HTTP NUNCA se degrada a lista vacía: eso convertiría un
            # 400 de SoQL en un "no hay resultados" indistinguible.
            raise error_cls(
                f"SODA3 responded HTTP {status} on dataset {dataset_id}",
                dataset_id=dataset_id, http_status=status,
                retryable=status in (429, 500, 502, 503, 504),
            )

        try:
            decoded = response.json()
        except ValueError as exc:
            raise Soda3DecodeError(
                f"SODA3 returned a body that is not JSON on dataset {dataset_id}",
                dataset_id=dataset_id, http_status=status,
            ) from exc

        if not isinstance(decoded, list) or not all(isinstance(row, dict) for row in decoded):
            raise Soda3ContractViolationError(
                f"SODA3 returned an unexpected payload shape on dataset {dataset_id}",
                dataset_id=dataset_id, http_status=status,
            )
        return decoded

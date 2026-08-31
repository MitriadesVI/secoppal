from __future__ import annotations

import time
from typing import Any

try:
    from sodapy import Socrata
except ImportError:  # pragma: no cover - guarded for environments without deps
    Socrata = None

from app.core.instrumentation import http_span, record_http
from app.core.socrata_transport import (
    DEFAULT_ORDERING_SPECIFIER,
    ClassicSocrataTransport,
    Soda3OrderingSpecifier,
    SocrataTransport,
)
from app.utils.logging import get_logger

logger = get_logger(__name__)


class SecopClient:
    """Thin wrapper around Socrata with retries and SECOP quirks handling.

    Desde SODA3_TRANSPORT_V1 la salida a la red pasa por un
    ``SocrataTransport`` (ver ``app/core/socrata_transport/``). Ambos
    transportes siguen siendo seleccionables por configuración: ``soda3`` es
    el default productivo y ``classic`` permanece como rollback explícito.
    """

    def __init__(
        self,
        domain: str,
        app_token: str | None,
        timeout: int = 30,
        client: Any | None = None,
        max_retries: int = 1,
        transport: SocrataTransport | None = None,
    ) -> None:
        if client is None and transport is None and Socrata is None:
            raise RuntimeError("sodapy is required to instantiate SecopClient without a custom client.")

        self.domain = domain
        self.app_token = app_token
        self.timeout = timeout
        self._injected_client = client  # None salvo en tests / clientes custom
        # Transporte explicito (factory / tests de transporte). Cuando es None el
        # cliente opera en clasico y `self.client` sigue siendo el objeto sodapy.
        self._explicit_transport = transport
        if transport is not None:
            self.client = client
        else:
            self.client = client or Socrata(domain, app_token=app_token, timeout=timeout)
        self.max_retries = max_retries

    @property
    def transport(self) -> SocrataTransport:
        """Transporte activo de esta instancia.

        Sin transporte explicito se envuelve ``self.client`` **en cada acceso**:
        la envoltura clasica no tiene estado propio y asi reasignar
        ``secop_client.client`` --como hacen los harnesses de auditoria para
        cortar la red-- sigue surtiendo efecto inmediato.
        """
        if self._explicit_transport is not None:
            return self._explicit_transport
        return ClassicSocrataTransport(self.client)

    @property
    def transport_name(self) -> str:
        return self.transport.name

    def derived_for_timeout(self, timeout_s: int) -> "SecopClient":
        """Cliente derivado acotado a ``timeout_s``, **thread-safe** (no muta esta
        instancia ni su transporte compartido).

        - Si ``query``/``count`` fueron mockeados en ESTA instancia (monkeypatch de
          tests), devuelve ``self`` para preservar esos métodos — sin mutar nada.
        - Si hay un cliente inyectado (tests con fake), reutiliza ese mismo transporte
          en un SecopClient nuevo (el fake es inmutable respecto al timeout).
        - En producción crea un Socrata propio con el timeout acotado: cada turno/llamada
          obtiene su transporte, sin race sobre el singleton compartido.
        """
        if "query" in self.__dict__ or "count" in self.__dict__:
            return self  # métodos mockeados en la instancia: preservarlos, no mutar
        if self._explicit_transport is not None or self._injected_client is not None:
            return self._respawn(timeout_s)
        return self.with_timeout(timeout_s)

    def with_timeout(self, timeout: int) -> "SecopClient":
        """Return a new SecopClient with a different transport timeout.

        Preserva el cliente inyectado (tests/custom) y el transporte explícito:
        cambiar el timeout no debe descartar un cliente mockeado, abrir un
        Socrata real ni degradar un transporte SODA3 a clásico. Solo se crea un
        Socrata nuevo cuando este cliente usa uno propio.
        """
        if self._explicit_transport is not None or self._injected_client is not None:
            return self._respawn(timeout)
        if Socrata is None:
            raise RuntimeError("sodapy is required for with_timeout.")
        return SecopClient(
            domain=self.domain,
            app_token=self.app_token,
            timeout=timeout,
            max_retries=self.max_retries,
        )

    def _respawn(self, timeout: int) -> "SecopClient":
        """Copia acotada a ``timeout`` que conserva cliente y transporte.

        El transporte SODA3 se comparte a propósito: no guarda el timeout (lo
        recibe por llamada) y reutilizar su pool de conexiones es correcto.
        """
        return SecopClient(
            domain=self.domain,
            app_token=self.app_token,
            timeout=timeout,
            client=self._injected_client,
            max_retries=self.max_retries,
            transport=self._explicit_transport,
        )

    def aggregate(
        self,
        dataset_id: str,
        soql: str,
        *,
        ordering_specifier: Soda3OrderingSpecifier = DEFAULT_ORDERING_SPECIFIER,
    ) -> list[dict]:
        """Execute a single aggregation query. No retries — best-effort for observer.

        Declara ``TOTAL`` de forma consciente: es la política que el discovery
        midió para los 37 ``aggregate`` del corpus. Un ``aggregate`` sin
        ``ORDER BY`` **no** es un fallback H4 y no hereda su política.
        """
        with http_span("aggregate", dataset_id, soql) as span:
            rows = list(self.transport.aggregate(
                dataset_id, soql,
                timeout_seconds=self.timeout,
                ordering_specifier=ordering_specifier,
            ))
            span["rows_received"] = len(rows)
        return rows

    def count(
        self,
        dataset_id: str,
        soql_count_query: str,
        *,
        ordering_specifier: Soda3OrderingSpecifier = DEFAULT_ORDERING_SPECIFIER,
    ) -> int:
        """Execute a SELECT count(*) query and return the integer result.

        Declara ``TOTAL`` de forma consciente. Un ``count`` no lleva
        ``ORDER BY`` por naturaleza: esa ausencia **no** lo convierte en H4.
        """
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                with http_span("count", dataset_id, soql_count_query, attempt=attempt + 1) as span:
                    rows = self.transport.count(
                        dataset_id, soql_count_query,
                        timeout_seconds=self.timeout,
                        ordering_specifier=ordering_specifier,
                    )
                    span["rows_received"] = len(rows)
                if rows:
                    raw = list(rows[0].values())[0]
                    return int(raw)
                return 0
            except Exception as exc:  # pragma: no cover - network/runtime path
                last_error = exc
                logger.warning("SECOP count failed on attempt %s/%s: %s", attempt + 1, self.max_retries, exc)
                if attempt < self.max_retries - 1:
                    delay = 2**attempt
                    record_http({
                        "operation": "retry_delay", "dataset": dataset_id,
                        "attempt": attempt + 1, "duration_ms": float(delay * 1000),
                        "status": "completed", "rows_received": 0,
                    })
                    time.sleep(delay)

        raise RuntimeError(f"Unable to count SECOP dataset {dataset_id}") from last_error

    def query(
        self,
        dataset_id: str,
        soql_query: str,
        *,
        ordering_specifier: Soda3OrderingSpecifier = DEFAULT_ORDERING_SPECIFIER,
    ) -> list[dict]:
        """Ejecuta el SoQL recibido y normaliza las filas.

        ``ordering_specifier`` es la **intención semántica de la ruta**, no una
        lectura del texto del SoQL. El default ``TOTAL`` cubre toda operación
        normal; solo el fallback H4 —que retira el ``ORDER BY`` a propósito—
        pasa ``NO_PRESERVE``, y lo hace explícitamente desde ``execution.py``.
        """
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                logger.info("Executing SECOP query against %s", dataset_id)
                with http_span("query", dataset_id, soql_query, attempt=attempt + 1) as span:
                    rows = self.transport.query(
                        dataset_id, soql_query,
                        timeout_seconds=self.timeout,
                        ordering_specifier=ordering_specifier,
                    )
                    span["rows_received"] = len(rows)
                return [self._normalize_row(dataset_id, row) for row in rows]
            except Exception as exc:  # pragma: no cover - network/runtime path
                last_error = exc
                logger.warning("SECOP query failed on attempt %s/%s: %s", attempt + 1, self.max_retries, exc)
                if attempt < self.max_retries - 1:
                    delay = 2**attempt
                    record_http({
                        "operation": "retry_delay", "dataset": dataset_id,
                        "attempt": attempt + 1, "duration_ms": float(delay * 1000),
                        "status": "completed", "rows_received": 0,
                    })
                    time.sleep(delay)

        raise RuntimeError(f"Unable to query SECOP dataset {dataset_id}") from last_error

    def _normalize_row(self, dataset_id: str, row: dict) -> dict:
        normalized = dict(row)
        url_value = normalized.get("urlproceso", "")
        if isinstance(url_value, dict):
            url_value = url_value.get("url", "")
        url_value = str(url_value or "")

        # Jerarquía de reparación de URL (FIX-URL-FREEFORM-001 + DOGFOOD-URL-005),
        # re-auditada el 2026-08-31 contra datos.gov.co
        # (docs/audits/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE.md):
        # 1. La URL original si es usable (no vacía, no la pantalla "Login").
        # 2. Construir desde id_del_proceso / id_contrato.
        # 3. Fallback a la referencia url-encoded.
        # 4. Cadena vacía → la vista ofrece la búsqueda pública oficial.
        #
        # El guard CO1.REQ se CONSERVA, pero su motivo era falso y se corrige
        # aquí: no es que "CO1.REQ = pre-publicación". Los 9.096.877 procesos de
        # p6dx-8zbt llevan id CO1.REQ.* —el 100%— y el 98,97% trae vista pública
        # entregada por SECOP. Lo que el guard evita es lo único demostrado:
        # NINGUNA fila usa su propio id como noticeUID (0 de 9.096.877), así que
        # construir la URL desde él produce un enlace roto. El spike tampoco
        # halló vía oficial de resolución (0/18 filas sin vista pública se
        # resolvieron por referencia+NIT o por id_del_portafolio), de modo que
        # fallar cerrado sigue siendo la respuesta correcta.
        if not url_value or "Login" in url_value:
            if dataset_id == "p6dx-8zbt":
                process_id = normalized.get("id_del_proceso", "")
                reference = normalized.get("referencia_del_proceso", "")
            else:
                process_id = normalized.get("id_contrato", "")
                reference = normalized.get("referencia_del_contrato", "")

            url_value = ""
            pid = str(process_id or "").strip()
            # Un id CO1.REQ.* nunca es un noticeUID válido (medido: 0 de
            # 9.096.877). Mejor no mostrar link que mostrar uno roto.
            if pid and not pid.upper().startswith("CO1.REQ."):
                from urllib.parse import quote
                url_value = (
                    "https://community.secop.gov.co/Public/Tendering/"
                    f"OpportunityDetail/Index?noticeUID={quote(pid)}"
                )
            elif reference and not pid.upper().startswith("CO1.REQ."):
                from urllib.parse import quote
                ref_str = str(reference).strip()
                url_value = (
                    "https://community.secop.gov.co/Public/Tendering/"
                    f"OpportunityDetail/Index?noticeUID={quote(ref_str)}"
                )

        normalized["urlproceso"] = url_value

        # Normalize monetary values to float for consistent handling
        for money_field in ("precio_base", "valor_del_contrato", "valor_contrato_con_adiciones"):
            if money_field in normalized:
                normalized[money_field] = self._safe_money(normalized[money_field])

        return normalized

    @staticmethod
    def _safe_money(value: object) -> float | None:
        """Robustly convert SECOP monetary values (strings, floats, nulls)."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.replace(",", "").replace("$", "").replace(" ", "").strip()
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None


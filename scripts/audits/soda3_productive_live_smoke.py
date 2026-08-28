#!/usr/bin/env python3
"""Smoke LIVE acotado de la implementación PRODUCTIVA de SODA3.

Esto no vuelve a decidir si SODA3 es mejor transporte: eso ya se adjudicó
(``docs/audits/2026-08-27_SODA3_QUERY_FINAL_ADJUDICATION.md`` → ``GO_INTEGRATION``).
La única pregunta aquí es de **costura**:

    ¿El wiring productivo `Settings → SecopalWorkflow → SecopClient →
    Soda3HttpTransport` emite contra www.datos.gov.co requests compatibles
    con los contratos que los probes experimentales congelaron?

Por eso son cuatro casos, no una campaña: pocos, tomados del manifiesto
congelado del canario V1, y ejecutados por el camino real.

    python scripts/audits/soda3_productive_live_smoke.py \
      --env-file .env --out /tmp/soda3_productive_live_smoke.jsonl

Lo que este runner NO hace, a propósito:

* no toca ``app/``, ni builders, ni la política H4, ni los defaults;
* no escribe ``.env`` ni persiste ``SECOPPAL_SOCRATA_TRANSPORT`` (la variable
  se pone en el proceso y se restaura al salir — el gate de rollback depende
  de que no quede estado residual);
* no amplía ningún timeout para hacer pasar un caso;
* no registra App Token, headers ni secretos en el JSONL.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.core.secop_client import SecopClient  # noqa: E402
from app.core.socrata_transport import (  # noqa: E402
    DEFAULT_ORDERING_SPECIFIER,
    DEFAULT_TRANSPORT_KIND,
    TRANSPORT_ENV_VAR,
    Soda3OrderingSpecifier,
)

#: Manifiesto congelado del canario runtime V1 — la fuente de los SoQL exactos.
FROZEN_MANIFEST = Path("/tmp/soda3_runtime_canary_manifest_frozen_v3.json")

#: Fecha de referencia del corpus congelado: fija el SoQL derivado de fechas.
REFERENCE_DATE = date(2026, 8, 27)

# ── Casos ────────────────────────────────────────────────────────────────────
# A y B atraviesan el workflow real; C y D usan el SecopClient que ese mismo
# workflow construyó, con SoQL congelado, porque son formas remotas que el
# runtime emite por dentro y que aquí interesa comparar bit a bit.

CASE_A_QUERY = "licitaciones publicas de obra en atlantico"      # RT-TAB-006
CASE_B_QUERY = "contratos de interventoria tecnica de obra"      # RT-TAB-002
CASE_B_SEAM_CALL = "RT-TAB-002__call_002"   # el fallback H4 histórico
CASE_C_CALL = "RT-TAB-006__call_002"        # count del mismo caso A
CASE_D_CALL = "RT-ACTOR-001__call_001"      # aggregate escalar (Actor Profile)

#: Presupuesto del fallback H4 en producción (``FALLBACK_FETCH_SECONDS``).
H4_FALLBACK_TIMEOUT_S = 3


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _git(*args: str) -> str | None:
    result = subprocess.run(
        ["git", *args], cwd=ROOT, check=False, capture_output=True, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _error_class(exc: BaseException) -> str:
    names = [type(exc).__name__]
    cause = exc.__cause__ or exc.__context__
    while cause is not None and len(names) < 4:
        names.append(type(cause).__name__)
        cause = cause.__cause__ or cause.__context__
    return " <- ".join(names)


# ── Ledger ───────────────────────────────────────────────────────────────────


class Ledger:
    """Acumula eventos y los vuelca a JSONL. Sin secretos, por construcción."""

    def __init__(self, out: Path) -> None:
        self.out = out
        self.events: list[dict[str, Any]] = []
        self.arm = "preflight"
        self.case = "preflight"

    def emit(self, kind: str, **payload: Any) -> dict[str, Any]:
        event = {
            "ts": _now(), "kind": kind, "arm": self.arm, "case": self.case,
            **payload,
        }
        self.events.append(event)
        return event

    def flush(self) -> None:
        with self.out.open("w", encoding="utf-8") as handle:
            for event in self.events:
                handle.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")

    @contextmanager
    def scope(self, arm: str, case: str):
        previous = (self.arm, self.case)
        self.arm, self.case = arm, case
        try:
            yield
        finally:
            self.arm, self.case = previous

    def ops(self, arm: str, case: str, operation: str | None = None) -> list[dict]:
        return [
            e for e in self.events
            if e["kind"] == "socrata_op" and e["arm"] == arm and e["case"] == case
            and (operation is None or e["operation"] == operation)
        ]

    def http(self, arm: str, case: str) -> list[dict]:
        return [
            e for e in self.events
            if e["kind"] == "soda3_http" and e["arm"] == arm and e["case"] == case
        ]


# ── Observación del alambre SODA3 ────────────────────────────────────────────


class RecordingHttpClient:
    """Envuelve el ``httpx.Client`` que el transporte productivo ya construyó.

    Es el único punto donde se puede leer el ``orderingSpecifier`` **efectivo**:
    el que viaja en el body del POST, no el que creemos haber pasado. No se
    registran headers — ahí vive el App Token.
    """

    def __init__(self, inner: Any, ledger: Ledger) -> None:
        self._inner = inner
        self._ledger = ledger

    def post(self, url: str, *, json: Any = None, timeout: Any = None, **kwargs: Any):
        payload = json or {}
        soql = str(payload.get("query", ""))
        started = time.perf_counter()
        status: int | None = None
        error: str | None = None
        try:
            response = self._inner.post(url, json=json, timeout=timeout, **kwargs)
            status = response.status_code
        except BaseException as exc:
            error = _error_class(exc)
            raise
        finally:
            elapsed = round((time.perf_counter() - started) * 1000, 3)
            row_count = None
            raw_sample: list[dict] | None = None
            if status == 200:
                try:
                    body = response.json()
                except ValueError:
                    body = None
                if isinstance(body, list):
                    row_count = len(body)
                    raw_sample = [
                        {
                            key: {"type": type(value).__name__, "repr": str(value)[:80]}
                            for key, value in row.items()
                            if key in _RAW_SAMPLE_FIELDS
                        }
                        for row in body[:2] if isinstance(row, dict)
                    ]
            self._ledger.emit(
                "soda3_http",
                url=url,
                ordering_specifier=payload.get("orderingSpecifier"),
                include_synthetic=payload.get("includeSynthetic"),
                payload_timeout=payload.get("timeout"),
                request_timeout=timeout,
                soql_sha256=_sha256(soql),
                soql_length=len(soql),
                http_status=status,
                elapsed_ms=elapsed,
                row_count=row_count,
                raw_sample=raw_sample,
                error_class=error,
            )
        return response

    def close(self) -> None:
        self._inner.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


#: Campos cuya forma cruda interesa para probar que la normalización corrió.
_RAW_SAMPLE_FIELDS = frozenset({
    "id_contrato", "id_del_proceso", "valor_del_contrato", "precio_base",
    "urlproceso", "total_value", "total_count", "count",
})


class RecordingSecopClient(SecopClient):
    """``SecopClient`` real que además anota cada operación.

    Subclase y no proxy: ``_client_for`` y ``_query_with_ordering`` deciden por
    ``isinstance``, y ``derived_for_timeout`` mira ``self.__dict__``. Cualquier
    otra forma cambiaría el camino que se quiere observar.

    Reconstruye el cliente con los MISMOS parámetros del delegado —incluido
    ``transport``— para no degradar SODA3 a clásico ni hornear un timeout
    ajeno en el brazo clásico.
    """

    def __init__(self, delegate: SecopClient, ledger: Ledger) -> None:
        super().__init__(
            domain=delegate.domain,
            app_token=delegate.app_token,
            timeout=delegate.timeout,
            client=delegate._injected_client,
            max_retries=delegate.max_retries,
            transport=delegate._explicit_transport,
        )
        self._ledger = ledger

    def _wrap(self, other: Any) -> Any:
        if isinstance(other, RecordingSecopClient) or not isinstance(other, SecopClient):
            return other
        return RecordingSecopClient(other, self._ledger)

    def _record(
        self,
        operation: str,
        dataset_id: str,
        soql: str,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> Any:
        started = time.perf_counter()
        status, error, result = "ok", None, None
        try:
            result = getattr(SecopClient, operation)(
                self, dataset_id, soql, ordering_specifier=ordering_specifier,
            )
            return result
        except BaseException as exc:
            status, error = "error", _error_class(exc)
            raise
        finally:
            rows = result if isinstance(result, list) else None
            self._ledger.emit(
                "socrata_op",
                operation=operation,
                dataset_id=dataset_id,
                transport_name=self.transport_name,
                client_timeout_s=self.timeout,
                ordering_specifier=Soda3OrderingSpecifier(ordering_specifier).value,
                soql=soql,
                soql_sha256=_sha256(soql),
                soql_length=len(soql),
                status=status,
                error_class=error,
                elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
                row_count=(len(rows) if rows is not None else None),
                scalar=(result if isinstance(result, int) else None),
                ids=(_ids(dataset_id, rows) if rows else None),
                normalized_sample=(_normalized_sample(rows) if rows else None),
            )

    def query(
        self, dataset_id: str, soql_query: str, *,
        ordering_specifier: Soda3OrderingSpecifier = DEFAULT_ORDERING_SPECIFIER,
    ) -> list[dict]:
        return self._record("query", dataset_id, soql_query, ordering_specifier)

    def count(
        self, dataset_id: str, soql_count_query: str, *,
        ordering_specifier: Soda3OrderingSpecifier = DEFAULT_ORDERING_SPECIFIER,
    ) -> int:
        return self._record("count", dataset_id, soql_count_query, ordering_specifier)

    def aggregate(
        self, dataset_id: str, soql: str, *,
        ordering_specifier: Soda3OrderingSpecifier = DEFAULT_ORDERING_SPECIFIER,
    ) -> list[dict]:
        return self._record("aggregate", dataset_id, soql, ordering_specifier)

    def derived_for_timeout(self, timeout_s: int) -> Any:
        return self._wrap(super().derived_for_timeout(timeout_s))

    def with_timeout(self, timeout: int) -> Any:
        return self._wrap(super().with_timeout(timeout))


def _ids(dataset_id: str, rows: list[dict]) -> list[str]:
    field = "id_del_proceso" if dataset_id == "p6dx-8zbt" else "id_contrato"
    return [str(row.get(field, "")) for row in rows]


def _normalized_sample(rows: list[dict]) -> list[dict]:
    """Muestra de la fila YA normalizada por ``SecopClient._normalize_row``."""
    return [
        {
            key: {"type": type(value).__name__, "repr": str(value)[:80]}
            for key, value in row.items()
            if key in _RAW_SAMPLE_FIELDS
        }
        for row in rows[:2]
    ]


# ── Wiring productivo ────────────────────────────────────────────────────────


@contextmanager
def transport_env(kind: str | None):
    """Pone (o quita) el flag en el proceso y lo restaura íntegro al salir.

    El gate ``ROLLBACK_TO_CLASSIC`` exige que apagar SODA3 no deje residuo:
    esa promesa se prueba restaurando el entorno, no editando ``.env``.
    """
    saved = os.environ.get(TRANSPORT_ENV_VAR)
    try:
        if kind is None:
            os.environ.pop(TRANSPORT_ENV_VAR, None)
        else:
            os.environ[TRANSPORT_ENV_VAR] = kind
        yield
    finally:
        if saved is None:
            os.environ.pop(TRANSPORT_ENV_VAR, None)
        else:
            os.environ[TRANSPORT_ENV_VAR] = saved


def build_workflow(overrides: dict, ledger: Ledger) -> Any:
    """Construye el workflow por el camino de producción y lo instrumenta."""
    from app.config import Settings, get_settings
    from app.core.conversation_store import ConversationStore
    from app.core.orchestrator import SecopalWorkflow

    get_settings.cache_clear()
    settings = Settings(**overrides["settings"])
    workflow = SecopalWorkflow(settings)
    selected = workflow.secop_client.transport_name
    workflow.secop_client = RecordingSecopClient(workflow.secop_client, ledger)
    workflow.conv_store = ConversationStore(base_path=overrides["conversation_dir"])

    if selected == "soda3":
        transport = workflow.secop_client._explicit_transport
        # ``http_client`` es un punto de extensión del propio transporte; se
        # envuelve el cliente que él mismo construyó (con sus headers), no uno
        # paralelo, para que el request siga siendo el de producción.
        transport._http_client = RecordingHttpClient(transport.http_client, ledger)

    ledger.emit(
        "workflow_built",
        configured_env=os.environ.get(TRANSPORT_ENV_VAR),
        settings_transport=settings.secoppal_socrata_transport,
        transport_name=selected,
        domain=settings.datos_gov_domain,
        client_timeout_s=settings.secop_timeout_seconds,
        app_token_present=bool(settings.secop_app_token),
    )
    return workflow, selected


def run_case(workflow: Any, ledger: Ledger, query: str, chat_id: str) -> dict:
    started = time.perf_counter()
    try:
        result = workflow.run_query(
            query, channel="streamlit", chat_id=chat_id, today=REFERENCE_DATE,
        )
        status, error = "ok", None
    except BaseException as exc:  # noqa: BLE001 - se registra y se sigue
        result, status, error = {}, "error", _error_class(exc)

    summary = {
        "runtime_status": status,
        "runtime_error_class": error,
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        "route_reason": result.get("route_reason"),
        "dataset_id": result.get("dataset_id"),
        "soql_query": result.get("soql_query"),
        "result_count": len(result.get("results") or []),
        "total_count": result.get("total_count"),
        "query_error": result.get("query_error"),
        "result_order_guarantee": result.get("result_order_guarantee"),
        "fallback_attempted": result.get("fallback_attempted"),
        "fallback_used": result.get("fallback_used"),
        "fallback_strategy": result.get("fallback_strategy"),
        "fallback_query_status": result.get("fallback_query_status"),
        "primary_query_status": result.get("primary_query_status"),
        "transformations_applied": result.get("transformations_applied"),
        "substantive_filters_preserved": result.get("substantive_filters_preserved"),
        "degraded": result.get("degraded"),
        "needs_clarification": result.get("needs_clarification"),
    }
    ledger.emit("workflow_case", user_query=query, **summary)
    return summary


# ── Casos directos sobre el SecopClient productivo ───────────────────────────


def frozen_call(manifest: dict, call_id: str) -> dict:
    for op in manifest["operations"]:
        if op["call_id"] == call_id:
            return op
    raise SystemExit(f"call_id ausente del manifiesto congelado: {call_id}")


def run_frozen_op(client: SecopClient, ledger: Ledger, call: dict,
                  ordering: Soda3OrderingSpecifier = DEFAULT_ORDERING_SPECIFIER,
                  timeout_s: int | None = None) -> Any:
    """Ejecuta un SoQL congelado por el SecopClient PRODUCTIVO."""
    target = client if timeout_s is None else client.derived_for_timeout(timeout_s)
    method = getattr(target, call["operation"])
    try:
        return method(call["dataset_id"], call["exact_soql"], ordering_specifier=ordering)
    except BaseException as exc:  # noqa: BLE001 - queda registrado en socrata_op
        ledger.emit("frozen_op_failed", call_id=call["call_id"], error_class=_error_class(exc))
        return None


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=None)
    parser.add_argument("--manifest", type=Path, default=FROZEN_MANIFEST)
    parser.add_argument("--out", type=Path, default=Path("/tmp/soda3_productive_live_smoke.jsonl"))
    args = parser.parse_args()

    if args.env_file:
        load_dotenv(args.env_file, override=False)

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    ledger = Ledger(args.out)

    from scripts.audits.audit_isolation import isolated_runtime_stores

    ledger.emit(
        "preflight",
        git_head=_git("rev-parse", "HEAD"),
        git_branch=_git("branch", "--show-current"),
        tracked_dirty=bool(_git("status", "--short", "--untracked-files=no")),
        default_transport_kind=DEFAULT_TRANSPORT_KIND.value,
        transport_env_var=TRANSPORT_ENV_VAR,
        transport_env_value_at_start=os.environ.get(TRANSPORT_ENV_VAR),
        manifest=str(args.manifest),
        manifest_git_head=manifest.get("git_head"),
        corpus_sha256=manifest.get("corpus_sha256"),
    )

    case_c = frozen_call(manifest, CASE_C_CALL)
    case_d = frozen_call(manifest, CASE_D_CALL)
    case_b_seam = frozen_call(manifest, CASE_B_SEAM_CALL)

    with isolated_runtime_stores(prefix="soda3_productive_smoke_") as stores:
        overrides = {
            "settings": stores.settings_overrides,
            "conversation_dir": stores.conversation_dir,
        }

        # ── Brazo clásico ────────────────────────────────────────────────
        with transport_env(None):
            ledger.arm, ledger.case = "classic", "build"
            wf_classic, classic_name = build_workflow(overrides, ledger)
            with ledger.scope("classic", "A"):
                run_case(wf_classic, ledger, CASE_A_QUERY, "smoke-classic-A")
            with ledger.scope("classic", "C"):
                run_frozen_op(wf_classic.secop_client, ledger, case_c)
            with ledger.scope("classic", "D"):
                run_frozen_op(wf_classic.secop_client, ledger, case_d)

        # ── Brazo SODA3 ──────────────────────────────────────────────────
        with transport_env("soda3"):
            ledger.arm, ledger.case = "soda3", "build"
            wf_soda3, soda3_name = build_workflow(overrides, ledger)
            with ledger.scope("soda3", "A"):
                run_case(wf_soda3, ledger, CASE_A_QUERY, "smoke-soda3-A")
            with ledger.scope("soda3", "B"):
                case_b = run_case(wf_soda3, ledger, CASE_B_QUERY, "smoke-soda3-B")
            if not case_b.get("fallback_used"):
                # El primary no agotó: la ruta H4 no se recorrió sola. Se prueba
                # la COSTURA — que NO_PRESERVE llegue al alambre desde el cliente
                # productivo — replicando el fallback congelado tal cual.
                with ledger.scope("soda3", "B_seam"):
                    run_frozen_op(
                        wf_soda3.secop_client, ledger, case_b_seam,
                        ordering=Soda3OrderingSpecifier.NO_PRESERVE,
                        timeout_s=H4_FALLBACK_TIMEOUT_S,
                    )
            with ledger.scope("soda3", "C"):
                run_frozen_op(wf_soda3.secop_client, ledger, case_c)
            with ledger.scope("soda3", "D"):
                run_frozen_op(wf_soda3.secop_client, ledger, case_d)

        # ── Rollback ─────────────────────────────────────────────────────
        with transport_env(None):
            ledger.arm, ledger.case = "rollback", "build"
            wf_rollback, rollback_name = build_workflow(overrides, ledger)
            with ledger.scope("rollback", "A"):
                run_case(wf_rollback, ledger, CASE_A_QUERY, "smoke-rollback-A")

    ledger.arm, ledger.case = "summary", "summary"
    ledger.emit(
        "arms",
        classic=classic_name, soda3=soda3_name, rollback=rollback_name,
        transport_env_value_at_end=os.environ.get(TRANSPORT_ENV_VAR),
    )
    ledger.flush()
    print(f"eventos: {len(ledger.events)} → {args.out}")
    print(f"arms: classic={classic_name} soda3={soda3_name} rollback={rollback_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

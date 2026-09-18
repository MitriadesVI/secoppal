"""ZEUS recheck — ataques al mecanismo de integridad introducido en la remediación."""
from __future__ import annotations

import argparse
import json

from scripts.audits import audit_gazetteer_source_bindings_v1 as audit
from tests.test_gazetteer_source_binding_audit import FakeSource, canonical


def _args(tmp_path, **kw):
    base = dict(workers=1, limit=0, only=None, start_after=None, recheck_existing=False,
                summary_only=False, transport="soda3", timeout=1.0, retries=0, resume=False)
    base.update(kw)
    return argparse.Namespace(output=str(tmp_path), **base)


def test_z16_deleting_the_hash_index_must_not_launder_altered_evidence(tmp_path, monkeypatch):
    """Borrar `records.sha256` no puede convertir evidencia alterada en `ok`.

    El propio docstring de `verify_records_integrity` dice que regenerar el
    índice sobre evidencia alterada lavaría la alteración; eso vale igual cuando
    el índice está AUSENTE, que es un `rm` de distancia.
    """
    monkeypatch.setattr(audit, "build_inventory", lambda authorities: [canonical("A")])
    audit.run(_args(tmp_path), source_factory=lambda s, a: FakeSource(like={}))

    record_path = next((tmp_path / "records").glob("*.json"))
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    payload["record"]["status"] = "EXACT_BINDABLE"          # evidencia alterada
    record_path.write_text(json.dumps(payload), encoding="utf-8")

    # con el índice presente, la alteración se detecta (control)
    caught = audit.run(_args(tmp_path, summary_only=True),
                       source_factory=lambda s, a: FakeSource(like={}))
    assert caught != 0, "control: con índice, la alteración debe bloquear"

    (tmp_path / "records.sha256").unlink()                   # el atacante borra el índice
    code = audit.run(_args(tmp_path, summary_only=True),
                     source_factory=lambda s, a: FakeSource(like={}))
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert code != 0 or not summary["report_status"]["ok"], (
        "borrar records.sha256 regeneró el índice desde la evidencia alterada y "
        "el reporte quedó ok=true: la ausencia del índice se convirtió en éxito"
    )

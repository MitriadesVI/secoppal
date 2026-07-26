#!/usr/bin/env python3
"""Analizador READ-ONLY de la compuerta de producto M3C-C → M3C-D.

Responde una sola pregunta contractual:

    ¿La frecuencia REAL de aclaraciones bloqueadas de modalidad justifica
    construir el ciclo persistente ``PendingModalityClarification``?

No importa ``app/``, no abre red, no escribe nada y no toca ``data/``. Consume un
log JSONL de eventos AGREGADOS de turno (uno por turno de follow-up con señal de
modalidad) cuyo esquema V1 queda congelado abajo, y emite un reporte de códigos
y conteos. Nunca emite ``procedure_ids``, texto de usuario, ``chat_id`` ni ningún
valor tomado del registro: el reporte se arma exclusivamente con contadores y con
códigos de una lista blanca fija (lo desconocido se agrupa como ``unknown_code``).

Esquema del registro (v1, congelado por M3C-CG-I)::

    {
      "schema_version": 1,
      "event_id": "<uuid estable, obligatorio>",
      "recorded_at": "<ISO-8601 UTC>",
      "source": "runtime",             # solo "runtime" es evidencia de producto
      "is_followup": true,
      "had_previous_modality_filter": false,
      "has_final_modality_filter": false,
      "has_modality_operation": false,
      "operation": "replace|add_include|...|clear|null",
      "instruction_status": "resolved|ambiguous|conflict|none|null",
      "resolution_status": "applied|no_op|blocked|null",
      "clarification_code": "<código del motor>|null",
      "blocked": false,
      "no_op": false,
      "clear": false,
      "implicit_replace": false
    }

Definición congelada de turno válido (también §3 del contrato): cuenta en
``modality_followup_turns_total`` el registro que (1) es un turno de follow-up
real (``is_followup=true``, ``source="runtime"``) y (2) trae señal de modalidad
—``has_modality_operation=true``, ``had_previous_modality_filter=true``,
``has_final_modality_filter=true`` o ``blocked=true``. Todo lo demás se excluye y
se reporta por separado. Ninguna muestra sintética (corpus, tests, fixtures) puede
autorizar M3C-D: los registros con ``source`` distinto de ``runtime`` se excluyen
del conteo.

Uso::

    python scripts/analyze_modality_followup_gate.py --input <telemetria.jsonl>
    python scripts/analyze_modality_followup_gate.py --input <f> --format md
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

SCHEMA_VERSION = 1
SAMPLE_SOURCE = "modality_followup_telemetry_v1"

# Solo el canal productivo es evidencia de producto (§3).
REAL_SOURCES = frozenset({"runtime"})

VALID_RESOLUTION_STATUS = frozenset({"applied", "no_op", "blocked"})

VALID_OPERATIONS = frozenset({
    "replace", "add_include", "add_exclude", "remove_member",
    "remove_exclude", "restore_include", "clear",
})

VALID_INSTRUCTION_STATUS = frozenset({"none", "resolved", "ambiguous", "conflict"})

# Bloqueos atribuibles a un estado JURÍDICO de M2 (§2: no se mezclan con
# target_missing conversacional).
CODE_TO_M2_STATUS = {
    "partial_target": "PARTIAL",
    "ambiguous_target": "AMBIGUOUS",
    "conflicting_target": "CONFLICT",
    "non_searchable_target": "NOT_SEARCHABLE",
    "dataset_inapplicable_target": "DATASET_INAPPLICABLE",
}

M2_STATUSES = ("PARTIAL", "AMBIGUOUS", "CONFLICT", "NOT_SEARCHABLE", "DATASET_INAPPLICABLE")

# Bloqueos de naturaleza CONVERSACIONAL (el turno siguiente puede resolverlos).
CODE_TARGET_MISSING = "target_missing"
CODE_CONTROL_CONFLICT = "control_conflict"

# Lista blanca de códigos que el motor puro puede emitir. Un código fuera de esta
# lista jamás se copia a la salida: se agrupa como "unknown_code".
KNOWN_CODES = frozenset(CODE_TO_M2_STATUS) | {
    CODE_TARGET_MISSING,
    CODE_CONTROL_CONFLICT,
    "ambiguous_clear_or_null_value",
    "ambiguous_control",
    "mixed_target_polarity",
    "unknown_procedure_id",
}
UNKNOWN_CODE = "unknown_code"

# Bloqueos que un ciclo persistente PendingModalityClarification PODRÍA resolver
# con la respuesta del turno siguiente (subconjunto de blocked_total; se reporta
# aparte y NO participa en la aritmética de la compuerta).
CLARIFICATION_CANDIDATE_CODES = frozenset({
    CODE_TARGET_MISSING,
    CODE_CONTROL_CONFLICT,
    "ambiguous_clear_or_null_value",
    "ambiguous_control",
    "ambiguous_target",
    "partial_target",
    "mixed_target_polarity",
})

# Compuerta contractual cerrada (§2).
GATE_MINIMUM_SAMPLE = 100
GATE_BLOCKED_ABSOLUTE = 10
GATE_BLOCKED_RATE = 0.05
GATE_BLOCKED_RATE_MINIMUM = 5


class _Counters:
    """Contadores planos; nunca guarda valores tomados del registro."""

    def __init__(self) -> None:
        self.total = 0
        self.blocked = 0
        self.by_status = {status: 0 for status in M2_STATUSES}
        self.by_code: dict[str, int] = {}
        self.target_missing = 0
        self.control_conflict = 0
        self.noop = 0
        self.clear = 0
        self.candidates = 0
        self.duplicates = 0
        self.invalid = 0
        self.non_followup = 0
        self.no_signal = 0
        self.non_real_source = 0
        self.timestamps: list[str] = []


def _is_valid_uuid(value: object) -> bool:
    """Valida básica de UUID sin exponer datos."""
    if type(value) is not str or not value:
        return False
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def _canonical_utc_timestamp(value: object) -> str | None:
    """Valida ISO-8601 con zona UTC y devuelve representación canónica.

    Rechaza strings arbitrarios para evitar que contenido externo (texto de
    usuario, rutas, etc.) se reproduzca en ``sample_start``/``sample_end``.
    """
    if type(value) is not str or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        return None
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _is_structurally_valid(record: object) -> tuple[bool, str | None]:
    """Validación estructural hostil: sin coerciones, tipos exactos.

    Devuelve ``(valid, canonical_timestamp)``.  Si ``valid`` es ``False`` el
    registro se cuenta como inválido.  El timestamp canónico nunca se copia
    directamente desde la entrada.
    """
    if not isinstance(record, dict):
        return False, None
    if type(record.get("schema_version")) is not int or record["schema_version"] != SCHEMA_VERSION:
        return False, None
    if not _is_valid_uuid(record.get("event_id")):
        return False, None
    canonical_ts = _canonical_utc_timestamp(record.get("recorded_at"))
    if canonical_ts is None:
        return False, None
    if type(record.get("source")) is not str:
        return False, None
    if type(record.get("is_followup")) is not bool:
        return False, None
    if type(record.get("had_previous_modality_filter")) is not bool:
        return False, None
    if type(record.get("has_final_modality_filter")) is not bool:
        return False, None
    if type(record.get("has_modality_operation")) is not bool:
        return False, None
    if type(record.get("blocked")) is not bool:
        return False, None
    if type(record.get("no_op")) is not bool:
        return False, None
    if type(record.get("clear")) is not bool:
        return False, None
    if type(record.get("implicit_replace")) is not bool:
        return False, None

    operation = record.get("operation")
    if operation is not None and operation not in VALID_OPERATIONS:
        return False, None
    status = record.get("instruction_status")
    if status is not None and status not in VALID_INSTRUCTION_STATUS:
        return False, None
    resolution = record.get("resolution_status")
    if resolution is not None and resolution not in VALID_RESOLUTION_STATUS:
        return False, None
    code = record.get("clarification_code")
    if code is not None and type(code) is not str:
        return False, None
    return True, canonical_ts


def _has_modality_signal(record: dict) -> bool:
    return bool(
        record.get("has_modality_operation")
        or record.get("had_previous_modality_filter")
        or record.get("has_final_modality_filter")
        or record.get("blocked")
    )


def _safe_code(record: dict) -> str:
    """Código normalizado contra la lista blanca (nunca devuelve texto externo)."""
    code = record.get("clarification_code")
    return code if code in KNOWN_CODES else UNKNOWN_CODE


def _is_no_op(record: dict) -> bool:
    """Un turno resuelto sin cambios efectivos: señal de modalidad presente."""
    return record.get("no_op") is True


def _is_clear(record: dict) -> bool:
    """Una operación CLEAR que se aplicó o no-op."""
    return record.get("operation") == "clear" and record.get("resolution_status") != "blocked"


def iter_records(lines) -> "list[tuple[bool, object]]":
    """Parsea líneas JSONL. Devuelve ``(ok, objeto)``; ``ok=False`` = malformada."""
    parsed: list[tuple[bool, object]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            parsed.append((True, json.loads(line)))
        except (ValueError, UnicodeDecodeError):
            parsed.append((False, None))
    return parsed


def aggregate(lines, *, sample_provenance: str = "unknown", sample_source: str = SAMPLE_SOURCE) -> dict:
    """Agrega el log a un reporte determinista (misma entrada ⇒ misma salida)."""
    counters = _Counters()
    seen_ids: set[str] = set()

    for ok, raw in iter_records(lines):
        if not ok:
            counters.invalid += 1
            continue
        record = cast(dict[str, Any], raw)
        valid, canonical_ts = _is_structurally_valid(record)
        if not valid or canonical_ts is None:
            counters.invalid += 1
            continue
        # Deduplicación por identificador estable ANTES de cualquier conteo: un
        # registro repetido no puede inflar ningún contador. Un registro sin
        # event_id no llega hasta aquí (se rechaza como inválido): no se puede
        # probar que sea distinto de otro, y esa es la lectura conservadora.
        event_id = record["event_id"]
        if event_id in seen_ids:
            counters.duplicates += 1
            continue
        seen_ids.add(event_id)

        if record["source"] not in REAL_SOURCES:
            counters.non_real_source += 1
            continue
        if not record["is_followup"]:
            counters.non_followup += 1
            continue
        if not _has_modality_signal(record):
            counters.no_signal += 1
            continue

        counters.total += 1
        counters.timestamps.append(canonical_ts)
        resolution = record.get("resolution_status")

        if _is_no_op(record):
            counters.noop += 1
        if _is_clear(record):
            counters.clear += 1

        if resolution != "blocked":
            continue

        counters.blocked += 1
        code = _safe_code(record)
        counters.by_code[code] = counters.by_code.get(code, 0) + 1
        if code in CODE_TO_M2_STATUS:
            counters.by_status[CODE_TO_M2_STATUS[code]] += 1
        elif code == CODE_TARGET_MISSING:
            counters.target_missing += 1
        elif code == CODE_CONTROL_CONFLICT:
            counters.control_conflict += 1
        if code in CLARIFICATION_CANDIDATE_CODES:
            counters.candidates += 1

    return _report(counters, sample_provenance=sample_provenance, sample_source=sample_source)


def _load_manifest(path: Path | None) -> tuple[str, str]:
    """Carga manifest explicito de procedencia. Defaults: unknown, source interno."""
    if path is None:
        return "unknown", SAMPLE_SOURCE
    try:
        with path.open(encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            return "unknown", SAMPLE_SOURCE
        provenance = str(data.get("sample_provenance", "unknown")).lower()
        if provenance not in ("real", "synthetic", "unknown"):
            provenance = "unknown"
        source = str(data.get("sample_source", SAMPLE_SOURCE))
        return provenance, source
    except (OSError, json.JSONDecodeError):
        return "unknown", SAMPLE_SOURCE


def _report(c: _Counters, *, sample_provenance: str, sample_source: str) -> dict:
    total = c.total
    blocked = c.blocked
    # La procedencia real se declara explícitamente; no se infiere del source.
    sample_is_real = sample_provenance == "real"
    blocked_rate = (blocked / total) if total else 0.0
    minimum_met = sample_is_real and total >= GATE_MINIMUM_SAMPLE
    threshold_met = (
        blocked >= GATE_BLOCKED_ABSOLUTE
        or (blocked_rate >= GATE_BLOCKED_RATE and blocked >= GATE_BLOCKED_RATE_MINIMUM)
    )
    authorized = minimum_met and threshold_met
    return {
        "schema_version": SCHEMA_VERSION,
        "sample_source": sample_source,
        "sample_provenance": sample_provenance,
        "sample_is_real": sample_is_real,
        "sample_start": min(c.timestamps) if c.timestamps else None,
        "sample_end": max(c.timestamps) if c.timestamps else None,
        "modality_followup_turns_total": total,
        "modality_followup_blocked_total": blocked,
        "blocked_rate": blocked_rate,
        "blocked_by_status": dict(c.by_status),
        "target_missing_total": c.target_missing,
        "control_conflict_total": c.control_conflict,
        "noop_total": c.noop,
        "clear_total": c.clear,
        "clarification_candidate_total": c.candidates,
        "duplicate_records_excluded": c.duplicates,
        "invalid_records_excluded": c.invalid,
        "gate_minimum_sample_met": minimum_met,
        "gate_threshold_met": threshold_met,
        "m3c_d_recommendation": "AUTHORIZED" if authorized else "NOT_AUTHORIZED",
        # Extensiones de auditoría (conteos y códigos de lista blanca, nada más).
        "blocked_by_code": dict(sorted(c.by_code.items())),
        "non_followup_records_excluded": c.non_followup,
        "no_modality_signal_records_excluded": c.no_signal,
        "non_real_source_records_excluded": c.non_real_source,
        "gate_minimum_required": GATE_MINIMUM_SAMPLE,
    }


def render_markdown(report: dict) -> str:
    verdict = (
        "GO" if report["m3c_d_recommendation"] == "AUTHORIZED"
        else ("INSUFFICIENT_SAMPLE" if not report["gate_minimum_sample_met"] else "NO-GO")
    )
    lines = [
        "# Compuerta de producto M3C-C → M3C-D",
        "",
        f"- sample_source: `{report['sample_source']}`",
        f"- sample_provenance: `{report['sample_provenance']}`",
        f"- sample_is_real: **{str(report['sample_is_real']).lower()}**",
        f"- ventana: `{report['sample_start']}` → `{report['sample_end']}`",
        "",
        "| métrica | valor |",
        "| --- | ---: |",
        f"| modality_followup_turns_total | {report['modality_followup_turns_total']} |",
        f"| mínimo requerido | {report['gate_minimum_required']} |",
        f"| modality_followup_blocked_total | {report['modality_followup_blocked_total']} |",
        f"| blocked_rate | {report['blocked_rate']:.4f} |",
        f"| target_missing_total (conversacional) | {report['target_missing_total']} |",
        f"| control_conflict_total | {report['control_conflict_total']} |",
        f"| noop_total | {report['noop_total']} |",
        f"| clear_total | {report['clear_total']} |",
        f"| clarification_candidate_total | {report['clarification_candidate_total']} |",
        f"| duplicate_records_excluded | {report['duplicate_records_excluded']} |",
        f"| invalid_records_excluded | {report['invalid_records_excluded']} |",
        "",
        "## Bloqueos por estado jurídico M2",
        "",
        "| status | n |",
        "| --- | ---: |",
    ]
    lines += [f"| {status} | {report['blocked_by_status'][status]} |" for status in M2_STATUSES]
    lines += [
        "",
        f"- gate_minimum_sample_met: **{str(report['gate_minimum_sample_met']).lower()}**",
        f"- gate_threshold_met: **{str(report['gate_threshold_met']).lower()}**",
        f"- m3c_d_recommendation: **{report['m3c_d_recommendation']}**",
        f"- veredicto: **{verdict}**",
        "",
    ]
    return "\n".join(lines)


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--input", required=True,
        help="JSONL de eventos agregados de turno (no se modifica).",
    )
    parser.add_argument("--format", choices=("json", "md", "both"), default="json")
    parser.add_argument(
        "--sample-provenance",
        choices=("real", "synthetic", "unknown"),
        default="unknown",
        help="Procedencia explicita de la muestra: real, synthetic o unknown.",
    )
    parser.add_argument(
        "--manifest",
        help="JSON con declaracion explicita de procedencia (sample_provenance, sample_source).",
    )
    args = parser.parse_args(argv)

    manifest_path = Path(args.manifest) if args.manifest else None
    manifest_provenance, manifest_source = _load_manifest(manifest_path)
    sample_provenance = manifest_provenance if manifest_provenance != "unknown" else args.sample_provenance
    sample_source = manifest_source

    path = Path(args.input)
    if path.exists():
        with path.open(encoding="utf-8") as handle:
            report = aggregate(
                handle,
                sample_provenance=sample_provenance,
                sample_source=sample_source,
            )
    else:
        # Ausencia de log => muestra vacia, no error: el estado esperado mientras
        # la instrumentacion no exista.
        report = aggregate([], sample_provenance=sample_provenance, sample_source=sample_source)

    if args.format in ("json", "both"):
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if args.format in ("md", "both"):
        print(render_markdown(report))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

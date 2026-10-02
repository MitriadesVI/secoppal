"""The one parser of the M2a contract text (U19: one question -> one authority -> everyone else consumes).

Pure text: it reads ``docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`` and imports nothing from ``app``. Its consumers:
the sealed extraction of §19 (``extract_contract_dependencies.py``), the Gold checker (annex A.3 vocabulary) and the
representability sweep (``tests/test_m2a_contract_r3_sweep.py``). A section or table that is not where the contract puts
it fails loudly (``ContractTextError``); it never yields an empty list.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md"

CODE = re.compile(r"`([A-Z_]+(?::<[a-zA-Z]+>)?)`")  # an annex A.3 code; a bare `<DIM>` in a comment is not one
_CASE = re.compile(r"\b(SF|R)(\d{3,4})(?:\s*[–-]\s*(?:SF|R)(\d{3,4}))?\b")
_CELL_SPLIT = re.compile(r"(?<!\\)\|")


class ContractTextError(ValueError):
    """The contract text does not have the shape its consumers read."""


def read(path: Path = CONTRACT) -> str:
    return path.read_text(encoding="utf-8")


def section(text: str, start: str, end: str | None = None) -> str:
    """The text after the first ``start`` and before the next ``end``; both must exist."""
    if start not in text:
        raise ContractTextError(f"missing section start: {start!r}")
    body = text.split(start, 1)[1]
    if end is not None:
        if end not in body:
            raise ContractTextError(f"missing section end: {end!r}")
        body = body.split(end, 1)[0]
    return body


def _cells(line: str) -> list[str]:
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
        raise ContractTextError(f"not a table row: {line!r}")
    return [cell.strip() for cell in _CELL_SPLIT.split(stripped[1:-1])]


def table(text: str, header: str) -> list[list[str]]:
    """Rows (cells) of the markdown table whose header line is exactly ``header``. The next line must be the separator;
    rows run until the first line that is not a table row. Every row has as many cells as the header."""
    lines = text.splitlines()
    starts = [i for i, line in enumerate(lines) if line.strip() == header]
    if len(starts) != 1:
        raise ContractTextError(f"table header found {len(starts)} times: {header!r}")
    width = len(_cells(header))
    at = starts[0] + 1
    if at >= len(lines) or not re.fullmatch(r"\|(\s*:?-+:?\s*\|)+", lines[at].strip()):
        raise ContractTextError(f"table without separator: {header!r}")
    rows = []
    for line in lines[at + 1:]:
        if not line.strip().startswith("|"):
            break
        cells = _cells(line)
        if len(cells) != width:
            raise ContractTextError(f"row with {len(cells)} cells, header has {width}: {line[:80]!r}")
        rows.append(cells)
    if not rows:
        raise ContractTextError(f"empty table: {header!r}")
    return rows


def codes(cell: str) -> list[str]:
    return CODE.findall(cell)


def case_ids(cell: str) -> list[str]:
    """Case ids cited in a cell, ranges (``SF065–SF067``) expanded, in order of citation."""
    out: list[str] = []
    for prefix, first, last in _CASE.findall(cell):
        width = len(first)
        stop = int(last) if last else int(first)
        if stop < int(first):
            raise ContractTextError(f"inverted case range in {cell[:60]!r}")
        out += [f"{prefix}{n:0{width}d}" for n in range(int(first), stop + 1)]
    return out


def leading_count(cell: str) -> int | None:
    match = re.match(r"\s*(\d+)\b", cell)
    return int(match.group(1)) if match else None


# ---- extractors of the contract's closed lists (each one reads ONE place of the contract) ------------------------------
def annex_a3(text: str) -> dict[str, list[str]]:
    """Annex A.3: ``{disposition: [codes]}``; a parametric code keeps its placeholder (``UNRESOLVED_VALUE:<DIM>``)."""
    body = section(text, "**A.3 Códigos de razón:**", "Retirados:")
    return {codes(row[0])[0]: codes(row[1]) for row in table(body, "| Disposición | Códigos |")}


def annex_a1(text: str) -> list[str]:
    return re.findall(r"`([^`]+)`", section(text, "**A.1 `TurnScope`:**", "**A.2"))


def refusal_codes(text: str) -> list[str]:
    """§6: the closed ``DeltaRefusal`` list as the paragraph writes it."""
    block = re.search(r"Códigos de rechazo \(`DeltaRefusal`, cerrados\):(.*?)\.\s", text, re.S)
    if block is None:
        raise ContractTextError("missing the §6 DeltaRefusal list")
    return re.findall(r"`([^`]+)`", block.group(1))


def refusal_table(text: str) -> dict[str, list[str]]:
    """§6 (R3-2): ``DeltaRefusal`` -> the A.3 codes it publishes; «cualquier otro» publishes each listed code itself."""
    out: dict[str, list[str]] = {}
    for left, right in table(text, "| `DeltaRefusal` | `CLARIFY` (A.3) |"):
        sources, targets = codes(left), codes(right)
        if sources:
            out.update({source: targets for source in sources})
        else:
            out.update({target: [target] for target in targets})
    return out


def row_4b_motives(text: str) -> dict[str, str]:
    """§7.1 fila 4b (D-6): ``{motivo: dueño}`` of the closed list."""
    out = {}
    for cells in table(text, "| Motivo (cerrado) | P… | Dueño |"):
        motive = re.fullmatch(r"`([A-Z_]+)`", cells[0])
        if motive is None:
            raise ContractTextError(f"malformed motive cell {cells[0]!r}")
        out[motive.group(1)] = cells[2]
    return out


def dim_parameter(text: str) -> tuple[set[str], set[str]]:
    """A.3 «Parámetros (R3)», bullet ``<DIM>`` (R3-3): ``(named, excluded)``. The bullet says «las once dimensiones de
    ranura … y UNIVERSE» and «Nunca una dimensión que no es ranura (…)»: the domain of ``<DIM>`` is every dimension
    minus ``excluded``, plus ``named`` (UNIVERSE)."""
    params = section(text, "Parámetros (R3):").split("\n\n", 2)[1]
    bullet = next((item for item in params.split("\n- ") if item.lstrip("- ").startswith("`<DIM>`")), None)
    if bullet is None or "Nunca" not in bullet or "once dimensiones de ranura" not in bullet:
        raise ContractTextError("A.3: the <DIM> parameter bullet does not have its shape")
    before, after = bullet.split("Nunca", 1)
    named = set(re.findall(r"`([A-Z_]+)`", before)) - {"TOPIC", "CARDINALITY"}  # «(`TOPIC` … `CARDINALITY`)» is a span
    excluded = set(re.findall(r"`([A-Z_]+)`", after.split(".", 1)[0]))
    return named, excluded

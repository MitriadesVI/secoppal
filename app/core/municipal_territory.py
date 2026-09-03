"""DIVIPOLA municipal geography, independent of BuyerUnit identity and NIT."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re

from app.core.entity_types import name_tokens
from app.core.query_text_consumption import mask_consumed_spans
from app.data.departamentos import DEPARTAMENTOS
from app.utils.money import normalize_text

PATH = Path(__file__).resolve().parents[1] / 'data/municipal_territories_v1.json'
VERSION = 'DANE_DIVIPOLA_MGN_2025_M0_2026-09-03'


def key(text: str) -> str:
    return ' '.join(name_tokens(text))


@dataclass(frozen=True)
class MunicipalTerritory:
    divipola: str
    municipality: str
    department: str
    aliases: tuple[str, ...]

    @property
    def department_value(self) -> str:
        # DANE vs SECOP departmental nomenclature, not municipal buyer inference.
        official = DEPARTAMENTOS.get(normalize_text(self.department))
        if official:
            return official
        return {'11': 'Distrito Capital de Bogotá',
                '88': 'San Andrés, Providencia y Santa Catalina'}[self.divipola[:2]]

    def to_dict(self) -> dict:
        return dict(divipola=self.divipola, municipality=self.municipality,
                    department=self.department, department_value=self.department_value)


class MunicipalTerritoryRegistry:
    def __init__(self, path: Path = PATH):
        data = json.loads(path.read_bytes())
        if data.get('schema_version') != 1 or data.get('version') != VERSION:
            raise ValueError('Unsupported municipal territory registry')
        self.provenance = data['provenance']
        if (self.provenance.get('classification') != 'MPIO_TIPO == MUNICIPIO'
                or not re.fullmatch(r'[a-f0-9]{64}', self.provenance.get('uncompressed_sha256', ''))):
            raise ValueError('Invalid DIVIPOLA provenance')
        by_code = {}
        by_alias = defaultdict(list)
        for row in data['territories']:
            territory = MunicipalTerritory(**{**row, 'aliases': tuple(row['aliases'])})
            if (not re.fullmatch(r'\d{5}', territory.divipola)
                    or territory.divipola in by_code
                    or not territory.municipality or not territory.department
                    or territory.aliases != (key(territory.municipality),)):
                raise ValueError('Invalid or duplicate municipal territory')
            if territory.department_value not in DEPARTAMENTOS.values():
                raise ValueError('Unknown municipal department')
            by_code[territory.divipola] = territory
            for alias in territory.aliases:
                by_alias[alias].append(territory)
        if len(by_code) != 1103:
            raise ValueError('DIVIPOLA municipal denominator must be 1103')
        self.by_code = by_code
        self.by_alias = {alias: tuple(rows) for alias, rows in by_alias.items()}
        self.max_words = max(len(a.split()) for a in self.by_alias)
        self.department_aliases = {key(a): v for a, v in DEPARTAMENTOS.items()}
        self.department_aliases.update({key(v): v for v in DEPARTAMENTOS.values()})
        for t in by_code.values():
            self.department_aliases[key(t.department)] = t.department_value
        self.max_department_words = max(len(a.split()) for a in self.department_aliases)

    def lookup(self, name: str, department: str | None = None) -> tuple[MunicipalTerritory, ...]:
        found = self.by_alias.get(key(name), ())
        if department is None:
            return found
        canonical = self.department_aliases.get(key(department))
        return tuple(t for t in found if t.department_value == canonical)

    def name_spans(self, text: str) -> list[tuple[int, int]]:
        words = list(re.finditer(r'\w+', text))
        result = []
        index = 0
        while index < len(words):
            for width in range(min(self.max_words, len(words) - index), 0, -1):
                start, end = words[index].start(), words[index + width - 1].end()
                if key(text[start:end]) in self.by_alias:
                    result.append((start, end))
                    index += width
                    break
            else:
                index += 1
        return result

    def consume(self, text: str, *, protected_spans=()) -> tuple[dict, str, list[str]]:
        """Recognize explicit `en X[, departamento Y]`; never scan naked buyers.

        Homonyms stay unresolved. Existing department fields can still represent
        a possible departmental reading, but the plan and builder must block it
        until the user selects the geographic type/department.
        """
        for marker in re.finditer(r'\ben\s+(?:(?:el\s+)?municipio\s+de\s+)?', text):
            if any(a < marker.start() < b for a, b in protected_spans):
                continue
            words = list(re.finditer(r'\w+', text[marker.end():]))
            for width in range(min(self.max_words, len(words)), 0, -1):
                end = marker.end() + words[width - 1].end()
                name = text[marker.end():end]
                candidates = self.lookup(name)
                if candidates:
                    break
            else:
                continue
            department = None
            qualifier = re.match(
                r'(?:\s*,?\s+(?:(?:en\s+)?(?:el\s+)?(?:departamento|depto\.?)(?:\s+de(?:l)?)?\s+)|\s*,\s*)',
                text[end:],
            )
            if qualifier:
                start = end + qualifier.end()
                tail = list(re.finditer(r'\w+', text[start:]))
                for width in range(min(self.max_department_words, len(tail)), 0, -1):
                    dep_end = start + tail[width - 1].end()
                    department = self.department_aliases.get(key(text[start:dep_end]))
                    if department:
                        end = dep_end
                        break
            filtered = self.lookup(name, department) if department else candidates
            department_reading = DEPARTAMENTOS.get(normalize_text(name))
            typed_municipal = 'municipio' in marker.group()
            ambiguous_type = bool(department_reading and not department and not typed_municipal)
            resolution = dict(version=VERSION, mention=name,
                              candidates=[t.to_dict() for t in candidates])
            params = {}
            if len(filtered) == 1 and not ambiguous_type:
                territory = filtered[0]
                resolution.update(status='resolved', **territory.to_dict())
                params.update(ciudad=territory.municipality.title(),
                              departamento_resolved=territory.department_value)
            else:
                resolution['status'] = 'ambiguous' if filtered else 'department_mismatch'
                resolution['clarification_question'] = (
                    f"¿En qué departamento está el municipio de {name}? "
                    + 'Opciones: ' + ', '.join(sorted({t.department_value for t in candidates}))
                    + (f'. Si te refieres al departamento de {department_reading}, indícalo.'
                       if ambiguous_type else '.')
                )
                # Preserve the established department parser surface, never
                # execute that provisional interpretation without clarification.
                if department_reading:
                    params['departamento_resolved'] = department_reading
            params['municipal_territory'] = resolution
            span = (marker.start(), end)
            return params, mask_consumed_spans(text, [span]), [text[span[0]:span[1]]]
        return {}, text, []


@lru_cache(maxsize=1)
def get_municipal_territories() -> MunicipalTerritoryRegistry:
    return MunicipalTerritoryRegistry()


def municipal_clarification(params: dict) -> str | None:
    resolution = params.get('municipal_territory') or {}
    if resolution.get('status') in {'ambiguous', 'department_mismatch'}:
        return resolution['clarification_question']
    return None

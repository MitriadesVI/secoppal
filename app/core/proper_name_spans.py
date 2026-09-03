"""Exact accredited phrase spans; recognition does not select a buyer owner.

This index may include ambiguous or scan-disabled names: protecting their text
from control extraction is not permission to resolve them as a BuyerUnit.
"""

from collections.abc import Iterable
import re

from app.utils.money import normalize_text


class ExactNameIndex:
    def __init__(self, names: Iterable[str]):
        self.names = frozenset(normalize_text(name) for name in names if name)
        self._widths: dict[str, int] = {}
        for name in self.names:
            words = name.split()
            if words:
                self._widths[words[0]] = max(self._widths.get(words[0], 0), len(words))

    def spans_in(self, text: str) -> list[tuple[int, int]]:
        """Longest non-overlapping exact phrases, offsets over the input text."""
        words = list(re.finditer(r'\S+', text))
        spans = []
        index = 0
        while index < len(words):
            start = words[index].start()
            width = self._widths.get(normalize_text(words[index].group()), 0)
            for size in range(min(width, len(words) - index), 0, -1):
                end = words[index + size - 1].end()
                if normalize_text(text[start:end]) in self.names:
                    spans.append((start, end))
                    index += size
                    break
            else:
                index += 1
        return spans

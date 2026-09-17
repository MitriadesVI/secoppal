"""La clase cerrada PRIVADA del residual credenciado (C4A.3, contenida en C4B-R1).

C4A.3 adjudicó un puñado de formas funcionales que el residual del carril
credenciado seguía reportando como material: interrogativos y cópula/auxiliar
(``quien``, ``es``…) y clíticos y preposiciones de clase cerrada (``le``, ``a``,
``su``…). Es exactamente el mismo hueco de clase cerrada que el vocabulario
general ya cubría para otras formas — no vocabulario de dominio, no verbos del
dominio, no nada nuevo.

Vivían en ``app/data/stopwords.py``, que es GLOBAL: todo cambio ahí mueve al
parser legacy, a ``source_occurrence`` y al corrector ortográfico. Integrarlas
ahí cambió decisiones que C4B no tenía por qué tocar (el test congelado de
ruido temático del parser dejó de pasar). Aquí quedan contenidas: sólo el
ensamblaje credenciado las une al vocabulario histórico.

Dos invariantes:

- Ningún consumidor legacy importa este módulo (lo comprueba
  ``tests/test_residual_verifier_contract.py``).
- El residual credenciado conserva EXACTAMENTE su comportamiento: la unión
  ``STOPWORDS | RESIDUAL_FUNCTION_WORDS`` es la misma clase que C4A.3 adjudicó.
"""

from __future__ import annotations

# Las formas que C4A.3 adjudicó, ya normalizadas (sin tildes, minúsculas).
RESIDUAL_FUNCTION_WORDS = frozenset({
    # Interrogativos generales y cópula/auxiliar.
    "quien", "quienes", "es", "son", "ha",
    # Clíticos y preposiciones de clase cerrada.
    "a", "al", "ante", "cabe", "contra", "desde", "durante", "hacia", "hasta",
    "le", "les", "lo", "me", "mediante", "mi", "mis", "nos",
    "su", "sus", "te", "tras", "tu", "tus",
})


def residual_stopwords(historical: frozenset[str] | set[str]) -> frozenset[str]:
    """El vocabulario del residual: la clase histórica más la privada del carril."""
    return frozenset(historical) | RESIDUAL_FUNCTION_WORDS


__all__ = ["RESIDUAL_FUNCTION_WORDS", "residual_stopwords"]

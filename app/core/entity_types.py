"""
Entity Types — semantic entity rewriting for SECOPPAL.

Maps common morphologic/syntactic variants of public entity names
to canonical forms that match the gazetteer (aliases_db.json).

Deterministic, no LLM. Runs BEFORE entity_resolver so the rewritten
forms feed into the existing alias→exact→fuzzy→LIKE pipeline.
"""

import re
import unicodedata
from typing import Callable, NamedTuple

# ── Autoridad única del límite de captura ────────────────────────────────
# Las siete familias de reescritura compartían la MISMA cadena de lookahead,
# copiada siete veces. Copiado siete veces estaba también el defecto: `de|del`
# figuraba como frontera dura, y `de|del` es el conector interno de casi todo
# nombre institucional colombiano —Norte DE Santander, Valle DEL Cauca, Villa
# DE Leyva, San Juan DE Pasto, Universidad Nacional DE Colombia, Hospital San
# Rafael DE Fusagasugá—. La gramática cortaba el nombre por su propia juntura.
#
# `de|del` sale de la frontera dura y pasa a ser un PUNTO DE CORTE CANDIDATO
# (ver ``_candidate_lengths``): la captura llega hasta el siguiente conector
# que sí abre otra cláusula, y quien decide dónde termina el nombre es el
# resolver contra el gazetteer, no el regex.
#
# Una cláusula empieza con un TOKEN independiente: tanto el espacio anterior
# como el límite posterior importan (Susacón, Belén, Valparaíso, Carmen).
CAPTURE_BOUNDARY = r"(?=\s+(?:para\b|donde\b|por\b|con\b|en\b|sobre\b)|\s*$)"

# Conectores internos de nombre propio. No terminan un nombre: lo articulan.
_NAME_CONNECTORS = frozenset({"de", "del"})


class EntityCandidate(NamedTuple):
    """Un intento de nombre canónico con su span y su nivel de exigencia.

    Sigue siendo indexable por posición —``c[0]`` es el nombre canónico— para
    los consumidores que ya la tratan como tupla.
    """

    canonical: str
    consumed_span: str
    rewrite_type: str
    # True cuando el span se obtuvo cortando un grupo ``de|del``: es un
    # FRAGMENTO de un nombre compuesto. Un fragmento puede acreditarse por
    # alias exacto, nunca por parecido — así "Villa" (de "Villa de Leyva")
    # deja de resolver a VILLAVICENCIO y "San" (de "San Juan de Pasto") a
    # MUNICIPIO DE SAN JOSE DE CUCUTA.
    requires_deterministic: bool = False
    # True para las familias que nombran una institución sectorial (hospital,
    # ESE, universidad). Ninguna de ellas puede ser un comprador raíz
    # territorial: un hospital no se llama "DEPARTAMENTO DE SANTANDER". Sin
    # esta restricción, al capturar el nombre completo el fuzzy sin puerta de
    # estas familias contesta una pregunta por un hospital con el departamento.
    rejects_territorial_root: bool = False


# Each entry: (compiled_regex, replacer_fn)
# replacer_fn receives the captured entity name (str) and returns list[str] of
# canonical candidates to try against the gazetteer, in priority order.
ENTITY_REWRITES: list[tuple[re.Pattern, Callable]] = [
    # ── Alcaldía variants ──
    # El límite de captura es CAPTURE_BOUNDARY, compartido por las siete
    # familias. `de|del` NO está: es conector de nombre, no frontera.
    (
        re.compile(
            r"\balcald[ií]a\s+(?:municipal\s+)?de\s+(.+?)" + CAPTURE_BOUNDARY,
            re.IGNORECASE,
        ),
        lambda x: [
            f"Municipio de {x}",
            f"Alcaldía de {x}",
            f"Alcaldía Municipal de {x}",
        ],
    ),
    # ── Gobernación variants ──
    # ``de(?:l)?`` — INV-NO-SILENT-SEMANTIC-FALLBACK (ADR-069). Con solo "de",
    # "gobernación DEL Vichada" no generaba ningún candidato de entidad: el span
    # sobrevivía hasta el scan de departamento y una pregunta por el COMPRADOR
    # se contestaba con TODO el territorio. "del" es la forma natural para la
    # mitad de los departamentos (del Atlántico, del Huila, del Valle), así que
    # la asimetría no era un borde raro. ``generic_buyer._NAME_QUALIFIER`` ya
    # trataba ambas formas como comprador calificado; esto lo alinea.
    (
        re.compile(
            r"\bgobernaci[oó]n\s+de(?:l)?\s+(.+?)" + CAPTURE_BOUNDARY,
            re.IGNORECASE,
        ),
        lambda x: [
            f"Departamento de {x}",
            f"Gobernación de {x}",
        ],
    ),
    # ── Gobierno departamental variants ──
    # Es la forma física real del comprador Tolima, pero la frase sólo expresa
    # buyer intent cuando incluye territorio.  Los candidatos entran al mismo
    # guard de evidencia raíz que la gramática de gobernación.
    (
        re.compile(
            r"\bgobierno\s+departamental\s+de(?:l)?\s+(.+?)" + CAPTURE_BOUNDARY,
            re.IGNORECASE,
        ),
        lambda x: [
            f"Departamento de {x}",
            f"Gobernación de {x}",
            f"Gobierno departamental de {x}",
        ],
    ),
    # ── ESE / Empresa Social del Estado ──
    # SECTORIAL: nunca es un comprador raíz territorial.
    (
        re.compile(
            r"\bese\s+(.+?)" + CAPTURE_BOUNDARY,
            re.IGNORECASE,
        ),
        lambda x: [
            f"Empresa Social del Estado {x}",
            f"ESE {x}",
        ],
    ),
    # ── Hospital variants ──
    # SECTORIAL: nunca es un comprador raíz territorial.
    (
        re.compile(
            r"\b(?:el\s+)?hospital\s+(.+?)" + CAPTURE_BOUNDARY,
            re.IGNORECASE,
        ),
        lambda x: [
            f"Hospital {x}",
            f"ESE Hospital {x}",
        ],
    ),
    # ── Universidad ──
    # SECTORIAL: nunca es un comprador raíz territorial.
    (
        re.compile(
            r"\b(?:la\s+)?universidad\s+(.+?)" + CAPTURE_BOUNDARY,
            re.IGNORECASE,
        ),
        lambda x: [
            f"Universidad {x}",
        ],
    ),
    # ── Municipio de X → keep as-is but also try Alcaldía ──
    (
        re.compile(
            r"\bmunicipio\s+de\s+(.+?)" + CAPTURE_BOUNDARY,
            re.IGNORECASE,
        ),
        lambda x: [
            f"Municipio de {x}",
            f"Alcaldía de {x}",
        ],
    ),
]

# Cuántos tokens trailing como máximo se intentan descartar al hacer shrink.
# Cubre "este mes" (1) / "pavimentacion" (1) / "este mes pasado" (2) y sobra;
# acota el costo cuando la captura agarra una cola larga de basura.
_MAX_SHRINK_LEVELS = 5

# El ruido de registro de SECOP no siempre queda en los bordes del token:
# "DEPARTAMENTO DE ANTIOQUIA//" y "MUNICIPIO DE SOACHA.." lo dejan al final,
# pero "ALCALDIA LOCAL DE TEUSAQUILLO(oficial)" lo pega DENTRO, sin espacio.
# Por eso se tokeniza por corridas alfanuméricas en vez de recortar extremos:
# recortando, "teusaquillo(oficial" seguía siendo un solo token y el nombre se
# volvía inalcanzable.
_WORD_RUN = re.compile(r"[0-9a-z]+")


def _fold(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(char for char in nfkd if not unicodedata.combining(char))


def is_name_connector(word: str) -> bool:
    """True si ``word`` articula un nombre propio en vez de terminarlo."""
    runs = _WORD_RUN.findall(_fold(word))
    return len(runs) == 1 and runs[0] in _NAME_CONNECTORS


def name_tokens(text: str) -> list[str]:
    """Tokens de un nombre, sin el ruido de registro de SECOP."""
    return _WORD_RUN.findall(_fold(text))


# Cabezas que nombran una institución sectorial. No son un lexicón de
# entidades: son las cabezas que ESTE módulo produce en ``ENTITY_REWRITES``
# para las familias hospital / ESE / universidad.
_SECTORAL_HEADS = ("hospital", "ese ", "empresa social del estado", "universidad")


def is_sectoral_name(canonical: str) -> bool:
    """True si la forma canónica pide una institución sectorial, no un territorio."""
    folded = _fold(canonical).strip()
    return any(folded.startswith(head) for head in _SECTORAL_HEADS)


def _candidate_lengths(words: list[str]) -> list[int]:
    """Longitudes de prefijo a probar, de la más larga a la más corta.

    Dos fuentes, unidas y ordenadas:

    * **Escalera de un token** (ENTITY-TRAILING-001) — para la cola de basura
      que la captura greedy se traga ("este mes", "pavimentacion"). Acotada por
      ``_MAX_SHRINK_LEVELS`` porque una cola larga es rara y cara.
    * **Cortes en cada ``de|del``** — los únicos puntos donde un nombre
      compuesto puede terminar de verdad. Se incluyen SIEMPRE, sin importar la
      profundidad: en "Medellín de obras de infraestructura de vías" el corte
      útil está a cinco tokens del final, fuera del alcance de la escalera.

    Nunca se propone un prefijo que termine en conector ("Departamento de
    Antioquia de"): un nombre no acaba en "de".
    """
    total = len(words)
    lengths = {
        k for k in range(total, max(1, total - _MAX_SHRINK_LEVELS + 1) - 1, -1)
    }
    lengths.update(
        index for index, word in enumerate(words) if index > 0 and is_name_connector(word)
    )
    return [
        k for k in sorted(lengths, reverse=True)
        if 0 < k <= total and not is_name_connector(words[k - 1])
    ]


def rewrite_entity(query: str) -> list[EntityCandidate]:
    """Scan query for entity-type patterns and generate canonical candidates.

    Skips temporal/measure phrases that look like entities (e.g., "ese año").

    Shrink-fallback (ENTITY-TRAILING-001): la captura ``(.+?)`` es greedy hasta
    el siguiente conector de cláusula o el fin de línea, así que se traga
    tokens trailing que NO son parte del nombre de la entidad ("este mes",
    "pavimentacion", basura). Para tolerarlo, por cada match generamos
    candidatos con la captura progresivamente más corta — del span MÁS LARGO al
    más corto — cada uno con su propio ``consumed_span``. El caller
    (query_router 4.6) prueba en orden y se queda con el primero que resuelve =
    el prefijo válido más largo. Esto:
      - NO regresa casos que hoy resuelven (el span completo se prueba primero),
      - evita over-shrink (longest-prefix-that-matches),
      - deja el leftover (p.ej. "pavimentacion") libre para fluir al objeto.

    COMPOUND-BUYER-CAPTURE (R2A.1): ``de|del`` dejó de ser frontera de captura,
    así que el span más largo es ahora el nombre COMPLETO —"Norte de Santander",
    no "Norte"—. Los cortes en cada ``de|del`` siguen disponibles como
    candidatos más cortos, pero se marcan ``requires_deterministic``: un
    fragmento de nombre compuesto puede acreditarse por alias exacto, nunca por
    parecido léxico.
    """
    # Words that should NOT trigger entity rewrite after ESE/patterns
    _EXCLUSION_CAPTURES = frozenset({
        "año", "ano", "periodo", "período", "rango",
        "año fiscal", "mismo año",
    })
    results: list[EntityCandidate] = []

    for pattern, replacer in ENTITY_REWRITES:
        match = pattern.search(query)
        if not match:
            continue
        words = match.group(1).split()
        if not words:
            continue
        # Prefijo literal entre el inicio del match y el inicio de la captura
        # (ej. "alcaldía de "), preservado tal cual aparece en `query` para que
        # `consumed_span` sea siempre un substring exacto de la entrada.
        prefix = query[match.start(0):match.start(1)]
        rewrite_type = pattern.pattern.split(r"\b")[1].split(r"\s")[0].lower()
        # Si la captura articula un nombre con ``de|del``, CUALQUIER prefijo más
        # corto es un trozo de ese nombre — no sólo el que corta justo en el
        # conector. "San Andrés de Sotavento" se encoge a "San Andrés" y luego a
        # "San", y "San" acreditaba a MUNICIPIO DE SAN JOSE DE CUCUTA por
        # parecido. Sin conector interno no hay nombre compuesto que partir y la
        # escalera de basura trailing (ENTITY-TRAILING-001) queda intacta.
        compound = any(
            is_name_connector(word) for index, word in enumerate(words) if index > 0
        )
        for k in _candidate_lengths(words):
            captured = " ".join(words[:k]).strip()
            if not captured or captured.lower() in _EXCLUSION_CAPTURES:
                continue
            truncates_compound = compound and k < len(words)
            consumed_span = (prefix + captured).strip()
            for candidate in replacer(captured):
                results.append(
                    EntityCandidate(
                        canonical=candidate,
                        consumed_span=consumed_span,
                        rewrite_type=rewrite_type,
                        requires_deterministic=truncates_compound,
                        rejects_territorial_root=is_sectoral_name(candidate),
                    )
                )

    return results

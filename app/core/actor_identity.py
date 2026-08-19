"""Identidad tipada de un ACTOR (proponente / contratista) — AP1.

Hermano de ``result_identity.py``: aquel identifica una FILA (proceso o
contrato); este identifica a QUIÉN pertenece un documento. Son sujetos
distintos y por eso son dos módulos, no uno.

Qué problema resuelve
---------------------
El mismo actor se escribe distinto en cada fuente::

    responsabilidad fiscal:  8,600,345,941   (comas y DV pegado)
    RUP:                     900264685       (limpio, sin DV)
    RUES:                    NIT y DV en columnas separadas
    SIRI:                    7534386         (cédula limpia)

Un JOIN ingenuo entre esas formas omite registros en silencio (falso
"limpio") o señala a la persona equivocada (falsa atribución). En este
dominio eso no es un bug de formato: es una afirmación falsa sobre un
tercero. Por eso la normalización de identidad es el spine de Actor
Profile y vive en su propio módulo, puro y testeado, antes de que
cualquier consulta la use.

La llave y la procedencia son cosas distintas
---------------------------------------------
``IdentityKey`` es lo que UNE fuentes: ``(document_type, base_number)`` y
nada más. Si la llave llevara ``source``, ``raw_value``, ``check_digit``
o ``confidence``, dos registros del mismo actor vistos desde fuentes
distintas dejarían de comparar iguales y el JOIN fallaría — que es
exactamente el fallo que este módulo existe para impedir.

``IdentityMatch`` es lo que se AUDITA: conserva el crudo verbatim, de
dónde vino, el DV cuando se pudo separar, y por qué se decidió el tipo.

Sobre el tipo documental
------------------------
**La longitud jamás decide el tipo.** Cédulas y NIT se solapan en número
de dígitos, así que clasificar por longitud produce atribuciones falsas
con apariencia de certeza. El orden de evidencia admitido es:

1. tipo declarado por quien llama (columna explícita de la fuente:
   ``tipo_documento_proveedor`` en SECOP Integrado,
   ``tipo_identifi_del_contratista`` en SECOP I);
2. dígito de verificación válido → evidencia POSITIVA de NIT;
3. nada de lo anterior → ``UNKNOWN``, declarado como tal.

El punto 2 es evidencia, no prueba: un número cualquiera satisface el
módulo 11 con probabilidad ≈1/11, así que un DV válido nunca produce
``confidence="declared"``. Y la ausencia de DV **no** es evidencia de
cédula: la cédula simplemente no tiene DV, no lo tiene inválido.

Autoridad sobre la identidad ≠ calidad del dato
-----------------------------------------------
El DV no forma parte del NIT: es un dato asociado que se consigna aparte.
De ahí que sean dos preguntas independientes —*cuál es el número que la
fuente declaró* y *si el DV que la acompaña supera la validación*— y que
este módulo nunca deje que la segunda reescriba la primera::

    fuente publica  900123456-7
        ├─ identidad declarada → IdentityKey("NIT", "900123456")   (intacta)
        └─ DV declarado 7 vs calculado 8 → CHECK_DIGIT_MISMATCH    (calidad)

Corregir el número a ``900123456-8`` porque el cálculo dice otra cosa
sería fabricar una identidad que ninguna fuente entregó. Ignorar la
discrepancia sería perder la única señal de que ese registro está mal
publicado. Se hace lo tercero: se conserva la identidad y se anota el
defecto, de modo que un ``900123456`` de RUP y un ``900123456-7`` de otra
fuente se unan por la misma llave sin que se pierda que una de las dos
traía un DV inconsistente.

Frontera del módulo
-------------------
Puro: sin red, sin base de datos, sin filesystem, sin reloj, sin
configuración, sin feature flags. No importa nada de ``app`` a propósito
— es la capa más baja, igual que ``result_identity``.

Fuera de alcance por decisión explícita (bloques posteriores): identidad
persistente de actor, alias, resolución por nombre, relaciones UT, y
cualquier consulta a una fuente. Este módulo representa y normaliza; no
resuelve ni consulta.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

__all__ = [
    "CHECK_DIGIT_WEIGHTS",
    "DocumentType",
    "IdentityConfidence",
    "IdentityKey",
    "IdentityMatch",
    # Los códigos de razón son contrato público: quien consuma esta capa
    # decide sobre ellos (p. ej. advertir de un DV defectuoso) y debe
    # compararlos contra la constante, no contra un literal suelto.
    "REASON_CHECK_DIGIT_EXPLICIT",
    "REASON_CHECK_DIGIT_MISMATCH",
    "REASON_CHECK_DIGIT_NOT_APPLICABLE",
    "REASON_CHECK_DIGIT_NOT_SEPARATED",
    "REASON_CHECK_DIGIT_VALIDATED",
    "REASON_EMPTY",
    "REASON_INVALID_TYPE",
    "REASON_NO_DIGITS",
    "REASON_OUT_OF_RANGE",
    "REASON_SENTINEL_DISCARDED",
    "REASON_TYPE_DECLARED",
    "REASON_TYPE_UNDETERMINED",
    "compute_check_digit",
    "normalize_identity",
]


# ---------------------------------------------------------------------------
# Vocabularios cerrados.
# ---------------------------------------------------------------------------

# Solo lo que AP1 necesita. Un catálogo exhaustivo de documentos
# colombianos (CE, PA, TI, NUIP…) se añade cuando exista una fuente que
# los distinga de verdad; inventarlo ahora sería vocabulario sin datos.
DocumentType = Literal["NIT", "CC", "UNKNOWN"]

# Cómo se decidió el TIPO documental — no cuán "seguro" es el número.
# El número es determinista (son sus dígitos); lo incierto es de qué
# clase de documento se trata.
IdentityConfidence = Literal[
    "declared",                   # el llamador dio el tipo (columna explícita)
    "inferred_from_check_digit",  # DV válido: evidencia positiva de NIT
    "undetermined",               # sin evidencia suficiente → UNKNOWN
]

_VALID_DOCUMENT_TYPES: frozenset[str] = frozenset({"NIT", "CC", "UNKNOWN"})

# Pesos oficiales del DV colombiano (módulo 11), aplicados de derecha a
# izquierda sobre el número base.
CHECK_DIGIT_WEIGHTS: tuple[int, ...] = (
    3, 7, 13, 17, 19, 23, 29, 37, 41, 43, 47, 53, 59, 67, 71,
)

# Rango de sanidad, NO discriminador de tipo. Descarta "12" o un teléfono
# pegado; jamás decide entre NIT y CC — eso lo prohíbe INV-NO-LENGTH.
_MIN_DOCUMENT_DIGITS = 5
_MAX_DOCUMENT_DIGITS = 11

# Centinelas textuales que las fuentes usan en lugar de dejar el campo
# vacío. Se observaron en SECOP Integrado, donde `documento_proveedor` y
# `tipo_documento_proveedor` traen "NO DEFINIDO"
# (docs/secop_integrado/SCHEMA_AUDIT.md). Tratarlos como un documento
# produciría un actor llamado "NODEFINIDO" que agruparía a todos los
# registros sin dato — la peor fusión posible.
_SENTINEL_TEXTS: frozenset[str] = frozenset({
    "NO DEFINIDO", "NO DEFINIDA", "NODEFINIDO", "NO APLICA", "N/A", "NA",
    "SIN DATO", "SIN INFORMACION", "SIN INFORMACIÓN", "NULL", "NONE", "-",
})

# Un DV explícitamente separado: exactamente UN dígito final precedido de
# separador. "860.034.594-1" y "860034594 1" coinciden; "860.034.594" no
# (su último grupo tiene tres dígitos) y "8,600,345,941" tampoco (su
# último grupo también tiene tres) — ese cae a la regla de validación.
_EXPLICIT_CHECK_DIGIT_RE = re.compile(r"[-\s.](\d)$")

_NON_DIGITS_RE = re.compile(r"[^0-9]")


# ---------------------------------------------------------------------------
# Códigos de razón (estables, UPPER_SNAKE_CASE, nunca prosa de producto).
# ---------------------------------------------------------------------------

REASON_TYPE_DECLARED = "IDENTITY_TYPE_DECLARED"
REASON_TYPE_UNDETERMINED = "IDENTITY_TYPE_UNDETERMINED"
REASON_CHECK_DIGIT_EXPLICIT = "IDENTITY_CHECK_DIGIT_EXPLICIT"
REASON_CHECK_DIGIT_VALIDATED = "IDENTITY_CHECK_DIGIT_VALIDATED"
REASON_CHECK_DIGIT_NOT_SEPARATED = "IDENTITY_CHECK_DIGIT_NOT_SEPARATED"
REASON_CHECK_DIGIT_NOT_APPLICABLE = "IDENTITY_CHECK_DIGIT_NOT_APPLICABLE"
# Reservado para UN caso: una fuente publicó un DV y el cálculo lo
# desmiente. Nunca se emite cuando nadie declaró un DV — si se usara
# también para la hipótesis descartada de un número desnudo, dejaría de
# poder señalar el dato defectuoso, que es lo único que interesa medir.
REASON_CHECK_DIGIT_MISMATCH = "IDENTITY_CHECK_DIGIT_MISMATCH"
REASON_EMPTY = "IDENTITY_EMPTY"
REASON_INVALID_TYPE = "IDENTITY_INVALID_TYPE"
REASON_SENTINEL_DISCARDED = "IDENTITY_SENTINEL_DISCARDED"
REASON_NO_DIGITS = "IDENTITY_NO_DIGITS"
REASON_OUT_OF_RANGE = "IDENTITY_OUT_OF_RANGE"


# ---------------------------------------------------------------------------
# IdentityKey — lo único que une fuentes.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IdentityKey:
    """Llave canónica de join: ``(document_type, base_number)``.

    Deliberadamente NO contiene ``source``, ``raw_value``, ``check_digit``
    ni ``confidence``. Todo eso es procedencia y vive en
    ``IdentityMatch``; meterlo aquí rompería la igualdad entre fuentes,
    que es la única razón por la que esta llave existe.

    ``base_number`` es el documento SIN dígito de verificación y sin
    separadores. El DV, cuando se conoce, viaja en el match: es un dato
    derivado del base, no parte de la identidad. Dos registros del mismo
    NIT, uno con DV y otro sin él, son el MISMO actor.

    **INV-ID-TYPE:** dos números iguales de tipos documentales distintos
    NO son iguales. ``IdentityKey("NIT", "123456789")`` y
    ``IdentityKey("CC", "123456789")`` son dos actores, y la igualdad de
    dataclass lo garantiza por construcción.
    """

    document_type: DocumentType
    base_number: str

    def __post_init__(self) -> None:
        if self.document_type not in _VALID_DOCUMENT_TYPES:
            raise ValueError(f"invalid document_type: {self.document_type!r}")
        if not isinstance(self.base_number, str) or not self.base_number:
            raise ValueError("base_number must be a non-empty string")
        if _NON_DIGITS_RE.search(self.base_number):
            raise ValueError(
                f"base_number must contain only digits: {self.base_number!r}"
            )


# ---------------------------------------------------------------------------
# IdentityMatch — lo que se audita.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IdentityMatch:
    """Una lectura de identidad, con su procedencia y su porqué.

    ``key`` es ``None`` exactamente cuando el crudo no produjo ningún
    documento utilizable (vacío, centinela, sin dígitos, fuera de rango).
    En ese caso ``confidence`` es ``"undetermined"`` y los
    ``reason_codes`` dicen qué pasó. Fallar así —declarando "no sé"— es
    la respuesta segura: fabricar una llave desde un crudo inservible
    fusionaría actores distintos, y ese daño no es observable aguas
    abajo.

    ``raw_value`` se conserva VERBATIM. Es lo que permite auditar después
    por qué se atribuyó un contrato a un actor, y no se recorta ni se
    reescribe.

    ``check_digit`` es ``None`` cuando no venía separado y no se pudo
    validar. Ausencia de DV NO significa cédula: significa que no se
    conoce el DV.
    """

    key: IdentityKey | None
    check_digit: str | None
    raw_value: str
    source: str | None
    confidence: IdentityConfidence
    reason_codes: tuple[str, ...] = ()

    @property
    def is_resolved(self) -> bool:
        """True si hay un documento utilizable (con o sin tipo conocido)."""
        return self.key is not None

    @property
    def document_type(self) -> DocumentType:
        """Tipo del documento; ``"UNKNOWN"`` si no hay llave."""
        return self.key.document_type if self.key is not None else "UNKNOWN"

    @property
    def base_number(self) -> str | None:
        """Documento sin DV ni separadores; ``None`` si no hay llave."""
        return self.key.base_number if self.key is not None else None


# ---------------------------------------------------------------------------
# Dígito de verificación (módulo 11).
# ---------------------------------------------------------------------------


def compute_check_digit(base_number: str) -> int | None:
    """DV colombiano de ``base_number``, o ``None`` si no es calculable.

    Regla oficial: se ponderan los dígitos de derecha a izquierda con
    ``CHECK_DIGIT_WEIGHTS``, se toma la suma módulo 11 y, si el residuo
    es 0 o 1, ese residuo ES el DV; en cualquier otro caso el DV es
    ``11 - residuo``.

    Función pura. Devuelve ``None`` —nunca lanza— cuando la entrada no es
    una cadena de dígitos o excede los pesos definidos: un DV inventado
    sería peor que ninguno.
    """
    if not isinstance(base_number, str) or not base_number:
        return None
    if _NON_DIGITS_RE.search(base_number):
        return None
    if len(base_number) > len(CHECK_DIGIT_WEIGHTS):
        return None

    total = sum(
        int(digit) * CHECK_DIGIT_WEIGHTS[position]
        for position, digit in enumerate(reversed(base_number))
    )
    remainder = total % 11
    return remainder if remainder < 2 else 11 - remainder


# ---------------------------------------------------------------------------
# Normalización.
# ---------------------------------------------------------------------------


def _clean_raw(value: object) -> tuple[str | None, tuple[str, ...]]:
    """Devuelve ``(texto_utilizable, códigos_de_descarte)``.

    Solo un ``str`` puede ser un documento. Convertir un entero con
    ``str(...)`` perdería los ceros a la izquierda que la fuente sí
    entregó como texto, y fabricar una identidad a partir de un dato que
    la fuente no dio como cadena es inventarla.
    """
    if value is None:
        return None, (REASON_EMPTY,)
    if not isinstance(value, str):
        return None, (REASON_INVALID_TYPE,)
    stripped = value.strip()
    if not stripped:
        return None, (REASON_EMPTY,)
    if stripped.upper() in _SENTINEL_TEXTS:
        return None, (REASON_SENTINEL_DISCARDED,)
    return stripped, ()


def _split_check_digit(
    cleaned_raw: str, digits: str, declared_type: "DocumentType | None",
) -> tuple[str, str | None, tuple[str, ...]]:
    """Separa ``digits`` en ``(base, dv, razones)``.

    El dígito de verificación es un concepto del NIT. Una cédula no tiene
    DV, así que cuando la fuente declara ``CC`` no se aplica ninguna regla
    de DV: el número entero ES la cédula. Sin esta guarda, ~1 de cada 11
    cédulas de diez dígitos satisface el módulo 11 por azar y perdería su
    último dígito, convirtiéndose en el documento de otra persona.

    Para todo lo demás hay dos caminos, y solo dos:

    1. **DV explícitamente separado** en el crudo (``…-1``, ``… 1``). El
       ``base_number`` se respeta tal cual: la fuente ya declaró dónde
       termina el número, y reescribirlo con aritmética propia sería
       inventar una identidad distinta de la que la fuente entregó. El DV
       declarado SÍ se contrasta con el cálculo, pero el resultado es
       *calidad del dato*, no autoridad sobre la identidad: si discrepa se
       anota ``CHECK_DIGIT_MISMATCH`` y nada más cambia. Identidad y
       calidad son dos preguntas distintas y se responden por separado.
    2. **Secuencia desnuda.** Aquí nadie declaró un DV, así que no hay
       nada con qué discrepar: solo una hipótesis que el módulo 11
       confirma o no. Si confirma, se separa; si no, el número se conserva
       ÍNTEGRO y se anota únicamente que no se separó. Emitir
       ``MISMATCH`` en este camino haría irreconocible el caso que sí
       importa —una fuente que publicó un DV equivocado—.

    El punto 2 corrige el defecto que el diagnóstico AP0 dejó documentado:
    ``8600345941`` no coincidía con ``860034594`` y el registro se perdía
    en silencio. Aquí sí coincide, porque ``dv("860034594") == 1``.
    """
    if declared_type == "CC":
        return digits, None, (REASON_CHECK_DIGIT_NOT_APPLICABLE,)

    explicit = _EXPLICIT_CHECK_DIGIT_RE.search(cleaned_raw)
    if explicit is not None and len(digits) > 1:
        base, declared_dv = digits[:-1], digits[-1]
        reasons = [REASON_CHECK_DIGIT_EXPLICIT]
        expected = compute_check_digit(base)
        if expected is None or str(expected) != declared_dv:
            # La identidad NO se toca: solo se registra que el DV que la
            # fuente publicó no supera la validación.
            reasons.append(REASON_CHECK_DIGIT_MISMATCH)
        else:
            reasons.append(REASON_CHECK_DIGIT_VALIDATED)
        return base, declared_dv, tuple(reasons)

    if len(digits) > 1:
        candidate_base, candidate_dv = digits[:-1], digits[-1]
        expected = compute_check_digit(candidate_base)
        if expected is not None and str(expected) == candidate_dv:
            return candidate_base, candidate_dv, (REASON_CHECK_DIGIT_VALIDATED,)
        return digits, None, (REASON_CHECK_DIGIT_NOT_SEPARATED,)

    return digits, None, (REASON_CHECK_DIGIT_NOT_SEPARATED,)


def normalize_identity(
    raw_value: object,
    *,
    document_type: str | None = None,
    source: str | None = None,
) -> IdentityMatch:
    """Normaliza un documento crudo a un ``IdentityMatch``. Función pura.

    ``document_type`` es el tipo DECLARADO por quien llama —típicamente
    la columna explícita de la fuente—. Cuando llega, manda: es la
    evidencia de mayor calidad que existe y este módulo no la
    sobrescribe con inferencias propias. Cuando no llega, el tipo se
    infiere SOLO si el dígito de verificación lo respalda, y en ningún
    otro caso.

    Nunca lanza por un crudo inservible: devuelve un match sin llave con
    los ``reason_codes`` que explican el descarte.
    """
    reasons: list[str] = []
    original = raw_value if isinstance(raw_value, str) else ""

    declared_type: DocumentType | None = None
    if document_type is not None:
        normalized_type = str(document_type).strip().upper()
        if normalized_type not in _VALID_DOCUMENT_TYPES:
            # Un tipo que no reconocemos no se adivina ni se aproxima: se
            # ignora como declaración y el flujo cae a la inferencia por
            # DV, dejando constancia en las razones.
            reasons.append(REASON_INVALID_TYPE)
        elif normalized_type != "UNKNOWN":
            declared_type = normalized_type  # type: ignore[assignment]

    cleaned, discard_codes = _clean_raw(raw_value)
    reasons.extend(discard_codes)
    if cleaned is None:
        return IdentityMatch(
            key=None, check_digit=None, raw_value=original, source=source,
            confidence="undetermined", reason_codes=_dedupe(reasons),
        )

    digits = _NON_DIGITS_RE.sub("", cleaned)
    if not digits:
        reasons.append(REASON_NO_DIGITS)
        return IdentityMatch(
            key=None, check_digit=None, raw_value=cleaned, source=source,
            confidence="undetermined", reason_codes=_dedupe(reasons),
        )

    base_number, check_digit, split_reasons = _split_check_digit(
        cleaned, digits, declared_type,
    )
    reasons.extend(split_reasons)

    # Sanidad de rango sobre el BASE. No discrimina tipo (INV-NO-LENGTH):
    # descarta lo que no puede ser un documento en ninguna de las clases.
    if not (_MIN_DOCUMENT_DIGITS <= len(base_number) <= _MAX_DOCUMENT_DIGITS):
        reasons.append(REASON_OUT_OF_RANGE)
        return IdentityMatch(
            key=None, check_digit=None, raw_value=cleaned, source=source,
            confidence="undetermined", reason_codes=_dedupe(reasons),
        )

    if declared_type is not None:
        resolved_type: DocumentType = declared_type
        confidence: IdentityConfidence = "declared"
        reasons.append(REASON_TYPE_DECLARED)
    elif check_digit is not None:
        # Evidencia POSITIVA de NIT, no certeza: la confianza lo dice.
        resolved_type = "NIT"
        confidence = "inferred_from_check_digit"
    else:
        # Sin tipo declarado y sin DV que respalde nada. La ausencia de DV
        # no es evidencia de cédula, así que no se afirma CC: se declara
        # que el tipo no está determinado.
        resolved_type = "UNKNOWN"
        confidence = "undetermined"
        reasons.append(REASON_TYPE_UNDETERMINED)

    return IdentityMatch(
        key=IdentityKey(document_type=resolved_type, base_number=base_number),
        check_digit=check_digit,
        raw_value=cleaned,
        source=source,
        confidence=confidence,
        reason_codes=_dedupe(reasons),
    )


def _dedupe(codes: list[str]) -> tuple[str, ...]:
    """Quita repetidos conservando el orden de aparición.

    El orden es el del razonamiento, no alfabético: leer las razones en
    el orden en que se produjeron es lo que hace auditable la decisión.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for code in codes:
        if code not in seen:
            seen.add(code)
            ordered.append(code)
    return tuple(ordered)

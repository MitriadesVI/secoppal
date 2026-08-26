"""Propiedades de `normalize_actor_name`, verificadas con Hypothesis.

La suite HCA prueba los casos que a alguien se le ocurrieron. Esto prueba
*invariantes*: afirmaciones que deben cumplirse para toda entrada, incluidas
las que nadie imagino. Hypothesis genera los casos y, cuando encuentra un
fallo, lo reduce al ejemplo minimo que lo reproduce.

Se eligio este normalizador porque es la espina dorsal de la identidad de
actores: si dos pasadas del mismo nombre no coinciden, el indice de alias se
parte en silencio y la resolucion exacta empieza a fallar sin error visible.
"""

from __future__ import annotations

import unicodedata

from hypothesis import given, strategies as st

from app.core.actor_name import normalize_actor_name

# Texto arbitrario, incluidos acentos, puntuacion, espacios raros y vacio.
cualquier_texto = st.text(max_size=120)


@given(cualquier_texto)
def test_idempotente(value: str) -> None:
    """Normalizar dos veces da lo mismo que una.

    Es la propiedad critica: el indice de alias se construye normalizando, y
    las consultas se normalizan otra vez al llegar. Si no fuera idempotente,
    un alias indexado no encontraria su propia consulta.
    """
    una = normalize_actor_name(value)
    assert normalize_actor_name(una) == una


@given(cualquier_texto)
def test_sin_espacios_colgantes_ni_dobles(value: str) -> None:
    """La salida no tiene espacios al borde ni espacios repetidos."""
    out = normalize_actor_name(value)
    assert out == out.strip()
    assert "  " not in out


@given(cualquier_texto)
def test_sin_marcas_de_acento(value: str) -> None:
    """No queda ningun caracter combinante: 'MEDELLÍN' y 'MEDELLIN' colapsan."""
    out = normalize_actor_name(value)
    assert not any(unicodedata.combining(c) for c in out)


@given(cualquier_texto)
def test_solo_alfanumerico_y_espacios(value: str) -> None:
    """Toda puntuacion se volvio separador: no sobrevive ningun simbolo."""
    out = normalize_actor_name(value)
    assert all(c.isalnum() or c == " " for c in out)


@given(cualquier_texto)
def test_casefold_estable(value: str) -> None:
    """La salida ya esta plegada: volver a plegarla no la cambia."""
    out = normalize_actor_name(value)
    assert out.casefold() == out


def test_none_y_vacio_dan_cadena_vacia() -> None:
    """Los bordes explicitos, que Hypothesis no tiene por que adivinar."""
    assert normalize_actor_name(None) == ""
    assert normalize_actor_name("") == ""
    assert normalize_actor_name("   ") == ""

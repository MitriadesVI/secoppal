"""Estado DERIVADO DE EJECUCIÓN — lo que el motor añade y el usuario no pidió.

INV-PURE-ORDERING-PRESERVES-SOURCE-UNIVERSE (HCA-FOLLOWUP-016).

Un follow-up que solo cambia el orden —"los más antiguos primero"— puede cambiar
ORDER BY y el cursor de paginación, y nada más. Si el WHERE del turno 2 difiere
del turno 1, el usuario está viendo otro universo del que pidió reordenar.

El problema
-----------
``resolved_params`` mezcla dos cosas de naturaleza distinta:

* ESTADO SEMÁNTICO DEL USUARIO — tema, territorio, comprador, dinero, tiempo,
  estado, modalidad. Es lo que la persona pidió y debe sobrevivir a un
  follow-up.
* ESTADO DERIVADO DE EJECUCIÓN — lo que el motor calcula DESPUÉS de consultar la
  fuente, para enriquecer la respuesta. No fue parte del fetch.

``entidad_ecosystem`` es el segundo caso. ``normal_flow`` lo inyecta tras
ejecutar, cuando la consulta tiene territorio pero no comprador: reconstruye el
SoQL para la respuesta asesora, pero NO vuelve a consultar, así que las filas
que el usuario vio nunca lo aplicaron. Al persistirse en el turno, el siguiente
follow-up lo heredaba como si fuera un filtro solicitado, y "ordénalos por fecha"
sobre Cali añadía un OR con diez compradores del ecosistema municipal.

Tres capas ya se defendían por su cuenta — el buffer de paginación, la repesca
de página 2 y el guardado de radares, cada una con su propia lista. Faltaba la
cuarta, la persistencia conversacional, que es justamente por donde se propaga.
Este módulo es esa lista, una sola vez.

Un ecosistema que el usuario SÍ declaró ("contratos del ICBF en Antioquia") no
viaja por esta clave: el router por nombre lo representa mediante
``entidad_in_candidates``/``entidad_contains_all``. Esos filtros sí pertenecen
al universo solicitado y deben sobrevivir a un follow-up.

Puro, determinista, sin estado y sin I/O.
"""
from __future__ import annotations

from typing import Any

__all__ = [
    "DERIVED_EXECUTION_KEYS",
    "strip_derived_execution_state",
]

# Claves que el motor produce después de ejecutar y que no describen lo que el
# usuario pidió. No pueden persistir como estado semántico del turno.
DERIVED_EXECUTION_KEYS: frozenset[str] = frozenset({
    "entidad_ecosystem",
})


def strip_derived_execution_state(params: dict[str, Any]) -> dict[str, Any]:
    """Copia de ``params`` sin el estado derivado de ejecución."""
    return {k: v for k, v in params.items() if k not in DERIVED_EXECUTION_KEYS}

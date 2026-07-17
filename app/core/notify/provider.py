"""provider.py — el contrato con el mundo exterior (P1B-4, R2C).

Este módulo no habla con nadie: no importa un SDK, no abre un socket, no conoce
Telegram. Define **qué puede afirmar** quien sí lo haga, y lo define en términos de
una sola pregunta, que es la única que el worker necesita responder para no duplicar
un mensaje:

    ¿pudo haber salido algo hacia una persona real?

Tres respuestas, y ni una más:

- ``no_external_effect``       — con certeza no salió nada. Reintentar es seguro.
- ``possible_external_effect`` — no se sabe. Reintentar es apostar.
- ``confirmed_external_effect`` — salió. Reintentar duplica.

El eje **no** es "éxito o error". Un timeout es un error para el proveedor y, aun
así, puede haber entregado el mensaje; un rechazo por dirección inválida es un error
y con certeza no entregó nada. Clasificar por éxito/fallo mezclaría esos dos casos y
el worker tomaría la peor decisión posible: reintentar el primero.

``retryable`` es un campo aparte y no una función de ``effect`` porque solo tiene un
grado de libertad real: dentro de ``no_external_effect``, un rate limit se reintenta
y una dirección inválida no. En los otros dos efectos la respuesta la fija el
contrato, y el constructor la hace cumplir en vez de confiar en que el implementador
la recuerde.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from app.core.notify.schema import ERROR_CLASSES

NO_EXTERNAL_EFFECT = "no_external_effect"
POSSIBLE_EXTERNAL_EFFECT = "possible_external_effect"
CONFIRMED_EXTERNAL_EFFECT = "confirmed_external_effect"

EFFECTS = (NO_EXTERNAL_EFFECT, POSSIBLE_EXTERNAL_EFFECT, CONFIRMED_EXTERNAL_EFFECT)


class ProviderContractError(Exception):
    """El proveedor afirmó algo que el contrato no admite.

    No es un fallo de entrega: es un fallo del *implementador*. Se lanza al construir
    el resultado —no al usarlo— para que el error apunte al proveedor que lo inventó y
    no al worker que lo leyó tres capas más abajo.
    """


@dataclass(frozen=True)
class ProviderRequest:
    """Todo lo que un proveedor necesita para enviar, y nada más.

    ``external_recipient_id`` y ``text`` van con ``repr=False``: son,
    respectivamente, la identidad de una persona real y el contenido de su mensaje.
    Este objeto es exactamente lo que alguien vuelca a un log al depurar un envío
    fallido, y lo que quede en el repr acaba en disco.

    El ``claim_token`` **no está** y no puede añadirse: es la prueba de propiedad del
    batch, y un proveedor no tiene nada que hacer con ella. Que no exista el campo es
    lo que hace imposible filtrarla por aquí.
    """

    notification_id: str
    attempt_id: str
    channel: str
    external_idempotency_key: str
    external_recipient_id: str = field(repr=False)
    text: str = field(repr=False)
    payload_version: str


@dataclass(frozen=True)
class ProviderResult:
    """Lo que el proveedor afirma que pasó.

    Las combinaciones imposibles se rechazan aquí, en el constructor, y no en el
    worker: si la validación viviera en quien lee, cada lector futuro tendría que
    acordarse de repetirla, y el primero que no lo hiciera reintentaría un mensaje ya
    entregado. Un ``ProviderResult`` que existe es un ``ProviderResult`` coherente.

    ``error_message_safe`` es texto libre **sin PII**: una categoría legible, no el
    volcado del proveedor. El mensaje crudo de una excepción puede traer la URL con
    el token, el número de teléfono o el cuerpo del mensaje. Va con ``repr=False``
    justamente porque es texto libre: "seguro" es una promesa que hace quien lo
    escribe, no algo que este tipo pueda verificar, y el único campo del contrato
    cuyo contenido no está acotado por un vocabulario cerrado no puede ser el que
    acabe volcado entero en un log. Sigue persistiéndose intacto — no se imprime.

    ``error_class``, si no es ``None``, tiene que pertenecer a ``ERROR_CLASSES`` —el
    mismo vocabulario que ``schema.py`` graba en el ``CHECK`` de
    ``radar_delivery_attempts``. Sin esta validación aquí, un proveedor que inventara
    una clase (``"dns_error"``) produciría un ``ProviderResult`` válido para este
    módulo que ``finish_attempt`` rechazaría después de haber llamado al proveedor: un
    "no hubo efecto, es reintentable" conocido y barato se convertiría en un intento
    que revienta a mitad de cerrarse, queda ``dispatching`` y la siguiente corrida lo
    reconcilia como ``ambiguous`` —el resultado caro— por una razón que no tiene nada
    que ver con si el mensaje salió.
    """

    effect: str
    retryable: bool = False
    provider_message_id: str | None = None
    error_class: str | None = None
    error_message_safe: str | None = field(default=None, repr=False)
    retry_after: datetime | None = None

    def __post_init__(self) -> None:
        if self.effect not in EFFECTS:
            raise ProviderContractError(
                f"effect inválido: {self.effect!r}. Válidos: {', '.join(EFFECTS)}."
            )
        if not isinstance(self.retryable, bool):
            raise ProviderContractError(
                f"retryable debe ser bool, no {type(self.retryable).__name__}."
            )
        if self.error_class is not None and self.error_class not in ERROR_CLASSES:
            raise ProviderContractError(
                f"error_class inválido: {self.error_class!r}. Válidos: "
                f"{', '.join(ERROR_CLASSES)}. Es el mismo vocabulario que la base "
                f"exige en radar_delivery_attempts.error_class; una clase que la base "
                f"rechazaría no puede llegar a existir en un ProviderResult."
            )
        if self.effect == CONFIRMED_EXTERNAL_EFFECT and self.retryable:
            raise ProviderContractError(
                "confirmed_external_effect con retryable=True: el mensaje ya llegó a "
                "una persona. Reintentarlo no es recuperarse de un fallo, es enviar un "
                "duplicado."
            )
        if self.effect == POSSIBLE_EXTERNAL_EFFECT and self.retryable:
            raise ProviderContractError(
                "possible_external_effect con retryable=True: si no se sabe si el "
                "mensaje salió, reintentar es apostar a que no. Este sistema no apuesta "
                "con el teléfono de nadie: posible efecto nunca se reintenta solo."
            )
        if self.retry_after is not None:
            if not isinstance(self.retry_after, datetime):
                raise ProviderContractError(
                    f"retry_after debe ser datetime, no "
                    f"{type(self.retry_after).__name__}."
                )
            if (self.retry_after.tzinfo is None
                    or self.retry_after.utcoffset() is None):
                raise ProviderContractError(
                    f"retry_after debe incluir zona horaria: {self.retry_after!r}. Un "
                    f"instante naive comparado contra marcas UTC ordena mal y el "
                    f"reintento saldría cuando no debe."
                )


class NotificationProvider(Protocol):
    """Un canal capaz de entregar. Lo implementa el fake de las pruebas y, cuando
    exista, el adaptador real de Telegram.

    ``send`` **no** debe lanzar para señalar un fallo de entrega: un fallo conocido es
    un ``ProviderResult``. Una excepción que escape significa "no sé qué pasó", y el
    worker no tiene más remedio que tratarla como posible efecto externo — que es la
    respuesta cara. Lanzar por comodidad convierte un rate limit reintentable en un
    batch que ningún reintento automático va a tocar.
    """

    def send(self, request: ProviderRequest) -> ProviderResult:
        ...


__all__ = [
    "CONFIRMED_EXTERNAL_EFFECT",
    "EFFECTS",
    "NO_EXTERNAL_EFFECT",
    "POSSIBLE_EXTERNAL_EFFECT",
    "NotificationProvider",
    "ProviderContractError",
    "ProviderRequest",
    "ProviderResult",
]

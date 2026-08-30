"""
SECOPPAL — Identidad de sesión web y frontera entre usuarios
=============================================================

Tres piezas pequeñas que la UI Streamlit necesita para que un navegador
compartido no mezcle dos personas. Viven aquí —y no en ``streamlit_app.py``—
porque ese módulo ejecuta la aplicación entera al importarse: una frontera de
seguridad que no se puede probar sin levantar la app no es una frontera, es una
intención.

No es un gestor de sesiones. No sabe qué es una cuenta, ni un token, ni una
cookie; solo genera identidades de conversación y vacía un mapa de estado.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import MutableMapping
from typing import Any


STREAMLIT_CHAT_PREFIX = "st_"


def new_streamlit_chat_id() -> str:
    """Identidad de **conversación** para una sesión Streamlit nueva.

    El esquema anterior derivaba ``chat_id`` de la cuenta autenticada como
    ``st_email_<email>``. Esto escribía PII en el identificador persistido y
    hacía que la misma cuenta reutilizara el hilo entre sesiones o después de
    un F5. No hacía que una cuenta B heredara el ``ConversationStore`` de A:
    ``_current_user_identity()`` reasignaba el id con el email de B en cada
    rerun.

    ``st_<uuid>`` es una decisión deliberada para este beta: no contiene PII,
    queda aislado por sesión y rota en cada logout/login. Como Streamlit crea
    una sesión nueva en un F5, también crea entonces una conversación nueva.
    El tradeoff conocido es que la memoria conversacional no sobrevive el F5 y
    puede quedar un archivo adicional de ``ConversationStore`` por sesión. Es
    P2 / future session-continuity design, no el P0 de aislamiento entre
    usuarios.

    ``uuid4`` hace despreciable una colisión entre sesiones. El prefijo ``st_``
    es el que ``ConversationStore._sanitize_chat_id`` reconoce y deja intacto.
    """
    return f"{STREAMLIT_CHAT_PREFIX}{uuid.uuid4().hex}"


def scratch_chat_id(purpose: str, owner: str) -> str:
    """Hilo de trabajo interno, estable por dueño y sin email en claro.

    Algunas operaciones (resolver la búsqueda al crear un radar) pasan por el
    pipeline conversacional pero no son la conversación del usuario: necesitan
    un hilo aparte para no contaminarla. Ese hilo debe ser estable por dueño
    —si no, cada creación deja un archivo nuevo— pero el dueño es ``email:...``,
    y concatenarlo escribía el email en el nombre del archivo.

    El hash produce un identificador pseudónimo determinista: mismo dueño,
    mismo hilo, sin incluir el email en claro.
    """
    digest = hashlib.sha256(owner.encode("utf-8")).hexdigest()[:16]
    return f"_{purpose}_{digest}"


def reset_user_session(
    state: MutableMapping[str, Any], *, keep: tuple[str, ...] = (),
) -> list[str]:
    """Vacía el estado de sesión. Devuelve las claves borradas, ordenadas.

    **Borra todo salvo lo que se pida conservar explícitamente**, en vez de
    quitar una lista de claves conocidas. La lista era el defecto: el logout
    eliminaba cinco claves y dejaba en pie ``history``, ``chat_id``,
    ``_open_radar``, ``_open_event``, filtros y cachés del usuario anterior.
    Una lista solo protege de lo que alguien recordó escribir, y cada feature
    nueva que guarda estado la deja incompleta otra vez —en silencio—. Con la
    polaridad invertida, la clave nueva nace protegida y el olvido pasa a ser
    "se borró algo que quizá no hacía falta", no una fuga.

    Acepta cualquier ``MutableMapping`` —``st.session_state`` lo es— para poder
    probar la frontera con un ``dict``. Materializa las claves antes de borrar:
    mutar el mapa mientras se itera es un error en el propio proxy de Streamlit.
    """
    doomed = [key for key in list(state.keys()) if key not in keep]
    for key in doomed:
        del state[key]
    return sorted(doomed)

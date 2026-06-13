"""commands.py — detección de los comandos conversacionales del Radar.

Tres comandos (sección 11 del doc de producto):

- **guardar** — "guardar esta búsqueda como Radar PAE Caribe" / "crear radar X".
- **listar**  — "mis radares" / "ver radares" / "listar radares".
- **ejecutar**— "ejecutar radar PAE Caribe" / "revisar radar X".

``parse_radar_command`` devuelve un ``RadarCommand`` o ``None`` (no es comando
de radar → el handler cae al flujo normal). La detección es por regex sobre la
palabra ancla "radar"; el nombre se captura preservando el casing original.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

SAVE = "save"
LIST = "list"
RUN = "run"


@dataclass
class RadarCommand:
    kind: str           # SAVE | LIST | RUN
    name: str = ""      # nombre del radar (SAVE / RUN)


def radar_owner(user_identity: dict | None, chat_id: str | None) -> str:
    """Clave de propietario estable, consistente entre chat y pestaña Streamlit.

    Prefiere ``channel_user_key`` (``email:...`` / ``anon:...``) que sobrevive
    entre sesiones; cae a ``chat_id`` y, en último caso, a ``"anon"``.
    """
    if user_identity:
        key = (user_identity.get("channel_user_key") or "").strip()
        if key:
            return key
    return (chat_id or "").strip() or "anon"


# "mis radares", "ver/listar/mostrar (mis) radares", "lista de radares"
_LIST_RE = re.compile(
    r"^\s*(?:mis\s+radares"
    r"|(?:ver|listar|lista(?:r)?|muestra(?:me)?|mostrar)\s+(?:mis\s+)?radares"
    r"|lista\s+de\s+radares)\s*[?.!]*\s*$",
    re.IGNORECASE,
)

# "guardar/guarda (esta búsqueda|esto|...) como (el) radar <nombre>"
# "crear/crea (un) radar (llamado) <nombre>"
_SAVE_RE = re.compile(
    r"(?:guard\w*|cre\w*)\b.*?\bradar\b\s+(?:llamad[oa]\s+)?(?P<name>.+?)\s*[?.!]*\s*$",
    re.IGNORECASE | re.DOTALL,
)

# "ejecutar/ejecuta/revisar/revisa/correr/corre/actualizar/actualiza (el) radar <nombre>"
_RUN_RE = re.compile(
    r"^\s*(?:ejecut\w*|revis\w*|corre\w*|actualiz\w*|run)\s+(?:el\s+|mi\s+)?radar\b"
    r"\s+(?P<name>.+?)\s*[?.!]*\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _clean_name(raw: str) -> str:
    name = raw.strip().strip("\"'“”«»").strip()
    # quitar prefijo "como" colgante si la captura lo arrastró
    return name


def _mentions_radar(text: str) -> bool:
    norm = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    return "radar" in norm


def parse_radar_command(text: str) -> RadarCommand | None:
    """Clasifica ``text`` como comando de radar, o ``None`` si no lo es."""
    if not text or not _mentions_radar(text):
        return None
    stripped = text.strip()

    if _LIST_RE.match(stripped):
        return RadarCommand(kind=LIST)

    m = _RUN_RE.match(stripped)
    if m:
        name = _clean_name(m.group("name"))
        if name:
            return RadarCommand(kind=RUN, name=name)

    m = _SAVE_RE.search(stripped)
    if m:
        name = _clean_name(m.group("name"))
        if name:
            return RadarCommand(kind=SAVE, name=name)

    return None

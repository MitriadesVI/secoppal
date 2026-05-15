"""
SECOPPAL — ConversationStore
=============================
Memoria conversacional por chat_id. Persiste turnos en JSONL append-only.
Un archivo por chat: data/conversations/<sanitized_chat_id>.jsonl

Uso:
    store = ConversationStore()
    turn_id = store.append_turn(
        chat_id="tg_123456",
        user_query="procesos de pavimentacion en el Choco",
        response="Encontre 5 resultados...",
        parsed_params={"objeto": "pavimentacion", "departamento_resolved": "CHOCÓ"},
        result_ids=["ABC-001", "ABC-002"],
        trace_id="abc123def456",
    )
    history = store.get_history("tg_123456", last_n=5)
    store.clear("tg_123456")
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Reglas de merge para follow-up detection (v1.2 MVP)
# objeto: replace (no aditivo). Aditivo ("y tambien X") va en v1.3.
# dataset: keep_previous_unless_explicit — el usuario que dice "y en bolivar?"
#          probablemente sigue en procesos, no cambia a contratos.
# ---------------------------------------------------------------------------
MERGE_RULES: dict[str, str] = {
    "departamento_resolved":  "replace",
    "departamento":           "replace",
    "entidad_resolved":       "replace",
    "entidad":                "replace",
    "fecha_desde":            "replace",
    "fecha_hasta":            "replace",
    "valor_min":              "replace",
    "valor_max":              "replace",
    "estado_family":          "replace",
    "objeto":                 "keep_previous_unless_explicit",
    "dataset":                "keep_previous_unless_explicit",
}

# Ventana temporal para follow-up: si el ultimo turno tiene mas de N minutos, no aplicar.
FOLLOWUP_WINDOW_MINUTES: int = 30

# Palabras que indican continuation en el inicio de la query (lowercased).
CONTINUATION_WORDS: tuple[str, ...] = (
    "y ", "ahora", "también", "tambien", "pero ", "solo ",
    "muéstrame", "muestrame", "dame", "ordena", "ordénalos",
    "ordenalos", "filtremos", "filtra",
    "quiero ver", "quiero mirar", "quiero revisar", "quiero mostrar",
    "ver los", "ver las",
    "mostrar los", "mostrar las",
    "los de", "las de",
)

# Verbos que indican nueva busqueda — si la query empieza con uno de estos,
# NO es follow-up aunque sea corta.
# Excluimos "quiero" porque "quiero ver X" es refinamiento, no nueva búsqueda.
# Excluimos "ver " porque "ver los de mayor valor" también es refinamiento.
# En su lugar, se usa is_delta_query() para detectar estos casos.
NEW_SEARCH_VERBS: tuple[str, ...] = (
    "busca", "encuentra", "necesito", "consulta",
    "muestra procesos", "muestra contratos", "listar",
)

# Comandos de reset reconocidos.
RESET_COMMANDS: frozenset[str] = frozenset({"/reset", "reset", "nueva", "limpiar", "nuevo"})


@dataclass
class Turn:
    turn_id: str
    timestamp: str      # ISO UTC
    chat_id: str
    user_query: str
    response: str
    parsed_params: dict
    result_ids: list[str] = field(default_factory=list)
    trace_id: str = ""
    suggestions: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "turn_id": self.turn_id,
            "timestamp": self.timestamp,
            "chat_id": self.chat_id,
            "user_query": self.user_query,
            "response": self.response,
            "parsed_params": self.parsed_params,
            "result_ids": self.result_ids,
            "trace_id": self.trace_id,
            "suggestions": self.suggestions,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Turn":
        return cls(
            turn_id=d.get("turn_id", ""),
            timestamp=d.get("timestamp", ""),
            chat_id=d.get("chat_id", ""),
            user_query=d.get("user_query", ""),
            response=d.get("response", ""),
            parsed_params=d.get("parsed_params", {}),
            result_ids=d.get("result_ids", []),
            trace_id=d.get("trace_id", ""),
            suggestions=d.get("suggestions", []),
        )

    def age_minutes(self) -> float:
        """Minutos transcurridos desde este turno. Retorna inf si timestamp invalido."""
        try:
            ts = datetime.fromisoformat(self.timestamp)
            now = datetime.now(timezone.utc)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return (now - ts).total_seconds() / 60
        except Exception:
            return float("inf")


class ConversationStore:
    """Almacena turnos conversacionales en JSONL, un archivo por chat_id."""

    def __init__(self, base_path: str | Path = "data/conversations"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def append_turn(
        self,
        chat_id: str,
        user_query: str,
        response: str,
        parsed_params: dict,
        result_ids: list[str] | None = None,
        trace_id: str = "",
        suggestions: list[dict] | None = None,
    ) -> str:
        """Agrega un turno al historial. Retorna turn_id."""
        turn = Turn(
            turn_id=uuid.uuid4().hex[:12],
            timestamp=datetime.now(timezone.utc).isoformat(),
            chat_id=chat_id,
            user_query=user_query,
            response=response,
            parsed_params=dict(parsed_params),
            result_ids=list(result_ids or []),
            trace_id=trace_id,
            suggestions=suggestions or [],
        )
        path = self._path_for(chat_id)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(turn.to_dict(), ensure_ascii=False, default=str) + "\n")
        return turn.turn_id

    def get_history(self, chat_id: str, last_n: int = 5) -> list[Turn]:
        """Retorna los ultimos last_n turnos. Lineas corruptas se omiten silenciosamente."""
        path = self._path_for(chat_id)
        if not path.exists():
            return []
        turns: list[Turn] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    turns.append(Turn.from_dict(json.loads(line)))
                except (json.JSONDecodeError, KeyError):
                    continue
        return turns[-last_n:]

    def get_last_suggestions(self, chat_id: str) -> list[dict] | None:
        """Retorna las suggestions del ultimo turno, o None si no hay historial."""
        history = self.get_history(chat_id, last_n=1)
        if not history:
            return None
        last = history[-1]
        return last.suggestions or None

    def clear(self, chat_id: str) -> None:
        """Elimina el historial del chat_id. No falla si no existe."""
        path = self._path_for(chat_id)
        if path.exists():
            path.unlink()

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _path_for(self, chat_id: str) -> Path:
        safe = _sanitize_chat_id(chat_id)
        return self.base_path / f"{safe}.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize_chat_id(chat_id: str) -> str:
    """
    Convierte chat_id a nombre de archivo seguro.
    Telegram IDs (enteros, pueden ser negativos) → "tg_<abs>"
    WhatsApp sender (+573...) → "wa_<digitos>"
    Streamlit session_id → "st_<alphanum>"
    Otros → reemplazar no-alphanum con _
    """
    s = str(chat_id).strip()

    # Telegram: empieza con "tg_" ya sanitizado, o es un entero (positivo/negativo)
    if s.startswith("tg_"):
        return s
    if re.fullmatch(r"-?\d+", s):
        return f"tg_{abs(int(s))}"

    # WhatsApp: empieza con "+" o "wa_"
    if s.startswith("wa_"):
        return s
    if s.startswith("+"):
        digits = re.sub(r"\D", "", s)
        return f"wa_{digits}"

    # Streamlit: empieza con "st_"
    if s.startswith("st_"):
        return s

    # Fallback: conservar alphanum y guiones bajos
    safe = re.sub(r"[^a-zA-Z0-9_\-]", "_", s)
    return safe[:64]


def merge_params(context_params: dict, new_params: dict) -> dict:
    """
    Fusiona new_params sobre context_params segun MERGE_RULES.
    Campos None en new_params se ignoran (conservar valor anterior).

    Si new_params solo trae delta keys (ordering_signal, fecha, valor, estado)
    sin objeto/entidad/departamento propio, conserva TODO el contexto anterior
    para esas claves ausentes.
    """
    # Delta keys: refinamientos que NO deben reemplazar contexto si vienen solos
    _DELTA_KEYS = frozenset({
        "ordering_signal", "fecha_desde", "fecha_hasta",
        "valor_min", "valor_max",
        "estado", "estado_field", "estado_family",
        "estado_contrato", "estado_de_apertura_del_proceso", "estado_del_procedimiento",
    })
    has_real_content = any(
        k not in _DELTA_KEYS and k != "dataset" and k != "dataset_explicit"
        for k, v in new_params.items() if v not in (None, [], "", {}, False)
    )

    merged = dict(context_params)
    for key, new_val in new_params.items():
        if new_val is None:
            continue  # parser no detecto nada → conservar anterior
        rule = MERGE_RULES.get(key, "replace")
        if rule == "replace":
            merged[key] = new_val
        elif rule == "keep_previous_unless_explicit":
            # Para dataset: si no fue explicitamente mencionado, conservar anterior
            if key == "dataset" and new_params.get("dataset_explicit") is False:
                continue
            if new_val:  # solo sobreescribir si el nuevo valor es truthy
                merged[key] = new_val
            # else: ya esta en merged del contexto anterior

    # Si new_params es puro delta (sin objeto, entidad, departamento propios),
    # asegurar que esas claves se conserven del contexto anterior
    if not has_real_content:
        for preserve_key in ("objeto", "entidad", "entidad_resolved",
                              "departamento", "departamento_resolved",
                              "ciudad", "contratista"):
            if preserve_key not in merged and preserve_key in context_params:
                merged[preserve_key] = context_params[preserve_key]

    # Clean up dataset_explicit from final merged params
    merged.pop("dataset_explicit", None)
    return merged


def is_reset_command(text: str) -> bool:
    """Retorna True si el texto es un comando de reset."""
    return text.strip().lower() in RESET_COMMANDS


def normalize_pagination_text(text: str) -> str:
    """Normaliza texto para detección de paginación pura."""
    t = text.strip().lower()
    # Quitar tildes comunes
    for char, repl in ("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"):
        t = t.replace(char, repl)
    # Quitar puntuación que suele acompañar cortesías: "más, por favor"
    t = re.sub(r"[¿?¡!.,;:]+", " ", t)
    # Quitar cortesías conocidas sin tocar contenido semántico
    t = re.sub(r"\b(?:por favor|gracias|please|pls|porfa|dale)\b", " ", t)
    # Compactar espacios
    t = re.sub(r"\s+", " ", t).strip()
    return t


def is_pagination_phrase(text: str) -> bool:
    """Retorna True solo para paginación pura, sin filtros nuevos."""
    t = normalize_pagination_text(text)
    # Fullmatch intencional: si quedan tokens sustantivos después de "más",
    # es un refinamiento normal, no paginación.
    _PAG_PATTERNS = re.compile(
        r"^(?:muestrame mas|muestra mas|dame mas|ver mas|"
        r"mas resultados|siguientes|los siguientes|"
        r"otros 10|otros diez|continuar|sigue|"
        r"siguiente pagina|quiero ver mas|"
        r"muestrame los siguientes)$",
        re.IGNORECASE,
    )
    return bool(_PAG_PATTERNS.fullmatch(t))


def is_followup(query: str, last_turn: Turn | None) -> bool:
    """
    Detecta si query es un follow-up del turno anterior.
    Requiere turno reciente (<30 min). Sin turno previo o turno viejo → False.

    Condicion A: query empieza con palabra de continuacion.
    Condicion C: query es delta puro (solo refinamiento).
    Condicion B: query corta (<8 palabras) sin verbo de nueva busqueda.
    """
    if last_turn is None:
        return False
    if last_turn.age_minutes() > FOLLOWUP_WINDOW_MINUTES:
        return False

    q = query.strip().lower()

    # Strip leading noise tokens (greetings, fillers) that won't affect meaning
    _NOISE_PREFIXES = ("hola ", "holaa ", "holaaa ", "buenas ", "buenass ", "ok ", "okay ", "oye ", "ey ")
    while any(q.startswith(p) for p in _NOISE_PREFIXES):
        for p in _NOISE_PREFIXES:
            if q.startswith(p):
                q = q[len(p):]
                break

    # Condicion A: continuation word (ej: "quiero ver", "los de", "ver los")
    for word in CONTINUATION_WORDS:
        if q.startswith(word):
            # Si es "quiero ver" + busqueda completa, NO es follow-up
            if word in ("quiero ver", "quiero mirar", "quiero revisar", "quiero mostrar"):
                rest = q[len(word):].strip()
                has_dataset_kw = any(kw in rest for kw in ("contrato", "proceso", "licitacion"))
                has_content = len(rest.split()) >= 3
                if has_dataset_kw and has_content:
                    return False
            return True

    # Condicion C: delta query — refinamiento puro sin nueva búsqueda
    if _is_delta_query(q):
        return True

    # Condicion B: corta y sin verbo de nueva busqueda — solo si es delta puro
    words = q.split()
    if len(words) < 8:
        for verb in NEW_SEARCH_VERBS:
            if q.startswith(verb):
                return False
        # Solo marcar como follow-up si es corta Y es delta (refinamiento puro)
        # Consultas cortas como "contratos de mantenimiento" NO son follow-up
        if _is_delta_query(q):
            return True

    return False


def _is_delta_query(q: str) -> bool:
    """Detecta si la query es puro refinamiento (orden, fecha, estado, valor)
    sin ser una búsqueda nueva con objeto/entidad/departamento.

    Retorna True si matchea algún patrón de delta y NO parece búsqueda nueva.
    """
    # Números de selección (1, 2, 3)
    if q in ("1", "2", "3"):
        return True

    # Ordenamiento: mayor valor, más caros, más altos, etc.
    _DELTA_ORDERING = re.compile(
        r"\b(?:mayor\s+valor|mayor\s+cuant(?:ia|ía)"
        r"|m[áa]s\s+(?:caro|caros|alta|altas|alto|altos|grande|grandes|reciente|recientes|nuevo|nuevos)"
        r"|menos\s+(?:barato|baratos|bajo|bajos)"
        r"|los\s+(?:d[ée]\s+)?(?:mayor|m[áa]s)\s+\w*valor?\w*"
        r"|ordena|ordenalos|ord[eé]nalos"
        r"|por\s+valor|por\s+fecha|por\s+cuant(?:ia|ía)"
        r")\b",
        re.IGNORECASE,
    )

    # Fecha simple: "en 2026", "en 2025", "de 2024", "2026"
    _DELTA_DATE = re.compile(r"\b(?:en\s+|de\s+)?20\d{2}\b")

    # Estado simple: "firmados", "en ejecucion", etc.
    _DELTA_STATE = re.compile(
        r"\b(?:firmados|en\s+ejecuci[oó]n|ejecuci[oó]n|suspendidos|terminados"
        r"|abiertos|cerrados|cancelados|liquidados|celebrados)"
        r"\b",
        re.IGNORECASE,
    )

    # Si tiene palabras de búsqueda nueva (objeto, entidad concreta), NO es delta
    _NEW_SEARCH_SIGNALS = re.compile(
        r"\b(?:contratos?\s+d[ée]\s+|procesos?\s+d[ée]\s+|licitaciones?\s+d[ée]\s+)"
        r"\w{4,}",  # seguido de palabra sustantiva
        re.IGNORECASE,
    )

    # También detectar palabras sustantivas sueltas (>=5 chars) que NO sean
    # palabras delta conocidas — si existen, probablemente es búsqueda nueva
    _CONTENT_WORD_RE = re.compile(r"\b[a-záéíóúñ]{5,}\b", re.IGNORECASE)
    _DELTA_WORDS = frozenset({
        "mayor", "mayores", "valor", "cuantia", "cuantía",
        "caro", "caros", "cara", "caras",
        "alto", "altos", "alta", "altas",
        "grande", "grandes",
        "reciente", "recientes",
        "nuevo", "nuevos", "nueva", "nuevas",
        "barato", "baratos", "bajo", "bajos",
        "firmados", "ejecucion", "ejecución",
        "suspendidos", "terminados",
        "abiertos", "cerrados", "cancelados",
        "liquidados", "celebrados",
        "ordena", "ordenalos", "ordénalos",
        "fecha", "fechas",
        "primero", "primera", "ultimo", "ultimos",
        # Conversacional (no afectan el significado de búsqueda)
        "ahora", "muestrame", "muestreme", "muestra", "mostrar",
        "quiero", "necesito", "dame", "dime", "busca", "buscar",
        "entendi", "entendido", "listo", "vamos",
        "gracias", "favor", "porfa",
    })

    if _NEW_SEARCH_SIGNALS.search(q):
        return False  # tiene objeto concreto → es búsqueda nueva

    # Si tiene palabras sustantivas que no son delta, no es refinamiento
    content_words = _CONTENT_WORD_RE.findall(q)
    non_delta_words = [w for w in content_words if w.lower() not in _DELTA_WORDS]
    if non_delta_words:
        return False  # tiene contenido sustantivo → búsqueda nueva

    # Si solo tiene señales de refinamiento, es delta
    has_ordering = bool(_DELTA_ORDERING.search(q))
    has_date = bool(_DELTA_DATE.search(q))
    has_state = bool(_DELTA_STATE.search(q))
    # Palabras totales <= 5 y solo refinamiento
    words = q.split()
    is_short = len(words) <= 8  # ligeramente más permisivo para queries con prefijo

    return (has_ordering or has_date or has_state) and is_short

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
import os
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from app.core._followup_constants import (
    FOLLOWUP_WINDOW_MINUTES,
    CONTINUATION_WORDS,
    NEW_SEARCH_VERBS,
    NOISE_PREFIXES,
    RESET_COMMANDS,
)
from app.core.conversation_envelope import (
    ConversationEnvelope,
    EnvelopeError,
    EnvelopeRead,
    EnvelopeStatus,
    now_iso,
)
from app.core.derived_execution_state import strip_derived_execution_state
from app.core.pagination_buffer import PaginationBuffer, pagination_fingerprint
from app.core.store_lock import conversation_lock


# ---------------------------------------------------------------------------
# Reglas de merge para follow-up detection (v1.2 MVP)
# DEPRECATED — ver followup_engine.FollowupMerger (H3/ADR-001).
# Las políticas de merge están en app/core/followup_engine.py.
# merge_params() se mantiene por compatibilidad con código que lo importe.
# ---------------------------------------------------------------------------
MERGE_RULES: dict[str, str] = {  # noqa: B006 — dict inmutable a efectos prácticos
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

# Re-exportadas desde _followup_constants.py (fuente única de verdad H3).
# Se mantienen como alias a nivel de módulo para backward compat.


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
    # Etapa 4: el Turn ES el ExecutionSnapshot. Persistido a JSONL y releído como
    # copia fresca en cada get_history → inmutable de facto (un frame futuro no puede
    # mutar el histórico). created_at_utc = timestamp; la duración se mide aparte.
    outcome: str = ""
    outcome_reason: str = ""
    result_count: int = 0
    query_fingerprint: str = ""

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
            "outcome": self.outcome,
            "outcome_reason": self.outcome_reason,
            "result_count": self.result_count,
            "query_fingerprint": self.query_fingerprint,
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
            outcome=d.get("outcome", ""),
            outcome_reason=d.get("outcome_reason", ""),
            result_count=d.get("result_count", 0),
            query_fingerprint=d.get("query_fingerprint", ""),
        )

    def had_results(self) -> bool:
        """True si este turno entregó resultados (para detectar zero-after-refinement)."""
        from app.core.outcome import Outcome
        if self.outcome:
            return self.outcome in (Outcome.SUCCESS, Outcome.PARTIAL)
        return bool(self.result_ids)  # compat con turnos previos a Etapa 4

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


DEFAULT_CONVERSATION_PATH = Path("data/conversations")

# El sobre publicado vive en su propio JSONL append-only, junto al historial.
# No sustituye a `append_turn`: la ruta V1.1 sigue escribiendo su turno, y la
# V1.2 publica ADEMÁS un registro versionado completo. Mezclarlos habría
# obligado a que un lector viejo entendiera el sobre nuevo, que es justo lo que
# la prueba de rollback debe poder rechazar.
ENVELOPE_SUFFIX = ".envelopes.jsonl"


class StaleRevisionError(RuntimeError):
    """Otro escritor publicó primero. No se pisa: se falla y se informa."""


@dataclass(frozen=True)
class TurnReservation:
    """El turno reservado ANTES de ejecutar: sus IDs y la revisión que vio."""

    chat_id: str
    turn_id: str
    result_id: str
    expected_revision: int
    previous: EnvelopeRead

    @property
    def revision(self) -> int:
        """La revisión que este turno va a publicar si nadie se adelanta."""
        return self.expected_revision + 1



class ConversationStore:
    """Almacena turnos conversacionales en JSONL, un archivo por chat_id."""

    def __init__(self, base_path: str | Path | None = None):
        configured = os.getenv("CONVERSATION_STORE_DIR")
        self.base_path = Path(base_path or configured or DEFAULT_CONVERSATION_PATH)
        if (
            os.getenv("PYTEST_CURRENT_TEST")
            and self.base_path.resolve() == DEFAULT_CONVERSATION_PATH.resolve()
        ):
            raise RuntimeError(
                "Tests may not write to data/conversations; set CONVERSATION_STORE_DIR "
                "or inject base_path=tmp_path."
            )
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._pagination_lock = threading.RLock()

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
        outcome: str = "",
        outcome_reason: str = "",
        result_count: int = 0,
        query_fingerprint: str = "",
    ) -> str:
        """Agrega un turno al historial. Retorna turn_id."""
        turn = Turn(
            turn_id=uuid.uuid4().hex[:12],
            timestamp=datetime.now(timezone.utc).isoformat(),
            chat_id=chat_id,
            user_query=user_query,
            response=response,
            # INV-PURE-ORDERING-PRESERVES-SOURCE-UNIVERSE: el historial guarda
            # lo que el usuario pidió, no lo que el motor derivó después de
            # ejecutar. El filtro vive aquí y no en los cuatro handlers que
            # escriben turnos porque el invariante es del historial: lo que
            # entra aquí es exactamente lo que el próximo follow-up hereda.
            parsed_params=strip_derived_execution_state(dict(parsed_params)),
            result_ids=list(result_ids or []),
            trace_id=trace_id,
            suggestions=suggestions or [],
            outcome=outcome,
            outcome_reason=outcome_reason,
            result_count=result_count,
            query_fingerprint=query_fingerprint,
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

    def last_successful_turn(self, chat_id: str, *, window: int = 10) -> "Turn | None":
        """Último turno con resultados (referencia para deshacer/ampliar un refinamiento).

        No es una copia paralela de params: es una referencia al snapshot ya
        persistido. ``failed_refinement_delta`` se calcula como diff contra su frame.
        """
        for turn in reversed(self.get_history(chat_id, last_n=window)):
            if turn.had_results():
                return turn
        return None

    def clear(self, chat_id: str) -> None:
        """Elimina buffer primero y luego historial (orden seguro ante crash)."""
        with self._pagination_lock:
            pagination_path = self._pagination_path_for(chat_id)
            if pagination_path.exists():
                pagination_path.unlink()
            path = self._path_for(chat_id)
            if path.exists():
                path.unlink()
        # Un reset explícito invalida también la continuidad publicada: dejar
        # el sobre vivo mantendría un contrato que referencia un historial que
        # ya no existe.
        envelope_path = self._envelope_path_for(chat_id)
        with conversation_lock(envelope_path):
            if envelope_path.exists():
                envelope_path.unlink()

    def save_pagination_buffer(self, chat_id: str, buffer: PaginationBuffer) -> None:
        """Persiste atomicamente un snapshot por grupo, conservando sus hermanos."""
        with self._pagination_lock:
            path = self._pagination_path_for(chat_id)
            payload = self._read_pagination_payload(path)
            payload[buffer.group] = buffer.to_dict()
            self._write_pagination_payload(path, payload)

    def get_pagination_buffer(
        self,
        chat_id: str,
        group: str,
        query_fingerprint: str | None,
        *,
        now: datetime | None = None,
    ) -> PaginationBuffer | None:
        """Lee un buffer vigente; mismatch o TTL vencido lo invalidan y eliminan."""
        with self._pagination_lock:
            path = self._pagination_path_for(chat_id)
            payload = self._read_pagination_payload(path)
            raw = payload.get(group)
            if not isinstance(raw, dict):
                return None
            # Un sidecar sin conversación solo puede provenir de un crash entre
            # save_pagination_buffer y append_turn. Es inerte y se elimina al leer.
            if not self.get_history(chat_id, last_n=1):
                self._delete_pagination_group(path, payload, group)
                return None
            try:
                buffer = PaginationBuffer.from_dict(raw)
            except (KeyError, TypeError, ValueError):
                self._delete_pagination_group(path, payload, group)
                return None
            expected_fingerprint = query_fingerprint or pagination_fingerprint(buffer.query_params)
            if buffer.query_fingerprint != expected_fingerprint or buffer.is_expired(now):
                self._delete_pagination_group(path, payload, group)
                return None
            return buffer

    def delete_pagination_buffer(self, chat_id: str, group: str) -> None:
        with self._pagination_lock:
            path = self._pagination_path_for(chat_id)
            payload = self._read_pagination_payload(path)
            self._delete_pagination_group(path, payload, group)

    def invalidate_pagination_buffers(self, chat_id: str) -> None:
        """Drop every cached result universe before a semantic search turn.

        A sidecar is only a reproducible pagination cache; conversation history
        remains authoritative and untouched. Removing the whole sidecar also
        prevents a stale sibling group from a previous dual-dataset query from
        surviving a later single-dataset refinement.
        """
        with self._pagination_lock:
            path = self._pagination_path_for(chat_id)
            if path.exists():
                path.unlink()

    # ------------------------------------------------------------------
    # Publicación versionada (V1.2)
    # ------------------------------------------------------------------

    def reserve_turn(self, chat_id: str) -> TurnReservation:
        """Lee la revisión vigente y reserva serialmente el turno del chat.

        Los IDs se asignan AQUÍ, antes de ejecutar, porque el contrato de
        continuación tiene que referenciarlos y una referencia no puede
        apuntar a algo que todavía no tiene nombre.
        """
        path = self._envelope_path_for(chat_id)
        with conversation_lock(path):
            read = self._read_envelope(path)
        return TurnReservation(
            chat_id=chat_id,
            turn_id=uuid.uuid4().hex[:12],
            result_id=uuid.uuid4().hex[:12],
            expected_revision=read.revision,
            previous=read,
        )

    def publish_turn(
        self,
        reservation: TurnReservation,
        *,
        user_query: str,
        response: str,
        profile_state: dict | None = None,
        continuation: dict | None = None,
        execution_record: dict | None = None,
    ) -> ConversationEnvelope:
        """Publica respuesta, estado, contrato y ejecución como UN registro.

        `expected_revision` se comprueba BAJO la exclusión, no antes de pedirla:
        comprobarla fuera deja la ventana en la que otro escritor publica entre
        la lectura y la escritura.
        """
        envelope = ConversationEnvelope(
            revision=reservation.revision,
            turn_id=reservation.turn_id,
            result_id=reservation.result_id,
            chat_id=reservation.chat_id,
            user_query=user_query,
            response=response,
            timestamp=now_iso(),
            profile_state=profile_state,
            continuation=continuation,
            execution_record=execution_record,
        )
        line = envelope.to_json()
        path = self._envelope_path_for(reservation.chat_id)
        with conversation_lock(path):
            current = self._read_envelope(path).revision
            if current != reservation.expected_revision:
                raise StaleRevisionError(
                    f"expected revision {reservation.expected_revision}, "
                    f"found {current}",
                )
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        return envelope

    def load_envelope(self, chat_id: str) -> EnvelopeRead:
        """El último sobre publicado, o por qué no hay continuidad válida."""
        path = self._envelope_path_for(chat_id)
        with conversation_lock(path):
            return self._read_envelope(path)

    def _envelope_path_for(self, chat_id: str) -> Path:
        safe = _sanitize_chat_id(chat_id)
        return self.base_path / f"{safe}{ENVELOPE_SUFFIX}"

    @staticmethod
    def _read_envelope(path: Path) -> EnvelopeRead:
        if not path.exists():
            return EnvelopeRead(EnvelopeStatus.EMPTY)
        lines = [
            line for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if not lines:
            return EnvelopeRead(EnvelopeStatus.EMPTY)
        try:
            envelope = ConversationEnvelope.from_json(lines[-1])
        except EnvelopeError as exc:
            # Una cola corrupta invalida la continuidad. NO se retrocede a una
            # línea anterior: esa línea describía otro resultado, y reactivar su
            # cursor mostraría filas de un universo que ya no está en pantalla.
            return EnvelopeRead(
                EnvelopeStatus.CORRUPT, None, len(lines), str(exc),
            )
        if envelope.revision != len(lines):
            return EnvelopeRead(
                EnvelopeStatus.CORRUPT, None, len(lines),
                "revision does not match the published sequence",
            )
        return EnvelopeRead(EnvelopeStatus.OK, envelope, envelope.revision)

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _path_for(self, chat_id: str) -> Path:
        safe = _sanitize_chat_id(chat_id)
        return self.base_path / f"{safe}.jsonl"

    def _pagination_path_for(self, chat_id: str) -> Path:
        safe = _sanitize_chat_id(chat_id)
        return self.base_path / f"{safe}.pagination.json"

    @staticmethod
    def _read_pagination_payload(path: Path) -> dict:
        if not path.exists():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _write_pagination_payload(path: Path, payload: dict) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8",
        )
        tmp.replace(path)

    def _delete_pagination_group(self, path: Path, payload: dict, group: str) -> None:
        if group not in payload:
            return
        payload.pop(group, None)
        if payload:
            self._write_pagination_payload(path, payload)
        elif path.exists():
            path.unlink()


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
    """DEPRECATED — ver followup_engine.detect_and_merge() (H3/ADR-001).

    Se mantiene por backward compat con código que lo importa directamente.
    La lógica de merge canónica está en FollowupMerger.merge().
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
    """Detecta si hay contexto conversacional utilizable para apply_context.

    ADR-001 / H8 (2026-05-15): esta funcion ahora solo verifica prerrequisitos
    (turno reciente valido). La clasificacion granular de intencion la hace
    followup_engine.detect_and_merge() en apply_context.

    Sin turno previo o turno viejo → False.
    Con turno reciente → True (delegamos a detect_and_merge la clasificacion).
    """
    if last_turn is None:
        return False
    if last_turn.age_minutes() > FOLLOWUP_WINDOW_MINUTES:
        return False
    return True

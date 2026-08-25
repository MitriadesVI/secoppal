"""Aislamiento de persistencia para runners de auditoría (offline / read-only).

Contrato — INV-AUDIT-RUNNER-NO-PRODUCTION-WRITES:

    Ejecutar un runner de auditoría NUNCA debe escribir en los stores reales
    del proyecto (``data/`` del checkout), sin importar el entorno del usuario
    ni el directorio de trabajo. La seguridad es el comportamiento por defecto:
    no depende de exportar variables antes de invocar el script.

El aislamiento vive aquí y no en ``app/``: es una propiedad del arnés de
auditoría, no del runtime. ``app.config.Settings`` conserva sus defaults de
producción intactos; este módulo se limita a construir un directorio temporal
y a entregar (a) las variables de entorno que leen los stores que no reciben
``Settings`` y (b) los overrides explícitos con los que el runner construye
``Settings``.

Distinción deliberada entre dos temporales:

* **stores runtime** — efímeros, se borran siempre al salir (también ante
  excepción). Nadie los va a leer después de la corrida.
* **salida de auditoría** (``--out``) — la pide el usuario y sobrevive. El
  runner la resuelve por su cuenta; este módulo no la toca.

Recursos legítimamente de solo lectura (gazetteer de alias, índice SQLite de
actores) NO se redirigen: la auditoría necesita leerlos y el runtime los abre
en modo lectura.
"""
from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

# Stores escribibles que se configuran por entorno porque sus consumidores no
# reciben ``Settings`` (ConversationStore lo lee con os.getenv) o porque el
# valor debe estar fijado antes de importar ``app`` — nombre → subruta en tmp.
WRITABLE_ENV_STORES: dict[str, str] = {
    "SECOP_FEEDBACK_PATH": "feedback.jsonl",
    "SECOP_FEEDBACK_DB_PATH": "secoppal.db",
    "CONVERSATION_STORE_DIR": "conversations",
    # Default de producción None, pero un .env o un shell del usuario puede
    # apuntarlo a data/: se neutraliza igual que los demás.
    "MODALITY_FOLLOWUP_TELEMETRY_PATH": "modality_followup_telemetry.jsonl",
}

# Campos de ``Settings`` que apuntan a stores escribibles. Se pasan como
# argumentos explícitos al construir Settings para que el aislamiento no
# dependa de que pydantic lea el entorno.
WRITABLE_SETTINGS_FIELDS: dict[str, str] = {
    "secop_feedback_path": "feedback.jsonl",
    "secop_feedback_db_path": "secoppal.db",
    "modality_followup_telemetry_path": "modality_followup_telemetry.jsonl",
}


@dataclass(frozen=True)
class AuditIsolation:
    """Rutas efectivas del sandbox de una corrida de auditoría."""

    root: Path

    def path(self, name: str) -> Path:
        return self.root / name

    @property
    def settings_overrides(self) -> dict[str, Path]:
        """Overrides explícitos para ``Settings(**overrides)``."""
        return {
            field: self.root / sub
            for field, sub in WRITABLE_SETTINGS_FIELDS.items()
        }

    @property
    def conversation_dir(self) -> Path:
        return self.root / WRITABLE_ENV_STORES["CONVERSATION_STORE_DIR"]

    def contains(self, candidate: str | Path | None) -> bool:
        """True si ``candidate`` cae dentro del sandbox."""
        if candidate is None:
            return False
        try:
            return Path(candidate).resolve().is_relative_to(self.root.resolve())
        except OSError:
            return False


@contextmanager
def isolated_runtime_stores(prefix: str = "hca_audit_stores_") -> Iterator[AuditIsolation]:
    """Redirige todos los stores escribibles a un temporal y lo borra al salir.

    El entorno previo se restaura íntegro (incluida la ausencia de una
    variable) y el temporal se elimina también si el bloque revienta:
    ``TemporaryDirectory`` como context manager cubre ambos caminos.
    """
    saved: dict[str, str | None] = {
        name: os.environ.get(name) for name in WRITABLE_ENV_STORES
    }
    with tempfile.TemporaryDirectory(prefix=prefix) as tmp:
        root = Path(tmp)
        try:
            for name, sub in WRITABLE_ENV_STORES.items():
                os.environ[name] = str(root / sub)
            yield AuditIsolation(root=root)
        finally:
            for name, previous in saved.items():
                if previous is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = previous

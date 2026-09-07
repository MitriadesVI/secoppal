"""Synthetic compatible SQLite fixtures; never use the production directory."""
import json
import sqlite3
from pathlib import Path

from app.core.actor_name import normalize_actor_name
from app.data.actor_alias_index import build_actor_alias_index
from app.data.actor_alias_resolver import ActorNameResolver


NAME = "ana maria lopez"
EXACT = ("CC", "70000001", NAME)
CHALLENGERS = [("CC", "70000002", NAME + " perez"), ("CC", "70000003", NAME + " diaz")]


def resolver_for(path: Path, rows, **kwargs):
    # Direct compatible rows deliberately bypass acquisition's dirty-bucket
    # filters, so dense SAME-IDENTITY fixtures really exercise runtime reads.
    build_actor_alias_index([], path, source_dataset="synthetic-prefix-v1",
                            built_at="2026-09-06T00:00:00+00:00")
    with sqlite3.connect(path) as connection:
        connection.executemany(
            "INSERT INTO actor_aliases(normalized_alias,raw_alias,raw_aliases_json,"
            "document_type,base_number,occurrences,source,disposition) "
            "VALUES (?,?,?,?,?,1,'synthetic','INDEXED_STABLE')",
            [(normalize_actor_name(name), name.upper(), json.dumps([name.upper()]), kind, number)
             for kind, number, name in rows],
        )
        connection.execute("INSERT INTO actor_aliases_fts(actor_aliases_fts) VALUES ('rebuild')")
        assert connection.execute("SELECT COUNT(*) FROM actor_aliases").fetchone()[0] == len(rows)
    return ActorNameResolver(path, **kwargs)

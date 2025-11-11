"""Utility script to populate a Chroma collection with SECOP metadata."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import chromadb


def load_records(path: Path) -> Iterable[dict]:
    if path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, list):
            for item in data:
                yield item
    else:
        raise ValueError("Only JSON files are supported in this lightweight example")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Path to metadata JSON file")
    parser.add_argument("--collection", default="entities", help="Chroma collection name")
    args = parser.parse_args()

    client = chromadb.Client()
    collection = client.get_or_create_collection(args.collection)

    ids = []
    documents = []
    metadatas = []

    for idx, record in enumerate(load_records(args.source)):
        ids.append(str(idx))
        documents.append(record.get("name") or record.get("title") or "")
        metadatas.append(record)

    if not ids:
        raise SystemExit("No records found in source file")

    collection.add(ids=ids, documents=documents, metadatas=metadatas)
    print(f"Inserted {len(ids)} records into collection '{args.collection}'")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()

"""Entity resolution utilities using ChromaDB and Cohere reranking."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ResolvedEntity:
    """Represents the best matching SECOP entity."""

    name: str
    score: float
    metadata: Dict[str, Any]


class EntityResolver:
    """Resolve free-form entity mentions against a ChromaDB collection."""

    def __init__(
        self,
        chroma_client: Optional[Any] = None,
        collection_name: str = "entities",
        cohere_client: Optional[Any] = None,
    ) -> None:
        self._chroma_client = chroma_client
        self._cohere_client = cohere_client
        self._collection_name = collection_name

    def resolve(self, mention: str, top_k: int = 5) -> Optional[ResolvedEntity]:
        """Return the highest ranked entity for ``mention``."""

        logger.debug("Resolving entity for mention: %s", mention)
        candidates = self._search_chroma(mention, top_k)
        if not candidates:
            logger.debug("No candidates found in Chroma; returning None")
            return None

        if self._cohere_client is not None and len(candidates) > 1:
            candidates = self._rerank_with_cohere(mention, candidates)

        top_candidate = candidates[0]
        logger.debug("Resolved entity: %s (score %.2f)", top_candidate.name, top_candidate.score)
        return top_candidate

    # ------------------------------------------------------------------
    def _search_chroma(self, mention: str, top_k: int) -> List[ResolvedEntity]:
        if self._chroma_client is None:
            logger.debug("Chroma client not configured; using mention as entity")
            return [ResolvedEntity(name=mention, score=1.0, metadata={})]

        try:
            collection = self._chroma_client.get_or_create_collection(self._collection_name)
            results = collection.query(query_texts=[mention], n_results=top_k)
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Chroma query failed; falling back to mention")
            return [ResolvedEntity(name=mention, score=1.0, metadata={})]

        documents = results.get("documents") or [[]]
        metadatas = results.get("metadatas") or [[]]
        distances = results.get("distances") or [[]]
        candidates: List[ResolvedEntity] = []
        for doc, metadata, distance in zip(documents[0], metadatas[0], distances[0]):
            score = 1.0 - float(distance)
            candidates.append(ResolvedEntity(name=doc, score=score, metadata=metadata or {}))

        if not candidates:
            candidates.append(ResolvedEntity(name=mention, score=0.0, metadata={}))
        return candidates

    def _rerank_with_cohere(
        self, mention: str, candidates: List[ResolvedEntity]
    ) -> List[ResolvedEntity]:
        try:
            response = self._cohere_client.rerank(
                query=mention,
                documents=[{"text": c.name, "metadata": c.metadata} for c in candidates],
            )
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Cohere reranking failed; returning original candidates")
            return candidates

        id_to_candidate = {idx: c for idx, c in enumerate(candidates)}
        reranked: List[ResolvedEntity] = []
        for item in response.results:
            candidate = id_to_candidate.get(item.index)
            if candidate is None:
                continue
            reranked.append(
                ResolvedEntity(
                    name=candidate.name,
                    score=float(item.relevance_score),
                    metadata=candidate.metadata,
                )
            )

        return reranked or candidates


__all__ = ["ResolvedEntity", "EntityResolver"]

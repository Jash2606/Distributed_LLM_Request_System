"""
Strategy pattern — IEmbeddingProvider with two implementations:
  • MockEmbeddingProvider  — deterministic, hash-based, 384-dim unit vectors.
  • SentenceTransformerProvider — real model, drop-in swap (requires extra dep).
"""

import hashlib
from abc import ABC, abstractmethod

import numpy as np


class IEmbeddingProvider(ABC):
    @abstractmethod
    def embed(self, text: str) -> list[float]:
        ...

    @abstractmethod
    def dim(self) -> int:
        ...


class MockEmbeddingProvider(IEmbeddingProvider):
    """
    Deterministic embedding: sha256(normalize(text)) seeds an RNG that produces
    a reproducible 384-dim unit vector.  Same text → same vector every time,
    making cache-hit tests fully predictable without a real model.
    """

    _DIM = 384

    def embed(self, text: str) -> list[float]:
        norm = " ".join(text.lower().split())
        seed = int.from_bytes(hashlib.sha256(norm.encode()).digest()[:8], "big")
        rng = np.random.default_rng(seed)
        v = rng.standard_normal(self._DIM)
        v = v / np.linalg.norm(v)
        return v.tolist()

    def dim(self) -> int:
        return self._DIM


class SentenceTransformerProvider(IEmbeddingProvider):
    """
    Real model provider — swap in by setting EMBED_PROVIDER=sentence_transformer.
    Lazy-loads the model so test imports don't require the extra dependency.
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self._model_name = model_name
        self._model = None

    def _load(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self._model_name)
        return self._model

    def embed(self, text: str) -> list[float]:
        model = self._load()
        v = model.encode(text, normalize_embeddings=True)
        return v.tolist()

    def dim(self) -> int:
        return 384


def embedding_provider_factory(settings) -> IEmbeddingProvider:
    if settings.embed_provider == "sentence_transformer":
        return SentenceTransformerProvider()
    return MockEmbeddingProvider()

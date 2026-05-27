import numpy as np
import pytest
from src.services.embeddings import MockEmbeddingProvider


def test_deterministic():
    p = MockEmbeddingProvider()
    assert p.embed("hello world") == p.embed("hello world")


def test_different_texts_different_vectors():
    p = MockEmbeddingProvider()
    assert p.embed("quantum computing") != p.embed("pizza recipe")


def test_unit_vector():
    p = MockEmbeddingProvider()
    v = np.array(p.embed("test"))
    assert abs(np.linalg.norm(v) - 1.0) < 1e-6


def test_dim():
    p = MockEmbeddingProvider()
    assert len(p.embed("x")) == p.dim() == 384


def test_normalisation_insensitive_to_case_and_whitespace():
    p = MockEmbeddingProvider()
    assert p.embed("Hello   World") == p.embed("hello world")

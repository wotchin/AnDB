"""Unit tests for Semantic Histogram."""

import numpy as np
import pytest

from andb.sql.optimizer.semantic_histogram import SemanticHistogram


class MockEmbeddingModel:
    """Mock embedding model for testing."""

    def __init__(self, dim=64):
        self.dim = dim
        self._cache = {}

    def generate_embeddings(self, text_list, normalize_embeddings=False):
        embeddings = []
        for text in text_list:
            if text not in self._cache:
                np.random.seed(hash(text) % (2**31))
                emb = np.random.randn(self.dim).astype(np.float32)
                if normalize_embeddings:
                    norm = np.linalg.norm(emb)
                    if norm > 0:
                        emb = emb / norm
                self._cache[text] = emb
            embeddings.append(self._cache[text].copy())
        return np.array(embeddings, dtype=np.float32)


@pytest.fixture
def embedding_model():
    return MockEmbeddingModel(dim=64)


@pytest.fixture
def sample_texts():
    return [
        "Neural networks for image recognition",
        "Deep learning in computer vision",
        "Convolutional neural networks",
        "Natural language processing with transformers",
        "Text generation using GPT models",
        "Sentiment analysis of reviews",
        "Reinforcement learning for game playing",
        "Robot navigation using RL",
        "Graph neural networks",
        "Knowledge graph embedding methods",
    ]


def test_build_histogram(sample_texts, embedding_model):
    hist = SemanticHistogram(table_oid=1, column_name='abstract', n_clusters=3)
    hist.build(sample_texts, embedding_model)

    assert hist.is_built
    assert hist.total_count == len(sample_texts)
    assert hist.centroids.shape[0] == 3
    assert hist.weights.sum() == len(sample_texts)


def test_build_empty_histogram(embedding_model):
    hist = SemanticHistogram(table_oid=1, column_name='abstract')
    hist.build([], embedding_model)

    assert hist.is_built
    assert hist.total_count == 0


def test_estimate_selectivity(sample_texts, embedding_model):
    hist = SemanticHistogram(table_oid=1, column_name='abstract', n_clusters=3)
    hist.build(sample_texts, embedding_model)

    sel = hist.estimate_selectivity("neural networks", embedding_model, threshold=0.3)
    assert 0.0 <= sel <= 1.0


def test_estimate_selectivity_empty(embedding_model):
    hist = SemanticHistogram(table_oid=1, column_name='abstract')
    hist.build([], embedding_model)

    sel = hist.estimate_selectivity("test", embedding_model)
    assert sel == 0.5  # default for empty histogram


def test_estimate_cardinality(sample_texts, embedding_model):
    hist = SemanticHistogram(table_oid=1, column_name='abstract', n_clusters=3)
    hist.build(sample_texts, embedding_model)

    card = hist.estimate_cardinality("neural networks", embedding_model, threshold=0.3)
    assert isinstance(card, int)
    assert 0 <= card <= len(sample_texts)


def test_serialize_deserialize(sample_texts, embedding_model):
    hist = SemanticHistogram(table_oid=42, column_name='abstract', n_clusters=3)
    hist.build(sample_texts, embedding_model)

    # Serialize
    data = hist.serialize()
    assert len(data) > 0

    # Deserialize
    hist2 = SemanticHistogram.deserialize(data)
    assert hist2 is not None
    assert hist2.table_oid == 42
    assert hist2.column_name == 'abstract'
    assert hist2.total_count == hist.total_count
    assert np.allclose(hist2.centroids, hist.centroids)
    assert np.allclose(hist2.weights, hist.weights)


def test_serialize_empty():
    hist = SemanticHistogram(table_oid=1, column_name='test')
    data = hist.serialize()
    assert data == b''

    result = SemanticHistogram.deserialize(b'')
    assert result is None


def test_k_adjustment(embedding_model):
    """Test that k is adjusted when n < n_clusters."""
    texts = ["short text 1", "short text 2"]
    hist = SemanticHistogram(table_oid=1, column_name='text', n_clusters=32)
    hist.build(texts, embedding_model)

    assert hist.is_built
    assert hist.centroids.shape[0] <= len(texts)


def test_selectivity_range(sample_texts, embedding_model):
    """Test that selectivity is always in [0, 1]."""
    hist = SemanticHistogram(table_oid=1, column_name='abstract', n_clusters=3)
    hist.build(sample_texts, embedding_model)

    for threshold in [0.0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0]:
        sel = hist.estimate_selectivity("test query", embedding_model, threshold=threshold)
        assert 0.0 <= sel <= 1.0, f"Selectivity {sel} out of range for threshold {threshold}"

"""Unit tests for semantic filter physical operators."""

import pytest
from unittest.mock import MagicMock, patch

import numpy as np

from andb.executor.operator.physical.semantic_filter import (
    LLMFilterScan, CodegenFilterScan, EmbeddingFilterScan, HybridFilterScan,
    SemanticFilterBase,
)


class MockCondition:
    """Mock SemanticCondition for testing."""

    def __init__(self, condition_text="topic is about AI", threshold=None):
        self.condition = condition_text
        self.threshold = threshold
        self.table_columns = []


class MockColumn:
    def __init__(self, column_name):
        self.column_name = column_name
        self.table_name = 'test_table'


class MockChild:
    """Mock child operator that yields tuples."""

    def __init__(self, tuples, columns=None):
        self._tuples = tuples
        self.columns = columns or [MockColumn('text')]

    def open(self):
        pass

    def next(self):
        for t in self._tuples:
            yield t

    def close(self):
        pass


def test_llm_filter_scan_accuracy_score():
    condition = MockCondition()
    op = LLMFilterScan(condition)
    assert op.accuracy_score == 1.0
    assert op.name == 'LLMFilterScan'


def test_codegen_filter_scan_accuracy_score():
    condition = MockCondition()
    op = CodegenFilterScan(condition)
    assert op.accuracy_score == 0.75
    assert op.name == 'CodegenFilterScan'


def test_embedding_filter_scan_accuracy_score():
    condition = MockCondition()
    op = EmbeddingFilterScan(condition)
    assert op.accuracy_score == 0.85
    assert op.name == 'EmbeddingFilterScan'


def test_hybrid_filter_scan_accuracy_score():
    condition = MockCondition()
    op = HybridFilterScan(condition)
    assert op.accuracy_score == 0.95
    assert op.name == 'HybridFilterScan'


def test_embedding_filter_threshold_from_condition():
    condition = MockCondition(threshold=0.7)
    op = EmbeddingFilterScan(condition)
    assert op.threshold == 0.7


def test_hybrid_filter_threshold_from_condition():
    condition = MockCondition(threshold=0.8)
    op = HybridFilterScan(condition)
    assert op.strict_threshold == 0.8
    assert op.loose_threshold == 0.5  # max(0.1, 0.8 - 0.3)


def test_codegen_is_safe_code():
    condition = MockCondition()
    op = CodegenFilterScan(condition)

    # Safe code
    assert op._is_safe_code("def filter_fn(row): return 'ai' in row['text'].lower()")

    # Unsafe code (import)
    assert not op._is_safe_code("import os\ndef filter_fn(row): os.system('rm -rf /')")

    # Unsafe code (eval)
    assert not op._is_safe_code("def filter_fn(row): return eval(row['text'])")

    # Invalid syntax
    assert not op._is_safe_code("def filter_fn(row) return True")


def test_format_tuple_text():
    condition = MockCondition()
    op = LLMFilterScan(condition)
    columns = [MockColumn('title'), MockColumn('abstract')]
    tup = ('Neural Networks', 'A paper about deep learning')

    text = op._format_tuple_text(tup, columns)
    assert 'title: Neural Networks' in text
    assert 'abstract: A paper about deep learning' in text


@patch('andb.executor.operator.physical.semantic_filter.default_embedding_model')
def test_embedding_filter_scan_next(mock_embed_factory):
    """Test EmbeddingFilterScan with mock embedding model."""
    dim = 8
    mock_model = MagicMock()

    # Create distinct embeddings for predicate and tuples
    pred_emb = np.random.randn(1, dim).astype(np.float32)
    pred_emb = pred_emb / np.linalg.norm(pred_emb)

    # Create tuple embeddings: some close to predicate, some far
    close_emb = pred_emb + np.random.randn(1, dim).astype(np.float32) * 0.1
    close_emb = close_emb / np.linalg.norm(close_emb)
    far_emb = -pred_emb  # opposite direction
    far_emb = far_emb / np.linalg.norm(far_emb)

    mock_model.generate_embeddings = MagicMock(side_effect=[
        pred_emb,  # predicate embedding
        np.vstack([close_emb, far_emb])  # tuple embeddings
    ])
    mock_embed_factory.return_value = mock_model

    condition = MockCondition(threshold=0.5)
    op = EmbeddingFilterScan(condition, threshold=0.5)
    child = MockChild([('close text',), ('far text',)])
    op.children = [child]

    op.open()
    results = list(op.next())
    op.close()

    # At least the close embedding should pass
    assert len(results) >= 1

"""Unit tests for semantic join physical operators."""

import pytest
from unittest.mock import MagicMock, patch

import numpy as np

from andb.executor.operator.physical.semantic_join import (
    IndexedNLSemanticJoin, HashSemanticJoin, SemanticJoinBase,
)


class MockCondition:
    def __init__(self, condition_text="are these related?", threshold=None):
        self.condition = condition_text
        self.threshold = threshold
        self.table_columns = []


class MockColumn:
    def __init__(self, column_name):
        self.column_name = column_name
        self.table_name = 'test'


class MockChild:
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


def test_inlsj_properties():
    condition = MockCondition()
    op = IndexedNLSemanticJoin(condition, 'INNER', ['t1', 't2'], k_candidates=5)
    assert op.accuracy_score == 0.90
    assert op.name == 'IndexedNLSemanticJoin'
    assert op.k_candidates == 5


def test_hsj_properties():
    condition = MockCondition()
    op = HashSemanticJoin(condition, 'INNER', ['t1', 't2'],
                          n_hash_bits=8, n_tables=4)
    assert op.accuracy_score == 0.87
    assert op.name == 'HashSemanticJoin'
    assert op.n_hash_bits == 8
    assert op.n_tables == 4


def test_extract_text():
    condition = MockCondition()
    op = IndexedNLSemanticJoin(condition, 'INNER', ['t1', 't2'])
    cols = [MockColumn('title'), MockColumn('content')]
    tup = ('Hello', 'World')

    text = op._extract_text(tup, cols)
    assert 'Hello' in text
    assert 'World' in text


def test_collect_tuples():
    condition = MockCondition()
    op = IndexedNLSemanticJoin(condition, 'INNER', ['t1', 't2'])
    child = MockChild([('a',), ('b',), ('c',)])

    result = op._collect_tuples(child)
    assert len(result) == 3
    assert result[0] == ('a',)


def test_hsj_build_lsh():
    """Test LSH hash table construction."""
    condition = MockCondition()
    op = HashSemanticJoin(condition, 'INNER', ['t1', 't2'],
                          n_hash_bits=4, n_tables=2)

    dim = 8
    embeddings = np.random.randn(5, dim).astype(np.float32)

    hash_tables, hyperplanes = op._build_lsh(embeddings, dim)

    assert len(hash_tables) == 2
    assert len(hyperplanes) == 2

    # Each embedding should be in at least one bucket per table
    for table in hash_tables:
        total_entries = sum(len(v) for v in table.values())
        assert total_entries == 5


def test_hsj_probe_lsh():
    """Test LSH probe finds candidates."""
    condition = MockCondition()
    op = HashSemanticJoin(condition, 'INNER', ['t1', 't2'],
                          n_hash_bits=4, n_tables=2)

    dim = 8
    embeddings = np.random.randn(5, dim).astype(np.float32)

    hash_tables, hyperplanes = op._build_lsh(embeddings, dim)

    # Probe with one of the original embeddings - should find itself
    candidates = op._probe_lsh(embeddings[0], hash_tables, hyperplanes)
    assert 0 in candidates

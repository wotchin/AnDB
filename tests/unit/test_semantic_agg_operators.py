"""Unit tests for semantic aggregation physical operators."""

import pytest
from unittest.mock import MagicMock

from andb.executor.operator.physical.semantic_agg import (
    HashBasedSemanticAggregation, EmbeddingClusterAggregation,
    SemanticAggregationBase,
)
from andb.executor.operator.logical import FunctionColumn, TableColumn


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


def test_hash_based_agg_properties():
    tc = TableColumn('test', 'text')
    agg_fns = [FunctionColumn('count', [tc])]
    op = HashBasedSemanticAggregation(
        group_instruction='classify by topic',
        agg_functions=agg_fns,
        grouping_columns=[]
    )
    assert op.accuracy_score == 0.95
    assert op.name == 'HashBasedSemanticAggregation'


def test_embedding_cluster_agg_properties():
    tc = TableColumn('test', 'text')
    agg_fns = [FunctionColumn('count', [tc])]
    op = EmbeddingClusterAggregation(
        group_instruction='classify by topic',
        agg_functions=agg_fns,
        grouping_columns=[],
        k=5
    )
    assert op.accuracy_score == 0.80
    assert op.name == 'EmbeddingClusterAggregation'
    assert op.k == 5


def test_extract_text():
    tc = TableColumn('test', 'text')
    agg_fns = [FunctionColumn('count', [tc])]
    op = HashBasedSemanticAggregation(
        group_instruction='test',
        agg_functions=agg_fns,
        grouping_columns=[]
    )

    columns = [MockColumn('title'), MockColumn('content')]
    tup = ('AI Paper', 'Deep learning methods')

    text = op._extract_text(tup, columns)
    assert 'title: AI Paper' in text
    assert 'content: Deep learning methods' in text


def test_compute_aggregates_count():
    tc = TableColumn('test', 'text')
    count_fn = FunctionColumn('count', [tc])
    agg_fns = [count_fn]

    op = HashBasedSemanticAggregation(
        group_instruction='test',
        agg_functions=agg_fns,
        grouping_columns=[]
    )

    columns = [MockColumn('text')]
    group = [('hello',), ('world',), ('test',)]

    results = op._compute_aggregates(group, agg_fns, columns)
    assert len(results) == 1
    assert list(results.values())[0] == 3  # count = 3

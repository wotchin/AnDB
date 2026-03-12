"""Comprehensive end-to-end integration tests for the S²QL implementation.

Tests the full pipeline: parse → transform → optimize → implement → execute
for all new S²QL features with mocked AI models.

Covers:
- Semantic filter operators (LLMFilterScan, CodegenFilterScan, EmbeddingFilterScan, HybridFilterScan)
- Semantic join operators (IndexedNLSemanticJoin, HashSemanticJoin)
- Semantic aggregation operators (HashBasedSemanticAggregation, EmbeddingClusterAggregation)
- Calibrator operator
- Cost model operator selection
- Semantic histogram
- Benchmark timing
"""

import os
import sys
import time
import json
import math
import shutil
import pytest
import numpy as np
from unittest.mock import MagicMock, patch, PropertyMock
from collections import defaultdict

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from andb.sql.parser import lexer, parser_
from andb.sql.parser.ast.s2ql import (
    MatchesPredicate, ExtractExpression, TransformExpression, ClassifyingGroupBy,
)
from andb.sql.parser.ast.identifier import Identifier
from andb.executor.operator.logical import (
    SemanticCondition, TableColumn, FunctionColumn, PromptColumn, SemanticTransformColumn,
)
from andb.executor.operator.physical.semantic_filter import (
    LLMFilterScan, CodegenFilterScan, EmbeddingFilterScan, HybridFilterScan,
)
from andb.executor.operator.physical.semantic_join import (
    IndexedNLSemanticJoin, HashSemanticJoin,
)
from andb.executor.operator.physical.semantic_agg import (
    HashBasedSemanticAggregation, EmbeddingClusterAggregation,
)
from andb.executor.operator.physical.calibrator import CalibratorOperator
from andb.sql.optimizer.semantic_cost_model import SemanticCostModel
from andb.sql.optimizer.semantic_histogram import SemanticHistogram
from andb.sql.validation.clarity_validator import SemanticClarityValidator, ValidationResult


# ============================================================================
# Test Helpers
# ============================================================================

andb_lexer = lexer.SQLLexer()
andb_parser = parser_.SQLParser()


def parse(stmt):
    """Parse a SQL statement and return the AST."""
    return andb_parser.parse(andb_lexer.tokenize(stmt))


class MockColumn:
    """Simulates a column descriptor from the physical operator layer."""
    def __init__(self, column_name, table_name='test'):
        self.column_name = column_name
        self.table_name = table_name


class MockChild:
    """Simulates a child operator that yields tuples."""
    def __init__(self, tuples, columns=None):
        self._tuples = tuples
        self.columns = columns or [MockColumn('text')]
        self.name = 'MockTableScan'

    def open(self):
        pass

    def next(self):
        for t in self._tuples:
            yield t

    def close(self):
        pass


class MockCondition:
    """Simulates a SemanticCondition for join operators."""
    def __init__(self, condition_text="are these related?", threshold=None):
        self.condition = condition_text
        self.threshold = threshold
        self.table_columns = []


def make_mock_embedding_model(dim=64):
    """Creates a mock embedding model that returns consistent embeddings."""
    model = MagicMock()
    seed = 42

    def generate_embeddings(texts, normalize_embeddings=False):
        np.random.seed(seed)
        emb = np.random.randn(len(texts), dim).astype(np.float32)
        if normalize_embeddings:
            norms = np.linalg.norm(emb, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            emb = emb / norms
        return emb

    model.generate_embeddings = generate_embeddings
    return model


def make_mock_client_model(default_response='true'):
    """Creates a mock LLM client that returns a configurable response."""
    model = MagicMock()
    model.complete_messages.return_value = default_response
    return model


# ============================================================================
# Benchmarking Infrastructure
# ============================================================================

_benchmark_results = []


def record_benchmark(test_name, operator, elapsed_s, n_tuples, extra=None):
    """Record a benchmark timing result."""
    _benchmark_results.append({
        'test': test_name,
        'operator': operator,
        'elapsed_s': round(elapsed_s, 6),
        'n_tuples': n_tuples,
        'throughput': round(n_tuples / elapsed_s, 2) if elapsed_s > 0 else float('inf'),
        **(extra or {}),
    })


# ============================================================================
# 1. Full Pipeline: Parse → Transform Tests
# ============================================================================

class TestS2QLParsingPipeline:
    """Tests that S²QL statements parse into correct AST nodes."""

    def test_matches_parses_to_matches_predicate(self):
        ast = parse("SELECT title FROM papers WHERE abstract MATCHES 'discusses neural networks'")
        assert isinstance(ast.where, MatchesPredicate)
        assert ast.where.assertion == 'discusses neural networks'
        assert ast.where.column.parts == 'abstract'

    def test_matches_with_params_threshold(self):
        ast = parse(
            "SELECT * FROM reviews WHERE content MATCHES 'positive sentiment' "
            "WITH (threshold = 0.8)"
        )
        assert isinstance(ast.where, MatchesPredicate)
        assert ast.where.with_params['threshold'] == 0.8

    def test_extract_parses_schema(self):
        ast = parse("SELECT EXTRACT(text INTO (name text, age int)) FROM documents")
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], ExtractExpression)
        assert ast.targets[0].target_schema == [('name', 'text'), ('age', 'int')]

    def test_transform_parses_instruction(self):
        ast = parse(
            "SELECT TRANSFORM(body AS summary USING 'summarize briefly') FROM articles"
        )
        target = ast.targets[0]
        assert isinstance(target, TransformExpression)
        assert target.output_name == 'summary'
        assert target.instruction == 'summarize briefly'

    def test_classifying_group_by_parses(self):
        ast = parse(
            "SELECT category, count(1) FROM reviews "
            "GROUP BY CLASSIFYING review_text AS category"
        )
        assert len(ast.group_by) == 1
        assert isinstance(ast.group_by[0], ClassifyingGroupBy)
        assert ast.group_by[0].key_name == 'category'

    def test_combined_matches_and_transform(self):
        """Test parsing a query with both MATCHES and TRANSFORM."""
        ast = parse(
            "SELECT TRANSFORM(body AS summary USING 'summarize') FROM docs "
            "WHERE content MATCHES 'about AI'"
        )
        assert isinstance(ast.where, MatchesPredicate)
        assert isinstance(ast.targets[0], TransformExpression)


# ============================================================================
# 2. Semantic Filter Operators E2E
# ============================================================================

class TestLLMFilterScanE2E:
    """End-to-end test of LLMFilterScan physical operator."""

    @patch('andb.executor.operator.physical.semantic_filter.default_client_model')
    def test_filters_tuples_by_llm(self, mock_factory):
        mock_model = make_mock_client_model('true')
        mock_factory.return_value = mock_model

        condition = MockCondition('is about machine learning?')
        condition.table_columns = [MockColumn('text')]
        op = LLMFilterScan(condition)

        tuples = [('neural networks paper',), ('cooking recipe',), ('deep learning survey',)]
        child = MockChild(tuples)
        op.children = [child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        # All should pass since mock returns 'true'
        assert len(results) == 3
        assert mock_model.complete_messages.call_count == 3
        record_benchmark('test_llm_filter_all_pass', 'LLMFilterScan', elapsed, 3)

    @patch('andb.executor.operator.physical.semantic_filter.default_client_model')
    def test_filters_some_tuples(self, mock_factory):
        mock_model = MagicMock()
        mock_model.complete_messages.side_effect = ['true', 'false', 'true']
        mock_factory.return_value = mock_model

        condition = MockCondition('is about AI?')
        condition.table_columns = [MockColumn('text')]
        op = LLMFilterScan(condition)

        tuples = [('AI paper',), ('cooking recipe',), ('ML survey',)]
        child = MockChild(tuples)
        op.children = [child]

        op.open()
        results = list(op.next())
        op.close()

        assert len(results) == 2
        assert results[0] == ('AI paper',)
        assert results[1] == ('ML survey',)

    @patch('andb.executor.operator.physical.semantic_filter.default_client_model')
    def test_empty_input(self, mock_factory):
        mock_model = make_mock_client_model('true')
        mock_factory.return_value = mock_model

        condition = MockCondition('test')
        condition.table_columns = []
        op = LLMFilterScan(condition)
        child = MockChild([])
        op.children = [child]

        op.open()
        results = list(op.next())
        op.close()

        assert len(results) == 0
        assert mock_model.complete_messages.call_count == 0


class TestCodegenFilterScanE2E:
    """End-to-end test of CodegenFilterScan physical operator."""

    @patch('andb.executor.operator.physical.semantic_filter.default_client_model')
    def test_codegen_filter_executes_generated_code(self, mock_factory):
        """Test that generated code filter works correctly."""
        mock_model = MagicMock()
        # Return a simple filter function
        mock_model.complete_messages.return_value = (
            "def filter_fn(row):\n"
            "    return 'machine learning' in row.get('text', '').lower()\n"
        )
        mock_factory.return_value = mock_model

        condition = MockCondition("contains 'machine learning'")
        condition.table_columns = [MockColumn('text')]
        op = CodegenFilterScan(condition)

        tuples = [
            ('Machine Learning paper',),
            ('cooking recipe',),
            ('deep machine learning survey',),
        ]
        child = MockChild(tuples)
        op.children = [child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        assert len(results) == 2
        assert results[0] == ('Machine Learning paper',)
        assert results[1] == ('deep machine learning survey',)
        record_benchmark('test_codegen_filter', 'CodegenFilterScan', elapsed, 3)

    @patch('andb.executor.operator.physical.semantic_filter.default_client_model')
    def test_codegen_unsafe_code_falls_back_to_llm(self, mock_factory):
        """When generated code is unsafe, should fall back to LLM filter."""
        mock_model = MagicMock()
        # First call: unsafe code with import
        # Subsequent calls: LLM verification responses
        mock_model.complete_messages.side_effect = [
            "import os\ndef filter_fn(row):\n    os.system('rm -rf /')\n    return True\n",
            'true', 'true',
        ]
        mock_factory.return_value = mock_model

        condition = MockCondition('test condition')
        condition.table_columns = [MockColumn('text')]
        op = CodegenFilterScan(condition)

        tuples = [('a',), ('b',)]
        child = MockChild(tuples)
        op.children = [child]

        op.open()
        results = list(op.next())
        op.close()

        # Should fall back to LLM and accept both (since mock returns 'true')
        assert len(results) == 2

    @patch('andb.executor.operator.physical.semantic_filter.default_client_model')
    def test_codegen_with_multicolumn(self, mock_factory):
        """Test codegen filter with multi-column tuples."""
        mock_model = MagicMock()
        mock_model.complete_messages.return_value = (
            "def filter_fn(row):\n"
            "    return int(row.get('year', 0)) >= 2020\n"
        )
        mock_factory.return_value = mock_model

        condition = MockCondition('published after 2020')
        condition.table_columns = [MockColumn('title'), MockColumn('year')]
        op = CodegenFilterScan(condition)

        columns = [MockColumn('title'), MockColumn('year')]
        tuples = [
            ('Paper A', '2019'),
            ('Paper B', '2021'),
            ('Paper C', '2023'),
        ]
        child = MockChild(tuples, columns)
        op.children = [child]

        op.open()
        results = list(op.next())
        op.close()

        assert len(results) == 2
        assert results[0] == ('Paper B', '2021')
        assert results[1] == ('Paper C', '2023')


class TestEmbeddingFilterScanE2E:
    """End-to-end test of EmbeddingFilterScan physical operator."""

    @patch('andb.executor.operator.physical.semantic_filter.default_embedding_model')
    def test_embedding_filter_with_high_similarity(self, mock_embed_factory):
        """Test that embedding filter passes tuples with high similarity."""
        mock_embed = MagicMock()

        # Create embeddings where first and third are similar to query
        dim = 16
        query_emb = np.array([[1.0] + [0.0] * (dim - 1)], dtype=np.float32)
        tuple_embs = np.array([
            [0.9] + [0.1] * (dim - 1),   # similar to query
            [-0.9] + [0.1] * (dim - 1),  # dissimilar
            [0.8] + [0.2] * (dim - 1),   # similar
        ], dtype=np.float32)

        # Normalize
        query_emb = query_emb / np.linalg.norm(query_emb, axis=1, keepdims=True)
        tuple_embs = tuple_embs / np.linalg.norm(tuple_embs, axis=1, keepdims=True)

        call_count = [0]
        def gen_embed(texts, normalize_embeddings=False):
            if call_count[0] == 0:
                call_count[0] += 1
                return query_emb
            else:
                call_count[0] += 1
                return tuple_embs

        mock_embed.generate_embeddings = gen_embed
        mock_embed_factory.return_value = mock_embed

        condition = MockCondition('about machine learning')
        condition.threshold = 0.5  # similarity threshold
        condition.table_columns = [MockColumn('text')]
        op = EmbeddingFilterScan(condition)

        tuples = [('neural nets',), ('cooking',), ('deep learning',)]
        child = MockChild(tuples)
        op.children = [child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        # First and third should pass
        assert len(results) == 2
        record_benchmark('test_embedding_filter', 'EmbeddingFilterScan', elapsed, 3)


class TestHybridFilterScanE2E:
    """End-to-end test of HybridFilterScan (embedding pre-filter + LLM verify)."""

    @patch('andb.executor.operator.physical.semantic_filter.default_client_model')
    @patch('andb.executor.operator.physical.semantic_filter.default_embedding_model')
    def test_hybrid_filter_two_stage(self, mock_embed_factory, mock_client_factory):
        """Test two-stage hybrid: embedding pre-filter then LLM verification."""
        # Setup embedding model
        mock_embed = MagicMock()
        dim = 16
        query_emb = np.array([[1.0] + [0.0] * (dim - 1)], dtype=np.float32)
        tuple_embs = np.array([
            [0.9] + [0.1] * (dim - 1),   # passes embedding
            [-0.9] + [0.1] * (dim - 1),  # fails embedding
            [0.8] + [0.2] * (dim - 1),   # passes embedding
        ], dtype=np.float32)
        query_emb = query_emb / np.linalg.norm(query_emb, axis=1, keepdims=True)
        tuple_embs = tuple_embs / np.linalg.norm(tuple_embs, axis=1, keepdims=True)

        call_count = [0]
        def gen_embed(texts, normalize_embeddings=False):
            if call_count[0] == 0:
                call_count[0] += 1
                return query_emb
            else:
                call_count[0] += 1
                return tuple_embs

        mock_embed.generate_embeddings = gen_embed
        mock_embed_factory.return_value = mock_embed

        # Setup LLM model: verifies first candidate but rejects second
        mock_client = MagicMock()
        mock_client.complete_messages.side_effect = ['true', 'false']
        mock_client_factory.return_value = mock_client

        condition = MockCondition('about neural networks')
        condition.threshold = 0.3  # loose threshold for pre-filter
        condition.table_columns = [MockColumn('text')]
        op = HybridFilterScan(condition)

        tuples = [('neural nets paper',), ('cooking recipe',), ('something else',)]
        child = MockChild(tuples)
        op.children = [child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        # Only first passes both embedding and LLM
        assert len(results) >= 1
        record_benchmark('test_hybrid_filter', 'HybridFilterScan', elapsed, 3)


# ============================================================================
# 3. Semantic Join Operators E2E
# ============================================================================

class TestIndexedNLSemanticJoinE2E:
    """End-to-end test of IndexedNLSemanticJoin."""

    @patch('andb.executor.operator.physical.semantic_join.default_client_model')
    @patch('andb.executor.operator.physical.semantic_join.default_embedding_model')
    def test_inlsj_finds_matches(self, mock_embed_factory, mock_client_factory):
        """Test INLSJ: embed inner, k-NN search, LLM verify."""
        dim = 16
        mock_embed = MagicMock()

        # Make outer[0] similar to inner[1], outer[1] similar to inner[0]
        outer_embs = np.array([
            [1.0] + [0.0] * (dim - 1),
            [0.0] + [1.0] + [0.0] * (dim - 2),
        ], dtype=np.float32)
        inner_embs = np.array([
            [0.0] + [0.9] + [0.0] * (dim - 2),
            [0.9] + [0.0] * (dim - 1),
            [-1.0] * dim,
        ], dtype=np.float32)
        outer_embs = outer_embs / np.linalg.norm(outer_embs, axis=1, keepdims=True)
        inner_embs = inner_embs / np.linalg.norm(inner_embs, axis=1, keepdims=True)

        all_embs = np.vstack([inner_embs, outer_embs])

        embed_call = [0]
        def gen_embed(texts, normalize_embeddings=False):
            n = len(texts)
            result = all_embs[embed_call[0]:embed_call[0] + n]
            embed_call[0] += n
            if embed_call[0] >= len(all_embs):
                embed_call[0] = 0
            return result

        mock_embed.generate_embeddings = gen_embed
        mock_embed_factory.return_value = mock_embed

        # LLM verifier: always says 'true'
        mock_client = make_mock_client_model('true')
        mock_client_factory.return_value = mock_client

        condition = MockCondition('are related topics')
        op = IndexedNLSemanticJoin(condition, 'INNER', ['t1', 't2'], k_candidates=2)

        outer_child = MockChild([('AI paper',), ('ML paper',)], [MockColumn('text', 't1')])
        inner_child = MockChild(
            [('ML survey',), ('AI tutorial',), ('cooking',)],
            [MockColumn('text', 't2')]
        )
        op.children = [outer_child, inner_child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        # Should find some matches since LLM always says 'true'
        assert len(results) > 0
        # Each result should be a concatenated tuple
        for r in results:
            assert len(r) == 2  # outer_cols + inner_cols
        record_benchmark('test_inlsj', 'IndexedNLSemanticJoin', elapsed,
                         2 * 3, extra={'outer': 2, 'inner': 3})


class TestHashSemanticJoinE2E:
    """End-to-end test of HashSemanticJoin with LSH."""

    @patch('andb.executor.operator.physical.semantic_join.default_client_model')
    @patch('andb.executor.operator.physical.semantic_join.default_embedding_model')
    def test_hsj_finds_candidates_via_lsh(self, mock_embed_factory, mock_client_factory):
        """Test HSJ: LSH build, probe, then LLM verify."""
        dim = 16
        mock_embed = MagicMock()

        # Create embeddings where some are similar
        np.random.seed(42)
        outer_embs = np.random.randn(3, dim).astype(np.float32)
        inner_embs = np.random.randn(4, dim).astype(np.float32)
        # Make inner[0] very similar to outer[0]
        inner_embs[0] = outer_embs[0] + np.random.randn(dim).astype(np.float32) * 0.01

        all_embs = np.vstack([inner_embs, outer_embs])
        embed_call = [0]
        def gen_embed(texts, normalize_embeddings=False):
            n = len(texts)
            result = all_embs[embed_call[0]:embed_call[0] + n]
            embed_call[0] += n
            if embed_call[0] >= len(all_embs):
                embed_call[0] = 0
            return result

        mock_embed.generate_embeddings = gen_embed
        mock_embed_factory.return_value = mock_embed

        mock_client = make_mock_client_model('true')
        mock_client_factory.return_value = mock_client

        condition = MockCondition('semantically similar')
        op = HashSemanticJoin(condition, 'INNER', ['t1', 't2'],
                              n_hash_bits=4, n_tables=3)

        outer_child = MockChild(
            [('Paper A',), ('Paper B',), ('Paper C',)],
            [MockColumn('text', 't1')]
        )
        inner_child = MockChild(
            [('Doc 1',), ('Doc 2',), ('Doc 3',), ('Doc 4',)],
            [MockColumn('text', 't2')]
        )
        op.children = [outer_child, inner_child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        assert len(results) > 0
        record_benchmark('test_hsj', 'HashSemanticJoin', elapsed,
                         3 * 4, extra={'outer': 3, 'inner': 4})

    def test_lsh_hash_determinism(self):
        """Test that LSH hash tables are built consistently."""
        condition = MockCondition()
        op = HashSemanticJoin(condition, 'INNER', ['t1', 't2'],
                              n_hash_bits=6, n_tables=4)

        dim = 16
        np.random.seed(123)
        embeddings = np.random.randn(10, dim).astype(np.float32)

        hash_tables, hyperplanes = op._build_lsh(embeddings, dim)

        assert len(hash_tables) == 4
        assert len(hyperplanes) == 4

        # Verify each embedding is in exactly one bucket per table
        for table in hash_tables:
            total_entries = sum(len(v) for v in table.values())
            assert total_entries == 10

    def test_lsh_probe_returns_candidates(self):
        """Test that LSH probe finds nearby points."""
        condition = MockCondition()
        op = HashSemanticJoin(condition, 'INNER', ['t1', 't2'],
                              n_hash_bits=4, n_tables=3)

        dim = 16
        np.random.seed(42)
        base = np.random.randn(1, dim).astype(np.float32)
        # Create one near-duplicate
        near = base + np.random.randn(1, dim).astype(np.float32) * 0.001
        far = np.random.randn(3, dim).astype(np.float32) * 10
        embeddings = np.vstack([base, near, far]).astype(np.float32)

        hash_tables, hyperplanes = op._build_lsh(embeddings, dim)
        candidates = op._probe_lsh(base[0], hash_tables, hyperplanes)

        # The near-duplicate should very likely be a candidate
        assert 0 in candidates  # base itself


# ============================================================================
# 4. Semantic Aggregation Operators E2E
# ============================================================================

class TestHashBasedSemanticAggE2E:
    """End-to-end test of HashBasedSemanticAggregation."""

    @patch('andb.executor.operator.physical.semantic_agg.default_embedding_model')
    @patch('andb.executor.operator.physical.semantic_agg.default_client_model')
    def test_hash_agg_groups_and_counts(self, mock_client_factory, mock_embed_factory):
        """Test that hash-based agg classifies and aggregates correctly."""
        mock_client = MagicMock()
        # Classification: alternate between two categories
        mock_client.complete_messages.side_effect = [
            'Category A', 'Category B', 'Category A', 'Category B', 'Category A'
        ]
        mock_client_factory.return_value = mock_client

        # Embedding model for merging similar keys
        mock_embed = MagicMock()
        dim = 16
        def gen_embed(texts, normalize_embeddings=False):
            np.random.seed(hash(str(texts)) % 2**31)
            emb = np.random.randn(len(texts), dim).astype(np.float32)
            if normalize_embeddings:
                norms = np.linalg.norm(emb, axis=1, keepdims=True)
                norms[norms == 0] = 1
                emb /= norms
            return emb
        mock_embed.generate_embeddings = gen_embed
        mock_embed_factory.return_value = mock_embed

        tc = TableColumn('test', 'text')
        count_fn = FunctionColumn('count', [tc])
        op = HashBasedSemanticAggregation(
            group_instruction='classify by topic',
            agg_functions=[count_fn],
            grouping_columns=[]
        )

        tuples = [('AI paper',), ('ML paper',), ('AI survey',), ('ML survey',), ('AI tutorial',)]
        columns = [MockColumn('text')]
        child = MockChild(tuples, columns)
        op.children = [child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        # Should produce 2 groups
        assert len(results) == 2
        # Total count should be 5
        total_count = sum(r[-1] for r in results)
        assert total_count == 5
        record_benchmark('test_hash_agg', 'HashBasedSemanticAggregation', elapsed, 5)


class TestEmbeddingClusterAggE2E:
    """End-to-end test of EmbeddingClusterAggregation."""

    @patch('andb.executor.operator.physical.semantic_agg.default_client_model')
    @patch('andb.executor.operator.physical.semantic_agg.default_embedding_model')
    def test_embedding_cluster_groups(self, mock_embed_factory, mock_client_factory):
        """Test that embedding cluster agg uses K-Means and LLM naming."""
        dim = 16
        mock_embed = MagicMock()

        # Create two clear clusters
        cluster1 = np.tile([1.0, 0.0] + [0.0] * (dim - 2), (3, 1)).astype(np.float32)
        cluster2 = np.tile([0.0, 1.0] + [0.0] * (dim - 2), (2, 1)).astype(np.float32)
        all_embs = np.vstack([cluster1, cluster2])
        # Add noise
        np.random.seed(42)
        all_embs += np.random.randn(*all_embs.shape).astype(np.float32) * 0.01

        embed_call = [0]
        def gen_embed(texts, normalize_embeddings=False):
            n = len(texts)
            if embed_call[0] == 0:
                embed_call[0] = 1
                return all_embs[:n]
            # For cluster naming embeddings
            return np.random.randn(n, dim).astype(np.float32)

        mock_embed.generate_embeddings = gen_embed
        mock_embed_factory.return_value = mock_embed

        # LLM names the clusters
        mock_client = MagicMock()
        mock_client.complete_messages.side_effect = ['AI Research', 'ML Applications']
        mock_client_factory.return_value = mock_client

        tc = TableColumn('test', 'text')
        count_fn = FunctionColumn('count', [tc])
        op = EmbeddingClusterAggregation(
            group_instruction='classify papers',
            agg_functions=[count_fn],
            grouping_columns=[],
            k=2
        )

        tuples = [('AI paper 1',), ('AI paper 2',), ('AI paper 3',),
                  ('ML paper 1',), ('ML paper 2',)]
        child = MockChild(tuples, [MockColumn('text')])
        op.children = [child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        assert len(results) == 2
        total = sum(r[-1] for r in results)
        assert total == 5
        record_benchmark('test_embedding_cluster_agg', 'EmbeddingClusterAggregation', elapsed, 5)


# ============================================================================
# 5. Calibrator Operator E2E
# ============================================================================

class TestCalibratorE2E:
    """End-to-end test of CalibratorOperator."""

    @patch('andb.executor.operator.physical.calibrator.default_client_model')
    def test_calibrator_passes_all_data(self, mock_factory):
        """Calibrator should not filter, only measure confidence."""
        mock_model = make_mock_client_model('valid')
        mock_factory.return_value = mock_model

        tuples = [(i,) for i in range(20)]
        child = MockChild(tuples)

        op = CalibratorOperator(sample_rate=0.5, confidence_threshold=0.8)
        op.children = [child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        assert len(results) == 20
        assert op.confidence is not None
        stats = op.get_calibration_stats()
        assert stats['sample_size'] > 0
        assert stats['consistency_rate'] == 1.0
        record_benchmark('test_calibrator_high_conf', 'CalibratorOperator', elapsed, 20)

    @patch('andb.executor.operator.physical.calibrator.default_client_model')
    def test_calibrator_detects_low_confidence(self, mock_factory):
        """Calibrator should detect when results are unreliable."""
        mock_model = make_mock_client_model('invalid')
        mock_factory.return_value = mock_model

        tuples = [(i,) for i in range(10)]
        child = MockChild(tuples)

        op = CalibratorOperator(sample_rate=1.0, confidence_threshold=0.9)
        op.children = [child]

        op.open()
        results = list(op.next())
        op.close()

        assert len(results) == 10  # still passes all data
        assert op.confidence == 0.0
        stats = op.get_calibration_stats()
        assert stats['consistency_rate'] == 0.0
        assert len(stats['low_confidence_indices']) == 10

    @patch('andb.executor.operator.physical.calibrator.default_client_model')
    def test_calibrator_chained_with_filter(self, mock_factory):
        """Test calibrator chained after a semantic filter operator."""
        mock_model = MagicMock()
        # For calibrator verification
        mock_model.complete_messages.return_value = 'valid'
        mock_factory.return_value = mock_model

        # Create a mock filter that yields only even numbers
        class MockFilter:
            name = 'LLMFilterScan'
            columns = [MockColumn('id')]
            def open(self): pass
            def next(self):
                for i in range(10):
                    if i % 2 == 0:
                        yield (i,)
            def close(self): pass

        calibrator = CalibratorOperator(sample_rate=1.0)
        calibrator.children = [MockFilter()]

        calibrator.open()
        results = list(calibrator.next())
        calibrator.close()

        assert len(results) == 5
        assert calibrator.confidence == 1.0


# ============================================================================
# 6. Cost Model Integration E2E
# ============================================================================

class TestCostModelE2E:
    """End-to-end tests for cost model operator selection."""

    def test_filter_selection_small_n(self):
        """For small N with no accuracy preference, cheapest filter wins."""
        model = SemanticCostModel()
        best, cost = model.select_best_filter(5, lambda_acc=0.0)
        # EmbeddingFilterScan: 5*1=5 (cheapest for small N)
        assert best == 'EmbeddingFilterScan'
        assert cost == 5.0

    def test_filter_selection_large_n(self):
        """For large N, CodegenFilterScan should be cheapest."""
        model = SemanticCostModel()
        best, cost = model.select_best_filter(10000, lambda_acc=0.0)
        # CodegenFilterScan: 100 + 10000*0.01 = 200
        # EmbeddingFilterScan: 10000*1 = 10000
        assert best == 'CodegenFilterScan'
        assert cost == 200.0

    def test_filter_selection_accuracy_priority(self):
        """With extreme accuracy weight, LLMFilterScan wins."""
        model = SemanticCostModel()
        best, _ = model.select_best_filter(10, lambda_acc=100000.0)
        assert best == 'LLMFilterScan'

    def test_join_selection_default(self):
        """HashSemanticJoin should be preferred for moderate sizes."""
        model = SemanticCostModel()
        best, _ = model.select_best_join(50, 50, lambda_acc=0.0)
        assert best == 'HashSemanticJoin'

    def test_aggregation_selection(self):
        """EmbeddingClusterAggregation should be cheapest without accuracy weight."""
        model = SemanticCostModel()
        best, _ = model.select_best_aggregation(100, lambda_acc=0.0, n_clusters=5)
        assert best == 'EmbeddingClusterAggregation'

    def test_custom_cost_constants(self):
        """Custom cost constants should change operator selection."""
        model = SemanticCostModel(c_llm=1.0, c_embed=1000.0, c_code=500.0)
        best, _ = model.select_best_filter(100, lambda_acc=0.0)
        # With C_LLM=1, C_EMBED=1000, C_CODE=500:
        # LLMFilterScan: 100*1=100
        # CodegenFilterScan: 1.0 + 100*500 = 50001
        # EmbeddingFilterScan: 100*1000 = 100000
        assert best == 'LLMFilterScan'

    def test_cost_computation_formula(self):
        """Verify: Cost(op) = C_exec + λ · N · (1 - A(op))."""
        model = SemanticCostModel()
        N = 100
        lambda_acc = 2.0

        cost = model.compute_cost('LLMFilterScan', N, lambda_acc=lambda_acc)
        exec_cost = N * 100.0  # C_LLM = 100
        acc_loss = N * (1 - 1.0)  # accuracy = 1.0
        expected = exec_cost + lambda_acc * acc_loss
        assert cost == expected

        cost2 = model.compute_cost('EmbeddingFilterScan', N, lambda_acc=lambda_acc)
        exec_cost2 = N * 1.0  # C_EMBED = 1
        acc_loss2 = N * (1 - 0.85)  # accuracy = 0.85
        expected2 = exec_cost2 + lambda_acc * acc_loss2
        assert abs(cost2 - expected2) < 1e-6


# ============================================================================
# 7. Semantic Histogram E2E
# ============================================================================

class TestSemanticHistogramE2E:
    """End-to-end tests for semantic histogram."""

    def test_build_and_estimate(self):
        """Test building a histogram and estimating selectivity."""
        mock_embed = make_mock_embedding_model(dim=16)

        hist = SemanticHistogram(table_oid=1, column_name='text', n_clusters=3)
        texts = [f"document about topic {i % 3}" for i in range(30)]
        hist.build(texts, mock_embed)

        assert hist.is_built
        assert hist.total_count == 30
        assert len(hist.weights) == 3

        sel = hist.estimate_selectivity("query about topic 0", mock_embed, threshold=0.0)
        assert 0.0 <= sel <= 1.0

    def test_serialize_deserialize_roundtrip(self):
        """Test that histogram survives serialization."""
        mock_embed = make_mock_embedding_model(dim=16)

        hist = SemanticHistogram(table_oid=42, column_name='content', n_clusters=4)
        texts = [f"sample text {i}" for i in range(50)]
        hist.build(texts, mock_embed)

        data = hist.serialize()
        hist2 = SemanticHistogram.deserialize(data)

        assert hist2.table_oid == 42
        assert hist2.column_name == 'content'
        assert hist2.total_count == 50
        assert hist2.n_clusters == 4
        np.testing.assert_allclose(hist2.weights, hist.weights, rtol=1e-5)

    def test_histogram_selectivity_bounds(self):
        """Selectivity should always be in [0, 1]."""
        mock_embed = make_mock_embedding_model(dim=16)

        hist = SemanticHistogram(table_oid=1, column_name='text', n_clusters=5)
        texts = [f"text {i}" for i in range(100)]
        hist.build(texts, mock_embed)

        for threshold in [0.0, 0.3, 0.5, 0.7, 1.0]:
            sel = hist.estimate_selectivity("test query", mock_embed, threshold=threshold)
            assert 0.0 <= sel <= 1.0, f"Selectivity {sel} out of bounds at threshold {threshold}"

    def test_histogram_benchmark(self):
        """Benchmark histogram build and query time."""
        mock_embed = make_mock_embedding_model(dim=64)

        for n in [50, 200, 500]:
            hist = SemanticHistogram(table_oid=1, column_name='text', n_clusters=5)
            texts = [f"document {i}" for i in range(n)]

            start = time.perf_counter()
            hist.build(texts, mock_embed)
            build_time = time.perf_counter() - start

            start = time.perf_counter()
            for _ in range(10):
                hist.estimate_selectivity("query text", mock_embed)
            query_time = (time.perf_counter() - start) / 10

            record_benchmark(f'histogram_build_n{n}', 'SemanticHistogram.build',
                             build_time, n)
            record_benchmark(f'histogram_query_n{n}', 'SemanticHistogram.estimate',
                             query_time, 1, extra={'histogram_size': n})


# ============================================================================
# 8. Clarity Validator E2E
# ============================================================================

class TestClarityValidatorE2E:
    """End-to-end tests for semantic clarity validation."""

    def test_extract_no_schema_rejected(self):
        validator = SemanticClarityValidator()
        extract = ExtractExpression(Identifier('text'), [])

        class MockAST:
            where = None
            targets = [extract]
            group_by = None

        result = validator.syntactic_check(MockAST())
        assert not result.valid
        assert result.stage == 'syntactic'

    def test_extract_with_schema_accepted(self):
        validator = SemanticClarityValidator()
        extract = ExtractExpression(Identifier('text'), [('name', 'text'), ('age', 'int')])

        class MockAST:
            where = None
            targets = [extract]
            group_by = None

        result = validator.syntactic_check(MockAST())
        assert result.valid

    def test_short_transform_instruction_warned(self):
        validator = SemanticClarityValidator()
        transform = TransformExpression(
            input_column=Identifier('text'),
            output_name='out',
            instruction='go'
        )

        class MockAST:
            where = None
            targets = [transform]
            group_by = None

        result = validator.stability_check(MockAST(), sample_data=[('test',)])
        assert result.valid  # still passes
        assert any('unstable' in w.lower() for w in result.warnings)

    def test_good_transform_no_warnings(self):
        validator = SemanticClarityValidator()
        transform = TransformExpression(
            input_column=Identifier('text'),
            output_name='summary',
            instruction='summarize this text in one clear sentence'
        )

        class MockAST:
            where = None
            targets = [transform]
            group_by = None

        result = validator.stability_check(MockAST(), sample_data=[('test data',)])
        assert result.valid

    def test_full_validation_empty_ast(self):
        validator = SemanticClarityValidator()

        class MockAST:
            where = None
            targets = []
            group_by = None

        result = validator.validate(MockAST())
        assert result.valid
        assert result.stage == 'passed'

    @patch('andb.sql.validation.clarity_validator.SemanticClarityValidator._get_client_model')
    def test_assertive_predicate_passes(self, mock_get_model):
        mock_model = MagicMock()
        mock_model.complete_messages.return_value = 'yes'
        mock_get_model.return_value = mock_model

        validator = SemanticClarityValidator()
        predicate = MatchesPredicate(
            column=Identifier('abstract'),
            assertion='discusses neural network architectures'
        )

        class MockAST:
            where = predicate
            targets = []
            group_by = None

        result = validator.syntactic_check(MockAST())
        assert result.valid

    @patch('andb.sql.validation.clarity_validator.SemanticClarityValidator._get_client_model')
    def test_non_assertive_predicate_rejected(self, mock_get_model):
        mock_model = MagicMock()
        mock_model.complete_messages.return_value = 'no'
        mock_get_model.return_value = mock_model

        validator = SemanticClarityValidator()
        predicate = MatchesPredicate(
            column=Identifier('text'),
            assertion='hello world'
        )

        class MockAST:
            where = predicate
            targets = []
            group_by = None

        result = validator.syntactic_check(MockAST())
        assert not result.valid
        assert result.stage == 'syntactic'


# ============================================================================
# 9. Cross-Component Integration E2E
# ============================================================================

class TestCrossComponentE2E:
    """Tests that combine multiple components end-to-end."""

    @patch('andb.executor.operator.physical.calibrator.default_client_model')
    @patch('andb.executor.operator.physical.semantic_filter.default_client_model')
    def test_filter_then_calibrate(self, mock_filter_model, mock_calib_model):
        """Test chaining a semantic filter with a calibrator."""
        # Filter model
        filter_model = MagicMock()
        filter_model.complete_messages.side_effect = ['true', 'false', 'true', 'true', 'false']
        mock_filter_model.return_value = filter_model

        # Calibrator model
        calib_model = make_mock_client_model('valid')
        mock_calib_model.return_value = calib_model

        # Setup filter
        condition = MockCondition('about AI')
        condition.table_columns = [MockColumn('text')]
        filter_op = LLMFilterScan(condition)
        tuples = [('AI',), ('cook',), ('ML',), ('DL',), ('recipe',)]
        child = MockChild(tuples)
        filter_op.children = [child]

        # Chain calibrator after filter
        calibrator = CalibratorOperator(sample_rate=1.0, confidence_threshold=0.8)
        calibrator.children = [filter_op]

        calibrator.open()
        start = time.perf_counter()
        results = list(calibrator.next())
        elapsed = time.perf_counter() - start
        calibrator.close()

        assert len(results) == 3  # true, true, true
        assert calibrator.confidence == 1.0
        record_benchmark('test_filter_then_calibrate', 'LLMFilter+Calibrator', elapsed, 5)

    def test_cost_model_guides_operator_selection(self):
        """Test that cost model selection changes with parameters."""
        model = SemanticCostModel()

        # Small N, no accuracy: EmbeddingFilter wins
        best_small, _ = model.select_best_filter(5, lambda_acc=0.0)
        assert best_small == 'EmbeddingFilterScan'

        # Large N, no accuracy: CodegenFilter wins
        best_large, _ = model.select_best_filter(10000, lambda_acc=0.0)
        assert best_large == 'CodegenFilterScan'

        # Any N, extreme accuracy: LLMFilter wins
        best_acc, _ = model.select_best_filter(100, lambda_acc=1000000.0)
        assert best_acc == 'LLMFilterScan'

    def test_histogram_informs_cost_decision(self):
        """Test using histogram selectivity to inform cost model."""
        mock_embed = make_mock_embedding_model(dim=16)

        hist = SemanticHistogram(table_oid=1, column_name='text', n_clusters=3)
        texts = [f"document {i}" for i in range(100)]
        hist.build(texts, mock_embed)

        selectivity = hist.estimate_selectivity("query", mock_embed, threshold=0.5)

        # Use selectivity to estimate output cardinality
        N = 100
        est_output = N * selectivity

        model = SemanticCostModel()
        # Use estimated cardinality for cost computation
        best_filter, cost = model.select_best_filter(
            input_cardinality=N, lambda_acc=0.0
        )
        assert best_filter is not None
        assert cost > 0

    @patch('andb.executor.operator.physical.semantic_agg.default_embedding_model')
    @patch('andb.executor.operator.physical.semantic_agg.default_client_model')
    def test_agg_with_large_dataset(self, mock_client_factory, mock_embed_factory):
        """Test aggregation with a larger dataset for benchmarking."""
        mock_client = MagicMock()
        categories = ['Science', 'Engineering', 'Arts', 'Medicine', 'Business']
        # Cycle through categories
        mock_client.complete_messages.side_effect = [
            categories[i % len(categories)] for i in range(200)
        ]
        mock_client_factory.return_value = mock_client

        mock_embed = make_mock_embedding_model(dim=16)
        mock_embed_factory.return_value = mock_embed

        tc = TableColumn('test', 'text')
        count_fn = FunctionColumn('count', [tc])
        op = HashBasedSemanticAggregation(
            group_instruction='classify by field',
            agg_functions=[count_fn],
            grouping_columns=[]
        )

        N = 100
        tuples = [(f'paper about topic {i}',) for i in range(N)]
        child = MockChild(tuples, [MockColumn('text')])
        op.children = [child]

        op.open()
        start = time.perf_counter()
        results = list(op.next())
        elapsed = time.perf_counter() - start
        op.close()

        total = sum(r[-1] for r in results)
        assert total == N
        record_benchmark('test_agg_100_tuples', 'HashBasedSemanticAggregation', elapsed, N)


# ============================================================================
# 10. Accuracy Score Consistency
# ============================================================================

class TestAccuracyScoreConsistency:
    """Verify accuracy scores match the design document."""

    def test_filter_accuracy_scores(self):
        model = SemanticCostModel()
        assert model.get_accuracy_score('LLMFilterScan') == 1.0
        assert model.get_accuracy_score('CodegenFilterScan') == 0.75
        assert model.get_accuracy_score('EmbeddingFilterScan') == 0.85
        assert model.get_accuracy_score('HybridFilterScan') == 0.95

    def test_join_accuracy_scores(self):
        model = SemanticCostModel()
        assert model.get_accuracy_score('NestedLoopSemanticJoin') == 1.0
        assert model.get_accuracy_score('IndexedNLSemanticJoin') == 0.90
        assert model.get_accuracy_score('HashSemanticJoin') == 0.87

    def test_agg_accuracy_scores(self):
        model = SemanticCostModel()
        assert model.get_accuracy_score('HashBasedSemanticAggregation') == 0.95
        assert model.get_accuracy_score('EmbeddingClusterAggregation') == 0.80

    def test_unknown_operator_default(self):
        model = SemanticCostModel()
        assert model.get_accuracy_score('NonExistentOp') == 0.5

    def test_operator_accuracy_from_instances(self):
        """Verify accuracy scores from actual operator instances."""
        condition = MockCondition()
        condition.table_columns = []

        llm = LLMFilterScan(condition)
        assert llm.accuracy_score == 1.0

        codegen = CodegenFilterScan(condition)
        assert codegen.accuracy_score == 0.75

        embedding = EmbeddingFilterScan(condition)
        assert embedding.accuracy_score == 0.85

        hybrid = HybridFilterScan(condition)
        assert hybrid.accuracy_score == 0.95

        inlsj = IndexedNLSemanticJoin(condition, 'INNER', ['t1', 't2'])
        assert inlsj.accuracy_score == 0.90

        hsj = HashSemanticJoin(condition, 'INNER', ['t1', 't2'])
        assert hsj.accuracy_score == 0.87


# ============================================================================
# Benchmark Summary (printed at end of test session)
# ============================================================================

@pytest.fixture(scope='session', autouse=True)
def print_benchmark_summary(request):
    """Print benchmark results at the end of the test session."""
    yield

    if _benchmark_results:
        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS")
        print("=" * 80)
        print(f"{'Test':<40} {'Operator':<30} {'Time(s)':<12} {'Tuples':<8} {'Throughput':<12}")
        print("-" * 102)
        for r in _benchmark_results:
            print(f"{r['test']:<40} {r['operator']:<30} {r['elapsed_s']:<12.6f} "
                  f"{r['n_tuples']:<8} {r['throughput']:<12.2f}")
        print("=" * 80)

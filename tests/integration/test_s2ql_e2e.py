"""End-to-end integration tests for S²QL query processing.

Tests the full pipeline: parsing -> transformation -> implementation
for new S²QL syntax (MATCHES, EXTRACT, TRANSFORM, CLASSIFYING).
"""

import pytest

from andb.sql.parser import lexer, parser_
from andb.sql.parser.ast.s2ql import (
    MatchesPredicate, ExtractExpression, TransformExpression, ClassifyingGroupBy,
)
from andb.sql.parser.ast.semantic import SemanticMatch

andb_lexer = lexer.SQLLexer()
andb_parser = parser_.SQLParser()


def parse(stmt):
    """Parse a SQL statement and return the AST."""
    return andb_parser.parse(andb_lexer.tokenize(stmt))


class TestS2QLParsing:
    """Test that S²QL statements parse correctly."""

    def test_matches_basic(self):
        ast = parse("SELECT title FROM papers WHERE abstract MATCHES 'discusses neural networks'")
        assert isinstance(ast.where, MatchesPredicate)
        assert ast.where.assertion == 'discusses neural networks'

    def test_matches_with_threshold(self):
        ast = parse(
            "SELECT * FROM reviews WHERE content MATCHES 'positive sentiment' "
            "WITH (threshold = 0.8)"
        )
        assert isinstance(ast.where, MatchesPredicate)
        assert ast.where.with_params['threshold'] == 0.8

    def test_extract_into(self):
        ast = parse("SELECT EXTRACT(text INTO (name text, age int)) FROM documents")
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], ExtractExpression)
        assert ast.targets[0].target_schema == [('name', 'text'), ('age', 'int')]

    def test_transform_as_using(self):
        ast = parse(
            "SELECT TRANSFORM(body AS summary USING 'summarize briefly') FROM products"
        )
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], TransformExpression)
        assert ast.targets[0].output_name == 'summary'

    def test_classifying_group_by(self):
        ast = parse(
            "SELECT category, count(1) FROM reviews "
            "GROUP BY CLASSIFYING review_text AS category"
        )
        assert len(ast.group_by) == 1
        assert isinstance(ast.group_by[0], ClassifyingGroupBy)

    def test_backward_compat_sem_match(self):
        """Ensure old SEM_MATCH syntax still works."""
        ast = parse("SELECT title FROM t1 WHERE SEM_MATCH('topic is {title}')")
        assert isinstance(ast.where, SemanticMatch)

    def test_backward_compat_sem_match_with_threshold(self):
        ast = parse("SELECT title FROM t1 WHERE SEM_MATCH('topic is {title}', 0.8)")
        assert isinstance(ast.where, SemanticMatch)
        assert ast.where.threshold == 0.8


class TestS2QLCostModel:
    """Test the semantic cost model integration."""

    def test_cost_model_filter_selection(self):
        from andb.sql.optimizer.semantic_cost_model import SemanticCostModel
        model = SemanticCostModel()

        # Large cardinality, no accuracy preference
        # CodegenFilterScan: C_LLM + N*C_CODE = 100 + 1000*0.01 = 110
        # EmbeddingFilterScan: N*C_EMBED = 1000
        best, _ = model.select_best_filter(1000, lambda_acc=0.0)
        assert best == 'CodegenFilterScan'

        # Very high accuracy weight -> LLMFilterScan wins (zero acc loss)
        best, _ = model.select_best_filter(10, lambda_acc=100000.0)
        assert best == 'LLMFilterScan'

    def test_cost_model_join_selection(self):
        from andb.sql.optimizer.semantic_cost_model import SemanticCostModel
        model = SemanticCostModel()

        # Large tables -> HashSemanticJoin preferred
        best, _ = model.select_best_join(100, 100, lambda_acc=0.0)
        assert best == 'HashSemanticJoin'

    def test_cost_model_aggregation_selection(self):
        from andb.sql.optimizer.semantic_cost_model import SemanticCostModel
        model = SemanticCostModel()

        # No accuracy preference -> EmbeddingCluster wins (cheaper)
        best, _ = model.select_best_aggregation(100, lambda_acc=0.0)
        assert best == 'EmbeddingClusterAggregation'


class TestS2QLHistogram:
    """Test semantic histogram functionality."""

    def test_histogram_build_and_query(self):
        import numpy as np
        from andb.sql.optimizer.semantic_histogram import SemanticHistogram

        class MockEmbed:
            def generate_embeddings(self, texts, normalize_embeddings=False):
                np.random.seed(42)
                return np.random.randn(len(texts), 16).astype(np.float32)

        hist = SemanticHistogram(table_oid=1, column_name='text', n_clusters=3)
        texts = [f"sample text {i}" for i in range(20)]
        hist.build(texts, MockEmbed())

        assert hist.is_built
        assert hist.total_count == 20

        sel = hist.estimate_selectivity("query text", MockEmbed(), threshold=0.0)
        assert 0.0 <= sel <= 1.0

    def test_histogram_serialization(self):
        import numpy as np
        from andb.sql.optimizer.semantic_histogram import SemanticHistogram

        class MockEmbed:
            def generate_embeddings(self, texts, normalize_embeddings=False):
                np.random.seed(42)
                return np.random.randn(len(texts), 16).astype(np.float32)

        hist = SemanticHistogram(table_oid=99, column_name='content', n_clusters=2)
        hist.build(["text1", "text2", "text3"], MockEmbed())

        data = hist.serialize()
        hist2 = SemanticHistogram.deserialize(data)

        assert hist2.table_oid == 99
        assert hist2.column_name == 'content'
        assert hist2.total_count == 3


class TestS2QLClarityValidation:
    """Test semantic clarity validation."""

    def test_extract_no_schema_fails(self):
        from andb.sql.validation.clarity_validator import SemanticClarityValidator
        from andb.sql.parser.ast.s2ql import ExtractExpression
        from andb.sql.parser.ast.identifier import Identifier

        validator = SemanticClarityValidator()
        extract = ExtractExpression(Identifier('text'), [])

        class MockAST:
            where = None
            targets = [extract]
            group_by = None

        result = validator.syntactic_check(MockAST())
        assert not result.valid

    def test_good_extract_passes(self):
        from andb.sql.validation.clarity_validator import SemanticClarityValidator
        from andb.sql.parser.ast.s2ql import ExtractExpression
        from andb.sql.parser.ast.identifier import Identifier

        validator = SemanticClarityValidator()
        extract = ExtractExpression(Identifier('text'), [('name', 'text')])

        class MockAST:
            where = None
            targets = [extract]
            group_by = None

        result = validator.syntactic_check(MockAST())
        assert result.valid

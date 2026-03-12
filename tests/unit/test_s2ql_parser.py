"""Unit tests for S²QL parser extensions."""

from andb.sql.parser import lexer, parser_
from andb.sql.parser.ast.s2ql import (
    MatchesPredicate, ExtractExpression, TransformExpression,
    ClassifyingGroupBy, WithClause,
)
from andb.sql.parser.ast.semantic import SemanticMatch

andb_lexer = lexer.SQLLexer()
andb_parser = parser_.SQLParser()


def parse(stmt):
    return andb_parser.parse(andb_lexer.tokenize(stmt))


def test_matches_predicate_basic():
    """Test: SELECT title FROM papers WHERE abstract MATCHES 'discusses neural networks'"""
    ast = parse("SELECT title FROM papers WHERE abstract MATCHES 'discusses neural networks'")
    assert ast is not None
    assert ast.where is not None
    assert isinstance(ast.where, MatchesPredicate)
    assert ast.where.assertion == 'discusses neural networks'


def test_matches_predicate_with_params():
    """Test: MATCHES with WITH clause"""
    ast = parse(
        "SELECT title FROM papers WHERE abstract MATCHES 'positive sentiment' "
        "WITH (threshold = 0.8)"
    )
    assert ast is not None
    assert isinstance(ast.where, MatchesPredicate)
    assert ast.where.assertion == 'positive sentiment'
    assert ast.where.with_params is not None
    assert ast.where.with_params.get('threshold') == 0.8


def test_extract_expression():
    """Test: SELECT EXTRACT(text INTO (name text, age int)) FROM documents"""
    ast = parse("SELECT EXTRACT(text INTO (name text, age int)) FROM documents")
    assert ast is not None
    assert len(ast.targets) == 1
    target = ast.targets[0]
    assert isinstance(target, ExtractExpression)
    assert len(target.target_schema) == 2
    assert target.target_schema[0] == ('name', 'text')
    assert target.target_schema[1] == ('age', 'int')


def test_transform_expression():
    """Test: SELECT TRANSFORM(body AS summary USING 'summarize in one sentence') FROM products"""
    ast = parse(
        "SELECT TRANSFORM(body AS summary USING 'summarize in one sentence') FROM products"
    )
    assert ast is not None
    assert len(ast.targets) == 1
    target = ast.targets[0]
    assert isinstance(target, TransformExpression)
    assert target.output_name == 'summary'
    assert target.instruction == 'summarize in one sentence'


def test_classifying_group_by():
    """Test: SELECT category, COUNT(1) FROM reviews GROUP BY CLASSIFYING review_text AS category"""
    ast = parse(
        "SELECT category, count(1) FROM reviews GROUP BY CLASSIFYING review_text AS category"
    )
    assert ast is not None
    assert ast.group_by is not None
    assert len(ast.group_by) == 1
    gb = ast.group_by[0]
    assert isinstance(gb, ClassifyingGroupBy)
    assert gb.key_name == 'category'


def test_sem_match_still_works():
    """Test backward compatibility: SEM_MATCH still works."""
    ast = parse("SELECT title FROM t1 WHERE SEM_MATCH('topic is {title}', 0.8)")
    assert ast is not None
    assert isinstance(ast.where, SemanticMatch)
    assert ast.where.threshold == 0.8


def test_matches_column_reference():
    """Test that MATCHES correctly captures the column reference."""
    ast = parse("SELECT * FROM books WHERE title MATCHES 'about machine learning'")
    assert ast is not None
    assert isinstance(ast.where, MatchesPredicate)
    assert ast.where.column.parts == 'title'
    assert ast.where.assertion == 'about machine learning'


def test_extract_single_column():
    """Test EXTRACT with a single output column."""
    ast = parse("SELECT EXTRACT(content INTO (summary text)) FROM docs")
    assert ast is not None
    target = ast.targets[0]
    assert isinstance(target, ExtractExpression)
    assert len(target.target_schema) == 1
    assert target.target_schema[0] == ('summary', 'text')


def test_transform_with_table_column():
    """Test TRANSFORM referencing a table column."""
    ast = parse(
        "SELECT TRANSFORM(body AS translated USING 'translate to Chinese') FROM articles"
    )
    assert ast is not None
    target = ast.targets[0]
    assert isinstance(target, TransformExpression)
    assert target.output_name == 'translated'
    assert target.instruction == 'translate to Chinese'

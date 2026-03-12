"""Unit tests for Semantic Clarity Validator."""

import pytest
from unittest.mock import MagicMock, patch

from andb.sql.validation.clarity_validator import (
    SemanticClarityValidator, ValidationResult,
)
from andb.sql.parser.ast.s2ql import (
    MatchesPredicate, ExtractExpression, TransformExpression, ClassifyingGroupBy,
)
from andb.sql.parser.ast.identifier import Identifier


def test_validation_result_default():
    result = ValidationResult()
    assert result.valid is True
    assert result.stage == 'passed'
    assert result.warnings == []
    assert result.confidence == 1.0


def test_validation_result_custom():
    result = ValidationResult(valid=False, warnings=['test'], stage='syntactic', confidence=0.5)
    assert result.valid is False
    assert result.stage == 'syntactic'
    assert 'test' in result.warnings


@patch('andb.sql.validation.clarity_validator.SemanticClarityValidator._get_client_model')
def test_syntactic_check_assertive_predicate(mock_get_model):
    """Assertive predicates should pass syntactic check."""
    mock_model = MagicMock()
    mock_model.complete_messages.return_value = 'yes'
    mock_get_model.return_value = mock_model

    validator = SemanticClarityValidator()
    predicate = MatchesPredicate(
        column=Identifier('abstract'),
        assertion='discusses neural networks'
    )

    # Create a mock AST that contains the predicate
    class MockAST:
        where = predicate
        targets = []
        group_by = None

    result = validator.syntactic_check(MockAST())
    assert result.valid is True


@patch('andb.sql.validation.clarity_validator.SemanticClarityValidator._get_client_model')
def test_syntactic_check_non_assertive_predicate(mock_get_model):
    """Non-assertive predicates should fail syntactic check."""
    mock_model = MagicMock()
    mock_model.complete_messages.return_value = 'no'
    mock_get_model.return_value = mock_model

    validator = SemanticClarityValidator()
    predicate = MatchesPredicate(
        column=Identifier('text'),
        assertion='hello there'
    )

    class MockAST:
        where = predicate
        targets = []
        group_by = None

    result = validator.syntactic_check(MockAST())
    assert result.valid is False
    assert result.stage == 'syntactic'


def test_syntactic_check_extract_no_schema():
    """EXTRACT with no target schema should fail."""
    validator = SemanticClarityValidator()
    extract = ExtractExpression(
        source_column=Identifier('text'),
        target_schema=[]
    )

    class MockAST:
        where = None
        targets = [extract]
        group_by = None

    result = validator.syntactic_check(MockAST())
    assert result.valid is False
    assert 'EXTRACT must define at least one target column' in result.warnings[0]


def test_syntactic_check_extract_with_schema():
    """EXTRACT with valid schema should pass."""
    validator = SemanticClarityValidator()
    extract = ExtractExpression(
        source_column=Identifier('text'),
        target_schema=[('name', 'text'), ('age', 'int')]
    )

    class MockAST:
        where = None
        targets = [extract]
        group_by = None

    result = validator.syntactic_check(MockAST())
    assert result.valid is True


def test_stability_check_short_transform():
    """Very short TRANSFORM instructions should be flagged."""
    validator = SemanticClarityValidator()
    transform = TransformExpression(
        input_column=Identifier('text'),
        output_name='result',
        instruction='go'  # too short
    )

    class MockAST:
        where = None
        targets = [transform]
        group_by = None

    result = validator.stability_check(MockAST(), sample_data=[('test',)])
    assert result.valid is True  # still passes, but may have warnings
    assert any('unstable' in w.lower() for w in result.warnings)


def test_stability_check_good_transform():
    """Reasonable TRANSFORM instructions should pass."""
    validator = SemanticClarityValidator()
    transform = TransformExpression(
        input_column=Identifier('description'),
        output_name='summary',
        instruction='summarize this text in one sentence'
    )

    class MockAST:
        where = None
        targets = [transform]
        group_by = None

    result = validator.stability_check(MockAST(), sample_data=[('test data',)])
    assert result.valid is True


def test_validate_full_pass():
    """Full validation with no issues should pass."""
    validator = SemanticClarityValidator()

    class MockAST:
        where = None
        targets = []
        group_by = None

    result = validator.validate(MockAST())
    assert result.valid is True
    assert result.stage == 'passed'

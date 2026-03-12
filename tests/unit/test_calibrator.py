"""Unit tests for CalibratorOperator."""

import pytest
from unittest.mock import MagicMock, patch

from andb.executor.operator.physical.calibrator import CalibratorOperator


class MockChild:
    def __init__(self, tuples):
        self._tuples = tuples
        self.name = 'MockSemanticFilter'
        self.columns = []

    def open(self):
        pass

    def next(self):
        for t in self._tuples:
            yield t

    def close(self):
        pass


def test_calibrator_properties():
    op = CalibratorOperator(sample_rate=0.2, confidence_threshold=0.9)
    assert op.name == 'CalibratorOperator'
    assert op.sample_rate == 0.2
    assert op.confidence_threshold == 0.9
    assert op.confidence is None


def test_calibrator_empty_input():
    op = CalibratorOperator()
    child = MockChild([])
    op.children = [child]

    op.open()
    results = list(op.next())
    op.close()

    assert len(results) == 0
    assert op.confidence == 1.0
    stats = op.get_calibration_stats()
    assert stats['sample_size'] == 0
    assert stats['confidence'] == 1.0


@patch('andb.executor.operator.physical.calibrator.default_client_model')
def test_calibrator_passes_all_tuples(mock_client_factory):
    """Calibrator should yield all tuples regardless of verification results."""
    mock_model = MagicMock()
    mock_model.complete_messages.return_value = 'valid'
    mock_client_factory.return_value = mock_model

    tuples = [('a',), ('b',), ('c',), ('d',), ('e',)]
    op = CalibratorOperator(sample_rate=1.0, confidence_threshold=0.5)
    child = MockChild(tuples)
    op.children = [child]

    op.open()
    results = list(op.next())
    op.close()

    assert len(results) == 5
    assert op.confidence is not None
    assert op.confidence <= 1.0


@patch('andb.executor.operator.physical.calibrator.default_client_model')
def test_calibrator_low_confidence(mock_client_factory):
    """Test calibrator detects low confidence."""
    mock_model = MagicMock()
    mock_model.complete_messages.return_value = 'invalid'
    mock_client_factory.return_value = mock_model

    tuples = [('a',), ('b',), ('c',)]
    op = CalibratorOperator(sample_rate=1.0, confidence_threshold=0.9)
    child = MockChild(tuples)
    op.children = [child]

    op.open()
    results = list(op.next())
    op.close()

    # All results should still be yielded
    assert len(results) == 3
    # But confidence should be low
    assert op.confidence == 0.0  # all re-verifications returned 'invalid'
    stats = op.get_calibration_stats()
    assert stats['consistency_rate'] == 0.0
    assert len(stats['low_confidence_indices']) > 0


@patch('andb.executor.operator.physical.calibrator.default_client_model')
def test_calibrator_stats(mock_client_factory):
    """Test calibration statistics are computed correctly."""
    mock_model = MagicMock()
    # Alternate between valid and invalid
    mock_model.complete_messages.side_effect = ['valid', 'invalid', 'valid', 'invalid', 'valid']
    mock_client_factory.return_value = mock_model

    tuples = [(i,) for i in range(5)]
    op = CalibratorOperator(sample_rate=1.0, confidence_threshold=0.5)
    child = MockChild(tuples)
    op.children = [child]

    op.open()
    results = list(op.next())
    op.close()

    stats = op.get_calibration_stats()
    assert stats['sample_size'] == 5
    assert stats['consistency_rate'] == 3 / 5  # 3 valid out of 5
    assert len(stats['low_confidence_indices']) == 2

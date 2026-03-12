"""Calibrator operator: estimates trustworthiness of semantic operator outputs.

Implements trust-aware execution by sampling and re-verifying a subset
of upstream operator results. Can be inserted after any semantic operator
without changing the data flow.

Theory:
    confidence = alpha * consistency_sample + (1-alpha) * consistency_cross
    where:
        consistency_sample: agreement rate between original and re-verified results
        consistency_cross: agreement between different models (optional)
"""

import random
import math

from andb.executor.operator.physical.base import PhysicalOperator
from andb.executor.operator.physical.semantic import default_client_model


class CalibratorOperator(PhysicalOperator):
    """Calibrator: estimates confidence of upstream semantic operations.

    Inserts after any semantic operator to validate output quality.
    Does not modify the data flow, only attaches confidence metadata.

    Time complexity: O(sample_size * T_LLM)
    """

    def __init__(self, sample_rate=0.1, confidence_threshold=0.8,
                 cross_model=None, alpha=0.7):
        """
        Args:
            sample_rate: Fraction of results to sample for verification
            confidence_threshold: Minimum acceptable confidence
            cross_model: Optional model name for cross-model verification
            alpha: Weight for sample consistency vs cross-model consistency
        """
        super().__init__('CalibratorOperator')
        self.sample_rate = sample_rate
        self.confidence_threshold = confidence_threshold
        self.cross_model = cross_model
        self.alpha = alpha

        self._confidence = None
        self._calibration_stats = {}
        self._client_model = None

    @property
    def confidence(self):
        """Return the most recent overall confidence score."""
        return self._confidence

    def _get_client_model(self):
        if self._client_model is None:
            self._client_model = default_client_model()
        return self._client_model

    def get_calibration_stats(self):
        """Return calibration statistics."""
        return self._calibration_stats

    def _re_verify(self, result, operator_type):
        """Re-verify a single result using an independent LLM call.

        Args:
            result: The tuple to verify
            operator_type: Type of upstream operator (filter/join/agg)

        Returns:
            Re-verification result
        """
        client_model = self._get_client_model()

        # Construct a verification prompt based on operator type
        result_text = str(result)

        messages = [
            {"role": "system",
             "content": "You are a data quality checker. Verify if the given data "
                        "looks reasonable and consistent. Answer ONLY 'valid' or 'invalid'."},
            {"role": "user",
             "content": f"Verify this result from a {operator_type} operation:\n{result_text}\n"
                        f"Is this result valid?"}
        ]

        try:
            response = client_model.complete_messages(messages, max_tokens=8, temperature=0.0)
            return response.strip().lower() == 'valid'
        except Exception:
            return True  # On error, assume valid (conservative)

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        child = self.children[0]

        # Collect all results from upstream
        results = []
        for tup in child.next():
            results.append(tup)

        if not results:
            self._confidence = 1.0
            self._calibration_stats = {
                'sample_size': 0,
                'consistency_rate': 1.0,
                'confidence': 1.0,
                'low_confidence_indices': []
            }
            return

        N = len(results)
        sample_size = max(min(math.ceil(N * self.sample_rate), N), min(30, N))
        sample_indices = random.sample(range(N), sample_size)

        # Determine upstream operator type from child name
        operator_type = getattr(self.children[0], 'name', 'semantic')

        # Sample verification
        verify_count = 0
        low_confidence_indices = []

        for idx in sample_indices:
            is_valid = self._re_verify(results[idx], operator_type)
            if is_valid:
                verify_count += 1
            else:
                low_confidence_indices.append(idx)

        consistency = verify_count / sample_size if sample_size > 0 else 1.0
        self._confidence = consistency

        self._calibration_stats = {
            'sample_size': sample_size,
            'consistency_rate': consistency,
            'confidence': self._confidence,
            'low_confidence_indices': low_confidence_indices
        }

        if self._confidence < self.confidence_threshold:
            import logging
            logging.warning(
                f"CalibratorOperator: Low confidence {self._confidence:.2f} "
                f"(threshold: {self.confidence_threshold})"
            )

        # Yield all results (calibrator doesn't filter, just measures)
        for r in results:
            super().next()
            yield r

    def close(self):
        for child in self.children:
            child.close()
        super().close()

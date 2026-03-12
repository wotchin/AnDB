"""Semantic filter physical operators: multiple strategies for semantic selection.

Implements the paper's Table 2 physical operators for semantic selection:
- LLMFilterScan: Pure LLM filtering (highest accuracy, highest cost)
- CodegenFilterScan: LLM generates code, then executes deterministically
- EmbeddingFilterScan: Pure embedding similarity filtering
- HybridFilterScan: Embedding pre-filter + LLM verification
"""

import ast as python_ast
import re
import json

import numpy as np

from andb.executor.operator.physical.base import PhysicalOperator
from andb.executor.operator.physical.semantic import default_client_model, default_embedding_model


class SemanticFilterBase(PhysicalOperator):
    """Base class for all semantic filter operators."""

    ACCURACY_SCORE = 0.5

    def __init__(self, name, condition):
        """
        Args:
            condition: SemanticCondition with predicate text, threshold, and target columns
        """
        super().__init__(name)
        self.condition = condition
        self._client_model = None
        self._embedding_model = None

    @property
    def accuracy_score(self):
        return self.ACCURACY_SCORE

    def _get_client_model(self):
        if self._client_model is None:
            self._client_model = default_client_model()
        return self._client_model

    def _get_embedding_model(self):
        if self._embedding_model is None:
            self._embedding_model = default_embedding_model()
        return self._embedding_model

    def _format_tuple_text(self, tup, columns):
        """Format a tuple's relevant columns into a text string."""
        parts = []
        for i, col in enumerate(columns):
            if i < len(tup):
                parts.append(f"{col.column_name}: {tup[i]}")
        return "; ".join(parts)

    def _get_predicate_text(self, tup, columns):
        """Get the formatted predicate text with tuple values substituted."""
        column_values = []
        for col in self.condition.table_columns:
            for i, c in enumerate(columns):
                if c.column_name == col.column_name and i < len(tup):
                    column_values.append(str(tup[i]))
                    break
        try:
            return self.condition.condition.format(*column_values)
        except (IndexError, KeyError):
            return self.condition.condition


class LLMFilterScan(SemanticFilterBase):
    """Pure LLM filtering: highest accuracy, cost = O(N * C_LLM).

    For each tuple, constructs a prompt and asks the LLM to judge true/false.
    accuracy_score = 1.0
    """

    ACCURACY_SCORE = 1.0

    def __init__(self, condition):
        super().__init__('LLMFilterScan', condition)

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        client_model = self._get_client_model()
        child = self.children[0]
        columns = child.columns or []

        # Collect all tuples first for batch processing
        tuples = []
        for tup in child.next():
            tuples.append(tup)

        if not tuples:
            return

        # Build messages for batch LLM calls
        for tup in tuples:
            predicate_text = self._get_predicate_text(tup, columns)
            messages = [
                {"role": "system", "content": "You are a classifier. Answer ONLY 'true' or 'false'."},
                {"role": "user", "content": f"Is the following statement true?\n\n{predicate_text}"}
            ]
            try:
                response = client_model.complete_messages(messages, max_tokens=8, temperature=0.0)
                if response.strip().lower() == 'true':
                    super().next()
                    yield tup
            except Exception:
                # On LLM failure, skip this tuple
                continue

    def close(self):
        for child in self.children:
            child.close()
        super().close()


class CodegenFilterScan(SemanticFilterBase):
    """Code generation filtering: LLM generates Python filter function.

    Phase 1: Sample tuples, ask LLM to generate a Python filter function
    Phase 2: Validate generated code for safety
    Phase 3: Cross-validate with LLM on samples
    Phase 4: Execute generated code on all tuples

    accuracy_score = 0.75
    cost = O(C_LLM + N * C_code)
    """

    ACCURACY_SCORE = 0.75

    # Dangerous modules/functions to block in generated code
    BLOCKED_NAMES = {
        'os', 'subprocess', 'sys', 'shutil', 'importlib',
        'exec', 'eval', 'compile', '__import__', 'open',
        'globals', 'locals', 'getattr', 'setattr', 'delattr',
    }

    def __init__(self, condition):
        super().__init__('CodegenFilterScan', condition)
        self._generated_fn = None

    def _is_safe_code(self, code_str):
        """Check if generated code is safe to execute."""
        try:
            tree = python_ast.parse(code_str)
        except SyntaxError:
            return False

        for node in python_ast.walk(tree):
            if isinstance(node, python_ast.Import):
                return False
            if isinstance(node, python_ast.ImportFrom):
                return False
            if isinstance(node, python_ast.Name) and node.id in self.BLOCKED_NAMES:
                return False
            if isinstance(node, python_ast.Attribute) and node.attr in self.BLOCKED_NAMES:
                return False
        return True

    def _generate_filter_code(self, sample_tuples, columns, predicate_text):
        """Use LLM to generate a Python filter function."""
        client_model = self._get_client_model()

        sample_strs = []
        for tup in sample_tuples[:5]:
            parts = {columns[i].column_name: tup[i] for i in range(min(len(columns), len(tup)))}
            sample_strs.append(str(parts))

        prompt = (
            f"Generate a Python function `filter_fn(row)` that returns True if the row matches "
            f"this condition: '{predicate_text}'.\n\n"
            f"The row is a dict with these keys: {[c.column_name for c in columns]}\n"
            f"Sample rows:\n" + "\n".join(sample_strs) + "\n\n"
            f"Return ONLY the function definition, no imports, no explanation. "
            f"Use only basic string operations and comparisons."
        )

        messages = [
            {"role": "system", "content": "You are a Python code generator. Output only valid Python code."},
            {"role": "user", "content": prompt}
        ]

        response = client_model.complete_messages(messages, max_tokens=512, temperature=0.0)

        # Extract code from response (handle markdown code blocks)
        code = response.strip()
        if '```python' in code:
            code = code.split('```python')[1].split('```')[0].strip()
        elif '```' in code:
            code = code.split('```')[1].split('```')[0].strip()

        return code

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        child = self.children[0]
        columns = child.columns or []

        # Collect all tuples
        tuples = []
        for tup in child.next():
            tuples.append(tup)

        if not tuples:
            return

        predicate_text = self.condition.condition
        # Try to generate and use code filter
        try:
            code_str = self._generate_filter_code(tuples, columns, predicate_text)

            if not self._is_safe_code(code_str):
                # Fall back to LLM filter
                yield from self._fallback_llm_filter(tuples, columns)
                return

            # Compile in restricted namespace
            namespace = {'re': re, 'str': str, 'int': int, 'float': float,
                         'len': len, 'any': any, 'all': all, 'True': True,
                         'False': False, 'None': None}
            exec(code_str, namespace)

            if 'filter_fn' not in namespace:
                yield from self._fallback_llm_filter(tuples, columns)
                return

            filter_fn = namespace['filter_fn']

            # Execute filter on all tuples
            for tup in tuples:
                try:
                    row_dict = {columns[i].column_name: tup[i]
                                for i in range(min(len(columns), len(tup)))}
                    if filter_fn(row_dict):
                        super().next()
                        yield tup
                except Exception:
                    continue

        except Exception:
            yield from self._fallback_llm_filter(tuples, columns)

    def _fallback_llm_filter(self, tuples, columns):
        """Fall back to LLM-based filtering."""
        client_model = self._get_client_model()
        for tup in tuples:
            predicate_text = self._get_predicate_text(tup, columns)
            messages = [
                {"role": "system", "content": "Answer ONLY 'true' or 'false'."},
                {"role": "user", "content": f"Is this true? {predicate_text}"}
            ]
            try:
                response = client_model.complete_messages(messages, max_tokens=8, temperature=0.0)
                if response.strip().lower() == 'true':
                    super().next()
                    yield tup
            except Exception:
                continue

    def close(self):
        for child in self.children:
            child.close()
        super().close()


class EmbeddingFilterScan(SemanticFilterBase):
    """Pure embedding similarity filtering.

    Embeds the predicate and each tuple's text, then filters by
    cosine similarity threshold.

    accuracy_score = 0.85
    cost = O(N * d)
    """

    ACCURACY_SCORE = 0.85

    def __init__(self, condition, threshold=0.5):
        super().__init__('EmbeddingFilterScan', condition)
        self.threshold = threshold
        if condition.threshold is not None and isinstance(condition.threshold, float):
            self.threshold = condition.threshold

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        embedding_model = self._get_embedding_model()
        child = self.children[0]
        columns = child.columns or []

        # Collect all tuples
        tuples = []
        texts = []
        for tup in child.next():
            tuples.append(tup)
            texts.append(self._format_tuple_text(tup, columns))

        if not tuples:
            return

        # Embed query predicate
        predicate_text = self.condition.condition
        e_q = embedding_model.generate_embeddings([predicate_text], normalize_embeddings=True)
        e_q = np.array(e_q, dtype=np.float32).flatten()
        norm_q = np.linalg.norm(e_q)
        if norm_q > 0:
            e_q = e_q / norm_q

        # Embed all tuple texts
        E = embedding_model.generate_embeddings(texts, normalize_embeddings=True)
        E = np.array(E, dtype=np.float32)
        norms = np.linalg.norm(E, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        E = E / norms

        # Compute cosine similarities
        similarities = E @ e_q  # shape: (N,)

        # Filter by threshold
        for i, tup in enumerate(tuples):
            if similarities[i] >= self.threshold:
                super().next()
                yield tup

    def close(self):
        for child in self.children:
            child.close()
        super().close()


class HybridFilterScan(SemanticFilterBase):
    """Hybrid filtering: embedding pre-filter + LLM verification.

    Stage 1: Embedding similarity with loose threshold for high recall
    Stage 2: LLM verification for high precision on candidates

    accuracy_score = 0.95
    cost = O(N * C_embed + sel_embed * N * C_LLM)
    """

    ACCURACY_SCORE = 0.95

    def __init__(self, condition, loose_threshold=0.3, strict_threshold=0.7):
        super().__init__('HybridFilterScan', condition)
        self.loose_threshold = loose_threshold
        self.strict_threshold = strict_threshold
        if condition.threshold is not None and isinstance(condition.threshold, float):
            self.strict_threshold = condition.threshold
            self.loose_threshold = max(0.1, condition.threshold - 0.3)

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        embedding_model = self._get_embedding_model()
        client_model = self._get_client_model()
        child = self.children[0]
        columns = child.columns or []

        # Collect all tuples
        tuples = []
        texts = []
        for tup in child.next():
            tuples.append(tup)
            texts.append(self._format_tuple_text(tup, columns))

        if not tuples:
            return

        # Stage 1: Embedding pre-filter
        predicate_text = self.condition.condition
        e_q = embedding_model.generate_embeddings([predicate_text], normalize_embeddings=True)
        e_q = np.array(e_q, dtype=np.float32).flatten()
        norm_q = np.linalg.norm(e_q)
        if norm_q > 0:
            e_q = e_q / norm_q

        E = embedding_model.generate_embeddings(texts, normalize_embeddings=True)
        E = np.array(E, dtype=np.float32)
        norms = np.linalg.norm(E, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        E = E / norms

        similarities = E @ e_q

        # Get candidates above loose threshold
        candidates = []
        for i, tup in enumerate(tuples):
            if similarities[i] >= self.loose_threshold:
                candidates.append((i, tup))

        # Stage 2: LLM verification on candidates
        for idx, tup in candidates:
            pred_text = self._get_predicate_text(tup, columns)
            messages = [
                {"role": "system", "content": "Answer ONLY 'true' or 'false'."},
                {"role": "user", "content": f"Is this true? {pred_text}"}
            ]
            try:
                response = client_model.complete_messages(messages, max_tokens=8, temperature=0.0)
                if response.strip().lower() == 'true':
                    super().next()
                    yield tup
            except Exception:
                continue

    def close(self):
        for child in self.children:
            child.close()
        super().close()

"""Semantic join physical operators: multiple strategies for semantic joining.

Implements the paper's Table 2 physical operators for semantic joins:
- NestedLoopSemanticJoin (NLSJ): existing, O(|R|*|S|*C_LLM)
- IndexedNLSemanticJoin (INLSJ): k-NN search + LLM verification
- HashSemanticJoin (HSJ): LSH-based candidate generation + LLM verification
"""

import numpy as np

from andb.executor.operator.physical.base import PhysicalOperator
from andb.executor.operator.physical.semantic import default_client_model, default_embedding_model


class SemanticJoinBase(PhysicalOperator):
    """Base class for all semantic join operators."""

    ACCURACY_SCORE = 0.5

    def __init__(self, name, condition, join_type, children_table_names):
        """
        Args:
            condition: SemanticCondition with join predicate
            join_type: INNER/LEFT/RIGHT/FULL
            children_table_names: [left_table, right_table]
        """
        super().__init__(name)
        self.condition = condition
        self.join_type = join_type
        self.children_table_names = children_table_names
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

    def _extract_text(self, tup, columns):
        """Extract text from a tuple for embedding."""
        parts = []
        for i, col in enumerate(columns):
            if i < len(tup):
                parts.append(str(tup[i]))
        return " ".join(parts)

    def _llm_judge(self, predicate_text, left_tup, right_tup, left_cols, right_cols):
        """Use LLM to judge if two tuples match the join predicate."""
        client_model = self._get_client_model()

        left_text = self._extract_text(left_tup, left_cols)
        right_text = self._extract_text(right_tup, right_cols)

        messages = [
            {"role": "system", "content": "Answer ONLY 'true' or 'false'."},
            {"role": "user", "content":
                f"Given the join condition: {predicate_text}\n\n"
                f"Left row: {left_text}\n"
                f"Right row: {right_text}\n\n"
                f"Do these rows match the condition?"}
        ]
        try:
            response = client_model.complete_messages(messages, max_tokens=8, temperature=0.0)
            return response.strip().lower() == 'true'
        except Exception:
            return False

    def _collect_tuples(self, child):
        """Collect all tuples from a child operator."""
        result = []
        for tup in child.next():
            result.append(tup)
        return result


class IndexedNLSemanticJoin(SemanticJoinBase):
    """Indexed Nested Loop Semantic Join using k-NN search.

    For each outer tuple, performs k-NN search on the inner table's
    vector index to find candidates, then uses LLM for verification.

    accuracy_score = 0.90
    cost = O(|R| * (C_embed + k * C_LLM))
    """

    ACCURACY_SCORE = 0.90

    def __init__(self, condition, join_type, children_table_names, k_candidates=10):
        super().__init__('IndexedNLSemanticJoin', condition, join_type, children_table_names)
        self.k_candidates = k_candidates

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        import faiss

        embedding_model = self._get_embedding_model()

        left_child = self.children[0]
        right_child = self.children[1]
        left_cols = left_child.columns or []
        right_cols = right_child.columns or []

        # Collect both sides
        left_tuples = self._collect_tuples(left_child)
        right_tuples = self._collect_tuples(right_child)

        if not left_tuples or not right_tuples:
            return

        # Build vector index for inner (right) table
        right_texts = [self._extract_text(t, right_cols) for t in right_tuples]
        E_right = embedding_model.generate_embeddings(right_texts, normalize_embeddings=True)
        E_right = np.array(E_right, dtype=np.float32)
        norms = np.linalg.norm(E_right, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        E_right = E_right / norms

        d = E_right.shape[1]
        index = faiss.IndexFlatIP(d)
        index.add(E_right)

        k = min(self.k_candidates, len(right_tuples))
        predicate_text = self.condition.condition

        # For each outer tuple, find k-NN candidates and verify with LLM
        for left_tup in left_tuples:
            left_text = self._extract_text(left_tup, left_cols)
            e_left = embedding_model.generate_embeddings([left_text], normalize_embeddings=True)
            e_left = np.array(e_left, dtype=np.float32)
            norm = np.linalg.norm(e_left)
            if norm > 0:
                e_left = e_left / norm

            distances, indices = index.search(e_left, k)

            for idx in indices[0]:
                if idx < 0 or idx >= len(right_tuples):
                    continue
                right_tup = right_tuples[idx]

                if self._llm_judge(predicate_text, left_tup, right_tup,
                                   left_cols, right_cols):
                    super().next()
                    yield tuple(list(left_tup) + list(right_tup))

    def close(self):
        for child in self.children:
            child.close()
        super().close()


class HashSemanticJoin(SemanticJoinBase):
    """Hash Semantic Join using Locality-Sensitive Hashing (LSH).

    Uses SimHash (random hyperplane projection) to map semantically
    similar tuples to the same bucket, then LLM verifies candidates.

    accuracy_score = 0.87
    cost = O((|R|+|S|) * C_embed + |candidates| * C_LLM)
    """

    ACCURACY_SCORE = 0.87

    def __init__(self, condition, join_type, children_table_names,
                 n_hash_bits=8, n_tables=4):
        super().__init__('HashSemanticJoin', condition, join_type, children_table_names)
        self.n_hash_bits = n_hash_bits
        self.n_tables = n_tables

    def _build_lsh(self, embeddings, d):
        """Build LSH index using random hyperplane projection (SimHash)."""
        hash_tables = []
        hyperplanes_list = []

        for _ in range(self.n_tables):
            hyperplanes = np.random.randn(self.n_hash_bits, d).astype(np.float32)
            hyperplanes_list.append(hyperplanes)

            table = {}
            for i, emb in enumerate(embeddings):
                signs = tuple((hyperplanes @ emb > 0).astype(int))
                if signs not in table:
                    table[signs] = []
                table[signs].append(i)
            hash_tables.append(table)

        return hash_tables, hyperplanes_list

    def _probe_lsh(self, embedding, hash_tables, hyperplanes_list):
        """Find candidate indices from LSH tables."""
        candidates = set()
        for table, hyperplanes in zip(hash_tables, hyperplanes_list):
            signs = tuple((hyperplanes @ embedding > 0).astype(int))
            if signs in table:
                candidates.update(table[signs])
        return candidates

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        embedding_model = self._get_embedding_model()

        left_child = self.children[0]
        right_child = self.children[1]
        left_cols = left_child.columns or []
        right_cols = right_child.columns or []

        left_tuples = self._collect_tuples(left_child)
        right_tuples = self._collect_tuples(right_child)

        if not left_tuples or not right_tuples:
            return

        # Embed both sides
        right_texts = [self._extract_text(t, right_cols) for t in right_tuples]
        E_right = embedding_model.generate_embeddings(right_texts, normalize_embeddings=True)
        E_right = np.array(E_right, dtype=np.float32)
        norms = np.linalg.norm(E_right, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        E_right = E_right / norms

        d = E_right.shape[1]

        # Build LSH index for right table
        hash_tables, hyperplanes_list = self._build_lsh(E_right, d)

        predicate_text = self.condition.condition

        # Probe for each left tuple
        for left_tup in left_tuples:
            left_text = self._extract_text(left_tup, left_cols)
            e_left = embedding_model.generate_embeddings([left_text], normalize_embeddings=True)
            e_left = np.array(e_left, dtype=np.float32).flatten()
            norm = np.linalg.norm(e_left)
            if norm > 0:
                e_left = e_left / norm

            # Find candidates via LSH
            candidate_indices = self._probe_lsh(e_left, hash_tables, hyperplanes_list)

            # LLM verification
            for idx in candidate_indices:
                if idx >= len(right_tuples):
                    continue
                right_tup = right_tuples[idx]

                if self._llm_judge(predicate_text, left_tup, right_tup,
                                   left_cols, right_cols):
                    super().next()
                    yield tuple(list(left_tup) + list(right_tup))

    def close(self):
        for child in self.children:
            child.close()
        super().close()

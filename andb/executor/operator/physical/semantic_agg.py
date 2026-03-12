"""Semantic aggregation physical operators: multiple strategies for semantic grouping.

Implements the paper's Table 2 physical operators for semantic aggregation:
- HashBasedSemanticAggregation: LLM classifies each tuple, then standard hash aggregation
- EmbeddingClusterAggregation: K-Means clustering on embeddings, then LLM names clusters
"""

import json
from collections import defaultdict

import numpy as np

from andb.executor.operator.physical.base import PhysicalOperator
from andb.executor.operator.physical.semantic import default_client_model, default_embedding_model
from andb.executor.operator.logical import AggregationFunctions


class SemanticAggregationBase(PhysicalOperator):
    """Base class for all semantic aggregation operators."""

    ACCURACY_SCORE = 0.5

    def __init__(self, name, group_instruction, agg_functions, grouping_columns):
        """
        Args:
            group_instruction: Text instruction for how to group
            agg_functions: list of FunctionColumn for aggregation (COUNT, SUM, etc.)
            grouping_columns: list of columns to group by
        """
        super().__init__(name)
        self.group_instruction = group_instruction
        self.agg_functions = agg_functions
        self.grouping_columns = grouping_columns
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
        """Extract relevant text from a tuple."""
        parts = []
        for i, col in enumerate(columns):
            if i < len(tup):
                parts.append(f"{col.column_name}: {tup[i]}")
        return "; ".join(parts)

    def _compute_aggregates(self, group_tuples, agg_functions, columns):
        """Compute aggregate values for a group of tuples.

        Returns dict mapping function column repr to aggregate value.
        """
        results = {}
        for func_col in agg_functions:
            func_name = func_col.function_name.upper()
            func_cls = AggregationFunctions.get(func_name)

            # Collect values for the aggregation column
            values = []
            for tup in group_tuples:
                for target_col in func_col.columns:
                    for i, col in enumerate(columns):
                        if col.column_name == target_col.column_name and i < len(tup):
                            values.append(tup[i])
                            break

            if func_cls:
                results[repr(func_col)] = func_cls()(values)
            elif func_name == 'COUNT':
                results[repr(func_col)] = len(values)
            else:
                results[repr(func_col)] = len(values)

        return results


class HashBasedSemanticAggregation(SemanticAggregationBase):
    """Hash-based semantic aggregation: LLM classifies each tuple.

    Phase 1: LLM generates a group key for each tuple
    Phase 2: Standard hash aggregation on generated keys
    Phase 3: Normalize similar key names using embedding similarity

    accuracy_score = 0.95
    cost = O(N * C_LLM)
    """

    ACCURACY_SCORE = 0.95

    def __init__(self, group_instruction, agg_functions, grouping_columns):
        super().__init__('HashBasedSemanticAggregation', group_instruction,
                         agg_functions, grouping_columns)

    def _normalize_keys(self, keys):
        """Normalize similar keys using embedding similarity."""
        if len(set(keys)) <= 1:
            return keys

        try:
            embedding_model = self._get_embedding_model()
            unique_keys = list(set(keys))
            if len(unique_keys) <= 1:
                return keys

            embeddings = embedding_model.generate_embeddings(unique_keys, normalize_embeddings=True)
            embeddings = np.array(embeddings, dtype=np.float32)
            norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
            norms = np.where(norms == 0, 1, norms)
            embeddings = embeddings / norms

            # Merge keys with similarity >= 0.9
            key_map = {}
            for i, key in enumerate(unique_keys):
                if key in key_map:
                    continue
                key_map[key] = key
                for j in range(i + 1, len(unique_keys)):
                    if unique_keys[j] in key_map:
                        continue
                    sim = np.dot(embeddings[i], embeddings[j])
                    if sim >= 0.9:
                        key_map[unique_keys[j]] = key

            return [key_map.get(k, k) for k in keys]
        except Exception:
            return keys

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        client_model = self._get_client_model()
        child = self.children[0]
        columns = child.columns or []

        # Collect all tuples
        tuples = []
        for tup in child.next():
            tuples.append(tup)

        if not tuples:
            return

        # Phase 1: LLM generates group keys
        keys = []
        for tup in tuples:
            text = self._extract_text(tup, columns)
            messages = [
                {"role": "system",
                 "content": "Classify the data into a single category. "
                            "Output ONLY the category name (a short label)."},
                {"role": "user",
                 "content": f"Instruction: {self.group_instruction}\n"
                            f"Data: {text}\n"
                            f"Category:"}
            ]
            try:
                response = client_model.complete_messages(messages, max_tokens=32, temperature=0.0)
                key = response.strip().strip('"').strip("'")
                keys.append(key)
            except Exception:
                keys.append("unknown")

        # Normalize similar keys
        keys = self._normalize_keys(keys)

        # Phase 2: Hash aggregation
        groups = defaultdict(list)
        for key, tup in zip(keys, tuples):
            groups[key].append(tup)

        # Phase 3: Output results
        for key, group_tuples in groups.items():
            agg_results = self._compute_aggregates(group_tuples, self.agg_functions, columns)
            # Build output row: (group_key, agg_val1, agg_val2, ...)
            row = [key]
            for func_col in self.agg_functions:
                row.append(agg_results.get(repr(func_col), 0))
            super().next()
            yield tuple(row)

    def close(self):
        for child in self.children:
            child.close()
        super().close()


class EmbeddingClusterAggregation(SemanticAggregationBase):
    """Embedding cluster aggregation: K-Means then LLM naming.

    Phase 1: Embed all tuples
    Phase 2: K-Means clustering
    Phase 3: LLM names each cluster
    Phase 4: Standard aggregation within clusters

    accuracy_score = 0.80
    cost = O(N*C_embed + k*C_LLM)
    """

    ACCURACY_SCORE = 0.80

    def __init__(self, group_instruction, agg_functions, grouping_columns,
                 k=5, n_iter=20):
        super().__init__('EmbeddingClusterAggregation', group_instruction,
                         agg_functions, grouping_columns)
        self.k = k
        self.n_iter = n_iter

    def open(self):
        super().open()
        for child in self.children:
            child.open()

    def next(self):
        import faiss

        embedding_model = self._get_embedding_model()
        client_model = self._get_client_model()
        child = self.children[0]
        columns = child.columns or []

        # Collect all tuples
        tuples = []
        texts = []
        for tup in child.next():
            tuples.append(tup)
            texts.append(self._extract_text(tup, columns))

        if not tuples:
            return

        # Phase 1: Embed
        E = embedding_model.generate_embeddings(texts, normalize_embeddings=True)
        E = np.array(E, dtype=np.float32)
        norms = np.linalg.norm(E, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        E = E / norms

        d = E.shape[1]
        k = min(self.k, len(tuples))

        # Phase 2: K-Means clustering
        kmeans = faiss.Kmeans(d, k, niter=self.n_iter, verbose=False)
        kmeans.train(E)
        _, assignments = kmeans.index.search(E, 1)
        assignments = assignments.flatten()

        # Group tuples by cluster
        clusters = defaultdict(list)
        cluster_texts = defaultdict(list)
        for i, (tup, text) in enumerate(zip(tuples, texts)):
            cluster_id = int(assignments[i])
            clusters[cluster_id].append(tup)
            cluster_texts[cluster_id].append(text)

        # Phase 3: LLM names each cluster
        cluster_names = {}
        for cluster_id, group_texts in cluster_texts.items():
            # Take representatives closest to centroid
            representatives = group_texts[:5]
            examples = "\n".join(f"- {t}" for t in representatives)

            messages = [
                {"role": "system",
                 "content": "You name categories. Output ONLY a short label (1-4 words)."},
                {"role": "user",
                 "content": f"Name a category for this group of data:\n{examples}\n"
                            f"Context: {self.group_instruction}\n"
                            f"Category name:"}
            ]
            try:
                response = client_model.complete_messages(messages, max_tokens=32, temperature=0.0)
                cluster_names[cluster_id] = response.strip().strip('"').strip("'")
            except Exception:
                cluster_names[cluster_id] = f"group_{cluster_id}"

        # Phase 4: Aggregation
        for cluster_id, group_tuples in clusters.items():
            name = cluster_names.get(cluster_id, f"group_{cluster_id}")
            agg_results = self._compute_aggregates(group_tuples, self.agg_functions, columns)

            row = [name]
            for func_col in self.agg_functions:
                row.append(agg_results.get(repr(func_col), 0))
            super().next()
            yield tuple(row)

    def close(self):
        for child in self.children:
            child.close()
        super().close()

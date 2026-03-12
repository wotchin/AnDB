"""Semantic Histogram: embedding-space clustering for selectivity estimation.

Provides cardinality estimation for semantic predicates by clustering
text embeddings and using cluster-level statistics.

Theory:
    Given relation R's text column A, the semantic histogram H_sem(R.A)
    is a triple (C, W, f_embed) where:
    - C = {c_1, ..., c_k}: cluster centroids in R^d
    - W = {w_1, ..., w_k}: tuple counts per cluster
    - f_embed: String -> R^d embedding function

    Selectivity estimation:
    sel(p_sem, H_sem) = sum_{i: sim(e_q, c_i) >= tau} w_i / sum(w_i)

    Error bound: |sel - sel*| <= epsilon_cluster + epsilon_embed
"""

import struct
import numpy as np


class SemanticHistogram:
    """Semantic histogram based on embedding-space clustering.

    Uses K-Means clustering on text embeddings to build a compact
    representation of data distribution for selectivity estimation.
    """

    def __init__(self, table_oid, column_name, n_clusters=32):
        """
        Args:
            table_oid: Target table OID
            column_name: Target text column name
            n_clusters: Number of clusters k, recommend min(32, ceil(sqrt(n)))
        """
        self.table_oid = table_oid
        self.column_name = column_name
        self.n_clusters = n_clusters

        self.centroids = None  # shape: (k, d)
        self.weights = None  # shape: (k,)
        self.total_count = 0
        self.variances = None  # shape: (k,) - intra-cluster variance
        self._built = False

    @property
    def is_built(self):
        return self._built

    def build(self, text_values, embedding_model):
        """Build semantic histogram from text values.

        Time complexity: O(n*d*(1 + k*I))
        Space complexity: O(n*d) temporary + O(k*d) persistent

        Args:
            text_values: list of text strings
            embedding_model: EmbeddingModel instance with generate_embeddings()
        """
        import faiss

        n = len(text_values)
        if n == 0:
            self._built = True
            self.centroids = np.empty((0, 0), dtype=np.float32)
            self.weights = np.empty(0, dtype=np.float32)
            self.variances = np.empty(0, dtype=np.float32)
            return

        # Adjust k to be reasonable for the dataset size
        k = min(self.n_clusters, n, max(1, int(np.ceil(np.sqrt(n)))))

        # Generate embeddings
        embeddings = embedding_model.generate_embeddings(text_values, normalize_embeddings=True)
        embeddings = np.array(embeddings, dtype=np.float32)
        if embeddings.ndim == 1:
            embeddings = embeddings.reshape(1, -1)

        d = embeddings.shape[1]

        # L2 normalize
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1, norms)
        embeddings = embeddings / norms

        # K-Means clustering using FAISS
        kmeans = faiss.Kmeans(d, k, niter=20, verbose=False)
        kmeans.train(embeddings)

        self.centroids = np.array(kmeans.centroids, dtype=np.float32)

        # Compute cluster assignments
        _, assignments = kmeans.index.search(embeddings, 1)
        assignments = assignments.flatten()

        # Compute weights (tuple counts per cluster)
        self.weights = np.zeros(k, dtype=np.float32)
        for a in assignments:
            self.weights[a] += 1

        # Compute intra-cluster variances
        self.variances = np.zeros(k, dtype=np.float32)
        for i, a in enumerate(assignments):
            diff = embeddings[i] - self.centroids[a]
            self.variances[a] += np.dot(diff, diff)
        for i in range(k):
            if self.weights[i] > 0:
                self.variances[i] /= self.weights[i]

        self.total_count = n
        self._built = True

    def estimate_selectivity(self, predicate_text, embedding_model,
                             threshold=0.5, margin=0.1):
        """Estimate selectivity for a semantic predicate.

        Time complexity: O(k*d)

        Args:
            predicate_text: Semantic predicate text
            embedding_model: EmbeddingModel instance
            threshold: Similarity threshold tau
            margin: Soft-matching margin

        Returns:
            Selectivity value in [0.0, 1.0]
        """
        if not self._built or self.total_count == 0:
            return 0.5  # default estimate when no histogram

        # Embed the predicate
        e_q = embedding_model.generate_embeddings([predicate_text], normalize_embeddings=True)
        e_q = np.array(e_q, dtype=np.float32).flatten()
        norm = np.linalg.norm(e_q)
        if norm > 0:
            e_q = e_q / norm

        # Compute similarities with all centroids
        similarities = self.centroids @ e_q  # shape: (k,)

        # Weighted matching with soft margin
        matched_count = 0.0
        for i in range(len(self.weights)):
            sim = similarities[i]
            if sim >= threshold:
                matched_count += self.weights[i]
            elif sim >= threshold - margin:
                # Soft matching in boundary region
                ratio = (sim - (threshold - margin)) / margin
                matched_count += self.weights[i] * ratio

        sel = matched_count / self.total_count
        return max(0.0, min(1.0, sel))

    def estimate_cardinality(self, predicate_text, embedding_model,
                             threshold=0.5):
        """Estimate output cardinality = selectivity * total_count."""
        sel = self.estimate_selectivity(predicate_text, embedding_model, threshold)
        return int(sel * self.total_count)

    def serialize(self):
        """Serialize histogram for persistent storage."""
        if not self._built:
            return b''

        parts = []
        # Header: table_oid, n_clusters, total_count, embedding_dim
        k = len(self.weights)
        d = self.centroids.shape[1] if k > 0 else 0
        parts.append(struct.pack('!iiif', self.table_oid, k, self.total_count, 0.0))
        # Column name
        col_bytes = self.column_name.encode('utf-8')
        parts.append(struct.pack('!i', len(col_bytes)))
        parts.append(col_bytes)
        # Centroids, weights, variances
        if k > 0:
            parts.append(struct.pack('!i', d))
            parts.append(self.centroids.tobytes())
            parts.append(self.weights.tobytes())
            parts.append(self.variances.tobytes())
        else:
            parts.append(struct.pack('!i', 0))

        return b''.join(parts)

    @classmethod
    def deserialize(cls, data):
        """Restore histogram from serialized data."""
        if not data:
            return None

        offset = 0
        table_oid, k, total_count, _ = struct.unpack_from('!iiif', data, offset)
        offset += struct.calcsize('!iiif')

        col_name_len = struct.unpack_from('!i', data, offset)[0]
        offset += 4
        column_name = data[offset:offset + col_name_len].decode('utf-8')
        offset += col_name_len

        d = struct.unpack_from('!i', data, offset)[0]
        offset += 4

        hist = cls(table_oid, column_name, n_clusters=k)
        hist.total_count = total_count

        if k > 0 and d > 0:
            centroids_size = k * d * 4
            hist.centroids = np.frombuffer(data[offset:offset + centroids_size],
                                           dtype=np.float32).reshape(k, d).copy()
            offset += centroids_size

            weights_size = k * 4
            hist.weights = np.frombuffer(data[offset:offset + weights_size],
                                         dtype=np.float32).copy()
            offset += weights_size

            variances_size = k * 4
            hist.variances = np.frombuffer(data[offset:offset + variances_size],
                                           dtype=np.float32).copy()
        else:
            hist.centroids = np.empty((0, 0), dtype=np.float32)
            hist.weights = np.empty(0, dtype=np.float32)
            hist.variances = np.empty(0, dtype=np.float32)

        hist._built = True
        return hist

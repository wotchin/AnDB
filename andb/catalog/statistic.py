"""
Catalog for relation-level and column-level statistics.

Used by the cost model to make informed decisions about query plans.
Statistics are maintained by ANALYZE and updated as data changes.
"""

import threading


class RelationStatistic:
    """Statistics for a single relation (table)."""

    def __init__(self, oid, num_pages=0, num_tuples=0, avg_tuple_width=0):
        self.oid = oid
        self.num_pages = num_pages
        self.num_tuples = num_tuples
        self.avg_tuple_width = avg_tuple_width  # bytes

    def __repr__(self):
        return (f"RelationStatistic(oid={self.oid}, pages={self.num_pages}, "
                f"tuples={self.num_tuples}, width={self.avg_tuple_width})")


class ColumnStatistic:
    """Statistics for a single column."""

    def __init__(self, class_oid, attr_num, n_distinct=0, null_fraction=0.0,
                 avg_width=0, most_common_values=None, most_common_freqs=None,
                 histogram_bounds=None):
        self.class_oid = class_oid
        self.attr_num = attr_num
        self.n_distinct = n_distinct       # Number of distinct values (-1 = unique)
        self.null_fraction = null_fraction  # Fraction of NULLs (0.0 to 1.0)
        self.avg_width = avg_width          # Average width of column values in bytes
        self.most_common_values = most_common_values or []
        self.most_common_freqs = most_common_freqs or []
        self.histogram_bounds = histogram_bounds or []

    def __repr__(self):
        return (f"ColumnStatistic(oid={self.class_oid}, attr={self.attr_num}, "
                f"distinct={self.n_distinct}, null_frac={self.null_fraction})")


class StatisticCache:
    """In-memory cache for relation and column statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._relation_stats = {}   # oid -> RelationStatistic
        self._column_stats = {}     # (class_oid, attr_num) -> ColumnStatistic

    def update_relation_stats(self, oid, num_pages, num_tuples, avg_tuple_width=0):
        """Update or insert statistics for a relation."""
        with self._lock:
            self._relation_stats[oid] = RelationStatistic(
                oid, num_pages, num_tuples, avg_tuple_width
            )

    def get_relation_stats(self, oid):
        """Get statistics for a relation. Returns None if not available."""
        with self._lock:
            return self._relation_stats.get(oid)

    def update_column_stats(self, class_oid, attr_num, **kwargs):
        """Update or insert statistics for a column."""
        with self._lock:
            key = (class_oid, attr_num)
            existing = self._column_stats.get(key)
            if existing:
                for k, v in kwargs.items():
                    if hasattr(existing, k):
                        setattr(existing, k, v)
            else:
                self._column_stats[key] = ColumnStatistic(class_oid, attr_num, **kwargs)

    def get_column_stats(self, class_oid, attr_num):
        """Get statistics for a column. Returns None if not available."""
        with self._lock:
            return self._column_stats.get((class_oid, attr_num))

    def clear(self):
        """Clear all cached statistics."""
        with self._lock:
            self._relation_stats.clear()
            self._column_stats.clear()


# Global statistics cache instance
STAT_CACHE = StatisticCache()

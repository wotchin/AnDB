"""
Cost model for the AnDB query optimizer.

This module provides cost estimation for different physical operators.
The cost model uses a simple I/O-based model where:
- Sequential page read cost = 1.0 (baseline)
- Random page read cost = 4.0 (random I/O is ~4x slower)
- CPU tuple processing cost = 0.01
- CPU operator cost = 0.0025
- CPU index tuple cost = 0.005

These are similar to PostgreSQL's default cost parameters.
"""

import math

from andb.catalog.syscache import CATALOG_ANDB_CLASS, CATALOG_ANDB_INDEX, CATALOG_ANDB_ATTRIBUTE
from andb.common.utils import filesize_to_pageno
from andb.common.file_operation import file_size
from andb.storage.engines.heap.relation import open_relation, close_relation, RelationKinds
from andb.storage.lock.rlock import NO_LOCK


# Cost constants (similar to PostgreSQL defaults)
SEQ_PAGE_COST = 1.0         # Cost of a sequential page read
RANDOM_PAGE_COST = 4.0      # Cost of a random page read
CPU_TUPLE_COST = 0.01       # Cost of processing a tuple
CPU_INDEX_TUPLE_COST = 0.005  # Cost of processing an index entry
CPU_OPERATOR_COST = 0.0025  # Cost of evaluating a filter/operator

# Default statistics when actual stats are not available
DEFAULT_NUM_PAGES = 10
DEFAULT_NUM_TUPLES = 1000
DEFAULT_SELECTIVITY = 0.33  # Default selectivity for an equality predicate
DEFAULT_RANGE_SELECTIVITY = 0.5  # Default selectivity for range predicates


class RelationStats:
    """Statistics about a relation (table or index)."""

    def __init__(self, oid, num_pages=DEFAULT_NUM_PAGES, num_tuples=DEFAULT_NUM_TUPLES):
        self.oid = oid
        self.num_pages = num_pages
        self.num_tuples = num_tuples

    @property
    def tuples_per_page(self):
        if self.num_pages == 0:
            return self.num_tuples
        return self.num_tuples / self.num_pages


def get_relation_stats(relation_oid):
    """Get statistics for a relation. Falls back to defaults if not available."""
    relation = open_relation(relation_oid, lock_mode=NO_LOCK)
    if relation is None:
        return RelationStats(relation_oid)

    try:
        fsize = file_size(relation.fd)
        num_pages = max(1, filesize_to_pageno(fsize))
        # Estimate tuples based on pages
        # A rough estimate: assume average tuple ~100 bytes, page ~8KB => ~80 tuples/page
        estimated_tuples = num_pages * 80
        return RelationStats(relation_oid, num_pages, estimated_tuples)
    except Exception:
        return RelationStats(relation_oid)
    finally:
        close_relation(relation_oid, lock_mode=NO_LOCK)


def estimate_selectivity(condition):
    """Estimate the selectivity of a filter condition.

    Selectivity is a value between 0 and 1 representing the fraction of
    rows that pass the filter.

    Currently uses simple heuristics:
    - Equality (=): 0.1 (10% of rows)
    - Inequality (<, >, <=, >=): 0.33 (33% of rows)
    - NOT EQUAL (!=, <>): 0.9 (90% of rows)
    - AND: product of children selectivities
    - OR: 1 - product of (1 - child selectivity)
    """
    if condition is None:
        return 1.0

    from andb.executor.operator.utils import ExprOperation

    expr = condition.expr
    if expr == ExprOperation.EQ:
        return 0.1
    elif expr in (ExprOperation.LT, ExprOperation.GT,
                  ExprOperation.LTE, ExprOperation.GTE):
        return DEFAULT_RANGE_SELECTIVITY
    elif expr in (ExprOperation.NE,):
        return 0.9
    elif expr == ExprOperation.AND:
        sel = 1.0
        for child in condition.get_iterator():
            if child is not condition:
                sel *= estimate_selectivity(child)
        return sel
    elif expr == ExprOperation.OR:
        sel = 1.0
        for child in condition.get_iterator():
            if child is not condition:
                sel *= (1.0 - estimate_selectivity(child))
        return 1.0 - sel
    else:
        return DEFAULT_SELECTIVITY


class CostEstimate:
    """Represents the estimated cost of a plan node."""

    def __init__(self, startup_cost=0.0, total_cost=0.0, output_rows=0, output_width=0):
        self.startup_cost = startup_cost  # Cost before first row is returned
        self.total_cost = total_cost      # Total cost to return all rows
        self.output_rows = output_rows    # Estimated number of output rows
        self.output_width = output_width  # Estimated average row width in bytes

    def __repr__(self):
        return (f"Cost(startup={self.startup_cost:.2f}, "
                f"total={self.total_cost:.2f}, "
                f"rows={self.output_rows}, "
                f"width={self.output_width})")

    def __lt__(self, other):
        return self.total_cost < other.total_cost


def cost_table_scan(relation_oid, filter_condition=None):
    """Estimate cost of a sequential table scan.

    Cost = seq_page_cost * num_pages + cpu_tuple_cost * num_tuples
           + cpu_operator_cost * num_tuples (if filter)
    """
    stats = get_relation_stats(relation_oid)
    selectivity = estimate_selectivity(filter_condition)

    startup_cost = 0.0
    run_cost = (SEQ_PAGE_COST * stats.num_pages +
                CPU_TUPLE_COST * stats.num_tuples)
    if filter_condition:
        run_cost += CPU_OPERATOR_COST * stats.num_tuples

    total_cost = startup_cost + run_cost
    output_rows = max(1, int(stats.num_tuples * selectivity))

    return CostEstimate(
        startup_cost=startup_cost,
        total_cost=total_cost,
        output_rows=output_rows,
        output_width=100  # default estimate
    )


def cost_index_scan(index_oid, table_oid, filter_condition=None):
    """Estimate cost of an index scan.

    For an index scan:
    - Read index pages (mostly sequential)
    - For each matching key, random I/O to fetch heap page
    - Cost depends on selectivity

    Cost = index_pages * seq_page_cost
           + matching_tuples * random_page_cost (heap fetches)
           + matching_tuples * cpu_index_tuple_cost
           + matching_tuples * cpu_tuple_cost
    """
    table_stats = get_relation_stats(table_oid)
    index_stats = get_relation_stats(index_oid)
    selectivity = estimate_selectivity(filter_condition)

    matching_tuples = max(1, int(table_stats.num_tuples * selectivity))

    # Estimate number of heap pages that need to be fetched
    # Using Mackert-Lohman formula approximation
    if table_stats.num_pages <= 1:
        heap_pages_fetched = 1
    else:
        # Correlation factor: assume random distribution
        heap_pages_fetched = min(
            matching_tuples,
            int(table_stats.num_pages * (1 - math.exp(-matching_tuples / table_stats.num_pages)))
        )
        heap_pages_fetched = max(1, heap_pages_fetched)

    # Index traversal cost (log N for B-tree, plus leaf pages)
    if index_stats.num_pages > 0:
        tree_height = max(1, int(math.log2(max(1, index_stats.num_pages))))
    else:
        tree_height = 1
    index_startup_cost = tree_height * RANDOM_PAGE_COST

    index_scan_cost = (
        index_startup_cost +
        selectivity * index_stats.num_pages * SEQ_PAGE_COST +
        heap_pages_fetched * RANDOM_PAGE_COST +
        matching_tuples * CPU_INDEX_TUPLE_COST +
        matching_tuples * CPU_TUPLE_COST
    )

    if filter_condition:
        index_scan_cost += matching_tuples * CPU_OPERATOR_COST

    return CostEstimate(
        startup_cost=index_startup_cost,
        total_cost=index_scan_cost,
        output_rows=matching_tuples,
        output_width=100
    )


def cost_covered_index_scan(index_oid, filter_condition=None):
    """Estimate cost of a covered (index-only) scan.

    No heap fetches needed - all data comes from the index.
    Much cheaper than a regular index scan.
    """
    index_stats = get_relation_stats(index_oid)
    selectivity = estimate_selectivity(filter_condition)

    matching_tuples = max(1, int(index_stats.num_tuples * selectivity))

    if index_stats.num_pages > 0:
        tree_height = max(1, int(math.log2(max(1, index_stats.num_pages))))
    else:
        tree_height = 1

    startup_cost = tree_height * RANDOM_PAGE_COST
    run_cost = (
        selectivity * index_stats.num_pages * SEQ_PAGE_COST +
        matching_tuples * CPU_INDEX_TUPLE_COST
    )

    if filter_condition:
        run_cost += matching_tuples * CPU_OPERATOR_COST

    return CostEstimate(
        startup_cost=startup_cost,
        total_cost=startup_cost + run_cost,
        output_rows=matching_tuples,
        output_width=50
    )


def cost_nested_loop_join(outer_cost, inner_cost, join_condition=None):
    """Estimate cost of a nested loop join.

    Cost = outer_cost + outer_rows * inner_cost + join_filter_cost
    """
    selectivity = estimate_selectivity(join_condition)

    startup_cost = outer_cost.startup_cost + inner_cost.startup_cost
    run_cost = (
        outer_cost.total_cost +
        outer_cost.output_rows * inner_cost.total_cost +
        outer_cost.output_rows * inner_cost.output_rows * CPU_OPERATOR_COST
    )

    output_rows = max(1, int(outer_cost.output_rows * inner_cost.output_rows * selectivity))

    return CostEstimate(
        startup_cost=startup_cost,
        total_cost=startup_cost + run_cost,
        output_rows=output_rows,
        output_width=outer_cost.output_width + inner_cost.output_width
    )


def cost_sort(input_cost, sort_columns_count=1):
    """Estimate cost of a sort operation.

    Uses comparison-based sort: O(n log n) comparisons.
    """
    n = max(1, input_cost.output_rows)
    comparisons = n * math.log2(max(2, n))

    startup_cost = input_cost.total_cost  # Must read all input first
    sort_cost = comparisons * CPU_OPERATOR_COST * sort_columns_count

    return CostEstimate(
        startup_cost=startup_cost + sort_cost,
        total_cost=startup_cost + sort_cost + n * CPU_TUPLE_COST,
        output_rows=n,
        output_width=input_cost.output_width
    )


def cost_hash_aggregation(input_cost, num_groups=None):
    """Estimate cost of a hash aggregation.

    Cost = input_cost + hashing_cost + output_cost
    """
    n = max(1, input_cost.output_rows)
    if num_groups is None:
        num_groups = max(1, n // 10)  # rough estimate

    startup_cost = input_cost.total_cost  # Must process all input
    hash_cost = n * CPU_OPERATOR_COST  # Hash each input tuple
    output_cost = num_groups * CPU_TUPLE_COST

    return CostEstimate(
        startup_cost=startup_cost + hash_cost,
        total_cost=startup_cost + hash_cost + output_cost,
        output_rows=num_groups,
        output_width=input_cost.output_width
    )

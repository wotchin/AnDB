from andb.storage.engines.heap.relation import close_relation, open_relation
from andb.storage.lock import rlock
from andb.errno.errors import InitializationStageError, ExecutionStageError, FinalizationStageError
from andb.storage.engines.heap.relation import hot_simple_select, bt_search_range, bt_search, bt_scan_all_keys
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_INDEX, CATALOG_ANDB_CLASS
from andb.runtime import global_vars, session_vars
from andb.sql.parser.ast.misc import Star

from ..logical import Condition, TableColumn
from ..utils import expression_eval, ExprOperation
from .base import PhysicalOperator


def compare_values(expr, left_values, right_values):
    if expr == 'and':
        rv = True
        for left_value in left_values:
            for right_value in right_values:
                rv = rv and expression_eval('and', left_value, right_value)
                if not rv:  # None, False
                    return rv
    elif expr == 'or':
        rv = False
        for left_value in left_values:
            for right_value in right_values:
                rv = rv or expression_eval('and', left_value, right_value)
                if rv:  # True
                    return rv
    elif expr == 'in':
        assert len(left_values) == 1
        return left_values[0] in right_values
    else:
        assert len(left_values) == 1 and len(right_values) == 1
        return expression_eval(expr, left_values[0], right_values[0])

    return rv


class Filter(PhysicalOperator):

    def __init__(self, condition: Condition):
        super().__init__('Filter')
        self.condition = condition
        self.column_condition = {}

        def dfs(node: Condition):
            if node is None:
                return False
            if not isinstance(node.left, TableColumn):
                # node.right can be int, float, TableColumn, Condition ...
                return False

            # construct mapper
            if node.left not in self.column_condition:
                self.column_condition[node.left] = []
            self.column_condition[node.left].append(node)

            # check validity for all condition expression
            left_validity = True
            right_validity = True
            if isinstance(node.left, Condition):
                left_validity = dfs(node.left)
            if isinstance(node.right, Condition):
                right_validity = dfs(node.right)
            return left_validity and right_validity

        assert dfs(condition)

        self.attr_num_cache = {}
        for table_column in self.column_condition:
            table_oid = CATALOG_ANDB_CLASS.get_relation_oid(table_column.table_name, session_vars.database_oid)
            self.attr_num_cache[table_column] = CATALOG_ANDB_ATTRIBUTE.get_table_attr_num(table_oid,
                                                                                          table_column.column_name)

    def judge(self, column_value_pairs):
        def dfs(node: Condition):
            if node is None:
                raise ValueError(f'node should not be None.')

            left_value = node.left
            right_value = node.right
            if isinstance(node.left, Condition):
                left_value = dfs(node.left)
            if isinstance(node.right, Condition):
                right_value = dfs(node.right)

            if isinstance(left_value, TableColumn):
                left_value = column_value_pairs[left_value]
            if isinstance(right_value, TableColumn):
                right_value = column_value_pairs[right_value]

            if not isinstance(left_value, list):
                left_value = [left_value]
            if not isinstance(right_value, list):
                right_value = [right_value]
            return compare_values(node.expr.value, left_value, right_value)

        return dfs(self.condition)

    def filter(self, iterator):
        columns = list(self.column_condition.keys())
        assert len(columns) > 0
        assert isinstance(columns[0], TableColumn)

        for tuple_ in iterator:
            if not tuple_:
                break

            pairs = {column: [] for column in columns}
            for column in columns:
                attr_num = self.attr_num_cache[column]
                pairs[column].append(tuple_[attr_num])

            if self.judge(pairs):
                yield tuple_

    def next(self):
        assert len(self.children) == 1
        child = self.children[0]
        assert isinstance(child, Scan)

        yield self.filter(child.next())


class Scan(PhysicalOperator):
    START_POSITION = -1

    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__('Scan')
        self.relation_oid = relation_oid
        self.columns = columns
        self.projection_attr_idx = None
        self._filter = filter_
        self.relation = None
        self.lock = lock
        self._pageno = 0
        self._tid = 0

    def set_cursor(self, pageno, tid):
        self._pageno, self._tid = pageno, tid

    def get_cursor(self):
        return self._pageno, self._tid

    def open(self):
        self.relation = open_relation(self.relation_oid, self.lock)
        if not self.relation:
            raise InitializationStageError(f'cannot open relation {self.relation_oid}.')

        attr_form_array = CATALOG_ANDB_ATTRIBUTE.get_table_forms(self.relation_oid)
        self.projection_attr_idx = []

        # None means scanning all columns
        if not self.columns:
            self.columns = []
            for i, attr_form in enumerate(attr_form_array):
                assert i == attr_form.num
                self.projection_attr_idx.append(attr_form.num)
                self.columns.append(TableColumn(table_name=self.relation.name, column_name=attr_form.name))
        else:
            for column in self.columns:
                for attr_form in attr_form_array:
                    if attr_form.name == column.column_name:
                        self.projection_attr_idx.append(attr_form.num)

    def next(self):
        for tuple_ in self.project():
            yield tuple_

    def project(self):
        for tuple_ in self.filter():
            new_tuple = []
            for idx in self.projection_attr_idx:
                new_tuple.append(tuple_[idx])
            yield tuple(new_tuple)

    def filter(self):
        if not self._filter:
            for tuple_ in self.next_internal():
                yield tuple_
        else:
            for tuple_ in self._filter.filter(self.next_internal()):
                yield tuple_

    def next_internal(self):
        raise NotImplementedError()

    def close(self):
        close_relation(self.relation_oid, self.lock)


class IndexScan(Scan):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'IndexScan'
        self.index_forms = None
        self.table_attr_forms = None
        self.index_columns = None
        self.table_relation = None

    def open(self):
        super().open()
        self.index_forms = CATALOG_ANDB_INDEX.get_index_forms(self.relation_oid)
        self.table_relation = open_relation(self.index_forms[0].table_oid, lock_mode=rlock.ACCESS_SHARE_LOCK)

        self.table_attr_forms = CATALOG_ANDB_INDEX.get_attr_form_array(self.relation_oid)
        self.index_columns = []
        for i, form in enumerate(self.index_forms):
            assert form.index_num == i
            assert self.table_attr_forms[i].num == form.attr_num
            self.index_columns.append(TableColumn(self.table_relation.name, self.table_attr_forms[form.attr_num].name))

    def close(self):
        super().close()
        close_relation(self.table_relation, lock_mode=rlock.ACCESS_SHARE_LOCK)

    @staticmethod
    def combine(columns, const_values):
        combinations = []

        def dfs(i, constructing):
            if i >= len(columns):
                combinations.append(constructing)
                return
            column = columns[i]
            column_values = const_values[column]
            for value in column_values:
                dfs(i + 1, constructing + [value])

        dfs(0, [])
        return combinations

    def fetch_tuple(self, key):
        for pointer in bt_search(self.relation, key=key):
            tuple_ = hot_simple_select(self.table_relation, pointer.pageno, pointer.tid)
            self.set_cursor(pointer.pageno, pointer.tid)
            if tuple_:
                yield tuple_

    def next_internal(self):
        # todo: range
        assert isinstance(self._filter.condition.expr, ExprOperation)
        const_values = {column: [] for column in self.index_columns}
        for column in self.index_columns:
            for node in self._filter.column_condition[column]:
                # todo: check node.left must be const value
                assert (isinstance(node.left, int)
                        or isinstance(node.left, float)
                        or isinstance(node.left, str)
                        or isinstance(node.left, bool)
                        or node.left is None)
                const_values[column].append(node.left)

        predicate_num = 0
        for column in self.index_columns:
            if not const_values[column]:
                break
            predicate_num += 1

        # follow leftmost prefix rule
        assert predicate_num > 0
        keys = self.combine(self.index_columns[:predicate_num], const_values)
        if self._filter.condition.expr == ExprOperation.EQ:
            for key in keys:
                for tuple_ in self.fetch_tuple(key):
                    yield tuple_
        else:
            raise NotImplementedError('not supported no-equal query')


class CoveredIndexScan(IndexScan):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'CoveredIndexScan'

    def open(self):
        super().open()
        self.projection_attr_idx = []
        for column in self.columns:
            idx = self.index_columns.index(column)
            self.projection_attr_idx.append(idx)

    def next_internal(self):
        if self._filter:
            for tuple_ in super().next_internal():
                yield tuple_
        else:
            # index only scan
            for tuple_ in bt_scan_all_keys(self.relation):
                yield tuple_


class TableScan(Scan):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'TableScan'

    def next_internal(self):
        for pageno in range(0, self.relation.last_pageno() + 1):
            buffer_page = global_vars.buffer_manager.get_page(self.relation, pageno)
            global_vars.buffer_manager.pin_page(buffer_page)
            for tid in range(0, len(buffer_page.page.item_ids)):
                tuple_ = hot_simple_select(self.relation, pageno, tid)
                self.set_cursor(pageno, tid)
                if tuple_:
                    yield tuple_
            global_vars.buffer_manager.unpin_page(buffer_page)


class TempTableScan(Scan):
    pass


class FunctionScan(Scan):
    pass


class Append(PhysicalOperator):
    pass


class Join(PhysicalOperator):
    pass


class HashJoin(Join):
    pass


class NestedLoopJoin(Join):
    pass


class SortMergeJoin(Join):
    pass


class Materialize(PhysicalOperator):
    pass


class Limit(PhysicalOperator):
    pass


class SortAgg(PhysicalOperator):
    pass


class HashAgg(PhysicalOperator):
    pass


class Sort(PhysicalOperator):
    pass


class QuickSort(Sort):
    pass


class HeapSort(Sort):
    pass


class OutSort(Sort):
    pass


class PhysicalQuery(PhysicalOperator):

    def __init__(self, logical_query):
        super().__init__('Query')
        self.logical_query = logical_query
        self.logical_plan = logical_query.children[0]
        self.simple_plan = len(logical_query.scan_operators)
        self.has_join_clause = logical_query.join_operator is not None
        self.scan_operators = []

    def open(self):
        if len(self.children) == 0:
            raise InitializationStageError('not found children.')

        self.children[0].open()

    def next(self):
        for tuple_ in self.children[0].next():
            yield tuple_

    def close(self):
        self.children[0].close()

from andb.storage.engines.heap.relation import close_relation, open_relation
from andb.storage.lock import rlock
from andb.errno.errors import InitializationStageError, ExecutionStageError, FinalizationStageError
from andb.storage.engines.heap.relation import hot_simple_select, bt_search_range, bt_search, bt_scan_all_keys
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_INDEX, CATALOG_ANDB_CLASS
from andb.runtime import global_vars, session_vars
from andb.sql.parser.ast.misc import Star
from andb.sql.parser.ast.join import JoinType

from ..logical import Condition, TableColumn
from ..utils import expression_eval, ExprOperation
from .base import PhysicalOperator


class Filter(PhysicalOperator):

    def __init__(self, condition: Condition):
        super().__init__('Filter')
        self.condition = condition
        self.column_condition = {}
        self.columns = None
        self._index_of_tuple_lookup = {}

        self._construct_mapper()

    def get_args(self):
        return (('condition', self.condition),) + super().get_args()

    def __repr__(self):
        return f'{self.name}: {self.condition}'

    def _construct_mapper(self):
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

        dfs(self.condition)

    def set_tuple_columns(self, columns):
        # operator must tell the filter columns, otherwise, filter cannot
        # know the meaning of each tuple's column.
        self.columns = columns

    def get_index_of_tuple_by_column(self, lookup_table_column):
        # if self._index_of_tuple_lookup:
        #     for table_column in self.column_condition:
        #         table_oid = CATALOG_ANDB_CLASS.get_relation_oid(table_column.table_name, session_vars.database_oid)
        #         self._index_of_tuple_lookup[table_column] = CATALOG_ANDB_ATTRIBUTE.get_table_attr_num(table_oid,
        #                                                                                       table_column.column_name)
        assert self.columns
        # construct a lookup hashtable to speed up searching
        if not self._index_of_tuple_lookup:
            for i, column in enumerate(self.columns):
                self._index_of_tuple_lookup[column] = i
        return self._index_of_tuple_lookup[lookup_table_column]

    def compare_values(self, expr, left_values, right_values):
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
            return self.compare_values(node.expr.value, left_value, right_value)

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
                attr_num = self.get_index_of_tuple_by_column(column)
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

        # set each input column name for tuple filter
        if self._filter:
            columns = []
            for attr_form in attr_form_array:
                table_column = TableColumn(self.relation.name, attr_form.name)
                columns.append(table_column)
            self._filter.set_tuple_columns(columns)

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

    def get_args(self):
        if self._filter:
            return (('index_name', self.relation.name), ('index_oid', self.relation_oid),
                    ('condition', self._filter)) + super().get_args()
        return (('index_name', self.relation.name), ('index_oid', self.relation_oid)) + super().get_args()

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

    def get_args(self):
        if self._filter:
            return (('table_name', self.relation.name), ('table_oid', self.relation_oid),
                    ('condition', self._filter)) + super().get_args()
        return (('table_name', self.relation.name), ('table_oid', self.relation_oid)) + super().get_args()

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


class Append(Scan):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'Append'

    def open(self):
        for child in self.children:
            child.open()

    def next_internal(self):
        for child in self.children:
            for tuple_ in child.next():
                yield tuple_

    def close(self):
        for child in self.children:
            child.close()


class TempTableScan(Append):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'TempTableScan'


class FunctionScan(Scan):
    pass


class Join(PhysicalOperator):
    def __init__(self, join_operator, join_type, target_columns=None, join_filter: Filter = None):
        super().__init__(join_operator)
        self.columns = target_columns  # target columns
        self.projection_attr_idx = []
        self.join_columns = None
        self.join_filter = join_filter
        self.join_type = join_type

    def get_args(self):
        return (('join_type', self.join_type), ('condition', self.join_filter)) + super().get_args()

    def open(self):
        # open can initialize all works
        # if one certain operator's columns is None, by using open method we can set value for that.
        self.left_tree.open()
        self.right_tree.open()
        self.join_columns = self.left_tree.columns + self.right_tree.columns
        if not self.columns:
            self.columns = self.join_columns

        # todo: semi-join and anti semi-join should prune self.columns

        # check all target columns are all in joined columns
        # time complexity can reduce to O(N) from O(N2)
        for i, target_column in enumerate(self.columns):
            for j, join_column in enumerate(self.join_columns):
                if target_column == join_column:
                    self.projection_attr_idx.append(j)

        if len(self.projection_attr_idx) != len(self.columns):
            raise InitializationStageError('target columns are not all in joined columns.')

        if self.join_filter:
            self.join_filter.set_tuple_columns(columns=self.join_columns)

    def close(self):
        self.left_tree.close()
        self.right_tree.close()

    def next(self):
        for tuple_ in self.project():
            yield tuple_

    def project(self):
        for tuple_ in self.join_generator():
            new_tuple = []
            for idx in self.projection_attr_idx:
                new_tuple.append(tuple_[idx])
            yield tuple(new_tuple)

    def join_generator(self):
        if self.join_type == JoinType.CROSS_JOIN:
            return self.cross_join()
        elif self.join_type == JoinType.INNER_JOIN:
            return self.inner_join()
        # in our code, left table is outer table in default, so we use a parameter
        # exchange tuple to mark we need to exchange tuples' position.
        elif self.join_type == JoinType.LEFT_JOIN:
            return self.outer_join(self.left_tree, self.right_tree, exchange_tuple=False)
        elif self.join_type == JoinType.RIGHT_JOIN:
            return self.outer_join(self.right_tree, self.left_tree, exchange_tuple=True)
        elif self.join_type == JoinType.FULL_JOIN:
            return self.full_join()
        else:
            raise NotImplementedError(f'not supported {self.join_type}.')

    def cross_join(self):
        raise NotImplementedError()

    def inner_join(self):
        raise NotImplementedError()

    def outer_join(self, outer_table, inner_table, exchange_tuple):
        raise NotImplementedError()

    def full_join(self):
        raise NotImplementedError()


class NestedLoopJoin(Join):
    def __init__(self, join_type, target_columns=None, join_filter: Filter = None):
        super().__init__('NestedLoopJoin', join_type, target_columns, join_filter)

    def cross_join(self):
        for left_tuple in self.left_tree.next():
            for right_tuple in self.right_tree.next():
                yield left_tuple + right_tuple

    def inner_join(self):
        for joined_tuple in self.join_filter.filter(self.cross_join()):
            yield joined_tuple

    def outer_join(self, outer_table, inner_table, exchange_tuple=False):
        attr_nums = {}
        for column in self.join_filter.column_condition:
            attr_num = self.join_filter.get_index_of_tuple_by_column(column)
            attr_nums[column] = attr_num

        # prepare to use for outer join
        if not exchange_tuple:
            padding_nulls = tuple(None for _ in range(len(inner_table.columns)))
        else:
            padding_nulls = tuple(None for _ in range(len(outer_table.columns)))

        # left table is the outer table, so that we can transform right join to its dual left join depending on
        # our needs in the optimization phase.
        for outer_tuple in outer_table.next():
            matching_tuples = []
            for inner_tuple in inner_table.next():
                if not exchange_tuple:
                    joined_tuple = outer_tuple + inner_tuple
                else:
                    joined_tuple = inner_tuple + outer_tuple

                # construct a value pair for judgment
                value_pairs = {}
                for column in attr_nums:
                    attr_num = attr_nums[column]
                    value_pairs[column] = joined_tuple[attr_num]

                if self.join_filter.judge(value_pairs):
                    matching_tuples.append(joined_tuple)

            # if outer join, we should fill up null for the result.
            if not matching_tuples:
                if not exchange_tuple:
                    matching_tuples.append(outer_tuple + padding_nulls)
                else:
                    matching_tuples.append(padding_nulls + outer_tuple)

            for t in matching_tuples:
                yield t

    def full_join(self):
        attr_nums = {}
        for column in self.join_filter.column_condition:
            attr_num = self.join_filter.get_index_of_tuple_by_column(column)
            attr_nums[column] = attr_num

        # prepare to use for outer join
        left_table_nulls = tuple(None for _ in range(len(self.left_tree.columns)))
        right_table_nulls = tuple(None for _ in range(len(self.right_tree.columns)))

        materialized_left_tuples = []
        materialized_right_tuples = []
        for left_tuple in self.left_tree.next():
            materialized_left_tuples.append(left_tuple)
        for right_tuple in self.right_tree.next():
            materialized_right_tuples.append(right_tuple)

        # left join first
        for left_tuple in materialized_left_tuples:
            matching_tuples = []
            for right_tuple in materialized_right_tuples:
                joined_tuple = left_tuple + right_tuple
                # construct a value pair for judgment
                value_pairs = {}
                for column in attr_nums:
                    attr_num = attr_nums[column]
                    value_pairs[column] = joined_tuple[attr_num]

                if self.join_filter.judge(value_pairs):
                    matching_tuples.append(joined_tuple)

            if not matching_tuples:
                matching_tuples.append(left_tuple + right_table_nulls)

            for t in matching_tuples:
                yield t

        # right join second
        for right_tuple in materialized_right_tuples:
            not_matched = True
            for left_tuple in materialized_left_tuples:
                joined_tuple = left_tuple + right_tuple
                # construct a value pair for judgment
                value_pairs = {}
                for column in attr_nums:
                    attr_num = attr_nums[column]
                    value_pairs[column] = joined_tuple[attr_num]

                if self.join_filter.judge(value_pairs):
                    not_matched = False
                    break

            if not_matched:
                yield left_table_nulls + right_tuple


class HashJoin(Join):
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
        self.has_join_clause = len(logical_query.join_operators) > 0

    def open(self):
        if len(self.children) == 0:
            raise InitializationStageError('not found children.')

        self.children[0].open()

    def next(self):
        for tuple_ in self.children[0].next():
            yield tuple_

    def close(self):
        self.children[0].close()

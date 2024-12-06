import os
from andb.ai import embedding_model
from andb.sql.parser.ast.operation import Function
from andb.storage.engines.heap.relation import close_relation, open_relation
from andb.storage.lock import rlock
from andb.errno.errors import InitializationStageError, ExecutionStageError, FinalizationStageError
from andb.storage.engines.heap.relation import hot_simple_select, bt_search_range, bt_search, bt_scan_all_keys
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_INDEX, CATALOG_ANDB_FUNCTIONS, get_all_catalogs
from andb.runtime import global_vars, session_vars
from andb.sql.parser.ast.misc import Constant, Star
from andb.sql.parser.ast.join import JoinType

from ..logical import Condition, DummyTableName, TableColumn, AggregationFunctions, FunctionColumn
from ..utils import expression_eval, ExprOperation
from .base import PhysicalOperator
from andb.executor.operator import utils

class ExpressionContext:
    def __init__(self, column_value_pairs):
        self.column_value_pairs = column_value_pairs

    def get_column_value(self, table_name, column_name):
        return self.column_value_pairs.get(TableColumn(table_name, column_name), None)

def evaluate_expression(expr, context):
    if isinstance(expr, FunctionColumn):
        # evaluate function call
        function_name = expr.function_name
        columns = [evaluate_expression(column, context) for column in expr.columns]
        # get function result
        return CATALOG_ANDB_FUNCTIONS.perform_function(function_name, session_vars.SessionVars.database_oid, columns)
    elif isinstance(expr, ExprOperation):
        # evaluate operator, such as <, >, =, etc.
        left = evaluate_expression(expr.left, context)
        right = evaluate_expression(expr.right, context)
        return expression_eval(expr.op, left, right)
    elif isinstance(expr, TableColumn):
        # get column value from context
        return context.get_column_value(expr.table_name, expr.column_name)
    elif isinstance(expr, Constant):
        return expr.value
    elif isinstance(expr, (int, str, float, bool, type(None))):
        # sometimes, the value is not wrapped in Constant
        return expr
    else:
        raise NotImplementedError(f"Expression type '{type(expr)}' is not supported.")


class Filter(PhysicalOperator):

    def __init__(self, condition: Condition):
        super().__init__('Filter')
        self.condition = condition
        self.column_condition = {}
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
            if not isinstance(node.left, (TableColumn, FunctionColumn)):
                # node.right can be int, float, TableColumn, Condition ...
                return False

            # construct mapper
            node_left_columns = []
            if isinstance(node.left, FunctionColumn):
                for column in node.left.columns:
                    if isinstance(column, TableColumn):
                        node_left_columns.append(column)
            elif isinstance(node.left, TableColumn):
                node_left_columns.append(node.left)
            else:
                raise NotImplementedError('not supported this type of column.')

            for column in node_left_columns:
                if column not in self.column_condition:
                    self.column_condition[column] = []
                self.column_condition[column].append(node)

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
        def inner_dfs(node: Condition):
            if node is None:
                raise ValueError(f'node should not be None.')

            left_value = node.left
            right_value = node.right
            if isinstance(node.left, Condition):
                left_value = inner_dfs(node.left)
            if isinstance(node.right, Condition):
                right_value = inner_dfs(node.right)

            context = ExpressionContext(column_value_pairs)

            # evaluate left and right
            # left_evaluated = evaluate_expression(left_value, context) if isinstance(left_value, FunctionColumn) else (
            #     column_value_pairs[left_value] if isinstance(left_value, TableColumn) else left_value
            # )
            # right_evaluated = evaluate_expression(right_value, context) if isinstance(right_value, FunctionColumn) else (
            #     column_value_pairs[right_value] if isinstance(right_value, TableColumn) else right_value
            # )
            left_evaluated = evaluate_expression(left_value, context)
            right_evaluated = evaluate_expression(right_value, context)

            return expression_eval(node.expr.value, left_evaluated, right_evaluated)

        return inner_dfs(self.condition)

    def filter(self, iterator):
        columns = list(self.column_condition.keys())
        assert len(columns) > 0
        assert isinstance(columns[0], TableColumn)

        for tuple_ in iterator:
            if not tuple_:
                break

            pairs = {column: None for column in columns}
            for column in columns:
                attr_num = self.get_index_of_tuple_by_column(column)
                pairs[column] = tuple_[attr_num]

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
        self.base_table_oid = relation_oid  # for index scan
        # if specify columns, that means the scan involves projection
        self.columns = columns
        self.projection_attr_idx = None
        self._filter = filter_
        self.relation = None
        self.base_table_relation = None
        self.lock = lock
        self._pageno = 0
        self._tid = 0

    def set_cursor(self, pageno, tid):
        self._pageno, self._tid = pageno, tid

    def get_cursor(self):
        return self._pageno, self._tid

    def open(self):
        super().open()

        self.relation = open_relation(self.relation_oid, self.lock)
        self.base_table_relation = open_relation(self.base_table_oid, self.lock)
        if not self.relation:
            raise InitializationStageError(f'cannot open relation {self.relation_oid}.')

        attr_form_array = CATALOG_ANDB_ATTRIBUTE.get_table_forms(self.base_table_oid)
        self.projection_attr_idx = []

        # set each input column name for tuple filter
        if self._filter:
            columns = []
            for attr_form in attr_form_array:
                table_column = TableColumn(self.base_table_relation.name, attr_form.name)
                columns.append(table_column)
            self._filter.set_tuple_columns(columns)

        # None means scanning all columns
        if not self.columns:
            self.columns = []
            for i, attr_form in enumerate(attr_form_array):
                assert i == attr_form.num
                self.projection_attr_idx.append(attr_form.num)
                self.columns.append(TableColumn(table_name=self.base_table_relation.name, column_name=attr_form.name))
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
        close_relation(self.base_table_oid, self.lock)
        super().close()

    def get_args(self):
        return (('columns', self.columns),) + super().get_args()


class IndexScan(Scan):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'IndexScan'
        # for index scan, the relation_oid is the index oid, but we need the base table oid
        # to get the attribute forms.
        self.base_table_oid = CATALOG_ANDB_INDEX.get_index_forms(relation_oid)[0].table_oid
        self.index_forms = None
        self.table_attr_forms = None
        self.index_columns = None

    def get_args(self):
        if self._filter:
            return (('index_name', self.relation.name), ('index_oid', self.relation_oid),
                    ('condition', self._filter)) + super().get_args()
        return (('index_name', self.relation.name), ('index_oid', self.relation_oid)) + super().get_args()

    def open(self):
        super().open()

        self.index_forms = CATALOG_ANDB_INDEX.get_index_forms(self.relation_oid)
        self.table_attr_forms = CATALOG_ANDB_INDEX.get_attr_form_array(self.relation_oid)
        self.index_columns = []
        for i, form in enumerate(self.index_forms):
            assert form.index_num == i
            assert self.table_attr_forms[i].num == form.attr_num
            self.index_columns.append(TableColumn(self.base_table_relation.name, self.table_attr_forms[form.attr_num].name))

    def close(self):
        super().close()

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
            tuple_ = hot_simple_select(self.base_table_relation, pointer.pageno, pointer.tid)
            self.set_cursor(pointer.pageno, pointer.tid)
            if tuple_:
                yield tuple_

    def next_internal(self):
        #TODO: range
        assert isinstance(self._filter.condition.expr, ExprOperation)
        const_values = {column: [] for column in self.index_columns}
        for column in self.index_columns:
            for node in self._filter.column_condition[column]:
                if utils.is_const_value(node.left) and isinstance(node.right, TableColumn):
                    const_values[column].append(node.left)
                elif utils.is_const_value(node.right) and isinstance(node.left, TableColumn):
                    const_values[column].append(node.right)
                else:
                    raise NotImplementedError('constant value on both sides.')

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


class SystemTableScan(TableScan):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'SystemTableScan'

    def next_internal(self):
        for catalog_table in get_all_catalogs():
            if catalog_table.__oid__ == self.relation_oid:
                for catalog_form in catalog_table.rows:
                    yield catalog_form.to_tuple(catalog_form)
                break
                

class Append(Scan):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'Append'

    def open(self):
        super().open()
        
        columns = None
        for child in self.children:
            child.open()
            if columns is None:
                columns = child.columns
            else:
                if columns != child.columns:
                    raise RuntimeError('Columns do not match')

    def next_internal(self):
        for child in self.children:
            for tuple_ in child.next():
                yield tuple_

    def close(self):
        for child in self.children:
            child.close()
        
        super().close()

class TempTableScan(Append):
    def __init__(self, relation_oid, columns, filter_: Filter = None, lock=rlock.ACCESS_SHARE_LOCK):
        super().__init__(relation_oid, columns, filter_, lock)
        self.name = 'TempTableScan'


class FunctionScan(Scan):
    pass



class FileScan(PhysicalOperator):
    def __init__(self, file_path, columns):
        super().__init__('FileScan')
        self.file_path = file_path
        self.fd = None
        self.columns = columns
    
    def open(self):
        if self.file_path[-3:] != 'txt':
            raise NotImplementedError(f"File {self.file_path} is not supported")
        
        real_file_path = os.path.join(os.path.realpath('./files'), self.file_path)

        self.fd = open(real_file_path, 'r', errors='ignore')

    def next(self):
        assert len(self.columns) == 2  # content, embedding

        content = self.fd.readlines()
        embedding = embedding_model.text_to_embedding(content)
        yield (str(content), embedding)
    
    def close(self):
        self.fd.close()


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
        super().open()

        # open can initialize all works
        # if one certain operator's columns is None, by using open method we can set value for that.
        self.left_tree.open()
        self.right_tree.open()
        self.join_columns = self.left_tree.columns + self.right_tree.columns
        if not self.columns:
            self.columns = self.join_columns

        #TODO: semi-join and anti semi-join should prune self.columns

        # check all target columns are all in joined columns
        # time complexity can reduce to O(N) from O(N2)
        for i, target_column in enumerate(self.columns):
            for j, join_column in enumerate(self.join_columns):
                if target_column == join_column:
                    self.projection_attr_idx.append(j)
                    # when we found an index, we don't need to go ahead
                    # otherwise, such as self-join, we will add redundant element
                    break

        if len(self.projection_attr_idx) != len(self.columns):
            raise InitializationStageError('target columns are not all in joined columns.')

        if self.join_filter:
            self.join_filter.set_tuple_columns(columns=self.join_columns)

    def close(self):
        self.left_tree.close()
        self.right_tree.close()
        
        super().close()

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
    def __init__(self, name):
        super().__init__(name)
        self.in_memory_tuples = []
        self.gathered = False

    def gather(self):
        if self.gathered:
            return
        for child in self.children:
            for tuple_ in child.next():
                self.in_memory_tuples.append(tuple_)
        self.gathered = True


class Limit(PhysicalOperator):
    pass


class Aggregation(Materialize):
    pass


class SortAggregation(Aggregation):
    pass


class HashAggregation(Aggregation):
    def __init__(self, function_name, aggregation_columns, grouping_columns, agg_condition: Filter = None):
        super().__init__('HashAggregation')
        self.function_name = function_name
        self.aggregation_columns = aggregation_columns
        self.grouping_columns = grouping_columns
        self.aggregation_column_idx = None
        self.grouping_column_idx = None
        self.agg_condition = agg_condition
        self._hash_table = {}

    def get_args(self):
        return (('function_name', self.function_name), ('groupby', self.grouping_columns),
                ('aggregation', self.aggregation_columns)) + super().get_args()

    def open(self):
        super().open()
        
        assert len(self.children) == 1
        child = self.children[0]
        child.open()

        #TODO: fix the relationship between involved_columns and self.columns
        # and their using places.
        involved_columns = self.grouping_columns + self.aggregation_columns
        self.columns = self.grouping_columns + [FunctionColumn(self.function_name, self.aggregation_columns)]
        output_table_columns = set(c.core() for c in involved_columns)
        input_table_columns = set(c.core() for c in child.columns)
        if not output_table_columns.issubset(input_table_columns):
            raise InitializationStageError(f'not found all group keys {self.grouping_columns}.')

        if len(self.aggregation_columns) != 1 or len(self.grouping_columns) != 1:
            raise NotImplementedError('only supported one column to aggregate or group.')
        self.aggregation_column_idx = child.columns.index(self.aggregation_columns[0].core())
        self.grouping_column_idx = child.columns.index(self.grouping_columns[0].core())

        # check for having clause
        if self.agg_condition:
            self.agg_condition.set_tuple_columns(involved_columns)

    def next_internal(self):
        self.gather()

        # Step 1: Hashing and Grouping
        for row in self.in_memory_tuples:
            key = row[self.grouping_column_idx]
            value = row[self.aggregation_column_idx]

            if key not in self._hash_table:
                self._hash_table[key] = [value]
            else:
                self._hash_table[key].append(value)

        # Step 2: Aggregation
        for key, values in self._hash_table.items():
            aggregated_value = getattr(AggregationFunctions, self.function_name).value(values)
            yield key, aggregated_value

    def next(self):
        if self.agg_condition:
            for tuple_ in self.agg_condition.filter(self.next_internal()):
                yield tuple_
        else:
            for tuple_ in self.next_internal():
                yield tuple_

    def close(self):
        self.children[0].close()
        
        super().close()

class Sort(Materialize):
    INTERNAL_SORT = 'internal_sort'
    EXTERNAL_SORT = 'external_sort'
    HEAP_SORT = 'heap_sort'

    def __init__(self, sort_columns, ascending_orders=None):
        super().__init__('Sort')
        self.sort_columns = sort_columns
        self.sort_key_num = []
        self.sort_key_ascending_order = ascending_orders

        assert len(self.sort_columns) == len(self.sort_key_ascending_order)

        # default method is internal sort (quick sort)
        self.sort_method = self.INTERNAL_SORT

    def _sort_keys(self):
        assert self.sort_key_num
        assert self.sort_key_ascending_order
        output = []
        for k, asc in zip(self.sort_columns, self.sort_key_ascending_order):
            output.append(f'{k}' if asc else f'{k} DESC')
        return ','.join(output)

    def get_args(self):
        return (('sort_method', self.sort_method), ('keys', self._sort_keys())) + super().get_args()

    def open(self):
        super().open()
        if len(self.children) != 1:
            raise InitializationStageError('sort operator only can have one child.')
        child = self.children[0]
        child.open()
        self.columns = child.columns

        # construct an index array for lookup
        for sort_column in self.sort_columns:
            # find the ith for our sort columns
            for i, table_column in enumerate(self.columns):
                if table_column == sort_column:
                    self.sort_key_num.append(i)
                    break
        assert len(self.sort_key_num) == len(self.sort_columns)

    @staticmethod
    def quick_sort(arr_, sort_keys_, asc_=None):
        def custom_quick_sort(arr, low, high, sort_keys, asc):
            if low < high:
                pi = custom_partition(arr, low, high, sort_keys, asc)

                custom_quick_sort(arr, low, pi - 1, sort_keys, asc)
                custom_quick_sort(arr, pi + 1, high, sort_keys, asc)

        def custom_partition(arr, low, high, sort_keys, asc):
            pivot = arr[high]
            i = low - 1

            for j in range(low, high):
                if compare_keys(arr[j], pivot, sort_keys, asc) < 0:
                    i += 1
                    arr[i], arr[j] = arr[j], arr[i]

            arr[i + 1], arr[high] = arr[high], arr[i + 1]
            return i + 1

        def compare_keys(row1, row2, sort_keys, asc):
            for key in sort_keys:
                cmp_result = (row1[key] > row2[key]) - (row1[key] < row2[key])
                if cmp_result != 0:
                    return cmp_result if asc[sort_keys.index(key)] else -cmp_result
            return 0

        if asc_ is None:
            asc_ = [False] * len(sort_keys_)
        custom_quick_sort(arr_, 0, len(arr_) - 1, sort_keys_, asc_)
        return arr_

    def sort(self):
        if self.sort_method == self.INTERNAL_SORT:
            self.quick_sort(self.in_memory_tuples, self.sort_key_num, self.sort_key_ascending_order)
            sorted_tuples = self.in_memory_tuples
        else:
            raise NotImplementedError(f'not supported this sort method {self.sort_method}.')

        return sorted_tuples

    def next(self):
        self.gather()
        for tuple_ in self.sort():
            yield tuple_

    def close(self):
        self.in_memory_tuples.clear()

        child = self.children[0]
        child.close()


class PhysicalQuery(PhysicalOperator):

    def __init__(self, logical_query):
        super().__init__('Result')
        self.logical_query = logical_query
        self.logical_plan = logical_query.children[0]
        self.simple_plan = len(logical_query.scan_operators)
        self.has_join_clause = len(logical_query.join_operators) > 0
        self.projection_column_idx = []

    def open(self):
        if len(self.children) == 0:
            raise InitializationStageError('not found children.')

        super().open()
        self.children[0].open()
        child_columns = self.children[0].columns
        self.columns = self.logical_query.target_list
        # only output target columns
        for target_column in self.columns:
            for i, child_column in enumerate(child_columns):
                if target_column == child_column:
                    self.projection_column_idx.append(i)
                    break

    def next(self):
        for child in self.children:
            self.actual_rows += 1
            for tup in child.next():
                # projecting
                yield tuple(tup[i] for i in self.projection_column_idx)

    def close(self):
        self.children[0].close()
        super().close()

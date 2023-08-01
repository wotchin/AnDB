from andb.executor.operator.physical import select, insert, delete, update
from andb.catalog.oid import INVALID_OID
from andb.runtime import session_vars, global_vars
from andb.catalog.syscache import CATALOG_ANDB_CLASS, CATALOG_ANDB_INDEX, CATALOG_ANDB_ATTRIBUTE
from andb.storage.engines.heap.relation import RelationKinds
from andb.errno.errors import InitializationStageError
from andb.executor.operator.physical.select import TableScan, IndexScan, CoveredIndexScan, Filter

from .base import BaseImplementation
from .patterns import *


class UtilityImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, UtilityOperator)

    @classmethod
    def on_implement(cls, old_operator):
        return old_operator.physical_operator


class ScanImplementation(BaseImplementation):
    @staticmethod
    def _find_corresponding_index(table_oid):
        indexes = {}
        for index_form in CATALOG_ANDB_INDEX.search(lambda r: r.table_oid == table_oid):
            index_name = index_form.name
            index_oid = index_form.oid
            if index_name not in indexes:
                indexes[index_oid] = []
            indexes[index_oid].append(index_form)
        return indexes

    @staticmethod
    def _extract_predicates(condition: Condition):
        # todo: process OR
        predicates = []
        if not condition:
            return predicates
        for node in condition.get_iterator():
            if node.is_constant_condition():
                predicates.append(node)
            if node.expr.value == 'or':
                raise NotImplementedError('not supported OR expression')
        return predicates

    @staticmethod
    def _is_index_matched(index_forms, table_attr_nums, follow_leftmost_prefix_rule=False):
        matched_count = 0
        for i, index_form in enumerate(index_forms):
            for j, attr_num in enumerate(table_attr_nums):
                if attr_num == index_form.attr_num:
                    # if follows leftmost prefix rule, all attributions must be the same order
                    if follow_leftmost_prefix_rule and i != j:
                        break
                    matched_count += 1
        return matched_count == len(table_attr_nums)

    @classmethod
    def _implement_scan_operator(cls, scan_operator):
        predicates = cls._extract_predicates(scan_operator.condition)
        if len(predicates) == 0:
            return TableScan(relation_oid=scan_operator.table_oid, columns=scan_operator.table_columns, filter_=None)
        table_forms = CATALOG_ANDB_ATTRIBUTE.get_table_forms(scan_operator.table_oid)
        table_attr_nums = []
        for condition in predicates:
            for table_form in table_forms:
                if condition.left.column_name == table_form.name:
                    table_attr_nums.append(table_form.num)

        # rule-based optimizer
        # todo: currently, only supports simple index
        candidate_indexes = []
        all_indexes = cls._find_corresponding_index(scan_operator.table_oid)
        for index_oid in all_indexes:
            if cls._is_index_matched(all_indexes[index_oid], table_attr_nums, follow_leftmost_prefix_rule=True):
                candidate_indexes.append(index_oid)

        # todo: use selectivity
        if len(candidate_indexes) == 0:
            return TableScan(relation_oid=scan_operator.table_oid, columns=scan_operator.table_columns,
                             filter_=Filter(scan_operator.condition))

        # rule: choose covered index scan first
        for index_oid in candidate_indexes:
            # if they are both length, it means we got a covered index.
            if len(table_attr_nums) == len(all_indexes[index_oid]):
                return CoveredIndexScan(relation_oid=index_oid, columns=scan_operator.table_columns,
                                        filter_=Filter(scan_operator.condition))

        # rule: choose the shortest index
        shortest_index_oid = candidate_indexes[0]
        for index_oid in candidate_indexes:
            if len(all_indexes[index_oid]) <= len(all_indexes[shortest_index_oid]):
                shortest_index_oid = index_oid

        return IndexScan(relation_oid=shortest_index_oid, columns=scan_operator.table_columns,
                         filter_=Filter(scan_operator.condition))

    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, select.PhysicalQuery) and len(operator.scan_operators) == 0

    @classmethod
    def on_implement(cls, query: select.PhysicalQuery):
        # todo: according to SP
        # todo: index scan, table scan and index only scan
        scan_operator: "ScanOperator"
        for scan_operator in query.logical_query.scan_operators:
            query.scan_operators.append(cls._implement_scan_operator(scan_operator))
        # todo: other operators
        if len(query.scan_operators) > 1:
            raise NotImplementedError('not supported join')

        query.add_child(query.scan_operators[0])


class JoinImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, select.PhysicalQuery) and operator.has_join_clause

    @classmethod
    def on_implement(cls, old_operator):
        raise NotImplementedError()


class QueryImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, LogicalQuery)
        # todo: according to SP
        # todo: index scan, table scan and index only scan

    @classmethod
    def on_implement(cls, old_operator: LogicalQuery):
        physical_query = select.PhysicalQuery(old_operator)
        # todo: non-SJP, estimation

        # SPJ
        if ScanImplementation.match(physical_query):
            ScanImplementation.on_implement(physical_query)
        if JoinImplementation.match(physical_query):
            JoinImplementation.on_implement(physical_query)
        return physical_query


class InsertImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, InsertOperator)

    @classmethod
    def on_implement(cls, old_operator: InsertOperator):
        select_physical_plan = None
        if old_operator.select:
            select_physical_plan = QueryImplementation.on_implement(old_operator.select)

        return insert.InsertPhysicalOperator(table_oid=old_operator.table_oid,
                                             python_tuples=old_operator.values, select=select_physical_plan)


class DeleteImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, DeleteOperator)

    @classmethod
    def on_implement(cls, old_operator: DeleteOperator):
        table_oid = CATALOG_ANDB_CLASS.get_relation_oid(old_operator.table_name,
                                                        database_oid=session_vars.database_oid,
                                                        kind=RelationKinds.HEAP_TABLE)
        if table_oid == INVALID_OID:
            raise InitializationStageError(f'not found table {old_operator.table_name}.')

        physical_query = QueryImplementation.on_implement(old_operator.query)

        return delete.DeletePhysicalOperator(
            table_oid, physical_query.scan_operators[0]
        )


class UpdateImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, UpdateOperator)

    @classmethod
    def on_implement(cls, old_operator: UpdateOperator):
        table_oid = CATALOG_ANDB_CLASS.get_relation_oid(relation_name=old_operator.table_name,
                                                        database_oid=session_vars.database_oid,
                                                        kind=RelationKinds.HEAP_TABLE)
        if table_oid == INVALID_OID:
            raise InitializationStageError(f'cannot get oid for the table {old_operator.table_name}.')

        physical_query = QueryImplementation.on_implement(old_operator.query)

        attr_num_value_pair = {}
        table_attrs = CATALOG_ANDB_ATTRIBUTE.get_table_forms(table_oid)
        for table_column, value in zip(old_operator.columns, old_operator.values):
            for table_attr in table_attrs:
                if table_column.column_name == table_attr.name:
                    attr_num_value_pair[table_attr.num] = value
        if len(attr_num_value_pair) != len(old_operator.columns):
            raise InitializationStageError(f'cannot update these columns: {attr_num_value_pair.keys()}')

        return update.UpdatePhysicalOperator(table_oid=table_oid, scan_operator=physical_query.scan_operators[0],
                                             attr_num_value_pair=attr_num_value_pair)


_all_implementations = [impl() for impl in BaseImplementation.__subclasses__()]


def andb_logical_plan_implement(logical_plan):
    for impl in _all_implementations:
        if impl.match(logical_plan):
            return impl.on_implement(logical_plan)
    return logical_plan

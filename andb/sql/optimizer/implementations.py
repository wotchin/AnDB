from andb.catalog.oid import INVALID_OID, OID_SCANNING_FILE, OID_TEMP_TABLE
from andb.catalog.syscache import CATALOG_ANDB_CLASS, CATALOG_ANDB_INDEX, CATALOG_ANDB_ATTRIBUTE
from andb.errno.errors import InitializationStageError
from andb.executor.operator.physical import select, insert, delete, semantic, update, utility
from andb.executor.operator.physical.select import FileScan, TableScan, IndexScan, CoveredIndexScan, Filter
from andb.runtime import session_vars
from andb.sql.parser.ast.join import JoinType
from andb.storage.engines.heap.relation import RelationKinds
from .base import BaseImplementation
from .patterns import *


class UtilityImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, UtilityOperator)

    @classmethod
    def on_implement(cls, old_operator):
        if isinstance(old_operator.physical_operator, utility.ExplainOperator):
            explain_operator = old_operator.physical_operator
            explain_operator.physical_plan = andb_logical_plan_implement(explain_operator.logical_plan)
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
        #TODO: process OR
        predicates = []
        if not condition:
            return predicates
        for node in condition.get_iterator():
            if node.is_constant_comparison():
                predicates.append(node)
            elif node.is_function_comparison():
                predicates.append(node)
            if node.expr.value == 'or':
                raise NotImplementedError('not supported OR expression')
            
        return predicates

    @staticmethod
    def _is_index_matched(index_forms, table_attr_nums, follow_leftmost_prefix_rule=False):
        matched_count = 0
        for i, index_form in enumerate(index_forms):
            for j, attr_num in enumerate(table_attr_nums):
                #TODO: now, we only support single column index
                if attr_num == index_form.attr_num:
                    # if follows leftmost prefix rule, all attributions must be the same order
                    if follow_leftmost_prefix_rule and i != j:
                        break
                    matched_count += 1
        return matched_count == len(table_attr_nums)
    
    @staticmethod
    def _is_covered_index_matched(index_forms, table_attr_nums):
        index_attr_nums = [form.attr_num for form in index_forms]
        if len(index_attr_nums) != len(table_attr_nums):
            return False
        for i, index_attr_num in enumerate(index_attr_nums):
            for j, table_attr_num in enumerate(table_attr_nums):
                #TODO: now, we only support single column index
                if table_attr_num == index_attr_num:
                    # if follows leftmost prefix rule, all attributions must be the same order
                    if i != j:
                        return False
        return True

    @classmethod
    def _implement_scan_operator(cls, scan_operator):
        # temp table scan
        assert scan_operator.table_oid != INVALID_OID
        table_kind = CATALOG_ANDB_CLASS.get_relation_kind(scan_operator.table_oid)

        if scan_operator.table_oid == OID_SCANNING_FILE:
            return select.FileScan(file_path=scan_operator.table_name, 
                                   columns=scan_operator.table_columns)
        if scan_operator.table_oid == OID_TEMP_TABLE:
            assert table_kind == RelationKinds.TEMPORARY_TABLE
            return select.TempTableScan(scan_operator.table_oid, scan_operator.table_columns,
                                        filter_=Filter(scan_operator.condition))
        if table_kind == RelationKinds.SYSTEM_TABLE:
            return select.SystemTableScan(scan_operator.table_oid, scan_operator.table_columns,
                                          filter_=(None if scan_operator.condition is None
                                                     else Filter(scan_operator.condition)))
        if table_kind == RelationKinds.MEMORY_TABLE:
            raise InitializationStageError(f'not supported memory table {scan_operator.table_name}.')

        predicates = cls._extract_predicates(scan_operator.condition)
        if len(predicates) == 0:
            return TableScan(relation_oid=scan_operator.table_oid, 
                             columns=scan_operator.table_columns, filter_=None)
        table_forms = CATALOG_ANDB_ATTRIBUTE.get_table_forms(scan_operator.table_oid)
        predicate_attr_nums = []
        for condition in predicates:
            for table_form in table_forms:
                if isinstance(condition.left, TableColumn) and condition.left.column_name == table_form.name:
                    predicate_attr_nums.append(table_form.num)

        #TODO: we will support vector index in the future here.

        # rule-based optimizer
        #TODO: currently, only supports simple index
        candidate_indexes = []
        all_indexes = cls._find_corresponding_index(scan_operator.table_oid)
        for index_oid in all_indexes:
            if cls._is_index_matched(all_indexes[index_oid], predicate_attr_nums, follow_leftmost_prefix_rule=True):
                candidate_indexes.append(index_oid)

        #TODO: use selectivity
        if len(candidate_indexes) == 0:
            return TableScan(relation_oid=scan_operator.table_oid, columns=scan_operator.table_columns,
                             filter_=Filter(scan_operator.condition))

        # rule: try to choose covered index scan first
        target_form_nums = []
        for column in scan_operator.table_columns:
            for table_form in table_forms:
                if column.column_name == table_form.name:
                    target_form_nums.append(table_form.num)
        for index_oid in candidate_indexes:
            # if they are both length, it means we got a covered index.
            if cls._is_covered_index_matched(all_indexes[index_oid], target_form_nums):
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
        return isinstance(operator, ScanOperator)

    @classmethod
    def on_implement(cls, scan_operator: ScanOperator):
        #TODO: different options
        return cls._implement_scan_operator(scan_operator)


class JoinImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, JoinOperator)

    @classmethod
    def on_implement(cls, join_operator: JoinOperator):
        #TODO: choose the best join type (e.g., HashJoin, SortMergeJoin)
        left_node, right_node = join_operator.children[0], join_operator.children[1]
        if left_node.table_oid == OID_SCANNING_FILE or right_node.table_oid == OID_SCANNING_FILE:
            return semantic.SemanticJoin(join_type=join_operator.join_type,
                                        target_columns=join_operator.table_columns,
                                        join_filter=Filter(join_operator.join_condition))
        else:
            # that means the scan operator includes index scan
            return select.NestedLoopJoin(join_type=join_operator.join_type,
                                        target_columns=join_operator.table_columns,
                                        join_filter=Filter(join_operator.join_condition))



class SortImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, SortOperator)

    @classmethod
    def on_implement(cls, old_operator: SortOperator):
        return select.Sort(sort_columns=old_operator.sort_columns,
                           ascending_orders=old_operator.ascending_orders)


class AggregationImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, GroupOperator)

    @classmethod
    def on_implement(cls, old_operator: GroupOperator):
        #TODO: sort agg
        if old_operator.having_clause:
            agg_condition = Filter(old_operator.having_clause)
        else:
            agg_condition = None
        return select.HashAggregation(function_name=old_operator.aggregate_function.function_name,
                                      aggregation_columns=old_operator.aggregate_function.table_columns,
                                      grouping_columns=old_operator.group_by_columns,
                                      agg_condition=agg_condition)


class OperatorOption:
    def __init__(self):
        self.name = 'OperatorOption'
        self.implementations = []
        self.children = []

    def add_child(self, node):
        self.children.append(node)


class QueryImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, LogicalQuery)

    @staticmethod
    def implement_tree(node):
        if node is None:
            return None

        if ScanImplementation.match(node):
            new_node = ScanImplementation.on_implement(node)
        elif JoinImplementation.match(node):
            new_node = JoinImplementation.on_implement(node)
        elif SortImplementation.match(node):
            new_node = SortImplementation.on_implement(node)
        elif AggregationImplementation.match(node):
            new_node = AggregationImplementation.on_implement(node)
        elif SemanticTransformImplementation.match(node):
            new_node = SemanticTransformImplementation.on_implement(node)
        else:
            raise NotImplementedError(f'unknown operator {node.name}')

        if len(node.children) == 0:
            return new_node

        for child in node.children:
            new_node.add_child(QueryImplementation.implement_tree(child))
        return new_node

    @classmethod
    def on_implement(cls, old_operator: LogicalQuery):
        physical_query = select.PhysicalQuery(old_operator)
        #TODO: non-SJP, estimation
        root_node = cls.implement_tree(physical_query.logical_query.children[0])
        physical_query.add_child(root_node)

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
                                                        database_oid=session_vars.SessionVars.database_oid,
                                                        kind=RelationKinds.HEAP_TABLE)
        if table_oid == INVALID_OID:
            raise InitializationStageError(f'not found table {old_operator.table_name}.')

        physical_query = QueryImplementation.on_implement(old_operator.query)

        return delete.DeletePhysicalOperator(
            table_oid, physical_query.children[0]
        )


class UpdateImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        return isinstance(operator, UpdateOperator)

    @classmethod
    def on_implement(cls, old_operator: UpdateOperator):
        table_oid = CATALOG_ANDB_CLASS.get_relation_oid(relation_name=old_operator.table_name,
                                                        database_oid=session_vars.SessionVars.database_oid,
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

        return update.UpdatePhysicalOperator(table_oid=table_oid, scan_operator=physical_query.children[0],
                                             attr_num_value_pair=attr_num_value_pair)


class SemanticTransformImplementation(BaseImplementation):
    @classmethod
    def match(cls, operator) -> bool:
        # Check if any target in the target list is a PromptColumn
        return isinstance(operator, SemanticTransformOperator)

    @classmethod 
    def on_implement(cls, old_operator):
        # Create semantic target list operator to process prompt columns
        return semantic.SemanticTransform(
            # Pass through the original columns
            target_columns=old_operator.columns,
            # Pass through any filter conditions
            prompt_text=old_operator.semantic_prompt
        )


_all_implementations = [impl() for impl in BaseImplementation.__subclasses__()]


def andb_logical_plan_implement(logical_plan):
    for impl in _all_implementations:
        if impl.match(logical_plan):
            return impl.on_implement(logical_plan)
    return logical_plan

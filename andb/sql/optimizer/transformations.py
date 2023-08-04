from andb.executor.operator.logical import *
from andb.executor.operator.physical.utility import CreateIndexOperator, CreateTableOperator, ExplainOperator
from andb.runtime import session_vars
from andb.sql.parser.ast.create import CreateTable, CreateIndex
from andb.sql.parser.ast.insert import Insert
from andb.sql.parser.ast.delete import Delete
from andb.sql.parser.ast.update import Update
from andb.sql.parser.ast.select import Select
from andb.sql.parser.ast.join import Join
from andb.sql.parser.ast.misc import Star
from andb.sql.parser.ast.explain import Explain
from andb.errno.errors import AnDBNotImplementedError, InitializationStageError
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_CLASS
from andb.catalog.oid import INVALID_OID
from andb.storage.engines.heap.relation import RelationKinds
from .base import BaseTransformation
from andb.executor.operator.utils import ExprOperation

# from .patterns import *
from ...executor.operator.utils import expression_eval


class UtilityTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, CreateIndex) or \
               isinstance(ast, CreateTable) or \
               isinstance(ast, Explain)

    @staticmethod
    def on_transform(ast):
        physical_operator = None
        if isinstance(ast, CreateIndex):
            fields = [id_.parts for id_ in ast.columns]
            physical_operator = CreateIndexOperator(index_name=ast.name.parts, table_name=ast.table_name.parts,
                                                    fields=fields, database_oid=session_vars.database_oid,
                                                    index_type=ast.index_type)
        elif isinstance(ast, CreateTable):
            physical_operator = CreateTableOperator(
                table_name=ast.name.parts, fields=ast.columns, database_oid=session_vars.database_oid
            )
        elif isinstance(ast, Explain):
            physical_operator = ExplainOperator(logical_plan=andb_ast_transform(ast.target))

        return UtilityOperator(physical_operator)


# def get_table_attr_forms(table_name, database_oid):
#     for class_form in CATALOG_ANDB_CLASS.search(lambda r: r.database_oid == database_oid
#                                                           and r.name == table_name
#                                                           and r.kind == RelationKinds.HEAP_TABLE):
#         return CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == class_form.oid)
#
#     return []
#
#
# def get_table_column_names(table_name, database_oid):
#     attr_forms = get_table_attr_forms(table_name, database_oid)
#     # todo: return type name?
#     return [attr_form.name for attr_form in attr_forms]


class ConditionTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Condition)

    @staticmethod
    def on_transform(ast: Condition):
        def swap(node: Condition):
            # let column is at left hand side
            if isinstance(node.right, TableColumn):
                node.left, node.right = node.right, node.left
            return node

        def dfs(node: Condition):
            if node is None:
                return
            if not isinstance(node, Condition):
                return node
            left_node = dfs(node.left)
            right_node = dfs(node.right)

            # only convert constant
            if (not isinstance(left_node, TableColumn) and
                    not isinstance(right_node, TableColumn)):
                return expression_eval(node.expr.value, node.left, node.right)
            else:
                node.left = left_node
                node.right = right_node
                swap(node)
            return node

        return dfs(ast)


#
# class PredicatePushDownTransformation(BaseTransformation):
#
#
#
#     @staticmethod
#     def match(query: LogicalQuery) -> bool:
#         if not isinstance(query, LogicalQuery):
#             return False
#         if not query.condition:
#             return False
#
#         # friendly to lookup
#         scan_operators = {}
#         for operator in query.scan_operators:
#             scan_operators[operator.table_name] = operator
#
#         # todo: reuse group
#         for condition in query.condition.get_iterator():
#             # a constant value predicate
#
#                 if condition.left.table_name not in scan_operators:
#                     raise
#                 scan_operators[]
#
#
#     @staticmethod
#     def on_transform(ast):
#         pass
#
#
# class ColumnPruneTransformation(BaseTransformation):
#     @staticmethod
#     def match(ast) -> bool:
#         return False
#
#     @staticmethod
#     def on_transform(ast):
#         return ast


class QueryLogicalPlanTransformation(BaseTransformation):
    # @staticmethod
    # def _condition_dfs(table_attr_forms, node: Condition):
    #     if node is None:
    #         return None
    #     if node.is_constant_condition():
    #         table_name = node.left.table_name
    #         table_oid = table_attr_forms[table_name][0].class_oid
    #         return ScanOperator(table_name, table_oid=table_oid, condition=node)
    #     left = QueryLogicalPlanTransformation._condition_dfs(table_attr_forms, node.left)
    #     right = QueryLogicalPlanTransformation._condition_dfs(table_attr_forms, node.right)
    #     if node.expr == ExprOperation.AND:
    #         # todo:
    #         pass
    #     elif node.expr == ExprOperation.OR:
    #         return AppendOperator([left, right])
    #     raise

    @staticmethod
    def match(query) -> bool:
        return isinstance(query, LogicalQuery) and len(query.children) == 0

    @staticmethod
    def process_non_join_scan(query: LogicalQuery):
        assert len(query.scan_operators) == 1  # todo: support 0 in future
        scan = query.scan_operators[0]

        if not query.condition:
            return scan

        # simple predicate pushdown
        scan.condition = query.condition
        scan.table_columns = []
        for table_column in query.target_list:
            # todo: because there may be function
            if table_column.table_name == scan.table_name:
                scan.table_columns.append(table_column)
        # column prune:
        # because this is non-join query, it is very simple. if this query has join clause, we have to
        # add join column for the scan operator.
        return scan

    @staticmethod
    def process_join_scan(query: LogicalQuery):
        condition_table_names = {}
        if query.condition:
            for condition in query.condition.get_iterator():
                if isinstance(condition.left, TableColumn):
                    condition_table_names[condition.left.table_name] = condition
                if isinstance(condition.right, TableColumn):
                    condition_table_names[condition.right.table_name] = condition

        scan_operator: "ScanOperator"
        for scan_operator in query.scan_operators:
            # if the query has a condition and this condition only contains one table,
            # push down the predicate (condition).
            if query.condition and len(condition_table_names) == 1 \
                    and scan_operator.table_name in condition_table_names:
                scan_operator.condition = query.condition

            scan_operator.table_columns = []
            for table_column in query.target_list:
                # todo: because there may be function
                if table_column.table_name == scan_operator.table_name:
                    scan_operator.table_columns.append(table_column)

        # add table columns that come from join conditions
        join_table_columns = []
        join_operator: "JoinOperator"
        for join_operator in query.join_operators:
            # skip cross join
            if not join_operator.join_condition:
                continue

            for condition in join_operator.join_condition.get_iterator():
                if isinstance(condition.left, TableColumn):
                    join_table_columns.append(condition.left)
                if isinstance(condition.right, TableColumn):
                    join_table_columns.append(condition.right)
            # todo: can be further pruned
            # join_operator.table_columns = None
        for join_table_column in join_table_columns:
            for scan_operator in query.scan_operators:
                if scan_operator.table_name != join_table_column.table_name:
                    continue
                if join_table_column not in scan_operator.table_columns:
                    scan_operator.table_columns.append(join_table_column)

        if len(set(condition_table_names.values())) > 1 or len(query.join_operators) > 1:
            # need a filter to filter results
            # temp_scan = ScanOperator(table_name=ScanOperator.TEMP_TABLE_NAME,
            #                          table_oid=INVALID_OID,
            #                          condition=query.condition)
            # for join_operator in query.join_operators:
            #     temp_scan.add_child(join_operator)
            # return temp_scan
            raise NotImplementedError('not supported multiple tables join')

        return query.join_operators[0]

    @staticmethod
    def on_transform(query: LogicalQuery):
        if not query.join_operators:
            query.children.append(
                QueryLogicalPlanTransformation.process_non_join_scan(query)
            )
        else:
            query.children.append(
                QueryLogicalPlanTransformation.process_join_scan(query)
            )

        # todo: limit, group by, ...
        return query


class SelectTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Select)

    @staticmethod
    def _supplement_table_name(where_condition: Condition, table_attr_forms):
        for node in where_condition.get_iterator():
            for arg in (node.left, node.right):
                if not isinstance(arg, TableColumn):
                    continue

                if arg.table_name is not None:
                    found = False
                    for attr_form in table_attr_forms[arg.table_name]:
                        if attr_form.name == arg.column_name:
                            found = True
                            break
                    if not found:
                        raise InitializationStageError(f"not found '{str(arg)}'.")
                else:
                    arg_table_name = None
                    for table_name in table_attr_forms:
                        for attr_form in table_attr_forms[table_name]:
                            if attr_form.name == arg.column_name:
                                if arg_table_name is not None:
                                    raise InitializationStageError(
                                        f'both table {arg_table_name} and {table_name} have'
                                        f' the same column {arg.column_name}.')
                                arg_table_name = table_name
                    if arg_table_name is None:
                        raise InitializationStageError(f"not found '{arg.column_name}'.")
                    arg.table_name = arg_table_name
        return where_condition

    @classmethod
    def on_transform(cls, ast: Select):
        from_tables = {}
        unchecked_tables = []
        join_clause = None
        table_attr_forms = {}

        if isinstance(ast.from_table, Identifier):
            unchecked_tables.append(ast.from_table.parts)
        elif isinstance(ast.from_table, Join):
            join_clause = ast.from_table
            unchecked_tables.append(ast.from_table.left.parts)
            unchecked_tables.append(ast.from_table.right.parts)
        else:
            raise NotImplementedError()

        for table_name in unchecked_tables:
            table_oid = CATALOG_ANDB_CLASS.get_relation_oid(table_name, database_oid=session_vars.database_oid,
                                                            kind=RelationKinds.HEAP_TABLE)
            if table_oid != INVALID_OID:
                from_tables[table_name] = table_oid
                table_attr_forms[table_name] = CATALOG_ANDB_ATTRIBUTE.get_table_forms(table_oid)
            else:
                raise InitializationStageError(f'not found the table {table_name}.')

        target_columns = []
        for target in ast.targets:
            # parse star
            if isinstance(target, Star):
                for table_name in from_tables:
                    for attr_form in table_attr_forms[table_name]:
                        target_columns.append(TableColumn(table_name, attr_form.name))
            elif isinstance(target, Identifier) and '.' in target.parts:
                items = target.parts.split('.')
                if len(items) != 2:
                    raise InitializationStageError(f"syntax error: '{target.parts}'.")
                table_name, column_name = items
                if table_name not in from_tables:
                    raise InitializationStageError(f"not found '{target.parts}'.")
                found = False
                for attr_form in table_attr_forms[table_name]:
                    if attr_form.name == column_name:
                        found = True
                        break
                if not found:
                    raise InitializationStageError(f"not found '{target.parts}'.")
                target_columns.append(TableColumn(table_name, column_name))
            elif isinstance(target, Identifier):
                target_column_name = target.parts
                target_table_name = None
                for table_name in from_tables:
                    for attr_form in table_attr_forms[table_name]:
                        if attr_form.name == target_column_name:
                            if target_table_name is not None:
                                raise InitializationStageError(f'both table {target_table_name} and {table_name} have'
                                                               f' the same column {target_column_name}.')
                            target_table_name = table_name
                target_columns.append(TableColumn(target_table_name, target_column_name))
            else:
                # todo: function and agg
                raise NotImplementedError('not supported this syntax.')

        # projection_operator = ProjectionOperator(columns=target_columns)
        if ast.where is not None:
            where_condition = ConditionTransformation.on_transform(Condition(ast.where))
            # supplement missing table name and check existing table name
            where_condition = cls._supplement_table_name(where_condition, table_attr_forms)
            # selection_operator = SelectionOperator(condition=where_condition)
        else:
            # selection_operator = None
            where_condition = None

        scan_operators = []
        for table_name in from_tables:
            scan_operators.append(ScanOperator(table_name, table_oid=from_tables[table_name]))

        if join_clause:
            if not join_clause.implicit:
                join_condition = ConditionTransformation.on_transform(Condition(join_clause.condition))
                join_condition = cls._supplement_table_name(join_condition, table_attr_forms)
            else:
                join_condition = None

            join_operator = JoinOperator(join_condition=join_condition,
                                         join_type=join_clause.join_type)
            left_table_name, right_table_name = join_clause.left.parts, join_clause.right.parts
            left_scan_operator = right_scan_operator = None
            for scan_operator in scan_operators:
                if scan_operator.table_name == left_table_name:
                    left_scan_operator = scan_operator
                elif scan_operator.table_name == right_table_name:
                    right_scan_operator = scan_operator

            join_operator.add_child(left_scan_operator)
            join_operator.add_child(right_scan_operator)

        else:
            join_operator = None

        # todo: group by
        # todo: having
        # todo: distinct
        # todo: sort
        # todo: limit

        query = LogicalQuery()
        query.table_attr_forms = table_attr_forms

        query.target_list = target_columns
        query.condition = where_condition
        if join_operator:
            query.join_operators.append(join_operator)
        query.distinct = ast.distinct
        query.scan_operators = scan_operators

        if QueryLogicalPlanTransformation.match(query):
            query = QueryLogicalPlanTransformation.on_transform(query)

        return query


class InsertTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Insert)

    @staticmethod
    def on_transform(ast: Insert):
        table_oid = CATALOG_ANDB_CLASS.get_relation_oid(relation_name=ast.table.parts,
                                                        database_oid=session_vars.database_oid,
                                                        kind=RelationKinds.HEAP_TABLE)
        if table_oid == INVALID_OID:
            raise InitializationStageError(f'cannot get oid for the table {ast.table.parts}.')

        attr_forms = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == table_oid)
        if not attr_forms:
            raise InitializationStageError(f'cannot get the table {ast.table.parts}.')

        rows = []
        for value in ast.values:
            row = [None for _ in range(len(attr_forms))]
            if isinstance(value, Constant):
                row[0] = value.value
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    attr_num = attr_forms[i].num
                    row[attr_num] = v.value
            else:
                raise
            rows.append(row)
            for i in range(len(attr_forms)):
                if row[i] is None and attr_forms[i].notnull:
                    raise InitializationStageError(f'{attr_forms[i].name} should not be null.')

        if ast.columns:
            columns = [id_.parts for id_ in ast.columns]
        else:
            columns = [attr.name for attr in attr_forms]

        operator = InsertOperator(
            table_name=ast.table.parts, table_oid=table_oid, columns=columns,
            values=rows, select=None
        )
        if ast.from_select:
            select_logical_plan = SelectTransformation.on_transform(ast.from_select)
            operator.select = select_logical_plan

        return operator


class DeleteTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Delete)

    @staticmethod
    def on_transform(ast: Delete):
        # transform to a query
        # todo: extract where predicates
        select = Select(targets=[Star()])
        select.from_table = ast.table
        select.where = ast.where
        query = SelectTransformation.on_transform(select)
        return DeleteOperator(ast.table.parts, query)


class UpdateTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Update)

    @staticmethod
    def on_transform(ast: Update):
        columns = []
        values = []
        for column_name, value_expr in ast.columns.items():
            columns.append(TableColumn(table_name=ast.table.parts,
                                       column_name=column_name))
            if isinstance(value_expr, Constant):
                values.append(value_expr.value)
            else:
                raise NotImplementedError('not supported this syntax yet.')
        condition = ConditionTransformation.on_transform(Condition(ast.where))

        select = Select(targets=[Star()])
        select.from_table = ast.table
        select.where = ast.where
        query = SelectTransformation.on_transform(select)

        operator = UpdateOperator(table_name=ast.table.parts, query=query, columns=columns,
                                  values=values, condition=condition)
        return operator


_all_transformations = [trans() for trans in BaseTransformation.__subclasses__()]


def andb_ast_transform(ast):
    for trans in _all_transformations:
        if trans.match(ast):
            return trans.on_transform(ast)
    raise AnDBNotImplementedError('not supported this grammar yet')

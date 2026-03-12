import os

from andb.catalog.oid import INVALID_OID, OID_SCANNING_DIRECTORY
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_CLASS, CATALOG_ANDB_TYPE
from andb.errno.errors import AnDBNotImplementedError, InitializationStageError
from andb.executor.operator.logical import *
from andb.executor.operator.physical.utility import CreateIndexOperator, CreateMemoryTableOperator, CreateTableOperator, \
    ExplainOperator, DropTableOperator, DropMemoryTableOperator, DropIndexOperator, CommandOperator
from andb.runtime.session_vars import get_session_value
from andb.runtime import session_vars
from andb.sql.parser.ast.create import CreateTable, CreateIndex, CreateMemoryTable, DropMemoryTable
from andb.sql.parser.ast.delete import Delete
from andb.sql.parser.ast.drop import DropIndex, DropTable
from andb.sql.parser.ast.explain import Explain
from andb.sql.parser.ast.insert import Insert
from andb.sql.parser.ast.join import Join
from andb.sql.parser.ast.misc import Star
from andb.sql.parser.ast.operation import Function
from andb.sql.parser.ast.select import Select
from andb.sql.parser.ast.semantic import Prompt, FileSource, DirectorySource, SemanticTabular, SemanticGroup, \
    SemanticMatch
from andb.sql.parser.ast.s2ql import MatchesPredicate, ExtractExpression, TransformExpression, ClassifyingGroupBy
from andb.sql.parser.ast.update import Update
from andb.sql.parser.ast.utility import Command

from .base import BaseTransformation
from ...executor.operator.utils import expression_eval


class UtilityTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, (CreateIndex, CreateTable, DropTable, DropIndex, Explain, Command))

    @staticmethod
    def get_column_types(columns, select_ast):
        query = LogicalQuery()
        SelectTransformation.transform_from_clause(select_ast.from_table, query)
        SelectTransformation.transform_target_list(select_ast.targets, query)
        if columns is not None and len(query.target_list) != len(columns):
            raise InitializationStageError(
                f"Subquery returns {len(query.target_list)} columns but {len(columns)} are defined")

        defined_columns = []
        for i, col in enumerate(query.target_list):
            if columns is not None:
                col_name = columns[i].parts
            elif col.alias:
                col_name = col.alias
            else:
                col_name = col.column_name
            defined_columns.append([col_name, col.type])

        return defined_columns

    @classmethod
    def on_transform(cls, ast):
        physical_operator = None
        if isinstance(ast, CreateIndex):
            fields = [id_.parts for id_ in ast.columns]
            physical_operator = CreateIndexOperator(index_name=ast.name.parts, table_name=ast.table_name.parts,
                                                    fields=fields, database_oid=get_session_value('database_oid'),
                                                    index_type=ast.index_type)
        elif isinstance(ast, CreateTable):
            if isinstance(ast, CreateMemoryTable):
                defined_columns = ast.columns
                if hasattr(ast, 'select_query'):
                    defined_columns = cls.get_column_types(ast.columns, ast.select_query)
                physical_operator = CreateMemoryTableOperator(
                    table_name=ast.name.parts, fields=defined_columns,
                    database_oid=get_session_value('database_oid'),
                    temporary=ast.temporary
                )
            else:
                physical_operator = CreateTableOperator(
                    table_name=ast.name.parts, fields=ast.columns, database_oid=get_session_value('database_oid')
                )
        elif isinstance(ast, DropTable):
            if isinstance(ast, DropMemoryTable):
                physical_operator = DropMemoryTableOperator(
                    table_name=ast.name.parts, database_oid=get_session_value('database_oid')
                )
            else:
                physical_operator = DropTableOperator(
                    table_name=ast.name.parts, database_oid=get_session_value('database_oid')
                )
        elif isinstance(ast, DropIndex):
            physical_operator = DropIndexOperator(
                index_name=ast.name.parts, database_oid=get_session_value('database_oid')
            )
        elif isinstance(ast, Explain):
            physical_operator = ExplainOperator(logical_plan=andb_ast_transform(ast.target))
        elif isinstance(ast, Command):
            physical_operator = CommandOperator(ast.command, ast.parameters)

        return UtilityOperator(physical_operator)


class ConditionTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Condition)

    @staticmethod
    def on_transform(ast: Condition):
        def swap(node: Condition):
            # let column is at left hand side
            if isinstance(node.right, AbstractColumn):
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
            if (not isinstance(left_node, AbstractColumn) and
                    not isinstance(right_node, AbstractColumn)):
                return expression_eval(node.expr.value, node.left, node.right)
            else:
                node.left = left_node
                node.right = right_node
                swap(node)
            return node

        return dfs(ast)


class QueryLogicalPlanTransformation(BaseTransformation):
    @staticmethod
    def match(query) -> bool:
        return isinstance(query, LogicalQuery) and len(query.children) == 0

    @staticmethod
    def process_non_join_scan(query: LogicalQuery):
        assert len(query.scan_operators) == 1  #TODO: support 0 in future
        scan = list(query.scan_operators.values())[0]

        if not query.condition:
            query.add_child(scan)
            return

        # simple predicate pushdown
        scan.condition = query.condition
        scan.table_columns = []
        for table_column in query.get_seen_table_columns(scan.table_name):
            # TODO: because there may be function
            scan.table_columns.append(table_column)
        # column prune:
        # because this is non-join query, it is very simple. if this query has join clause, we have to
        # add join column for the scan operator.
        query.add_child(scan)

    @staticmethod
    def process_join_scan(query: LogicalQuery):
        condition_table_names = {}
        if query.condition and not isinstance(query.condition, SemanticCondition):
            for condition in query.condition.get_iterator():
                if isinstance(condition.left, TableColumn):
                    condition_table_names[condition.left.table_name] = condition
                if isinstance(condition.right, TableColumn):
                    condition_table_names[condition.right.table_name] = condition
        elif query.condition and isinstance(query.condition, SemanticCondition):
            for table_col in query.condition.table_columns:
                if table_col.table_name not in condition_table_names:
                    condition_table_names[table_col.table_name] = query.condition

        scan_operator: "ScanOperator"
        for table_name, scan_operator in query.scan_operators.items():
            # if the query has a condition and this condition only contains one table,
            # push down the predicate (condition).
            if query.condition and len(condition_table_names) == 1 \
                    and table_name in condition_table_names:
                scan_operator.condition = query.condition

            scan_operator.table_columns = []
            for table_column in query.get_seen_table_columns(scan_operator.table_name):
                # TODO: because there may be function
                scan_operator.table_columns.append(table_column)

        # add table columns that come from join conditions
        join_table_columns = []
        for join_operator in query.join_operators:
            if isinstance(join_operator, SemanticJoinOperator):
                join_table_columns.extend(join_operator.condition.table_columns)
            elif not join_operator.join_condition:
                # skip cross join
                continue
            else:
                for condition in join_operator.join_condition.get_iterator():
                    if isinstance(condition.left, TableColumn):
                        join_table_columns.append(condition.left)
                    if isinstance(condition.right, TableColumn):
                        join_table_columns.append(condition.right)
                # TODO: can be further pruned
                # join_operator.table_columns = None

        for join_table_column in join_table_columns:
            scan_operator = query.scan_operators[join_table_column.table_name]
            if join_table_column not in scan_operator.table_columns:
                scan_operator.table_columns.append(join_table_column)

        if len(set(condition_table_names.values())) > 1 or len(query.join_operators) > 1:
            raise NotImplementedError('not supported multiple tables join')

        query.add_child(query.join_operators[0])

    @staticmethod
    def process_groupby(query: LogicalQuery):
        aggregation_functions = []
        for target in query.target_list:
            if isinstance(target, FunctionColumn) and AggregationFunctions.get(target.function_name):
                aggregation_functions.append(target)
        if len(aggregation_functions) == 0:
            return

        groupby_columns = set(query.groupby_columns)

        for func in aggregation_functions:
            groupby_columns.add(func)
        if groupby_columns != set(query.target_list):
            raise InitializationStageError('not found all columns are in the group by list.')
        operator = GroupOperator(group_by_columns=query.groupby_columns,
                                 aggregate_functions=aggregation_functions,
                                 having_clause=query.having_clause)
        # set the group by operator onto all nodes.
        operator.children = query.children
        query.children = [operator]

    @staticmethod
    def process_semantic_transform(query: LogicalQuery):
        # Check if there are any prompt columns in target list
        sem_trfm_columns = [col for col in query.target_list if isinstance(col, SemanticTransformColumn)]
        prompt_columns = [col for col in query.target_list if isinstance(col, PromptColumn)]
        sem_trfm_columns.extend(prompt_columns)
        
        if not sem_trfm_columns:
            return

        # Create semantic transform operator
        semantic_op = SemanticTransformOperator(
            columns=query.target_list,
            children=query.children
        )

        # Replace query's children with semantic operator
        query.children = [semantic_op]

    @staticmethod
    def on_transform(query: LogicalQuery):
        # TODO: extract all involved columns, then prune useless columns
        # TODO: rewrite

        if not query.join_operators:
            QueryLogicalPlanTransformation.process_non_join_scan(query)
        else:
            QueryLogicalPlanTransformation.process_join_scan(query)

        # Add semantic transform processing
        QueryLogicalPlanTransformation.process_semantic_transform(query)

        # TODO: limit, ...
        if query.sort_clause:
            query.sort_clause.children = query.children
            query.children = [query.sort_clause]

        QueryLogicalPlanTransformation.process_groupby(query)

        return query


class SelectTransformation(BaseTransformation):
    class TempAttrForm:
        __fields__ = {'name':'text', 'type_oid': 'bigint'}

        def __init__(self, name, type_oid):
            self.name = name
            self.type_oid = type_oid

    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Select)

    @staticmethod
    def _supplement_table_name_for_column(column: TableColumn, table_attr_forms):
        if column.table_name is not None:
            found = False
            for attr_form in table_attr_forms[column.table_name]:
                if attr_form.name == column.column_name:
                    found = True
                    break
            if not found:
                raise InitializationStageError(f"not found '{str(column)}'.")
        else:
            column_table_name = None
            for table_name in table_attr_forms:
                for attr_form in table_attr_forms[table_name]:
                    if attr_form.name == column.column_name:
                        if column_table_name is not None:
                            raise InitializationStageError(
                                f'both table {column_table_name} and {table_name} have'
                                f' the same column {column.column_name}.')
                        column_table_name = table_name
                        column.type = CATALOG_ANDB_TYPE.get_type_name(attr_form.type_oid)
            if column_table_name is None:
                raise InitializationStageError(f"not found '{column.column_name}'.")
            column.table_name = column_table_name

    @staticmethod
    def _supplement_table_name(condition: Condition, table_attr_forms):
        for node in condition.get_iterator():
            for arg in (node.left, node.right):
                if isinstance(arg, TableColumn):
                    SelectTransformation._supplement_table_name_for_column(arg, table_attr_forms)
                elif isinstance(arg, FunctionColumn):
                    for column in arg.columns:
                        if isinstance(column, TableColumn):
                            SelectTransformation._supplement_table_name_for_column(column, table_attr_forms)
        return condition

    @classmethod
    def _find_table_name(cls, query, lookup_column_name):
        target_table_name = None
        for table_name in query.from_tables:
            for attr_form in query.table_attr_forms[table_name]:
                if attr_form.name == lookup_column_name:
                    if target_table_name is not None:
                        raise InitializationStageError(f'both table {target_table_name} and {table_name} have'
                                                       f' the same column {lookup_column_name}.')
                    target_table_name = table_name
        return target_table_name

    @classmethod
    def _get_semantic_condition(cls, sem_match, query):
        # Get the table columns by converting identifiers into TableColumn
        table_columns = []
        for target in sem_match.identifiers:
            column = TableColumn(target.items[0], target.items[1])
            cls._supplement_table_name_for_column(column, query.table_attr_forms)
            table_columns.append(column)
            
        return SemanticCondition(sem_match.condition, sem_match.threshold, table_columns)

    @classmethod
    def transform_from_clause(cls, ast_outer, query):
        def process_identifier_table(table_name, add_op=False):
            table_oid = CATALOG_ANDB_CLASS.get_relation_oid(table_name, get_session_value('database_oid'),
                                                                    kind=None)
            if table_oid == INVALID_OID:
                raise InitializationStageError(f'not found the table {table_name}.')

            query.from_tables[table_name] = table_oid
            query.table_attr_forms[table_name] = CATALOG_ANDB_ATTRIBUTE.get_table_forms(table_oid)
            if add_op:
                query.scan_operators[table_name] = ScanOperator(table_name, table_oid=table_oid)

        def process_file_table(file_path):
            database_oid = get_session_value('database_oid')
            real_file_path = os.path.join(os.path.realpath(f'./base/{database_oid}/files'), file_path)
            if os.path.exists(real_file_path) == False:
                raise InitializationStageError(f'not found the file {file_path} from files directory.')
            
            query.from_tables[file_path] = OID_SCANNING_FILE
            query.table_attr_forms[file_path] = CATALOG_ANDB_ATTRIBUTE.get_table_forms(OID_SCANNING_FILE)                
            return ScanOperator(file_path, table_oid=OID_SCANNING_FILE) 

        def process_dir_table(dir_path):
            database_oid = get_session_value('database_oid')
            real_dir_path = os.path.join(os.path.realpath(f'./base/{database_oid}/files'), dir_path)
            files = [f for f in os.listdir(real_dir_path) if os.path.isfile(os.path.join(real_dir_path, f))]
            if os.path.exists(real_dir_path) == False or os.path.isdir(real_dir_path) == False or len(files) == 0:
                raise InitializationStageError(f'not found the directory {dir_path} from files directory or directory is empty.')
            
            query.from_tables[dir_path] = OID_SCANNING_DIRECTORY
            query.table_attr_forms[dir_path] = CATALOG_ANDB_ATTRIBUTE.get_table_forms(OID_SCANNING_DIRECTORY)
            return ScanOperator(dir_path, table_oid=OID_SCANNING_DIRECTORY) 

        def inner_transform(ast):
            if isinstance(ast, Identifier):
                process_identifier_table(ast.parts, True)
            elif isinstance(ast, Join):
                inner_transform(ast.left)
                inner_transform(ast.right)
            elif isinstance(ast, FileSource):
                query.scan_operators[ast.parts] = process_file_table(ast.parts)
            elif isinstance(ast, DirectorySource):
                query.scan_operators[ast.parts] = process_dir_table(ast.parts)
            elif isinstance(ast, SemanticTabular):
                # Process the table source within the SemanticTabular
                if isinstance(ast.table_source, FileSource):
                    table_source_scan = process_file_table(ast.table_source.parts)
                elif isinstance(ast.table_source, DirectorySource):
                    table_source_scan = process_dir_table(ast.table_source.parts)
                else:
                    raise InitializationStageError(f"Source not file is not supported currently...")
                process_identifier_table(ast.identifier.parts)

                semantic_schemas = ast.semantic_schemas
                table_name = ast.identifier.parts
                prompt_columns = []
                for prompt_col, attr_form in zip(semantic_schemas, query.table_attr_forms[table_name]):
                    if not isinstance(prompt_col, Prompt):
                        raise InitializationStageError(f"Invalid schema definition: {prompt_col}")

                    column = PromptColumn(
                        column_name=attr_form.name,
                        prompt_text=prompt_col.prompt_text,
                        table_name=table_name,
                    )
                    prompt_columns.append(column)

                semantic_op = SemanticScanOperator(
                    table_name=table_name,
                    prompt_columns=prompt_columns,
                    condition=None,
                    children=[table_source_scan]
                )
                query.scan_operators[table_name] = semantic_op
            else:
                raise NotImplementedError()

        inner_transform(ast_outer)
        
    @classmethod
    def transform_target_list(cls, ast, query):
        for target in ast:
            # parse star
            if isinstance(target, Star):
                for table_name in query.from_tables:
                    if query.from_tables[table_name] != OID_SCANNING_FILE:
                        # Skip file's attributes, which is content and embedding
                        for attr_form in query.table_attr_forms[table_name]:
                            column = TableColumn(table_name, attr_form.name, CATALOG_ANDB_TYPE.get_type_name(attr_form.type_oid))
                            query.target_list.append(column)
                            query.add_seen_table_column(column)
            elif isinstance(target, Identifier):
                column = TableColumn(target.items[0], target.items[1])
                cls._supplement_table_name_for_column(column, query.table_attr_forms)
                if target.alias:
                    column.alias = target.alias.parts
                query.target_list.append(column)
                query.add_seen_table_column(column)
            elif isinstance(target, Function):
                # TODO: multiple parameters
                table_columns = []
                for id_ in target.args:
                    table_column = TableColumn(id_.items[0], id_.items[1])
                    cls._supplement_table_name_for_column(table_column, query.table_attr_forms)
                    table_column.function_name = target.op
                    table_columns.append(table_column)
                    query.add_seen_table_column(table_column)
                function_column = FunctionColumn(target.op, table_columns, table_columns[0].type)
                if target.alias:
                    function_column.alias = target.alias.parts
                query.target_list.append(function_column)
            elif isinstance(target, SemanticGroup):
                # Find the column that this GroupBy is referring to, and then add as GroupByColumn
                column_alias = target.alias.parts
                table_column = TableColumn(target.identifier.items[0], target.identifier.items[1])
                cls._supplement_table_name_for_column(table_column, query.table_attr_forms)            
                if not isinstance(target.prompt, Prompt):
                    raise InitializationStageError(f"SemanticGroupBy is not provided with prompt.")
                semantic_column = SemanticTransformColumn(table_column.table_name, [table_column.column_name], column_alias,
                                                            target.prompt.prompt_text, target.k, table_column.type)
                query.add_seen_table_column(table_column)
                query.target_list.append(semantic_column)
            elif isinstance(target, Prompt):
                if target.defined_column:
                    prompt_column = PromptColumn(column_name=target.defined_column[0],
                                                prompt_text=target.prompt_text)
                else:
                    prompt_column = PromptColumn(prompt_text=target.prompt_text)
                query.target_list.append(prompt_column)
            elif isinstance(target, ExtractExpression):
                # S²QL EXTRACT(source INTO (col1 TYPE1, ...))
                source_name = target.source_column.parts if hasattr(target.source_column, 'parts') else str(target.source_column)
                for col_name, col_type in target.target_schema:
                    prompt_text = f"Extract '{col_name}' from the text"
                    table_name = cls._find_table_name(query, source_name) or list(query.from_tables.keys())[0]
                    prompt_column = PromptColumn(
                        column_name=col_name,
                        prompt_text=prompt_text,
                        table_name=table_name,
                        type=col_type
                    )
                    query.target_list.append(prompt_column)
            elif isinstance(target, TransformExpression):
                # S²QL TRANSFORM(input AS output USING 'instruction')
                input_name = target.input_column.parts if hasattr(target.input_column, 'parts') else str(target.input_column)
                table_name = cls._find_table_name(query, input_name) or list(query.from_tables.keys())[0]
                table_column = TableColumn(table_name, input_name)
                cls._supplement_table_name_for_column(table_column, query.table_attr_forms)
                query.add_seen_table_column(table_column)
                sem_col = SemanticTransformColumn(
                    table_name=table_column.table_name,
                    original_columns=[table_column.column_name],
                    target_column=target.output_name,
                    prompt_text=target.instruction,
                    k=1,
                    type=table_column.type
                )
                query.target_list.append(sem_col)
            else:
                # TODO: function and agg
                raise NotImplementedError('not supported this syntax.')

    @classmethod
    def transform_where_clause(cls, ast, query):
        if ast is not None:
            if isinstance(ast, SemanticMatch):
                query.condition = cls._get_semantic_condition(ast, query)
            elif isinstance(ast, MatchesPredicate):
                # S²QL MATCHES predicate -> SemanticCondition
                column = TableColumn(ast.column.items[0] if hasattr(ast.column, 'items') else None,
                                     ast.column.parts if hasattr(ast.column, 'parts') else str(ast.column))
                cls._supplement_table_name_for_column(column, query.table_attr_forms)
                threshold = ast.with_params.get('threshold', None) if ast.with_params else None
                condition_str = "'{0}' matches '" + ast.assertion + "'"
                query.condition = SemanticCondition(condition_str, threshold, [column])
            else:
                where_condition = ConditionTransformation.on_transform(Condition(ast))
                if isinstance(where_condition, bool):
                    if where_condition:
                        query.condition = None  # we don't need condition
                    else:
                        raise NotImplementedError('should return empty set directly.')
                elif isinstance(where_condition, Condition):
                    # supplement missing table name and check existing table name
                    query.condition = cls._supplement_table_name(where_condition, query.table_attr_forms)
                else:
                    raise NotImplementedError('not supported this syntax.')

    @classmethod
    def transform_join_clause(cls, ast, query):
        if isinstance(ast, Join):
            # maybe it is a multi-way join
            if isinstance(ast.left, Join):
                SelectTransformation.transform_join_clause(ast.left, query)
            # right should always be a table for now
            if isinstance(ast.right, Join):
                SelectTransformation.transform_join_clause(ast.right, query)

            join_clause = ast

            # Get left and right table names
            if isinstance(join_clause.left, SemanticTabular):
                left_table_name = join_clause.left.identifier.parts
            else:
                left_table_name = join_clause.left.parts
            if isinstance(join_clause.right, SemanticTabular):
                right_table_name = join_clause.right.identifier.parts
            else:
                right_table_name = join_clause.right.parts

            if not join_clause.implicit:
                if isinstance(join_clause.condition, SemanticMatch):
                    semantic_condition = cls._get_semantic_condition(join_clause.condition, query)
                    join_operator = SemanticJoinOperator(condition=semantic_condition,
                                                         children_table_names=[left_table_name, right_table_name],
                                                         join_type=join_clause.join_type)
                else:
                    join_condition = ConditionTransformation.on_transform(Condition(join_clause.condition))
                    join_condition = cls._supplement_table_name(join_condition, query.table_attr_forms)
                    join_operator = JoinOperator(join_condition=join_condition,
                                                 join_type=join_clause.join_type)
            else:
                join_operator = JoinOperator(join_condition=None,
                                             join_type=join_clause.join_type)

            # for self-joining, the left and right table reuse a same scan operator
            left_scan_operator = query.scan_operators[left_table_name]
            right_scan_operator = query.scan_operators[right_table_name]

            join_operator.add_child(left_scan_operator)
            join_operator.add_child(right_scan_operator)

            query.join_operators.append(join_operator)

    @classmethod
    def transform_order_clause(cls, ast, query):
        if ast:
            sort_columns = []
            ascending_orders = []
            for node in ast:
                table_column = TableColumn(node.attr.items[0], node.attr.items[1])
                cls._supplement_table_name_for_column(table_column, query.table_attr_forms)
                sort_columns.append(table_column)
                ascending_orders.append(node.direction == 'ASC')

            query.sort_clause = SortOperator(sort_columns, ascending_orders)

    @classmethod
    def transform_group_clause(cls, ast, query):
        if ast.group_by:
            for id_ in ast.group_by:
                # S²QL CLASSIFYING group by
                if isinstance(id_, ClassifyingGroupBy):
                    source_name = id_.source_column.parts if hasattr(id_.source_column, 'parts') else str(id_.source_column)
                    table_name = cls._find_table_name(query, source_name)
                    if table_name is None:
                        table_name = list(query.from_tables.keys())[0]
                    table_column = TableColumn(table_name, source_name)
                    cls._supplement_table_name_for_column(table_column, query.table_attr_forms)
                    query.add_seen_table_column(table_column)
                    sem_col = SemanticTransformColumn(
                        table_name=table_column.table_name,
                        original_columns=[table_column.column_name],
                        target_column=id_.key_name,
                        prompt_text=f"Classify into categories",
                        k=5,
                        type=table_column.type
                    )
                    query.groupby_columns.append(sem_col)
                    # Also add to target list if not already there
                    if not any(isinstance(t, SemanticTransformColumn) and t.column_name == id_.key_name
                               for t in query.target_list):
                        query.target_list.insert(0, sem_col)
                    continue
                if '.' in id_.parts:
                    items = id_.parts.split('.')
                    if len(items) != 2:
                        raise InitializationStageError(f"syntax error: '{id_.parts}'.")
                    table_name, column_name = items
                    if table_name not in query.from_tables:
                        raise InitializationStageError(f"not found '{id_.parts}'.")
                    table_column = TableColumn(id_.items[0], id_.items[1])
                    query.groupby_columns.append(table_column)
                    query.add_seen_table_column(table_column)
                else:
                    column_name = id_.parts
                    table_name = cls._find_table_name(query, column_name)
                    found = False
                    if table_name is None:
                        for target in query.target_list:
                            if isinstance(target, SemanticTransformColumn) and column_name == target.column_name:
                                query.groupby_columns.append(target)
                                found = True
                                break
                            
                        if not found:
                            raise InitializationStageError(f"not found '{id_.parts}'.")    
                    else:
                        table_column = TableColumn(table_name, column_name)
                        query.groupby_columns.append(table_column)
                        query.add_seen_table_column(table_column)

        if ast.having:
            if not ast.group_by:
                raise NotImplementedError('only support having clause for group by clause.')
            having_clause = Condition(ast.having)
            cls._supplement_table_name(having_clause, query.table_attr_forms)
            query.having_clause = ConditionTransformation.on_transform(having_clause)

    @classmethod
    def on_transform(cls, ast: Select):
        query = LogicalQuery()

        cls.transform_from_clause(ast.from_table, query)
        cls.transform_target_list(ast.targets, query)
        cls.transform_where_clause(ast.where, query)
        cls.transform_join_clause(ast.from_table, query)
        cls.transform_order_clause(ast.order_by, query)
        cls.transform_group_clause(ast, query)  # we need to pass both ast.group_by and ast.having

        if len(query.unchecked_columns) > 0:
            raise InitializationStageError(f"not found '{query.unchecked_columns[0]}'.")

        query.distinct = ast.distinct
        if ast.limit:
            query.limit = ast.limit.value

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
                                                        database_oid=get_session_value('database_oid'))
        if table_oid == INVALID_OID:
            raise InitializationStageError(f'cannot get oid for the table {ast.table.parts}.')

        attr_forms = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == table_oid)
        if not attr_forms:
            raise InitializationStageError(f'cannot get the table {ast.table.parts}.')

        attr_forms = dict((form.num, form) for form in attr_forms)

        if ast.columns:
            val_len = len(ast.columns)
            column_idxs = [None] * len(ast.columns)
            visited = [False] * len(attr_forms)
            for i in range(len(ast.columns)):
                col_name = ast.columns[i].parts
                for form in attr_forms.values():
                    if form.name == col_name:
                        column_idxs[i] = form.num
                        visited[form.num] = True
                        break
                if column_idxs[i] is None:
                    raise InitializationStageError(f'Column {col_name} does not exist in {ast.table.parts}.')
            for i in range(len(column_idxs)):
                if not visited[i] and attr_forms[i].notnull:
                    raise InitializationStageError(f'{attr_forms[i]} should not be null.')
        else:
            val_len = len(attr_forms)
            column_idxs = list(range(len(attr_forms)))

        if isinstance(ast.from_values, Select):
            select_query = SelectTransformation.on_transform(ast.from_values)
            if len(select_query.target_list) != val_len:
                raise InitializationStageError(
                    f'Select returns {len(select_query.target_list)} columns while {val_len} should be given.')
            for i, target_col in enumerate(select_query.target_list):
                table_form = attr_forms[column_idxs[i]]
                if target_col.type is None:
                    target_col.type = "text"
                if target_col.type != CATALOG_ANDB_TYPE.get_type_name(table_form.type_oid):
                    raise InitializationStageError(f'Incompatible types for {target_col.type} and {str(table_form.name)}')
            operator_values = select_query
            operator_select = True
        else:
            rows = []
            for value in ast.from_values:
                row = [None for _ in range(len(attr_forms))]
                if isinstance(value, Constant):
                    row[0] = CATALOG_ANDB_TYPE.get_type_form_by_oid(
                        attr_forms[0].type_oid).format_value(value.value)
                elif isinstance(value, list) or isinstance(value, tuple):
                    for i, v in enumerate(value):
                        target_form = attr_forms[column_idxs[i]]
                        if target_form.enum_vals is not None:
                            if v.value not in target_form.enum_vals:
                                raise InitializationStageError(
                                    f'Invalid enum value {v.value} for column {target_form.name}.')
                        row[column_idxs[i]] = CATALOG_ANDB_TYPE.get_type_form_by_oid(target_form.type_oid)
                        row[column_idxs[i]] = row[column_idxs[i]].format_value(v.value)
                else:
                    raise NotImplementedError('not supported this syntax yet.')
                rows.append(row)
            operator_values = rows
            operator_select = False

        operator = InsertOperator(
            table_name=ast.table.parts, table_oid=table_oid, columns=column_idxs,
            values=operator_values, is_select=operator_select
        )

        return operator


class DeleteTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Delete)

    @staticmethod
    def on_transform(ast: Delete):
        # transform to a query
        # TODO: extract where predicates
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
        table_oid = CATALOG_ANDB_CLASS.get_relation_oid(relation_name=ast.table.parts,
                                                        database_oid=get_session_value('database_oid'))
        if table_oid == INVALID_OID:
            raise InitializationStageError(f'cannot get oid for the table {ast.table.parts}.')

        columns = []
        values = []
        for column_name, value_expr in ast.columns.items():
            columns.append(TableColumn(table_name=ast.table.parts,
                                       column_name=column_name))
            # format the value to avoid type mismatch
            attr_form = CATALOG_ANDB_ATTRIBUTE.get_table_attr(table_oid, column_name)
            type_form = CATALOG_ANDB_TYPE.get_type_form_by_oid(attr_form.type_oid)
            if isinstance(value_expr, Constant):
                values.append(type_form.format_value(value_expr.value))
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


class SemanticQueryTransformation(BaseTransformation):
    pass


_all_transformations = [trans() for trans in BaseTransformation.__subclasses__()]


def andb_ast_transform(ast):
    for trans in _all_transformations:
        if trans.match(ast):
            return trans.on_transform(ast)
    raise AnDBNotImplementedError('not supported this grammar yet')

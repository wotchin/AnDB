from andb.executor.operator.logical import *
from andb.executor.operator.physical.utility import CreateIndexOperator, CreateTableOperator
from andb.runtime import session_vars
from andb.sql.parser.ast.create import CreateTable, CreateIndex
from andb.sql.parser.ast.insert import Insert
from andb.sql.parser.ast.delete import Delete
from andb.sql.parser.ast.update import Update
from andb.sql.parser.ast.select import Select
from andb.sql.parser.ast.identifier import Identifier
from andb.errno.errors import AnDBNotImplementedError
from .base import BaseTransformation


# from .patterns import *


class CreateIndexTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, CreateIndex)

    @staticmethod
    def on_transform(ast: CreateIndex):
        fields = [id_.parts for id_ in ast.columns]
        return UtilityOperator(
            CreateIndexOperator(index_name=ast.name.parts, table_name=ast.table_name.parts,
                                fields=fields, database_oid=session_vars.database_oid,
                                index_type=ast.index_type)
        )


class CreateTableTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, CreateTable)

    @staticmethod
    def on_transform(ast: CreateTable):
        return UtilityOperator(
            CreateTableOperator(
                table_name=ast.name.parts, fields=ast.columns, database_oid=session_vars.database_oid
            )
        )


class ConditionTransformation(BaseTransformation):
    @staticmethod
    def eval_(op, left, right):
        if op == '+':
            return left + right
        elif op == '-':
            return left - right
        elif op == '*':
            return left * right
        elif op == '/':
            return left / right
        elif op == 'and':
            if left is None or right is None:
                return None  # null
            else:
                # todo: string type 'true' and 'false'
                return left and right
        elif op == 'or':
            if left is None or right is None:
                return None  # null
            else:
                # todo: string type 'true' and 'false'
                return left or right
        else:
            raise NotImplementedError()

    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Condition)

    @staticmethod
    def on_transform(ast: Condition):
        def dfs(node: Condition):
            if node is None:
                return
            left_node = dfs(node.left)
            right_node = dfs(node.right)

            # only convert constant
            if (not isinstance(left_node, TableColumn) and
                    not isinstance(right_node, TableColumn)):
                return ConditionTransformation.eval_(node.expr.value, node.left, node.right)
            else:
                node.left = left_node
                node.right = right_node
            return node

        return dfs(ast)


class SelectTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Select)

    @staticmethod
    def on_transform(ast: Select):
        raise NotImplementedError()


class InsertTransformation(BaseTransformation):
    @staticmethod
    def match(ast) -> bool:
        return isinstance(ast, Insert)

    @staticmethod
    def on_transform(ast: Insert):
        if ast.columns:
            columns = [TableColumn(table_name=ast.table.parts, column_name=id_.parts) for id_ in ast.columns]
        else:
            # todo: comes from catalog!
            columns = []
            pass

        values = []
        for cell in columns:
            if isinstance(cell, Identifier):
                values.append((cell.parts,))
            elif isinstance(cell, list):
                values.append([id_.parts for id_ in cell])
            else:
                raise

        operator = InsertOperator(
            table_name=ast.table.parts, columns=columns, values=values
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
        condition = ConditionTransformation.on_transform(ast.where)
        return DeleteOperator(table_name=ast.table.parts, condition=condition)


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
                raise NotImplementedError()
        condition = ConditionTransformation.on_transform(ast.where)
        operator = UpdateOperator(table_name=ast.table.parts, columns=columns,
                                  values=values, condition=condition)
        return operator


_all_transformations = [trans() for trans in BaseTransformation.__subclasses__()]


def andb_ast_transform(ast):
    for trans in _all_transformations:
        if trans.match(ast):
            return trans.on_transform(ast)
    raise AnDBNotImplementedError('not supported this grammar yet')

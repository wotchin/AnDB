import sly

from .lexer import SQLLexer
from .ast.base import ASTNode
from .ast.explain import Explain
from .ast.alter import AlterTable
from .ast.create import CreateIndex, CreateTable
from .ast.union import Union
from .ast.select import Select
from .ast.insert import Insert
from .ast.update import Update
from .ast.delete import Delete
from .ast.order_by import OrderBy
from .ast.join import Join, JoinType
from .ast.operation import Operation, Function, BinaryOperation
from .ast.identifier import Identifier
from .ast.misc import Constant, Star, Tuple
from .exception import ParsingException


def check_select_keywords(select, operation):
    operations = (
        ('FROM', select.from_table),
        ('WHERE', select.where),
        ('GROUP BY', select.group_by),
        ('HAVING', select.having),
        ('ORDER BY', select.order_by),
        ('LIMIT', select.limit),
        ('OFFSET', select.offset),
    )

    def get_attribute(name):
        for k, v in operations:
            if k == name:
                return v

    if get_attribute(operation):
        raise ParsingException(f'{operation} already specified.')

    # check the ordering of each keyword
    seen = False
    for op, attr in operations:
        if seen and attr:
            raise ParsingException(
                f'{operation} should be set before {op}.'
            )

        if op == operation:
            seen = True

    requirements = {
        'WHERE': ['FROM'],
        'GROUP BY': ['FROM'],
        'ORDER BY': ['FROM'],
    }

    for op_requirement in requirements.get(operation, []):
        if not get_attribute(op_requirement):
            raise ParsingException(f'{operation} requirements {op_requirement}.')


class SQLParser(sly.Parser):
    tokens = SQLLexer.tokens

    precedence = (
        ('left', OR),
        ('left', AND),
        ('left', EQ, NE),
        ('left', PLUS, MINUS),
        ('left', STAR, DIVIDE),
        ('nonassoc', LT, LEQ, GT, GEQ, IN, BETWEEN, IS, IS_NOT, LIKE),
    )

    # top-level statements
    @_(
        'explain',
        'alter_table',
        'create',
        'union',
        'select',
        'delete',
        'insert',
        'update'
    )
    def query(self, p):
        return p[0]

    # explain
    @_('EXPLAIN select')
    def explain(self, p):
        return Explain(target=p.select)

    # alter table
    @_('ALTER TABLE identifier id id')
    def alter_table(self, p):
        return AlterTable(target=p.identifier,
                          arg=f'{p.id0} {p.id1}')

    # union
    @_('select UNION select')
    def union(self, p):
        return Union(left=p.select0, right=p.select1,
                     unique=True)

    @_('select UNION ALL select')
    def union(self, p):
        return Union(left=p.select0, right=p.select1,
                     unique=False)

    # select
    @_('select OFFSET constant')
    def select(self, p):
        select = p.select
        check_select_keywords(select, 'OFFSET')
        if not isinstance(p.constant.value, int):
            raise ParsingException(
                f'OFFSET value must be an integer type not "{p.constant.value}"'
            )

        select.offset = p.constant
        return select

    @_('select LIMIT constant')
    def select(self, p):
        select = p.select
        check_select_keywords(select, 'LIMIT')
        if not isinstance(p.constant.value, int):
            raise ParsingException(
                f'LIMIT value must be an integer type not "{p.constant.value}"'
            )
        select.limit = p.constant
        return select

    @_('select LIMIT constant COMMA constant')
    def select(self, p):
        select = p.select
        check_select_keywords(select, 'LIMIT')
        if not isinstance(p.constant0.value, int) or not isinstance(p.constant1.value, int):
            raise ParsingException(
                f'LIMIT value must be an integer type not "{p.constant0.value}, {p.constant1.value}"'
            )
        select.offset = p.constant0
        select.limit = p.constant1
        return select

    @_('select ORDER_BY ordering_terms')
    def select(self, p):
        select = p.select
        check_select_keywords(select, 'ORDER BY')
        select.order_by = p.ordering_terms
        return select

    @_('ordering_terms COMMA ordering_term')
    def ordering_terms(self, p):
        terms = p.ordering_terms
        terms.append(p.ordering_term)
        return terms

    @_('ordering_term')
    def ordering_terms(self, p):
        return [p.ordering_term]

    @_('identifier DESC')
    def ordering_term(self, p):
        return OrderBy(column=p.identifier, direction='DESC')

    @_('identifier',
       'identifier ASC')
    def ordering_term(self, p):
        return OrderBy(column=p.identifier, direction='ASC')

    @_('select GROUP_BY expr_list')
    def select(self, p):
        select = p.select
        check_select_keywords(select, 'GROUP BY')
        group_by = p.expr_list
        if not isinstance(group_by, list):
            group_by = [group_by]

        select.group_by = group_by
        return select

    @_('select HAVING expr')
    def select(self, p):
        select = p.select
        check_select_keywords(select, 'HAVING')
        having = p.expr
        if not isinstance(having, Operation):
            raise ParsingException(
                f"Require an operation for HAVING clause.")
        select.having = having
        return select

    @_('select WHERE expr')
    def select(self, p):
        select = p.select
        check_select_keywords(select, 'WHERE')
        where_expr = p.expr
        if not isinstance(where_expr, Operation):
            raise ParsingException(
                f"Require an operation for WHERE clause.")
        select.where = where_expr
        return select

    @_('select FROM from_table_aliased',
       'select FROM join_tables_implicit',
       'select FROM join_tables')
    def select(self, p):
        select = p.select
        check_select_keywords(select, 'FROM')
        select.from_table = p[2]
        return select

    # TODO: subquery, CTE
    # join
    @_('from_table_aliased join_clause from_table_aliased',
       'join_tables join_clause from_table_aliased')
    def join_tables(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=p.join_clause)

    @_('from_table_aliased join_clause from_table_aliased ON expr',
       'join_tables join_clause from_table_aliased ON expr')
    def join_tables(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=p.join_clause,
                    condition=p.expr)

    @_('from_table_aliased COMMA from_table_aliased',
       'join_tables_implicit COMMA from_table_aliased')
    def join_tables_implicit(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=JoinType.CROSS_JOIN,
                    implicit=True)

    @_('from_table AS identifier',
       'from_table identifier',
       'from_table')
    def from_table_aliased(self, p):
        entity = p.from_table
        if hasattr(p, 'identifier'):
            entity.alias = p.identifier
        return entity

    @_('LPAREN query RPAREN')
    def from_table(self, p):
        query = p.query
        query.parentheses = True
        return query

    @_('identifier')
    def from_table(self, p):
        return p.identifier

    @_('JOIN',
       'LEFT JOIN',
       'RIGHT JOIN',
       'INNER JOIN',
       'FULL JOIN',
       'CROSS JOIN',
       'OUTER JOIN',
       )
    def join_clause(self, p):
        return ' '.join([x for x in p])

    @_('SELECT DISTINCT result_columns')
    def select(self, p):
        targets = p.result_columns
        return Select(targets=targets, distinct=True)

    @_('SELECT result_columns')
    def select(self, p):
        targets = p.result_columns
        return Select(targets=targets)

    @_('result_columns COMMA result_column')
    def result_columns(self, p):
        p.result_columns.append(p.result_column)
        return p.result_columns

    @_('result_column')
    def result_columns(self, p):
        return [p.result_column]

    @_('result_column AS identifier',
       'result_column identifier')
    def result_column(self, p):
        col = p.result_column
        if col.alias:
            raise ParsingException(f'Should not provide two aliases for {str(col)}')
        col.alias = p.identifier
        return col

    @_('LPAREN select RPAREN')
    def result_column(self, p):
        select = p.select
        select.parentheses = True
        return select

    @_('star')
    def result_column(self, p):
        return p.star

    @_('expr',
       'function', )
    def result_column(self, p):
        return p[0]

    # OPERATIONS

    @_('LPAREN select RPAREN')
    def expr(self, p):
        select = p.select
        select.parentheses = True
        return select

    @_('LPAREN expr RPAREN')
    def expr(self, p):
        if isinstance(p.expr, ASTNode):
            p.expr.parentheses = True
        return p.expr

    @_('id LPAREN expr_list_or_nothing RPAREN')
    def function(self, p):
        args = p.expr_list_or_nothing
        if not args:
            args = []
        return Function(op=p.id, args=args)

    @_('expr_list')
    def expr_list_or_nothing(self, p):
        return p.expr_list

    @_('empty')
    def expr_list_or_nothing(self, p):
        pass

    @_('enumeration')
    def expr_list(self, p):
        return p.enumeration

    @_('expr')
    def expr_list(self, p):
        return [p.expr]

    @_('LPAREN enumeration RPAREN')
    def expr(self, p):
        tup = Tuple(items=p.enumeration)
        return tup

    @_('STAR')
    def star(self, p):
        return Star()

    @_('expr NOT IN expr')
    def expr(self, p):
        op = p[1] + ' ' + p[2]
        return BinaryOperation(op=op, args=(p.expr0, p.expr1))

    @_('expr PLUS expr',
       'expr MINUS expr',
       'expr STAR expr',
       'expr DIVIDE expr',
       'expr MODULO expr',
       'expr EQ expr',
       'expr NE expr',
       'expr GEQ expr',
       'expr GT expr',
       'expr LEQ expr',
       'expr LT expr',
       'expr AND expr',
       'expr OR expr',
       'expr IS_NOT expr',
       'expr NOT expr',
       'expr IS expr',
       'expr LIKE expr',
       'expr CONCAT expr',
       'expr IN expr')
    def expr(self, p):
        return BinaryOperation(op=p[1], args=(p.expr0, p.expr1))

    # update fields list
    @_('update_parameter',
       'update_parameter_list COMMA update_parameter')
    def update_parameter_list(self, p):
        params = getattr(p, 'update_parameter_list', {})
        params.update(p.update_parameter)
        return params

    @_('id EQ expr')
    def update_parameter(self, p):
        return {p.id: p.expr}

    # EXPRESSIONS

    @_('enumeration COMMA expr')
    def enumeration(self, p):
        return p.enumeration + [p.expr]

    @_('expr COMMA expr')
    def enumeration(self, p):
        return [p.expr0, p.expr1]

    @_('identifier')
    def expr(self, p):
        return p.identifier

    @_('constant')
    def expr(self, p):
        return p.constant

    @_('NULL')
    def constant(self, p):
        return Constant(value=None)

    @_('TRUE')
    def constant(self, p):
        return Constant(value=True)

    @_('FALSE')
    def constant(self, p):
        return Constant(value=False)

    @_('integer')
    def constant(self, p):
        return Constant(value=int(p.integer))

    @_('float')
    def constant(self, p):
        return Constant(value=float(p.float))

    @_('string')
    def constant(self, p):
        return Constant(value=str(p[0]))

    @_('identifier DOT identifier')
    def identifier(self, p):
        p.identifier0.parts += p.identifier1.parts
        return p.identifier0

    @_('id')
    def identifier(self, p):
        return Identifier(p[0])

    @_('ID')
    def id(self, p):
        return p[0]

    @_('quote_string',
       'dquote_string')
    def string(self, p):
        return p[0]

    @_('FLOAT')
    def float(self, p):
        return float(p[0])

    @_('INTEGER')
    def integer(self, p):
        return int(p[0])

    @_('QUOTE_STRING')
    def quote_string(self, p):
        return p[0].strip('\'')

    @_('DQUOTE_STRING')
    def dquote_string(self, p):
        return p[0].strip('\"')

    @_('')
    def empty(self, p):
        pass

    def error(self, p):
        if p:
            raise ParsingException(f"Syntax error at token {p.type}: \"{p.value}\"")
        else:
            raise ParsingException("Syntax error at EOF")

    # insert
    @_('INSERT INTO from_table LPAREN result_columns RPAREN select',
       'INSERT INTO from_table select')
    def insert(self, p):
        columns = getattr(p, 'result_columns', None)
        return Insert(table=p.from_table, columns=columns, from_select=p.select)

    @_('INSERT INTO from_table LPAREN result_columns RPAREN VALUES expr_list_set',
       'INSERT INTO from_table VALUES expr_list_set')
    def insert(self, p):
        columns = getattr(p, 'result_columns', None)
        return Insert(table=p.from_table, columns=columns, values=p.expr_list_set)

    @_('expr_list_set COMMA expr_list_set')
    def expr_list_set(self, p):
        return p.expr_list_set0 + p.expr_list_set1

    @_('LPAREN expr_list RPAREN')
    def expr_list_set(self, p):
        return [p.expr_list]

    # update
    @_('UPDATE identifier SET update_parameter_list',
       'UPDATE identifier SET update_parameter_list WHERE expr')
    def update(self, p):
        where = getattr(p, 'expr', None)
        return Update(table=p.identifier,
                      columns=p.update_parameter_list,
                      where=where)

    # delete
    @_('DELETE FROM from_table WHERE expr',
       'DELETE FROM from_table')
    def delete(self, p):
        where = getattr(p, 'expr', None)

        if where is not None and not isinstance(where, Operation):
            raise ParsingException(
                f"WHERE clause must contain boolean condition not: {str(where)}")

        return Delete(table=p.from_table, where=where)

    # DDL
    @_('defined_columns COMMA defined_column')
    def defined_columns(self, p):
        p.defined_columns.append(p.defined_column)
        return p.defined_columns

    @_('defined_column')
    def defined_columns(self, p):
        return [p.defined_column]

    @_('id id')
    def defined_column(self, p):
        return [p.id0, p.id1]

    @_('id id NOT NULL')
    def defined_column(self, p):
        return [p.id0, p.id1, True]

    @_('CREATE TABLE identifier LPAREN defined_columns RPAREN')
    def create(self, p):
        return CreateTable(name=p.identifier, columns=p.defined_columns)

    @_('CREATE INDEX identifier ON identifier LPAREN result_columns RPAREN',
       'CREATE INDEX identifier ON identifier LPAREN result_columns RPAREN USING identifier')
    def create(self, p):
        index_type = getattr(p, 'identifier2', None)
        return CreateIndex(
            name=p.identifier0, table_name=p.identifier1,
            columns=p.result_columns, index_type=index_type
        )

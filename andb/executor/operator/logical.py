from enum import Enum

from andb.executor.operator.utils import ExprOperation
from andb.sql.parser.ast.operation import BinaryOperation
from andb.sql.parser.ast.misc import Constant
from andb.sql.parser.ast.identifier import Identifier


class LogicalOperator:
    OPERATOR_NAME = 'name'
    OPERATOR_CHILDREN = 'children'

    def __init__(self, name, children=None):
        self.name = name
        # copy all elements from children
        if children:
            self.children = children.copy()
        else:
            self.children = []

    def add_child(self, child_operator):
        self.children.append(child_operator)

    def get_args(self):
        return ()

    def __call__(self, *args, **kwargs):
        assert len(args) == 1
        assert isinstance(args[0], LogicalOperator)
        self.children.append(args[0])
        # return attached operator
        return args[0]


class DummyTableName:
    TEMP_TABLE_NAME = 'temp_table'
    FUNCTION_PLACEHOLDER = 'function'
    UNKNOWN = 'unknown'


class AggregationFunctions(Enum):
    count = len
    sum = sum
    max = max
    min = min
    avg = (lambda x: sum(x) / len(x))


class TableColumn:
    def __init__(self, table_name, column_name):
        self.table_name = table_name
        self.column_name = column_name
        self.function_name = None
        self.alias = None

    def __repr__(self):
        if not self.function_name:
            return f'{self.table_name}.{self.column_name}'
        return f'{self.function_name}({self.table_name}.{self.column_name})'

    def __eq__(self, other):
        if not isinstance(other, TableColumn):
            return False
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.table_name, self.column_name, self.function_name))

    def core(self):
        return TableColumn(self.table_name, self.column_name)


class FunctionColumn:
    def __init__(self, function_name, table_columns):
        self.function_name = function_name
        self.table_columns = table_columns
        self.alias = function_name  # default

    def __repr__(self):
        table_columns = ', '.join(self.table_columns)
        return f'{self.function_name}({table_columns})'

    def __eq__(self, other):
        if not isinstance(other, FunctionColumn):
            return False
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.function_name, *self.table_columns))


class Condition(LogicalOperator):
    def __init__(self, operation, children=None):
        super().__init__('Expression', children)
        assert isinstance(operation, BinaryOperation)
        self.expr = None
        for o in ExprOperation:
            if o.value == operation.op:
                self.expr = o
                break
        assert self.expr

        self.left = self._convert(operation.args[0])
        self.right = self._convert(operation.args[1])

    @staticmethod
    def _convert(node):
        if isinstance(node, Constant):
            return node.value
        elif isinstance(node, Identifier):
            items = node.parts.split('.')
            if len(items) == 2:
                return TableColumn(table_name=items[0], column_name=items[1])
            elif len(items) == 1:
                return TableColumn(table_name=None, column_name=node.parts)
            else:
                raise
        else:
            raise

    def add_child(self, child_operator):
        assert isinstance(child_operator, Condition)
        super().add_child(child_operator)

    def get_args(self):
        return ('expression', f'({self.left} {self.expr.value} {self.right})'),

    def __repr__(self):
        return f'{str(self.left)} {self.expr.value} {str(self.right)}'

    def get_iterator(self):
        root_node = self
        node_queue = [root_node]
        while len(node_queue) > 0:
            node = node_queue.pop(0)
            for child in node.children:
                if isinstance(child, Condition):
                    node_queue.append(child)
            yield node

    def is_constant_condition(self):
        return isinstance(self.left, TableColumn) and (
                not isinstance(self.right, TableColumn) and
                not isinstance(self.right, Condition)
        )


class LogicalQuery(LogicalOperator):
    def __init__(self, entry=None):
        super().__init__('Query', entry)
        self.table_attr_forms = {}
        self.from_tables = {}

        # todo: currently, we only support two table join.
        self.join_operators = []
        self.groupby_columns = []
        self.having_clause = None
        self.scan_operators = []
        self.sort_clause = None
        self.target_list = []
        self.condition = None
        self.alias = {}
        self.limit = None
        self.distinct = False

        self._seen_table_columns = set()

    def add_seen_table_column(self, table_column):
        self._seen_table_columns.add((table_column.table_name, table_column.column_name))

    def get_seen_table_columns(self, lookup_table_name=None):
        # return table columns in order
        table_columns = []
        for table_name in self.from_tables:
            if lookup_table_name is not None and lookup_table_name != table_name:
                continue
            for attr_from in self.table_attr_forms[table_name]:
                if (table_name, attr_from.name) in self._seen_table_columns:
                    table_columns.append(TableColumn(table_name, attr_from.name))
        return table_columns


class ProjectionOperator(LogicalOperator):
    def __init__(self, columns, children=None):
        super().__init__('Projection', children)
        self.columns = columns

    def get_args(self):
        return ('columns', self.columns),


class SelectionOperator(LogicalOperator):
    def __init__(self, condition: Condition, children=None):
        super().__init__('Selection', children)
        self.condition = condition

    def get_args(self):
        return ('condition', self.condition),


class JoinOperator(LogicalOperator):
    def __init__(self, join_condition: Condition, join_type, table_columns=None, children=None):
        super().__init__('Join', children)
        self.join_condition = join_condition
        self.join_type = join_type
        self.table_columns = table_columns

    def get_args(self):
        return (('join_condition', self.join_condition),
                ('join_type', self.join_type))


class GroupOperator(LogicalOperator):
    def __init__(self, group_by_columns, aggregate_function, having_clause=None, children=None):
        super().__init__('Group', children)
        self.group_by_columns = group_by_columns
        self.aggregate_function = aggregate_function
        self.having_clause = having_clause

    def get_args(self):
        return (
            ('group_by_columns', self.group_by_columns),
            ('aggregate_function', self.aggregate_function)
        )


class AppendOperator(LogicalOperator):
    def __init__(self, scan_children):
        super().__init__('Append', scan_children)


class ScanOperator(LogicalOperator):
    def __init__(self, table_name, table_oid, condition: Condition = None):
        super().__init__('Scan')
        self.table_name = table_name
        self.table_oid = table_oid
        self.condition = condition
        self.table_columns = None

    def get_args(self):
        return (('table_name', self.table_name),
                ('condition', self.condition))


class SortOperator(LogicalOperator):
    def __init__(self, sort_columns, ascending_orders=None, children=None):
        super().__init__('Sort', children)
        self.sort_columns = sort_columns
        self.ascending_orders = ascending_orders
        if ascending_orders is None:
            self.ascending_orders = [True for _ in range(len(self.sort_columns))]

    def get_args(self):
        return ('sort_columns', self.sort_columns),


class DuplicateRemovalOperator(LogicalOperator):
    def __init__(self, children=None):
        super().__init__('DuplicateRemoval', children)


class LimitOperator(LogicalOperator):
    def __init__(self, limit_count, children=None):
        super().__init__('Limit', children)
        self.limit_count = limit_count

    def get_args(self):
        return ('limit_count', self.limit_count),


class UnionOperator(LogicalOperator):
    def __init__(self, children=None):
        super().__init__('Union', children)


class IntersectOperator(LogicalOperator):
    def __init__(self, children=None):
        super().__init__('Intersect', children)


class ExceptOperator(LogicalOperator):
    def __init__(self, children=None):
        super().__init__('Except', children)


class UtilityOperator(LogicalOperator):
    def __init__(self, physical_operator):
        super().__init__('Utility')
        self.physical_operator = physical_operator

    def get_args(self):
        return ('PhysicalOperator', self.physical_operator.name),


class InsertOperator(LogicalOperator):
    def __init__(self, table_name, table_oid, columns, values=None, select=None):
        super().__init__('Insert')
        self.table_name = table_name
        self.table_oid = table_oid
        self.columns = columns
        self.values = values
        self.select = select

    def get_args(self):
        return (('table_name', self.table_name),
                ('table_oid', self.table_oid),
                ('columns', self.columns),
                ('from_select', bool(self.select)))


class DeleteOperator(LogicalOperator):
    def __init__(self, table_name, query):
        super().__init__('Delete')
        self.table_name = table_name
        self.query = query

    def get_args(self):
        return (('table_name', self.table_name),
                ('condition', self.query.condition))


class UpdateOperator(LogicalOperator):
    def __init__(self, table_name, query, columns, values, condition: Condition = None):
        super().__init__('Update')
        self.table_name = table_name
        self.columns = columns
        self.values = values
        self.condition = condition
        self.query = query

    def get_args(self):
        return (('table_name', self.table_name),
                ('columns', self.columns),
                ('condition', self.condition))

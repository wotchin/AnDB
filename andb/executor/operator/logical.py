from enum import Enum

from andb.catalog.oid import OID_SCANNING_FILE
from andb.executor.operator.utils import ExprOperation
from andb.sql.parser.ast.operation import BinaryOperation, Function
from andb.sql.parser.ast.misc import Constant
from andb.sql.parser.ast.identifier import Identifier
from andb.sql.parser.ast.misc import Tuple


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
    SCANNING_FILE_NAME = 'scanning_file'
    FUNCTION_PLACEHOLDER = 'function'
    UNKNOWN = 'unknown'


class FunctionBase(Enum):
    """Base class for function categories, allowing flexible function storage and retrieval."""

    def __new__(cls, func_cls):
        obj = object.__new__(cls)
        obj._value_ = func_cls  # Store function class reference
        return obj

    def __call__(self, *args, **kwargs):
        """Creates an instance of the function class with given args and kwargs."""
        return self.value(*args, **kwargs)  # Instantiate the function class

    @classmethod
    def register(cls, name, func_cls):
        """Registers a new function dynamically."""
        setattr(cls, name.upper(), cls(func_cls))

    @classmethod
    def get(cls, name):
        """Retrieves a function class by name, returning a factory function."""
        func_enum = getattr(cls, name.upper(), None)
        if func_enum:
            return func_enum.value  # Return the function class itself
        return None

    @staticmethod
    def is_registered_function(obj, target_class=None):
        obj_cls = obj.__class__  # Get the object's class
        if target_class:
            return any(
                target_class in [enum.value for enum in subclass]
                for subclass in FunctionBase.__subclasses__()
            ) and isinstance(obj, target_class)
        else:
            return any(
                obj_cls in [enum.value for enum in subclass]
                for subclass in FunctionBase.__subclasses__()
            )


class Coalesce:
    def __init__(self, *args):
        """Allow users to specify the indices (args) to check for non-null values."""
        self.args = args

    def __call__(self, row):
        """Given a tuple (row), return the first non-null value in the given indices."""
        for i in self.args:
            if row[i] is not None:
                return row[i]
        return None  # Return None if all values are null


class UtilityFunctions(FunctionBase):
    """Miscellaneous utility functions that work on lists of values."""
    COALESCE = Coalesce  # Register Coalesce class


class Count:
    """Counts non-null values in an iterable."""

    def __call__(self, x):
        return sum(1 for y in x if y is not None)


class Sum:
    """Sums all values in an iterable."""

    def __call__(self, x):
        return sum(x)


class Max:
    """Finds the maximum value in an iterable."""

    def __call__(self, x):
        return max(x)


class Min:
    """Finds the minimum value in an iterable."""

    def __call__(self, x):
        return min(x)


class Avg:
    """Computes the average of values in an iterable."""

    def __call__(self, x):
        return sum(x) / len(x) if x else 0


class AggregationFunctions(FunctionBase):
    """Functions that aggregate multiple values into a single result."""
    COUNT = Count
    SUM = Sum
    MAX = Max
    MIN = Min
    AVG = Avg


class AbstractColumn:
    def __init__(self):
        self.alias = None

    @property
    def standard_name(self):
        if self.alias:
            return self.alias
        return self.__repr__()


class TableColumn(AbstractColumn):
    def __init__(self, table_name, column_name, type=None):
        super().__init__()
        self.table_name = table_name
        self.column_name = column_name
        self.function_name = None
        self.alias = None
        self.type = type

    def __repr__(self):
        if not self.function_name:
            return f'{self.table_name}.{self.column_name}'
        return f'{self.table_name}.{self.column_name}'

    def __eq__(self, other):
        if not isinstance(other, TableColumn):
            return False
        return self.table_name == other.table_name and self.column_name == other.column_name

    def __hash__(self):
        return hash((self.table_name, self.column_name, self.function_name))

    def core(self):
        return TableColumn(self.table_name, self.column_name)


class FunctionColumn(AbstractColumn):
    def __init__(self, function_name, columns, type=None):
        super().__init__()
        self.function_name = function_name
        assert isinstance(columns, list) or isinstance(columns, tuple)
        self.columns = columns
        self.alias = function_name  # default
        self.type = type

    def __repr__(self):
        table_columns = ', '.join(str(c) for c in self.columns)
        return f'{self.function_name}({table_columns})'

    def __eq__(self, other):
        if not isinstance(other, FunctionColumn):
            return False
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.function_name, *self.columns))


class PromptColumn(AbstractColumn):
    def __init__(self, prompt_text, column_name='prompt', table_name=DummyTableName.TEMP_TABLE_NAME, type=None):
        super().__init__()
        self.table_name = table_name
        self.column_name = column_name
        self.prompt_text = prompt_text
        self.function_name = None
        self.alias = None
        self.type = type

    def __repr__(self):
        return f'{self.column_name}'

    def __eq__(self, other):
        if not isinstance(other, PromptColumn):
            return False
        return vars(self) == vars(other)

    def __hash__(self):
        return hash((self.table_name, self.prompt_text, self.column_name))

    def core(self):
        return PromptColumn(self.table_name, self.prompt_text, self.column_name)


class SemanticTransformColumn(AbstractColumn):
    def __init__(self, table_name, original_columns, target_column, prompt_text, k=1, type=None):
        super().__init__()
        self.table_name = table_name
        self.original_columns = original_columns
        self.column_name = target_column
        self.prompt_text = prompt_text
        self.k = k
        self.function_name = None
        self.alias = None
        self.type = type

    def __repr__(self):
        return f'{self.column_name}'

    def __eq__(self, other):
        if not isinstance(other, SemanticTransformColumn):
            return False
        return vars(self) == vars(other)

    def __hash__(self):
        return hash((self.table_name, tuple(self.original_columns), self.column_name,
                     self.prompt_text, self.k))

    def core(self):
        return SemanticTransformColumn(self.table_name, self.original_columns, self.column_name,
                                       self.prompt_text, self.k, self.type)


class VirtualColumn(AbstractColumn):
    def __init__(self, column_name):
        super().__init__()
        self.table_name = DummyTableName.TEMP_TABLE_NAME
        self.column_name = column_name
        self.function_name = None
        self.alias = None

    def __repr__(self):
        return f'{self.column_name}'

    def __eq__(self, other):
        if not isinstance(other, VirtualColumn):
            return False
        return str(self) == str(other)

    def __hash__(self):
        return hash(self.column_name)

    def core(self):
        return VirtualColumn(self.column_name)


class Condition(LogicalOperator):
    def __init__(self, operation, children=None):
        super().__init__('Expression', children)
        assert isinstance(operation, (BinaryOperation))
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
        elif isinstance(node, Function):
            columns = []
            for arg in node.args:
                columns.append(Condition._convert(arg))
            return FunctionColumn(function_name=node.op, columns=columns)
        elif isinstance(node, Tuple):
            assert (all(isinstance(x, Constant) for x in node.items))
            for x in node.items:
                if isinstance(x.value, type(node.items[0].value)) == False:
                    raise
            return [x.value for x in node.items]
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

    def is_constant_comparison(self):
        return isinstance(self.left, TableColumn) and (
                not isinstance(self.right, TableColumn) and
                not isinstance(self.right, FunctionColumn) and
                not isinstance(self.right, Condition)
        )

    def is_function_comparison(self):
        return isinstance(self.left, FunctionColumn) and (
                not isinstance(self.right, TableColumn) and
                not isinstance(self.right, FunctionColumn) and
                not isinstance(self.right, Condition)
        )


class LogicalQuery(LogicalOperator):
    def __init__(self, entry=None):
        super().__init__('Query', entry)
        self.table_attr_forms = {}
        self.from_tables = {}

        # TODO: currently, we only support two table join.
        self.join_operators = []
        self.groupby_columns = []
        self.having_clause = None
        self.scan_operators = {}
        self.sort_clause = None
        self.target_list = []
        self.condition = None
        self.alias = None
        self.limit = None
        self.distinct = False
        self.unchecked_columns = []

        self._seen_table_columns = set()

    def add_seen_table_column(self, table_column):
        self._seen_table_columns.add((table_column.table_name, table_column.column_name))

    def get_seen_table_columns(self, lookup_table_name=None):
        # return table columns in order
        table_columns = []
        for table_name, table_oid in self.from_tables.items():
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


class SemanticScanOperator(LogicalOperator):
    def __init__(self, table_name, prompt_columns, condition, children=None):
        super().__init__('SemanticScan', children)
        self.table_name = table_name
        self.prompt_columns = prompt_columns
        self.condition = condition
        self.table_columns = []
        for col in prompt_columns:
            self.table_columns.append(TableColumn(col.table_name, col.column_name))

    def get_args(self):
        return (('prompt columns', self.prompt_columns), ('condition', self.condition))


class SemanticTransformOperator(LogicalOperator):
    def __init__(self, columns, children=None):
        super().__init__('SemanticTransform', children)
        self.columns = columns

    def get_args(self):
        return ('transform columns', self.columns),


class SemanticJoinOperator(LogicalOperator):
    def __init__(self, condition, join_type, children_table_names, children=None):
        super().__init__('SemanticJoin', children)
        self.condition = condition
        self.join_type = join_type
        self.children_table_names = children_table_names

    def get_args(self):
        return (('condition', self.condition),
                ('join type', self.join_type),
                ('children table names', self.children_table_names))


class SemanticCondition(LogicalOperator):
    def __init__(self, condition, threshold, table_columns, children=None):
        super().__init__('SemanticCondition', children)
        self.condition = condition
        self.threshold = threshold
        self.table_columns = table_columns

    def __repr__(self):
        column_representations = [f"{col.table_name}.{col.column_name}" for col in self.table_columns]
        formatted_condition = self.condition.format(*column_representations)
        return f"{formatted_condition}; threshold: {self.threshold}"

    def get_args(self):
        return (('condition', self.condition),
                ('threshold', self.threshold),
                ('table columns', self.table_columns))


class SelectionOperator(LogicalOperator):
    def __init__(self, condition: Condition, children=None):
        super().__init__('Selection', children)
        self.condition = condition

    def get_args(self):
        return ('condition', self.condition),


class JoinOperator(LogicalOperator):
    def __init__(self, join_condition, join_type, table_columns=None, children=None):
        super().__init__('Join', children)
        self.join_condition = join_condition
        self.join_type = join_type
        self.table_columns = table_columns

    def get_args(self):
        return (('join_condition', self.join_condition),
                ('join_type', self.join_type))


class GroupOperator(LogicalOperator):
    def __init__(self, group_by_columns, aggregate_functions, having_clause=None, children=None):
        super().__init__('Group', children)
        self.group_by_columns = group_by_columns
        self.aggregate_functions = aggregate_functions
        self.having_clause = having_clause

    def get_args(self):
        return (
            ('group_by_columns', self.group_by_columns),
            ('aggregate_functions', self.aggregate_functions)
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
    def __init__(self, table_name, table_oid, columns, values=None, is_select=False):
        super().__init__('Insert')
        self.table_name = table_name
        self.table_oid = table_oid
        self.columns = columns
        self.values = values
        self.is_select = is_select

    def get_args(self):
        return (('table_name', self.table_name),
                ('table_oid', self.table_oid),
                ('columns', self.columns),
                ('values', self.values),
                ('is_select', self.is_select),)


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

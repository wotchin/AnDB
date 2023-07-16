class LogicalOperator:
    def __init__(self, operator_type, children=None):
        self.operator_type = operator_type
        self.children = children or []

    def add_child(self, child_operator):
        self.children.append(child_operator)

    def generate_logical_plan(self):
        logical_plan = {
            'operator': self.operator_type,
            'children': []
        }
        for child_operator in self.children:
            child_logical_plan = child_operator.generate_logical_plan()
            logical_plan['children'].append(child_logical_plan)
        return logical_plan


class ProjectionOperator(LogicalOperator):
    def __init__(self, attributes, children=None):
        super().__init__('projection', children)
        self.attributes = attributes

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        logical_plan['attributes'] = self.attributes
        return logical_plan


class SelectionOperator(LogicalOperator):
    def __init__(self, condition, children=None):
        super().__init__('selection', children)
        self.condition = condition

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        logical_plan['condition'] = self.condition
        return logical_plan


class JoinOperator(LogicalOperator):
    def __init__(self, join_condition, children=None):
        super().__init__('join', children)
        self.join_condition = join_condition

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        logical_plan['join_condition'] = self.join_condition
        return logical_plan


class GroupOperator(LogicalOperator):
    def __init__(self, group_by_attributes, aggregate_functions, children=None):
        super().__init__('group', children)
        self.group_by_attributes = group_by_attributes
        self.aggregate_functions = aggregate_functions

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        logical_plan['group_by_attributes'] = self.group_by_attributes
        logical_plan['aggregate_functions'] = self.aggregate_functions
        return logical_plan


class ScanOperator(LogicalOperator):
    def __init__(self, table_name):
        super().__init__('scan')
        self.table_name = table_name

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        logical_plan['table_name'] = self.table_name
        return logical_plan


class SortOperator(LogicalOperator):
    def __init__(self, sort_attributes, children=None):
        super().__init__('sort', children)
        self.sort_attributes = sort_attributes

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        logical_plan['sort_attributes'] = self.sort_attributes
        return logical_plan


class DuplicateRemovalOperator(LogicalOperator):
    def __init__(self, children=None):
        super().__init__('duplicate_removal', children)

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        return logical_plan


class LimitOperator(LogicalOperator):
    def __init__(self, limit_count, children=None):
        super().__init__('limit', children)
        self.limit_count = limit_count

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        logical_plan['limit_count'] = self.limit_count
        return logical_plan


class UnionOperator(LogicalOperator):
    def __init__(self, children=None):
        super().__init__('union', children)

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        return logical_plan


class IntersectOperator(LogicalOperator):
    def __init__(self, children=None):
        super().__init__('intersect', children)

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        return logical_plan


class ExceptOperator(LogicalOperator):
    def __init__(self, children=None):
        super().__init__('except', children)

    def generate_logical_plan(self):
        logical_plan = super().generate_logical_plan()
        return logical_plan


# Example usage
projection = ProjectionOperator(['name', 'age'])
selection = SelectionOperator({'op': '>', 'left': 'age', 'right': 25})
join_condition = {'op': '=', 'left': 'city', 'right': 'city'}
join = JoinOperator(join_condition)

projection.add_child(selection)
selection.add_child(join)

logical_plan = projection.generate_logical_plan()
print(logical_plan)


scan = ScanOperator('employees')
group_by_attributes = ['department']
aggregate_functions = {'salary_sum': 'SUM(salary)', 'salary_avg': 'AVG(salary)'}
group = GroupOperator(group_by_attributes, aggregate_functions)

scan.add_child(group)

logical_plan = scan.generate_logical_plan()
print(logical_plan)


def explain_logical_plan(logical_plan, indent=''):
    operator = logical_plan['operator']
    print(f"{indent}├─ Operator: {operator}")

    if operator == 'projection':
        attributes = logical_plan['attributes']
        print(f"{indent}│    Attributes: {attributes}")

    if operator == 'selection':
        condition = logical_plan['condition']
        print(f"{indent}│    Condition: {condition}")

    if operator == 'join':
        join_condition = logical_plan['join_condition']
        print(f"{indent}│    Join Condition: {join_condition}")

    if 'children' in logical_plan:
        children_count = len(logical_plan['children'])
        for i, child_plan in enumerate(logical_plan['children']):
            is_last_child = (i == children_count - 1)
            branch_char = '└' if is_last_child else '├'
            branch_indent = '    ' if is_last_child else '│   '
            child_indent = f"{indent}{branch_char}─{branch_indent}"
            explain_logical_plan(child_plan, child_indent)


explain_logical_plan(logical_plan)

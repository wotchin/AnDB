from andb.sql.optimizer.transformations import *
from andb.sql.optimizer.implementations import *
from andb.sql.parser.ast.join import JoinType

def test_logical_plan():
    # Example usage
    projection = ProjectionOperator(['name', 'age'])
    selection = SelectionOperator({'op': '>', 'left': 'age', 'right': 25})
    join_condition = {'op': '=', 'left': 'city', 'right': 'city'}
    join = JoinOperator(join_condition, JoinType.INNER_JOIN)

    projection.add_child(selection)
    selection.add_child(join)

    _logical_plan = projection.to_dict()
    # print(_logical_plan)

    scan = ScanOperator('employees', 1)
    group_by_attributes = ['department']
    aggregate_functions = {'salary_sum': 'SUM(salary)', 'salary_avg': 'AVG(salary)'}
    group = GroupOperator(group_by_attributes, aggregate_functions)

    scan.add_child(group)

    # _logical_plan = scan.to_dict()
    # print(_logical_plan)
    #
    # lines = explain_logical_plan(projection)
    # print('\n'.join(lines))


def test_transformation():
    from andb.sql.parser import SQLLexer, SQLParser
    andb_lexer = SQLLexer()
    andb_parser = SQLParser()

    stmt = "create table t1 (a int not null, b char)"
    ast = andb_parser.parse(andb_lexer.tokenize(stmt))
    trans = CreateTableTransformation()
    if trans.match(ast):
        operator = trans.on_transform(ast)
        lines = explain_logical_plan(operator)
        # print('\n'.join(lines))
        impl = UtilityImplementation()
        if impl.match(operator):
            physical_plan = impl.on_implement(operator)
            # print(physical_plan)
            physical_plan.open()
            physical_plan.next()
            r = physical_plan.close()

    stmt = "create index idx on t1 (a, b)"
    ast = andb_parser.parse(andb_lexer.tokenize(stmt))
    trans = CreateIndexTransformation()
    if trans.match(ast):
        operator = trans.on_transform(ast)
        lines = explain_logical_plan(operator)
        # print('\n'.join(lines))


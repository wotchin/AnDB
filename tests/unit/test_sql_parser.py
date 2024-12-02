from andb.sql.parser import lexer
from andb.sql.parser import parser_

andb_lexer = lexer.SQLLexer()
andb_parser = parser_.SQLParser()


def assert_parsing(stmt, s):
    ast = andb_parser.parse(andb_lexer.tokenize(stmt))
    assert str(ast) == s


def test_dql():
    assert_parsing(
        'select * from t1',
        '<Select targets=[<Star>] distinct=False from_table=<Identifier parts=t1> where=None group_by=None having=None order_by=None limit=None offset=None>')
    assert_parsing(
        "select a, b from t1",
        '<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=<Identifier parts=t1> where=None group_by=None having=None order_by=None limit=None offset=None>'
    )
    assert_parsing(
        "select distinct a, b from t1",
        '<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=True from_table=<Identifier parts=t1> where=None group_by=None having=None order_by=None limit=None offset=None>'
    )
    assert_parsing(
        "select a, b from t1, t2",
        "<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=<Join left=<Identifier parts=t1> right=<Identifier parts=t2> join_type=CROSS JOIN condition=None implicit=True> where=None group_by=None having=None order_by=None limit=None offset=None>"
    )
    assert_parsing(
        "select a, b from t1 where t1.a > 100",
        '<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=<Identifier parts=t1> where=<BinaryOperation op=> args=[<Identifier parts=t1.a>, <Constant value=100>]> group_by=None having=None order_by=None limit=None offset=None>'

    )
    assert_parsing(
        "select a, b from t1 left join t2 on t1.a = t2.b",
        '<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=<Join left=<Identifier parts=t1> right=<Identifier parts=t2> join_type=left join condition=<BinaryOperation op== args=[<Identifier parts=t1.a>, <Identifier parts=t2.b>]> implicit=False> where=None group_by=None having=None order_by=None limit=None offset=None>'

    )
    assert_parsing(
        "select a, b from t1 where a > 100 and b < 100 limit 10",
        '<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=<Identifier parts=t1> where=<BinaryOperation op=and args=[<BinaryOperation op=> args=[<Identifier parts=a>, <Constant value=100>]>, <BinaryOperation op=< args=[<Identifier parts=b>, <Constant value=100>]>]> group_by=None having=None order_by=None limit=<Constant value=10> offset=None>'

    )
    assert_parsing(
        "select count(1) from t1 where a is null",
        '<Select targets=[<Function op=count args=[<Constant value=1>]>] distinct=False from_table=<Identifier parts=t1> where=<BinaryOperation op=is args=[<Identifier parts=a>, <Constant value=None>]> group_by=None having=None order_by=None limit=None offset=None>'

    )
    assert_parsing(
        "select a, b from t1 order by a, b desc",
        '<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=<Identifier parts=t1> where=None group_by=None having=None order_by=[<OrderBy attr=<Identifier parts=a> direction=ASC>, <OrderBy attr=<Identifier parts=b> direction=DESC>] limit=None offset=None>'

    )
    assert_parsing(
        "select a, b from t1, t2 where t1.a = t2.b",
        '<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=<Join left=<Identifier parts=t1> right=<Identifier parts=t2> join_type=CROSS JOIN condition=None implicit=True> where=<BinaryOperation op== args=[<Identifier parts=t1.a>, <Identifier parts=t2.b>]> group_by=None having=None order_by=None limit=None offset=None>'

    )
    assert_parsing(
        "select a, b",
        '<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=None where=None group_by=None having=None order_by=None limit=None offset=None>'

    )
    assert_parsing(
        "select count(a), a from t1 group by a having a > 100",
        '<Select targets=[<Function op=count args=[<Identifier parts=a>]>, <Identifier parts=a>] distinct=False from_table=<Identifier parts=t1> where=None group_by=[<Identifier parts=a>] having=<BinaryOperation op=> args=[<Identifier parts=a>, <Constant value=100>]> order_by=None limit=None offset=None>'

    )


def p(stmt):
    a = andb_parser.parse(andb_lexer.tokenize(stmt))
    print(a)


def test_dml():
    assert_parsing(
        "update t1 set a = 1 where b > 100",
        "<Update table=<Identifier parts=t1> columns={'a': <Constant value=1>} where=<BinaryOperation op=> args=[<Identifier parts=b>, <Constant value=100>]>>")
    assert_parsing(
        "insert into t1 values (1, 2), (3, 4), (5, 6)",
        "<Insert table=<Identifier parts=t1> columns=None from_select=None values=[[<Constant value=1>, <Constant value=2>], [<Constant value=3>, <Constant value=4>], [<Constant value=5>, <Constant value=6>]]>")
    assert_parsing(
        "insert into t1 values (1)",
        "<Insert table=<Identifier parts=t1> columns=None from_select=None values=[[<Constant value=1>]]>")
    assert_parsing(
        "insert into t1(a, b) values (1, 2), (3, 4), (5, 6)",
        "<Insert table=<Identifier parts=t1> columns=[<Identifier parts=a>, <Identifier parts=b>] from_select=None values=[[<Constant value=1>, <Constant value=2>], [<Constant value=3>, <Constant value=4>], [<Constant value=5>, <Constant value=6>]]>")
    assert_parsing(
        "insert into t1(a, b) select a, b from t1",
        "<Insert table=<Identifier parts=t1> columns=[<Identifier parts=a>, <Identifier parts=b>] from_select=<Select targets=[<Identifier parts=a>, <Identifier parts=b>] distinct=False from_table=<Identifier parts=t1> where=None group_by=None having=None order_by=None limit=None offset=None> values=None>")
    assert_parsing(
        "delete from t1 where a > 100",
        "<Delete table=<Identifier parts=t1> where=<BinaryOperation op=> args=[<Identifier parts=a>, <Constant value=100>]>>")


def test_ddl():
    assert_parsing("CREATE TABLE t1 (a int, b int)",
                   "<CreateTable name=<Identifier parts=t1> columns=[['a', 'int'], ['b', 'int']]>")
    assert_parsing("CREATE index idx on t1 (a)",
                   "<CreateIndex name=<Identifier parts=idx> table_name=<Identifier parts=t1> columns=[<Identifier parts=a>] index_type=None>")
    assert_parsing("CREATE index idx on t1 (a) using btree",
                   "<CreateIndex name=<Identifier parts=idx> table_name=<Identifier parts=t1> columns=[<Identifier parts=a>] index_type=<Identifier parts=btree>>")
    assert_parsing("CREATE index idx on t1 (a) using lsmtree",
                   "<CreateIndex name=<Identifier parts=idx> table_name=<Identifier parts=t1> columns=[<Identifier parts=a>] index_type=<Identifier parts=lsmtree>>")
    assert_parsing("DROP TABLE t1",
                   "<DropTable name=<Identifier parts=t1>>")
    assert_parsing("DROP INDEX idx",
                   "<DropIndex name=<Identifier parts=idx>>")


def test_checkpoint():
    assert_parsing("CHECKPOINT",
                   "<Command command=CHECKPOINT>")

from andb.entrance import execute_simple_query


def test_execute_simple_query():
    execute_simple_query('create table t1 (a int not null, b text)')
    execute_simple_query("insert into t1 values (1, 'aaa')")
    execute_simple_query("insert into t1 values (2, 'bbb')")
    execute_simple_query("insert into t1 values (3, null)")
    execute_simple_query("insert into t1 values (4, 'ccc')")
    execute_simple_query("select * from t1;")
    execute_simple_query("select * from t1 where a = 1;")
    execute_simple_query("select * from t1 where a > 2;")
    execute_simple_query("select b from t1 where a > 2;")
    execute_simple_query("select a, count(a) from t1 group by a where a > 2;")

    execute_simple_query("create table t2 (a int, city char[10])")
    execute_simple_query("insert into t2 values (1, 'beijing')")
    execute_simple_query("insert into t2 values (2, 'shanghai')")
    execute_simple_query("insert into t2 values (3, 'guangdong')")
    execute_simple_query("insert into t2 values (4, 'shenzhen')")
    execute_simple_query("select a, city from t1, t2;")

    execute_simple_query("explain select a, city from t1, t2 where t1.a = t2.a;")
    execute_simple_query("select a, city from t1, t2 where t1.a = t2.a;")

    execute_simple_query('create index idx1 on t1 (a)')
    execute_simple_query("select a, city from t1, t2 where t1.a = t2.a;")
    execute_simple_query("explain select a, city from t1, t2 where t1.a = t2.a;")

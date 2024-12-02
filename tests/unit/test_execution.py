from andb.catalog.class_ import RelationKinds
from andb.catalog.oid import OID_DATABASE_ANDB
from andb.catalog.syscache import CATALOG_ANDB_CLASS
from andb.entrance import execute_simple_query
from andb.errno.errors import InitializationStageError
from andb.executor.operator.logical import Condition, InsertOperator, SelectionOperator, TableColumn, UpdateOperator
from andb.executor.operator.utils import ExprOperation
from andb.executor.portal import ExecutionPortal
from andb.runtime import global_vars
from andb.sql.optimizer.implementations import InsertImplementation, QueryImplementation, UpdateImplementation
from andb.sql.optimizer.planner import andb_query_plan
from andb.sql.parser import andb_query_parse, get_ast_type
from andb.sql.parser.ast.identifier import Identifier
from andb.sql.parser.ast.misc import Constant
from andb.sql.parser.ast.operation import BinaryOperation, Operation
from andb.storage.engines.heap.relation import close_relation, hot_simple_select_all, open_relation


def test_execute_simple_query():
    execute_simple_query('create table t1 (a int not null, b text)')
    execute_simple_query("insert into t1 values (1, 'aaa')")
    execute_simple_query("insert into t1 values (2, 'bbb')")
    execute_simple_query("insert into t1 values (3, null)")
    execute_simple_query("insert into t1 values (4, 'ccc')")
    execute_simple_query("select * from t1 order by a, b")
    execute_simple_query("select * from t1 order by a, b DESC")
    execute_simple_query("delete from t1 where a = 4")
    execute_simple_query("delete from t1")
    execute_simple_query("insert into t1 values (1, 'aaa')")
    execute_simple_query("insert into t1 values (2, 'bbb')")
    execute_simple_query("insert into t1 values (3, null)")
    execute_simple_query("insert into t1 values (4, 'ccc')")
    execute_simple_query("update t1 set a = 5 where b = 'ccc'")
    execute_simple_query("insert into t1 values (4, 'ccc')")
    execute_simple_query("insert into t1 values (4, 'ccc')")
    execute_simple_query("select * from t1;")
    execute_simple_query("select * from t1 where a = 1;")
    execute_simple_query("select * from t1 where a > 2;")
    execute_simple_query("select b from t1 where a > 2;")
    execute_simple_query("select a, count(a) from t1 where a > 2 group by a;")
    execute_simple_query("select a, count(a) from t1 where a > 2 group by a having a > 3;")

    execute_simple_query("create table t2 (a int, city text)")
    execute_simple_query("insert into t2 values (1, 'beijing')")
    execute_simple_query("insert into t2 values (2, 'shanghai')")
    execute_simple_query("insert into t2 values (3, 'guangdong')")
    execute_simple_query("insert into t2 values (4, 'shenzhen')")
    execute_simple_query("select t1.a, t2.city from t1, t2")

    execute_simple_query("explain select t1.a, city from t1, t2 where t1.a = t2.a")
    execute_simple_query("select t1.a, t2.city from t1, t2 where t1.a = t2.a")

    execute_simple_query('create index idx1 on t1 (a)')
    execute_simple_query('select a from t1')
    execute_simple_query("select t1.a, city from t1, t2 where t1.a = t2.a;")
    execute_simple_query("explain select t2.a, city from t1, t2 where t1.a = t2.a;")

    execute_simple_query("drop index idx1")
    execute_simple_query("drop table t1")
    execute_simple_query("drop table t2")



def test_abort_transaction():
    execute_simple_query('create table t1 (a int not null, b text)')

    oid = CATALOG_ANDB_CLASS.get_relation_oid('t1', OID_DATABASE_ANDB, RelationKinds.HEAP_TABLE)

    execute_simple_query("insert into t1 values (1, 'a1')")
    execute_simple_query("insert into t1 values (2, 'b2')")
    execute_simple_query("insert into t1 values (3, null)")
    execute_simple_query("insert into t1 values (4, 'c4')")

    buffer_page = global_vars.buffer_manager.get_page(open_relation(oid), 0)
    assert buffer_page.page.item_count == 4

    buffer_page = global_vars.buffer_manager.get_page(open_relation(oid), 1)
    assert buffer_page.page.item_count == 0

    relation = open_relation(oid)
    old_rows = list(hot_simple_select_all(relation))
    assert old_rows == [(1, 'a1'), (2, 'b2'), (3, None), (4, 'c4')]

    # test for insert
    xid = global_vars.xact_manager.allocate_xid()
    global_vars.xact_manager.begin_transaction(xid)
    logical_insert = InsertOperator(
        't1', oid,
        [TableColumn('t1', 'a'), TableColumn('t1', 'b')],
        [(1, 'hello'), (2, 'world')]
    )
    physical_insert = InsertImplementation.on_implement(logical_insert)
    physical_insert.open()
    list(physical_insert.next())
    physical_insert.close()
    inserted_rows = list(hot_simple_select_all(relation))
    assert inserted_rows == old_rows + [(1, 'hello'), (2, 'world')]
    global_vars.xact_manager.abort_transaction(xid)

    rows = list(hot_simple_select_all(relation))
    assert rows == old_rows
    global_vars.xact_manager.checkpoint()
    assert rows == old_rows
    close_relation(oid)

    # test for update
    xid = global_vars.xact_manager.allocate_xid()
    global_vars.xact_manager.begin_transaction(xid)

    ast = andb_query_parse("update t1 set b = 'hello' where a = 1")
    plan_tree = andb_query_plan(ast)
    portal = ExecutionPortal(ast, get_ast_type(ast), plan_tree)
    portal.xid = xid

    portal.initialize()
    portal.execute()
    portal.finalize()

    relation = open_relation(oid)
    updated_rows = list(hot_simple_select_all(relation))
    updated_rows.sort(key=lambda x: x[0])
    assert updated_rows == [(1, 'hello'), (2, 'b2'), (3, None), (4, 'c4')]
    global_vars.xact_manager.abort_transaction(xid)

    rows = list(hot_simple_select_all(relation))
    rows.sort(key=lambda x: x[0])
    print(rows)
    assert rows == old_rows

    close_relation(oid)

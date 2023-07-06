from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_CLASS
from andb.storage.engines.heap.relation import HeapTuple
from andb.errno.errors import RollbackError
from andb.storage.engines.heap.relation import hot_simple_delete, hot_create_table, hot_drop_table, hot_simple_insert, \
    hot_simple_select, hot_simple_update, close_relation, open_relation
from andb.catalog.oid import OID_DATABASE_ANDB
from andb.runtime import global_vars


def test_heap_tuple():
    tuple_desc = (
        ('id', 'int', True),
        ('name', 'text', False),
        ('city', 'varchar2', False),
    )
    class_oid = 123456
    CATALOG_ANDB_ATTRIBUTE.define_table_fields(class_oid=class_oid, fields=tuple_desc)
    attrs = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == class_oid)
    h = HeapTuple((1, 'xiaoming', 'beijing'))
    h_bytes = h.to_bytes(attrs)
    assert HeapTuple.from_bytes(h_bytes, attrs).python_tuple == (1, 'xiaoming', 'be')
    h = HeapTuple((2, None, 'sh'))
    h_bytes = h.to_bytes(attrs)
    assert HeapTuple.from_bytes(h_bytes, attrs).python_tuple == (2, None, 'sh')
    h = HeapTuple((None, None, 'sh'))

    try:
        h.to_bytes(attrs)
    except RollbackError:
        pass
    else:
        raise AssertionError()


def test_hot():
    fields = (
        ('id', 'int', True),
        ('name', 'text', False),
        ('city', 'varchar2', False),
    )
    created_table_oid = hot_create_table('test_hot', fields, database_oid=OID_DATABASE_ANDB)
    table_oid = CATALOG_ANDB_CLASS.search(lambda r: r.name == 'test_hot')[0].oid
    assert created_table_oid == table_oid

    test_hot_relation = open_relation(table_oid)
    results = hot_simple_select(test_hot_relation, test_hot_relation.last_pageno(), 0)
    assert len(results) == 0

    hot_simple_insert(test_hot_relation, (1, 'xiaoming', 'beijing'))
    hot_simple_insert(test_hot_relation, (2, 'xm2', 'b2'))
    hot_simple_insert(test_hot_relation, (3, 'xm3', 'b3'))
    hot_simple_insert(test_hot_relation, (4, 'xm4', 'b4'))

    result = hot_simple_select(test_hot_relation, test_hot_relation.last_pageno(), 0)
    assert result == (1, 'xiaoming', 'be')

    result = hot_simple_select(test_hot_relation, test_hot_relation.last_pageno(), 3)
    assert result == (4, 'xm4', 'b4')

    assert hot_simple_delete(test_hot_relation, test_hot_relation.last_pageno(), 3)
    assert hot_simple_select(test_hot_relation, test_hot_relation.last_pageno(), 3) == ()

    pageno, tid = hot_simple_update(test_hot_relation, test_hot_relation.last_pageno(), 2, (1, None, None))
    assert hot_simple_select(test_hot_relation, test_hot_relation.last_pageno(), 2) == ()
    assert hot_simple_select(test_hot_relation, test_hot_relation.last_pageno(), tid) == (1, None, None)

    global_vars.buffer_manager.sync()

    assert hot_simple_select(test_hot_relation, pageno, tid) == (1, None, None)

    close_relation(table_oid)

    try:
        close_relation(table_oid)
    except Exception:
        pass
    else:
        raise AssertionError()

    hot_drop_table('test_hot')

    assert open_relation(table_oid) is None

    try:
        hot_simple_select(test_hot_relation, pageno, tid)
    except Exception:
        pass
    else:
        raise AssertionError()


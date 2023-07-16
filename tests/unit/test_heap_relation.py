from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_CLASS, CATALOG_ANDB_INDEX
from andb.storage.engines.heap.relation import TupleData
from andb.errno.errors import RollbackError, DDLException
from andb.storage.engines.heap.relation import hot_simple_delete, hot_create_table, hot_drop_table, hot_simple_insert, \
    hot_simple_select, hot_simple_update, close_relation, open_relation, bt_create_index, bt_drop_index, \
    bt_simple_insert, bt_delete, bt_search, bt_search_range, bt_update, search_relation, RelationKinds, TuplePointer
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
    h = TupleData((1, 'xiaoming', 'beijing'))
    h_bytes = h.to_bytes(attrs)
    assert TupleData.from_bytes(h_bytes, attrs).python_tuple == (1, 'xiaoming', 'be')
    h = TupleData((2, None, 'sh'))
    h_bytes = h.to_bytes(attrs)
    assert TupleData.from_bytes(h_bytes, attrs).python_tuple == (2, None, 'sh')
    h = TupleData((None, None, 'sh'))

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

    assert not hot_simple_select(test_hot_relation, pageno, tid)


def test_btree():
    # create a data table first
    fields = (
        ('id', 'int', True),
        ('name', 'text', False),
        ('city', 'varchar2', False),
    )
    created_table_oid = hot_create_table('test_bt_table', fields, database_oid=OID_DATABASE_ANDB)
    table_oid = CATALOG_ANDB_CLASS.search(lambda r: r.name == 'test_bt_table')[0].oid
    assert created_table_oid == table_oid

    test_hot_relation = open_relation(table_oid)
    results = hot_simple_select(test_hot_relation, test_hot_relation.last_pageno(), 0)
    assert len(results) == 0

    for i in range(1000):
        hot_simple_insert(test_hot_relation, (i, '0' * i, str(i)))

    id_index_oid = bt_create_index('test_bt_index_id', table_name='test_bt_table', fields=('id',))
    assert len(CATALOG_ANDB_INDEX.search(lambda r: r.oid == id_index_oid)) == 1  # only one column is indexed
    assert search_relation('test_bt_index_id', database_name='andb', kind=RelationKinds.BTREE_INDEX) == id_index_oid

    id_index_relation = open_relation(id_index_oid)
    for i in range(1000):
        results = bt_search(id_index_relation, key=(i,))
        assert len(results) == 1
        pointer = results[0]
        result = hot_simple_select(test_hot_relation, pageno=pointer.pageno, tid=pointer.tid)
        assert result == (i, '0' * i, str(i)[:2])

    assert bt_search(id_index_relation, key=(500,))
    bt_delete(id_index_relation, key=(500, ))
    assert not bt_search(id_index_relation, key=(500, ))

    old_tuple_pointer = bt_search(id_index_relation, key=(100,))[0]
    assert old_tuple_pointer
    bt_update(id_index_relation, key=(100,), tuple_pointer=TuplePointer(0, 0))
    assert bt_search(id_index_relation, key=(100,))[0] != old_tuple_pointer
    assert bt_search(id_index_relation, key=(100,))[0] == TuplePointer(0, 0)

    bt_simple_insert(id_index_relation, key=(100, ), tuple_pointer=TuplePointer(0, 1))
    bt_simple_insert(id_index_relation, key=(100, ), tuple_pointer=TuplePointer(0, 2))
    bt_simple_insert(id_index_relation, key=(100, ), tuple_pointer=TuplePointer(0, 3))
    results = bt_search(id_index_relation, key=(100,))
    assert len(results) == 4
    for i in range(4):
        assert results[i].pageno == 0
        assert results[i].tid == i

    results = bt_search_range(id_index_relation, start_key=(1, ), end_key=(100, ))
    assert len(results) == 98

    for i, r in enumerate(results):
        assert len(r) == 1
        pageno = r[0].pageno
        tid = r[0].tid
        t = hot_simple_select(test_hot_relation, pageno, tid)
        id_ = i + 2  # starts from 2
        assert t == (id_, '0' * id_, str(id_)[:2])

    close_relation(id_index_oid)

    # simulate crash
    global_vars.buffer_manager.sync()
    global_vars.buffer_manager.reset()
    id_index_relation = open_relation(id_index_oid)
    for i in range(1000):
        results = bt_search(id_index_relation, key=(i,))
        # we have modified before
        if i == 100:
            assert len(results) == 4
            for j in range(4):
                assert results[j].pageno == 0
                assert results[j].tid == j
        # we have modified before
        elif i == 500:
            assert len(results) == 0
        # common cases
        else:
            assert len(results) == 1
            pointer = results[0]
            result = hot_simple_select(test_hot_relation, pageno=pointer.pageno, tid=pointer.tid)
            assert result == (i, '0' * i, str(i)[:2])

    # test release lock
    try:
        bt_drop_index('test_bt_index_id')
    except DDLException:
        pass
    else:
        raise

    close_relation(id_index_oid)
    bt_drop_index('test_bt_index_id')

    try:
        bt_search(id_index_relation, key=(100,))
    except:
        pass
    else:
        raise AssertionError()

    id_index_relation = open_relation(id_index_oid)
    assert not id_index_relation

    # test multi-columns index
    id_index_oid = bt_create_index('test_bt_index_id_city', table_name='test_bt_table', fields=('id', 'city'))
    assert len(CATALOG_ANDB_INDEX.search(lambda r: r.oid == id_index_oid)) == 2  # 2 columns are indexed
    assert search_relation('test_bt_index_id_city', database_name='andb',
                           kind=RelationKinds.BTREE_INDEX) == id_index_oid

    id_index_relation = open_relation(id_index_oid)
    p = bt_search(id_index_relation, key=(1, '1'))[0]
    tuple_ = hot_simple_select(test_hot_relation, p.pageno, p.tid)
    assert tuple_[0] == 1 and tuple_[2] == '1'
    assert not bt_search(id_index_relation, key=(1, '2'))

    bt_simple_insert(id_index_relation, key=(1, '0'), tuple_pointer=TuplePointer(0, 0))
    bt_simple_insert(id_index_relation, key=(1, '2'), tuple_pointer=TuplePointer(0, 0))
    # todo: fix negative value
    # results = bt_search_range(id_index_relation, start_key=(-1, '0'), end_key=(2, '2'))
    results = bt_search_range(id_index_relation, start_key=(0, '0'), end_key=(2, '2'))
    assert len(results) == 3

from andb.catalog.class_ import AndbClassForm, AndbClassTable, RelationKinds
from andb.catalog.oid import OID_RELATION_START
from andb.catalog.type import AndbTypeTable, IntegerType


def test_andb_class():
    table = AndbClassTable()
    assert table.allocate_oid() == OID_RELATION_START
    t1 = AndbClassForm(oid=1, name='abc', kind=RelationKinds.HEAP_TABLE)
    t2 = AndbClassForm(oid=2, name='adafasfsdfsfsf', kind=RelationKinds.HEAP_TABLE)
    t3 = AndbClassForm(oid=3, name='sssssssssss', kind=RelationKinds.BTREE_INDEX)
    t4 = AndbClassForm(oid=4, name='0001233sssssssss', kind=RelationKinds.HEAP_TABLE)
    table.insert(t1)
    table.insert(t3)
    table.insert(t4)
    table.insert(t2)
    table.save()
    table2 = AndbClassTable()
    table2.load()
    assert table2.rows == table.rows
    assert table.allocate_oid() == 5
    table.insert(AndbClassForm(oid=5, name='aaa', kind=RelationKinds.BTREE_INDEX))
    table.save()
    table2.load()
    assert table2.rows == table.rows

    assert table.allocate_oid() == 6


def test_andb_class_form():
    t0 = AndbClassForm(oid=0, name='adafasfsdfsfsf', kind=RelationKinds.HEAP_TABLE)
    t1 = AndbClassForm(oid=1, name='abc', kind=RelationKinds.HEAP_TABLE)
    t2 = AndbClassForm(oid=2, name='adafasfsdfsfsf', kind=RelationKinds.HEAP_TABLE)
    assert t0 < t1 < t2


def test_andb_type():
    t = AndbTypeTable()
    t.init()
    assert t.get_type_oid('integer') == t.get_type_oid('int') == IntegerType.oid
    assert t.cast_bytes_to_datum('int', t.cast_datum_to_bytes('int', 0)) == 0
    assert t.cast_bytes_to_datum('bigint', t.cast_datum_to_bytes('bigint', -1)) == -1
    assert t.cast_bytes_to_datum('float', t.cast_datum_to_bytes('float', -1)) == -1
    assert t.cast_bytes_to_datum('double', t.cast_datum_to_bytes('double', 0.0001)) == 0.0001
    assert t.cast_bytes_to_datum('char', t.cast_datum_to_bytes('char', 'a')) == 'a'
    assert t.cast_bytes_to_datum('char', t.cast_datum_to_bytes('char', 97)) == 'a'
    assert t.cast_bytes_to_datum('text', t.cast_datum_to_bytes('text', 'hello world')) == 'hello world'
    assert t.cast_bytes_to_datum('varchar', t.cast_datum_to_bytes('varchar', 'hello world')) == 'hello world'

import os

from andb.catalog.andb_class import AndbClassTuple, AndbClassTable, RelationKinds
from andb.constants.filename import CATALOG_DIR
from andb.catalog.oid import OID_RELATION_START


def test_andb_class():
    if not os.path.exists(CATALOG_DIR):
        os.mkdir(CATALOG_DIR)
    table = AndbClassTable()
    assert table.allocate_oid() == OID_RELATION_START
    t1 = AndbClassTuple(oid=1, name='abc', kind=RelationKinds.HEAP_TABLE)
    t2 = AndbClassTuple(oid=2, name='adafasfsdfsfsf', kind=RelationKinds.HEAP_TABLE)
    t3 = AndbClassTuple(oid=3, name='sssssssssss', kind=RelationKinds.BTREE_INDEX)
    t4 = AndbClassTuple(oid=4, name='0001233sssssssss', kind=RelationKinds.HEAP_TABLE)
    table.add(t1)
    table.add(t3)
    table.add(t4)
    table.add(t2)
    table.save()
    table2 = AndbClassTable()
    table2.load()
    assert table2.rows == table.rows
    assert table.allocate_oid() == 5
    table.add(AndbClassTuple(oid=5, name='aaa', kind=RelationKinds.BTREE_INDEX))
    table.save()
    table2.load()
    assert table2.rows == table.rows

    assert table.allocate_oid() == 6

    os.unlink(os.path.join(CATALOG_DIR, AndbClassTable.__tablename__))
    os.rmdir(CATALOG_DIR)


def test_andb_class_tuple():
    t0 = AndbClassTuple(oid=0, name='adafasfsdfsfsf', kind=RelationKinds.HEAP_TABLE)
    t1 = AndbClassTuple(oid=1, name='abc', kind=RelationKinds.HEAP_TABLE)
    t2 = AndbClassTuple(oid=2, name='adafasfsdfsfsf', kind=RelationKinds.HEAP_TABLE)
    assert t0 < t1 < t2

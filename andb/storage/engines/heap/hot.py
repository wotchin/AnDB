import os

from andb.catalog.class_ import RelationKinds
from andb.catalog.oid import OID_DATABASE_ANDB
from andb.catalog.syscache import CATALOG_ANDB_CLASS, CATALOG_ANDB_TYPE, CATALOG_ANDB_ATTRIBUTE
from andb.storage.buffer import BufferManager
from andb.storage.engines.heap.bptree import BPlusTree
from andb.common.utils import touch
from andb.constants.filename import BASE_DIR


class BufferedBPTree(BPlusTree):
    def __init__(self, relation, bufmgr: BufferManager):
        super().__init__()
        self.relation = relation
        self.bufmgr = bufmgr

    def load_page(self, pageno):
        return self.bufmgr.get_page(self.relation, pageno).page

    def _need_to_split(self, node):
        # todo: user-defined load factor
        return super()._need_to_split(node)


# todo: using type catalog
class TableField:
    def __init__(self, name, type_name):
        self.name = name
        self.type_oid = CATALOG_ANDB_TYPE.get_type_oid(type_name)


class HeapOrientedTable:
    def __init__(self):
        pass

    def create_table(self, table_name, fields, database_oid=OID_DATABASE_ANDB):
        # todo: not supported atomic DDL yet
        oid = CATALOG_ANDB_CLASS.create(name=table_name,
                                        kind=RelationKinds.HEAP_TABLE,
                                        database_oid=database_oid)
        # todo: fields, schema, data file
        # fields format: (name, type_name, notnull)
        CATALOG_ANDB_ATTRIBUTE.define_table_fields(class_oid=oid, fields=fields)
        touch(
            os.path.join(BASE_DIR, str(database_oid), str(oid))
        )
        return oid

    def drop_table(self):
        # TODO: Implement the dropping of the table
        pass

    def insert(self, values):
        # TODO: Implement the insertion of a tuple into the table
        pass

    def update(self, key, values):
        # TODO: Implement the update operation on a tuple in the table
        pass

    def delete(self, key):
        # TODO: Implement the deletion of a tuple from the table
        pass

    def select(self, key):
        # TODO: Implement the selection of a tuple from the table
        pass

from andb.storage.buffer import BufferManager
from andb.storage.engines.heap.bptree import BPlusTree
from andb.catalog.andb_class import ANDB_CLASS, AndbClassTuple, RelationKinds
from andb.catalog.type import BUILTIN_TYPES


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
        self.type_oid = BUILTIN_TYPES.get_type_oid(type_name)


class HeapOrientedTable:
    def __init__(self):
        pass

    def create_table(self, table_name, table_schema='andb', fields=None):
        oid = ANDB_CLASS.allocate_oid()
        # todo: not supported atomic DDL yet
        t = AndbClassTuple(oid=oid, name=table_name, kind=RelationKinds.HEAP_TABLE)
        ANDB_CLASS.add(t)
        # todo: fields, schema, data file
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

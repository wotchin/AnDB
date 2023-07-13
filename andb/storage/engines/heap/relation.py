import os

from andb.catalog.class_ import RelationKinds
from andb.catalog.oid import OID_DATABASE_ANDB, INVALID_OID
from andb.catalog.syscache import CATALOG_ANDB_CLASS, CATALOG_ANDB_TYPE, CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_DATABASE
from andb.catalog.type import VARIABLE_LENGTH, VARIABLE_TYPE_HEADER_LENGTH, VarcharType
from andb.common.file_operation import directio_file_open, touch, file_size, file_close, file_write, file_open, \
    file_remove
from andb.constants.filename import BASE_DIR
from andb.constants.strings import LITTLE_END
from andb.constants.values import MAX_TABLE_COLUMNS, PAGE_SIZE
from andb.errno.errors import RollbackError, DDLException
from andb.runtime import global_vars
from andb.storage.common.page import INVALID_BYTES
from andb.storage.engines.heap.bptree import BPlusTree, TuplePointer
from andb.storage.lock import rlock


class BufferedBPTree(BPlusTree):
    def __init__(self, relation):
        super().__init__()
        self.relation = relation

    def load_page(self, pageno):
        return global_vars.buffer_manager.get_page(self.relation, pageno).page

    def _need_to_split(self, node):
        # todo: user-defined load factor
        return super()._need_to_split(node)


__relcache = {}


class Relation:
    def __init__(self, oid, database_oid, name):
        self.oid = oid
        self.database_oid = database_oid
        self.name = name
        self.attrs = ()
        self.file_path = os.path.join(BASE_DIR, str(database_oid), str(oid))
        self.refcount = 0
        self.is_index = False
        self.is_heap = False
        self.opened = True

    @property
    def fd(self):
        assert self.file_path
        assert self.opened
        return directio_file_open(self.file_path, os.O_RDWR | os.O_CREAT)

    def last_pageno(self):
        assert self.opened
        return file_size(self.fd) // PAGE_SIZE

    def __repr__(self):
        if self.oid == INVALID_OID:
            return '<InvalidRelation>'
        return str(self.oid)

    def __hash__(self):
        return self.oid

    def __eq__(self, other):
        return (isinstance(other, Relation) and
                other.oid == self.oid)


class HeapTuple:
    NULLS_BYTES = 8

    def __init__(self, python_tuple):
        self.python_tuple = python_tuple

    def __repr__(self):
        return str(self.python_tuple)

    def to_bytes(self, andb_attr_form_array):
        assert len(andb_attr_form_array) == len(self.python_tuple)
        nulls = 0x0000000000000000

        byte_array = bytearray()
        for i, form in enumerate(andb_attr_form_array):
            datum = self.python_tuple[i]
            if datum is None:
                # update nulls
                if form.notnull:
                    raise RollbackError(f'the field {form.name} cannot be null.')

                assert i < MAX_TABLE_COLUMNS
                nulls |= (1 << i)
            else:
                # as varchar is fixed length, we should truncate it here
                if form.type_oid == VarcharType.oid:
                    datum = datum[:form.length]
                bytes_ = CATALOG_ANDB_TYPE.cast_datum_to_bytes(
                    CATALOG_ANDB_TYPE.get_type_name(form.type_oid),
                    datum)
                byte_array.extend(bytes_)

        # concatenate nulls and data
        nulls_bytes = int.to_bytes(nulls, length=self.NULLS_BYTES, byteorder=LITTLE_END)
        return nulls_bytes + bytes(byte_array)

    @classmethod
    def from_bytes(cls, data, andb_attr_form_array):
        if len(data) < cls.NULLS_BYTES:
            raise RollbackError('the data corrupt')
        nulls = int.from_bytes(data[:cls.NULLS_BYTES], byteorder=LITTLE_END)
        values = []
        bytes_cursor = cls.NULLS_BYTES  # starts from real data
        for i, attr_form in enumerate(andb_attr_form_array):
            if nulls & (1 << i) == (1 << i):
                values.append(None)
            else:
                type_name = CATALOG_ANDB_TYPE.get_type_name(attr_form.type_oid)
                type_form = CATALOG_ANDB_TYPE.get_type_form(type_name)
                length = attr_form.length
                if length == VARIABLE_LENGTH:
                    length = type_form.bytes_length(
                        data[bytes_cursor: bytes_cursor + VARIABLE_TYPE_HEADER_LENGTH]) + VARIABLE_TYPE_HEADER_LENGTH
                value = CATALOG_ANDB_TYPE.cast_bytes_to_datum(
                    type_name, data[bytes_cursor: bytes_cursor + length]
                )
                values.append(value)
                bytes_cursor += length
        return cls(tuple(values))


def search_relation(relation_name, database_name, kind):
    results = CATALOG_ANDB_DATABASE.search(lambda r: r.name == database_name)
    if len(results) != 1:
        raise RollbackError('Not found the database.')

    database_oid = results[0].oid
    return search_relation_by_db_oid(relation_name, database_oid, kind)


def search_relation_by_db_oid(relation_name, database_oid, kind):
    results = CATALOG_ANDB_CLASS.search(
        lambda r: r.database_oid == database_oid and r.name == relation_name and r.kind == kind
    )
    if len(results) != 1:
        raise RollbackError('Not found the table.')

    return results[0].oid


def open_relation(oid, lock_mode=rlock.NO_LOCK):
    if oid in __relcache:
        relation = __relcache[oid]
    else:
        results = CATALOG_ANDB_CLASS.search(lambda r: r.oid == oid)
        if len(results) != 1:
            return None

        class_meta = results[0]
        relation = Relation(
            oid=oid, database_oid=class_meta.database_oid, name=class_meta.name
        )
        relation.attrs = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == oid)
        relation.is_index = class_meta.kind == RelationKinds.BTREE_INDEX
        relation.is_heap = class_meta.kind == RelationKinds.HEAP_TABLE
        __relcache[oid] = relation

    # try to get lock
    if lock_mode != rlock.NO_LOCK:
        rlock.lock_acquire(oid, lock_mode, False, 0)

    relation.refcount += 1
    return relation


def close_relation(oid, lock_mode=rlock.NO_LOCK):
    relation = __relcache[oid]
    if lock_mode != rlock.NO_LOCK:
        rlock.lock_release(relation.oid, lock_mode)
    relation.refcount -= 1
    if relation.refcount == 0:
        relation.opened = False
        del __relcache[oid]


def hot_create_table(table_name, fields, database_oid=OID_DATABASE_ANDB):
    # todo: not supported atomic DDL yet
    oid = CATALOG_ANDB_CLASS.create(name=table_name,
                                    kind=RelationKinds.HEAP_TABLE,
                                    database_oid=database_oid)
    # todo: fields, schema, data file
    # fields format: (name, type_name, notnull)
    CATALOG_ANDB_ATTRIBUTE.define_relation_fields(class_oid=oid, fields=fields)
    touch(
        os.path.join(BASE_DIR, str(database_oid), str(oid))
    )
    return oid


def hot_drop_table(table_name, database_oid=OID_DATABASE_ANDB):
    results = CATALOG_ANDB_CLASS.search(
        lambda r: r.name == table_name and r.database_oid == database_oid)
    # todo: refactor the return value
    if len(results) != 1:
        return False, 'Not found the table.'

    # todo: atomic
    oid = results[0].oid
    # todo: try to get lock and close fd if it opened
    relation = open_relation(oid, rlock.ACCESS_EXCLUSIVE_LOCK)
    file_close(relation.fd)
    os.unlink(os.path.join(BASE_DIR, str(database_oid), str(oid)))
    CATALOG_ANDB_ATTRIBUTE.delete(lambda r: r.class_oid == oid)
    CATALOG_ANDB_CLASS.delete(lambda r: r.oid == oid)

    # clean buffer
    global_vars.buffer_manager.clean_relation(relation)
    close_relation(oid, rlock.ACCESS_EXCLUSIVE_LOCK)
    return True


def hot_simple_insert(relation: Relation, python_tuple):
    # no redo and undo log here, only update cached page
    # todo: find from fsm first
    buffer_page = global_vars.buffer_manager.get_page(relation, relation.last_pageno())
    lsn = global_vars.lsn_manager.max_lsn()
    return buffer_page.pageno, buffer_page.page.insert(
        lsn, HeapTuple(python_tuple).to_bytes(relation.attrs))


def hot_simple_update(relation: Relation, pageno, tid, python_tuple):
    # todo: use a same LSN
    if hot_simple_delete(relation, pageno, tid):
        return hot_simple_insert(relation, python_tuple)


def hot_simple_delete(relation: Relation, pageno, tid):
    buffer_page = global_vars.buffer_manager.get_page(relation, pageno)
    lsn = global_vars.lsn_manager.max_lsn()
    return buffer_page.page.delete(lsn, tid)


def hot_simple_select(relation: Relation, pageno, tid):
    buffer_page = global_vars.buffer_manager.get_page(relation, pageno)
    data = buffer_page.page.select(tid)
    if data == INVALID_BYTES:
        return ()
    return HeapTuple.from_bytes(data, relation.attrs).python_tuple


def bt_create_index(index_name, table_name, fields, database_oid=OID_DATABASE_ANDB):
    table_oid = search_relation_by_db_oid(table_name, database_oid, kind=RelationKinds.HEAP_TABLE)
    table_relation = open_relation(table_oid, lock_mode=rlock.SHARE_LOCK)
    attr_form_array = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == table_oid)
    index_attr_form_array = []
    # only get index columns
    for field in fields:
        for attr in attr_form_array:
            if attr.name == field:
                index_attr_form_array.append(attr)

    tree = BufferedBPTree(relation=None)
    # todo: fix this lsn
    lsn = global_vars.lsn_manager.max_lsn()
    table_relation.last_pageno()
    for pageno in range(table_relation.last_pageno()):
        hot_page = global_vars.buffer_manager.get_page(table_relation, pageno)
        for idx in range(len(hot_page.item_ids)):
            tuple_data = hot_page.select(idx)
            if tuple_data == INVALID_BYTES:
                continue
            heap_tuple = HeapTuple.from_bytes(tuple_data, attr_form_array).python_tuple
            key_tuple = tuple(heap_tuple[attr.num] for attr in index_attr_form_array)
            key_data = HeapTuple(python_tuple=key_tuple).to_bytes(index_attr_form_array)
            tuple_pointer = TuplePointer(pageno, idx)
            tree.insert(lsn, key_data, tuple_pointer.pack())

    # generate final index file
    index_oid = CATALOG_ANDB_CLASS.create(index_name, RelationKinds.BTREE_INDEX, database_oid)
    index_fields = [(attr.name, CATALOG_ANDB_TYPE.get_type_name(attr.type_oid), attr.notnull)
                    for attr in index_attr_form_array]
    CATALOG_ANDB_ATTRIBUTE.define_relation_fields(class_oid=index_oid, fields=index_fields)
    fd = file_open(os.path.join(BASE_DIR, str(database_oid), str(index_oid)),
                   flags=os.O_RDWR | os.O_CREAT)
    file_write(fd, data=tree.serialize(), sync=True)
    return index_oid


def bt_drop_index(index_name, database_oid=OID_DATABASE_ANDB):
    results = CATALOG_ANDB_CLASS.search(lambda r: r.name == index_name and r.database_oid == database_oid)
    if len(results) == 0:
        raise DDLException('Not found the index name %s.' % index_name)

    class_form = results[0]
    CATALOG_ANDB_CLASS.delete(lambda r: r.name == index_name)
    CATALOG_ANDB_ATTRIBUTE.delete(lambda r: r.class_oid == class_form.oid)
    fd = file_open(os.path.join(BASE_DIR, str(database_oid), str(class_form.oid)), flags=os.O_RDWR)
    file_remove(fd)


def bt_simple_insert(relation: Relation, key, tuple_pointer):
    tree = BufferedBPTree(relation)
    # todo: lsn
    lsn = global_vars.lsn_manager.max_lsn()
    tree.insert(lsn, key, tuple_pointer.pack())


def bt_update(relation: Relation, key, tuple_pointer):
    tree = BufferedBPTree(relation)
    # todo: lsn
    lsn = global_vars.lsn_manager.max_lsn()
    tree.delete(lsn, key)
    tree.insert(lsn, key, tuple_pointer.pack())


def bt_delete(relation: Relation, key):
    tree = BufferedBPTree(relation)
    # todo: lsn
    lsn = global_vars.lsn_manager.max_lsn()
    tree.delete(lsn, key)


def bt_search(relation: Relation, key):
    tree = BufferedBPTree(relation)
    results = tree.search(key)

    if len(results) == 0:
        return ()
    pointers = [
        TuplePointer.unpack(TuplePointer(), data) for data in results
    ]
    return pointers

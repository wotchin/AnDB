import logging
import os
import threading

from andb.catalog.class_ import RelationKinds
from andb.catalog.oid import OID_DATABASE_ANDB, INVALID_OID
from andb.catalog.syscache import CATALOG_ANDB_CLASS, CATALOG_ANDB_TYPE, CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_DATABASE, \
    CATALOG_ANDB_INDEX
from andb.catalog.type import VARIABLE_LENGTH, VARIABLE_TYPE_HEADER_LENGTH, VarcharType
from andb.common.file_operation import directio_file_open, file_touch, file_size, file_close, file_write, file_open, \
    file_remove, file_read, file_lseek
from andb.common.utils import filesize_to_pageno
from andb.constants.filename import BASE_DIR
from andb.constants.strings import BIG_END
from andb.constants.values import MAX_TABLE_COLUMNS, PAGE_SIZE
from andb.errno.errors import RollbackError, DDLException
from andb.runtime import global_vars
from andb.storage.engines.heap.page import SlotPage, INVALID_BYTES
from andb.storage.engines.heap.page import INVALID_ITEM_ID
from andb.storage.engines.heap.bptree import BPlusTree, TuplePointer, create_node
from andb.storage.engines.heap.redo import WALAction, WALRecord
from andb.storage.engines.heap.undo import UndoOperation, UndoRecord
from andb.storage.lock import rlock
from andb.storage.utils import easy_tuple_serialize


class BufferedBPTree(BPlusTree):
    """Private node copies per operation; publish all split pages in one WAL record."""
    def __init__(self, relation):
        self.relation = relation
        self.nodes = {}
        self.before = {}
        self.changed = set()
        self.original_root = global_vars.buffer_manager.get_index_root(relation)
        super().__init__(root_node=self.load_page(self.original_root))

    def load_page(self, pageno):
        if pageno not in self.nodes:
            raw = global_vars.buffer_manager.get_page(self.relation, pageno).data
            self.before[pageno] = raw
            self.nodes[pageno] = create_node(raw)
        return self.nodes[pageno]

    def mark_dirty(self, node):
        super().mark_dirty(node)
        self.changed.add(node.get_pageno())

    def _allocate_pageno(self):
        return global_vars.buffer_manager.allocate_index_pageno(self.relation)

    def _allocate_node(self, is_leaf):
        node = super()._allocate_node(is_leaf)
        self.nodes[node.get_pageno()] = node
        self.before[node.get_pageno()] = None
        return node

    def save_changes(self):
        if not self.changed:
            return
        pages = [(n, self.before[n], self.nodes[n].pack()) for n in sorted(self.changed)]
        root = (self.original_root, self.root.get_pageno())
        global_vars.xact_manager.record_page_changes(self.relation, pages, root)


__relcache = {}
__relcache_lock = threading.RLock()


def _transaction_active():
    mgr = global_vars.xact_manager
    return mgr is not None and mgr.in_transaction()


def _relation_lock(relation, write=False):
    # There are no physical xmin/xmax versions yet. Use honest strict 2PL,
    # rather than allowing dirty reads behind an unused snapshot helper.
    mode = rlock.ACCESS_EXCLUSIVE_LOCK if write else rlock.ACCESS_SHARE_LOCK
    if rlock.lock_acquire(relation.oid, mode, dont_wait=False, wait_seconds=5) == rlock.LOCK_NOT_AVAILABLE:
        raise RollbackError('Could not acquire relation lock; retry the transaction')




class Relation:
    def __init__(self, oid, database_oid, name):
        self.oid = oid
        self.database_oid = database_oid
        self.name = name
        self.attrs = ()
        self.file_path = os.path.join(BASE_DIR, str(database_oid), str(oid))
        self.refcount = 0
        self.kind = None
        self._last_pageno = None

    @property
    def is_heap(self):
        return self.kind == RelationKinds.HEAP_TABLE

    @property
    def fd(self):
        assert self.file_path
        return directio_file_open(self.file_path, os.O_RDWR | os.O_CREAT)

    def last_pageno(self):
        assert self.is_heap
        disk = filesize_to_pageno(file_size(self.fd))
        cached = global_vars.buffer_manager.heap_highwater.get(self, 0)
        return max(disk, cached, self._last_pageno or 0)

    def increase_last_pageno(self):
        self._last_pageno = self.last_pageno() + 1

    def __repr__(self):
        if self.oid == INVALID_OID:
            return '<InvalidRelation>'
        return str(self.oid)

    def __hash__(self):
        return self.oid

    def __eq__(self, other):
        return (isinstance(other, Relation) and
                other.oid == self.oid)


class TupleData:
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
        nulls_bytes = int.to_bytes(nulls, length=self.NULLS_BYTES, byteorder=BIG_END)
        return nulls_bytes + bytes(byte_array)

    @classmethod
    def from_bytes(cls, data, andb_attr_form_array):
        if len(data) < cls.NULLS_BYTES:
            raise RollbackError('the data corrupt')
        nulls = int.from_bytes(data[:cls.NULLS_BYTES], byteorder=BIG_END)
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


# def search_relation(relation_name, database_name, kind):
#     results = CATALOG_ANDB_DATABASE.search(lambda r: r.name == database_name)
#     if len(results) != 1:
#         raise RollbackError('not found the database.')
#
#     database_oid = results[0].oid
#     return CATALOG_ANDB_CLASS.get_relation_oid(relation_name, database_oid, kind)
#
#
# def search_relation_by_db_oid(relation_name, database_oid, kind):
#     results = CATALOG_ANDB_CLASS.search(
#         lambda r: r.database_oid == database_oid and r.name == relation_name and r.kind == kind
#     )
#     if len(results) != 1:
#         raise RollbackError('Not found the table.')
#
#     return results[0].oid


def open_relation(oid, lock_mode=rlock.ACCESS_SHARE_LOCK):
    # Lock before entering the relcache mutex: a blocked writer must not prevent
    # a current reader from closing its relation and committing.
    actual_mode = lock_mode
    if lock_mode in (rlock.ROW_EXCLUSIVE_LOCK, rlock.SHARE_ROW_EXCLUSIVE_LOCK):
        actual_mode = rlock.ACCESS_EXCLUSIVE_LOCK
    if actual_mode != rlock.NO_LOCK:
        if rlock.lock_acquire(oid, actual_mode, dont_wait=False, wait_seconds=5) == rlock.LOCK_NOT_AVAILABLE:
            raise RollbackError('Could not acquire relation lock; retry the transaction')
    with __relcache_lock:
        if oid not in __relcache:
            results = CATALOG_ANDB_CLASS.search(lambda r: r.oid == oid)
            if len(results) != 1:
                if actual_mode != rlock.NO_LOCK and not _transaction_active():
                    rlock.lock_release(oid, actual_mode)
                return None
            meta = results[0]
            relation = Relation(oid, meta.database_oid, meta.name)
            relation.attrs = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == oid)
            relation.kind = meta.kind
            __relcache[oid] = relation
        relation = __relcache[oid]
        relation.refcount += 1
        return relation


def close_relation(oid, lock_mode=rlock.ACCESS_SHARE_LOCK):
    with __relcache_lock:
        relation = __relcache[oid]
        relation.refcount -= 1
        if not _transaction_active() and lock_mode != rlock.NO_LOCK:
            actual_mode = rlock.ACCESS_EXCLUSIVE_LOCK if lock_mode in (
                rlock.ROW_EXCLUSIVE_LOCK, rlock.SHARE_ROW_EXCLUSIVE_LOCK) else lock_mode
            rlock.lock_release(oid, actual_mode)
        if relation.refcount == 0:
            del __relcache[oid]


def hot_create_table(table_name, fields, database_oid=OID_DATABASE_ANDB):
    #TODO: not supported atomic DDL yet
    if CATALOG_ANDB_CLASS.exist_table(table_name, database_oid):
        raise DDLException('the same name table already exists.')

    oid = CATALOG_ANDB_CLASS.create(name=table_name,
                                    kind=RelationKinds.HEAP_TABLE,
                                    database_oid=database_oid)
    #TODO: fields, schema, data file
    # fields format: (name, type_name, notnull)
    CATALOG_ANDB_ATTRIBUTE.define_table_fields(class_oid=oid, fields=fields)
    file_touch(
        os.path.join(BASE_DIR, str(database_oid), str(oid))
    )
    return oid


def hot_drop_table(table_name, database_oid=OID_DATABASE_ANDB):
    #TODO: refactor the return value
    oid = CATALOG_ANDB_CLASS.get_relation_oid(table_name, database_oid, kind=RelationKinds.HEAP_TABLE)
    if oid == INVALID_OID:
        raise DDLException('not found the table.')

    results = CATALOG_ANDB_INDEX.search(lambda r: r.table_oid == oid)
    if len(results) > 0:
        raise DDLException('there are indexes associated with the table.')

    relation = open_relation(oid, rlock.ACCESS_EXCLUSIVE_LOCK)
    if not relation:
        raise DDLException('cannot drop the table because the table is in use.')
    file_close(relation.fd)
    os.unlink(os.path.join(BASE_DIR, str(database_oid), str(oid)))
    CATALOG_ANDB_ATTRIBUTE.delete(lambda r: r.class_oid == oid)
    CATALOG_ANDB_CLASS.delete(lambda r: r.oid == oid)

    # clean buffer
    global_vars.buffer_manager.evict_relation(relation)
    close_relation(oid, rlock.ACCESS_EXCLUSIVE_LOCK)
    return True


def hot_simple_insert(relation: Relation, python_tuple, lsn=None):
    _relation_lock(relation, write=True)
    mgr, bm = global_vars.xact_manager, global_vars.buffer_manager
    # Validate before any mutation; an oversized tuple must not leave a tombstone.
    data = TupleData(python_tuple).to_bytes(relation.attrs)
    if not SlotPage.allocate(0).can_put_item(data):
        raise RollbackError('Tuple is too large for a heap page')
    with mgr.storage_lock:
        pageno = relation.last_pageno()
        while True:
            before = bm.get_page(relation, pageno).data
            page = SlotPage.unpack(before)
            tid = page.insert(0, data)
            if tid != INVALID_ITEM_ID:
                break
            pageno += 1
        mgr.record_page_changes(relation, [(pageno, before, page.pack())])
        return pageno, tid


def hot_simple_update(relation: Relation, pageno, tid, python_tuple):
    _relation_lock(relation, write=True)
    data = TupleData(python_tuple).to_bytes(relation.attrs)
    if not SlotPage.allocate(0).can_put_item(data):
        raise RollbackError('Tuple is too large for a heap page')
    # The transaction lock prevents observation between these two logged steps;
    # an intervening crash undoes both, in reverse order.
    if hot_simple_delete(relation, pageno, tid):
        return hot_simple_insert(relation, python_tuple)


def hot_simple_delete(relation: Relation, pageno, tid, lsn=None):
    return hot_batch_delete(relation, pageno, [tid])


def hot_batch_delete(relation: Relation, pageno, tid_list):
    _relation_lock(relation, write=True)
    tids = list(tid_list)
    if not tids:
        return True
    if len(set(tids)) != len(tids):
        raise RollbackError('Duplicate tuple IDs in batch delete')
    mgr = global_vars.xact_manager
    with mgr.storage_lock:
        before = global_vars.buffer_manager.get_page(relation, pageno).data
        page = SlotPage.unpack(before)
        if any(page.select(tid) == INVALID_BYTES for tid in tids):
            return False
        for tid in tids:
            if not page.delete(0, tid):
                raise RollbackError('Could not mark tuple deleted')
        # Never renumber live tuple IDs; index pointers and scan cursors stay valid.
        mgr.record_page_changes(relation, [(pageno, before, page.pack())])
    return True


def hot_simple_select(relation: Relation, pageno, tid):
    _relation_lock(relation)
    buffer_page = global_vars.buffer_manager.get_page(relation, pageno)
    data = buffer_page.page.select(tid)
    if data == INVALID_BYTES:
        return ()
    return TupleData.from_bytes(data, relation.attrs).python_tuple

def hot_simple_select_all(relation: Relation):
    _relation_lock(relation)
    for pageno in range(relation.last_pageno() + 1):
        buffer_page = global_vars.buffer_manager.get_page(relation, pageno)
        for tid in range(buffer_page.page.item_count):
            tuple_ = hot_simple_select(relation, pageno, tid)
            if tuple_ != ():
                yield tuple_

def bt_create_index(index_name, table_name, fields, database_oid=OID_DATABASE_ANDB):
    table_oid = CATALOG_ANDB_CLASS.get_relation_oid(table_name, database_oid, kind=RelationKinds.HEAP_TABLE)
    # only get index columns

    attr_form_array = CATALOG_ANDB_ATTRIBUTE.search(lambda r: r.class_oid == table_oid)
    index_attr_form_array = []
    for field in fields:
        for attr in attr_form_array:
            if attr.name == field:
                index_attr_form_array.append(attr)
    if len(index_attr_form_array) != len(fields):
        raise RollbackError('the index exists invalid field.')
    return bt_create_index_internal(index_name, table_oid, attr_form_array, index_attr_form_array,
                                    database_oid=OID_DATABASE_ANDB)


def bt_create_index_internal(index_name, table_oid, attr_form_array, index_attr_form_array,
                             database_oid=OID_DATABASE_ANDB):
    table_relation = open_relation(table_oid, lock_mode=rlock.SHARE_LOCK)
    if not table_relation:
        raise DDLException('cannot get the table.')

    # generate final index file
    index_oid = CATALOG_ANDB_CLASS.create(index_name, RelationKinds.BTREE_INDEX, database_oid)
    CATALOG_ANDB_INDEX.define_index_fields(name=index_name, index_oid=index_oid,
                                           table_oid=table_oid, table_attr_forms=index_attr_form_array)

    tree = BPlusTree()
    #TODO: fix this lsn
    lsn = global_vars.xact_manager.max_lsn()
    last_pageno = table_relation.last_pageno()
    # iteration includes the last pageno
    for pageno in range(0, last_pageno + 1):
        buffer_page = global_vars.buffer_manager.get_page(table_relation, pageno)
        global_vars.buffer_manager.pin_page(buffer_page)
        hot_page = buffer_page.page
        for idx in range(len(hot_page.item_ids)):
            tuple_data = hot_page.select(idx)
            if tuple_data == INVALID_BYTES:
                continue
            heap_tuple = TupleData.from_bytes(tuple_data, attr_form_array).python_tuple
            key_tuple = tuple(heap_tuple[attr.num] for attr in index_attr_form_array)
            key_data = TupleData(python_tuple=key_tuple).to_bytes(index_attr_form_array)
            tuple_pointer = TuplePointer(pageno, idx)
            tree.insert(lsn, key_data, tuple_pointer)
        global_vars.buffer_manager.unpin_page(buffer_page)

    close_relation(table_oid, lock_mode=rlock.SHARE_LOCK)
    fd = file_open(os.path.join(BASE_DIR, str(database_oid), str(index_oid)),
                   flags=os.O_RDWR | os.O_CREAT)
    file_write(fd, data=tree.serialize(), sync=True)
    return index_oid


def bt_drop_index(index_name, database_oid=OID_DATABASE_ANDB):
    results = CATALOG_ANDB_CLASS.search(lambda r: r.name == index_name and r.database_oid == database_oid)
    if len(results) == 0:
        raise DDLException('Not found the index name %s.' % index_name)

    class_form = results[0]
    index_oid = class_form.oid
    relation = open_relation(index_oid, rlock.ACCESS_EXCLUSIVE_LOCK)
    if not relation:
        raise DDLException('cannot drop the index because the index is in use.')

    CATALOG_ANDB_CLASS.delete(lambda r: r.name == index_name)
    CATALOG_ANDB_INDEX.delete(lambda r: r.oid == class_form.oid)

    file_remove(relation.fd)
    global_vars.buffer_manager.evict_relation(relation)
    close_relation(index_oid, rlock.ACCESS_EXCLUSIVE_LOCK)


def _bt_key_tuple_to_data(key_tuple, index_attr_form_array):
    key_data = TupleData(python_tuple=key_tuple).to_bytes(index_attr_form_array)
    return key_data


def _bt_data_to_key_tuple(data, index_attr_form_array):
    key_tuple = TupleData.from_bytes(data, index_attr_form_array).python_tuple
    return key_tuple


def bt_simple_insert(relation: Relation, key, tuple_pointer):
    _relation_lock(relation, write=True)
    with global_vars.xact_manager.storage_lock:
        tree = BufferedBPTree(relation)
        attrs = CATALOG_ANDB_INDEX.get_attr_form_array(relation.oid)
        key_data = _bt_key_tuple_to_data(key, attrs)
        if tuple_pointer in tree.search(key_data):
            return  # logical duplicate of the same physical tuple is not a new row
        tree.insert(0, key_data, tuple_pointer)
        tree.save_changes()


def bt_update(relation: Relation, key, tuple_pointer):
    """Replace all pointers for a key (legacy public API, not SQL row update)."""
    _relation_lock(relation, write=True)
    with global_vars.xact_manager.storage_lock:
        tree = BufferedBPTree(relation)
        attrs = CATALOG_ANDB_INDEX.get_attr_form_array(relation.oid)
        key_data = _bt_key_tuple_to_data(key, attrs)
        tree.delete(0, key_data)
        tree.insert(0, key_data, tuple_pointer)
        tree.save_changes()


def bt_delete(relation: Relation, key, tuple_pointer=None):
    _relation_lock(relation, write=True)
    with global_vars.xact_manager.storage_lock:
        tree = BufferedBPTree(relation)
        attrs = CATALOG_ANDB_INDEX.get_attr_form_array(relation.oid)
        key_data = _bt_key_tuple_to_data(key, attrs)
        if tuple_pointer is None:
            tree.delete(0, key_data)
        elif tuple_pointer in tree.search(key_data):
            tree.delete_value(0, key_data, tuple_pointer)
        tree.save_changes()


def bt_search(relation: Relation, key):
    _relation_lock(relation)
    """Allow leftmost prefix rule"""
    tree = BufferedBPTree(relation)
    attrs = CATALOG_ANDB_INDEX.get_attr_form_array(relation.oid)
    assert len(key) <= len(attrs)
    key_data = _bt_key_tuple_to_data(key, attrs[:len(key)])
    results = tree.search(key_data)
    return results


def bt_search_range(relation: Relation, start_key, end_key):
    _relation_lock(relation)
    """Allow leftmost prefix rule"""
    tree = BufferedBPTree(relation)
    attrs = CATALOG_ANDB_INDEX.get_attr_form_array(relation.oid)
    assert len(start_key) <= len(attrs)
    assert len(end_key) <= len(attrs)

    start_key_data = _bt_key_tuple_to_data(start_key, attrs[:len(attrs)])
    end_key_data = _bt_key_tuple_to_data(end_key, attrs[:len(attrs)])
    results = tree.search_range(start_key_data, end_key_data)
    return results


def bt_scan_all_keys(relation: Relation):
    _relation_lock(relation)
    tree = BufferedBPTree(relation)
    attrs = CATALOG_ANDB_INDEX.get_attr_form_array(relation.oid)
    for key_data in tree.all_keys():
        yield _bt_data_to_key_tuple(key_data, attrs)


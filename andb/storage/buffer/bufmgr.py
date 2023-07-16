from andb.common.file_operation import file_size, file_write, file_read, file_lseek
from andb.common.replacement.lru import LRUCache
from andb.common.utils import get_the_nearest_two_power_number, pageno_to_filesize
from andb.constants.values import PAGE_SIZE
from andb.runtime import global_vars
from andb.storage.common.page import Page
from andb.storage.engines.heap.bptree import BPlusTree, create_node
from andb.storage.engines.heap.relation import RelationKinds
from andb.storage.lock.lwlock import LWLockName, lwlock_acquire, lwlock_release

SIXTEEN_MB = 1024 * 1024 * 16


def get_next_allocation_size(v, upper=SIXTEEN_MB):
    sz = get_the_nearest_two_power_number(v) << 1
    if sz > upper:
        return upper
    return sz


class BufferPage:
    def __init__(self, relation, pageno):
        self.relation = relation
        self._page_data = None
        self._page = None
        self.pageno = pageno
        self._dirty = False

    def set_page(self, page):
        self._page = page

    def set_data(self, data):
        self._page_data = data

    @property
    def page(self):
        if not self._page:
            self._page = Page.unpack(self._page_data)
        return self._page

    @property
    def data(self):
        if not self._page_data:
            self._page_data = self._page.pack()
        return self._page_data

    def mark_dirty(self):
        self._dirty = True

    def erase_dirty(self):
        self._dirty = False

    @property
    def dirty(self):
        if self.relation.kind == RelationKinds.BTREE_INDEX:
            return self.page.dirty
        return self._dirty


def heap_read_page(relation, pageno):
    filesize = file_size(relation.fd)
    if pageno_to_filesize(pageno) >= filesize:
        return None
    offset = pageno * PAGE_SIZE
    file_lseek(relation.fd, offset)
    data = file_read(relation.fd, PAGE_SIZE)
    buffer_page = BufferPage(relation, pageno)
    buffer_page.set_data(data)
    return buffer_page


def heap_allocate_page(relation, pageno):
    # because pageno starts from 0, if this is the first allocation,
    # don't need to increase
    if pageno > 0:
        relation.increase_last_pageno()
    page = Page.allocate(lsn=global_vars.xact_manager.max_lsn())
    buffer_page = BufferPage(relation, pageno)
    buffer_page.set_page(page)
    buffer_page.mark_dirty()
    return buffer_page


def heap_write_page(buffer_page):
    pageno = buffer_page.pageno
    # todo: allocate page space ahead in disk using the following code, but
    # currently we cannot because insert action depends on the last pageno, we
    # doesn't implement a fsm mechanism yet.

    # fz = file_size(buffer_page.relation.fd)
    # if fz <= (PAGE_SIZE * pageno):
    #     file_extend(buffer_page.relation.fd, get_next_allocation_size(fz))
    file_lseek(buffer_page.relation.fd, pageno_to_filesize(pageno))
    # not sync
    file_write(buffer_page.relation.fd, buffer_page.data)


def bt_read_page(relation, pageno):
    header_size = BPlusTree.Header.size()
    filesize = file_size(relation.fd) - header_size
    if pageno_to_filesize(pageno) >= filesize:
        return None
    offset = pageno * PAGE_SIZE + header_size
    file_lseek(relation.fd, offset)
    data = file_read(relation.fd, PAGE_SIZE)
    buffer_page = BufferPage(relation, pageno)
    node = create_node(data)
    # BTree's node is page
    buffer_page.set_page(node)
    return buffer_page


def bt_allocate_page(relation, pageno):
    assert False, 'should not run here'


def bt_write_page(buffer_page):
    pageno = buffer_page.pageno
    header_size = BPlusTree.Header.size()
    file_lseek(buffer_page.relation.fd, pageno_to_filesize(pageno) + header_size)
    # not sync
    file_write(buffer_page.relation.fd, buffer_page.data)


_registry = {
    RelationKinds.HEAP_TABLE: {
        'read': heap_read_page,
        'allocate': heap_allocate_page,
        'write': heap_write_page
    },
    RelationKinds.BTREE_INDEX: {
        'read': bt_read_page,
        'allocate': bt_allocate_page,
        'write': bt_write_page
    }
}


class BufferManager:
    def __init__(self):
        self.cache = LRUCache(capacity=global_vars.buffer_pool_size)

    def get_page(self, relation, pageno) -> BufferPage:
        key = (relation, pageno)
        page = self.cache.get(key)
        if page is None:
            page = self._read_page_from_disk(relation, pageno)
            self.cache.put(key, page)
        return page

    def put_page(self, buffer_page):
        assert isinstance(buffer_page, BufferPage)
        key = (buffer_page.relation, buffer_page.pageno)
        return self.cache.put(key, buffer_page)

    @staticmethod
    def create_buffer_page(relation, pageno, page):
        assert not isinstance(page, BufferPage), 'purge page only'
        buffer_page = BufferPage(relation=relation, pageno=pageno)
        buffer_page.set_page(page)
        return buffer_page

    def evict_relation(self, relation):
        lwlock_acquire(LWLockName.BUFFER_UPDATE)
        keys = list(self.cache.keys())
        for key in keys:
            r, p = key
            if r == relation:
                self.cache.pop(key)
        lwlock_release(LWLockName.BUFFER_UPDATE)

    def pin_page(self, buffer_page):
        key = (buffer_page.relation, buffer_page.pageno)
        self.cache.pin(key)

    def unpin_page(self, buffer_page):
        key = (buffer_page.relation, buffer_page.pageno)
        self.cache.unpin(key)

    def sync(self):
        lwlock_acquire(LWLockName.BUFFER_UPDATE)
        for buffer_page in self.cache.items():
            if buffer_page.dirty:
                self._write_page_to_disk(buffer_page)
                buffer_page.erase_dirty()
        self.sync_evicted_pages()
        lwlock_release(LWLockName.BUFFER_UPDATE)

    def reset(self):
        # todo: sync ahead?
        self.cache.clear()

    def sync_evicted_pages(self):
        evicted = self.cache.get_evicted_list()
        for buffer_page in evicted:
            if buffer_page.dirty:
                self._write_page_to_disk(buffer_page)
                buffer_page.erase_dirty()
        evicted.clear()

    @staticmethod
    def _read_page_from_disk(relation, pageno):
        kind = relation.kind
        buffer_page = _registry[kind]['read'](relation, pageno)
        if buffer_page:
            return buffer_page
        return _registry[kind]['allocate'](relation, pageno)

    @staticmethod
    def _write_page_to_disk(buffer_page: BufferPage):
        kind = buffer_page.relation.kind
        return _registry[kind]['write'](buffer_page)

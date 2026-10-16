"""Buffer cache with coherent serialization and WAL-before-data writeback."""
import os
import threading
from contextlib import nullcontext

from andb.common.file_operation import file_size, file_pwrite, file_pread
from andb.common.replacement.lru import LRUCache
from andb.common.utils import get_the_nearest_two_power_number, pageno_to_filesize
from andb.constants.values import PAGE_SIZE
from andb.runtime import global_vars
from andb.storage.engines.heap.page import SlotPage
from andb.storage.engines.heap.bptree import BPlusTree, create_node
from andb.catalog.class_ import RelationKinds

SIXTEEN_MB = 16 * 1024 * 1024


def get_next_allocation_size(v, upper=SIXTEEN_MB):
    return min(get_the_nearest_two_power_number(v) << 1, upper)


class BufferPage:
    def __init__(self, relation, pageno):
        self.relation, self.pageno = relation, pageno
        self._page_data = self._page = None
        self._dirty = False

    def set_page(self, page):
        self._page = page
        self._page_data = None

    def set_data(self, data):
        self._page_data = data
        self._page = None

    @property
    def page(self):
        if self._page is None:
            self._page = SlotPage.unpack(self._page_data)
        return self._page

    @property
    def data(self):
        # A live mutable page is authoritative; cached bytes may be obsolete.
        return self._page.pack() if self._page is not None else self._page_data

    def mark_dirty(self):
        self._dirty = True
        if self._page is not None:
            self._page_data = None

    def erase_dirty(self):
        self._dirty = False
        if self.relation.kind == RelationKinds.BTREE_INDEX and self._page is not None:
            self._page.dirty = False

    @property
    def dirty(self):
        return self._dirty or (self.relation.kind == RelationKinds.BTREE_INDEX and self.page.dirty)

    def __repr__(self):
        return f'BufferPage(relation={self.relation}, pageno={self.pageno}, dirty={self.dirty})'


def heap_read_page(relation, pageno):
    if pageno * PAGE_SIZE >= file_size(relation.fd):
        return None
    data = file_pread(relation.fd, PAGE_SIZE, pageno * PAGE_SIZE)
    if len(data) != PAGE_SIZE:
        raise ValueError('Truncated heap page')
    page = BufferPage(relation, pageno)
    page.set_data(data)
    return page


def heap_allocate_page(relation, pageno):
    relation._last_pageno = max(relation._last_pageno or 0, pageno)
    page = BufferPage(relation, pageno)
    # Allocation is not a logged modification; don't skip redo with a future LSN.
    page.set_page(SlotPage.allocate(0))
    page.mark_dirty()
    return page


def heap_write_page(buffer_page):
    file_pwrite(buffer_page.relation.fd, buffer_page.data, pageno_to_filesize(buffer_page.pageno), sync=True)


def bt_read_page(relation, pageno):
    offset = BPlusTree.Header.size() + pageno * PAGE_SIZE
    if offset >= file_size(relation.fd):
        return None
    data = file_pread(relation.fd, PAGE_SIZE, offset)
    if len(data) != PAGE_SIZE:
        raise ValueError('Truncated index page')
    page = BufferPage(relation, pageno)
    page.set_page(create_node(data))
    return page


def bt_allocate_page(relation, pageno):
    raise ValueError(f'Index page {pageno} does not exist')


def bt_write_page(buffer_page):
    offset = BPlusTree.Header.size() + buffer_page.pageno * PAGE_SIZE
    file_pwrite(buffer_page.relation.fd, buffer_page.data, offset, sync=True)


_registry = {
    RelationKinds.HEAP_TABLE: {'read': heap_read_page, 'allocate': heap_allocate_page, 'write': heap_write_page},
    RelationKinds.BTREE_INDEX: {'read': bt_read_page, 'allocate': bt_allocate_page, 'write': bt_write_page},
}


class BufferManager:
    def __init__(self):
        self.cache = LRUCache(capacity=global_vars.buffer_pool_size)
        self._lock = threading.RLock()
        self.index_roots = {}  # relation -> (root pageno, dirty)
        self.index_highwater = {}
        self.heap_highwater = {}

    def get_page(self, relation, pageno):
        with self._lock:
            key = (relation, pageno)
            page = self.cache.get(key)
            if page is None:
                # A dirty evicted page is still newer than the disk copy.
                for candidate in list(self.cache.evicted):
                    if (candidate.relation, candidate.pageno) == key:
                        self.cache.evicted.remove(candidate)
                        page = candidate
                        break
                if page is None:
                    page = self._read_page_from_disk(relation, pageno)
                self.cache.put(key, page)
            if relation.kind == RelationKinds.HEAP_TABLE:
                self.heap_highwater[relation] = max(pageno, self.heap_highwater.get(relation, 0))
            return page

    def put_page(self, buffer_page):
        with self._lock:
            key = (buffer_page.relation, buffer_page.pageno)
            self.cache.evicted[:] = [p for p in self.cache.evicted if (p.relation, p.pageno) != key]
            self.cache.put(key, buffer_page)
            if buffer_page.relation.kind == RelationKinds.BTREE_INDEX:
                self.index_highwater[buffer_page.relation] = max(buffer_page.pageno + 1,
                    self.index_highwater.get(buffer_page.relation, 0),
                    max(0, (file_size(buffer_page.relation.fd) - BPlusTree.Header.size()) // PAGE_SIZE))
            if buffer_page.relation.kind == RelationKinds.HEAP_TABLE:
                self.heap_highwater[buffer_page.relation] = max(buffer_page.pageno,
                                                              self.heap_highwater.get(buffer_page.relation, 0))

    def mark_dirty(self, relation, pageno):
        self.get_page(relation, pageno).mark_dirty()

    @staticmethod
    def create_buffer_page(relation, pageno, page):
        result = BufferPage(relation, pageno)
        result.set_page(page)
        return result

    def get_index_root(self, relation):
        with self._lock:
            if relation not in self.index_roots:
                raw = file_pread(relation.fd, BPlusTree.Header.size(), 0)
                self.index_roots[relation] = (BPlusTree.deserialize_header(raw).root_pageno, False)
            return self.index_roots[relation][0]

    def set_index_root(self, relation, pageno):
        self.index_roots[relation] = (pageno, True)

    def allocate_index_pageno(self, relation):
        """Reserve a unique index page number, including pages not yet on disk.

        On first use, initialize the next-free high-water from the file page
        count and both resident and evicted buffer pages. Looking only at file
        size can reuse a number already assigned to an unflushed split page.
        Advance the high-water under _lock before returning. This reserves an
        address only; it neither creates a page nor journals/publishes a split.
        Reservations are not rolled back, so aborted allocations may leave gaps.
        """
        with self._lock:
            if relation not in self.index_highwater:
                disk_pages = max(0, (file_size(relation.fd) - BPlusTree.Header.size()) // PAGE_SIZE)
                cached = [p.pageno + 1 for p in list(self.cache.items()) + self.cache.evicted if p.relation == relation]
                self.index_highwater[relation] = max([disk_pages] + cached)
            number = self.index_highwater[relation]
            self.index_highwater[relation] += 1
            return number

    def evict_relation(self, relation):
        with self._lock:
            for key in list(self.cache.keys()):
                if key[0] == relation:
                    self.cache.pop(key)
            self.cache.evicted[:] = [p for p in self.cache.evicted if p.relation != relation]
            self.index_roots.pop(relation, None)
            self.index_highwater.pop(relation, None)
            self.heap_highwater.pop(relation, None)

    def pin_page(self, page):
        with self._lock:
            self.cache.pin((page.relation, page.pageno))

    def unpin_page(self, page):
        with self._lock:
            self.cache.unpin((page.relation, page.pageno))

    def _guard(self):
        return getattr(global_vars.xact_manager, 'storage_lock', nullcontext())

    def _flush_pages(self, pages):
        try:
            if any(p.dirty for p in pages):
                global_vars.xact_manager.wal_manager.wal_buffer_flush()
            for page in pages:
                if page.dirty:
                    self._write_page_to_disk(page)
                    page.erase_dirty()  # only after a successful durable write
        except Exception:
            global_vars.xact_manager.failed = True
            raise

    def sync(self):
        with self._guard(), self._lock:
            self._flush_pages(list(self.cache.evicted) + list(self.cache.items()))
            self.cache.evicted.clear()
            # Publish root pointers only after their referenced pages are durable.
            for relation, (root, dirty) in list(self.index_roots.items()):
                if dirty:
                    global_vars.xact_manager.wal_manager.wal_buffer_flush()
                    header = BPlusTree.Header()
                    header.root_pageno = root
                    try:
                        file_pwrite(relation.fd, header.pack(), 0, sync=True)
                    except Exception:
                        global_vars.xact_manager.failed = True
                        raise
                    self.index_roots[relation] = (root, False)

    def sync_evicted_pages(self):
        with self._guard(), self._lock:
            self._flush_pages(list(self.cache.evicted))
            self.cache.evicted.clear()

    def reset(self):
        with self._lock:
            self.cache.clear()
            self.index_roots.clear()
            self.index_highwater.clear()
            self.heap_highwater.clear()

    @staticmethod
    def _read_page_from_disk(relation, pageno):
        reader = _registry[relation.kind]
        return reader['read'](relation, pageno) or reader['allocate'](relation, pageno)

    @staticmethod
    def _write_page_to_disk(page):
        return _registry[page.relation.kind]['write'](page)

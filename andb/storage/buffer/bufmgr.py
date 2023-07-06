import os

from andb.runtime import global_vars
from andb.constants import filename
from andb.constants.values import PAGE_SIZE
from andb.common.replacement.lru import LRUCache
from andb.common.file_operation import file_size, file_extend, file_write, file_read, file_lseek
from andb.common.utils import get_the_nearest_two_power_number
from andb.storage.common.page import Page

PAGE_TYPE_HEAP = 'heap'
PAGE_TYPE_INDEX = 'index'

SIXTEEN_MB = 1024 * 1024 * 16


def get_next_allocation_size(v, upper=SIXTEEN_MB):
    sz = get_the_nearest_two_power_number(v) << 1
    if sz > upper:
        return upper
    return sz


class BufferPage:
    def __init__(self, relation, page, pageno):
        self.relation = relation
        self.page = page
        self.pageno = pageno
        self.ref = 0

    def ref_increase(self):
        self.ref += 1

    def ref_decrease(self):
        self.ref -= 1


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

    def clean_relation(self, relation):
        keys = list(self.cache.keys())
        for key in keys:
            r, p = key
            if r == relation:
                self.cache.pop(key)

    def pin_page(self, relation, pageno):
        key = (relation, pageno)
        self.cache.pin(key)

    def unpin_page(self, relation, pageno):
        key = (relation, pageno)
        self.cache.pin(key)

    def sync(self):
        for buffer_page in self.cache.items():
            self._write_page_to_disk(buffer_page)
        # self.cache = {}

    def sync_evicted_pages(self):
        evicted = self.cache.get_evicted_list()
        for buffer_page in evicted:
            self._write_page_to_disk(buffer_page)
        evicted.clear()

    @staticmethod
    def _read_page_from_disk(relation, pageno):
        filesize = file_size(relation.fd)
        assert pageno * PAGE_SIZE >= filesize
        if pageno * PAGE_SIZE == filesize:
            # need to allocate
            # todo: create_node of Btree
            # todo: use relation.lsn
            return BufferPage(relation, Page.allocate(), pageno)
        offset = pageno * PAGE_SIZE
        file_lseek(relation.fd, offset)
        data = file_read(relation.fd, PAGE_SIZE)
        page = Page.unpack(data)
        return BufferPage(relation, page, pageno)

    @staticmethod
    def _write_page_to_disk(buffer_page: BufferPage):
        page = buffer_page.page
        data = page.pack()
        pageno = buffer_page.pageno
        fz = file_size(buffer_page.relation.fd)
        if fz <= (PAGE_SIZE * pageno):
            file_extend(buffer_page.relation.fd, get_next_allocation_size(fz * 2))
        file_lseek(buffer_page.relation.fd, PAGE_SIZE * pageno)
        # not sync
        file_write(buffer_page.relation.fd, data)


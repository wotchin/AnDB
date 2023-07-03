import os

from andb.catalog.oid import INVALID_OID
from andb.common.file_operation import directio_file_open


class Relation:
    def __init__(self):
        self.oid = INVALID_OID
        self.name = None
        self.file_path = None
        self.refcount = 0
        self.is_index = False
        self.is_heap = False

    @property
    def fd(self):
        assert self.file_path
        return directio_file_open(self.file_path, os.O_RDWR | os.O_CREAT)

    def __repr__(self):
        if self.oid == INVALID_OID:
            return '<InvalidRelation>'
        return str(self.oid)

    def __hash__(self):
        return self.oid

    def __eq__(self, other):
        return (isinstance(other, Relation) and
                other.oid == self.oid)


def open_relation(oid, lock_mode=None):
    pass


def close_relation(oid, lock_mode=None):
    pass


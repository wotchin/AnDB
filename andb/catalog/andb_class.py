import os
import pickle

from andb.catalog.oid import OID_RELATION_START
from andb.constants.filename import CATALOG_DIR


class RelationKinds:
    HEAP_TABLE = 'h'
    BTREE_INDEX = 'b'


class AndbClassTuple:
    def __init__(self, oid, name, kind):
        self.oid = oid
        self.name = name
        self.kind = kind

    def __eq__(self, other):
        if not isinstance(other, AndbClassTuple):
            return False
        return (other.oid == self.oid and
                other.name == self.name and
                other.kind == self.kind)

    def __hash__(self):
        return hash((self.oid, self.name, self.kind))

    def __lt__(self, other):
        return self.oid < other.oid


class AndbClassTable:
    __tablename__ = 'andb_class'

    def __init__(self):
        self.rows = []

    def allocate_oid(self):
        if len(self.rows) == 0:
            return OID_RELATION_START
        return self.rows[-1].oid + 1

    def add(self, t):
        # todo: binary search
        self.rows.append(t)
        self.rows.sort()
        self.save()

    def load(self):
        filename = os.path.join(CATALOG_DIR, self.__tablename__)
        data = bytearray()
        with open(filename, 'rb') as f:
            while True:
                buff = f.read(256)
                if not buff:
                    break
                data += buff
        self.rows = pickle.loads(data)

    def save(self):
        filename = os.path.join(CATALOG_DIR, self.__tablename__)
        with open(filename, 'w+b') as f:
            f.write(pickle.dumps(self.rows))


ANDB_CLASS = AndbClassTable()



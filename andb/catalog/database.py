from ._base import CatalogTable, CatalogTuple
from .oid import OID_RELATION_END, OID_DATABASE_ANDB


class AndbDatabaseTuple(CatalogTuple):
    __fields__ = {
        'oid': 'bigint',
        'name': 'text',
    }

    def __init__(self, oid, name):
        self.oid = oid
        self.name = name

    def __lt__(self, other):
        return self.oid < other.oid


class AndbDatabaseTable(CatalogTable):
    def init(self):
        self.insert(AndbDatabaseTuple(
            oid=OID_DATABASE_ANDB,
            name='andb'
        ))

    def create(self, name):
        results = self.search(lambda r: r.name == name)
        if len(results) > 0:
            return False

        # todo: reuse deleted oid
        next_oid = self.rows[-1].oid + 1
        if next_oid > OID_RELATION_END:
            return False

        self.insert(AndbDatabaseTuple(
            oid=next_oid,
            name=name
        ))
        return True

    def drop(self, name):
        results = self.search(lambda r: r.name == name)
        if len(results) == 0:
            return False

        self.delete(lambda r: r.name == name)
        return True

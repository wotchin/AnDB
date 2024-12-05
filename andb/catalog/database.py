from ._base import CatalogTable, CatalogForm
from .oid import OID_RELATION_END, OID_DATABASE_ANDB, OID_SYSTEM_TABLE_DATABASE


class AndbDatabaseForm(CatalogForm):
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
    __tablename__ = 'andb_database'
    __oid__ = OID_SYSTEM_TABLE_DATABASE
    __form__ = AndbDatabaseForm
    def init(self):
        self.insert(AndbDatabaseForm(
            oid=OID_DATABASE_ANDB,
            name='andb'
        ))

    def create(self, name):
        results = self.search(lambda r: r.name == name)
        if len(results) > 0:
            return False

        #TODO: reuse deleted oid
        next_oid = self.rows[-1].oid + 1
        if next_oid > OID_RELATION_END:
            return False

        self.insert(AndbDatabaseForm(
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


_ANDB_DATABASE = AndbDatabaseTable()

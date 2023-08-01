from andb.catalog.oid import OID_RELATION_START
from ._base import CatalogTable, CatalogForm
from .oid import OID_RELATION_END, OID_DATABASE_ANDB, INVALID_OID
from .database import _ANDB_DATABASE
from andb.errno.errors import DDLException


class RelationKinds:
    HEAP_TABLE = 'h'
    BTREE_INDEX = 'b'


class AndbClassForm(CatalogForm):
    __fields__ = {
        'oid': 'bigint',
        'database_oid': 'bigint',
        'name': 'text',
        'kind': 'char'
    }

    def __init__(self, oid, name, kind, database_oid=OID_DATABASE_ANDB):
        self.oid = oid
        self.database_oid = database_oid
        self.name = name
        self.kind = kind

    def __lt__(self, other):
        return self.oid < other.oid


class AndbClassTable(CatalogTable):
    __tablename__ = 'andb_class'

    def init(self):
        pass

    def allocate_oid(self):
        if len(self.rows) == 0:
            return OID_RELATION_START
        return self.rows[-1].oid + 1

    def get_relation_oid(self, relation_name, database_oid=OID_DATABASE_ANDB, kind=RelationKinds.HEAP_TABLE):
        results = self.search(lambda r: r.name == relation_name
                                        and r.database_oid == database_oid
                                        and r.kind == kind)
        if len(results) != 1:
            return INVALID_OID

        return results[0].oid

    def exist_table(self, table_name, database_oid=OID_DATABASE_ANDB):
        return self.get_relation_oid(table_name, database_oid, RelationKinds.HEAP_TABLE) != INVALID_OID

    def exist_index(self, index_name, database_oid=OID_DATABASE_ANDB):
        return self.get_relation_oid(index_name, database_oid, RelationKinds.BTREE_INDEX) != INVALID_OID

    def create(self, name, kind, database_oid=OID_DATABASE_ANDB):
        next_oid = self.allocate_oid()
        if next_oid > OID_RELATION_END:
            return INVALID_OID

        results = _ANDB_DATABASE.search(lambda r: r.oid == database_oid)
        if len(results) == 0:
            return INVALID_OID

        if len(self.search(lambda r: r.name == name)) > 0:
            raise DDLException('Cannot create a same name relation.')

        self.insert(
            AndbClassForm(oid=next_oid, name=name,
                          kind=kind, database_oid=database_oid)
        )
        return next_oid


_ANDB_CLASS = AndbClassTable()

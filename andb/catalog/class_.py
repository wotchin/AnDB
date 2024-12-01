from andb.catalog.oid import OID_RELATION_START
from ._base import CatalogTable, CatalogForm
from .oid import OID_RELATION_END, OID_DATABASE_ANDB, INVALID_OID, OID_SYSTEM_TABLE_CLASS, OID_SYSTEM_TABLE_END, OID_SYSTEM_TABLE_START, OID_TEMP_TABLE, OID_MEMORY_TABLE_START, OID_MEMORY_TABLE_END
from .database import _ANDB_DATABASE
from andb.errno.errors import DDLException


class RelationKinds:
    HEAP_TABLE = 'h'
    BTREE_INDEX = 'b'
    SYSTEM_TABLE = 's'
    TEMPORARY_TABLE = 't'
    MEMORY_TABLE = 'm'


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
    __oid__ = OID_SYSTEM_TABLE_CLASS
    __form__ = AndbClassForm

    def __init__(self):
        super().__init__()
        # system tables and memory tables are not stored in rows
        # because they are not stored in disk.
        self.system_tables = []
        self.memory_tables = []

    def init(self):
        pass

    def allocate_oid(self, kind=RelationKinds.HEAP_TABLE):
        assert kind is not RelationKinds.SYSTEM_TABLE, "system table oid is fixed."
        assert kind is not RelationKinds.TEMPORARY_TABLE, "temporary table oid is fixed."

        if kind == RelationKinds.MEMORY_TABLE:
            if len(self.memory_tables) == 0:
                return OID_MEMORY_TABLE_START
            # memory table oid is allocated from OID_MEMORY_TABLE_START
            # to OID_MEMORY_TABLE_END consecutively.
            oid = self.memory_tables[-1].oid + 1
            if oid > OID_MEMORY_TABLE_END:
                raise DDLException('No more memory table oid can be allocated.')
            return oid
        elif kind in (RelationKinds.HEAP_TABLE, RelationKinds.BTREE_INDEX):
            if len(self.rows) == 0:
                return OID_RELATION_START
            # heap table oid is allocated from OID_RELATION_START to OID_RELATION_END
            # consecutively.
            oid = self.rows[-1].oid + 1
            if oid > OID_RELATION_END:
                raise DDLException('No more relation oid can be allocated.')
            return oid
        else:
            raise ValueError(f'Invalid relation kind: {kind}')

    def search(self, lambda_condition):
        assert callable(lambda_condition)
        results = []
        for r in self.rows:
            if lambda_condition(r):
                results.append(r)
        if len(results) > 0:
            # we think we have found the result from regular tables
            # then, we don't need to search system tables and memory tables
            return results
        
        for r in self.system_tables:
            if lambda_condition(r):
                results.append(r)
                # return early
                return results
        for r in self.memory_tables:
            if lambda_condition(r):
                results.append(r)
        return results

    def get_relation_oid(self, relation_name, database_oid=OID_DATABASE_ANDB,
                          kind=None):
        # if kind is not specified, we search all kinds of relations    
        if kind is None:
            results = self.search(lambda r: r.name == relation_name
                                    and r.database_oid == database_oid)
        else:
            results = self.search(lambda r: r.name == relation_name
                                    and r.database_oid == database_oid
                                    and r.kind == kind)
        if len(results) != 1:
            return INVALID_OID

        return results[0].oid

    def get_relation_kind(self, relation_oid):
        if relation_oid == OID_TEMP_TABLE:
            return RelationKinds.TEMPORARY_TABLE
        elif OID_SYSTEM_TABLE_START <= relation_oid <= OID_SYSTEM_TABLE_END:
            return RelationKinds.SYSTEM_TABLE
        elif OID_MEMORY_TABLE_START <= relation_oid <= OID_MEMORY_TABLE_END:
            return RelationKinds.MEMORY_TABLE
        elif OID_RELATION_START <= relation_oid <= OID_RELATION_END:
            return RelationKinds.HEAP_TABLE
        return None

    def exist_table(self, table_name, database_oid=OID_DATABASE_ANDB):
        return self.get_relation_oid(table_name, database_oid, RelationKinds.HEAP_TABLE) != INVALID_OID

    def exist_index(self, index_name, database_oid=OID_DATABASE_ANDB):
        return self.get_relation_oid(index_name, database_oid, RelationKinds.BTREE_INDEX) != INVALID_OID

    def create(self, name, kind, database_oid=OID_DATABASE_ANDB):
        assert kind not in (RelationKinds.TEMPORARY_TABLE, 
                            RelationKinds.MEMORY_TABLE, 
                            RelationKinds.SYSTEM_TABLE), \
        "temporary table, memory table, and system table cannot be created by this function."
        results = _ANDB_DATABASE.search(lambda r: r.oid == database_oid)
        if len(results) == 0:
            return INVALID_OID

        next_oid = self.allocate_oid(kind)
        if next_oid == INVALID_OID:
            raise DDLException('Relation oid cannot be allocated.')
        
        if len(self.search(lambda r: r.name == name)) > 0:
            raise DDLException('Cannot create a same name relation.')

        self.insert(
            AndbClassForm(oid=next_oid, name=name,
                          kind=kind, database_oid=database_oid)
        )
        return next_oid
    
    def create_non_persistent(self, name, kind, database_oid=OID_DATABASE_ANDB, table_oid=None):
        # for creating a memory table or system table information
        # for storing intermediate results
        assert kind in (RelationKinds.MEMORY_TABLE, 
                        RelationKinds.SYSTEM_TABLE), \
        "only memory table and system table can be created by this function."
        if table_oid is None:
            table_oid = self.allocate_oid(kind)
        if table_oid == INVALID_OID:
            raise DDLException('The oid for non-persistent relation cannot be allocated.') 
        
        # todo: binary search for optimization
        if kind == RelationKinds.MEMORY_TABLE:
            self.memory_tables.append(AndbClassForm(oid=table_oid, name=name,
                                                    kind=kind, database_oid=database_oid))
            self.memory_tables.sort()
        else:
            for table in self.system_tables:
                if table.oid == table_oid:
                    raise DDLException(
                        f'The oid {table_oid} for the system table {table.name} is already used.'
                    )
            self.system_tables.append(AndbClassForm(oid=table_oid, name=name,
                                                    kind=kind, database_oid=database_oid))
            self.system_tables.sort()

_ANDB_CLASS = AndbClassTable()

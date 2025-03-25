from collections import defaultdict
from andb.catalog.class_ import RelationKinds
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_CLASS
from andb.errno.errors import DDLException
from andb.runtime import global_vars

# Global memory table storage
_memory_tables = defaultdict(dict)  # {database_oid: {table_oid: MemoryTable}}

class MemoryTable:
    def __init__(self, oid, name, fields):
        self.oid = oid
        self.name = name
        self.fields = fields
        self.rows = []  # List of tuples

def memory_create_table(table_name, fields, database_oid):
    # Check memory limit
    if len(_memory_tables[database_oid]) >= global_vars.max_memory_tables:
        raise DDLException('Memory table limit exceeded.')
    
    # Create table metadata in catalog
    table_oid = CATALOG_ANDB_CLASS.create_non_persistent(
        name=table_name,
        kind=RelationKinds.MEMORY_TABLE,
        database_oid=database_oid
    )
    
    # Create memory table instance
    memory_table = MemoryTable(table_oid, table_name, fields)
    _memory_tables[database_oid][table_oid] = memory_table
    
    # Register fields in attribute catalog
    CATALOG_ANDB_ATTRIBUTE.define_table_fields(
        class_oid=table_oid, 
        fields=fields,
        persistent=False  # memory table is not persistent
    )
    
    return table_oid

def memory_drop_table(table_oid, database_oid):
    if database_oid not in _memory_tables or table_oid not in _memory_tables[database_oid]:
        raise DDLException("Temporary table not found")

    # Remove from memory
    del _memory_tables[database_oid][table_oid]

    # Remove metadata
    CATALOG_ANDB_ATTRIBUTE.delete(lambda r: r.class_oid == table_oid)
    CATALOG_ANDB_CLASS.delete_by_kind(RelationKinds.MEMORY_TABLE, table_oid)
    return True 

def memory_get_row_id(table_oid, database_oid, row):
    memory_table = _memory_tables[database_oid][table_oid]
    return memory_table.rows.index(row)

def memory_select_all(table_oid, database_oid):
    memory_table = _memory_tables[database_oid][table_oid]
    return memory_table.rows

def memory_select_by_id(table_oid, database_oid, row_id):
    memory_table = _memory_tables[database_oid][table_oid]
    return memory_table.rows[row_id]

def memory_insert(table_oid, database_oid, row):
    memory_table = _memory_tables[database_oid][table_oid]
    # convert list to tuple
    memory_table.rows.append(tuple(row))
    return len(memory_table.rows) - 1

def memory_delete(table_oid, database_oid, row_id):
    memory_table = _memory_tables[database_oid][table_oid]
    del memory_table.rows[row_id]
    return len(memory_table.rows)

def memory_delete_batch(table_oid, database_oid, row_ids):
    memory_table = _memory_tables[database_oid][table_oid]
    new_rows = []
    for i, row in enumerate(memory_table.rows):
        if i not in row_ids:
            new_rows.append(row)
    memory_table.rows = new_rows
    return len(memory_table.rows)

def memory_update(table_oid, database_oid, row_id, row):
    memory_table = _memory_tables[database_oid][table_oid]
    memory_table.rows[row_id] = tuple(row)

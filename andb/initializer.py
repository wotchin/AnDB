import os

from andb.catalog.oid import OID_DATABASE_ANDB
from andb.constants.filename import BASE_DIR, DATABASE_DIR
from andb.storage.buffer import BufferManager
from andb.storage.xact import TransactionManager
from andb.runtime import global_vars
from andb.storage.lock import lwlock
from andb.catalog.syscache import CATALOG_ANDB_ATTRIBUTE, get_all_catalogs, CATALOG_ANDB_CLASS
from andb.catalog.class_ import RelationKinds


def init_buffer_pool():
    global_vars.buffer_manager = BufferManager()


def init_runtime():
    pass


def init_storage():
    global_vars.xact_manager = TransactionManager()
    lwlock.init_lwlock()


def init_shared_resource():
    pass


def init_logger():
    pass

def init_catalog():
    for catalog_table in get_all_catalogs():
        catalog_table.load()
    
    # after all catalogs are loaded, create system tables and attributes
    # but these are not persistent tables
    for catalog_table in get_all_catalogs():
        CATALOG_ANDB_CLASS.create_non_persistent(
            catalog_table.__tablename__, RelationKinds.SYSTEM_TABLE, 
            database_oid=OID_DATABASE_ANDB, table_oid=catalog_table.__oid__
        )
        CATALOG_ANDB_ATTRIBUTE.define_table_fields(
            catalog_table.__oid__, [
                (name, type_name, False) 
                for name, type_name in catalog_table.__form__.__fields__.items()
            ], persistent=False
        )

def init_all_database_components(database_dir=None):
    init_buffer_pool()
    if database_dir is None:
        database_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    DATABASE_DIR)
    if not os.path.exists(database_dir):
        raise FileNotFoundError(f"Database directory {database_dir} does not exist.")
    global_vars.database_directory = database_dir
    os.chdir(global_vars.database_directory)
    init_storage()
    init_catalog()

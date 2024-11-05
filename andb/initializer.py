import os

from andb.constants.filename import BASE_DIR, DATABASE_DIR
from andb.storage.buffer import BufferManager
from andb.storage.xact import TransactionManager
from andb.runtime import global_vars
from andb.storage.lock import lwlock
from andb.catalog.syscache import get_all_catalogs


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

def init_all_database_components(database_dir=None):
    init_buffer_pool()
    if database_dir is None:
        database_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    DATABASE_DIR)
    global_vars.database_directory = database_dir
    os.chdir(global_vars.database_directory)
    init_storage()
    init_catalog()

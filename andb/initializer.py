from andb.storage.buffer import BufferManager
from andb.storage.engines.heap.redo import WALManager
from andb.runtime import global_vars
from andb.storage.lock import lwlock


def init_buffer_pool():
    global_vars.buffer_manager = BufferManager()


def init_runtime():
    pass


def init_storage():
    global_vars.lsn_manager = WALManager()
    lwlock.init_lwlock()


def init_shared_resource():
    pass


def init_logger():
    pass


def init_all_database_components():
    init_buffer_pool()
    init_storage()

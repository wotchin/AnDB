from andb.storage.buffer import BufferManager
from andb.storage.engines.heap.wal import LsnManager
from andb.runtime import global_vars


def init_buffer_pool():
    global_vars.buffer_manager = BufferManager()


def init_runtime():
    pass


def init_storage():
    global_vars.lsn_manager = LsnManager()


def init_shared_resource():
    pass


def init_logger():
    pass


def init_all_database_components():
    init_buffer_pool()
    init_storage()

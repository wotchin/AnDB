# The following set values maybe doesn't take effect
# because ConfigurationMgr sets them at starting.
# The following values are just for test.
import platform

database_directory = None
buffer_pool_size = 512
wal_buffer_size = 10
max_memory_tables = 100

unix_like_env = (platform.uname().system != 'Windows' and platform.uname().system != 'Darwin')

buffer_manager: 'BufferManager' = None
xact_manager = None

# The following set values maybe doesn't take effect
# because ConfigurationMgr sets them at starting.
# The following values are just for test.
import platform

data_directory = 'data'
buffer_pool_size = 512
wal_buffer_size = 10

unix_like_env = (platform.uname().system != 'Windows')

buffer_manager: 'BufferManager' = None
xact_manager = None

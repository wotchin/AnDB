# The following set values maybe doesn't take effect
# because ConfigurationMgr sets them at starting.
# The following values are just for test.
import platform

data_directory = 'data'
buffer_pool_size = 512

unix_like_env = (platform.uname().system != 'Windows')

buffer_manager: 'BufferManager' = None
lsn_manager = None

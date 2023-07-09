from builtins import (
    FileExistsError,
    ValueError,
    FileNotFoundError
)


class BufferOverflow(Exception):
    pass


class RollbackError(Exception):
    pass


class WALError(Exception):
    pass


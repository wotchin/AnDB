class AndbInternalError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.errno = 0
        self.msg = msg


class RollbackError(AndbInternalError):
    def __init__(self, msg):
        super().__init__(msg)
        self.errno = 1


class FatalError(AndbInternalError):
    def __init__(self, msg):
        super().__init__(msg)
        self.errno = 2


class BufferOverflow(FatalError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 10


class WALError(FatalError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 11


class DDLException(RollbackError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 20


class InitializationStageError(RollbackError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 21


class ExecutionStageError(RollbackError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 22


class FinalizationStageError(RollbackError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 23


class AnDBNotImplementedError(RollbackError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 24


class UndoError(RollbackError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 25


class ConfigError(RollbackError):
    def __init__(self, msg):
        super().__init__(msg)

        self.errno = 26

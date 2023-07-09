from enum import Enum
import threading


class LWLockName(Enum):
    BUFFER_UPDATE = 1
    WAL_WRITE = 2


_lwlock_instances = {}


class LWLock:
    def __init__(self):
        self.holding_thread = None
        self._lock = threading.Lock()

    def acquire(self, blocking=True, timeout=-1):
        return self._lock.acquire(blocking, timeout)

    def release(self):
        return self._lock.release()


def init_lwlock():
    for lock_name in LWLockName:
        _lwlock_instances[lock_name.value] = LWLock()


def lwlock_acquire(lwlock, blocking=True, timeout=-1):
    lock = _lwlock_instances[lwlock.value]
    rc = lock.acquire(blocking, timeout)
    lock.holding_thread = threading.get_ident()
    return rc


def lwlock_release(lwlock):
    lock = _lwlock_instances[lwlock.value]
    lock.holding_thread = None
    return lock.release()



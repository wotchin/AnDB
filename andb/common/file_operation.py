import os
import stat
import threading
from functools import wraps

from andb.constants.values import MAX_OPEN_FILES
from andb.common.replacement.lru import LRUCache
from andb.runtime.global_vars import unix_like_env

_file_lock = threading.RLock()


def _synchronized(fn):
    @wraps(fn)
    def locked(*args, **kwargs):
        with _file_lock:
            return fn(*args, **kwargs)
    return locked


INVALID_FD = -1

FILE_MODE = stat.S_IWUSR | stat.S_IRUSR


class FileDescriptor:
    def __init__(self, filepath, flags, mode):
        self.filepath = os.path.abspath(filepath)
        self.flags = flags
        self.mode = mode

        self.file_object = self._open()
        self.flags &= ~os.O_TRUNC

    def _open(self):
        fd_no = os.open(self.filepath, self.flags, self.mode)
        return os.fdopen(fd_no, 'wb+', 0)

    def close(self):
        if self.file_object.closed:
            return
        os.fsync(self.file_object.fileno())
        self.file_object.close()

    def reopen(self):
        if not self.file_object.closed:
            return
        self.file_object = self._open()


class SLRU(LRUCache):
    def __init__(self):
        super().__init__(MAX_OPEN_FILES)

    def get(self, key):
        v = super().get(key)
        if not v:
            return v
        # prevent unexpected exit
        v.reopen()
        return v


_FD_SLRU = SLRU()


@_synchronized
def file_open(filepath, flags, mode=FILE_MODE):
    """We use a simple LRU cache to avoid file descriptor leaks."""
    filepath = os.path.abspath(filepath)
    fd = _FD_SLRU.get(filepath)
    if fd:
        return fd
    fd = FileDescriptor(filepath, flags, mode)
    _FD_SLRU.put(filepath, fd)
    for evicted in _FD_SLRU.get_evicted_list():
        evicted.close()
    _FD_SLRU.get_evicted_list().clear()
    return fd


@_synchronized
def file_close(fd: FileDescriptor):
    _FD_SLRU.pop(fd.filepath)
    return fd.close()


def _resolve(fd):
    cached = _FD_SLRU.get(fd.filepath)
    if cached is not None:
        return cached
    fd.reopen()
    _FD_SLRU.put(fd.filepath, fd)
    for evicted in _FD_SLRU.get_evicted_list():
        evicted.close()
    _FD_SLRU.get_evicted_list().clear()
    return fd


@_synchronized
def file_write(fd: FileDescriptor, data: bytes, sync=False):
    fd = _resolve(fd)
    old_position = file_tell(fd)
    remaining = memoryview(data)
    n = 0
    while remaining:
        written = fd.file_object.write(remaining)
        if not written:
            raise OSError('Short file write')
        remaining = remaining[written:]
        n += written
    assert len(data) + old_position == file_tell(fd)
    if n >= 0 and sync:
        os.fsync(fd.file_object.fileno())
    return n


@_synchronized
def file_read(fd: FileDescriptor, n):
    fd = _resolve(fd)
    return fd.file_object.read(n)


@_synchronized
def file_lseek(fd: FileDescriptor, offset, whence=os.SEEK_SET):
    fd = _resolve(fd)
    return fd.file_object.seek(offset, whence)


@_synchronized
def file_tell(fd: FileDescriptor):
    fd = _resolve(fd)
    return fd.file_object.tell()


@_synchronized
def file_size(fd: FileDescriptor):
    fd = _resolve(fd)
    return os.fstat(fd.file_object.fileno()).st_size


@_synchronized
def file_extend(fd: FileDescriptor, size=1024):
    old_position = file_tell(fd)
    file_lseek(fd, 0, os.SEEK_END)
    #TODO: use stream?
    rv = file_write(fd, bytes(size), sync=True)
    file_lseek(fd, old_position, os.SEEK_SET)
    return rv


def directio_file_open(filepath, flags, mode=FILE_MODE):
    # Note: O_DIRECT requires page-aligned memory buffers for all I/O operations.
    # Python's built-in bytes/bytearray objects are not guaranteed to be aligned,
    # so O_DIRECT will cause EINVAL errors on Linux. Since we already use fsync()
    # for durability, O_DIRECT is not required. We use regular buffered I/O instead.
    return file_open(filepath, flags, mode)


@_synchronized
def file_remove(fd: FileDescriptor):
    file_close(fd)
    os.remove(fd.filepath)


@_synchronized
def file_touch(filepath, startup_size=0):
    fd = file_open(filepath, flags=os.O_RDWR | os.O_CREAT)
    if startup_size > 0:
        file_extend(fd, size=startup_size)
    file_close(fd)
    if os.name == 'posix':
        directory = os.open(os.path.dirname(os.path.abspath(filepath)), os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


@_synchronized
def file_pread(fd: FileDescriptor, n, offset):
    fd = _resolve(fd)
    if hasattr(os, 'pread'):
        return os.pread(fd.file_object.fileno(), n, offset)
    old = fd.file_object.tell()
    try:
        fd.file_object.seek(offset)
        return fd.file_object.read(n)
    finally:
        fd.file_object.seek(old)


@_synchronized
def file_pwrite(fd: FileDescriptor, data, offset, sync=False):
    fd = _resolve(fd)
    if hasattr(os, 'pwrite'):
        remaining, written = memoryview(data), 0
        while remaining:
            n = os.pwrite(fd.file_object.fileno(), remaining, offset + written)
            if not n:
                raise OSError('Short positional write')
            written += n
            remaining = remaining[n:]
        if sync:
            os.fsync(fd.file_object.fileno())
        return written
    old = fd.file_object.tell()
    try:
        fd.file_object.seek(offset)
        return file_write(fd, data, sync)
    finally:
        fd.file_object.seek(old)

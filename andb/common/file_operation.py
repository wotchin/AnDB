import os
import stat

from andb.constants.values import MAX_OPEN_FILES
from andb.common.replacement.lru import LRUCache
from andb.runtime.global_vars import unix_like_env

INVALID_FD = -1

FILE_MODE = stat.S_IWUSR | stat.S_IRUSR


class FileDescriptor:
    def __init__(self, fd, filepath, flags):
        self.file_object = os.fdopen(fd, 'wb+', 0)
        self.filepath = filepath
        self._flags = flags

    def close(self):
        if self.file_object.closed:
            return
        os.fsync(self.file_object.fileno())
        self.file_object.close()


_FD_SLRU = LRUCache(MAX_OPEN_FILES)


def file_open(filepath, flags, mode=FILE_MODE):
    """We use a simple LRU cache to avoid file descriptor leaks."""
    fd = _FD_SLRU.get(filepath)
    if fd:
        return fd
    fd = FileDescriptor(os.open(filepath, flags, mode),
                        filepath, flags)
    _FD_SLRU.put(filepath, fd)
    for evicted in _FD_SLRU.get_evicted_list():
        evicted.close()
    _FD_SLRU.get_evicted_list().clear()
    return fd


def file_close(fd: FileDescriptor):
    _FD_SLRU.pop(fd.filepath)
    return fd.close()


def file_write(fd: FileDescriptor, data: bytes, sync=False):
    old_position = file_tell(fd)
    n = fd.file_object.write(data)
    assert len(data) + old_position == file_tell(fd)
    if n >= 0 and sync:
        os.fsync(fd.file_object.fileno())
    return n


def file_read(fd: FileDescriptor, n):
    fd = _FD_SLRU.get(fd.filepath)
    return fd.file_object.read(n)


def file_lseek(fd: FileDescriptor, offset, whence=os.SEEK_SET):
    fd = _FD_SLRU.get(fd.filepath)
    return fd.file_object.seek(offset, whence)


def file_tell(fd: FileDescriptor):
    return file_lseek(fd, 0, os.SEEK_CUR)


def file_size(fd: FileDescriptor):
    old_position = file_tell(fd)
    tail_position = file_lseek(fd, 0, os.SEEK_END)
    file_lseek(fd, old_position, os.SEEK_SET)
    return tail_position


def file_extend(fd: FileDescriptor, size=1024):
    old_position = file_tell(fd)
    file_lseek(fd, 0, os.SEEK_END)
    # todo: use stream?
    rv = file_write(fd, bytes(size), sync=True)
    file_lseek(fd, old_position, os.SEEK_SET)
    return rv


def directio_file_open(filepath, flags, mode=FILE_MODE):
    if unix_like_env:
        flags |= os.O_DIRECT
    return file_open(filepath, flags, mode)


def file_remove(fd: FileDescriptor):
    file_close(fd)
    os.remove(fd.filepath)


def touch(path):
    with open(path, 'w+') as f:
        f.write('')

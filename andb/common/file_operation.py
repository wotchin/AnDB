import os
import stat

from andb.constants.values import MAX_OPEN_FILES
from andb.common.replacement.lru import LRUCache
from andb.runtime.global_vars import unix_like_env

INVALID_FD = -1

FILE_MODE = stat.S_IWUSR | stat.S_IRUSR


class FileDescriptor:
    def __init__(self, filepath, flags, mode):
        self.filepath = filepath
        self.flags = flags
        self.mode = mode

        self.file_object = self._open()

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


def file_open(filepath, flags, mode=FILE_MODE):
    """We use a simple LRU cache to avoid file descriptor leaks."""
    fd = _FD_SLRU.get(filepath)
    if fd:
        return fd
    fd = FileDescriptor(filepath, flags, mode)
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

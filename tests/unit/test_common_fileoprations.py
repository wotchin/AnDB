import os

from andb.common.file_operation import (
    file_open, file_write, file_read,
    file_lseek, file_close, file_remove, file_size, file_tell
)


def test_open_file():
    filename = 'abc'
    fd = file_open(filename, os.O_RDWR | os.O_CREAT)
    assert file_write(fd, b'hello world') == len(b'hello world')
    assert (file_read(fd, 8)) == b''
    assert (file_lseek(fd, 0, os.SEEK_SET)) == 0
    assert (file_read(fd, 8)) == b'hello wo'
    assert (file_tell(fd)) == 8
    assert (file_size(fd)) == len(b'hello world')
    assert (file_tell(fd)) == 8

    file_close(fd)
    file_remove(fd)

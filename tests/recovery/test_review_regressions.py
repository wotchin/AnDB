"""Regression guards for review-driven imports, shared FD lifetime and PGI1 reuse."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import struct
import subprocess
import sys
import threading
import zlib

import pytest

from andb.common import file_operation as files
from andb.constants.values import PAGE_SIZE
from andb.errno.errors import WALError
from andb.storage.engines.heap.journal import encode_images, decode_images
from andb.storage.engines.heap.redo import WALAction, WALRecord

ROOT = Path(__file__).resolve().parents[2]
IMPORTS = (
    'andb.catalog._base',
    'andb.catalog.class_',
    'andb.common.file_operation',
    'andb.storage.engines.heap.redo',
    'andb.storage.engines.heap.relation',
    'andb.storage.xact.mgr',
)


@pytest.mark.parametrize('first', IMPORTS)
def test_module_imports_work_in_a_fresh_process(first):
    # pytest has already imported much of the engine; only a fresh interpreter
    # can expose cycles introduced by moving a deferred import to module scope.
    code = 'import importlib\n' + '\n'.join(
        'importlib.import_module(%r)' % name for name in (first,) + IMPORTS)
    result = subprocess.run([sys.executable, '-c', code], cwd=ROOT,
                            env=dict(os.environ, PYTHONPATH=str(ROOT)),
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


@pytest.fixture
def isolated_fd_cache(monkeypatch):
    cache = files.SLRU()
    monkeypatch.setattr(files, '_FD_SLRU', cache)
    yield cache
    for descriptor in list(cache.items()) + cache.evicted:
        descriptor.close()
    cache.clear()


@pytest.mark.parametrize('operation', ['close', 'reopen'])
def test_direct_descriptor_lifecycle_obeys_the_io_lock(tmp_path, isolated_fd_cache, operation):
    fd = files.file_open(str(tmp_path / 'lifecycle'), os.O_RDWR | os.O_CREAT)
    if operation == 'reopen':
        fd.close()
    attempted, completed = threading.Event(), threading.Event()

    def change_lifetime():
        attempted.set()
        getattr(fd, operation)()
        completed.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        with files._file_lock:
            future = pool.submit(change_lifetime)
            assert attempted.wait(5), 'lifecycle worker did not start'
            finished_during_io = completed.wait(0.1)
        future.result(timeout=5)
    assert not finished_during_io, 'direct %s bypassed the shared I/O lock' % operation
    assert completed.is_set()


def test_concurrent_open_shares_one_cached_descriptor(tmp_path, isolated_fd_cache):
    start = threading.Barrier(8)

    def open_same_file(_):
        start.wait(timeout=5)
        return files.file_open(str(tmp_path / 'shared'), os.O_RDWR | os.O_CREAT)

    with ThreadPoolExecutor(max_workers=8) as pool:
        handles = list(pool.map(open_same_file, range(8)))
    assert all(fd is handles[0] for fd in handles)
    assert len(isolated_fd_cache.cache) == 1


@pytest.mark.parametrize('positional', [False, True])
def test_short_writes_use_views_without_losing_bytes(tmp_path, isolated_fd_cache, monkeypatch, positional):
    fd = files.file_open(str(tmp_path / 'short'), os.O_RDWR | os.O_CREAT)
    payload = b'short-write-payload'
    real_file = fd.file_object
    chunks = []

    def short_write(view):
        assert isinstance(view, memoryview)
        assert view.obj is payload  # Slicing must not copy the remaining payload.
        chunks.append(len(view))
        return real_file.write(view[:3])

    class ShortWriter:
        def __getattr__(self, name):
            return getattr(real_file, name)

        write = staticmethod(short_write)

    if positional:
        # Exercise native positional I/O with forced positive short writes.
        offsets = []
        def short_pwrite(number, view, offset):
            assert isinstance(view, memoryview) and view.obj is payload
            offsets.append(offset)
            chunks.append(len(view))
            # A separate cursor here only emulates the kernel's pwrite for this test.
            old = real_file.tell()
            try:
                real_file.seek(offset)
                return real_file.write(view[:3])
            finally:
                real_file.seek(old)
        monkeypatch.setattr(files.os, 'pwrite', short_pwrite, raising=False)
        assert files.file_pwrite(fd, payload, 4, sync=True) == len(payload)
        assert offsets == list(range(4, 4 + len(payload), 3))
        assert files.file_tell(fd) == 0
        assert files.file_pread(fd, len(payload), 4) == payload
    else:
        fd.file_object = ShortWriter()
        assert files.file_write(fd, payload, sync=True) == len(payload)
        assert files.file_tell(fd) == len(payload)
        assert files.file_pread(fd, len(payload), 0) == payload
    assert len(chunks) > 1


def test_positional_fallback_keeps_cursor(tmp_path, isolated_fd_cache, monkeypatch):
    fd = files.file_open(str(tmp_path / 'fallback'), os.O_RDWR | os.O_CREAT)
    files.file_write(fd, b'0123456789')
    files.file_lseek(fd, 2)
    monkeypatch.delattr(files.os, 'pwrite', raising=False)
    monkeypatch.delattr(files.os, 'pread', raising=False)
    assert files.file_pwrite(fd, b'ABC', 5, sync=True) == 3
    assert files.file_tell(fd) == 2
    assert files.file_pread(fd, 10, 0) == b'01234ABC89'
    assert files.file_tell(fd) == 2


@pytest.mark.parametrize('heap,root,undo_of', [
    (True, None, 0), (True, None, 123456),
    (False, (0, 7), 0), (False, (7, 0), 987654),
])
def test_page_image_payload_keeps_existing_pgi1_bytes(heap, root, undo_of):
    # An independent frozen wire layout prevents a serializer refactor from
    # silently changing durable WAL bytes while still passing roundtrip tests.
    pages = [(0, b'b' * PAGE_SIZE, b'a' * PAGE_SIZE),
             (7, None, b'n' * PAGE_SIZE)]
    old_root, new_root = (0xffffffff, 0xffffffff) if root is None else root
    body = struct.pack('>4sBIIIQ', b'PGI1', int(heap), len(pages), old_root, new_root, undo_of)
    for number, before, after in pages:
        before = b'' if before is None else before
        body += struct.pack('>III', number, len(before), len(after)) + before + after
    expected = struct.pack('>I', zlib.crc32(body)) + body
    encoded = encode_images(heap, pages, root, undo_of)
    assert isinstance(encoded, bytes)
    assert encoded == expected
    assert decode_images(expected) == (heap, pages, root, undo_of)
    action = WALAction.COMPENSATION if undo_of else WALAction.PAGE_UPDATE
    record = WALRecord(1, 100, 0, 0, action, encoded)
    assert decode_images(WALRecord.unpack(record.pack()).data) == (heap, pages, root, undo_of)


@pytest.mark.parametrize('damage', ['checksum', 'truncate', 'duplicate', 'root', 'trailing'])
def test_page_image_envelope_still_rejects_invalid_payloads(damage):
    pages = [(0, None, b'x' * PAGE_SIZE)]
    root = None
    if damage == 'duplicate':
        pages *= 2
    elif damage == 'root':
        root = (0, 1)  # Heap records cannot publish an index root.
    encoded = encode_images(True, pages, root)
    if damage == 'checksum':
        encoded = encoded[:-1] + bytes([encoded[-1] ^ 1])
    elif damage == 'truncate':
        encoded = encoded[:8]
    elif damage == 'trailing':
        # Valid CRC must not hide extra bytes outside the declared page list.
        body = encoded[4:] + b'trailing'
        encoded = struct.pack('>I', zlib.crc32(body)) + body
    with pytest.raises(WALError):
        decode_images(encoded)

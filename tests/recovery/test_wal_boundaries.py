"""WAL boundary, tail, append-position and fail-stop regression tests."""
import os
from pathlib import Path

import pytest

from andb.errno.errors import WALError
from andb.storage.engines.heap import redo
from andb.storage.engines.heap.redo import WALManager, WALRecord, WALAction, WALPage


@pytest.fixture
def wal_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path('wal').mkdir()
    # Exercise segment switches without allocating megabytes for each test.
    monkeypatch.setattr(redo, 'WAL_SEGMENT_SIZE', 2 * redo.WAL_PAGE_SIZE)
    return Path('wal')


def record(i, data=b'', action=WALAction.HEAP_INSERT):
    return WALRecord(i, 100, 0, i, action, data)


@pytest.mark.parametrize('size', [0, 1, 8060, 8107, 8108, 8109, 8143, 8144, 8145, 16384, 33000])
def test_fragment_page_segment_and_exact_resume(wal_dir, size):
    writer = WALManager()
    records = [record(1, b'prefix'), record(2, b'x' * size), record(3, b'suffix')]
    ends = [writer.write_record(r) for r in records]
    writer.wal_buffer_flush()
    assert writer.flush_lsn == writer.write_lsn == ends[-1]
    original = {p.name: p.read_bytes() for p in wal_dir.iterdir()}
    writer.wal_buffer_flush()  # flushing zero new bytes is a true no-op
    assert {p.name: p.read_bytes() for p in wal_dir.iterdir()} == original
    loaded = list(WALManager.replay(0))
    assert [r.data for r in loaded] == [r.data for r in records]
    assert [r.end_lsn for r in loaded] == ends
    for i, end in enumerate(ends):
        assert [r.data for r in WALManager.replay(end)] == [r.data for r in records[i + 1:]]
    fresh = WALManager()
    assert fresh.restore() == ends[-1]
    fresh.write_record(record(4, b'after restart', WALAction.COMMIT))
    assert [r.data for r in WALManager.replay(0)] == [r.data for r in records] + [b'after restart']


@pytest.mark.parametrize('cut', [0, 1, 3, 8, 20, 35, 36, 37, 100, 8100, 8144, 8192, 8200, 16000, 20000])
def test_incomplete_logical_tail_is_discarded_before_append(wal_dir, cut):
    writer = WALManager()
    prefix = record(1, b'winner', WALAction.COMMIT)
    writer.write_record(prefix)
    writer.write_record(record(2, b'z' * 22000))
    writer.wal_buffer_flush()
    absolute_cut = prefix.end_lsn + cut
    for file in list(wal_dir.iterdir()):
        base = int(file.name, 16) * redo.WAL_SEGMENT_SIZE
        if base >= absolute_cut:
            file.unlink()
        elif base + file.stat().st_size > absolute_cut:
            with file.open('r+b') as stream:
                stream.truncate(absolute_cut - base)
    assert [r.data for r in WALManager.replay(0)] == [b'winner']
    restored = WALManager()
    assert restored.restore() == prefix.end_lsn
    restored.write_record(record(3, b'new winner', WALAction.COMMIT))
    assert [r.data for r in WALManager.replay(0)] == [b'winner', b'new winner']


def test_zero_preallocated_tail_is_not_append_position(wal_dir):
    writer = WALManager()
    end = writer.write_record(record(1, b'winner', WALAction.COMMIT))
    file = wal_dir / '0000000000000000'
    with file.open('ab') as stream:
        stream.write(bytes(redo.WAL_SEGMENT_SIZE - end))
    restored = WALManager()
    assert restored.restore() == end
    assert file.stat().st_size == end
    restored.write_record(record(2, b'new', WALAction.COMMIT))
    assert [r.data for r in WALManager.replay(0)] == [b'winner', b'new']


def test_missing_middle_segment_fails_closed(wal_dir):
    writer = WALManager()
    writer.write_record(record(1, b'x' * 50000, WALAction.COMMIT))
    (wal_dir / '0000000000000001').unlink()
    with pytest.raises(WALError, match='segment missing'):
        list(WALManager.replay(0))


def test_flush_failure_does_not_advance_durable_lsn(wal_dir, monkeypatch):
    writer = WALManager()
    writer.write_record(record(1, b'data'))
    def fail(_):
        raise OSError('injected fsync failure')
    monkeypatch.setattr(redo.os, 'fsync', fail)
    with pytest.raises(OSError, match='injected'):
        writer.wal_buffer_flush()
    assert writer.flush_lsn == 0
    with pytest.raises(WALError, match='restart'):
        writer.write_record(record(2, b'not allowed'))


def test_rejected_oversized_record_does_not_leak_lock(wal_dir, monkeypatch):
    writer = WALManager()
    monkeypatch.setattr(writer, 'MAX_RECORD_SIZE', 10)
    with pytest.raises(WALError):
        writer.write_record(record(1, b'x' * 11))
    assert writer.write_lsn == 0
    writer.write_record(record(2, b'ok', WALAction.COMMIT))
    assert [r.data for r in writer.replay(0)] == [b'ok']


def test_padding_is_serialized_before_pack(wal_dir):
    page = WALPage(0, 0)
    r = record(1, bytes(redo.WAL_PAGE_SIZE - WALPage.Header.size() - WALRecord.header_size() - 7))
    assert page.append_record(r)
    decoded = WALRecord.unpack(bytes(page.records[:r.total_size]))
    assert decoded._header.padding_size == 7
    assert WALPage.unpack(page.pack()).pack() == page.pack()


def test_zero_tail_before_later_segment_is_a_hole_not_end_of_log(wal_dir):
    writer = WALManager()
    writer.write_record(record(1, b'x' * 50000, WALAction.COMMIT))
    file = wal_dir / '0000000000000000'
    with file.open('r+b') as stream:
        stream.seek(redo.WAL_PAGE_SIZE + WALPage.Header.size())
        stream.write(bytes(redo.WAL_PAGE_SIZE - WALPage.Header.size()))
    with pytest.raises(WALError, match='Hole'):
        list(WALManager.replay(0))

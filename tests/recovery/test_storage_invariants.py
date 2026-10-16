from pathlib import Path
from types import SimpleNamespace

import pytest

from andb.catalog.class_ import RelationKinds
from andb.storage.buffer.bufmgr import BufferPage
from andb.storage.engines.heap.page import SlotPage
from tests.recovery.test_crash_recovery import run_db, schema


def test_serialized_buffer_is_invalidated_after_mutation():
    bp = BufferPage(SimpleNamespace(kind=RelationKinds.HEAP_TABLE), 0)
    bp.set_page(SlotPage.allocate(0))
    bp.page.insert(1, b'original')
    old_serialization = bp.data
    bp.page.insert(2, b'new data')
    bp.mark_dirty()
    assert bp.data != old_serialization
    assert SlotPage.unpack(bp.data).select(1) == b'new data'
    # Same check for a page initially loaded from bytes (not allocated in memory).
    bp.set_data(bp.data)
    bp.page.update(3, 0, b'modified')
    bp.mark_dirty()
    assert SlotPage.unpack(bp.data).select(0) == b'modified'


def test_descriptor_eviction_does_not_truncate_reopened_file(tmp_path, monkeypatch):
    from andb.common import file_operation as f
    monkeypatch.setattr(f._FD_SLRU, 'capacity', 2)
    first = f.file_open(str(tmp_path / 'first'), f.os.O_RDWR | f.os.O_CREAT | f.os.O_TRUNC)
    f.file_write(first, b'original', sync=True)
    for i in range(8):
        fd = f.file_open(str(tmp_path / str(i)), f.os.O_RDWR | f.os.O_CREAT)
        f.file_write(fd, b'other')
    assert f.file_pread(first, 8, 0) == b'original'
    f.file_pwrite(first, b' plus', 8, sync=True)
    assert (tmp_path / 'first').read_bytes() == b'original plus'
    f.file_close(first)


def test_wal_force_precedes_every_dirty_data_page_write(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        sql('begin')
        sql("insert into t values (1, 'uncommitted')")
        writer, bm = global_vars.xact_manager.wal_manager, global_vars.buffer_manager
        assert writer.write_lsn > writer.flush_lsn
        original = bm._write_page_to_disk
        checked = []
        def check(page):
            lsn = page.page.header.lsn if page.relation.is_heap else page.page.lsn
            assert lsn <= writer.flush_lsn <= writer.write_lsn
            checked.append(lsn)
            return original(page)
        bm._write_page_to_disk = check
        bm.sync()
        assert checked
        sql('rollback')
    ''')


def test_data_write_failure_stops_engine_until_recovery(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        from andb.errno.errors import WALError
        sql("insert into t values (1, 'winner')")
        sql('begin')
        sql("update t set v = 'loser' where id = 1")
        bm = global_vars.buffer_manager
        def fail(page):
            raise OSError('injected data write error')
        bm._write_page_to_disk = fail
        try:
            bm.sync()
        except OSError:
            pass
        else:
            assert False
        assert global_vars.xact_manager.failed
        try:
            sql('commit')
        except WALError:
            pass
        else:
            assert False, 'cannot acknowledge commit after an I/O failure'
        os._exit(0)
    ''')
    run_db(path, "assert sql('select * from t').tuples == [(1, 'winner')]")


def test_page_image_corruption_is_not_silently_skipped(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        from andb.storage.engines.heap.redo import WALManager, WALAction, WALRecord, WAL_SEGMENT_SIZE, lsn_to_filename
        sql("insert into t values (1, 'winner')")
        record = next(r for r in WALManager.replay(0) if r.action == WALAction.PAGE_UPDATE)
        offset = record.start_lsn + WALRecord.header_size() + 4
        with open(os.path.join('wal', lsn_to_filename(offset)), 'r+b', buffering=0) as stream:
            stream.seek(offset % WAL_SEGMENT_SIZE)
            byte = stream.read(1)
            stream.seek(offset % WAL_SEGMENT_SIZE)
            stream.write(bytes([byte[0] ^ 1]))
            os.fsync(stream.fileno())
        os._exit(0)
    ''')
    result = run_db(path, 'assert False, "corrupt recovery must not reach queries"', expected=1)
    assert 'checksum mismatch' in result.stderr


def test_xid_highwater_and_committed_csn_restored(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        from pathlib import Path
        sql('begin')
        xid = global_vars.xact_manager.get_xid()
        sql("insert into t values (1, 'winner')")
        sql('commit')
        global_vars.xact_manager.checkpoint()
        Path('old-xid').write_text(str(xid))
        os._exit(0)
    ''')
    run_db(path, '''
        from pathlib import Path
        old = int(Path('old-xid').read_text())
        manager = global_vars.xact_manager
        assert manager.csn_manager.is_committed(old)
        sql('begin')
        assert manager.get_xid() > old
        sql('rollback')
    ''')

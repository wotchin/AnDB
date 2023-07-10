from andb.constants.values import WAL_SEGMENT_SIZE
from andb.storage.engines.heap.redo import lsn_to_filename, WALManager, WALRecord, WALAction


def test_wal():
    assert (lsn_to_filename(1)) == '0000000000000000'
    assert (lsn_to_filename(WAL_SEGMENT_SIZE * 2)) == '0000000000000002'
    assert (lsn_to_filename(WAL_SEGMENT_SIZE * 2 + 1)) == '0000000000000002'


def test_wal_manager():
    manager = WALManager()
    for i in range(50000):
        record = WALRecord(i, action=WALAction.HEAP_INSERT, data=(str(i) * 100).encode())
        manager.write_record(record)
        if i % 5 == 0:
            commit_record = WALRecord(xid=i, action=WALAction.COMMIT, data=b'')
            manager.write_record(commit_record)
            if manager.flush_lsn != manager.write_lsn:
                assert False

            assert manager.flush_lsn == manager.write_lsn

    commit_record = WALRecord(xid=100, action=WALAction.COMMIT, data=b'')
    manager.write_record(commit_record)
    assert manager.flush_lsn == manager.write_lsn

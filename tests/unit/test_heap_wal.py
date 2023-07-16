from andb.constants.values import WAL_SEGMENT_SIZE
from andb.storage.engines.heap.redo import lsn_to_filename, WALManager, WALRecord, WALAction


def test_wal():
    assert (lsn_to_filename(1)) == '0000000000000000'
    assert (lsn_to_filename(WAL_SEGMENT_SIZE * 2)) == '0000000000000002'
    assert (lsn_to_filename(WAL_SEGMENT_SIZE * 2 + 1)) == '0000000000000002'


def a_test_wal_manager():
    manager = WALManager()
    iterations = 50000  # 50000

    test2_lsn = 0
    for i in range(iterations):
        record = WALRecord(i, action=WALAction.HEAP_INSERT, data=(str(i) * 100).encode())
        manager.write_record(record)
        if i % 5 == 0:
            commit_record = WALRecord(xid=i, action=WALAction.COMMIT, data=b'')
            manager.write_record(commit_record)
            assert manager.flush_lsn == manager.write_lsn
            if i == 0:
                test2_lsn = manager.flush_lsn

    commit_record = WALRecord(xid=100, action=WALAction.COMMIT, data=b'')
    manager.write_record(commit_record)
    assert manager.flush_lsn == manager.write_lsn

    # test replay
    record_generator = manager.replay(0)
    for i in range(iterations):
        record = next(record_generator)
        assert record.header.xid == i
        assert record.header.action == WALAction.HEAP_INSERT
        assert record.data == (str(i) * 100).encode()

        if i % 5 == 0:
            commit_record = next(record_generator)
            assert commit_record.header.xid == i
            assert commit_record.header.action == WALAction.COMMIT
            assert commit_record.data == b''

    commit_record = next(record_generator)
    assert commit_record.header.xid == 100
    assert commit_record.header.action == WALAction.COMMIT
    assert commit_record.data == b''

    # test replay
    record_generator = manager.replay(test2_lsn)
    for i in range(1, iterations):
        record = next(record_generator)
        assert record.header.xid == i
        assert record.header.action == WALAction.HEAP_INSERT
        assert record.data == (str(i) * 100).encode()

        if i % 5 == 0:
            commit_record = next(record_generator)
            assert commit_record.header.xid == i
            assert commit_record.header.action == WALAction.COMMIT
            assert commit_record.data == b''

    commit_record = next(record_generator)
    assert commit_record.header.xid == 100
    assert commit_record.header.action == WALAction.COMMIT
    assert commit_record.data == b''

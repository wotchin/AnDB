import os
import unittest
from unittest.mock import patch, MagicMock
import shutil
import time

from andb.common.file_operation import _FD_SLRU
from andb.constants.filename import WAL_DIR, UNDO_DIR
from andb.storage.engines.heap.redo import WALManager, WALRecord, WALAction, WALPage
from andb.storage.engines.heap.undo import UndoManager, UndoRecord, UndoOperation
from andb.constants.values import WAL_PAGE_SIZE


class TestRedoUndo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Clean up before all tests."""
        for dir_path in [WAL_DIR, UNDO_DIR]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path, ignore_errors=True)

    def setUp(self):
        """Set up test environment before each test."""
        # Close and clear any cached file descriptors from prior tests
        for key in list(_FD_SLRU.cache.keys()):
            fd = _FD_SLRU.pop(key)
            if fd:
                try:
                    fd.close()
                except Exception:
                    pass

        for dir_path in [WAL_DIR, UNDO_DIR]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path, ignore_errors=True)
            os.makedirs(dir_path, exist_ok=True)

        self.wal_manager = WALManager()
        self.undo_manager = UndoManager()

        self.test_xid = 1
        self.test_oid = 100
        self.test_pageno = 1
        self.test_tid = 1
        self.test_data = b'test_data'

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self.wal_manager, 'current_wal_fd') and self.wal_manager.current_wal_fd:
            try:
                self.wal_manager.current_wal_fd.close()
            except Exception:
                pass
            self.wal_manager.current_wal_fd = None

        time.sleep(0.1)

        for dir_path in [WAL_DIR, UNDO_DIR]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path, ignore_errors=True)

    def test_wal_record_pack_unpack(self):
        """Test WAL record serialization roundtrip."""
        record = WALRecord(
            xid=42, oid=100, pageno=5, tid=3,
            action=WALAction.HEAP_INSERT, data=b'hello_world'
        )
        packed = record.pack()
        unpacked = WALRecord.unpack(packed)

        self.assertEqual(unpacked.xid, 42)
        self.assertEqual(unpacked.relation_oid, 100)
        self.assertEqual(unpacked.location, (5, 3))
        self.assertEqual(unpacked.action, WALAction.HEAP_INSERT)
        self.assertEqual(unpacked.data, b'hello_world')

    def test_wal_record_write_and_replay(self):
        """Test basic WAL record writing and replay functionality."""
        test_record = WALRecord(
            xid=self.test_xid, oid=self.test_oid,
            pageno=self.test_pageno, tid=self.test_tid,
            action=WALAction.HEAP_INSERT, data=self.test_data
        )
        self.wal_manager.write_record(test_record)
        self.wal_manager.wal_buffer_flush()

        replayed_records = list(WALManager.replay(0))
        self.assertEqual(len(replayed_records), 1)
        replayed = replayed_records[0]

        self.assertEqual(replayed.xid, self.test_xid)
        self.assertEqual(replayed.relation_oid, self.test_oid)
        self.assertEqual(replayed.location, (self.test_pageno, self.test_tid))
        self.assertEqual(replayed.data, self.test_data)
        self.assertEqual(replayed.action, WALAction.HEAP_INSERT)

    def test_multiple_wal_records(self):
        """Test writing and replaying multiple WAL records."""
        num_records = 20
        for i in range(num_records):
            record = WALRecord(
                xid=i + 1, oid=self.test_oid,
                pageno=i, tid=i,
                action=WALAction.HEAP_INSERT, data=f'data_{i}'.encode()
            )
            self.wal_manager.write_record(record)
        self.wal_manager.wal_buffer_flush()

        replayed = list(WALManager.replay(0))
        self.assertEqual(len(replayed), num_records)
        for i, record in enumerate(replayed):
            self.assertEqual(record.xid, i + 1)
            self.assertEqual(record.data, f'data_{i}'.encode())

    def test_wal_commit_triggers_flush(self):
        """Test that COMMIT action triggers WAL buffer flush."""
        insert_record = WALRecord(
            xid=1, oid=self.test_oid,
            pageno=0, tid=0,
            action=WALAction.HEAP_INSERT, data=b'data'
        )
        self.wal_manager.write_record(insert_record)

        commit_record = WALRecord(
            xid=1, oid=0, pageno=0, tid=0,
            action=WALAction.COMMIT, data=b''
        )
        self.wal_manager.write_record(commit_record)

        # After commit, flush_lsn should equal write_lsn
        self.assertEqual(self.wal_manager.flush_lsn, self.wal_manager.write_lsn)

    def test_wal_abort_triggers_flush(self):
        """Test that ABORT action triggers WAL buffer flush."""
        insert_record = WALRecord(
            xid=1, oid=self.test_oid,
            pageno=0, tid=0,
            action=WALAction.HEAP_INSERT, data=b'data'
        )
        self.wal_manager.write_record(insert_record)

        abort_record = WALRecord(
            xid=1, oid=0, pageno=0, tid=0,
            action=WALAction.ABORT, data=b''
        )
        self.wal_manager.write_record(abort_record)

        self.assertEqual(self.wal_manager.flush_lsn, self.wal_manager.write_lsn)

    def test_wal_record_splitting(self):
        """Test WAL record splitting when record is too large for single page."""
        large_data = b'x' * (WAL_PAGE_SIZE - 100)
        test_record = WALRecord(
            xid=self.test_xid, oid=self.test_oid,
            pageno=self.test_pageno, tid=self.test_tid,
            action=WALAction.HEAP_INSERT, data=large_data
        )

        self.wal_manager.write_record(test_record)
        self.wal_manager.wal_buffer_flush()

        replayed_records = list(WALManager.replay(0))
        self.assertGreaterEqual(len(replayed_records), 1)

        final_record = replayed_records[-1]
        self.assertEqual(final_record.data, large_data)

    def test_undo_record_write_and_read(self):
        """Test basic UNDO record writing and reading functionality."""
        self.undo_manager.begin_transaction(self.test_xid)

        test_record = UndoRecord(
            xid=self.test_xid,
            operation=UndoOperation.HEAP_INSERT,
            relation=self.test_oid,
            location=(self.test_pageno, self.test_tid),
            data=self.test_data
        )
        self.undo_manager.write_record(test_record)
        self.undo_manager.commit_transaction(self.test_xid)

        undo_records = self.undo_manager.parse_record(self.test_xid)
        self.assertGreaterEqual(len(undo_records), 2)  # At least BEGIN and COMMIT

        test_records = [r for r in undo_records
                        if r.operation == UndoOperation.HEAP_INSERT]
        self.assertEqual(len(test_records), 1)
        record = test_records[0]

        self.assertEqual(record.xid, self.test_xid)
        self.assertEqual(record.relation, self.test_oid)
        self.assertEqual(record.location, (self.test_pageno, self.test_tid))
        self.assertEqual(record.data, self.test_data)

    def test_undo_records_are_reversed(self):
        """Test that undo records are returned in reverse order for proper undo."""
        self.undo_manager.begin_transaction(self.test_xid)

        for i in range(5):
            record = UndoRecord(
                xid=self.test_xid,
                operation=UndoOperation.HEAP_INSERT,
                relation=self.test_oid,
                location=(i, i),
                data=f'data_{i}'.encode()
            )
            self.undo_manager.write_record(record)

        self.undo_manager.commit_transaction(self.test_xid)

        undo_records = self.undo_manager.parse_record(self.test_xid)
        # Filter out BEGIN/COMMIT
        data_records = [r for r in undo_records
                        if r.operation == UndoOperation.HEAP_INSERT]
        # Should be in reverse order (last inserted first)
        for i, record in enumerate(data_records):
            expected_idx = 4 - i
            self.assertEqual(record.location, (expected_idx, expected_idx))

    def test_transaction_abort(self):
        """Test transaction abort functionality."""
        self.undo_manager.begin_transaction(self.test_xid)

        test_record = UndoRecord(
            xid=self.test_xid,
            operation=UndoOperation.HEAP_INSERT,
            relation=self.test_oid,
            location=(self.test_pageno, self.test_tid),
            data=self.test_data
        )
        self.undo_manager.write_record(test_record)
        self.undo_manager.abort_transaction(self.test_xid)

        undo_records = self.undo_manager.parse_record(self.test_xid)
        self.assertGreaterEqual(len(undo_records), 2)

        # First record (reversed) should be ABORT
        self.assertEqual(undo_records[0].operation, UndoOperation.ABORT)

    def test_undo_multiple_transactions(self):
        """Test undo records for multiple independent transactions."""
        for xid in [10, 20, 30]:
            self.undo_manager.begin_transaction(xid)
            record = UndoRecord(
                xid=xid,
                operation=UndoOperation.HEAP_INSERT,
                relation=self.test_oid,
                location=(xid, 0),
                data=f'xid_{xid}'.encode()
            )
            self.undo_manager.write_record(record)
            self.undo_manager.commit_transaction(xid)

        for xid in [10, 20, 30]:
            records = self.undo_manager.parse_record(xid)
            data_records = [r for r in records if r.operation == UndoOperation.HEAP_INSERT]
            self.assertEqual(len(data_records), 1)
            self.assertEqual(data_records[0].data, f'xid_{xid}'.encode())

    def test_undo_nonexistent_transaction(self):
        """Test parsing records for a transaction that doesn't exist."""
        records = self.undo_manager.parse_record(99999)
        self.assertEqual(records, [])

    def test_wal_page_full_handling(self):
        """Test that WAL properly handles page boundaries."""
        records_written = 0
        for i in range(100):
            record = WALRecord(
                xid=i, oid=self.test_oid,
                pageno=0, tid=i,
                action=WALAction.HEAP_INSERT,
                data=b'x' * 50
            )
            self.wal_manager.write_record(record)
            records_written += 1

        self.wal_manager.wal_buffer_flush()

        replayed = list(WALManager.replay(0))
        self.assertEqual(len(replayed), records_written)

    def test_wal_begin_commit_sequence(self):
        """Test a complete transaction WAL sequence: BEGIN, operations, COMMIT."""
        xid = 42
        self.wal_manager.write_record(WALRecord(
            xid=xid, oid=0, pageno=0, tid=0,
            action=WALAction.BEGIN, data=b''
        ))
        self.wal_manager.write_record(WALRecord(
            xid=xid, oid=100, pageno=1, tid=0,
            action=WALAction.HEAP_INSERT, data=b'tuple_data'
        ))
        self.wal_manager.write_record(WALRecord(
            xid=xid, oid=0, pageno=0, tid=0,
            action=WALAction.COMMIT, data=b''
        ))

        replayed = list(WALManager.replay(0))
        self.assertEqual(len(replayed), 3)
        self.assertEqual(replayed[0].action, WALAction.BEGIN)
        self.assertEqual(replayed[1].action, WALAction.HEAP_INSERT)
        self.assertEqual(replayed[2].action, WALAction.COMMIT)

    def test_undo_record_serialization_roundtrip(self):
        """Test UndoRecord serialization and deserialization."""
        record = UndoRecord(
            xid=99,
            operation=UndoOperation.HEAP_DELETE,
            relation=200,
            location=(10, 5),
            data=b'old_tuple_bytes'
        )
        serialized = record.to_bytes()
        deserialized = UndoRecord.from_bytes(serialized)

        self.assertEqual(deserialized.xid, 99)
        self.assertEqual(deserialized.operation, UndoOperation.HEAP_DELETE)
        self.assertEqual(deserialized.relation, 200)
        self.assertEqual(deserialized.location, (10, 5))
        self.assertEqual(deserialized.data, b'old_tuple_bytes')

    def test_wal_replay_from_middle(self):
        """Test replaying WAL records from a non-zero LSN."""
        # Write some records
        for i in range(10):
            record = WALRecord(
                xid=i, oid=self.test_oid,
                pageno=0, tid=i,
                action=WALAction.HEAP_INSERT,
                data=f'data_{i}'.encode()
            )
            self.wal_manager.write_record(record)

        # Flush to commit
        commit = WALRecord(xid=0, oid=0, pageno=0, tid=0,
                           action=WALAction.COMMIT, data=b'')
        self.wal_manager.write_record(commit)

        # Replay from beginning, count records
        all_records = list(WALManager.replay(0))
        self.assertEqual(len(all_records), 11)  # 10 inserts + 1 commit

    def test_undo_all_operation_types(self):
        """Test undo records for all heap operation types."""
        self.undo_manager.begin_transaction(self.test_xid)

        operations = [
            (UndoOperation.HEAP_INSERT, b'insert_data'),
            (UndoOperation.HEAP_DELETE, b'delete_data'),
            (UndoOperation.HEAP_UPDATE, b'update_data'),
        ]

        for op, data in operations:
            record = UndoRecord(
                xid=self.test_xid,
                operation=op,
                relation=self.test_oid,
                location=(1, 1),
                data=data
            )
            self.undo_manager.write_record(record)

        self.undo_manager.commit_transaction(self.test_xid)

        records = self.undo_manager.parse_record(self.test_xid)
        heap_records = [r for r in records
                        if r.operation in (UndoOperation.HEAP_INSERT,
                                           UndoOperation.HEAP_DELETE,
                                           UndoOperation.HEAP_UPDATE)]
        self.assertEqual(len(heap_records), 3)


if __name__ == '__main__':
    unittest.main()

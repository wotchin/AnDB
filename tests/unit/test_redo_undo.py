import os
import unittest
from unittest.mock import patch, MagicMock
import shutil
import time

from andb.constants.filename import WAL_DIR, UNDO_DIR
from andb.storage.engines.heap.redo import WALManager, WALRecord, WALAction
from andb.storage.engines.heap.undo import UndoManager, UndoRecord, UndoOperation

class TestRedoUndo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """在所有测试开始前执行一次"""
        # 确保测试目录不存在
        for dir_path in [WAL_DIR, UNDO_DIR]:
            if os.path.exists(dir_path):
                for file in os.listdir(dir_path):
                    try:
                        os.remove(os.path.join(dir_path, file))
                    except OSError:
                        pass
                try:
                    os.rmdir(dir_path)
                except OSError:
                    pass

    def setUp(self):
        """Set up test environment before each test"""
        # 确保目录是全新创建的
        for dir_path in [WAL_DIR, UNDO_DIR]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path, ignore_errors=True)
            os.makedirs(dir_path, exist_ok=True)
        
        # Initialize managers
        self.wal_manager = WALManager()
        self.undo_manager = UndoManager()
        
        # Test transaction data
        self.test_xid = 1
        self.test_oid = 100
        self.test_pageno = 1
        self.test_tid = 1
        self.test_data = b'test_data'

    def tearDown(self):
        """Clean up after each test"""
        # 关闭所有文件句柄
        if hasattr(self.wal_manager, 'current_wal_fd') and self.wal_manager.current_wal_fd:
            self.wal_manager.current_wal_fd.close()
            self.wal_manager.current_wal_fd = None
        
        # 确保 undo_manager 也关闭所有文件句柄
        if hasattr(self.undo_manager, '_fd'):
            self.undo_manager._fd.close()
            self.undo_manager._fd = None
            
        # 等待文件句柄完全释放
        time.sleep(0.1)
        
        # 清理目录
        for dir_path in [WAL_DIR, UNDO_DIR]:
            if os.path.exists(dir_path):
                try:
                    shutil.rmtree(dir_path, ignore_errors=True)
                except Exception as e:
                    print(f"Warning: Error cleaning up directory {dir_path}: {e}")

    def test_wal_record_write_and_replay(self):
        """Test basic WAL record writing and replay functionality"""
        # Create and write a test record
        test_record = WALRecord(
            xid=self.test_xid,
            oid=self.test_oid,
            pageno=self.test_pageno,
            tid=self.test_tid,
            action=WALAction.HEAP_INSERT,
            data=self.test_data
        )
        self.wal_manager.write_record(test_record)
        
        # Force flush
        self.wal_manager.wal_buffer_flush()
        
        # Replay and verify
        replayed_records = list(WALManager.replay(0))
        self.assertEqual(len(replayed_records), 1)
        replayed = replayed_records[0]
        
        # Verify record contents
        self.assertEqual(replayed.xid, self.test_xid)
        self.assertEqual(replayed.relation_oid, self.test_oid)
        self.assertEqual(replayed.location, (self.test_pageno, self.test_tid))
        self.assertEqual(replayed.data, self.test_data)
        self.assertEqual(replayed.action, WALAction.HEAP_INSERT)

    def test_undo_record_write_and_read(self):
        """Test basic UNDO record writing and reading functionality"""
        # Begin transaction
        self.undo_manager.begin_transaction(self.test_xid)
        
        # Create and write test undo record
        test_record = UndoRecord(
            xid=self.test_xid,
            operation=UndoOperation.HEAP_INSERT,
            relation=self.test_oid,
            location=(self.test_pageno, self.test_tid),
            data=self.test_data
        )
        self.undo_manager.write_record(test_record)
        
        # Commit transaction
        self.undo_manager.commit_transaction(self.test_xid)
        
        # Read and verify records
        undo_records = self.undo_manager.parse_record(self.test_xid)
        self.assertGreaterEqual(len(undo_records), 2)  # At least BEGIN and COMMIT
        
        # Find our test record
        test_records = [r for r in undo_records 
                       if r.operation == UndoOperation.HEAP_INSERT]
        self.assertEqual(len(test_records), 1)
        record = test_records[0]
        
        # Verify record contents
        self.assertEqual(record.xid, self.test_xid)
        self.assertEqual(record.relation, self.test_oid)
        self.assertEqual(record.location, (self.test_pageno, self.test_tid))
        self.assertEqual(record.data, self.test_data)

    def test_transaction_abort(self):
        """Test transaction abort functionality"""
        # Begin transaction
        self.undo_manager.begin_transaction(self.test_xid)
        
        # Write some records
        test_record = UndoRecord(
            xid=self.test_xid,
            operation=UndoOperation.HEAP_INSERT,
            relation=self.test_oid,
            location=(self.test_pageno, self.test_tid),
            data=self.test_data
        )
        self.undo_manager.write_record(test_record)
        
        # Abort transaction
        self.undo_manager.abort_transaction(self.test_xid)
        
        # Verify records
        undo_records = self.undo_manager.parse_record(self.test_xid)
        self.assertGreaterEqual(len(undo_records), 2)  # At least BEGIN and ABORT
        
        # Verify last record is ABORT
        self.assertEqual(undo_records[0].operation, UndoOperation.ABORT)

    def test_wal_record_splitting(self):
        """Test WAL record splitting when record is too large for single page"""
        # Create a large record that will need splitting
        large_data = b'x' * (1024 * 8)  # 64KB of data
        test_record = WALRecord(
            xid=self.test_xid,
            oid=self.test_oid,
            pageno=self.test_pageno,
            tid=self.test_tid,
            action=WALAction.HEAP_INSERT,
            data=large_data
        )
        
        # Write the record
        self.wal_manager.write_record(test_record)
        self.wal_manager.wal_buffer_flush()
        
        # Replay and verify
        replayed_records = list(WALManager.replay(0))
        self.assertGreaterEqual(len(replayed_records), 1)
        
        # Verify the data was correctly reconstructed
        final_record = replayed_records[-1]
        self.assertEqual(final_record.data, large_data)

if __name__ == '__main__':
    unittest.main()

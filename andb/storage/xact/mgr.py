import logging
import os
import time

from andb.catalog.oid import INVALID_OID
from andb.constants.macros import INVALID_XID, FIRST_XID, MAX_XID, XID_SIZE
from andb.constants.strings import BIG_END
from andb.errno.errors import UndoError
from andb.runtime import global_vars, session_vars
from andb.storage.engines.heap.bptree import TuplePointer
from andb.storage.engines.heap.page import INVALID_ITEM_ID
from andb.storage.engines.heap.redo import WALManager, WALRecord, WALAction, WAL_SEGMENT_SIZE
from andb.constants.filename import CHECKPOINT_FILE, WAL_DIR
from andb.storage.engines.heap.relation import BufferedBPTree, bt_delete, bt_simple_insert, open_relation, close_relation
from andb.storage.engines.heap.undo import UndoManager, UndoOperation
from andb.storage.lock import slock
from andb.storage.utils import easy_tuple_deserialize

STATUS_ACTIVE = 0
STATUS_COMMITTED = 1
STATUS_ABORTED = 2

def xid_to_bytes(xid):
    return int.to_bytes(xid, XID_SIZE, BIG_END, signed=False)

def bytes_to_xid(bytes_):
    assert len(bytes_) == XID_SIZE
    return int.from_bytes(bytes_, byteorder=BIG_END, signed=False)

class TransactionManager:
    def __init__(self):
        self.wal_manager = WALManager()
        self.undo_manager = UndoManager()
        self._xid_lock = slock.spinlock_create()
        self._current_xid = FIRST_XID
        self.active_transactions = {}

    def begin_transaction(self, xid):
        if xid < FIRST_XID or xid in self.active_transactions:
            return
        self.set_xid(xid)
        self.undo_manager.begin_transaction(xid)
        wal_record = WALRecord(xid=xid, oid=INVALID_OID, pageno=0, tid=0, action=WALAction.BEGIN, data=b'')
        self.wal_manager.write_record(wal_record)
        self.active_transactions[xid] = {
            'status': STATUS_ACTIVE,
            'start_time': time.time(),
            'last_lsn': self.wal_manager.max_lsn()
        }

    def commit_transaction(self, xid):
        """
        Commit a transaction and ensure all changes are persisted.
        The order of operations is critical:
        1. Flush undo logs to disk
        2. Ensure all dirty pages are written (redo logs)
        3. Write commit record to WAL
        """
        if xid < FIRST_XID or xid not in self.active_transactions:
            return

        transaction = self.active_transactions[xid]
        assert transaction['status'] == STATUS_ACTIVE
        transaction['status'] = STATUS_COMMITTED

        # Step 1: Flush undo logs
        self.undo_manager.flush(xid)
        
        # Step 2: Ensure all dirty pages are written
        global_vars.buffer_manager.sync()
        
        # Step 3: Write commit record to WAL
        wal_record = WALRecord(xid=xid, oid=INVALID_OID, pageno=0, tid=0, action=WALAction.COMMIT, data=b'')
        self.wal_manager.write_record(wal_record)
        
        # Step 4: Commit undo transaction
        self.undo_manager.commit_transaction(xid)
        transaction['last_lsn'] = self.wal_manager.max_lsn()

        del self.active_transactions[xid]
        self.set_xid(INVALID_XID)

    def abort_transaction(self, xid):
        if xid < FIRST_XID or xid not in self.active_transactions:
            return

        transaction = self.active_transactions[xid]
        assert transaction['status'] == STATUS_ACTIVE
        transaction['status'] = STATUS_ABORTED

        # Write abort record to WAL
        wal_record = WALRecord(xid=xid, oid=INVALID_OID, pageno=0, tid=0, action=WALAction.ABORT, data=xid_to_bytes(xid))
        self.wal_manager.write_record(wal_record)
        transaction['last_lsn'] = self.wal_manager.max_lsn()

        # Flush undo logs
        self.undo_manager.flush(xid)
        # Perform undo operations
        self.perform_undo(xid, transaction['last_lsn'])
        # Abort undo transaction
        self.undo_manager.abort_transaction(xid)

        del self.active_transactions[xid]
        self.set_xid(INVALID_XID)

    def allocate_xid(self):
        slock.spinlock_aquire(self._xid_lock)
        self._current_xid += 1
        xid = self._current_xid
        #TODO: we don't need to wraparound xid now
        assert xid <= MAX_XID
        slock.spinlock_release(self._xid_lock)
        return xid

    def max_lsn(self):
        return self.wal_manager.max_lsn()

    def recovery(self):
        """
        Recovery process to replay WAL records and undo uncommitted transactions.
        """
        if not os.path.exists(WAL_DIR):
            flush_lsn = 0
        else:
            wal_files = os.listdir(WAL_DIR)
            if not wal_files:
                flush_lsn = 0
            else:
                # remove checkpoint file from the list
                if CHECKPOINT_FILE in wal_files:
                    wal_files.remove(CHECKPOINT_FILE)
                wal_files.sort()
                last_wal_file = wal_files[-1]
                last_segment_number = int(last_wal_file, 16)
                last_segment_path = os.path.join(WAL_DIR, last_wal_file)
                last_segment_size = os.stat(last_segment_path).st_size
                flush_lsn = (last_segment_number * WAL_SEGMENT_SIZE) + last_segment_size

        self.wal_manager.flush_lsn = flush_lsn
        self.wal_manager.write_lsn = flush_lsn

        # 2. 读取上次的checkpoint位置
        checkpoint_lsn = self.read_checkpoint_lsn()
        replay_lsn = checkpoint_lsn
        transactions = set()

        # First pass: Identify the checkpoint and gather transaction IDs
        for redo_record in self.wal_manager.replay(lsn=checkpoint_lsn):
            replay_lsn += redo_record.total_size
            if redo_record.action == WALAction.CHECKPOINT:
                # 更新最新的checkpoint位置
                checkpoint_lsn = replay_lsn
                # 清空之前的事务集合,因为checkpoint之前的事务都已经完成
                transactions.clear()
            elif redo_record.action == WALAction.BEGIN:
                transactions.add(redo_record.xid)
            elif redo_record.action == WALAction.COMMIT:
                transactions.discard(redo_record.xid)
            elif redo_record.action == WALAction.ABORT:
                transactions.discard(redo_record.xid)

        # 4. 从最新的checkpoint开始重放WAL记录
        replay_lsn = checkpoint_lsn
        for redo_record in self.wal_manager.replay(lsn=checkpoint_lsn):
            replay_lsn += redo_record.total_size
            self.apply_redo(redo_record, replay_lsn)

        # 5. 对未完成的事务执行undo
        for xid in transactions:
            self.perform_undo(xid, replay_lsn)

    def apply_redo(self, redo_record, replay_lsn):
        """
        Apply redo log to the database.
        """
        xid = redo_record.xid
        action = redo_record.action
        relation = open_relation(redo_record.relation_oid)
        location = redo_record.location 
        data = redo_record.data

        if action == WALAction.HEAP_INSERT:
            pageno, tid = location
            page = global_vars.buffer_manager.get_page(relation, pageno).page
            if page.header.lsn < replay_lsn:
                new_tid = page.insert(replay_lsn, data)
                page.header.lsn = replay_lsn
                assert new_tid == tid
        elif action == WALAction.HEAP_DELETE:
            pageno, tid = location
            page = global_vars.buffer_manager.get_page(relation, pageno).page
            if page.header.lsn < replay_lsn:
                page.delete(replay_lsn, tid)
                page.header.lsn = replay_lsn
        elif action == WALAction.HEAP_UPDATE:
            # now, we implement inplace update by delete and insert,
            # so the replay processing is not in the branch
            pageno, tid = location
            page = global_vars.buffer_manager.get_page(relation, pageno).page
            if page.header.lsn < replay_lsn:
                page.update(replay_lsn, tid, data)
                page.header.lsn = replay_lsn
        elif action == WALAction.BTREE_INSERT:
            key_data, tuple_pointer_data = easy_tuple_deserialize(data)
            tuple_pointer = TuplePointer()
            tuple_pointer.unpack(tuple_pointer_data)
            tree = BufferedBPTree(relation)
            tree.insert(replay_lsn, key_data, tuple_pointer)
        elif action == WALAction.BTREE_DELETE:
            key_data, tuple_pointer_data = easy_tuple_deserialize(data)
            tuple_pointer = TuplePointer()
            tuple_pointer.unpack(tuple_pointer_data)
            tree = BufferedBPTree(relation)
            tree.delete_value(replay_lsn, key_data, tuple_pointer)
        elif action == WALAction.BTREE_UPDATE:
            key_data, tuple_pointer_data = easy_tuple_deserialize(data)
            tuple_pointer = TuplePointer()
            tuple_pointer.unpack(tuple_pointer_data)
            tree = BufferedBPTree(relation)
            tree.update(replay_lsn, key_data, tuple_pointer)
        elif action in (WALAction.BEGIN, WALAction.COMMIT, WALAction.ABORT, WALAction.CHECKPOINT):
            pass
        else:
            raise NotImplementedError(f"WAL action {action} is not implemented")
        
        if relation:
            close_relation(relation.oid)

    def perform_undo(self, xid, lsn):
        """
        Undo operations for a given transaction ID.
        """
        # 这些record本身就已经是从文件尾部往前读取的了，因为做过了reverse
        for undo_record in self.undo_manager.parse_record(xid):
            if undo_record.operation == UndoOperation.HEAP_INSERT:
                pageno, tid = undo_record.location
                page = global_vars.buffer_manager.get_page(undo_record.relation, pageno).page
                success = page.delete_inplace(lsn, tid)
                if not success:
                    logging.error(f'UNDO: failed to delete item {tid} in page {pageno}, items are {page.item_ids}')
                global_vars.buffer_manager.mark_dirty(undo_record.relation, pageno)
            elif undo_record.operation == UndoOperation.HEAP_DELETE:
                pageno, tid = undo_record.location
                page = global_vars.buffer_manager.get_page(undo_record.relation, pageno).page
                new_tid = page.insert(lsn, undo_record.data)
                if new_tid == INVALID_ITEM_ID:
                    logging.error(f'UNDO: failed to insert item to page {pageno}')
                    raise UndoError(f'UNDO: failed to insert item to page {pageno}')
                global_vars.buffer_manager.mark_dirty(undo_record.relation, pageno)
            elif undo_record.operation == UndoOperation.HEAP_UPDATE:
                pageno, tid = undo_record.location
                page = global_vars.buffer_manager.get_page(undo_record.relation, pageno).page
                item_id = page.update(lsn, tid, undo_record.data)
                if item_id == INVALID_ITEM_ID:
                    logging.error(f'UNDO: failed to find item {tid} in page {pageno}')
                    raise UndoError(f'UNDO: failed to find item {tid} in page {pageno}')
                global_vars.buffer_manager.mark_dirty(undo_record.relation, pageno)
            elif undo_record.operation == UndoOperation.BTREE_INSERT:
                key_data, tuple_pointer = undo_record.location
                tree = BufferedBPTree(undo_record.relation)
                lsn = self.max_lsn() #TODO: is this fine?
                # btree can mark dirty itself
                tree.delete_value(lsn, key_data, tuple_pointer)
            elif undo_record.operation == UndoOperation.BTREE_UPDATE:
                key_data, old_tuple_pointer = undo_record.location
                tree = BufferedBPTree(undo_record.relation)
                lsn = self.max_lsn() #TODO: is this fine?
                tree.update(lsn, key_data, old_tuple_pointer)
            elif undo_record.operation == UndoOperation.BTREE_DELETE:
                key_data, old_tuple_pointer = undo_record.location
                tree = BufferedBPTree(undo_record.relation)
                lsn = self.max_lsn() #TODO: is this fine?
                tree.insert(lsn, key_data, old_tuple_pointer)
            elif undo_record.operation == UndoOperation.BEGIN or \
                    undo_record.operation == UndoOperation.COMMIT or \
                undo_record.operation == UndoOperation.ABORT:
                pass
            else:
                raise NotImplementedError(f"Undo operation {undo_record.operation} is not implemented")

    def checkpoint(self):
        """
        Create a checkpoint in the WAL and flush dirty pages to disk.
        """
        # 1. 确保所有脏页都写入磁盘
        global_vars.buffer_manager.sync()
        
        # 2. 写入checkpoint记录到WAL
        checkpoint_lsn = self.max_lsn()
        self.wal_manager.write_record(
            WALRecord(
                xid=INVALID_XID,
                oid=INVALID_OID,
                pageno=0,
                tid=0,
                action=WALAction.CHECKPOINT,
                data=b''
            )
        )
        
        # 3. 刷新WAL缓冲区
        self.wal_manager.wal_buffer_flush()
        
        # 4. 持久化checkpoint位置
        self.write_checkpoint_lsn(checkpoint_lsn)

    def set_xid(self, xid):
        session_vars.session_xid = xid

    def get_xid(self):
        if session_vars.session_xid is None \
                or session_vars.session_xid == INVALID_XID:
            pass
            # logging.warning(
            #     f"Session XID is None or INVALID_XID: "
            #     f"{session_vars.session_xid}"
            # )
        return session_vars.session_xid

    def write_checkpoint_lsn(self, lsn):
        """
        Persist checkpoint LSN to disk.
        """
        checkpoint_file = os.path.join(WAL_DIR, CHECKPOINT_FILE)
        with open(checkpoint_file, 'wb') as f:
            f.write(int.to_bytes(lsn, 8, 'big'))
            f.flush()
            os.fsync(f.fileno())

    def read_checkpoint_lsn(self):
        """
        Read persisted checkpoint LSN from disk.
        Returns 0 if no checkpoint file exists.
        """
        checkpoint_file = os.path.join(WAL_DIR, CHECKPOINT_FILE)
        if not os.path.exists(checkpoint_file):
            return 0
        with open(checkpoint_file, 'rb') as f:
            data = f.read(8)
            return int.from_bytes(data, 'big')


"""Crash-safe page-image transactions with strict two-phase relation locking.

Recovery scans retained WAL from zero. The existing checkpoint format has no
transaction/dirty-page table, so its pointer is advisory, not a safe redo start.
Full-page before images and redo-only compensation records make interrupted
rollback restartable; volatile UndoManager files are not the recovery authority.
"""
import os
import threading
import time

from andb.catalog.oid import INVALID_OID
from andb.catalog.syscache import CATALOG_ANDB_CLASS
from andb.common.file_operation import sync_directory
from andb.constants.macros import INVALID_XID, FIRST_XID, MAX_XID, XID_SIZE
from andb.constants.strings import BIG_END
from andb.constants.filename import CHECKPOINT_FILE, WAL_DIR
from andb.errno.errors import WALError, RollbackError
from andb.runtime import global_vars
from andb.storage.engines.heap.journal import encode_images, decode_images
from andb.storage.engines.heap.bptree import LeafNode, create_node
from andb.storage.engines.heap.page import SlotPage, INVALID_ITEM_ID
from andb.storage.engines.heap.redo import WALManager, WALRecord, WALAction
from andb.storage.engines.heap.relation import open_relation, close_relation
from andb.storage.engines.heap.undo import UndoManager
from andb.storage.lock.rlock import NO_LOCK, lock_release_all
from andb.storage.utils import easy_tuple_deserialize
from andb.storage.xact.csn import CSNManager, SnapshotManager

STATUS_ACTIVE, STATUS_COMMITTED, STATUS_ABORTED = range(3)
ABORT_COMPLETE = b'undo-complete-v1'


def xid_to_bytes(xid):
    return int.to_bytes(xid, XID_SIZE, BIG_END, signed=False)


def bytes_to_xid(bytes_):
    assert len(bytes_) == XID_SIZE
    return int.from_bytes(bytes_, byteorder=BIG_END, signed=False)


class TransactionManager:
    def __init__(self):
        self.wal_manager = WALManager()
        self.undo_manager = UndoManager()  # retained for the standalone undo API
        self.storage_lock = threading.RLock()
        self._xid_lock = threading.RLock()
        self._local = threading.local()
        self._current_xid = FIRST_XID
        self.active_transactions = {}
        self.csn_manager = CSNManager()
        self.snapshot_manager = SnapshotManager(self.csn_manager)
        self.failed = False
        self.recovering = False

    def _healthy(self):
        if self.failed or self.wal_manager.failed:
            raise WALError('Storage is in failed state; restart and recover before further queries')

    def in_transaction(self):
        transaction = self.active_transactions.get(self.get_xid())
        return transaction is not None and transaction['owner'] == threading.get_ident()

    def begin_transaction(self, xid):
        with self.storage_lock:
            self._healthy()
            if xid in self.active_transactions:
                if self.active_transactions[xid]['owner'] != threading.get_ident():
                    raise RollbackError('Transaction ID is already owned by another session')
                return
            if self.in_transaction():
                raise RollbackError('A transaction is already active in this session')
            self.wal_manager.write_record(WALRecord(xid, INVALID_OID, 0, 0, WALAction.BEGIN, b''))
            self.set_xid(xid)
            self.snapshot_manager.register_transaction(xid)
            self.active_transactions[xid] = {
                'status': STATUS_ACTIVE, 'owner': threading.get_ident(),
                'start_time': time.time(), 'last_lsn': self.max_lsn(),
                'snapshot': self.snapshot_manager.take_snapshot(owner_xid=xid),
                'changes': [],
            }

    def _finish(self, xid, committed):
        if committed:
            self.csn_manager.assign_csn(xid)
        else:
            self.csn_manager.set_xid_aborted(xid)
        self.snapshot_manager.unregister_transaction(xid)
        self.active_transactions.pop(xid, None)
        lock_release_all()
        self.set_xid(INVALID_XID)

    def commit_transaction(self, xid):
        with self.storage_lock:
            self._healthy()
            if xid not in self.active_transactions:
                return
            if self.active_transactions[xid]['owner'] != threading.get_ident():
                raise RollbackError('Cannot commit another session transaction')
            # No-force: WAL (including inverse images) and COMMIT are durable before
            # acknowledgement. Data pages may be flushed independently afterwards.
            try:
                self.wal_manager.write_record(WALRecord(xid, INVALID_OID, 0, 0, WALAction.COMMIT, b''))
            except Exception:
                self.failed = True  # outcome can be uncertain; never "undo" a durable COMMIT
                raise
            self._finish(xid, True)

    def abort_transaction(self, xid):
        with self.storage_lock:
            self._healthy()
            if xid not in self.active_transactions:
                return
            if self.active_transactions[xid]['owner'] != threading.get_ident():
                raise RollbackError('Cannot abort another session transaction')
            try:
                self.perform_undo(xid, self.max_lsn())
                # ABORT means undo finished, not merely that undo was requested.
                self.wal_manager.write_record(WALRecord(xid, INVALID_OID, 0, 0, WALAction.ABORT, ABORT_COMPLETE))
            except Exception:
                self.failed = True
                raise
            self._finish(xid, False)

    def record_page_changes(self, relation, pages, root=None):
        """Log private page copies, then publish them under the writeback guard."""
        with self.storage_lock:
            self._healthy()
            if not self.in_transaction():
                raise RollbackError('Page changes require an active transaction')
            record = WALRecord(self.get_xid(), relation.oid, 0, 0, WALAction.PAGE_UPDATE,
                               encode_images(relation.is_heap, pages, root))
            try:
                self.wal_manager.write_record(record)
                self.active_transactions[record.xid]['changes'].append(record)
                self._apply_images(record, relation)
            except Exception:
                # An appended record whose publication failed requires recovery;
                # allowing subsequent writes could invert the wrong page version.
                self.failed = True
                raise
            return record.end_lsn

    def _apply_images(self, record, relation=None):
        heap, pages, root, _ = decode_images(record.data)
        opened = relation is None
        if opened:
            relation = open_relation(record.relation_oid, NO_LOCK)
        if relation is None:
            # Non-transactional DDL can remove a relation. New OIDs are never reused.
            return
        try:
            if relation.is_heap != heap:
                raise WALError('WAL relation kind mismatch; possible legacy OID reuse')
            bm = global_vars.buffer_manager
            # We deliberately repeat history from full images, not trust an
            # unchecksummed/torn on-disk pageLSN. All retained records are scanned.
            for pageno, _, after in pages:
                page = SlotPage.unpack(after) if heap else create_node(after)
                if heap:
                    page.header.lsn = record.end_lsn
                else:
                    if page.get_pageno() != pageno:
                        raise WALError('Index page image address mismatch')
                    page.lsn = record.end_lsn
                bp = bm.create_buffer_page(relation, pageno, page)
                bp.mark_dirty()
                bm.put_page(bp)
            if root is not None:
                bm.set_index_root(relation, root[1])
        finally:
            if opened:
                close_relation(relation.oid, NO_LOCK)

    def _compensate(self, record):
        heap, pages, root, _ = decode_images(record.data)
        inverse = []
        for pageno, before, after in pages:
            if before is None:
                if heap:
                    before = SlotPage.allocate(0).pack()
                else:
                    page = LeafNode()
                    page.set_pageno(pageno)
                    before = page.pack()
            inverse.append((pageno, after, before))
        inverse_root = None if root is None else (root[1], root[0])
        clr = WALRecord(record.xid, record.relation_oid, 0, 0, WALAction.COMPENSATION,
                        encode_images(heap, inverse, inverse_root, undo_of=record.end_lsn))
        self.wal_manager.write_record(clr)
        self._apply_images(clr)
        return clr

    def perform_undo(self, xid, lsn=None):
        for record in reversed(self.active_transactions[xid]['changes']):
            self._compensate(record)
        self.active_transactions[xid]['changes'].clear()

    def get_snapshot(self, xid):
        return self.active_transactions.get(xid, {}).get('snapshot')

    def allocate_xid(self):
        with self._xid_lock:
            self._healthy()
            if self._current_xid >= MAX_XID:
                raise RollbackError('Transaction ID space exhausted')
            self._current_xid += 1
            return self._current_xid

    def max_lsn(self):
        return self.wal_manager.max_lsn()

    def recovery(self):
        """Analysis, repeat history, then restartable reverse-LSN loser undo.

        Legacy logical WAL can be replayed for committed heap operations, but an
        unresolved legacy transaction is refused: its durable inverse may be
        missing, and the old ABORT was written before undo completed.
        """
        with self.storage_lock:
            self.recovering = True
            try:
                records = list(self.wal_manager.replay(0))
                self.wal_manager.restore()  # removes only incomplete tail, not valid history
                states, changes, compensated = {}, [], set()
                max_oid = 0
                for record in records:
                    self._current_xid = max(self._current_xid, record.xid)
                    max_oid = max(max_oid, record.relation_oid)
                    if record.action == WALAction.BEGIN:
                        states[record.xid] = 'active'
                        self.csn_manager.set_xid_in_progress(record.xid)
                    elif record.action == WALAction.COMMIT:
                        states[record.xid] = 'committed'
                        self.csn_manager.assign_csn(record.xid)
                    elif record.action == WALAction.ABORT:
                        states[record.xid] = 'aborted' if record.data == ABORT_COMPLETE else 'legacy-abort'
                        self.csn_manager.set_xid_aborted(record.xid)
                    elif record.action == WALAction.PAGE_UPDATE:
                        changes.append(record)
                    elif record.action == WALAction.COMPENSATION:
                        _, _, _, undo_of = decode_images(record.data)
                        compensated.add((record.xid, undo_of))
                # Persist an OID allocation high-water mark before any new DDL.
                CATALOG_ANDB_CLASS.reserve_oid(max_oid)
                for record in records:
                    if record.action in (WALAction.PAGE_UPDATE, WALAction.COMPENSATION):
                        self._apply_images(record)
                    elif record.action in (WALAction.HEAP_INSERT, WALAction.HEAP_DELETE,
                                           WALAction.HEAP_UPDATE, WALAction.HEAP_BATCH_DELETE,
                                           WALAction.BTREE_INSERT, WALAction.BTREE_DELETE, WALAction.BTREE_UPDATE):
                        if states.get(record.xid) != 'committed':
                            raise WALError('Unresolved legacy WAL transaction: use the original engine/backup to export committed data')
                        self.apply_redo(record, record.end_lsn)
                losers = {xid for xid, state in states.items() if state == 'active'}
                if any(state == 'legacy-abort' for state in states.values()):
                    raise WALError('Legacy ABORT is not proof of completed undo; recovery refused rather than return inconsistent data')
                for record in reversed(changes):
                    if record.xid in losers and (record.xid, record.end_lsn) not in compensated:
                        self._compensate(record)
                for xid in sorted(losers):
                    self.wal_manager.write_record(WALRecord(xid, INVALID_OID, 0, 0, WALAction.ABORT, ABORT_COMPLETE))
                    self.csn_manager.set_xid_aborted(xid)
                global_vars.buffer_manager.sync()
            except Exception:
                self.failed = True
                raise  # fail closed: never serve partially recovered data
            finally:
                self.recovering = False

    def apply_redo(self, record, replay_lsn):
        """Compatibility path for committed legacy heap WAL; images are preferred."""
        if record.action in (WALAction.BEGIN, WALAction.COMMIT, WALAction.ABORT, WALAction.CHECKPOINT):
            return
        if record.action in (WALAction.PAGE_UPDATE, WALAction.COMPENSATION):
            return self._apply_images(record)
        relation = open_relation(record.relation_oid, NO_LOCK)
        if relation is None:
            return
        try:
            if record.action in (WALAction.BTREE_INSERT, WALAction.BTREE_DELETE, WALAction.BTREE_UPDATE):
                raise WALError('Legacy logical index WAL is not reliably idempotent; export/rebuild before upgrading')
            pageno, tid = record.location
            bp = global_vars.buffer_manager.get_page(relation, pageno)
            page = bp.page
            if page.header.lsn >= replay_lsn:
                return
            if record.action == WALAction.HEAP_INSERT:
                if page.insert(replay_lsn, record.data) != tid:
                    raise WALError('Legacy heap redo TID mismatch')
            elif record.action == WALAction.HEAP_DELETE:
                if not page.delete(replay_lsn, tid):
                    raise WALError('Legacy heap redo delete mismatch')
            elif record.action == WALAction.HEAP_UPDATE:
                if page.update(replay_lsn, tid, record.data) == INVALID_ITEM_ID:
                    raise WALError('Legacy heap redo update failed')
            elif record.action == WALAction.HEAP_BATCH_DELETE:
                for tid in easy_tuple_deserialize(record.data):
                    if not page.delete(replay_lsn, tid):
                        raise WALError('Legacy batch delete mismatch')
            else:
                raise WALError('Unknown legacy WAL action')
            page.header.lsn = replay_lsn
            bp.mark_dirty()
        finally:
            close_relation(relation.oid, NO_LOCK)

    def checkpoint(self):
        with self.storage_lock:
            self._healthy()
            global_vars.buffer_manager.sync()
            lsn = self.max_lsn()
            self.wal_manager.write_record(WALRecord(INVALID_XID, INVALID_OID, 0, 0, WALAction.CHECKPOINT, b''))
            self.wal_manager.wal_buffer_flush()
            self.write_checkpoint_lsn(lsn)

    def set_xid(self, xid):
        self._local.xid = xid

    def get_xid(self):
        return getattr(self._local, 'xid', INVALID_XID)

    def write_checkpoint_lsn(self, lsn):
        path = os.path.join(WAL_DIR, CHECKPOINT_FILE)
        tmp = path + '.tmp'
        with open(tmp, 'wb') as stream:
            stream.write(lsn.to_bytes(8, 'big'))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        sync_directory(WAL_DIR)

    def read_checkpoint_lsn(self):
        try:
            with open(os.path.join(WAL_DIR, CHECKPOINT_FILE), 'rb') as stream:
                data = stream.read()
        except FileNotFoundError:
            return 0
        return int.from_bytes(data, 'big') if len(data) == 8 else 0

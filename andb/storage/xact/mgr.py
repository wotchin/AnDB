import time

from andb.storage.engines.heap.redo import WALManager, WALRecord, WALAction
from andb.constants.strings import BIG_END
from andb.storage.lock import slock

STATUS_ACTIVE = 0
STATUS_COMMITTED = 1
STATUS_ABORTED = 2

INVALID_XID = 0
DUMMY_XID = 1  # for select
FIRST_XID = 2
XID_SIZE = 8
MAX_XID = 0xffffffffffffffff  # 8 bytes


def xid_to_bytes(xid):
    return int.to_bytes(xid, XID_SIZE, BIG_END, signed=False)


def bytes_to_xid(bytes_):
    assert len(bytes_) == XID_SIZE
    return int.from_bytes(bytes_, byteorder=BIG_END, signed=False)


class TransactionManager:
    def __init__(self):
        self.wal_manager = WALManager()
        self.undo_manager = None
        # although this xid lock is unnecessary for Python due to GIL,
        # we believe that GIL will be removed from Python
        self._xid_lock = slock.spinlock_create()
        self._current_xid = FIRST_XID
        self.active_transactions = {}

    def begin_transaction(self, xid):
        if xid in self.active_transactions:
            return

        wal_record = WALRecord(xid, WALAction.BEGIN, b'')
        self.wal_manager.write_record(wal_record)
        self.active_transactions[xid] = {
            'status': STATUS_ACTIVE,
            'start_time': time.time(),
            'last_lsn': self.wal_manager.max_lsn(),
            'undo_log': []
        }

    def commit_transaction(self, xid):
        if xid not in self.active_transactions:
            return

        transaction = self.active_transactions[xid]
        assert transaction['status'] == STATUS_ACTIVE
        transaction['status'] = STATUS_COMMITTED

        wal_record = WALRecord(xid, WALAction.COMMIT, b'')
        # automatically flush to disk due to commit
        self.wal_manager.write_record(wal_record)
        transaction['last_lsn'] = self.wal_manager.max_lsn()

        del self.active_transactions[xid]

    def abort_transaction(self, xid):
        if xid not in self.active_transactions:
            return

        transaction = self.active_transactions[xid]
        assert transaction['status'] == STATUS_ACTIVE
        transaction['status'] = STATUS_ABORTED

        wal_record = WALRecord(xid, WALAction.ABORT, xid_to_bytes(xid))
        # automatically flush to disk due to abort
        self.wal_manager.write_record(wal_record)
        transaction['last_lsn'] = self.wal_manager.max_lsn()
        # todo: implement undo chain
        # self._undo_transaction(xid)

        del self.active_transactions[xid]

    def allocate_xid(self):
        slock.spinlock_aquire(self._xid_lock)
        self._current_xid += 1
        xid = self._current_xid
        # todo: we don't need to wraparound xid now
        assert xid <= MAX_XID
        slock.spinlock_release(self._xid_lock)
        return xid

    def replay_xid(self, record: WALRecord):
        # todo: impl
        pass

    def max_lsn(self):
        return self.wal_manager.max_lsn()

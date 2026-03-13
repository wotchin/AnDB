"""
Commit Sequence Number (CSN) based MVCC implementation.

CSN provides snapshot isolation (repeatable read) by assigning a monotonically
increasing sequence number to each committed transaction. A snapshot captures
the CSN at the time it is taken, and only sees changes from transactions whose
commit CSN <= snapshot CSN.

Key concepts:
- Each transaction gets an XID at begin time.
- Each committed transaction gets a CSN at commit time.
- A snapshot records: the current CSN and the set of active (uncommitted) XIDs.
- Tuple visibility: a tuple is visible if its xmin transaction is committed
  with CSN <= snapshot CSN and its xmax (if set) has CSN > snapshot CSN or is
  not committed.
"""

import threading

from andb.constants.macros import INVALID_XID

# Special CSN values
CSN_INVALID = 0
CSN_IN_PROGRESS = 1  # Transaction is still active
CSN_ABORTED = 2      # Transaction has been aborted
CSN_FROZEN = 3        # Very old committed transaction (always visible)
FIRST_VALID_CSN = 4   # First real CSN value


class CSNManager:
    """Manages the global Commit Sequence Number counter and per-transaction CSN mapping."""

    def __init__(self):
        self._lock = threading.Lock()
        self._current_csn = FIRST_VALID_CSN
        # Maps XID -> CSN for recently committed/aborted transactions.
        # In a real system this would be a bounded CLOG/CSN log on disk.
        self._xid_csn_map = {}

    def get_current_csn(self):
        """Return the current global CSN (the last assigned CSN)."""
        with self._lock:
            return self._current_csn

    def assign_csn(self, xid):
        """Assign and return the next CSN for a committing transaction."""
        with self._lock:
            csn = self._current_csn
            self._current_csn += 1
            self._xid_csn_map[xid] = csn
            return csn

    def set_xid_aborted(self, xid):
        """Mark a transaction as aborted."""
        with self._lock:
            self._xid_csn_map[xid] = CSN_ABORTED

    def set_xid_in_progress(self, xid):
        """Mark a transaction as in-progress."""
        with self._lock:
            self._xid_csn_map[xid] = CSN_IN_PROGRESS

    def get_xid_csn(self, xid):
        """Get the CSN for a given XID.
        Returns CSN_IN_PROGRESS if not yet committed or aborted."""
        with self._lock:
            return self._xid_csn_map.get(xid, CSN_IN_PROGRESS)

    def is_committed(self, xid):
        """Check if a transaction is committed (has a valid CSN >= FIRST_VALID_CSN)."""
        csn = self.get_xid_csn(xid)
        return csn >= FIRST_VALID_CSN

    def is_aborted(self, xid):
        """Check if a transaction has been aborted."""
        return self.get_xid_csn(xid) == CSN_ABORTED

    def is_in_progress(self, xid):
        """Check if a transaction is still in progress."""
        return self.get_xid_csn(xid) == CSN_IN_PROGRESS

    def cleanup_old_entries(self, oldest_active_xid):
        """Remove entries for XIDs older than the oldest active transaction.
        In production, this would be tied to VACUUM."""
        with self._lock:
            to_remove = [xid for xid in self._xid_csn_map
                         if xid < oldest_active_xid
                         and self._xid_csn_map[xid] != CSN_IN_PROGRESS]
            for xid in to_remove:
                del self._xid_csn_map[xid]


class Snapshot:
    """A point-in-time snapshot used for MVCC visibility checks.

    A snapshot records:
    - snapshot_csn: The CSN at the time the snapshot was taken.
      Only changes from transactions with commit CSN <= snapshot_csn are visible.
    - active_xids: Set of XIDs that were active (uncommitted) when the snapshot
      was taken. Even if these later commit with CSN <= snapshot_csn, their
      changes should NOT be visible (they were uncommitted at snapshot time).
    - owner_xid: The XID of the transaction that owns this snapshot.
      The owner's own changes are always visible.
    """

    def __init__(self, snapshot_csn, active_xids, owner_xid=INVALID_XID):
        self.snapshot_csn = snapshot_csn
        self.active_xids = frozenset(active_xids)
        self.owner_xid = owner_xid

    def is_visible(self, xmin, xmax, csn_manager):
        """Check if a tuple with given xmin/xmax is visible under this snapshot.

        Args:
            xmin: The XID that created the tuple.
            xmax: The XID that deleted/updated the tuple (INVALID_XID if not deleted).
            csn_manager: The CSNManager to look up transaction CSNs.

        Returns:
            True if the tuple is visible, False otherwise.
        """
        # Rule 1: Tuple must have been created by a visible transaction
        if not self._xid_is_visible(xmin, csn_manager):
            return False

        # Rule 2: If tuple has been deleted, the deleting transaction
        # must NOT be visible (i.e., the delete hasn't happened yet from our perspective)
        if xmax != INVALID_XID:
            if self._xid_is_visible(xmax, csn_manager):
                return False

        return True

    def _xid_is_visible(self, xid, csn_manager):
        """Check if changes made by `xid` are visible under this snapshot."""
        # Own changes are always visible
        if xid == self.owner_xid:
            return True

        # If xid was active when snapshot was taken, it's not visible
        # (even if it committed since then)
        if xid in self.active_xids:
            return False

        # Check the transaction's CSN
        csn = csn_manager.get_xid_csn(xid)

        # Aborted transactions are never visible
        if csn == CSN_ABORTED:
            return False

        # In-progress transactions are not visible (unless it's our own, handled above)
        if csn == CSN_IN_PROGRESS:
            return False

        # Frozen tuples are always visible
        if csn == CSN_FROZEN:
            return True

        # Committed: visible only if committed before our snapshot
        return csn <= self.snapshot_csn

    def __repr__(self):
        return (f"Snapshot(csn={self.snapshot_csn}, "
                f"active_xids={set(self.active_xids)}, "
                f"owner_xid={self.owner_xid})")


class SnapshotManager:
    """Manages snapshot creation and lifecycle."""

    def __init__(self, csn_manager):
        self._csn_manager = csn_manager
        self._lock = threading.Lock()
        # Set of currently active transaction XIDs
        self._active_xids = set()

    def register_transaction(self, xid):
        """Register a new transaction as active."""
        with self._lock:
            self._active_xids.add(xid)
        self._csn_manager.set_xid_in_progress(xid)

    def unregister_transaction(self, xid):
        """Remove a transaction from the active set (on commit or abort)."""
        with self._lock:
            self._active_xids.discard(xid)

    def take_snapshot(self, owner_xid=INVALID_XID):
        """Take a snapshot of the current database state.

        For REPEATABLE READ: take the snapshot once at transaction begin
        and reuse it for all reads within the transaction.

        For READ COMMITTED: take a new snapshot before each statement.
        """
        with self._lock:
            snapshot_csn = self._csn_manager.get_current_csn()
            active_xids = set(self._active_xids)
        return Snapshot(snapshot_csn, active_xids, owner_xid)

    def get_active_xids(self):
        """Return a copy of the currently active XIDs."""
        with self._lock:
            return set(self._active_xids)

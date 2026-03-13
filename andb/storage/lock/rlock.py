import threading
import time

NO_LOCK = 0
ACCESS_SHARE_LOCK = 1  # SELECT
ROW_SHARE_LOCK = 2  # SELECT FOR UPDATE/FOR SHARE
ROW_EXCLUSIVE_LOCK = 3  # INSERT, UPDATE, DELETE
SHARE_UPDATE_EXCLUSIVE_LOCK = 4  # ANALYZE
SHARE_LOCK = 5  # CREATE INDEX
SHARE_ROW_EXCLUSIVE_LOCK = 6  # EXCLUSIVE MODE
EXCLUSIVE_LOCK = 7  # SHARE/SELECT...FOR UPDATE
ACCESS_EXCLUSIVE_LOCK = 8  # ALTER TABLE, DROP TABLE
MAX_LOCK_MODE = ACCESS_EXCLUSIVE_LOCK

LOCK_NOT_AVAILABLE = 0
LOCK_OK = 1
LOCK_ALREADY_HELD = 2

# PostgreSQL-style lock compatibility matrix.
# _LOCK_COMPAT[held_mode][requested_mode] = True means compatible.
# Index 0 = NO_LOCK (always compatible), indices 1-8 correspond to the lock modes above.
_LOCK_COMPAT = {
    #                          NoLock  AccShr  RowShr  RowExc  ShrUpdExc  Shr   ShrRowExc  Excl   AccExcl
    NO_LOCK:                  {1: True, 2: True, 3: True, 4: True, 5: True, 6: True, 7: True, 8: True},
    ACCESS_SHARE_LOCK:        {1: True, 2: True, 3: True, 4: True, 5: True, 6: True, 7: True, 8: False},
    ROW_SHARE_LOCK:           {1: True, 2: True, 3: True, 4: True, 5: True, 6: True, 7: False, 8: False},
    ROW_EXCLUSIVE_LOCK:       {1: True, 2: True, 3: True, 4: True, 5: False, 6: False, 7: False, 8: False},
    SHARE_UPDATE_EXCLUSIVE_LOCK: {1: True, 2: True, 3: True, 4: False, 5: False, 6: False, 7: False, 8: False},
    SHARE_LOCK:               {1: True, 2: True, 3: False, 4: False, 5: True, 6: False, 7: False, 8: False},
    SHARE_ROW_EXCLUSIVE_LOCK: {1: True, 2: True, 3: False, 4: False, 5: False, 6: False, 7: False, 8: False},
    EXCLUSIVE_LOCK:           {1: True, 2: False, 3: False, 4: False, 5: False, 6: False, 7: False, 8: False},
    ACCESS_EXCLUSIVE_LOCK:    {1: False, 2: False, 3: False, 4: False, 5: False, 6: False, 7: False, 8: False},
}

_lock_table = {}
_lock_table_mutex = threading.Lock()


class LockHolderInfo:
    """Tracks which lock mode a specific thread holds on a resource."""
    def __init__(self, thread_id, lock_mode):
        self.thread_id = thread_id
        self.lock_mode = lock_mode


class LockEntry:
    def __init__(self, tag):
        self.tag = tag
        # Map from thread_id -> lock_mode held by that thread
        self.holders = {}
        # Condition variable for threads waiting for this lock
        self.wait_cv = threading.Condition(_lock_table_mutex)
        # Queue of (thread_id, lock_mode) for waiting threads (FIFO fairness)
        self.wait_queue = []

    def get_max_held_mode(self):
        """Return the strongest lock mode currently held by any thread."""
        if not self.holders:
            return NO_LOCK
        return max(self.holders.values())

    def is_compatible(self, requested_mode, requesting_thread):
        """Check if `requested_mode` is compatible with all currently held locks,
        excluding locks held by `requesting_thread` itself (for re-entrance/upgrade)."""
        for thread_id, held_mode in self.holders.items():
            if thread_id == requesting_thread:
                continue
            if not _LOCK_COMPAT[held_mode][requested_mode]:
                return False
        return True


def lock_acquire(tag, lock_mode, dont_wait=False, wait_seconds=5):
    """Acquire a lock on `tag` with `lock_mode`.

    Two-Phase Locking protocol: locks are acquired in the growing phase
    and only released during the shrinking phase (at commit/abort).

    Args:
        tag: Resource identifier (e.g., relation OID).
        lock_mode: One of the lock mode constants.
        dont_wait: If True, return immediately if lock is not available.
        wait_seconds: Maximum seconds to wait for the lock.

    Returns:
        LOCK_OK, LOCK_ALREADY_HELD, or LOCK_NOT_AVAILABLE.
    """
    thread_id = threading.get_ident()

    with _lock_table_mutex:
        if tag not in _lock_table:
            _lock_table[tag] = LockEntry(tag)
        entry = _lock_table[tag]

        # Check if this thread already holds the same or stronger lock
        if thread_id in entry.holders:
            if entry.holders[thread_id] >= lock_mode:
                return LOCK_ALREADY_HELD
            # Lock upgrade: thread wants a stronger lock
            # Check compatibility with other holders
            if entry.is_compatible(lock_mode, thread_id):
                entry.holders[thread_id] = lock_mode
                return LOCK_OK
            if dont_wait:
                return LOCK_NOT_AVAILABLE
            # Fall through to wait

        # Try to acquire directly
        if entry.is_compatible(lock_mode, thread_id):
            # Also check that no one ahead in the wait queue is blocked
            # (fairness: don't skip waiters)
            if not entry.wait_queue:
                entry.holders[thread_id] = lock_mode
                return LOCK_OK

        if dont_wait:
            return LOCK_NOT_AVAILABLE

        # Enqueue and wait
        entry.wait_queue.append((thread_id, lock_mode))
        deadline = time.monotonic() + wait_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Timeout: remove from wait queue
                entry.wait_queue = [(tid, m) for tid, m in entry.wait_queue
                                    if tid != thread_id]
                return LOCK_NOT_AVAILABLE

            entry.wait_cv.wait(timeout=min(remaining, 0.5))

            # Check if we're at the head of the queue and lock is compatible
            if (entry.wait_queue and entry.wait_queue[0][0] == thread_id
                    and entry.is_compatible(lock_mode, thread_id)):
                entry.wait_queue.pop(0)
                entry.holders[thread_id] = lock_mode
                return LOCK_OK


def lock_release(tag, lock_mode):
    """Release a lock held by the current thread.

    In 2PL, all locks are released together at transaction end.

    Args:
        tag: Resource identifier.
        lock_mode: The lock mode to release.

    Returns:
        True if successfully released, False otherwise.
    """
    thread_id = threading.get_ident()

    with _lock_table_mutex:
        if tag not in _lock_table:
            return False

        entry = _lock_table[tag]

        if thread_id not in entry.holders:
            return False

        held_mode = entry.holders[thread_id]
        if held_mode < lock_mode:
            return False

        del entry.holders[thread_id]

        # Wake up waiters so they can re-check compatibility
        entry.wait_cv.notify_all()

        # Clean up empty entries
        if not entry.holders and not entry.wait_queue:
            del _lock_table[tag]

        return True


def lock_release_all():
    """Release all locks held by the current thread.
    Called at transaction commit or abort (shrinking phase of 2PL).
    """
    thread_id = threading.get_ident()

    with _lock_table_mutex:
        tags_to_check = list(_lock_table.keys())
        for tag in tags_to_check:
            entry = _lock_table[tag]
            if thread_id in entry.holders:
                del entry.holders[thread_id]
                entry.wait_cv.notify_all()
                if not entry.holders and not entry.wait_queue:
                    del _lock_table[tag]

import unittest
import threading
import time

from andb.storage.lock.rlock import (
    lock_acquire, lock_release, lock_release_all,
    NO_LOCK, ACCESS_SHARE_LOCK, ROW_SHARE_LOCK, ROW_EXCLUSIVE_LOCK,
    SHARE_UPDATE_EXCLUSIVE_LOCK, SHARE_LOCK, SHARE_ROW_EXCLUSIVE_LOCK,
    EXCLUSIVE_LOCK, ACCESS_EXCLUSIVE_LOCK,
    LOCK_OK, LOCK_ALREADY_HELD, LOCK_NOT_AVAILABLE,
    _lock_table,
)


class TestLockCompatibility(unittest.TestCase):
    """Test the PostgreSQL-style lock compatibility matrix."""

    def setUp(self):
        _lock_table.clear()

    def tearDown(self):
        lock_release_all()
        _lock_table.clear()

    def test_access_share_compatible_with_access_share(self):
        """Two SELECT queries should not block each other."""
        tag = 'table1'
        result1 = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=True)
        self.assertEqual(result1, LOCK_OK)

        # Simulate second thread
        acquired = [None]
        def other_thread():
            acquired[0] = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=True)
        t = threading.Thread(target=other_thread)
        t.start()
        t.join()
        self.assertEqual(acquired[0], LOCK_OK)

    def test_access_exclusive_blocks_all(self):
        """ACCESS EXCLUSIVE should block all other lock modes."""
        tag = 'table1'
        result = lock_acquire(tag, ACCESS_EXCLUSIVE_LOCK, dont_wait=True)
        self.assertEqual(result, LOCK_OK)

        # Try to acquire ACCESS_SHARE from another thread
        acquired = [None]
        def other_thread():
            acquired[0] = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=True)
        t = threading.Thread(target=other_thread)
        t.start()
        t.join()
        self.assertEqual(acquired[0], LOCK_NOT_AVAILABLE)

    def test_row_exclusive_compatible_with_row_exclusive(self):
        """Two INSERT/UPDATE/DELETE operations should not block each other (table-level)."""
        tag = 'table1'
        result1 = lock_acquire(tag, ROW_EXCLUSIVE_LOCK, dont_wait=True)
        self.assertEqual(result1, LOCK_OK)

        acquired = [None]
        def other_thread():
            acquired[0] = lock_acquire(tag, ROW_EXCLUSIVE_LOCK, dont_wait=True)
        t = threading.Thread(target=other_thread)
        t.start()
        t.join()
        self.assertEqual(acquired[0], LOCK_OK)

    def test_row_exclusive_conflicts_with_share_lock(self):
        """ROW EXCLUSIVE (DML) conflicts with SHARE (CREATE INDEX)."""
        tag = 'table1'
        result = lock_acquire(tag, ROW_EXCLUSIVE_LOCK, dont_wait=True)
        self.assertEqual(result, LOCK_OK)

        acquired = [None]
        def other_thread():
            acquired[0] = lock_acquire(tag, SHARE_LOCK, dont_wait=True)
        t = threading.Thread(target=other_thread)
        t.start()
        t.join()
        self.assertEqual(acquired[0], LOCK_NOT_AVAILABLE)

    def test_lock_release(self):
        """Test that releasing a lock allows others to acquire it."""
        tag = 'table1'
        lock_acquire(tag, ACCESS_EXCLUSIVE_LOCK, dont_wait=True)
        lock_release(tag, ACCESS_EXCLUSIVE_LOCK)

        acquired = [None]
        def other_thread():
            acquired[0] = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=True)
        t = threading.Thread(target=other_thread)
        t.start()
        t.join()
        self.assertEqual(acquired[0], LOCK_OK)

    def test_lock_reentrance(self):
        """Same thread can acquire the same lock (already held)."""
        tag = 'table1'
        result1 = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=True)
        self.assertEqual(result1, LOCK_OK)
        result2 = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=True)
        self.assertEqual(result2, LOCK_ALREADY_HELD)

    def test_lock_upgrade(self):
        """Test upgrading from a weaker to a stronger lock."""
        tag = 'table1'
        result1 = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=True)
        self.assertEqual(result1, LOCK_OK)
        # Upgrade to ROW_EXCLUSIVE
        result2 = lock_acquire(tag, ROW_EXCLUSIVE_LOCK, dont_wait=True)
        self.assertEqual(result2, LOCK_OK)

    def test_lock_wait(self):
        """Test that a lock waits when blocked and succeeds when released."""
        tag = 'table1'
        lock_acquire(tag, ACCESS_EXCLUSIVE_LOCK, dont_wait=True)

        acquired = [None]
        def other_thread():
            acquired[0] = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=False, wait_seconds=3)

        t = threading.Thread(target=other_thread)
        t.start()

        # Release after a short delay so the other thread can proceed
        time.sleep(0.3)
        lock_release(tag, ACCESS_EXCLUSIVE_LOCK)

        t.join(timeout=5)
        self.assertEqual(acquired[0], LOCK_OK)

    def test_lock_wait_timeout(self):
        """Test that lock acquisition times out."""
        tag = 'table1'
        lock_acquire(tag, ACCESS_EXCLUSIVE_LOCK, dont_wait=True)

        acquired = [None]
        def other_thread():
            acquired[0] = lock_acquire(tag, ACCESS_SHARE_LOCK, dont_wait=False, wait_seconds=0.5)

        t = threading.Thread(target=other_thread)
        t.start()
        t.join(timeout=3)
        self.assertEqual(acquired[0], LOCK_NOT_AVAILABLE)

    def test_release_all(self):
        """Test releasing all locks held by current thread."""
        lock_acquire('t1', ACCESS_SHARE_LOCK, dont_wait=True)
        lock_acquire('t2', ROW_EXCLUSIVE_LOCK, dont_wait=True)
        lock_release_all()
        # Both locks should be released
        self.assertNotIn('t1', _lock_table)
        self.assertNotIn('t2', _lock_table)


if __name__ == '__main__':
    unittest.main()

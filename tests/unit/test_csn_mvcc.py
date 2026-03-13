import unittest
from andb.constants.macros import INVALID_XID
from andb.storage.xact.csn import (
    CSNManager, SnapshotManager, Snapshot,
    CSN_INVALID, CSN_IN_PROGRESS, CSN_ABORTED, CSN_FROZEN, FIRST_VALID_CSN
)


class TestCSNManager(unittest.TestCase):
    def setUp(self):
        self.csn_mgr = CSNManager()

    def test_initial_csn(self):
        self.assertEqual(self.csn_mgr.get_current_csn(), FIRST_VALID_CSN)

    def test_assign_csn_increments(self):
        csn1 = self.csn_mgr.assign_csn(xid=10)
        csn2 = self.csn_mgr.assign_csn(xid=20)
        self.assertEqual(csn1, FIRST_VALID_CSN)
        self.assertEqual(csn2, FIRST_VALID_CSN + 1)

    def test_is_committed(self):
        self.csn_mgr.set_xid_in_progress(10)
        self.assertFalse(self.csn_mgr.is_committed(10))
        self.csn_mgr.assign_csn(10)
        self.assertTrue(self.csn_mgr.is_committed(10))

    def test_is_aborted(self):
        self.csn_mgr.set_xid_in_progress(10)
        self.assertFalse(self.csn_mgr.is_aborted(10))
        self.csn_mgr.set_xid_aborted(10)
        self.assertTrue(self.csn_mgr.is_aborted(10))

    def test_is_in_progress(self):
        # Unknown XID defaults to in-progress
        self.assertTrue(self.csn_mgr.is_in_progress(999))
        self.csn_mgr.set_xid_in_progress(10)
        self.assertTrue(self.csn_mgr.is_in_progress(10))
        self.csn_mgr.assign_csn(10)
        self.assertFalse(self.csn_mgr.is_in_progress(10))


class TestSnapshot(unittest.TestCase):
    def setUp(self):
        self.csn_mgr = CSNManager()

    def test_own_changes_visible(self):
        """A transaction can always see its own changes."""
        xid = 10
        self.csn_mgr.set_xid_in_progress(xid)
        snapshot = Snapshot(
            snapshot_csn=self.csn_mgr.get_current_csn(),
            active_xids={xid},
            owner_xid=xid
        )
        # xmin=10 (our own xid), xmax=INVALID (not deleted)
        self.assertTrue(snapshot.is_visible(xmin=xid, xmax=INVALID_XID, csn_manager=self.csn_mgr))

    def test_committed_before_snapshot_visible(self):
        """Changes committed before snapshot are visible."""
        xid = 10
        self.csn_mgr.set_xid_in_progress(xid)
        self.csn_mgr.assign_csn(xid)  # commit

        snapshot = Snapshot(
            snapshot_csn=self.csn_mgr.get_current_csn(),
            active_xids=set(),
            owner_xid=20
        )
        self.assertTrue(snapshot.is_visible(xmin=xid, xmax=INVALID_XID, csn_manager=self.csn_mgr))

    def test_committed_after_snapshot_not_visible(self):
        """Changes committed after snapshot are not visible."""
        snapshot_csn = self.csn_mgr.get_current_csn()

        xid = 10
        self.csn_mgr.set_xid_in_progress(xid)
        self.csn_mgr.assign_csn(xid)  # commit after snapshot

        snapshot = Snapshot(
            snapshot_csn=snapshot_csn,
            active_xids={xid},  # xid was active when snapshot was taken
            owner_xid=20
        )
        self.assertFalse(snapshot.is_visible(xmin=xid, xmax=INVALID_XID, csn_manager=self.csn_mgr))

    def test_aborted_not_visible(self):
        """Changes from aborted transactions are never visible."""
        xid = 10
        self.csn_mgr.set_xid_aborted(xid)

        snapshot = Snapshot(
            snapshot_csn=self.csn_mgr.get_current_csn(),
            active_xids=set(),
            owner_xid=20
        )
        self.assertFalse(snapshot.is_visible(xmin=xid, xmax=INVALID_XID, csn_manager=self.csn_mgr))

    def test_deleted_tuple_not_visible(self):
        """A tuple deleted by a visible transaction should not be visible."""
        xid_insert = 10
        xid_delete = 20

        self.csn_mgr.set_xid_in_progress(xid_insert)
        self.csn_mgr.assign_csn(xid_insert)

        self.csn_mgr.set_xid_in_progress(xid_delete)
        self.csn_mgr.assign_csn(xid_delete)

        snapshot = Snapshot(
            snapshot_csn=self.csn_mgr.get_current_csn(),
            active_xids=set(),
            owner_xid=30
        )
        # Both inserter and deleter committed before snapshot
        self.assertFalse(snapshot.is_visible(xmin=xid_insert, xmax=xid_delete, csn_manager=self.csn_mgr))

    def test_deleted_by_uncommitted_still_visible(self):
        """A tuple deleted by an uncommitted transaction is still visible."""
        xid_insert = 10
        xid_delete = 20

        self.csn_mgr.set_xid_in_progress(xid_insert)
        self.csn_mgr.assign_csn(xid_insert)

        self.csn_mgr.set_xid_in_progress(xid_delete)
        # xid_delete is NOT committed

        snapshot = Snapshot(
            snapshot_csn=self.csn_mgr.get_current_csn(),
            active_xids={xid_delete},  # delete tx active at snapshot time
            owner_xid=30
        )
        self.assertTrue(snapshot.is_visible(xmin=xid_insert, xmax=xid_delete, csn_manager=self.csn_mgr))

    def test_repeatable_read_isolation(self):
        """Demonstrate repeatable read: snapshot doesn't see later commits."""
        # T1 commits before snapshot
        self.csn_mgr.set_xid_in_progress(10)
        self.csn_mgr.assign_csn(10)

        # Take snapshot
        snapshot_csn = self.csn_mgr.get_current_csn()

        # T2 commits after snapshot
        self.csn_mgr.set_xid_in_progress(20)
        self.csn_mgr.assign_csn(20)

        snapshot = Snapshot(
            snapshot_csn=snapshot_csn,
            active_xids={20},  # T2 was active when snapshot taken
            owner_xid=30
        )

        # T1's changes visible
        self.assertTrue(snapshot.is_visible(xmin=10, xmax=INVALID_XID, csn_manager=self.csn_mgr))
        # T2's changes NOT visible (committed after snapshot)
        self.assertFalse(snapshot.is_visible(xmin=20, xmax=INVALID_XID, csn_manager=self.csn_mgr))


class TestSnapshotManager(unittest.TestCase):
    def setUp(self):
        self.csn_mgr = CSNManager()
        self.snap_mgr = SnapshotManager(self.csn_mgr)

    def test_register_and_unregister(self):
        self.snap_mgr.register_transaction(10)
        active = self.snap_mgr.get_active_xids()
        self.assertIn(10, active)

        self.snap_mgr.unregister_transaction(10)
        active = self.snap_mgr.get_active_xids()
        self.assertNotIn(10, active)

    def test_take_snapshot_captures_active(self):
        self.snap_mgr.register_transaction(10)
        self.snap_mgr.register_transaction(20)

        snapshot = self.snap_mgr.take_snapshot(owner_xid=10)
        self.assertIn(10, snapshot.active_xids)
        self.assertIn(20, snapshot.active_xids)

    def test_snapshot_after_commit(self):
        self.snap_mgr.register_transaction(10)
        self.csn_mgr.assign_csn(10)
        self.snap_mgr.unregister_transaction(10)

        snapshot = self.snap_mgr.take_snapshot(owner_xid=20)
        self.assertNotIn(10, snapshot.active_xids)
        # T10 committed before snapshot, so visible
        self.assertTrue(snapshot.is_visible(xmin=10, xmax=INVALID_XID, csn_manager=self.csn_mgr))


if __name__ == '__main__':
    unittest.main()

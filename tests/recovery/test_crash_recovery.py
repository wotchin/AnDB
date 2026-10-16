"""Real process restarts: no retained buffer, FD, relation or transaction cache."""
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

ROOT = Path(__file__).resolve().parents[2]
BOOT = '''
import os, sys
from andb.cmd.setup import setup_data_dir
from andb.initializer import init_all_database_components
from andb.entrance import execute_simple_query as sql
from andb.runtime import global_vars
from andb.storage.engines.heap.relation import *
from andb.catalog.syscache import CATALOG_ANDB_CLASS
from andb.catalog.oid import OID_DATABASE_ANDB
path = sys.argv[1]
if not os.path.exists(path):
    setup_data_dir(path)
init_all_database_components(path)
'''


def run_db(path, body, expected=0, before_init=''):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    boot = BOOT.replace('init_all_database_components(path)', textwrap.dedent(before_init) + '\ninit_all_database_components(path)')
    result = subprocess.run([sys.executable, '-c', boot + '\n' + textwrap.dedent(body), str(path)],
                            cwd=ROOT, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == expected, result.stdout[-2000:] + result.stderr[-6000:]
    return result


def schema(path):
    run_db(path, "sql('create table t (id int not null, v text)')")


def test_committed_wal_only_survives_two_restarts(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        # Force a no-force commit even on the old implementation.
        global_vars.buffer_manager.sync = lambda: None
        sql("insert into t values (1, 'committed')")
        os._exit(0)
    ''')
    for _ in range(2):
        run_db(path, '''
            assert sql('select * from t order by id').tuples == [(1, 'committed')]
            os._exit(0)
        ''')


def test_reload_modify_reload_does_not_write_stale_bytes(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        sql("insert into t values (1, 'old')")
        global_vars.buffer_manager.sync()
    ''')
    run_db(path, '''
        assert sql('select * from t').tuples == [(1, 'old')]
        sql("update t set v = 'new' where id = 1")
        global_vars.buffer_manager.sync()
        os._exit(0)
    ''')
    run_db(path, "assert sql('select * from t').tuples == [(1, 'new')]")


def test_multirow_delete_keeps_surviving_tuple_ids(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        for i in range(8):
            sql("insert into t values (%d, 'v')" % i)
        sql('delete from t where id < 6')
        assert sql('select * from t order by id').tuples == [(6, 'v'), (7, 'v')]
    ''')


def test_loser_flushed_before_commit_is_undone(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        sql("insert into t values (1, 'winner')")
        sql('begin')
        sql("update t set v = 'loser' where id = 1")
        global_vars.xact_manager.wal_manager.wal_buffer_flush()
        global_vars.buffer_manager.sync()
        os._exit(0)
    ''')
    for _ in range(2):
        run_db(path, "assert sql('select * from t').tuples == [(1, 'winner')]")


def test_page_rollover_redo_targets_actual_page(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        global_vars.buffer_manager.sync = lambda: None
        sql('begin')
        for i in range(20):
            sql("insert into t values (%d, '%s')" % (i, 'x' * 1000))
        sql('commit')
        os._exit(0)
    ''')
    run_db(path, '''
        rows = sql('select * from t order by id').tuples
        assert rows == [(i, 'x' * 1000) for i in range(20)], [(r[0],len(r[1])) for r in rows]
    ''')


def test_append_after_restart_preserves_old_wal(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    for i in range(4):
        run_db(path, f'''\n            sql("insert into t values ({i}, 'v')")
            os._exit(0)
        ''')
    run_db(path, "assert sql('select * from t order by id').tuples == [(i, 'v') for i in range(4)]")


def test_snapshot_excludes_transaction_born_later():
    from andb.storage.xact.csn import CSNManager, SnapshotManager
    csn = CSNManager()
    snapshots = SnapshotManager(csn)
    old = snapshots.take_snapshot(owner_xid=10)
    snapshots.register_transaction(20)
    csn.assign_csn(20)
    snapshots.unregister_transaction(20)
    assert not old.is_visible(20, 0, csn)


@pytest.mark.parametrize('phase', ['before_publish', 'after_publish'])
def test_recovery_itself_can_crash_during_undo(tmp_path, phase):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        sql("insert into t values (1, 'winner')")
        sql('create index idx on t (id)')
        sql('begin')
        sql("update t set v = 'loser' where id = 1")
        sql("insert into t values (2, 'loser')")
        global_vars.xact_manager.checkpoint()  # loser spans a checkpoint
        os._exit(0)
    ''')
    inject = f'''
        from andb.storage.xact.mgr import TransactionManager
        from andb.storage.engines.heap.redo import WALAction
        original = TransactionManager._apply_images
        def interrupted(self, record, relation=None):
            if record.action == WALAction.COMPENSATION:
                self.wal_manager.wal_buffer_flush()
                if {phase!r} == 'before_publish':
                    os._exit(73)
                original(self, record, relation)
                global_vars.buffer_manager.sync()
                os._exit(73)
            return original(self, record, relation)
        TransactionManager._apply_images = interrupted
    '''
    # First stop after writing a CLR. Second stop when redoing that same CLR.
    # Both crashes must remain recoverable, without double undo.
    run_db(path, 'assert False', expected=73, before_init=inject)
    run_db(path, 'assert False', expected=73, before_init=inject)
    for _ in range(2):
        run_db(path, '''
            assert sql('select * from t order by id').tuples == [(1, 'winner')]
            assert sql('select * from t where id = 1').tuples == [(1, 'winner')]
            assert sql('select * from t where id = 2').tuples == []
            os._exit(0)
        ''')


@pytest.mark.parametrize('durable_commit', [False, True])
def test_crash_at_commit_decision(tmp_path, durable_commit):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, f'''
        from andb.storage.engines.heap.redo import WALAction
        sql('begin')
        sql("insert into t values (1, 'value')")
        global_vars.buffer_manager.sync()
        writer = global_vars.xact_manager.wal_manager
        original = writer.write_record
        def inject(record):
            if record.action == WALAction.COMMIT:
                if {durable_commit!r}:
                    original(record)
                os._exit(74)
            return original(record)
        writer.write_record = inject
        sql('commit')
    ''', expected=74)
    expected = [(1, 'value')] if durable_commit else []
    run_db(path, f"assert sql('select * from t').tuples == {expected!r}")


def test_not_null_failure_rolls_back_whole_statement_and_transaction(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        from andb.errno.errors import RollbackError
        sql('begin')
        sql("insert into t values (1, 'first')")
        try:
            sql("insert into t values (null, 'invalid')")
        except RollbackError:
            pass
        else:
            assert False, 'NOT NULL must be enforced'
        try:
            sql("insert into t values (2, 'must not autocommit')")
        except RollbackError:
            pass
        else:
            assert False, 'failed transaction block must remain failed'
        sql('rollback')
        assert sql('select * from t').tuples == []
        sql("insert into t values (3, 'valid')")
        os._exit(0)
    ''')
    run_db(path, "assert sql('select * from t').tuples == [(3, 'valid')]")


def test_explicit_ddl_is_rejected_without_catalog_side_effects(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        from andb.errno.errors import RollbackError
        sql('begin')
        sql("insert into t values (1, 'rollback me')")
        try:
            sql('create table forbidden (id int)')
        except RollbackError:
            pass
        else:
            assert False
        sql('rollback')
        assert sql('select * from t').tuples == []
        assert CATALOG_ANDB_CLASS.get_relation_oid('forbidden', OID_DATABASE_ANDB) == 0
    ''')


def test_recovery_repairs_torn_heap_page_from_full_images(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        sql("insert into t values (1, 'winner')")
        global_vars.buffer_manager.sync()
        oid = CATALOG_ANDB_CLASS.get_relation_oid('t', OID_DATABASE_ANDB)
        relation = open_relation(oid)
        with open(relation.file_path, 'r+b') as stream:
            stream.truncate(111)  # simulate a torn data-page write, WAL intact
        os._exit(0)
    ''')
    run_db(path, "assert sql('select * from t').tuples == [(1, 'winner')]")


def test_dropped_oid_is_never_reused_by_new_table(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        sql("insert into t values (1, 'old incarnation')")
        old = CATALOG_ANDB_CLASS.get_relation_oid('t', OID_DATABASE_ANDB)
        sql('drop table t')
        sql('create table t (id int not null, v text)')
        new = CATALOG_ANDB_CLASS.get_relation_oid('t', OID_DATABASE_ANDB)
        assert new > old
        sql("insert into t values (2, 'new incarnation')")
        os._exit(0)
    ''')
    for _ in range(2):
        run_db(path, "assert sql('select * from t').tuples == [(2, 'new incarnation')]")


def test_duplicate_index_keys_survive_delete_update_abort_and_restart(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        sql('create index idx on t (v)')
        for i in range(6):
            sql("insert into t values (%d, 'same')" % i)
        sql('delete from t where id = 0')
        sql("update t set v = 'changed' where id = 1")
        sql("update t set id = 12 where id = 2")
        sql('begin')
        sql("delete from t where v = 'same'")
        sql('rollback')
        expected = [(1, 'changed'), (3, 'same'), (4, 'same'), (5, 'same'), (12, 'same')]
        assert sql('select * from t order by id').tuples == expected
        assert sorted(sql("select * from t where v = 'same'").tuples) == expected[1:]
        os._exit(0)
    ''')
    for _ in range(2):
        run_db(path, '''
            assert sorted(sql("select * from t where v = 'same'").tuples) == [(3, 'same'), (4, 'same'), (5, 'same'), (12, 'same')]
            assert sql("select * from t where v = 'changed'").tuples == [(1, 'changed')]
        ''')


def test_index_internal_split_dirty_eviction_and_restart(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        from andb.storage.engines.heap.bptree import InternalNode
        sql('create index idx on t (v)')
        global_vars.buffer_manager.cache.capacity = 3
        sql('begin')
        for i in range(400):
            key = '%04d-' % i + 'z' * 200
            sql("insert into t values (%d, '%s')" % (i, key))
            if i % 25 == 0:
                global_vars.buffer_manager.sync_evicted_pages()
        sql('commit')
        oid = CATALOG_ANDB_CLASS.get_relation_oid('idx', OID_DATABASE_ANDB)
        index = open_relation(oid)
        tree = BufferedBPTree(index)
        assert isinstance(tree.root, InternalNode)
        assert isinstance(tree.load_page(tree.root.children[0].get_pageno()), InternalNode)
        close_relation(oid)
        os._exit(0)
    ''')
    for _ in range(2):
        run_db(path, '''
            global_vars.buffer_manager.cache.capacity = 3
            rows = sql('select * from t order by id').tuples
            assert rows == [(i, '%04d-' % i + 'z' * 200) for i in range(400)]
            for i in [0, 7, 50, 123, 255, 399]:
                key = '%04d-' % i + 'z' * 200
                assert sql("select * from t where v = '%s'" % key).tuples == [(i, key)]
            index = open_relation(CATALOG_ANDB_CLASS.get_relation_oid('idx', OID_DATABASE_ANDB))
            assert len(list(bt_scan_all_keys(index))) == 400
            os._exit(0)
        ''')

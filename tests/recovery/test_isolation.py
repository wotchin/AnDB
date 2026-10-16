"""Controlled multi-session histories; no claim that heap MVCC exists."""
from tests.recovery.test_crash_recovery import run_db, schema


def test_reader_cannot_observe_uncommitted_writer(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        import threading
        from concurrent.futures import ThreadPoolExecutor
        sql("insert into t values (1, 'winner')")
        ready, release, attempted, done = [threading.Event() for _ in range(4)]
        def writer():
            sql('begin')
            sql("update t set v = 'dirty' where id = 1")
            ready.set()
            assert release.wait(5)
            sql('rollback')
        def reader():
            assert ready.wait(5)
            attempted.set()
            result = sql('select * from t').tuples
            done.set()
            return result
        with ThreadPoolExecutor(2) as pool:
            w = pool.submit(writer)
            r = pool.submit(reader)
            assert attempted.wait(5)
            assert not done.wait(0.15), 'reader returned before uncommitted writer released lock'
            release.set()
            w.result(timeout=5)
            assert r.result(timeout=5) == [(1, 'winner')]
        assert not global_vars.xact_manager.active_transactions
    ''')


def test_repeatable_read_and_phantom_prevention_by_retained_table_lock(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        import threading
        from concurrent.futures import ThreadPoolExecutor
        sql("insert into t values (1, 'original')")
        sql('begin')
        first = sql('select * from t').tuples
        attempted, done = threading.Event(), threading.Event()
        def writer():
            attempted.set()
            sql("insert into t values (2, 'phantom')")
            done.set()
        with ThreadPoolExecutor(1) as pool:
            result = pool.submit(writer)
            assert attempted.wait(5)
            assert not done.wait(0.15)
            assert sql('select * from t').tuples == first
            sql('commit')
            result.result(timeout=5)
        assert sql('select * from t order by id').tuples == [(1, 'original'), (2, 'phantom')]
    ''')


def test_concurrent_increments_do_not_lose_updates(tmp_path):
    path = tmp_path / 'db'
    run_db(path, '''
        from concurrent.futures import ThreadPoolExecutor
        sql('create table counter (id int not null, n int not null)')
        sql('insert into counter values (1, 0)')
        def worker():
            ids = []
            for i in range(20):
                sql('begin')
                ids.append(global_vars.xact_manager.get_xid())
                sql('update counter set n = n + 1 where id = 1')
                sql('commit')
            return ids
        with ThreadPoolExecutor(3) as pool:
            futures = [pool.submit(worker) for _ in range(3)]
            ids = [xid for future in futures for xid in future.result(timeout=15)]
        assert len(set(ids)) == 60, 'thread-local XIDs must not alias'
        assert sql('select * from counter').tuples == [(1, 60)]
        os._exit(0)
    ''')
    run_db(path, "assert sql('select * from counter').tuples == [(1, 60)]")


def test_disjoint_table_winner_survives_other_session_loser(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        from concurrent.futures import ThreadPoolExecutor
        sql('create table u (id int, v text)')
        sql("insert into t values (1, 'original')")
        sql('begin')
        loser = global_vars.xact_manager.get_xid()
        sql("update t set v = 'loser' where id = 1")
        def winner():
            sql('begin')
            xid = global_vars.xact_manager.get_xid()
            sql("insert into u values (2, 'winner')")
            sql('commit')
            return xid
        with ThreadPoolExecutor(1) as pool:
            assert pool.submit(winner).result(timeout=5) != loser
        assert global_vars.xact_manager.get_xid() == loser
        global_vars.buffer_manager.sync()
        os._exit(0)
    ''')
    run_db(path, '''
        assert sql('select * from t').tuples == [(1, 'original')]
        assert sql('select * from u').tuples == [(2, 'winner')]
    ''')


def test_connection_handler_rolls_back_on_disconnect(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        import socket
        from andb.net.server import ClientConnection
        from andb.entrance import _get_explicit_xid
        left, right = socket.socketpair()
        conn = ClientConnection(left, ('local', 0), None)
        conn._do_startup = lambda: True
        def disconnect():
            sql('begin')
            sql("insert into t values (1, 'must rollback')")
            # Returning simulates EOF/Terminate in the real handler.
        conn._main_loop = disconnect
        conn.handle()
        right.close()
        assert _get_explicit_xid() == 0
        assert not global_vars.xact_manager.active_transactions
        assert sql('select * from t').tuples == []
        sql("insert into t values (2, 'next connection')")
    ''')


def test_concurrent_readers_under_eviction_get_exact_rows(tmp_path):
    path = tmp_path / 'db'
    schema(path)
    run_db(path, '''
        from concurrent.futures import ThreadPoolExecutor
        sql('begin')
        expected = [(i, 'x' * 1000) for i in range(50)]
        for i, value in expected:
            sql("insert into t values (%d, '%s')" % (i, value))
        sql('commit')
        global_vars.buffer_manager.sync()
        global_vars.buffer_manager.reset()
        global_vars.buffer_manager.cache.capacity = 4
        def reader():
            for _ in range(8):
                assert sql('select * from t order by id').tuples == expected
        with ThreadPoolExecutor(3) as pool:
            futures = [pool.submit(reader) for _ in range(3)]
            for future in futures:
                future.result(timeout=10)
    ''')

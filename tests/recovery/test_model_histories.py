"""Seeded reference-model histories checked after actual SIGKILL and two replays."""
import random

import pytest

from tests.recovery.test_crash_recovery import run_db, schema


@pytest.mark.parametrize('seed', [7, 41, 2026])
def test_mixed_dml_commit_rollback_checkpoint_sigkill(seed, tmp_path):
    rng = random.Random(seed)
    committed, plans, next_id = {}, [], 0
    for tx in range(18):
        state, statements = dict(committed), []
        for step in range(6):
            action = rng.choice(['insert', 'update', 'delete']) if state else 'insert'
            value = f's{seed}-t{tx}-n{step}'
            if action == 'insert':
                key, next_id = next_id, next_id + 1
                statements.append(f"insert into t values ({key}, '{value}')")
                state[key] = value
            elif action == 'update':
                key = rng.choice(list(state))
                statements.append(f"update t set v = '{value}' where id = {key}")
                state[key] = value
            else:
                key = rng.choice(list(state))
                statements.append(f'delete from t where id = {key}')
                del state[key]
        decision = 'crash' if tx == 17 else rng.choice(['commit', 'commit', 'rollback'])
        plans.append((statements, decision, bool(tx % 3 == 0)))
        if decision == 'commit':
            committed = state
    path = tmp_path / 'db'
    schema(path)
    run_db(path, f'''
        import signal
        sql('create index idx on t (id)')
        global_vars.buffer_manager.cache.capacity = 3
        for statements, decision, checkpoint in {plans!r}:
            sql('begin')
            for statement in statements:
                sql(statement)
            if checkpoint:
                global_vars.xact_manager.checkpoint()
            if decision == 'crash':
                global_vars.buffer_manager.sync()  # a stolen loser page is allowed
                os.kill(os.getpid(), signal.SIGKILL)
            sql(decision)
    ''', expected=-9)
    expected = sorted(committed.items())
    for _ in range(2):
        run_db(path, f'''
            expected = {expected!r}
            assert sql('select * from t order by id').tuples == expected
            for key, value in expected:
                assert sql('select * from t where id = %d' % key).tuples == [(key, value)]
            index = open_relation(CATALOG_ANDB_CLASS.get_relation_oid('idx', OID_DATABASE_ANDB))
            assert sorted(key[0] for key in bt_scan_all_keys(index)) == [key for key, _ in expected]
            os._exit(0)
        ''')

import threading

from andb.constants.macros import INVALID_XID
from andb.constants.values import QUERY_TERMINATOR
from andb.sql.parser import andb_query_parse, get_ast_type, CmdType
from andb.sql.optimizer import andb_query_plan
from andb.executor.portal import ExecutionPortal
from andb.executor.operator.physical.utility import CommandOperator
from andb.executor.operator.logical import UtilityOperator
from andb.runtime import global_vars
from andb.constants.macros import DUMMY_XID
from andb.errno.errors import RollbackError, FatalError


def tell_session(errno, message):
    #TODO: not using print
    print(errno, message)


# Per-thread explicit transaction state
_thread_local = threading.local()


def _get_explicit_xid():
    return getattr(_thread_local, 'explicit_xid', INVALID_XID)


def _set_explicit_xid(xid):
    _thread_local.explicit_xid = xid


def _is_transaction_command(plan_tree):
    """Check if the plan tree is a transaction command (BEGIN/COMMIT/ROLLBACK)."""
    if isinstance(plan_tree, UtilityOperator) and isinstance(plan_tree.physical_operator, CommandOperator):
        return plan_tree.physical_operator.is_transaction_command
    return False


def _get_transaction_command(plan_tree):
    return plan_tree.physical_operator.command


def _execute_single_query(query):
    """Execute a single SQL statement. Returns ExecutionResult or raises."""
    query = query.strip()
    if not query:
        return None

    ast = andb_query_parse(query)
    plan_tree = andb_query_plan(ast)
    cmd_type = get_ast_type(ast)

    # Handle transaction control commands
    if _is_transaction_command(plan_tree):
        cmd = _get_transaction_command(plan_tree)
        portal = ExecutionPortal(query, cmd_type, plan_tree)

        if cmd == 'begin':
            if _get_explicit_xid() != INVALID_XID:
                tell_session(0, 'WARNING: there is already a transaction in progress')
                return portal.results()
            xid = global_vars.xact_manager.allocate_xid()
            if xid == INVALID_XID:
                tell_session(0, 'cannot get xid')
                return None
            global_vars.xact_manager.begin_transaction(xid)
            _set_explicit_xid(xid)
            return portal.results()

        elif cmd in ('commit',):
            xid = _get_explicit_xid()
            if xid == INVALID_XID:
                tell_session(0, 'WARNING: there is no transaction in progress')
                return portal.results()
            global_vars.xact_manager.commit_transaction(xid)
            _set_explicit_xid(INVALID_XID)
            return portal.results()

        elif cmd in ('rollback', 'abort'):
            xid = _get_explicit_xid()
            if xid == INVALID_XID:
                tell_session(0, 'WARNING: there is no transaction in progress')
                return portal.results()
            global_vars.xact_manager.abort_transaction(xid)
            _set_explicit_xid(INVALID_XID)
            return portal.results()

    # Regular query execution
    portal = ExecutionPortal(query, cmd_type, plan_tree)

    explicit_xid = _get_explicit_xid()
    in_explicit_txn = (explicit_xid != INVALID_XID)

    if in_explicit_txn:
        xid = explicit_xid
    elif cmd_type in (CmdType.CMD_INSERT, CmdType.CMD_DELETE, CmdType.CMD_UPDATE):
        xid = global_vars.xact_manager.allocate_xid()
        if xid == INVALID_XID:
            tell_session(0, 'cannot get xid')
            return None
        global_vars.xact_manager.begin_transaction(xid)
    else:
        xid = DUMMY_XID
        global_vars.xact_manager.begin_transaction(xid)

    try:
        portal.xid = xid
        portal.initialize()
        portal.execute()
        portal.finalize()
    except RollbackError as e:
        if in_explicit_txn:
            global_vars.xact_manager.abort_transaction(xid)
            _set_explicit_xid(INVALID_XID)
        else:
            global_vars.xact_manager.abort_transaction(xid)
        tell_session(e.errno, e.msg)
        raise
    except FatalError as e:
        raise e
    except Exception as e:
        if in_explicit_txn:
            global_vars.xact_manager.abort_transaction(xid)
            _set_explicit_xid(INVALID_XID)
        else:
            global_vars.xact_manager.abort_transaction(xid)
        raise e
    else:
        if not in_explicit_txn:
            global_vars.xact_manager.commit_transaction(xid)

    return portal.results()


def execute_simple_query(query_string):
    """Execute one or more SQL statements separated by semicolons.
    Returns the last result for single statements, or a list of (query, result) pairs
    for multi-statement queries."""
    queries = query_string.split(QUERY_TERMINATOR)
    queries = [q.strip() for q in queries if q.strip()]

    if len(queries) == 0:
        return None

    if len(queries) == 1:
        return _execute_single_query(queries[0])

    results = []
    for query in queries:
        result = _execute_single_query(query)
        results.append((query, result))
    return results

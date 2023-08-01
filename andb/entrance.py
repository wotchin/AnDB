from andb.constants.values import QUERY_TERMINATOR
from andb.sql.parser import andb_query_parse, get_ast_type, CmdType
from andb.sql.optimizer import andb_query_plan
from andb.executor.portal import ExecutionPortal
from andb.runtime import global_vars
from andb.storage.xact.mgr import INVALID_XID, DUMMY_XID
from andb.errno.errors import RollbackError, FatalError


def tell_session(errno, message):
    print(errno, message)


def execute_simple_query(query_string):
    queries = query_string.split(QUERY_TERMINATOR)
    if '' in queries:
        queries.remove('')
    assert len(queries) == 1
    query = queries[0]
    ast = andb_query_parse(query)
    plan_tree = andb_query_plan(ast)
    portal = ExecutionPortal(query, get_ast_type(ast), plan_tree)
    xid = DUMMY_XID  # for select
    if portal.cmd_type in (
            CmdType.CMD_INSERT, CmdType.CMD_DELETE, CmdType.CMD_UPDATE
    ):
        xid = global_vars.xact_manager.allocate_xid()
        if xid == INVALID_XID:
            tell_session(0, 'cannot get xid')
            return
    global_vars.xact_manager.begin_transaction(xid)

    try:
        portal.xid = xid
        portal.initialize()
        portal.execute()
        portal.finalize()
    except RollbackError as e:
        global_vars.xact_manager.abort_transaction(xid)
        tell_session(e.errno, e.msg)
    except FatalError as e:
        # todo: exit
        raise e
    except Exception as e:
        raise e
    else:
        global_vars.xact_manager.commit_transaction(xid)

    print(portal.results())

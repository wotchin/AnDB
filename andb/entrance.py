from andb.constants.macros import INVALID_XID
from andb.constants.strings import QUERY_TERMINATOR
from andb.sql.parser import andb_query_parse, get_ast_type, CmdType
from andb.sql.optimizer import andb_query_plan, andb_get_stages
from andb.executor.portal import ExecutionPortal
from andb.runtime import global_vars
from andb.constants.macros import DUMMY_XID
from andb.errno.errors import RollbackError, FatalError


def tell_session(errno, message):
    #TODO: not used print
    print(errno, message)


def execute_simple_query(query_string):
    queries = query_string.split(QUERY_TERMINATOR)
    if '' in queries:
        queries.remove('')
    assert len(queries) == 1
    query = queries[0]
    ast = andb_query_parse(query)
    list_stages = andb_get_stages(query, ast)
    
    results = None
    
    try:
        for stage in list_stages:
            curr_ast = stage.get_ast()
            plan_tree = andb_query_plan(curr_ast)
            portal = ExecutionPortal(query, get_ast_type(curr_ast), plan_tree)
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
                stage.mark_success()
            except RollbackError as e:
                global_vars.xact_manager.abort_transaction(xid)
                tell_session(e.errno, e.msg)
            except FatalError as e:
                # non-rollbackable error
                raise e
            except Exception as e:
                #TODO: all failure transactions should be aborted
                global_vars.xact_manager.abort_transaction(xid)
                raise e
            else:
                global_vars.xact_manager.commit_transaction(xid)

            #TODO: add error information into result as well
            # and use a protocol to parse and serialize the result
            
            if stage.has_output():
                results = portal.results()
    finally:
        # Perform cleanup for all successfully executed stages with cleanup ASTs
        for stage in list_stages:
            if stage.is_success() and stage.get_cleanup_ast():
                try:
                    cleanup_plan_tree = andb_query_plan(stage.get_cleanup_ast())
                    cleanup_portal = ExecutionPortal(query, get_ast_type(stage.cleanup_ast), cleanup_plan_tree)
                    cleanup_portal.initialize()
                    cleanup_portal.execute()
                    cleanup_portal.finalize()
                except Exception as cleanup_error:
                    # Log cleanup failure but do not re-raise
                    tell_session(0, f"Cleanup failed: {str(cleanup_error)}")
            
    return results


def execute_nl_query(query_string):
    pass

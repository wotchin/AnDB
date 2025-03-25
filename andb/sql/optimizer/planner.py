from .stager import andb_decompose_ast, andb_rewrite_ast
from .transformations import andb_ast_transform
from .implementations import andb_logical_plan_implement

from andb.ai.client_model import ClientModelFactory
from andb.runtime import session_vars

def andb_get_stages(query, ast):
    new_ast = andb_rewrite_ast(query, ast)
    return andb_decompose_ast(new_ast)

def andb_query_plan(ast):
    logical_plan = andb_ast_transform(ast)
    physical_plan = andb_logical_plan_implement(logical_plan)
    return physical_plan    

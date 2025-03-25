from andb.sql.parser import andb_query_parse
from andb.sql.parser.ast.identifier import Identifier
from andb.sql.parser.ast.misc import Constant
from andb.sql.parser.ast.explain import Explain
from andb.sql.parser.ast.insert import Insert
from andb.sql.parser.ast.semantic import Prompt, FileSource, DirectorySource, SemanticTabular
from andb.sql.parser.ast.join import Join, JoinType
from andb.errno.errors import InitializationStageError

from andb.sql.parser.ast.create import CreateMemoryTable, DropMemoryTable
from andb.sql.parser.ast.select import Select
from andb.sql.parser.ast.insert import Insert
from andb.sql.parser.parser_ import CTE, SubQuery

from andb.ai.client_model import ClientModelFactory
from andb.runtime import session_vars

import json
import re
import logging

SETUP = "SETUP"
MAIN_QUERY = "MAIN_QUERY"
CLEANUP = "CLEANUP"

class Stage:
    def __init__(self, stage_type, ast, cleanup_ast=None):
        self.stage_type = stage_type
        self.ast = ast
        self.cleanup_ast = cleanup_ast  # Optional cleanup AST
        self._executed = False
        
    def is_success(self):
        return self._executed

    def mark_success(self):
        self._executed = True
        
    def get_ast(self):
        return self.ast
    
    def get_cleanup_ast(self):
        return self.cleanup_ast
        
    def has_output(self):
        return self.stage_type == MAIN_QUERY

def _create_temp_table_stage(semantic_tabular):
    # Modify the main query AST to reference the temporary table
    temp_table_name = semantic_tabular.identifier.parts
    columns_definition = []
    for prompt_col in semantic_tabular.semantic_schemas:
        assert(isinstance(prompt_col, Prompt) and len(prompt_col.defined_column) == 2)
        field_name, field_type = prompt_col.defined_column
        columns_definition.append(f"{field_name} {field_type}")
    columns_definition_str = ", ".join(columns_definition)
        
    create_temp_table_query = f"CREATE TEMPORARY TABLE {temp_table_name} ({columns_definition_str})" # TODO: Is this dangerous?
    create_table_ast = andb_query_parse(create_temp_table_query)
    
    drop_temp_table_query = f"DROP TEMPORARY TABLE {temp_table_name}" # TODO: Is this dangerous?
    drop_table_ast = andb_query_parse(drop_temp_table_query)

    return Stage(stage_type=SETUP, ast=create_table_ast, cleanup_ast=drop_table_ast)

def _create_with_table_stage(with_table, list_stages:list):
    if with_table.columns is not None:
        for col in with_table.columns:
            assert isinstance(col, Identifier)
            items = col.parts.split('.')
            if len(items) != 1:
                raise InitializationStageError(f"INVALID: {col.parts} WITH TABLE name must not contain dots")
    
    if len(with_table.name.parts.split(',')) != 1:
        raise InitializationStageError(f"INVALID: {with_table.name.parts} TABLE name")

    create_ast = CreateMemoryTable(name=with_table.name,
            columns=with_table.columns,
            temporary=True,
            select_query = with_table.query)
    drop_ast = DropMemoryTable(name=with_table.name)

    create_table_stage = Stage(stage_type=SETUP, ast=create_ast, cleanup_ast=drop_ast)
    
    _create_select_stage(with_table.query, list_stages)
    list_stages.append(create_table_stage)
    insert_ast = Insert(table=with_table.name, columns=None, from_values=with_table.query)
    insert_stage = Stage(stage_type=SETUP, ast=insert_ast)
    list_stages.append(insert_stage)

def _create_main_query_stage(ast):
    return Stage(stage_type=MAIN_QUERY, ast=ast)

def _create_join_stages(semantic_join:Join, list_stages:list):
    if isinstance(semantic_join.left, Join):
        _create_join_stages(semantic_join.left, list_stages)
    elif isinstance(semantic_join.left, SemanticTabular):
        list_stages.append(_create_temp_table_stage(semantic_join.left))
    
    if isinstance(semantic_join.right, SemanticTabular):
        list_stages.append(_create_temp_table_stage(semantic_join.right))
    elif isinstance(semantic_join.right, Select):
        list_stages.append(_create_select_stage(semantic_join.right))

def _create_select_stage(select_ast, list_stages:list):
    if isinstance(select_ast.from_table, SemanticTabular):
        list_stages.append(_create_temp_table_stage(select_ast.from_table))
    elif isinstance(select_ast.from_table, Join):
        _create_join_stages(select_ast.from_table, list_stages)

def _create_cte_stage(cte_ast, list_stages:list):
    for subquery in cte_ast.sub_queries:
        _create_with_table_stage(subquery, list_stages)

def andb_decompose_ast(original_ast):
    """Decompose the original AST into multiple stages.""" # TODO: Generalize
    list_stages = []
    
    curr_ast = original_ast
    if isinstance(original_ast, Explain):
        curr_ast = original_ast.target

    if isinstance(curr_ast, Select):
        _create_select_stage(curr_ast, list_stages)
    elif isinstance(curr_ast, CTE):
        _create_cte_stage(curr_ast, list_stages)
        curr_ast = curr_ast.main_query
        if isinstance(curr_ast, Select):
            _create_select_stage(curr_ast, list_stages)
        elif isinstance(curr_ast, Insert) and isinstance(curr_ast.from_values, Select):
            _create_select_stage(curr_ast.from_values, list_stages)
        original_ast = curr_ast
    elif isinstance(curr_ast, Insert) and isinstance(curr_ast.from_values, Select):
        _create_select_stage(curr_ast.from_values, list_stages)

    list_stages.append(_create_main_query_stage(original_ast))

    return list_stages

def _parse_json(output):
    try:
        # Try parsing directly first
        json_out = json.loads(output)
        if not isinstance(json_out, list):
            return [json_out]
        return json_out
    except json.JSONDecodeError:
        # Clean the output for common issues
        cleaned_output = output.strip()

        # Extract potential JSON objects or arrays
        cleaned_entries = []
        json_object_pattern = re.compile(r'\{.*?}', re.DOTALL)
        entries = json_object_pattern.findall(cleaned_output)
        for entry in entries:
            try:
                # Test if each entry is valid JSON
                json.loads(entry)
                cleaned_entries.append(entry)
            except json.JSONDecodeError:
                # Skip invalid entries
                pass

        # Reconstruct the cleaned JSON array
        cleaned_output = "[" + ",".join(cleaned_entries) + "]"

        # Attempt to parse again
        try:
            return json.loads(cleaned_output)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Error cleaning JSON: {e}")

def _check_if_rewrite_needed(curr_node, attr_name):
    return isinstance(getattr(curr_node, attr_name), FileSource) or isinstance(getattr(curr_node, attr_name), DirectorySource)

def _infer_tabular_schema(query, curr_node, attr_name):
    client_model = ClientModelFactory.create_model(model_type=session_vars.SessionParameter.client_llm,
                                           **session_vars.SessionParameter.__dict__)
    system_prompt = """
        You are a SQL-to-schema extraction specialist. Analyze the given SQL query to identify REQUIRED columns. Return a JSON object with column names as keys and their descriptions as values.
    """

    user_prompt = f"""
        Analyze this hypothetical SQL query:

        QUERY:
        {query}
        
        Your task is to think about the possible column_name and its description to help an LLM convert
        the following document `{str(getattr(curr_node, attr_name))}` into a 2D Tabular data. Therefore,
        all you need to give is the possible schema to be extracted from such document in order for the SQL to be valid SQL query.
        Generate a JSON object with column:description pairs following these requirements:
        - Keys: Column names from original document source
        - Values: Simple descriptions to help as a hint on extraction from the document source
        - Format: Strict JSON (no trailing commas)
        - Specifically if you see UDF **SEM_CLUSTER**, do not include the newly column defined as it will be a new column created.
        That is if if you see SEM_CLUSTER(...) AS area, do not include 'area' as part of your schema.

        Example query:
        SELECT PROMPT(Get me the area from the paper {{title}}) AS area, title, date FROM File('nips.txt');

        Example output:
        {{
            "title": "Paper titles used for topic analysis",
            "date": "Paper's date of publication"
        }}
        Explanation: Extracting title and date of the paper is enough since the UDF relies on title, while date is the other column.
        Area is not needed here since it will rely from column title.

        Your output:
    """
    
    messages = [{"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}]
    inferred_schema = _parse_json(client_model.complete_messages(messages))[0] # Get only first, rest must be garbage
    logging.warning(f"The given SQL query is ambiguous... attempting to guess possible schema: {inferred_schema}")
    inferred_schema = [Prompt(Constant(prompt_text), defined_column=[def_column, "text"]) for def_column, prompt_text in inferred_schema.items()]
    tabular_node = SemanticTabular(identifier=Identifier("inferred_tabular"),
                                   expr_list=inferred_schema,
                                   table_source=getattr(curr_node, attr_name))
    setattr(curr_node, attr_name, tabular_node)

def _traverse_join_ast(query, curr_node):
    if isinstance(curr_node.left, Join):
        _traverse_join_ast(query, curr_node.left)
    # NOTE if there are two FileSource/DirectorySource then it would be ambiguous in terms of the target list; so we don't support that for now
    if _check_if_rewrite_needed(curr_node, "left"):
        _infer_tabular_schema(query, curr_node, attr_name="left")
    elif _check_if_rewrite_needed(curr_node, "right"):
        _infer_tabular_schema(query, curr_node, attr_name="right")
    
def andb_rewrite_ast(query, original_ast):
    # Perform schema inference using LLM if needed        
    curr_ast = original_ast
    if isinstance(original_ast, Explain):
        curr_ast = original_ast.target
    elif isinstance(original_ast, Insert):
        curr_ast = original_ast.from_values
    
    # Check if target_list is all 'Prompt'; all 'Prompt' triggers RAG
    all_prompt = True
    if hasattr(curr_ast, 'targets'):
        for target in curr_ast.targets:
            if not isinstance(target, Prompt):
                all_prompt = False
                break
    
    if isinstance(original_ast, Explain):
        if not all_prompt and hasattr(original_ast.target, "from_table"):
            if _check_if_rewrite_needed(original_ast.target, "from_table"):
                _infer_tabular_schema(query, original_ast.target, attr_name="from_table")
            elif isinstance(original_ast.target.from_table, Join):
                _traverse_join_ast(query, original_ast.target.from_table)
    elif isinstance(original_ast, Insert):
        if not all_prompt and hasattr(original_ast.from_values, "from_table"):
            if _check_if_rewrite_needed(original_ast.from_values, "from_table"):
                _infer_tabular_schema(query, original_ast.from_values, attr_name="from_table")
            elif isinstance(original_ast.from_values.from_table, Join):
                _traverse_join_ast(query, original_ast.from_values.from_table)
    else:
        if not all_prompt and hasattr(original_ast, "from_table"):
            if _check_if_rewrite_needed(original_ast, "from_table"):
                _infer_tabular_schema(query, original_ast, attr_name="from_table")
            elif isinstance(original_ast.from_table, Join):
                _traverse_join_ast(query, original_ast.from_table)

    return original_ast

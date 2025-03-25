import logging
import os
import sys
import time
import json

# add project root to python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from andb.cmd.setup import setup_data_dir
from andb.initializer import init_all_database_components
from andb.entrance import execute_simple_query
from andb.executor.portal import ExecuteResultSet

TEST_DATA_DIRECTOR = os.path.join(os.path.realpath(os.path.dirname(__file__)), 
                                  'local_client_data')
DISCLAIMER_TEXT = """
    #################### NOTICE ####################
    - THIS IS A DEMONSTRATION ENVIRONMENT
    - PRE-GENERATED RESULTS FROM ACTUAL EXECUTIONS
    - OUTPUTS MAY VARY DUE TO MODEL RANDOMNESS
"""

def init_database():
    if os.path.exists(TEST_DATA_DIRECTOR):
        print("data directory already exists, reusing it then.")
    else:
        setup_data_dir(TEST_DATA_DIRECTOR)
    init_all_database_components(TEST_DATA_DIRECTOR)


def execute_demo_query(query):
    json_folder = os.path.join(os.path.realpath(os.path.dirname(os.path.dirname(__file__))), 
                                 'integration')
    time.sleep(1)
    
    if query.startswith("SELECT PROMPT('Name the area of publications"):
        outputs = []
        with open(os.path.join(json_folder, 'result_case1a_run.jsonl'), 'r') as f:
            for line in f.readlines():
                outputs.append(json.loads(line))
        
        result = ExecuteResultSet()
        for output in outputs:
            result.add_tuple(tuple(output))
        fields = [('prompt', 1007, -1, False)]
        result.define_fields(fields)
        
        print(result)
    elif query.startswith("SELECT SEM_GROUP(title, PROMPT('Area of publication of the paper'), 5) AS area, COUNT(title) FROM File('neurips_2024.txt') GROUP BY area"):
        outputs = []
        with open(os.path.join(json_folder, 'result_case1b_run.jsonl'), 'r') as f:
            for line in f.readlines():
                outputs.append(json.loads(line))
                
        result = ExecuteResultSet()
        for output in outputs:
            result.add_tuple(tuple(output))
        fields = [('title', 1007, -1, False), ('count', 1007, -1, False)]
        result.define_fields(fields)
        print(result)
    elif query.startswith("SELECT SEM_CLUSTER(title, PROMPT('Area of publication of the paper'), 5) AS area, COUNT(title) FROM TABULAR(P"):
        outputs = []
        with open(os.path.join(json_folder, 'result_case1c_run.jsonl'), 'r') as f:
            for line in f.readlines():
                outputs.append(json.loads(line))

        result = ExecuteResultSet()
        for output in outputs:
            result.add_tuple(tuple(output))
        fields = [('title', 1007, -1, False), ('count', 1007, -1, False)]
        result.define_fields(fields)
        print(result)
    elif query.startswith("WITH"):
        outputs = []
        with open(os.path.join(json_folder, 'result_case2_run.jsonl'), 'r') as f:
            for line in f.readlines():
                outputs.append(json.loads(line))

        result = ExecuteResultSet()
        for output in outputs:
            result.add_tuple(tuple(output))
        fields = [('area', 1007, -1, False), ('count2024', 1007, -1, False), ('count2023', 1007, -1, False)]
        result.define_fields(fields)
        print(result)
    else:
        print("Error")
    
def run_shell():
    init_database()
    print(DISCLAIMER_TEXT)
    print("Welcome to AnDB shell. Enter SQL statements terminated by ';'")
    print("Type 'exit;' to quit")
    
    while True:
        try:
            # collect user input until encountering a semicolon
            query = ""
            while not query.strip().endswith(';'):
                line = input('andb> ' if not query else '... ')
                if not line:  # empty line continue
                    continue
                query += line + ' '
                
            if query.strip() == 'exit;':
                break
                
            # execute the query
            execute_demo_query(query.strip()[:-1])  # remove trailing semicolon
        except KeyboardInterrupt:
            print("\nCtrl+C pressed. Type 'exit;' to quit.")
        except Exception as e:
            print(f"Error: {e}")

run_shell()

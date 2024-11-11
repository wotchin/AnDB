import logging
import os
import sys
import shutil

# add project root to python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from andb.cmd.setup import setup_data_dir
from andb.initializer import init_all_database_components
from andb.entrance import execute_simple_query

TEST_DATA_DIRECTOR = os.path.realpath('local_client_data')

def init_database():
    if os.path.exists(TEST_DATA_DIRECTOR):
        print("data directory already exists, reusing it then.")
    else:
        setup_data_dir(TEST_DATA_DIRECTOR)
    init_all_database_components(TEST_DATA_DIRECTOR)

def run_shell():
    init_database()
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
                query += line + " "
            
            query = query.strip()
            if query.lower() == 'exit;':
                break
                
            # execute query
            result = execute_simple_query(query)
            print(result)
            
        except KeyboardInterrupt:
            print("\nKeyboardInterrupt")
            continue
        except Exception as e:
            print(f"Error: {str(e)}")
            logging.exception(e)
            continue

if __name__ == '__main__':
    run_shell()

import logging
import os
import json
import sys

# add project root to python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from andb.cmd.setup import setup_data_dir
from andb.initializer import init_all_database_components
from andb.entrance import execute_simple_query
from andb.catalog.type import AndbNull

TEST_DATA_DIRECTOR = os.path.join(os.path.realpath(os.path.dirname(__file__)), 
                                  'local_client_data')

logging.basicConfig(
    stream=sys.stdout,  # Send logs to stdout so nohup captures them
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

SETUP_QUERIES = [
    'set external_api_key="None";',
]

def init_database():
    if os.path.exists(TEST_DATA_DIRECTOR):
        print("data directory already exists, reusing it then.")
    else:
        setup_data_dir(TEST_DATA_DIRECTOR)
    init_all_database_components(TEST_DATA_DIRECTOR)

def save_to_jsonl(results, output_file):
    """Saves query results to a JSONL file."""
    with open(output_file, "w", encoding="utf-8") as f:
        for tup in results:
            new_tup = []
            for t in tup:
                if isinstance(t, AndbNull):
                    new_tup.append(None)
                else:
                    new_tup.append(t)
            new_tup = tuple(new_tup)
            f.write(json.dumps(new_tup) + "\n")

def test_case1a():
    query1 = "SELECT PROMPT('Name the area of publications and count the number of publications in each area.') \
              FROM FILE('neurips_2024.txt');"

    exception_occur = False
    try:
        result1_run = execute_simple_query(query1).tuples
    except Exception as e:
        logging.warning("Exception occured!", e)
        exception_occur = True

    if not exception_occur:
        logging.warning("Saving to 1a!")
        save_to_jsonl(result1_run, f"result_case1a_run.jsonl")
    
def test_case1b():
    query1 = "SELECT SEM_CLUSTER(title, PROMPT('Area of publication of the paper'), 5) AS area, COUNT(title) \
              FROM File('neurips_2024.txt') GROUP BY area;"

    exception_occur = False
    try:
        result1_run = execute_simple_query(query1).tuples
    except Exception as e:
        logging.warning("Exception occured!", e)
        exception_occur = True
    
    if not exception_occur:
        logging.warning("Saving for case1b!")
        save_to_jsonl(result1_run, f"result_case1b_run.jsonl")
    
def test_case1c():
    query1 = "SELECT SEM_CLUSTER(title, PROMPT('Area of publication of the paper'), 5) AS area, COUNT(title) \
              FROM TABULAR(PROMPT('Authors of the paper') AS author text, \
              PROMPT('Title of the paper') AS title text FROM \
              File('neurips_2024.txt')) neurips2024 GROUP BY area;"

    exception_occur = False
    try:
        result1_run = execute_simple_query(query1).tuples
    except Exception as e:
        logging.warning("Exception occured!", e)
        exception_occur = True
    
    if not exception_occur:
        logging.warning("Saving to 1c!")
        save_to_jsonl(result1_run, f"result_case1c_run.jsonl")
    
def test_case2():
    query1 = """
        WITH 
        neurips2024 AS (
            SELECT SEM_CLUSTER(title, PROMPT('The area of publication'), 5) AS area, COUNT(title) AS title_count
            FROM TABULAR (
                PROMPT('title of the paper') as title text
                FROM FILE('neurips_2024.txt')
            ) scanned_2024
            GROUP BY area
        ), 
        neurips_2023 AS (
            SELECT SEM_CLUSTER(title, PROMPT('The area of publication'), 5) AS area, COUNT(title) AS title_count
            FROM TABULAR (
                PROMPT('title of the paper') as title text
                FROM FILE('neurips_2023.txt')
            ) scanned_2023
            GROUP BY area
        )
    SELECT neurips2024.area, neurips2024.title_count, neurips_2023.title_count
    FROM neurips2024 INNER JOIN neurips_2023
    ON SEM_MATCH('{neurips2024.area} and {neurips_2023.area} are the same technical area from both documents.', 0.8);"""
    
    exception_occur = False
    try:
        result1_run = execute_simple_query(query1).tuples
    except Exception as e:
        logging.warning("Exception occured!", e)
        exception_occur = True

    if not exception_occur:
        logging.warning("Saving to 2!")
        save_to_jsonl(result1_run, f"result_case2_run.jsonl")

if __name__ == '__main__':
    init_database()
    
    # Warmup
    for q in SETUP_QUERIES:
        execute_simple_query(q)

    test_case1a()
    test_case1b()
    test_case1c()
    test_case2()   

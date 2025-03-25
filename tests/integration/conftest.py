import os
import shutil
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import pytest

from andb.cmd.setup import setup_data_dir
from andb.initializer import init_all_database_components
from andb.runtime import session_vars
from andb.entrance import execute_simple_query

TEST_DATA_DIRECTORY = os.path.join(os.path.realpath(os.path.dirname(__file__)),
                                   'test_data')
TEST_DATASET_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dataset')


def setup(sql_file_setup_path=None):
    if os.path.exists(TEST_DATA_DIRECTORY):
        shutil.rmtree(TEST_DATA_DIRECTORY)
    setup_data_dir(TEST_DATA_DIRECTORY)

    init_all_database_components(TEST_DATA_DIRECTORY)

    db_oid = session_vars.get_session_value('database_oid')
    dest_dir = os.path.join(TEST_DATA_DIRECTORY, f'base/{db_oid}/files')
    os.makedirs(dest_dir, exist_ok=True)

    shutil.copytree(TEST_DATASET_DIR, dest_dir, dirs_exist_ok=True)

    # Read and execute SQL queries from the provided SQL file
    if sql_file_setup_path:
        if not os.path.exists(sql_file_setup_path):
            raise FileNotFoundError(f"SQL setup file not found: {sql_file_setup_path}")

        with open(sql_file_setup_path, "r") as sql_file:
            for query in sql_file:
                query = query.strip()
                if query:
                    execute_simple_query(query)


def teardown():
    shutil.rmtree(TEST_DATA_DIRECTORY, ignore_errors=True)


def assert_equal_tuples(tuples1, tuples2):
    # Ensure the first tuples (columns) are identical
    assert tuples1[0] == tuples2[0], f"Column names do not match: {tuples1[0]} != {tuples2[0]}"

    # Extract data rows (ignoring the first tuple)
    data1 = tuples1[1:]
    data2 = tuples2[1:]

    # Sort the data rows for order-independent comparison
    sorted_data1 = sorted(data1)
    sorted_data2 = sorted(data2)

    # Compare the sorted data
    assert sorted_data1 == sorted_data2, f"Data rows do not match!"


@pytest.fixture(scope='session')
def setup_and_teardown(request):
    sql_file_path = request.config.getoption("--sql-setup-file", default=None)
    setup(sql_file_path)

    yield

    teardown()


# To use, optionally provide sql-setup-file as an SQL file to run some initial queries to setup the database
# e.g. setting up client model
def pytest_addoption(parser):
    parser.addoption(
        "--sql-setup-file",
        action="store",
        default=None,
        help="Optional path to the SQL setup file"
    )

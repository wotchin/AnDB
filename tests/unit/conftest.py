import os
import shutil

import pytest

from andb.cmd.setup import setup_data_dir
from andb.initializer import init_all_database_components

TEST_DATA_DIRECTOR = os.path.join(os.path.realpath(os.path.dirname(__file__)), 
                                  'test_data')

def setup():
    if os.path.exists(TEST_DATA_DIRECTOR):
        shutil.rmtree(TEST_DATA_DIRECTOR)
    setup_data_dir(TEST_DATA_DIRECTOR)

    init_all_database_components(TEST_DATA_DIRECTOR)


def teardown():
    shutil.rmtree(TEST_DATA_DIRECTOR, ignore_errors=True)


@pytest.fixture(scope='session', autouse=True)
def run_before_and_after_test_case():
    setup()

    yield

    teardown()

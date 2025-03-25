import os
import sys
import shutil
#https://learnsql.com/blog/basic-sql-query-examples/

# add project root to python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from andb.cmd.setup import setup_data_dir
from andb.initializer import init_all_database_components
from andb.catalog.type import AndbNull
import pytest

TEST_DATA_DIRECTOR = os.path.join(os.path.realpath(os.path.dirname(__file__)), 
                                  'local_client_data')

def init_database():
    if os.path.exists(TEST_DATA_DIRECTOR):
        shutil.rmtree(TEST_DATA_DIRECTOR)
    setup_data_dir(TEST_DATA_DIRECTOR)
    init_all_database_components(TEST_DATA_DIRECTOR)

TYPE_OIDS = {'text':1007, 'int':1000, 'float':1002, None:0}
def compare_select(fields, tuples, result):
    for form in result.attr_forms:
        target_field = fields[form.num]
        assert target_field[0] == form.name
        assert TYPE_OIDS[target_field[1]] == form.type_oid
        
    assert len(result.tuples) == len(tuples)
    for i, row in enumerate(result.tuples):
        assert len(row) == len(tuples[i])
        for j, val in enumerate(row):
            if isinstance(val, float):
                assert val == pytest.approx(tuples[i][j])
            elif isinstance(val, AndbNull):
                assert tuples[i][j] is None
            else:
                assert val == tuples[i][j]

def getInsertValues(expected, idxs):
    insert_tuples = ""
    for tup in expected:
        value = '('
        for ind in idxs:
            elem = tup[ind]
            if isinstance(elem, str):
                value += f"'{elem}'"
            else:
                value += str(elem)
            value += ','
        insert_tuples += value[:-1] + '),'

    return insert_tuples[:-1] + ';'
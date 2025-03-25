from andb.entrance import execute_simple_query
from conftest import setup_and_teardown, assert_equal_tuples

def test_groupby_excessive_k(setup_and_teardown):
    query1 = "SELECT SEM_CLUSTER(title, PROMPT('topics of the paper'), 100) AS topic, COUNT(title) \
    FROM TABULAR(PROMPT('authors of the paper') AS author text, \
                PROMPT('title of the paper') AS title text FROM \
                File('nips_2024_small.txt')) nips \
    GROUP BY topic;"

    result1_first_run = execute_simple_query(query1).tuples
    result1_second_run = execute_simple_query(query1).tuples
    
    assert(len(result1_first_run) == 1) # At least, but only check length since it's not deterministic
    for tup in result1_first_run:
        assert(len(tup) == 2)
    
    assert_equal_tuples(result1_first_run, result1_second_run)

def test_groupby_proper_k(setup_and_teardown):
    query1 = "SELECT SEM_CLUSTER(title, PROMPT('topics of the paper'), 5) AS topic, COUNT(title) \
    FROM TABULAR(PROMPT('authors of the paper') AS author text, \
                PROMPT('title of the paper') AS title text FROM \
                File('nips_2024_small.txt')) nips \
    GROUP BY topic;"

    result1_first_run = execute_simple_query(query1).tuples
    result1_second_run = execute_simple_query(query1).tuples
    
    assert(len(result1_first_run) == 5 or len(result1_first_run) == 6) # At least, but only check length since it's not deterministic
    for tup in result1_first_run:
        assert(len(tup) == 2)
    
    assert_equal_tuples(result1_first_run, result1_second_run)

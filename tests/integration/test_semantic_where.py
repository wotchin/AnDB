from andb.entrance import execute_simple_query
from conftest import setup_and_teardown, assert_equal_tuples

def test_simple_where(setup_and_teardown):
    query1 = "SELECT * \
            FROM TABULAR(PROMPT('authors of the paper') AS author text, \
            PROMPT('title of the paper') AS title text FROM File('nips_2024_small.txt')) nips \
            WHERE SEM_MATCH('the topic of the paper {title} is planning');"

    result1_first_run = execute_simple_query(query1).tuples
    result1_second_run = execute_simple_query(query1).tuples
    
    assert(len(result1_first_run) >= 1) # It should be deterministic (only 1 paper), but sometimes doesn't follow instruction
    for tup in result1_first_run:
        assert(len(tup) == 2)
    
    assert_equal_tuples(result1_first_run, result1_second_run)
    
def test_challenging_where(setup_and_teardown):
    query1 = "SELECT * \
                FROM TABULAR(PROMPT('authors of the paper') AS author text, \
                            PROMPT('title of the paper') AS title text FROM \
                            File('nips_2024_small.txt')) nips \
              WHERE SEM_MATCH('paper {title} introduces a new benchmark dataset for vision tasks.');"

    result1_first_run = execute_simple_query(query1).tuples
    result1_second_run = execute_simple_query(query1).tuples
    
    assert(len(result1_first_run) > 1) # At least 2, but ideally should be 3
    for tup in result1_first_run:
        assert(len(tup) == 2)
    
    assert_equal_tuples(result1_first_run, result1_second_run)
    
def test_where_with_float_threshold(setup_and_teardown):
    query1 = "SELECT * \
            FROM TABULAR(PROMPT('authors of the paper') AS author text, \
            PROMPT('title of the paper') AS title text FROM File('nips_2024_small.txt')) nips \
            WHERE SEM_MATCH('the topic of the paper {title} is planning', 0.8);"

    result1_first_run = execute_simple_query(query1).tuples
    result1_second_run = execute_simple_query(query1).tuples
    
    assert(len(result1_first_run) >= 1) # It should be deterministic (only 1 paper), but sometimes doesn't follow instruction
    for tup in result1_first_run:
        assert(len(tup) == 2)
    
    assert_equal_tuples(result1_first_run, result1_second_run)
    
def test_where_with_int_threshold(setup_and_teardown):
    query1 = "SELECT * \
            FROM TABULAR(PROMPT('authors of the paper') AS author text, \
            PROMPT('title of the paper') AS title text FROM File('nips_2024_small.txt')) nips \
            WHERE SEM_MATCH('paper {title} introduces a new benchmark dataset for vision tasks.', 3);"

    result1_first_run = execute_simple_query(query1).tuples
    result1_second_run = execute_simple_query(query1).tuples
    
    assert(1 <= len(result1_first_run) <= 3) # It should be deterministic (only 1 paper), but sometimes doesn't follow instruction
    for tup in result1_first_run:
        assert(len(tup) == 2)
    
    assert_equal_tuples(result1_first_run, result1_second_run)

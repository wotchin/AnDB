from andb.entrance import execute_simple_query
from conftest import setup_and_teardown, assert_equal_tuples

def test_one_column_table(setup_and_teardown):
    query1 = "SELECT author \
        FROM TABULAR(PROMPT('authors of the paper') AS author text \
        FROM File('nips_2024_small.txt')) nips;"

    result1_first_run = execute_simple_query(query1).tuples
    result1_second_run = execute_simple_query(query1).tuples
    
    assert(len(result1_first_run) > 15) # At least, but only check length since it's not deterministic
    for tup in result1_first_run:
        assert(len(tup) == 1)
    
    assert_equal_tuples(result1_first_run, result1_second_run)
    
def test_star_one_column_table(setup_and_teardown):   
    query2 = "SELECT * \
        FROM TABULAR(PROMPT('authors of the paper') AS author text \
        FROM File('nips_2024_small.txt')) nips;"
    result2_first_run = execute_simple_query(query2).tuples
    result2_second_run = execute_simple_query(query2).tuples

    assert(len(result2_first_run) > 15) # At least, but only check length since it's not deterministic
    for tup in result2_first_run:
        assert(len(tup) == 1)
    
    assert_equal_tuples(result2_first_run, result2_second_run)

def test_many_columns_table(setup_and_teardown):
    query1 = "SELECT author, title \
        FROM TABULAR(PROMPT('authors of the paper') AS author text, \
                     PROMPT('title of the paper') AS title text \
        FROM File('nips_2024_small.txt')) nips;"
    result1_first_run = execute_simple_query(query1).tuples
    result1_second_run = execute_simple_query(query1).tuples
    
    assert(len(result1_first_run) > 15) # At least, but only check length since it's not deterministic
    for tup in result1_first_run:
        assert(len(tup) == 2)
    
    assert_equal_tuples(result1_first_run, result1_second_run)
    
def test_star_many_columns_table(setup_and_teardown):
    query2 = "SELECT * \
        FROM TABULAR(PROMPT('authors of the paper') AS author text, \
                     PROMPT('title of the paper') AS title text \
        FROM File('nips_2024_small.txt')) nips;"
    result2_first_run = execute_simple_query(query2).tuples
    result2_second_run = execute_simple_query(query2).tuples
    
    assert(len(result2_first_run) > 15) # At least, but only check length since it's not deterministic
    for tup in result2_first_run:
        assert(len(tup) == 2)
    
    assert_equal_tuples(result2_first_run, result2_second_run)

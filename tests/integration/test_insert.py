from conftest_operators import init_database, TEST_DATA_DIRECTOR, getInsertValues, compare_select
from andb.entrance import execute_simple_query
import shutil
import pytest
import time

#data retrieved from https://learnsql.com/blog/basic-sql-query-examples/
EMPLOYEE_TUPLES = [(1, 'Paul', 'Garrix', 'Corporate', 3547.25),\
                    (2, 'Astrid', 'Fox', 'Private Individuals', 2845.56),\
                    (3, 'Matthias', 'Johnson', 'Private Individuals', 3009.41), \
                    (4, 'Lucy', 'Patterson', 'Private Individuals', 3547.25), \
                    (5, 'Tom', 'Page', 'Corporate', 5974.41), \
                    (6, 'Claudia', 'Conte', 'Corporate', 4714.12),\
                    (7, 'Walter', 'Deer', 'Private Individuals', 3547.25),\
                    (8, 'Stephanie', 'Marx', 'Corporate', 2894.51),\
                    (9, 'Luca', 'Pavarotti', 'Private Individuals', 4123.45),\
                    (10, 'Victoria', 'Pollock', 'Corporate', 4789.53),\
                    (12, 'Lola', 'Tung', None, 2500.75)]
FLIGHT_TUPLES = [(0, 'New York', 'Los Angeles', 1030, 1330, 'Upcoming'),\
                (1, 'Chicago', 'Miami', 1115, 1410, 'Onboarding'),\
                (2, 'San Francisco', 'Seattle', 1230, 1405, 'Departed'),\
                (3, 'London', 'Paris', 900, 1030, 'Landed')]

class TestInsert:
    employee_fields = [('employees.id', 'int'), ('employees.first_name', 'text'), ('employees.last_name', 'text'), ('employees.department', 'text'), ('employees.salary', 'float')]
    flight_fields = [('flights.flight_no', 'int'), ('flights.source', 'text'), ('flights.destination', 'text'), ('flights.depart_time', 'int'), ('flights.land_time', 'int'), ('flights.status', 'text')]
    def setup_class(cls):
        init_database()

    def setup_method(self, method):
        create_table1 = 'CREATE TABLE employees(id int NOT NULL, first_name text NOT NULL, last_name text NOT NULL, department ENUM("Corporate", "Private Individuals"), salary float NOT NULL);'
        execute_simple_query(create_table1)
        create_table2 = 'CREATE TABLE flights(flight_no int NOT NULL, source text NOT NULL, destination text NOT NULL, depart_time int, land_time int, '\
         'status ENUM("Upcoming", "Onboarding", "Departed", "Landed") NOT NULL);'
        execute_simple_query(create_table2)

    def teardown_method(self, method):
        execute_simple_query('DROP TABLE employees;')
        execute_simple_query('DROP TABLE flights;')

    def test_insert_values(self):
        expected = EMPLOYEE_TUPLES[0:5]
        query = "INSERT INTO employees VALUES " + getInsertValues(expected, range(0, 5))
        
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        select_output = execute_simple_query("select * from employees;")
        compare_select(self.employee_fields, expected, select_output)

    def test_insert_values_columns_defined(self):
        expected = EMPLOYEE_TUPLES[5:10]
        idxs = [2, 1, 4, 3, 0]
        query = "INSERT INTO employees(last_name, first_name, salary, department, id) VALUES " + getInsertValues(expected, idxs)

        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        select_output = execute_simple_query("select * from employees;")
        compare_select(self.employee_fields, expected, select_output)

    def test_insert_invalid_enum(self):
        query = "INSERT INTO employees VALUES (11, 'Caine', 'Carraway', 'Freelance', 4000.00)"
        with pytest.raises(Exception):
            execute_simple_query(query)
        
        select_output = execute_simple_query("select * from employees;")
        compare_select(self.employee_fields, [], select_output)

    def test_insert_valid_null(self):
        expected = [EMPLOYEE_TUPLES[-1]]
        idxs = [0, 1, 2, 4]
        query = "INSERT INTO employees(id, first_name, last_name, salary) VALUES " + getInsertValues(expected, idxs)

        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        select_output = execute_simple_query("select * from employees;")
        compare_select(self.employee_fields, expected, select_output)

    def test_insert_invalid_null(self):
        query = "INSERT INTO employees(id, first_name, last_name, department) VALUES (13, 'Luigi', 'Menendex', 'Corporate')"
        with pytest.raises(Exception):
            execute_simple_query(query)
        
        select_output = execute_simple_query("select * from employees;")
        compare_select(self.employee_fields, [], select_output)

    def test_insert_enum(self):
        expected = FLIGHT_TUPLES
        query = "INSERT INTO flights VALUES " + getInsertValues(expected, range(0, 6))

        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        select_output = execute_simple_query("select * from flights;")
        compare_select(self.flight_fields, expected, select_output)

    def test_insert_enum_invalid_null(self):
        query = "INSERT INTO flights(flight_no, source, destination, depart_time, land_time) VALUES (10, 'Iowa', 'Texas', 1030, 1423);"
        with pytest.raises(Exception):
            execute_simple_query(query)
        
        select_output = execute_simple_query("select * from flights;")
        compare_select(self.flight_fields, [], select_output)

class TestInsertSelect:
    def setup_class(cls):
        create_table1 = 'CREATE TABLE employees(id int NOT NULL, first_name text NOT NULL, last_name text NOT NULL, department ENUM("Corporate", "Private Individuals"), salary float NOT NULL);'
        execute_simple_query(create_table1)
        query = "INSERT INTO employees VALUES " + getInsertValues(EMPLOYEE_TUPLES[:-1], range(0, len(EMPLOYEE_TUPLES[0])))
        output = execute_simple_query(query)
        assert output.effect_rows == len(EMPLOYEE_TUPLES) - 1
        
        query = "INSERT INTO employees(id, first_name, last_name, salary) VALUES " + getInsertValues([EMPLOYEE_TUPLES[-1]], [0, 1, 2, 4])
        output = execute_simple_query(query)
        assert output.effect_rows == 1

        create_table2 = 'CREATE TABLE flights(flight_no int NOT NULL, source text NOT NULL, destination text NOT NULL, depart_time int, land_time int, '\
         'status ENUM("Upcoming", "Onboarding", "Departed", "Landed") NOT NULL);'
        execute_simple_query(create_table2)
        query = "INSERT INTO flights VALUES " + getInsertValues(FLIGHT_TUPLES, range(0, len(FLIGHT_TUPLES[0])))
        output = execute_simple_query(query)
        assert output.effect_rows == len(FLIGHT_TUPLES)

        create_table3 = 'CREATE TABLE private_employees(first_name text NOT NULL, last_name text NOT NULL);'
        execute_simple_query(create_table3)

    def teardown_class(cls):
        execute_simple_query('DROP TABLE employees;')
        execute_simple_query('DROP TABLE private_employees;')
        execute_simple_query('DROP TABLE corporate_employees;')
        execute_simple_query('DROP TABLE strict_employees;')
        execute_simple_query('DROP TABLE flights;')
        execute_simple_query('DROP TABLE active_flights;')

    def test_insert_select(self):
        create_table = 'CREATE TABLE corporate_employees(first_name text NOT NULL, last_name text NOT NULL, id int NOT NULL, salary float NOT NULL);'
        insert_select = 'INSERT INTO corporate_employees select first_name, last_name, id, salary from employees where department="Corporate";'
        
        execute_simple_query(create_table)
        output_insert = execute_simple_query(insert_select)
        assert output_insert.effect_rows == 5

        output_select = execute_simple_query("select * from corporate_employees;")
        assert output_select.effect_rows == 5

        select_tuples = []
        idxs = [1, 2, 0, 4]
        for tup in EMPLOYEE_TUPLES:
            insert_tup = []
            if tup[3] == 'Corporate':
                for ind in idxs:
                    insert_tup.append(tup[ind])
                select_tuples.append(tuple(insert_tup))

        output_fields = [('corporate_employees.first_name', 'text'), ('corporate_employees.last_name', 'text'), ('corporate_employees.id', 'int'), ('corporate_employees.salary', 'float')]
        
        compare_select(output_fields, select_tuples, output_select)

    def test_insert_invalid_num_cols(self):
        query = 'INSERT INTO private_employees select first_name from employees where department="Private Individuals";'
        with pytest.raises(Exception):
            execute_simple_query(query)
        
        output_fields = [('private_employees.first_name', 'text'), ('private_employees.last_name', 'text')]
        select_output = execute_simple_query("select * from private_employees;")
        compare_select(output_fields, [], select_output)
    
    def test_insert_invalid_type_cols(self):
        query = 'INSERT INTO private_employees select first_name, id from employees where department="Private Individuals";'
        with pytest.raises(Exception):
            execute_simple_query(query)
        
        output_fields = [('private_employees.first_name', 'text'), ('private_employees.last_name', 'text')]
        select_output = execute_simple_query("select * from private_employees;")
        compare_select(output_fields, [], select_output)

    def test_insert_invalid_enum_null(self):
        create_table = 'CREATE TABLE strict_employees(id int NOT NULL, first_name text NOT NULL, last_name text NOT NULL, department ENUM("Corporate", "Private Individuals") NOT NULL, salary float NOT NULL);'
        execute_simple_query(create_table)

        query = 'INSERT INTO strict_employees select * from employees;'
        with pytest.raises(Exception):
            execute_simple_query(query)
        
        output_fields = [('strict_employees.id', 'int'), ('strict_employees.first_name', 'text'), ('strict_employees.last_name', 'text'), ('strict_employees.department', 'text'), ('strict_employees.salary', 'float')]
        select_output = execute_simple_query("select * from strict_employees;")
        compare_select(output_fields, [], select_output)

    def test_insert_invalid_enum(self):
        create_table = 'CREATE TABLE active_flights(flight_no int NOT NULL, source text NOT NULL, destination text NOT NULL, depart_time int, land_time int, '\
         'status ENUM("Upcoming", "Onboarding", "Departed") NOT NULL);'
        execute_simple_query(create_table)

        query = 'INSERT INTO active_flights select * from flights;'
        with pytest.raises(Exception):
            execute_simple_query(query)
        
        output_fields = [('active_flights.flight_no', 'int'), ('active_flights.source', 'text'), ('active_flights.destination', 'text'), ('active_flights.depart_time', 'int'), ('active_flights.land_time', 'int'), ('active_flights.status', 'text')]
        select_output = execute_simple_query("select * from active_flights;")
        compare_select(output_fields, [], select_output)
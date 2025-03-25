from conftest_operators import init_database, TEST_DATA_DIRECTOR, getInsertValues, compare_select
from andb.entrance import execute_simple_query
import shutil
import pytest


COURSES = [ ('Calculus', 1, 'Davis', 23, 62),\
            ('Calculus', 1, 'Stangy', 30, 55),\
            ('Data Structs and Algo', 1, 'Velma', 36, 67),\
            ('Data Structs and Algo', 1, 'Chris', 37, 52),\
            ('Programming on the Web', 4, 'Jenny', 28, 76),\
            ('Data Structs and Algo', 2, 'Tony', 73, 73),\
            ('Data Structs and Algo', 3, 'Vicky', 21, 68),\
            ('Data Structs and Algo', 3, 'Jenny', 25, 62),\
            ('Advanced Physics', 2, 'Davis', 24, 62),\
            ('Advanced Physics', 3, 'Robert', 29, 58),\
            ('Intro to ML', 4, 'Khan', 32, 68),\
            ('Calculus', 2, 'Dawson', 34, 63),\
            ('Calculus', 2, 'Helene', 36, 67),\
            ('Calculus', 2, 'Davis', 18, 61),\
            ('Calculus', 2, 'Dawson', 13, 62)]

class TestSelect:
    def setup_class(cls):
        init_database()
        create_table1 = 'CREATE TABLE courses(name text NOT NULL, year int NOT NULL, teacher text NOT NULL, num_students int NOT NULL, median_mark int NOT NULL);'
        execute_simple_query(create_table1)

        query = "INSERT INTO courses VALUES " + getInsertValues(COURSES, range(0, len(COURSES[0])))
        output = execute_simple_query(query)
        assert output.effect_rows == len(COURSES)

    def teardown_class(cls):
        execute_simple_query('DROP TABLE courses;')

    def test_group_sum(self):
        students = {}
        for tup in COURSES:
            count = students.get(tup[0], 0)
            students[tup[0]] = count + tup[-2]
        expected = list(students.items())
        expected.sort(key=lambda x:x[0])
        
        query = "select name, SUM(num_students) as num_students FROM courses GROUP BY name ORDER BY name;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.name', 'text'), ('num_students', None)]
        compare_select(fields, expected, output)

    def test_group_count_avg(self):
        teachers = {}
        for tup in COURSES:
            count, mark = teachers.get(tup[2], (0, 0))
            teachers[tup[2]] = (count + 1, mark + tup[-1])
        
        expected = [(key, val[0], val[1]/val[0]) for key, val in teachers.items()]
        expected.sort(key=lambda x:x[0])
        
        query = "select teacher, COUNT(median_mark) as num_classes, AVG(median_mark) as avg_mark FROM courses GROUP BY teacher ORDER BY teacher;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.teacher', 'text'), ('num_classes', None), ('avg_mark', None)]
        compare_select(fields, expected, output)

    def test_group_mult(self):
        classes = {}
        for tup in COURSES:
            count = classes.get((tup[0], tup[1]), 0)
            classes[(tup[0], tup[1])] = count + 1
        
        expected = [(key[0], key[1], val) for key, val in classes.items()]
        expected.sort(key=lambda x:(x[0], x[1]))
        
        query = "select name, year, COUNT(teacher) as num_classes FROM courses GROUP BY name, year ORDER BY name, year;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.name', 'text'), ('courses.year', 'int'), ('num_classes', None)]
        compare_select(fields, expected, output)
    
    def test_group_mult_func(self):
        classes = {}
        for tup in COURSES:
            count, sum, min_mark, max_mark = classes.get((tup[0], tup[1]), (0, 0, 100, 0))
            classes[(tup[0], tup[1])] = (count + 1, sum + tup[-1], min(min_mark, tup[-1]), max(max_mark, tup[-1]))
        
        expected = [(key[0], key[1], val[1]/val[0], val[2], val[3]) for key, val in classes.items()]
        expected.sort(key=lambda x:(x[0], x[1]))

        query = "select name, year, AVG(median_mark) as avg_median, MIN(median_mark) as min_median, MAX(median_mark) as max_median FROM courses GROUP BY name, year ORDER BY name, year;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.name', 'text'), ('courses.year', 'int'), ('avg_median', None), ('min_median', None), ('max_median', None)]
        compare_select(fields, expected, output)

    def test_distinct(self):
        teachers = set([tup[2] for tup in COURSES])
        expected = [(val,) for val in teachers]
        expected.sort(key=lambda x:x[0])

        query = "select DISTINCT teacher FROM courses ORDER BY teacher;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.teacher', 'text')]
        compare_select(fields, expected, output)

    def test_distinct_mult(self):
        teachers = set([(tup[0], tup[2]) for tup in COURSES])
        expected = [val for val in teachers]
        expected.sort(key=lambda x:(x[0], x[1]))

        query = "select DISTINCT name, teacher FROM courses ORDER BY name, teacher;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.name', 'text'), ('courses.teacher', 'text')]
        compare_select(fields, expected, output)

    def test_where_gt(self):
        teachers = set([(tup[0], tup[1]) for tup in COURSES if tup[1] > 2])
        expected = [val for val in teachers]
        expected.sort(key=lambda x:(x[0], x[1]))

        query = "select DISTINCT name, year FROM courses where year > 2 ORDER BY name, year;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.name', 'text'), ('courses.year', 'int')]
        compare_select(fields, expected, output)

    def test_where_lte(self):
        teachers = set([(tup[0], tup[1]) for tup in COURSES if tup[1] <= 2])
        expected = [val for val in teachers]
        expected.sort(key=lambda x:(x[0], x[1]))

        query = "select DISTINCT name, year FROM courses where year <= 2 ORDER BY name, year;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.name', 'text'), ('courses.year', 'int')]
        compare_select(fields, expected, output)

    def test_where_in(self):
        teachers = set([tup[0] for tup in COURSES if tup[2] in ('Davis', 'Dawson')])
        expected = [(val,) for val in teachers]
        expected.sort(key=lambda x:x[0])

        query = "select DISTINCT name FROM courses WHERE teacher IN ('Davis', 'Dawson') ORDER BY name;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.name', 'text')]
        compare_select(fields, expected, output)

    def test_limit(self):
        expected = [(tup[0], tup[1], tup[2], tup[-1]) for tup in COURSES]
        expected.sort(key=lambda x:(x[-1], x[0], x[1], x[2]))
        expected = expected[:6]

        query = "select name, year, teacher, median_mark FROM courses ORDER BY median_mark, name, year, teacher LIMIT 6;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('courses.name', 'text'), ('courses.year', 'int'), ('courses.teacher', 'text'), ('courses.median_mark', 'int')]
        compare_select(fields, expected, output)
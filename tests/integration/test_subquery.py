from conftest_operators import init_database, TEST_DATA_DIRECTOR, getInsertValues, compare_select
from andb.entrance import execute_simple_query
import shutil
import pytest


CRIMES_2023 = [('Monday', 'Theft'),\
                ('Thursday', 'Break in'),\
                ('Wednesday', 'Property Damage'),\
                ('Tuesday', 'Break in'),\
                ('Monday', 'Vandalism'),\
                ('Monday', 'Theft'),\
                ('Friday', 'Vandalism'),\
                ('Wednesday', 'Vandalism'),\
                ('Thursday', 'Theft'),\
                ('Tuesday', 'Break in'),\
                ('Monday', 'Property Damage'),\
                ('Friday', 'Vandalism'),\
                ('Thursday', 'Theft'),\
                ('Wednesday', 'Break in'),\
                ('Tuesday', 'Vandalism')]

CRIMES_2024 = [('Monday', 'Trespassing'),\
                ('Monday', 'Theft'),\
                ('Monday', 'Trespassing'),\
                ('Tuesday', 'Jaywalking'),\
                ('Tuesday', 'Jaywalking'),\
                ('Wednesday', 'Theft'),\
                ('Friday', 'Trespassing'),\
                ('Tuesday', 'Vandalism'),\
                ('Thursday', 'Jaywalking'),\
                ('Tuesday', 'Trespassing')]

class TestSubquery:
    def setup_class(cls):
        init_database()
        create_table1 = 'CREATE TABLE crimes_2023(weekday text NOT NULL, crime_type text NOT NULL);'
        execute_simple_query(create_table1)

        query = "INSERT INTO crimes_2023 VALUES " + getInsertValues(CRIMES_2023, range(0, len(CRIMES_2023[0])))
        output = execute_simple_query(query)
        assert output.effect_rows == len(CRIMES_2023)

        create_table2 = 'CREATE TABLE crimes_2024(weekday text NOT NULL, crime_type text NOT NULL);'
        execute_simple_query(create_table2)

        query = "INSERT INTO crimes_2024 VALUES " + getInsertValues(CRIMES_2024, range(0, len(CRIMES_2024[0])))
        output = execute_simple_query(query)
        assert output.effect_rows == len(CRIMES_2024)

    def teardown_class(cls):
        execute_simple_query('DROP TABLE crimes_2023;')
        execute_simple_query('DROP TABLE crimes_2024;')

    def test_group_sum(self):
        week2023 = {}
        week2024 = {}
        for tup in CRIMES_2023:
            count = week2023.get(tup[0], 0)
            week2023[tup[0]] = count + 1
        for tup in CRIMES_2024:
            count = week2024.get(tup[0], 0)
            week2024[tup[0]] = count + 1
        
        expected = []
        for day in week2023:
            if day in week2024:
                expected.append((day, week2023[day], week2024[day]))
        
        expected.sort(key=lambda x:x[0])
        
        query = "WITH grouped2023(day, crimes) AS (select weekday, COUNT(crime_type) FROM crimes_2023 GROUP BY weekday), "\
                "grouped2024(day, crimes) AS (select weekday, COUNT(crime_type) FROM crimes_2024 GROUP BY weekday) "\
                "select grouped2024.day, grouped2023.crimes, grouped2024.crimes "\
                "FROM grouped2023 INNER JOIN grouped2024 ON grouped2024.day = grouped2023.day "\
                "ORDER BY grouped2024.day;"
        #query = "select weekday AS day, COUNT(crime_type) FROM crimes_2024;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('grouped2024.day', 'text'), ('grouped2023.crimes', 'text'), ('grouped2024.crimes', 'text')]
        compare_select(fields, expected, output)
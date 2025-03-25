from conftest_operators import init_database, TEST_DATA_DIRECTOR, getInsertValues, compare_select
from andb.entrance import execute_simple_query
import shutil
import pytest


NIPS_2023 = [   ('Supervised Learning', 21),\
                ('Computer Vision', 32),\
                ('Transfer Learning', 35), \
                ('Anomaly Detection', 13), \
                ('Large Language Models', 23), \
                ('Active Learning', 45)]
NIPS_2024 = [   ('Deep Learning', 23),\
                ('Computer Vision', 45),\
                ('Graph Machine Learning', 23), \
                ('Adversarial Machine Learning', 45), \
                ('Large Language Models', 67), \
                ('Active Learning', 23),
                ('Recommendation Systems', 34)]

class TestJoin:
    def setup_class(cls):
        init_database()
        create_table1 = 'CREATE TABLE nips_2023(area text NOT NULL, num_papers int NOT NULL);'
        execute_simple_query(create_table1)
        create_table2 = 'CREATE TABLE nips_2024(area text NOT NULL, num_papers int NOT NULL);'
        execute_simple_query(create_table2)

        query = "INSERT INTO nips_2023 VALUES " + getInsertValues(NIPS_2023, range(0, len(NIPS_2023[0])))
        output = execute_simple_query(query)
        assert output.effect_rows == len(NIPS_2023)

        query = "INSERT INTO nips_2024 VALUES " + getInsertValues(NIPS_2024, range(0, len(NIPS_2024[0])))
        output = execute_simple_query(query)
        assert output.effect_rows == len(NIPS_2024)

    def teardown_class(cls):
        execute_simple_query('DROP TABLE nips_2023;')
        execute_simple_query('DROP TABLE nips_2024;')

    def test_inner_join(self):
        expected = []
        for tup1 in NIPS_2023:
            add_tuples = [(tup1[0], tup1[1], tup2[1]) for tup2 in NIPS_2024 if tup2[0] == tup1[0]]
            expected.extend(add_tuples)
        
        query = "select coalesce(nips_2023.area, nips_2024.area) as area, nips_2023.num_papers, nips_2024.num_papers from nips_2023 INNER JOIN nips_2024 ON nips_2023.area=nips_2024.area;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('area', None), ('nips_2023.num_papers', 'int'), ('nips_2024.num_papers', 'int')]
        compare_select(fields, expected, output)

    def test_left_join(self):
        expected = []
        for tup1 in NIPS_2023:
            add_tuples = [(tup1[0], tup1[1], tup2[1]) for tup2 in NIPS_2024 if tup2[0] == tup1[0]]
            if len(add_tuples) == 0:
                expected.append((tup1[0], tup1[1], None))
            else:
                expected.extend(add_tuples)
        
        query = "select nips_2023.area, nips_2023.num_papers, nips_2024.num_papers from nips_2023 LEFT JOIN nips_2024 ON nips_2023.area=nips_2024.area;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('nips_2023.area', 'text'), ('nips_2023.num_papers', 'int'), ('nips_2024.num_papers', 'int')]
        compare_select(fields, expected, output)

    def test_right_join(self):
        expected = []
        for tup2 in NIPS_2024:
            add_tuples = [(tup2[0], tup1[1], tup2[1]) for tup1 in NIPS_2023 if tup2[0] == tup1[0]]
            if len(add_tuples) == 0:
                expected.append((tup2[0], None, tup2[1]))
            else:
                expected.extend(add_tuples)
        
        query = "select nips_2024.area, nips_2023.num_papers, nips_2024.num_papers from nips_2023 RIGHT JOIN nips_2024 ON nips_2023.area=nips_2024.area;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('nips_2024.area', 'text'), ('nips_2023.num_papers', 'int'), ('nips_2024.num_papers', 'int')]
        compare_select(fields, expected, output)

    def test_full_join(self):
        expected = []
        right_inserted = set()
        for tup1 in NIPS_2023:
            add_tuples = [(tup1[0], tup1[1], tup2[1]) for tup2 in NIPS_2024 if tup2[0] == tup1[0]]
            if len(add_tuples) == 0:
                expected.append((tup1[0], tup1[1], None))
            else:
                expected.extend(add_tuples)
            right_inserted.add(tup1[0])
        expected.extend([(tup2[0], None, tup2[1]) for tup2 in NIPS_2024 if tup2[0] not in right_inserted])
        #expected.sort(key=lambda x:x[0])
        
        query = "select coalesce(nips_2023.area, nips_2024.area) as area, nips_2023.num_papers, nips_2024.num_papers from nips_2023 FULL JOIN nips_2024 ON nips_2023.area=nips_2024.area;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('area', None), ('nips_2023.num_papers', 'int'), ('nips_2024.num_papers', 'int')]
        compare_select(fields, expected, output)

    def test_nested_join(self):
        expected = []
        for tup1 in NIPS_2023:
            for tup2 in NIPS_2024:
                expected.append(tup1 + tup2)
        
        query = "select * from nips_2023, nips_2024;"
        output = execute_simple_query(query)
        assert output.effect_rows == len(expected)
        
        fields = [('nips_2023.area', 'text'), ('nips_2023.num_papers', 'int'), ('nips_2024.area', 'text'), ('nips_2024.num_papers', 'int')]
        compare_select(fields, expected, output)
create table t1 (a int, b text, c vector);
insert into t1 values (1, 'a', '[1, 2, 3, 4]'), (2, 'b', '[2, 3, 4, 5]'), (3, 'c', '[3, 4, 5, 6]');
select * from t1;
select * from t1 where cosine_distance(c, '[1, 2, 3, 4]') > 0.5;
-- select a, b, c, cosine_distance(c, '[1, 2, 3, 4]') from t1;
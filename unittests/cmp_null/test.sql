drop table if exists test;
create table test (name varchar, age int);
load data infile "source/unittests/cmp_null/input.csv" into table test;
select name, age into outfile "output.csv" from test where age = null;
drop table if exists test;
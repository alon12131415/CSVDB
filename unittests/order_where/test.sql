drop table if exists test;
create table test (name varchar, age int);
load data infile "source/unittests/noa17/input.csv" into table test;
select age into outfile "output.csv" from test where age >= 17 order by age asc;
drop table if exists test;
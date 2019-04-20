drop table if exists test;
create table test (name varchar, age int);
load data infile "source/unittests/age_ord_asc/input.csv" into table test;
select name,age into outfile "output.csv" from test order by age asc;
drop table if exists test;
-- drop table if exists test;
create table test (name varchar, age int);
load data infile "input.csv" into table test;
select name,age
into outfile "output.csv"
from test where age is NULL;

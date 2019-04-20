drop table if exists test;
create table test (category varchar, val int);
load data infile "source/unittests/group1/input.csv" into table test;
select category, sum(val) into outfile "output.csv" from test group by category;
drop table if exists test;
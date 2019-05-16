drop table if exists test;
create table test (category varchar, val int);
load data infile "input.csv" into table test;
select category, sum(val) as x into outfile "output.csv" from test group by category order by x DESC;
drop table if exists test;
drop table if exists test;

create table test (category varchar, val int, val2 int);

load data infile "input.csv" into table test;
load data infile "input2.csv" into table test;

select category, count(category) as x, max(val2)
into outfile "output.csv"
from test
where category <> "catD"
group by category
order by x DESC, category ASC;

drop table if exists test;

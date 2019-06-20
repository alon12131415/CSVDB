drop table if exists test;

create table test (category varchar, val int);

load data infile "input.csv" into table test;

select category,
sum(val) as x 
into outfile "output1.csv" 
from test 
group by category 
order by x DESC;

select category,
sum(val) as x 
into outfile "output2.csv" 
from test 
group by category 
having x > 100
order by x DESC;

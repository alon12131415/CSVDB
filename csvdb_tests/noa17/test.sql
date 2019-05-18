drop table if exists test;

create table test (
     name varchar,
     age int
);

load data infile "input.csv"
into table test;

select age
into outfile "output.csv"
from test
where age >= 17;

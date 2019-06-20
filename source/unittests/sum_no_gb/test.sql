drop table if exists test;

create table test (
     str varchar,
     num int
);

load data infile "input.csv"
into table test;

select sum(num) into outfile "output.csv" from test;

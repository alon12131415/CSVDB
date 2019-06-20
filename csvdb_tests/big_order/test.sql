drop table if exists test;
create table test (val int);
load data infile "input.csv" into table test;
select val into outfile "output.csv" from test order by val asc;
drop table if exists test;
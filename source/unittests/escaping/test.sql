drop table if exists test;
create table test (name varchar);
load data infile "input.csv" into table test;
select name into outfile "output.csv" from test;
drop table if exists test;

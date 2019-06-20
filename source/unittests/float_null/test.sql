drop table if exists test;
create table test (name varchar, age float);
load data infile "input.csv" into table test;
select name, age into outfile "output.csv" from test;
drop table if exists test;
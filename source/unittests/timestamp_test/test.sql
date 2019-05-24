drop table if exists test;
create table test (name varchar, timeSinceBorn timestamp);
load data infile "input.csv" into table test;
select name, timeSinceBorn into outfile "output.csv" from test;
drop table if exists test;
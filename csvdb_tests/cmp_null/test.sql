drop table if exists test;
create table test (name varchar, age int);
load data infile "input.csv" into table test;
select name, age into outfile "output1.csv" from test where age = null;
select name, age into outfile "output2.csv" from test where age is null;

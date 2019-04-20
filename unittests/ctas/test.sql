drop table if exists test;
drop table if exists people;
create table test (name varchar, age int);
load data infile "source/unittests/ctas/input.csv" into table test;
create table people as select name as shem, age as gil from test;
select gil,shem into outfile "output.csv" from people;
drop table if exists test;
drop table if exists people;
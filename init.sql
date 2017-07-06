pragma journal_mode = wal;
create table test (a int, b int, c int, d int, e text);
create table test2 (a int, b int);
create index test_a on test (a);

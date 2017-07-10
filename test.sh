rm -rf test.db*
touch test.db
./sqlite3 test.db < tpcc.sql
./sqlite3 test.db

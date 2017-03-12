rm -rf test.db
rm -rf test.db.log
touch test.db
sqlite3_rel test.db < init.sql
rm -rf test.db.log
./sqlite3 test.db < test.sql
./sqlite3 test.db

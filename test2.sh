rm -rf test.db
rm -rf test.db.log
touch test.db
sqlite3 test.db < init.sql
rm -rf test.db.log
sqlite3 test.db < test2.sql
sqlite3 test.db

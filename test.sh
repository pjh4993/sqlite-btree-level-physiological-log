rm -rf test$1.db
rm -rf test$1.db.log
touch test$1.db
sqlite3 test$1.db < init.sql
rm -rf test$1.db.log
if [ "$2" == "gdb" ]; then
    gdb ./sqlite3 -tui
elif [ "$2" == "test" ]; then
    ./sqlite3 test$1.db < test2.sql
    ./sqlite3 test$1.db
else
    ./sqlite3 test$1.db < test.sql
    ./sqlite3 test$1.db
fi

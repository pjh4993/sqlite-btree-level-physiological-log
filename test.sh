rm -rf test$1.db
rm -rf test$1.db.log
touch test$1.db
if [ "$2" == "tpcc" ]; then
    ./sqlite3 test$1.db < tpcc.sql
    gdb ./sqlite3 -tui
else
    ./sqlite3 test$1.db < init.sql
    rm -rf test$1.db.log
fi
if [ "$2" == "gdb" ]; then
    gdb ./sqlite3 -tui
elif [ "$2" == "test" ]; then
    ./sqlite3 test$1.db < testSql2.sql
    #gdb ./sqlite3 -tui
    ./sqlite3 test$1.db
elif [ "$2" == "done" ]; then
    ./sqlite3 test$1.db < testSql2.sql
    echo "done"
fi

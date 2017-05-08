rm -rf test.db
rm -rf test.db.log
touch test.db
sqlite3 test.db < init.sql
rm -rf test.db.log
echo $1
if [ "$1" == "gdb" ]; then
    gdb ./sqlite3 -tui
elif [ "$1" == "test" ]; then
    ./sqlite3 test.db < test.sql
    gdb ./sqlite3 -tui
else
    ./sqlite3 test.db < test.sql
    ./sqlite3 test.db
fi

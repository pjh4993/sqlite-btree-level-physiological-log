#ifndef SQLITE_LOGGING
#define SQLITE_LOGGING
#define LOG_LIMIT 1000
typedef struct LOGGER Logger;
typedef struct LOGCELL logCell;
typedef struct QLOGCELL qLogCell;
typedef struct PHYSICALLOG physicalLog;
typedef struct LOGICALLOG logicalLog;
struct PHYSICALLOG{
	int idx;
	int cellSize;
	int iTable;
	int curFlags;
	Pgno pgno;
	char* newCell;
};
struct LOGICALLOG{
	int iTable;
	int nKey;
	int nData;
	int nZero;
	int nMem;
	int seekResult;
	int curFlags;
	void *pData;
};
struct LOGGER{
    int log_fd;
	int p_check;
    void *log_buffer;
    unsigned int lastLsn;
	unsigned int syncedLsn;
	char *log_file_name;
	qLogCell *apLogCell;
};
struct LOGCELL{
	int lastLsn;
	int opcode;
	int data_size;
	void *data;
};
struct QLOGCELL{
	struct LOGCELL* m_logCell;
	struct QLOGCELL *next, *prev;
};
/*
 * insert
 * 	- redo
 * 		- page split, overflow occured : logical recovery with btree module , need
 * 		cursor
 * 		- physical recovery with cell level function
 *
 * 	- undo
 * 		- page split, overflow occured : WTF....
 * 		- physical recovery with cell level function
 *
 * delete
 * 	- redo
 * 		- page split, overflow occured : logical recovery with btree module , need
 * 		cursor
 * 		- physical recovery with cell level function
 * 	- undo
 * 		- page split, overflow occured : WTF....
 * 		- physical recovery with cell level function
 *
 * update
 * 	- redo
 * 		- page split, overflow occured : logical recovery with btree module , need
 * 		cursor
 * 		- physical recovery with cell level function
 * 	- undo
 * 		- page split, overflow occured : WTF....
 * 		- physical recovery with cell level function
 *
 */

void* serializeLog(int logType, void* logData, int* size);
int sqlite3LoggerOpen(sqlite3_vfs *pVfs,const char* zFilename, Logger **ppLogger);
void sqlite3Log(Logger *pLogger ,logCell *pLogCell);
int sqlite3LogAnalysis(Logger *pLogger);
void sqlite3LogRecovery(Btree* pBtree,sqlite3* db);
int sqlite3LogForceAtCommit(Logger* pLogger);
int sqlite3LogCheckPoint(Logger* pLogger);
#endif

#ifndef SQLITE_LOGGING
#define SQLITE_LOGGING
#define LOG_LIMIT 4096
#include "list.h"

enum opcode {EXIT , CREATETABLE, CREATEINDEX, INSERT, IDXINSERT, DELETE, IDXDELETE, COMMIT};
enum compare {UNKNOWN,INT, STRING, RECORD, WALINT, WALSTRING, WALRECORD};
enum loggerState {ON, OFF};

typedef struct LOGGER Logger;
typedef struct LOGCELL logCell;
typedef struct QLOGCELL qLogCell;
typedef struct LOGHDR logHdr;

struct LOGHDR{
    int stLsn;
    int vers;
};

struct CreateLog{
    int flags;
};

struct IdxInsertLog{
    int vers;
    int iDb;
    int iTable;
    int wrFlag;
    int seekResult;
    int appendBias;
    KeyInfo* pKeyInfo;
    const BtreePayload *pX;
};

struct IdxDeleteLog{
    int vers;
    int iTable;
    int wrFlag;
    int iCellIdx;
    int iCellDepth;
    u8 flags;
    u32 pPagePgno;
};

struct LOGGER{
    void *log_buffer;
    int log_fd;
	char *zFile;
    struct list_head q;
    logHdr hdr;
	int p_check;
    unsigned int lastLsn;
    enum loggerState state;
};

struct LOGCELL{
	int nextLsn;
	enum opcode op;
	int data_size;
	void *data;
};

struct QLOGCELL{
	struct LOGCELL* m_logCell;
    struct list_head q;
};

int sqlite3LoggerOpenPhaseOne(BtShared*  pBt, Logger **ppLogger);
int sqlite3LoggerOpenPhaseTwo(sqlite3_vfs *pVfs, const char* zPathname,int nPathname,Logger *pLogger, int vfsFlags);
int sqlite3LogCursor(BtCursor *pCur, int iDb, int iTable, int wrFlag, KeyInfo* pKeyInfo);
int sqlite3LogPayload(BtCursor* pCur,const BtreePayload *pX, int appendBias, int seekResult);
void sqlite3Log(Logger *pLogger, void *log, enum opcode op);
int sqlite3LogForceAtCommit(Logger *pLogger);
int sqlite3LogiCell(BtCursor* pCur, int iCellDepth, int iCellIdx, u32 pPagePgno);
void sqlite3LogFileInit(Logger *pLogger);
int sqlite3LogCreateTable(BtShared* p, int flags);
void sqlite3LoggerClose(Logger *pLogger);
int sqlite3LogInit(BtCursor* pCur);
int sqlite3LogCheckPoint(Logger *pLogger);
#endif

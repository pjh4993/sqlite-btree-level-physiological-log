#ifndef SQLITE_LOGGING;
#define SQLITE_LOGGING
#define LOG_LIMIT 20000

enum logtype { bt_open, bt_ibt, bt_flag, bt_save, csr_open = 0x10, csr_icsr, csr_insert, csr_flag, csr_unapcekd };

enum opcode {
    /*Default, recovery end*/
    EXIT = 0x00, 

    /*Btree Log
     * bt_open_log,
     * bt_iBt_log,
     * bt_flag_log,
     * bt_save_log
     * */

    /*bt_open_log*/
    BTREE_OPEN = 0x01,
    /*bt_iBt_log*/
    DROP = 0x10, CLEAR, BEGINTRANS, COMMIT,
    /*bt_flag_log*/
    BTREE_CLOSE = 0x20, ENDTRANS, CREATE, 
    /*bt_save_log*/
    SAVEPOINT = 0x30, 

    /*BtCursor log
     *
     * csr_open_log,
     * csr_icsr_log,
     * csr_insert_log,
     * csr_flag_log,
     * csr_unpacked_log
     *
     * */

    /*csr_open_log*/
    CSR_OPEN = 0x100,
    /*csr_icsr_log*/
    CSR_CLOSE, NEXT = 0x110, PREV, FIRST, LAST, CSR_EOF, INTEGERKEY, CLAER, RESTORE, INCRBLOB, 
    /*csr_insert_log*/
    INSERT = 0x120, 
    /*csr_flag_log*/
    DELETE = 0x130, 
    /*csr_unpacked_log*/
    MV_UNPACKED = 0x140
};

enum compare {UNKNOWN,INT, STRING, RECORD, WALINT, WALSTRING, WALRECORD};

/* Structures for managing log file and log queue
 * */

/* There are 3 state which logger has.
 * This is necessary to cover case which Database's journal_mode is not wal
 * */
enum loggerState {START, ON, OFF};

typedef struct LOGGER Logger;
typedef struct LOGCELL logCell;
typedef struct QLOGCELL qLogCell;
typedef struct LOGHDR logHdr;

struct LOGHDR{
    int stLsn;
    int vers;
};

struct LOGGER{
    void *log_buffer;
    int log_fd;
    int fd;
	char *zFile;
    struct list_head q;
    logHdr hdr;
	int p_check;
    unsigned int lastLsn;
    Btree *apBt[10];
    BtCursor *apCsr[10];
    enum loggerState state;
};

struct LOGCELL{
	int nextLsn;
	enum opcode op;
	int data_size;
    int vers;
	void *data;
};

struct QLOGCELL{
	struct LOGCELL* m_logCell;
    struct list_head q;
};



/* Structures for log Btree data
 * */

struct BtIbtLog{
    int iBt;
};

struct BtOpenLog{
    int iBt;
    int flags;
    int vfsFlags;
    char *zFilename;
};

struct BtFlagLog{
    int iBt;
    int flags;
};

struct BtSaveLog{
    int iBt;
    int op;
    int iSavepoint;
};

/* Structures for log BtCursor data
 * */

struct CsrOpenLog{
    int iCsr;
    int iBt;
    int iDb;
    int iTable;
    int wrFlag;
    struct KeyInfo *pKeyInfo;
};

struct CsrIcsrLog{
    int iCsr;
};

struct CsrUnpackedLog{
    int iCsr;
    i64 intKey;
    int biasRight;
    UnpackedRecord *pIdxKey;
};

struct CsrInsertLog{
    int iCsr;
    int seekResult;
    int appendBias;
    const BtreePayload *pX;
};

struct CsrFlagLog{
    int iCsr;
    int flags;
};


void sqlite3Log(Logger *pLogger, void *log, enum opcode op);

/* BtreeCursor logging
 * */
void inline sqlite3LogCursorOpen(int wrFlag, BtCursor *pCur, Btree* pBtree);
void inline sqlite3LogCursorClose(BtCursor *pCur);
void inline sqlite3LogCursorNext(BtCursor *pCur);
void inline sqlite3LogCursorPrev(BtCursor *pCur);
void inline sqlite3LogCursorEof(BtCursor *pCur);
void inline sqlite3LogCursorFirst(BtCursor *pCur);
void inline sqlite3LogCursorLast(BtCursor *pCur);
void inline sqlite3LogCursorIntegerKey(BtCursor *pCur);
void inline sqlite3LogCursorUnpacked(BtCursor *pCur, UnpackedRecord *pIdxKey, int intKey, int biasRight);
void inline sqlite3LogCursorClear(BtCursor *pCur);
void inline sqlite3LogCursorRestore(BtCursor *pCur);
void inline sqlite3LogCursorIncrBlob(BtCursor *pCur);
void inline sqlite3LogCursorInsert(BtCursor *pCur, const BtreePayload *pX, int appendBias, int seekResult);
void inline sqlite3LogCursorDelete(BtCursor *pCur, u8 flags);

/* Btree logging
 * */
void inline sqlite3LogBtreeOpen(Btree *pBtree, char* zFilename, int flags, int vfsFlags);
void inline sqlite3LogBtreeClose(Btree *pBtree);
void inline sqlite3LogBtreeCreate(Btree *pBtree, int flags);
void inline sqlite3LogBtreeDrop(Btree *pBtree, int iTable);
void inline sqlite3LogBtreeClear(Btree *pBtree, int iTable);
void inline sqlite3LogBtreeBeginTrans(Btree *pBtree, int wrFlag);
void inline sqlite3LogBtreeEndTrans(Btree *pBtree, int wrFlag);
void inline sqlite3LogBtreeEndTrans(Btree *pBtree);
void inline sqlite3LogBtreeSavepoints(Btree *pBtree, int op, int iSavepoint);





    
int sqlite3LoggerOpenPhaseOne(Logger **ppLogger);
int sqlite3LoggerOpenPhaseTwo(sqlite3_vfs *pVfs, const char* zPathname,int nPathname,Logger *pLogger);
int sqlite3LogPayload(BtCursor* pCur,const BtreePayload *pX, int appendBias, int seekResult);
int sqlite3LogForceAtCommit(Logger *pLogger);
int sqlite3LogiCell(BtCursor* pCur, int iCellDepth, int iCellIdx, u32 pPagePgno, u8 flags);
void sqlite3LogFileInit(Logger *pLogger);
int sqlite3LogCreateTable(BtShared* p, int flags);
void sqlite3LoggerClose(Logger *pLogger);
int sqlite3LogInit(BtCursor* pCur);
int sqlite3LogCheckPoint(Logger *pLogger);
int sqlite3LogRollback(Logger *pLogger);
int sqlite3LogiPage(BtCursor* pCur, int iPage, u32 pgno);
#endif

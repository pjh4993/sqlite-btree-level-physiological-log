#ifndef SQLITE_LOGGING
#define SQLITE_LOGGING
#define LOG_LIMIT 10
typedef struct LOGGER Logger;
typedef struct LOGCELL logCell;
typedef struct QLOGCELL qLogCell;
typedef struct PHYSICALLOG physicalLog;
typedef struct LOGICALLOG logicalLog;
struct PHYSICALLOG{
	int idx;
	int cellSize;
	Pgno pgno;
	int iTable;
	int curFlags;
    int loc;
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


#define INIT_LIST_HEAD(ptr) do { \
	(ptr)->next = (ptr); (ptr)->prev = (ptr); \
} while (0)

static __inline__ void __list_add(qLogCell * new,
	qLogCell * prev,
	qLogCell * next)
{
	next->prev = new;
	new->next = next;
	new->prev = prev;
	prev->next = new;
}

static __inline__ void list_add(qLogCell *new, qLogCell *head)
{
	__list_add(new, head, head->next);
}

static __inline__ void list_add_tail(qLogCell *new, qLogCell *head)
{
	__list_add(new, head->prev, head);
}

static __inline__ void __list_del(qLogCell * prev,
				  qLogCell * next)
{
	next->prev = prev;
	prev->next = next;
}

static __inline__ void list_del(qLogCell *entry)
{
	__list_del(entry->prev, entry->next);
}

/**
 * list_empty - tests whether a list is empty
 * @head: the list to test.
 */
static __inline__ int list_empty(qLogCell *head)
{
	return head->next == head;
}

/**
 * list_entry - get the struct for this entry
 * @ptr:	the &struct list_head pointer.
 * @type:	the type of the struct this is embedded in.
 * @member:	the name of the list_struct within the struct.
 */
#define list_entry(ptr, type, member) \
	((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

/**
 * list_for_each_prev	-	iterate over a list in reverse order
 * @pos:	the &struct list_head to use as a loop counter.
 * @head:	the head for your list.
 */
#define list_for_each_prev(pos, head) \
	for (pos = (head)->prev; pos != (head); \
        	pos = pos->prev)
 




#endif

#include "log.h"
#include <stdio.h>
#include "btree.h"
#define LOG_SIZE 4096*1024
#define HDR_SIZE sizeof(logHdr)

logHdr hdr;
qLogCell logqueue;
qLogCell* logqueue_head = &logqueue;



static inline void add_to_logqueue(qLogCell * p)
{
	list_add(p, logqueue_head);
}

static inline void del_from_logqueue(qLogCell * p)
{
	list_del(p);
	p->next = 0x0;
}

#define for_each_que(p) \
	for (p = logqueue_head ; (p = p->next) != logqueue_head ; )

int sqlite3LoggerOpen(sqlite3_vfs *pVfs,const char* zFilename,Logger **ppLogger){
	Logger *pLogger;
	int fileLen, fullPathLen, logNameLen, m_log_fd, tmp=0;
	void* m_log_buffer;
	char* zLogname;
	pLogger = sqlite3MallocZero(sizeof(Logger));
	if(!pLogger){
		return SQLITE_NOMEM_BKPT;
	}
	
	fileLen = sqlite3Strlen30(zFilename);
	fullPathLen = pVfs->mxPathname+1;
	logNameLen = MAX(fullPathLen+4,fileLen+4);
	zLogname = sqlite3Malloc(logNameLen);
	
	memcpy(zLogname, zFilename, fileLen);
	memcpy(zLogname + fileLen, ".log", 4);
	zLogname[fileLen+4] = '\0';
	pLogger->log_file_name = zLogname;
	
	m_log_fd = open(zLogname, O_RDWR, 0644);
    if(m_log_fd < 0){
        m_log_fd = open(zLogname,O_RDWR | O_CREAT,0644);
        tmp = 1;
    }
	if(m_log_fd < 0){
		fprintf(stderr, "LOG FILE OPEN ERROR\n");
	}else{
		ftruncate(m_log_fd,LOG_SIZE);
		pLogger->log_fd = m_log_fd;
	}

	m_log_buffer = (void*) mmap(NULL, LOG_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, m_log_fd,0);
    if(tmp){
        hdr.stLsn = 0;
        memcpy(m_log_buffer,&hdr,sizeof(hdr));
        tmp = -1;
        memcpy(m_log_buffer + HDR_SIZE,&tmp, sizeof(int));
        msync(m_log_buffer,sizeof(int) + HDR_SIZE, MS_SYNC);
    }
	if(m_log_buffer == MAP_FAILED){
		fprintf(stderr, "LOG FILE MAPPING ERROR\n");
	}
	pLogger->log_buffer = m_log_buffer;
	pLogger->p_check = 0;
    INIT_LIST_HEAD(logqueue_head);
	*ppLogger = pLogger;
	return SQLITE_OK;
}

void* serializeLog(int logType, void* logData, int* size);
void deserializeLog(int logType, void* data, void **dest);

void* serializeLog(int logType, void* logData, int* size){
	int m_size = 0, i;
	void* m_serialized;
	if(logType == 1){
		logicalLog* m_log = logData;
		*size = sizeof(int)*7 + m_log->nData;
		m_serialized = sqlite3Malloc(*size);
		memcpy(m_serialized, &(m_log->iTable), sizeof(int));
		memcpy(m_serialized + sizeof(int), &(m_log->nKey), sizeof(int));
		memcpy(m_serialized + sizeof(int)*2, &(m_log->nData), sizeof(int));
		memcpy(m_serialized + sizeof(int)*3, &(m_log->nZero), sizeof(int));
		memcpy(m_serialized + sizeof(int)*4, &(m_log->nMem), sizeof(int));
		memcpy(m_serialized + sizeof(int)*5, &(m_log->seekResult), sizeof(int));
		memcpy(m_serialized + sizeof(int)*6, &(m_log->curFlags), sizeof(int));
		memcpy(m_serialized + sizeof(int)*7, m_log->pData, sizeof(char)*m_log->nData);
	}else if(logType == 0){
		physicalLog* m_log = logData;
		*size = sizeof(int)*5 + sizeof(Pgno) + m_log->cellSize;
		m_serialized = sqlite3Malloc(*size);
		memcpy(m_serialized, &(m_log->idx), sizeof(int));
		memcpy(m_serialized + sizeof(int), &(m_log->cellSize), sizeof(int));
		memcpy(m_serialized + sizeof(int)*2, &(m_log->pgno), sizeof(Pgno));
		memcpy(m_serialized + sizeof(int)*2 + sizeof(Pgno), &(m_log->iTable), sizeof(int));
		memcpy(m_serialized + sizeof(int)*3 + sizeof(Pgno), &(m_log->curFlags), sizeof(int));
		memcpy(m_serialized + sizeof(int)*4 + sizeof(Pgno), &(m_log->loc), sizeof(int));
		memcpy(m_serialized + sizeof(int)*5 + sizeof(Pgno), m_log->newCell, sizeof(char)*m_log->cellSize);
	}else if(logType == 2){
        logicalDLog* m_log = logData;
        *size = sizeof(Pgno)*2 + sizeof(int) * 2 + sizeof(u8);
		m_serialized = sqlite3Malloc(*size);
        memcpy(m_serialized, &(m_log->pgno), sizeof(Pgno));
        memcpy(m_serialized + sizeof(Pgno), &(m_log->pPagePgno), sizeof(Pgno));
        memcpy(m_serialized + sizeof(Pgno)*2, &(m_log->iCellDepth), sizeof(int));
        memcpy(m_serialized + sizeof(Pgno)*2 + sizeof(int), &(m_log->iCellIdx), sizeof(int));
        memcpy(m_serialized + sizeof(Pgno)*2 + sizeof(int)*2, &(m_log->curFlags), sizeof(int));
        memcpy(m_serialized + sizeof(Pgno)*2 + sizeof(int)*3, &(m_log->flags), sizeof(u8));
    }
	return m_serialized;
}

void deserializeLog(int logType, void *data, void **dest){
	int i=0;
	if(logType == 1){
		logicalLog* m_lLog = sqlite3Malloc(sizeof(logicalLog));
		memcpy(&(m_lLog->iTable), data, sizeof(int));
		memcpy(&(m_lLog->nKey), data + sizeof(int), sizeof(int));
		memcpy(&(m_lLog->nData), data + sizeof(int)*2, sizeof(int));
		memcpy(&(m_lLog->nZero), data + sizeof(int)*3, sizeof(int));
		memcpy(&(m_lLog->nMem), data + sizeof(int)*4, sizeof(int));
		memcpy(&(m_lLog->seekResult), data + sizeof(int)*5, sizeof(int));
		memcpy(&(m_lLog->curFlags), data + sizeof(int)*6, sizeof(int));
		m_lLog->pData = data + sizeof(int)*7;
		*dest = m_lLog;
	}else if(logType == 0){
		physicalLog* m_pLog = sqlite3Malloc(sizeof(physicalLog));
		memcpy(&(m_pLog->idx), data, sizeof(int));
		memcpy(&(m_pLog->cellSize), data + sizeof(int), sizeof(int));
		memcpy(&(m_pLog->pgno), data + sizeof(int)*2, sizeof(Pgno));
		memcpy(&(m_pLog->iTable), data + sizeof(int)*2 + sizeof(Pgno), sizeof(int));
		memcpy(&(m_pLog->curFlags), data + sizeof(int)*3 + sizeof(Pgno), sizeof(int));
		memcpy(&(m_pLog->loc), data + sizeof(int)*4 + sizeof(Pgno), sizeof(int));
		m_pLog->newCell = data + sizeof(int)*5 + sizeof(Pgno);
		*dest = m_pLog;
	}else if(logType == 2){
        logicalDLog* m_dLog = sqlite3Malloc(sizeof(logicalDLog));
        memcpy(&(m_dLog->pgno), data, sizeof(Pgno));
        memcpy(&(m_dLog->pPagePgno), data+sizeof(Pgno), sizeof(Pgno));
        memcpy(&(m_dLog->iCellDepth), data+sizeof(Pgno)*2, sizeof(int));
        memcpy(&(m_dLog->iCellIdx), data+sizeof(Pgno)*2+sizeof(int), sizeof(int));
        memcpy(&(m_dLog->curFlags), data+sizeof(Pgno)*2+sizeof(int)*2, sizeof(int));
        memcpy(&(m_dLog->flags), data+sizeof(Pgno)*2+sizeof(int)*3, sizeof(u8));
        *dest = m_dLog; 
    }
}

void sqlite3Log(Logger *pLogger ,logCell *pLogCell){
    if(logState == RECOVERY)
        return;
	char *tmp1, *tmp2;
    int tmp;
	void *m_log_buffer = pLogger->log_buffer + HDR_SIZE + ((pLogCell->opcode != -2)?pLogger->lastLsn : pLogger->syncedLsn);
	char *m_logCell = sqlite3Malloc(sizeof(int) * 3 + pLogCell->data_size);
    if(pLogCell->opcode == -2){
        int m_buf = 0;
        memcpy(m_log_buffer, &m_buf, sizeof(int));
        msync(m_log_buffer,sizeof(int), MS_SYNC);
        return;
    }
    if(pLogCell->opcode > 0)
        pLogger->p_check = pLogger->p_check + 1;
	pLogger->lastLsn += (sizeof(int) * 3 + pLogCell->data_size);
    if(pLogger->lastLsn + sizeof(int) > LOG_SIZE){
        tmp = 0;
        memcpy(pLogger->prevLsn, &tmp, sizeof(int));
        m_log_buffer = pLogger->log_buffer + HDR_SIZE;
        pLogger->lastLsn = (sizeof(int)*3 + pLogCell->data_size);
    }
	pLogCell->nextLsn = pLogger->lastLsn;
	memcpy(m_logCell,&(pLogger->lastLsn),sizeof(int));
    pLogger->prevLsn = m_log_buffer;
	memcpy(m_logCell+ sizeof(int),&(pLogCell->opcode), sizeof(int));
	memcpy(m_logCell+ sizeof(int)*2,&(pLogCell->data_size), sizeof(int));
	if(pLogCell->data_size > 0 && pLogCell->data != NULL)
		memcpy(m_logCell+ sizeof(int)*3,pLogCell->data, pLogCell->data_size);
	memcpy(m_log_buffer, m_logCell, sizeof(int) * 3 + pLogCell->data_size);
	sqlite3_free(m_logCell);
    switch(pLogCell->opcode){
        case -1:
            hdr.stLsn = pLogger->lastLsn;
            memcpy(pLogger->log_buffer,&hdr,sizeof(logHdr)); 
            msync(pLogger->log_buffer,sizeof(logHdr),MS_SYNC);
        case 0:
            tmp = -1;
            memcpy(pLogger->log_buffer + HDR_SIZE + pLogger->lastLsn, &tmp, sizeof(int));
            if(pLogger->syncedLsn > pLogger->lastLsn){
                msync(pLogger->log_buffer + pLogger->syncedLsn, LOG_SIZE - pLogger->syncedLsn, MS_SYNC);
                pLogger->syncedLsn = 0;
            }
            msync(pLogger->log_buffer + pLogger->syncedLsn, pLogger->lastLsn - pLogger->syncedLsn + sizeof(int) , MS_SYNC);
            pLogger->syncedLsn = pLogger->lastLsn;
            break;
    }
	return;	
};

int parseLogBuffer(void* m_log_buffer, logCell **m_plogCell){
	*m_plogCell = sqlite3Malloc(sizeof(logCell));
	logCell* m_logCell = *m_plogCell;
	memcpy(&(m_logCell->nextLsn) ,m_log_buffer, sizeof(int));
    if(m_logCell->nextLsn == -1)
        return m_logCell->nextLsn;
	memcpy(&(m_logCell->opcode) ,m_log_buffer + sizeof(int), sizeof(int));
	memcpy(&(m_logCell->data_size) ,m_log_buffer + sizeof(int)*2, sizeof(int));
	m_logCell->data = sqlite3Malloc(m_logCell->data_size);
	memcpy(m_logCell->data ,m_log_buffer + sizeof(int)*3, m_logCell->data_size);
	return m_logCell->nextLsn;
};

//implement queue pls:
int sqlite3LogAnalysis(Logger *pLogger){
    memcpy(&hdr,pLogger->log_buffer, sizeof(logHdr));
	void* m_log_buffer = pLogger->log_buffer + HDR_SIZE + hdr.stLsn;
	logCell *m_logCell;
	qLogCell *tmpQueue,*nextTmp;
	int m_nextLsn, id = 0,start_id = 0 , i;
	while((m_nextLsn = parseLogBuffer(m_log_buffer, &m_logCell)) && (m_nextLsn >= 0)){
		if(m_logCell->opcode == -1 ){
			//flush queue
            list_for_each_prev(tmpQueue,logqueue_head){
                del_from_logqueue(tmpQueue);    
            }
            m_log_buffer = pLogger->log_buffer + m_nextLsn;
            continue;
		}else if(m_logCell->opcode == -2){
            list_for_each_prev(tmpQueue,logqueue_head){
                if(tmpQueue->m_logCell->opcode == 0)
                    break;
                del_from_logqueue(tmpQueue);    
            }
            m_log_buffer = pLogger->log_buffer + m_nextLsn;
            continue;
        }
		//insert into queue
		tmpQueue = sqlite3Malloc(sizeof(qLogCell));
		tmpQueue->m_logCell = m_logCell;
        tmpQueue->prev= tmpQueue->next = NULL;
        add_to_logqueue(tmpQueue);
		m_log_buffer = pLogger->log_buffer + HDR_SIZE + m_nextLsn;
	};
	return SQLITE_OK;
};

void sqlite3LogRecovery(Btree* pBtree, sqlite3* db){
    logState = RECOVERY;
	Logger *pLogger = pBtree->pBt->pLogger;
	logCell *mLogCell;
	BtCursor *btCsr = sqlite3Malloc(sizeof(BtCursor));
	logicalLog *m_lLog;
	physicalLog *m_pLog;
	logicalDLog *m_dLog;
	qLogCell *tmpQueue,*prevTmp;
	CellInfo info;
	unsigned char* oldCell;
	char* newCell;
	MemPage* pPage;
	int rc,i,loc;
	//while queue exists
    sqlite3BtreeBeginTrans(pBtree,4);
    list_for_each_prev(tmpQueue,logqueue_head){
		sqlite3BtreeCursorZero(btCsr);
		mLogCell = tmpQueue->m_logCell;
		switch(mLogCell->opcode){
			case 1:
				deserializeLog(1, mLogCell->data, (void**)&m_lLog);
				BtreePayload x;
				sqlite3BtreeCursor(pBtree, m_lLog->iTable, 4, 0x0, btCsr);
				rc = sqlite3BtreeLast(btCsr,&rc);
				x.nKey = m_lLog->nKey;
				x.nData = m_lLog->nData;
				x.nZero = m_lLog->nZero;
				x.nMem = m_lLog->nMem;
				x.aMem = NULL;
				x.pData = m_lLog->pData;
				x.pKey = NULL;
				btCsr->curFlags = m_lLog->curFlags;
				rc = sqlite3BtreeInsert(btCsr,&x,1,m_lLog->seekResult);
                //printf("logical insert %d\n",rc);
				break;
			case 2:
				deserializeLog(0, mLogCell->data, (void**)&m_pLog);
                loc = m_pLog->loc;
				rc = sqlite3BtreeCursor(pBtree, m_pLog->pgno, 4, 0x0, btCsr);
				rc = sqlite3BtreeLast(btCsr,&rc);
				newCell = pBtree->pBt->pTmpSpace;
				memcpy(newCell, m_pLog->newCell, m_pLog->cellSize);
				pPage = 0;
				rc = btreeGetPage(pBtree->pBt, m_pLog->pgno, &(pPage), 2);

                if( loc==0 ){
                    CellInfo info;
                    assert( m_pLog->idx<pPage->nCell );
                    rc = sqlite3PagerWrite(pPage->pDbPage);
                    if( rc ){
                        goto end_recovery;
                    }
                    oldCell = findCell(pPage, m_pLog->idx);
                    if( !pPage->leaf ){
                        memcpy(newCell, oldCell, 4);
                    }
                    rc = clearCell(pPage, oldCell, &info);
                    if( info.nSize==m_pLog->cellSize && info.nLocal==info.nPayload ){
                        assert( rc==SQLITE_OK ); 
                        if( oldCell+m_pLog->cellSize > pPage->aDataEnd )
                            goto end_recovery;
                        memcpy(oldCell, newCell, m_pLog->cellSize);
                        goto end_recovery;
                    }
                    dropCell(pPage, m_pLog->idx, info.nSize, &rc);
                    if( rc ) goto end_recovery;
                }
				insertCell(pPage, m_pLog->idx, newCell, m_pLog->cellSize, 0, 0, &rc);
				pPage->pDbPage->pPager->eState = PAGER_WRITER_FINISHED; 
				sqlite3PcacheMakeDirty(pPage->pDbPage);

                pLogger->p_check = LOG_LIMIT;
                pBtree->inTrans = TRANS_WRITE;
                pBtree->pBt->inTransaction = TRANS_WRITE;
                sqlite3BtreeCommit(pBtree);

                releasePage(pPage);
                //printf("physical insert %d\n",rc);
				break;
            case 3:
				deserializeLog(2, mLogCell->data, (void**)&m_dLog);
				rc = sqlite3BtreeCursor(pBtree, m_dLog->pgno, 4, 0x0, btCsr);
                rc = sqlite3BtreeFirst(btCsr, &rc);
                btCsr->iPage = m_dLog->iCellDepth;
                btCsr->curFlags = m_dLog->curFlags;
                btCsr->aiIdx[m_dLog->iCellDepth] = m_dLog->iCellIdx;
                btreeGetPage(pBtree->pBt,m_dLog->pPagePgno,&pPage,0);
                releasePage(btCsr->apPage[m_dLog->iCellDepth]);
                btCsr->apPage[m_dLog->iCellDepth] = pPage;
                btreeParseCell(btCsr->apPage[m_dLog->iCellDepth],m_dLog->iCellIdx,&btCsr->info);
                sqlite3BtreeDelete(btCsr,m_dLog->flags);
                //printf("logical delete %d\n",rc);
                break;
		}
end_recovery:
        if(mLogCell->opcode > 0)
            rc = sqlite3BtreeCloseCursor(btCsr);
        sqlite3_free(mLogCell);
        del_from_logqueue(tmpQueue);
	}

	char *zer;
	sqlite3_free(btCsr);
	pLogger->p_check = LOG_LIMIT;
	pBtree->inTrans = TRANS_WRITE;
	pBtree->pBt->inTransaction = TRANS_WRITE;
	sqlite3BtreeCommit(pBtree);
    hdr.stLsn = 0;
    int tmp = -1;
    memcpy(pLogger->log_buffer,&hdr,sizeof(logHdr));
    memcpy(pLogger->log_buffer + HDR_SIZE,&tmp, sizeof(int));
    msync(pLogger->log_buffer,sizeof(int) + HDR_SIZE, MS_SYNC);

	pLogger->p_check = 0;
    pLogger->lastLsn = pLogger->syncedLsn = 0;
    if(pBtree->pBt->pPager->pWal)
        logState = WORK;
};

int sqlite3LogForceAtCommit(Logger *pLogger){
	logCell m_logCell;
	m_logCell.opcode = 0;
	m_logCell.data_size = 0;
	m_logCell.data = NULL;
    //#call msync in here pls
    sqlite3Log(pLogger,&m_logCell);
};

int sqlite3LogRollback(Logger *pLogger){
    logCell m_logCell;
    m_logCell.opcode = -2;
    m_logCell.data_size = 0;
    m_logCell.data = NULL;
    sqlite3Log(pLogger, &m_logCell);
};

int sqlite3LogCheckPoint(Logger *pLogger){
    if(pLogger->p_check >= LOG_LIMIT){
        logCell m_logCell;
        m_logCell.opcode = -1;
        m_logCell.data_size = 0;
        m_logCell.data = NULL;
        sqlite3Log(pLogger,&m_logCell);
        //#wal 함수 부르기
        pLogger->p_check = 0;
    }
};



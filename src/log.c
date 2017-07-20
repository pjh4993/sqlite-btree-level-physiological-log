#include "list.h"
#include "btree.h"
#include <stdio.h>
#include "sqliteInt.h"
#include "log.h"
#define HDR_SIZE sizeof(logHdr)
#define LOG_SIZE 1024 * 1024 * 4

static inline void add_to_logqueue(Logger *pLogger, qLogCell * p)
{
	list_add(&p->q, &pLogger->q);
}

static inline void del_from_logqueue(qLogCell *p){
    list_del(&p->q);    
}

int sqlite3LoggerOpenPhaseOne(BtShared*  pBt, Logger **ppLogger){
    //will called in btree level
    //create logger for BtShared and initialize it
	Logger *pLogger;
	pLogger = (Logger *)sqlite3MallocZero(sizeof(Logger));
    pLogger->state = ON;
    INIT_LIST_HEAD(&pLogger->q);
	*ppLogger = pLogger;
	return SQLITE_OK;
};

int sqlite3LoggerOpenPhaseTwo(sqlite3_vfs *pVfs,const char* zPathname,int nPathname,Logger *pLogger, int vfsFlags){
    //will called in pager level
    //create file for logger
    int fout = 0;                    /* VFS flags returned by xOpen() */
    int rc;
    if(pVfs != 0){
        pLogger->zFile = (char*)sqlite3Malloc(nPathname+6);
        memcpy(pLogger->zFile, zPathname, nPathname);
        memcpy(&pLogger->zFile[nPathname], "-log\000", 4+2);
    }
    pLogger->log_fd = open(pLogger->zFile, O_RDWR, 0644);
    if(pLogger->log_fd == -1){
        pLogger->log_fd = open(pLogger->zFile, O_RDWR | O_CREAT, 0644);
        ftruncate(pLogger->log_fd, LOG_SIZE);
        pLogger->hdr.vers = 1;
    }
    //read log hdr
    pLogger->log_buffer = (void*) mmap(NULL, LOG_SIZE, PROT_READ|PROT_WRITE,MAP_SHARED,pLogger->log_fd,0);
    if(pLogger->hdr.vers == 0){
        memcpy(&pLogger->hdr,pLogger->log_buffer, sizeof(logHdr));
        pLogger->lastLsn = pLogger->hdr.stLsn + sizeof(logHdr); 
    }
    pLogger->log_buffer = mremap(pLogger->log_buffer,LOG_SIZE, LOG_SIZE*pLogger->hdr.vers, MREMAP_MAYMOVE);
    
}

void sqlite3LoggerClose(Logger *pLogger){
    if(pLogger->log_fd == 0)
        return;
    close(pLogger->log_fd);
    sqlite3_free(pLogger->zFile);
    sqlite3_free(pLogger);
}

void* serialize(void *log, enum opcode op, int *size){
    void *data = 0;
    void *tmp;
    switch(op){
        case INSERT:
            //printf("sqlite3Insert\n");
            *size = (sizeof(struct IdxInsertLog)
                    +sizeof(BtreePayload)
                    +((struct IdxInsertLog*)log)->pX->nData
                    +((struct IdxInsertLog*)log)->pX->nZero);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, (struct IdxInsertLog *)log, sizeof(struct IdxInsertLog));
            memcpy(tmp+=sizeof(struct IdxInsertLog), ((struct IdxInsertLog*)log)->pX, sizeof(BtreePayload));
            memcpy(tmp+=sizeof(BtreePayload),((struct IdxInsertLog*)log)->pX->pData,((struct IdxInsertLog*)log)->pX->nZero+((struct IdxInsertLog*)log)->pX->nData);
            return data;
        case IDXINSERT:
            //printf("sqlite3IndexInsert\n");
            *size = (sizeof(struct IdxInsertLog)
                    +sizeof(KeyInfo)
                    +sizeof(BtreePayload)+((struct IdxInsertLog*)log)->pX->nKey);
            tmp = data = sqlite3Malloc(*size);
            memcpy(tmp, (struct IdxInsertLog *)log, sizeof(struct IdxInsertLog));
            memcpy(tmp+=sizeof(struct IdxInsertLog),((struct IdxInsertLog*)log)->pKeyInfo,sizeof(KeyInfo));
            memcpy(tmp+=sizeof(KeyInfo), ((struct IdxInsertLog*)log)->pX, sizeof(BtreePayload));
            memcpy(tmp+=sizeof(BtreePayload),((struct IdxInsertLog*)log)->pX->pKey,((struct IdxInsertLog*)log)->pX->nKey);
            return data;
        case DELETE:
            //printf("sqlite3Delete\n");
            *size = (sizeof(struct IdxDeleteLog));
            tmp = data = sqlite3Malloc(*size);
            memcpy(tmp , (struct IdxDeleteLog *)log, sizeof(struct IdxDeleteLog));
            return data;
        case COMMIT:
            //printf("sqlite3Commit\n");
            *size = 0;
            return NULL;
        case CREATETABLE:
            *size = (sizeof(struct CreateLog));
            tmp = data = sqlite3Malloc(*size);
            memcpy(tmp , (struct CreateLog *)log, sizeof(struct CreateLog));
            return data;
    }
};

void deserialize(void* log, enum opcode op){
    struct IdxInsertLog *iil;
    struct idxDeleteLog *idl;
    void *tmp;
    switch(op){
        case INSERT:
            iil = log;
            iil->pX = log + sizeof(struct IdxInsertLog); 
            tmp = log + sizeof(struct IdxInsertLog) + sizeof(BtreePayload);
            memcpy((void*)&iil->pX->pData,
                    &tmp,
                 sizeof(char*));
            break;
        case IDXINSERT:
            iil = log;
            iil->pKeyInfo = log + sizeof(struct IdxInsertLog);
            iil->pKeyInfo->xCompare+=3;
            iil->pX = log + sizeof(struct IdxInsertLog) + sizeof(KeyInfo);
            tmp = (log + sizeof(struct IdxInsertLog) + sizeof(KeyInfo) + sizeof(BtreePayload));
            memcpy((void*)&iil->pX->pKey,
                    &tmp,
                 sizeof(char*));
            break;
        case COMMIT:
            break;
        default:
            return;
    }
}

int sqlite3LogInit(BtCursor* pCur){
    memset(&pCur->idxInsLog,0,sizeof(struct IdxInsertLog));
    memset(&pCur->idxDelLog,0,sizeof(struct IdxDeleteLog));
};

int sqlite3LogCursor(BtCursor* pCur, int iDb, int iTable, int wrFlag, KeyInfo* pKeyInfo){
    static int vers = 0;
    pCur->idxInsLog = (struct IdxInsertLog){.vers = vers, .iDb = iDb, .iTable = iTable, .wrFlag = wrFlag, .pKeyInfo = pKeyInfo}; 
    pCur->idxDelLog = (struct IdxDeleteLog){.vers = vers, .iTable = iTable, .wrFlag = wrFlag};
};

int sqlite3LogPayload(BtCursor* pCur, const BtreePayload *pX, int appendBias, int seekResult){
    pCur->idxInsLog.pX = pX;
    pCur->idxInsLog.appendBias = appendBias;
    pCur->idxInsLog.seekResult = seekResult;
};

int sqlite3LogiCell(BtCursor* pCur, int iCellDepth, int iCellIdx, u32 pPagePgno){
    pCur->idxDelLog.iCellDepth = iCellDepth;
    pCur->idxDelLog.iCellIdx = iCellIdx;
    pCur->idxDelLog.pPagePgno = pPagePgno;
};

int sqlite3LogCreateTable(BtShared* p, int flags){
    p->cLog.flags = flags;
};

void free_qLogCell(qLogCell* m_qLogCell){
    int rc;
    sqlite3_free((void*)m_qLogCell->m_logCell->data);
    sqlite3_free(m_qLogCell);
    //memset(m_qLogCell->m_logCell->data,0,m_qLogCell->m_logCell->data_size);
    //memset(m_qLogCell->m_logCell,0,sizeof(qLogCell)+sizeof(logCell));
};

void sqlite3Log(Logger *pLogger, void *log, enum opcode op){
    if(pLogger->state == OFF)
        return;
    void *pPtr;
    logCell *m_logCell;
    qLogCell *m_qLogCell;
    struct list_head *tmp1, *tmp2;
    m_qLogCell = pPtr = sqlite3MallocZero(sizeof(logCell)+sizeof(qLogCell));
    m_logCell = pPtr + sizeof(qLogCell);

    m_logCell->op = op;
    m_logCell->data = serialize(log, op, &m_logCell->data_size);
    m_qLogCell->m_logCell = m_logCell;
    add_to_logqueue(pLogger,m_qLogCell);
    pLogger->p_check += 1;

    if(op == COMMIT){
        int iOffset = pLogger->lastLsn;
        int tmp = pLogger->hdr.vers;
        int rc = 0;
        list_for_each_prev_safe(tmp1,tmp2,&pLogger->q){
            m_qLogCell = list_entry(tmp1, qLogCell, q);
            m_logCell = m_qLogCell->m_logCell;
            while(iOffset + sizeof(logCell) + m_logCell->data_size > LOG_SIZE*pLogger->hdr.vers){
                fallocate(pLogger->log_fd, 0, LOG_SIZE*pLogger->hdr.vers, LOG_SIZE);
                pLogger->hdr.vers++;
                pLogger->log_buffer = mremap(pLogger->log_buffer,LOG_SIZE*(pLogger->hdr.vers - 1), LOG_SIZE*pLogger->hdr.vers, MREMAP_MAYMOVE);
            }
            m_logCell->nextLsn = iOffset+(sizeof(logCell) + m_logCell->data_size);
            memcpy(pLogger->log_buffer + iOffset, m_logCell, sizeof(logCell));
            iOffset += sizeof(logCell);
            if(m_logCell->data != 0)
                memcpy(pLogger->log_buffer + iOffset, m_logCell->data, m_logCell->data_size);
            iOffset+=m_logCell->data_size;
            del_from_logqueue(m_qLogCell);
            free_qLogCell(m_qLogCell);
        }
        msync(pLogger->log_buffer + pLogger->lastLsn, iOffset - pLogger->lastLsn, MS_SYNC);
        if(tmp != pLogger->hdr.vers){
            memcpy(pLogger->log_buffer, &pLogger->hdr,sizeof(logHdr));
            msync(pLogger->log_buffer,sizeof(logHdr),MS_SYNC);
        }
        pLogger->lastLsn = iOffset;
    }
};

int sqlite3LogAnalysis(Logger *pLogger){
    logCell *m_logCell;
    qLogCell *m_qLogCell;
    void *tmp;
    int offset =  pLogger->hdr.stLsn + sizeof(logHdr);    
    while(1){
        m_logCell = sqlite3Malloc(sizeof(logCell));
        memcpy(m_logCell,pLogger->log_buffer + offset,sizeof(logCell));
        if(m_logCell->op == EXIT){
            sqlite3_free(m_logCell);
            break;
        }
        m_qLogCell = sqlite3Malloc(sizeof(qLogCell));
        m_qLogCell->m_logCell = m_logCell;
        add_to_logqueue(pLogger,m_qLogCell);
        m_logCell->data = sqlite3Malloc(m_logCell->data_size);
        offset+=sizeof(logCell);
        memcpy( m_logCell->data,pLogger->log_buffer + offset, m_logCell->data_size);
        offset+=m_logCell->data_size;
    }
    pLogger->lastLsn = offset - sizeof(logHdr);
    return SQLITE_OK;
};

void sqlite3LogFileInit(Logger *pLogger){
    close(pLogger->log_fd);
    sqlite3LoggerOpenPhaseTwo(0,0,0,pLogger,0);
}

void sqlite3LogRecovery(Logger *pLogger ,Btree* pBtree){
    qLogCell *m_qLogCell;
    logCell *m_logCell;
    struct list_head *tmp1, *tmp2;
    MemPage* pPage;

    struct IdxInsertLog *iil;
    struct IdxDeleteLog *idl;
    struct CreateLog *cl;
    BtCursor *btCsr;
    int rc;

    if(list_empty(&pLogger->q))
        return;
    btCsr= sqlite3Malloc(sizeof(BtCursor));
    pLogger->state = OFF;
    sqlite3BtreeBeginTrans(pBtree,4);
    list_for_each_prev_safe(tmp1,tmp2,&pLogger->q){
        sqlite3BtreeCursorZero(btCsr);
        m_qLogCell = list_entry(tmp1, qLogCell, q);
        m_logCell = m_qLogCell->m_logCell;
        deserialize(m_logCell->data,m_logCell->op);
        switch(m_logCell->op){
            case INSERT:
                iil = m_logCell->data;
                sqlite3BtreeCursor(pBtree,iil->iTable,iil->wrFlag,0x0,btCsr);
                sqlite3BtreeLast(btCsr, &rc);
                sqlite3BtreeIntegerKey(btCsr);
                sqlite3BtreeInsert(btCsr, iil->pX, iil->appendBias, iil->seekResult);
                break;
            case IDXINSERT:
                iil = m_logCell->data;
                sqlite3BtreeCursor(pBtree,iil->iTable, iil->wrFlag, iil->pKeyInfo, btCsr);
                sqlite3BtreeInsert(btCsr, iil->pX, iil->appendBias, iil->seekResult);
                break;
            case DELETE:
                idl = m_logCell->data;
                sqlite3BtreeCursor(pBtree,idl->iTable,idl->wrFlag, 0x0,btCsr);
                sqlite3BtreeFirst(btCsr, &rc);
                btCsr->iPage = idl->iCellDepth;
                btCsr->aiIdx[idl->iCellDepth] = idl->iCellIdx;
                btreeGetPage(pBtree->pBt,idl->pPagePgno,&pPage,0);
                releasePage(btCsr->apPage[idl->iCellDepth]);
                btCsr->apPage[idl->iCellDepth] = pPage;
                btreeParseCell(btCsr->apPage[idl->iCellDepth],idl->iCellIdx,&btCsr->info);
                sqlite3BtreeDelete(btCsr,idl->flags);
            case COMMIT:
                break;
            case CREATETABLE:
                cl = m_logCell->data;
                sqlite3BtreeCreateTable(pBtree,&rc,cl->flags);
                break;
        }
        sqlite3BtreeCloseCursor(btCsr);
        del_from_logqueue(m_qLogCell);
        free_qLogCell(m_qLogCell);
    }
	sqlite3_free(btCsr);
    pBtree->inTrans = TRANS_WRITE;
    pBtree->pBt->inTransaction = TRANS_WRITE;
	pLogger->p_check = LOG_LIMIT;
    sqlite3BtreeCommit(pBtree);
    sqlite3LogFileInit(pLogger);

	pLogger->p_check = 0;
    pLogger->state = ON;

};

int sqlite3LogForceAtCommit(Logger *pLogger){
	logCell m_logCell;
    memset(&m_logCell, 0, sizeof(m_logCell));
    sqlite3Log(pLogger,NULL,COMMIT);
};

int sqlite3LogRollback(Logger *pLogger){
    struct list_head *tmp1, *tmp2;
    qLogCell *m_qLogCell;
    list_for_each_safe(tmp1,tmp2,&pLogger->q){
        m_qLogCell = list_entry(tmp1, qLogCell, q);
        del_from_logqueue(m_qLogCell);
        free_qLogCell(m_qLogCell);
    }
};

int sqlite3LogCheckPoint(Logger *pLogger){
    pLogger->hdr.stLsn = pLogger->lastLsn - sizeof(logHdr);
    memcpy(pLogger->log_buffer, &pLogger->hdr,sizeof(logHdr));
    msync(pLogger->log_buffer, sizeof(logHdr),MS_SYNC);
};



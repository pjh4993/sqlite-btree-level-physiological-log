#include "list.h"
#include "btree.h"
#include <stdio.h>
#include "sqliteInt.h"
#include "log.h"
#define HDR_SIZE sizeof(logHdr)
#define LOG_SIZE 1024 * 1024 * 8

static inline void add_to_logqueue(Logger *pLogger, qLogCell * p)
{
	list_add(&p->q, &pLogger->q);
}

static inline void del_from_logqueue(qLogCell *p){
    list_del(&p->q);    
}

int sqlite3LoggerOpenPhaseOne(Logger **ppLogger){
    //will called in btree level
    //create logger for BtShared and initialize it
	Logger *pLogger;
    if(*ppLogger != 0){
        return SQLITE_OK;
    }
	pLogger = (Logger *)sqlite3MallocZero(sizeof(Logger));
    pLogger->state = START;
    INIT_LIST_HEAD(&pLogger->q);
	*ppLogger = pLogger;
	return SQLITE_OK;
};

int sqlite3LoggerOpenPhaseTwo(pager *pPager, int nPathname,Logger *pLogger){
    //create file for logger
    int fout = 0;                    /* VFS flags returned by xOpen() */
    int rc= 0;
    const char* zPathname = pPager->zFilename;
    int nPathname = sqlite3Strlen30(pPager->zFilename);
    if(pLogger == 0 || pLogger->log_fd != 0)
        return 0;
    if(pPager != 0x0){
        char *tmp = (char*)sqlite3Malloc(nPathname+7);
        pLogger->zFile = (char*)sqlite3Malloc(nPathname+6);
        memcpy(pLogger->zFile, zPathname, nPathname);
        memcpy(&pLogger->zFile[nPathname], "-log\000", 4+2);
        memcpy(tmp, zPathname, nPathname);
        memcpy(&tmp[nPathname], "-output\000", 7+2);
        pLogger->fd = open(tmp,O_RDWR|O_CREAT,0644);    
        sqlite3_free(tmp);
    }
    pLogger->log_fd = open(pLogger->zFile, O_RDWR, 0644);
    if(pLogger->log_fd == -1){
        rc = 1;
        if(pLogger->state == START){
            pLogger->log_fd = 0;
            pLogger->state = OFF;
            return 0;
        }
        pLogger->log_fd = open(pLogger->zFile, O_RDWR | O_CREAT, 0644);
        ftruncate(pLogger->log_fd, LOG_SIZE);
        pLogger->hdr.vers = 1;
    }
    //read log hdr
    pLogger->log_buffer = (void*) mmap(NULL, LOG_SIZE, PROT_READ|PROT_WRITE,MAP_SHARED,pLogger->log_fd,0);
    pLogger->state = ON;
    memcpy(&pLogger->hdr,pLogger->log_buffer, sizeof(logHdr));
    if(pLogger->hdr.vers == 0){
        pLogger->hdr.vers = 1;
        pLogger->hdr.stLsn = 0;
        memcpy(pLogger->log_buffer,&pLogger->hdr, sizeof(logHdr));
        msync(pLogger->log_buffer, sizeof(logHdr),MS_SYNC);
        pLogger->log_buffer = mremap(pLogger->log_buffer,LOG_SIZE, LOG_SIZE*pLogger->hdr.vers, MREMAP_MAYMOVE);
    }
    pLogger->lastLsn = pLogger->hdr.stLsn + sizeof(logHdr); 
    if(rc == 0){
        sqlite3OsDelete(pPager->pVfs, pPager->zWal, 0);
        rc = pagerOpenWal(pPager);

        if( rc==SQLITE_OK ){
            pPager->journalMode = PAGER_JOURNALMODE_WAL;
            pPager->eState = PAGER_OPEN;
        }

        sqlite3LogAnalysis(pLogger);
    }
}

void sqlite3LoggerClose(Logger *pLogger){
    if(!pLogger || pLogger->log_fd == 0)
        return;
    close(pLogger->log_fd);
    sqlite3_free(pLogger->zFile);
    sqlite3_free(pLogger);
}

void* serialize(void *log, enum opcode op, int *size){
    void *data = 0;
    void *tmp;
    int i;
    op = op >> 4;
    switch(op){
        case bt_open:
            *size = sizeof(struct BtOpenLog);
            if(TYPE(log, struct BtOpenLog)->zFilename != 0x0)
                *size+=sqlite3Strlen30(TYPE(log, struct BtOpenLog)->zFilename); 
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct BtOpenLog), sizeof(struct BtOpenLog));
            return data;
        case bt_ibt:
            *size = sizeof(struct BtIbtLog);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct BtIbtLog), sizeof(struct BtIbtLog));
            return data;
        case bt_flag:
            *size = sizeof(struct BtFlagLog);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct BtFlagLog), sizeof(struct BtFlagLog));
            return data;
        case bt_save:
            *size = sizeof(struct BtSaveLog);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct BtSaveLog), sizeof(struct BtSaveLog));
            return data;
        case csr_open:
            *size = sizeof(struct CsrOpenLog);
            if(TYPE(log, struct CsrOpenLog)->pKeyInfo != 0x0)
               *size += (sizeof(KeyInfo) + TYPE(log, struct CsrOpenLog)->pKeyInfo->nField*sizeof(u8)
                       +sqlite3Strlen30(TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0]->name)); 
            break;
        case csr_icsr:
            *size = sizeof(struct CsrIcsrLog);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct CsrIcsrLog), sizeof(struct CsrIcsrLog));
            return data;
        case csr_insert:
            *size = sizeof(struct CsrInsertLog);
            if(TYPE(log, struct CsrInsertLog)->pX != 0x)
                *size+=(sizeof(BtreePayload) + TYPE(log, struct CsrInsertLog)->pX->nMem * sizeof(Mem));
            break;
        case csr_flag:
            *size = sizeof(struct CsrFlagLog);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct CsrFlagLog), sizeof(struct CsrFlagLog));
            return data;
        case csr_unpacked:
            *size = sizeof(struct CsrUnpackedLog) 
                + sizeof(KeyInfo) + TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->nField*sizeof(u8)
                + sqlite3Strlen30(TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0]->name); 
            tmp = data  = sqlite3Malloc(*size);
            break;
    }
};

/* BtCursor logging
 * */

void inline sqlite3LogCursorOpen(int wrFlag, BtCursor *pCur, Btree *pBtree){
    pCur->al.csr_open_log = {.iCsr = pCur->idx_aries, .iDb = pCur->pBtree->idx_aries, 
        .wrFlag = wrFlag, .pKeyInfo = pCur->pKeyInfo, .iBt = pBtree->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_open_log, CSR_OPEN); 
};

void inline sqlite3LogCursorClose(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, CSR_CLOSE); 
};

void inline sqlite3LogCursorNext(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, NEXT); 
};

void inline sqlite3LogCursorPrev(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, PREV); 
};

void inline sqlite3LogCursorEof(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, CSR_EOF); 
};

void inline sqlite3LogCursorFirst(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, FIRST); 
};

void inline sqlite3LogCursorLast(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, LAST); 
};

void inline sqlite3LogCursorIntegerKey(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, INTEGERKEY); 
};

void inline sqlite3LogCursorUnpacked(BtCursor *pCur, UnpackedRecord *pIdxKey, int intKey, int biasRight){
    pCur->al.csr_unpacked_log = {.iCsr = pCur->idx_aries, .pIdxKey = pIdxKey, .intKey = intKey, .biasRight = biasRight};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_unpacked_log, MV_UNPACKED); 
};

void inline sqlite3LogCursorClear(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, CLEAR); 
};

void inline sqlite3LogCursorRestore(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, RESTORE); 
};

void inline sqlite3LogCursorIncrBlob(BtCursor *pCur){
    pCur->al.csr_icsr_log = {.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, INCRBLOB); 
};

void inline sqlite3LogCursorInsert(BtCursor *pCur, const BtreePayload *pX, int appendBias, int seekResult){
    pCur->al.csr_insert_log = {.iCsr = pCur->idx_aries, .pX = pX, .appendBias = appendBias, .seekResult = seekResult};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_insert_log, INSERT); 
};

void inline sqlite3LogCursorDelete(BtCursor *pCur, u8 flags){
    pCur->al.csr_flag_log = {.iCsr = pCur->idx_aries, .flags = flags};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_flag_log, DELETE); 
};


/* Btree logging
 * */

void inline sqlite3LogBtreeOpen(Btree *pBtree, char* zFilename, int flags, int vfsFlags){
    pBtree->al.bt_open_log = {.iBt = pBtree->idx_aries, .zFilename = zFilename, .flags = flags, .vfsFlags = vfsFlags};
    sqlite3Log(pBtree, &pBtree->al.bt_iBt_log, OPEN); 
};

void inline sqlite3LogBtreeClose(Btree *pBtree){
    pBtree->al.bt_iBt_log = {.iBt = pBtree->idx_aries};
    sqlite3Log(pBtree, &pBtree->al.bt_iBt_log, CLOSE); 
};

void inline sqlite3LogBtreeCreate(Btree *pBtree, int flags){
    pCur->al.bt_flag_log = {.iBt = pBtree->idx_aries, .flags = flags};
    sqlite3Log(pBtree, &pBtree->al.bt_flag_log, CREATE); 
};

void inline sqlite3LogBtreeDrop(Btree *pBtree, int iTable){
    pCur->al.bt_flag_log = {.iBt = pBtree->idx_aries, .flags = iTable};
    sqlite3Log(pBtree, &pBtree->al.bt_flag_log, DROP); 
};

void inline sqlite3LogBtreeClear(Btree *pBtree, int iTable){
    pCur->al.bt_flag_log = {.iBt = pBtree->idx_aries, .flags = iTable};
    sqlite3Log(pBtree, &pBtree->al.bt_flag_log, DROP); 
};

void inline sqlite3LogBtreeBeginTrans(Btree *pBtree, int wrFlag){
    pCur->al.bt_flag_log = {.iBt = pBtree->idx_aries, .flags = wrFlag};
    sqlite3Log(pBtree, &pBtree->al.bt_flag_log, BEGINTRANS); 
};

void inline sqlite3LogBtreeEndTrans(Btree *pBtree){
    pBtree->al.bt_iBt_log = {.iBt = pBtree->idx_aries};
    sqlite3Log(pBtree, &pBtree->al.bt_iBt_log, ENDTRANS); 
};

void inline sqlite3LogBtreeSavepoints(Btree *pBtree, int op, int iSavepoint){
    pCur->al.bt_flag_log = {.iBt = pBtree->idx_aries, .op = op, .iSavepoint = iSavepoint};
    sqlite3Log(pBtree, &pBtree->al.bt_save_log, SAVEPOINT); 
};

/* Commit and Rollback should be redesigned
 * */

int sqlite3LogForceAtCommit(Btree *pBtree){
    if(!pLogger || pLogger->state == OFF)
        return 0;
    sqlite3Log(pBtree,NULL,COMMIT);
};

int sqlite3LogRollback(Logger *pLogger){
    if(!pLogger || pLogger->state == OFF)
        return 0;

    struct list_head *tmp1, *tmp2;
    qLogCell *m_qLogCell;
    list_for_each_safe(tmp1,tmp2,&pLogger->q){
        m_qLogCell = list_entry(tmp1, qLogCell, q);
        del_from_logqueue(m_qLogCell);
        free_qLogCell(m_qLogCell);
    }
};


void free_qLogCell(qLogCell* m_qLogCell){
    int rc;
    sqlite3_free((void*)m_qLogCell->m_logCell->data);
    sqlite3_free(m_qLogCell);
};

void sqlite3Log(Btree * pBtree, void *log, enum opcode op){
    static int vers = 0;
    Logger *pLogger = pBtree->pBt->pLogger;
    void *pPtr;
    logCell *m_logCell;
    qLogCell *m_qLogCell;
    struct list_head *tmp1, *tmp2;

    if(!pLogger || pLogger->state == OFF){
        pLogger->p_check = LOG_LIMIT;
        return;
    }

    m_qLogCell = pPtr = sqlite3MallocZero(sizeof(logCell)+sizeof(qLogCell));
    m_logCell = pPtr + sizeof(qLogCell);
    m_logCell->vers = vers++;
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
                pLogger->p_check = LOG_LIMIT;
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
        msync(pLogger->log_buffer + pLogger->lastLsn, iOffset - (pLogger->lastLsn + sizeof(logHdr)), MS_SYNC);
        if(tmp != pLogger->hdr.vers){
            memcpy(pLogger->log_buffer, &pLogger->hdr,sizeof(logHdr));
            msync(pLogger->log_buffer,sizeof(logHdr),MS_SYNC);
        }
        pLogger->lastLsn = iOffset;
    }
};

int sqlite3LogAnalysis(Logger *pLogger){
    logCell *m_logCell;
    void *tmp;
    int offset =  pLogger->hdr.stLsn + sizeof(logHdr);    

    pLogger->state = OFF;
	pLogger->p_check = LOG_LIMIT;

    while(1){
        m_logCell = sqlite3Malloc(sizeof(logCell));
        memcpy(m_logCell,pLogger->log_buffer + offset,sizeof(logCell));
        if(m_logCell->op == EXIT){
            sqlite3_free(m_logCell);
            break;
        }
        m_logCell->data = sqlite3Malloc(m_logCell->data_size);
        offset+=sizeof(logCell);
        memcpy( m_logCell->data,pLogger->log_buffer + offset, m_logCell->data_size);
        offset+=m_logCell->data_size;
        sqlite3LogRecovery(m_logCell);
    }

    sqlite3LogFileInit(pLogger);
    sqlite3BtreeCheckpoint(pBtree,0,0,0);

	pLogger->p_check = 0;
    pLogger->state = ON;

    return SQLITE_OK;
};

void sqlite3LogFileInit(Logger *pLogger){
    close(pLogger->log_fd);
    remove(pLogger->zFile);
    pLogger->log_fd = 0;
    sqlite3LoggerOpenPhaseTwo(0,pLogger);
}

void sqlite3LogRecovery(Logger *pLogger, logCell *m_logCell){

    struct BtIbtLog *bt_ibt_log;
    struct BtOpenLog *bt_open_log;
    struct BtFlagLog *bt_flag_log;
    struct BtSaveLog *bt_save_log;

    struct CsrOpenLog *csr_open_log;
    struct CsrIcsrLog *csr_Icsr_log;
    struct CsrUnpackedLog *csr_unpacked_log;
    struct CsrInsertLog *csr_insert_log;
    struct CsrFlagLog *csr_flag_log;

    int iBt,iCsr;
    void* tmp = deserialize(m_logCell, m_logCell->op);

    switch(m_logCell->op){
        /* Btree recovery
         * */
        case BTREE_OPEN:
            bt_open_log = tmp;
            sqlite3BtreeOpen(pLogger->pVfs, bt_open_log->zFilename, pLogger->db, &pLogger->apBt[pLogger->nBtree],
                    bt_open_log->flags, bt_open_log->vfsFlags);
            pLogger->nBtree++;
            break;
        case BTREE_CLOSE:
            bt_iBt_log = tmp;
            iBt = FIND_IBT(bt_iBt_log);
            sqlite3BtreeClose(pLogger->apBt[iBt]);
            RELEASE_IBT(iBt);
            break;
        case CREATE:
            bt_flag_log = tmp;
            iBt = FIND_IBT(bt_flag_log);
            sqlite3BtreeCreateTable(pLogger->apBt[iBt], (int*)tmp, bt_flag_log->flags);
            break;
        case DROP:
            bt_flag_log = tmp;
            iBt = FIND_IBT(bt_flag_log);
            sqlite3BtreeDropTable(pLogger->apBt[iBt], bt_flag_log->flags, (int*)tmp);
            break;
        case CLEAR:
            bt_flag_log = tmp;
            iBt = FIND_IBT(bt_flag_log);
            sqlite3BtreeClearTable(pLogger->apBt[iBt], bt_flag_log->flags, (int*)tmp);
            break;
        case BEGINTRANS:
            bt_flag_log = tmp;
            iBt = FIND_IBT(bt_flag_log);
            sqlite3BtreeBeginTrans(pLogger->apBt[iBt], bt_flag_log->flags);
            break;
        case SAVEPOINT:
            bt_save_log = tmp;
            iBt = FIND_IBT(bt_flag_log);
            sqlite3BtreeSavepoint(pLogger->apBt[iBt], bt_save_log->op, bt_save_log->iSavepoint);
            break;
        case COMMIT:
            break;
        /* BtCursor recovery
         * */
        case CSR_OPEN:
            csr_open_log = tmp;
            iBt = FIND_IBT(csr_open_log);
            sqlite3BtreeCursor(pLogger->apBt[iBt], csr_open_log->iTable, csr_open_log->wrFlag, 
                    csr_open_log->pKeyInfo, pLogger->apCsr[pLogger->nCsr]);
            pLogger->nCsr++;
            break;
        case CSR_CLOSE:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeCloseCursor(pLogger->apCsr[iCsr]);
            RELEASE_ICSR(iCsr);
            break;
        case NEXT:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeNext(pLogger->apCsr[iCsr], (int*)tmp);
            break;
        case PREV:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreePrevious(pLogger->apCsr[iCsr], (int*)tmp);
            break;
        case FIRST:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeFirst(pLogger->apCsr[iCsr], (int*)tmp);
            break;
        case LAST:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeLast(pLogger->apCsr[iCsr], (int*)tmp);
            break;
        case CSR_EOF:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeEof(pLogger->apCsr[iCsr]);
            break;
        case INTEGERKEY:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeIntegerKey(pLogger->apCsr[iCsr]);
            break;
        case MV_UNPACKED:
            csr_unpacked_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeMoveToUnpacked(pLogger->apCsr[iCsr], csr_unpacked_log->pIdxKey, csr_unpacked_log->intKey,
                    csr_unpacked_log->biasRight, (int*)tmp);
            break;
        case CLEAR:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeClearCursor(pLogger->apCsr[iCsr]);
            break;
        case RESTORE:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeCursorRestore(pLogger->apCsr[iCsr], (int*)tmp);
            break;
        case INCRBLOB:
            csr_icsr_log = tmp;
            iCsr = FIND_ICSR(csr_icsr_log);
            sqlite3BtreeIncrBlobCursor(pLogger->apCsr[iCsr]);
            break;
        case INSERT:
            csr_insert_log = tmp;
            iCsr = FIND_ICSR(csr_insert_log);
            sqlite3BtreeInsert(pLogger->apCsr[iCsr], csr_insert_log->pX, 
                    csr_insert_log->appendBias, csr_insert_log->seekResult);
            break;
        case DELETE:
            csr_flag_log = tmp;
            iCsr = FIND_ICSR(csr_flag_log);
            sqlite3BtreeDelete(pLogger->apCsr[iCsr], csr_flag_log->flags);
            break;
    };
};


int sqlite3LogCheckPoint(Logger *pLogger){
    if(!pLogger || pLogger->state == OFF)
        return 0;

    dprintf(pLogger->fd,"operation : CHECKPOINT\n");

    logCell m_logCell = {};
    pLogger->hdr.stLsn = 0;
    pLogger->lastLsn = sizeof(logHdr);
    memcpy(pLogger->log_buffer, &pLogger->hdr,sizeof(logHdr));
    memcpy(pLogger->log_buffer + sizeof(logHdr), &m_logCell, sizeof(logCell));
    msync(pLogger->log_buffer, sizeof(logHdr) + sizeof(logCell) ,MS_SYNC);
    pLogger->p_check = 0;
};


/*
void deserialize(void* log, enum opcode op){
    struct IdxInsertLog *iil;
    struct idxDeleteLog *idl;
    int i;
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
        case UPDATE:
        case IDXINSERT:
            iil = log;
            iil->pKeyInfo = log + sizeof(struct IdxInsertLog);
            iil->pKeyInfo->xCompare+=3;
            iil->pKeyInfo->aSortOrder = log + sizeof(struct IdxInsertLog) + sizeof(KeyInfo);
            iil->pX = log + sizeof(struct IdxInsertLog) + sizeof(KeyInfo) + sizeof(u8)*iil->pKeyInfo->nField;
            tmp = (log + sizeof(struct IdxInsertLog) + sizeof(KeyInfo) + sizeof(u8)*iil->pKeyInfo->nField + sizeof(BtreePayload));
            memcpy((void*)&iil->pX->pKey,
                    &tmp,
                 sizeof(char*));
            tmp += iil->pX->nKey;
            memcpy((void*)&iil->pX->aMem,
                    &tmp,
                 sizeof(struct Mem*));
            tmp += sizeof(struct Mem)*iil->pX->nMem;
            for(i=0; i< iil->pX->nMem; i++){
                if(!(iil->pX->aMem[i].flags & (MEM_Str | MEM_Blob)))
                    continue;
                iil->pX->aMem[i].z = tmp;
                tmp+=iil->pX->aMem[i].n;
                iil->pX->aMem[i].zMalloc = tmp;
                tmp+=iil->pX->aMem[i].szMalloc;
            }
            break;
        case COMMIT:
            break;
        default:
            return;
    }
}
*/
/*
void* serialize(void *log, enum opcode op, int *size){
    void *data = 0;
    void *tmp;
    int i;
    switch(op){
        case INSERT:
            //printf("sqlite3Insert\n");
            *size = (sizeof(struct IdxInsertLog)
                    +sizeof(BtreePayload)
                    +((struct IdxInsertLog*)log)->pX->nData);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, (struct IdxInsertLog *)log, sizeof(struct IdxInsertLog));
            memcpy(tmp+=sizeof(struct IdxInsertLog), ((struct IdxInsertLog*)log)->pX, sizeof(BtreePayload));
            memcpy(tmp+=sizeof(BtreePayload),((struct IdxInsertLog*)log)->pX->pData,((struct IdxInsertLog*)log)->pX->nData);
            return data;
        case UPDATE:
        case IDXINSERT:
            //printf("sqlite3IndexInsert\n");
            *size = (sizeof(struct IdxInsertLog)
                    +sizeof(KeyInfo)
                    +sizeof(u8)*((struct IdxInsertLog*)log)->pKeyInfo->nField
                    +sizeof(BtreePayload)+((struct IdxInsertLog*)log)->pX->nKey)
                    +(((struct IdxInsertLog*)log)->pX->nMem) * sizeof(struct Mem);
            for(i=0; i < ((struct IdxInsertLog*)log)->pX->nMem; i++){
                if(!(((struct IdxInsertLog*)log)->pX->aMem[i].flags & (MEM_Str | MEM_Blob)))
                    continue;
                *size += ((struct IdxInsertLog*)log)->pX->aMem[i].n;
                *size += ((struct IdxInsertLog*)log)->pX->aMem[i].szMalloc;
            }
            tmp = data = sqlite3Malloc(*size);
            memcpy(tmp, (struct IdxInsertLog *)log, sizeof(struct IdxInsertLog));
            memcpy(tmp+=sizeof(struct IdxInsertLog),((struct IdxInsertLog*)log)->pKeyInfo,sizeof(KeyInfo));
            memcpy(tmp+=sizeof(KeyInfo), ((struct IdxInsertLog*)log)->pKeyInfo->aSortOrder, sizeof(u8)*((struct IdxInsertLog*)log)->pKeyInfo->nField);
            memcpy(tmp+=sizeof(u8)*((struct IdxInsertLog*)log)->pKeyInfo->nField,((struct IdxInsertLog*)log)->pX, sizeof(BtreePayload));
            memcpy(tmp+=sizeof(BtreePayload),((struct IdxInsertLog*)log)->pX->pKey,((struct IdxInsertLog*)log)->pX->nKey);
            tmp += ((struct IdxInsertLog*)log)->pX->nKey;
            for(i=0; i < ((struct IdxInsertLog*)log)->pX->nMem; i++){
                memcpy(tmp, &((struct IdxInsertLog*)log)->pX->aMem[i], sizeof(struct Mem));
                tmp+=sizeof(struct Mem);
            }

            for(i=0; i < ((struct IdxInsertLog*)log)->pX->nMem; i++){
                if(!(((struct IdxInsertLog*)log)->pX->aMem[i].flags & (MEM_Str | MEM_Blob)))
                    continue;
                if(((struct IdxInsertLog*)log)->pX->aMem[i].n > 0){
                    memcpy(tmp, ((struct IdxInsertLog*)log)->pX->aMem[i].z, ((struct IdxInsertLog*)log)->pX->aMem[i].n * sizeof(char));
                    tmp+=((struct IdxInsertLog*)log)->pX->aMem[i].n;
                }
                if(((struct IdxInsertLog*)log)->pX->aMem[i].szMalloc > 0){
                    memcpy(tmp, ((struct IdxInsertLog*)log)->pX->aMem[i].zMalloc, ((struct IdxInsertLog*)log)->pX->aMem[i].szMalloc * sizeof(char));
                    tmp+=((struct IdxInsertLog*)log)->pX->aMem[i].szMalloc;
                }
            }

            return data;
        case IDXDELETE:
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
*/


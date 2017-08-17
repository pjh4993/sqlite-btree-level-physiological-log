#include "list.h"
#include "btree.h"
#include "pager.h"
#include <stdio.h>
#include "sqliteInt.h"
#include "log.h"
#define HDR_SIZE sizeof(logHdr)
#define LOG_SIZE 1024 * 1024 * 8
#define FIND_IBT(log, pLogger) \
    (pLogger->pBtree)
#define FIND_ICSR(log, pLogger) \
    pLogger->apCsr[(log->iCsr%10)]
#define FIND_ICOL(log, pLogger) \
    sqlite3FindCollSeq(pLogger->db, (u8)(log->enc2), log->zName,1)
#define RELEASE_IBT(log, pLogger) \
    (pLogger->apBt[(log->iBt)] = 0x0)
#define RELEASE_ICSR(log, pLogger) \
    (pLogger->apCsr[(log->iCsr)] = 0x0)
#define TYPE(log, type) \
    ((type *)(log))
#define WRITE(dest, src, tmp) \
    tmp = (void*)dest; \
    (memcpy(&tmp, src, sizeof(src)))


static inline void add_to_logqueue(Logger *pLogger, qLogCell * p)
{
	list_add(&p->q, &pLogger->q);
}

static inline void del_from_logqueue(qLogCell *p){
    list_del(&p->q);    
}

int sqlite3LoggerOpenPhaseOne(Btree *pBtree){
    //will called in btree level
    //create logger for BtShared and initialize it
    BtShared *pBt = pBtree->pBt;
	Logger *pLogger;
    if(pBt->pLogger != 0){
        return SQLITE_OK;
    }
	pLogger = (Logger *)sqlite3MallocZero(sizeof(Logger));
    pLogger->state = START;
    INIT_LIST_HEAD(&pLogger->q);
    pLogger->db = pBtree->db;
    pLogger->pBtree = pBtree;
    pBt->pLogger = pLogger;
	return SQLITE_OK;
};

int sqlite3LoggerOpenPhaseTwo(struct Pager *pPager, Logger *pLogger){
    //create file for logger
    int fout = 0;                    /* VFS flags returned by xOpen() */
    int rc= 0;
    const char* zPathname;
    int nPathname;
    if(pLogger == 0 || pLogger->log_fd != 0)
        return 0;
    if(pPager != 0x0){
        zPathname = pPager->zFilename;
        nPathname = sqlite3Strlen30(pPager->zFilename);
        char *tmp = (char*)sqlite3Malloc(nPathname+7);
        pLogger->zFile = (char*)sqlite3Malloc(nPathname+6);
        memcpy(pLogger->zFile, zPathname, nPathname);
        memcpy(&pLogger->zFile[nPathname], "-log\000", 4+2);
        memcpy(tmp, zPathname, nPathname);
        memcpy(&tmp[nPathname], "-output\000", 7+2);
        pLogger->fd = open(tmp,O_RDWR|O_CREAT,0644);    
        pLogger->pVfs = pPager->pVfs;
        sqlite3_free(tmp);
    }
    pLogger->log_fd = open(pLogger->zFile, O_RDWR, 0644);
    if(pLogger->log_fd == -1){
        rc = 1;
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
            if(TYPE(log, struct BtOpenLog)->zFilename != 0x0)
                memcpy(tmp+=sizeof(struct BtOpenLog), TYPE(log, struct BtOpenLog)->zFilename, *size - sizeof(struct BtOpenLog));
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
            if(TYPE(log, struct CsrOpenLog)->pKeyInfo != 0x0){
                *size += (sizeof(struct KeyInfo) + TYPE(log, struct CsrOpenLog)->pKeyInfo->nField*sizeof(u8));
                if(TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0] != 0x0){
                    *size += sqlite3Strlen30(TYPE(log, struct CsrOpenLog)->zName);
                    TYPE(log,struct CsrOpenLog)->enc2 = TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0]->enc2;
                }
            }
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct CsrOpenLog),sizeof(struct CsrOpenLog));
            if(TYPE(log, struct CsrOpenLog)->pKeyInfo != 0x0){
                memcpy(tmp+=sizeof(struct CsrOpenLog), TYPE(log, struct CsrOpenLog)->pKeyInfo, sizeof(struct KeyInfo));
                memcpy(tmp+=sizeof(struct KeyInfo), TYPE(log, struct CsrOpenLog)->pKeyInfo->aSortOrder,
                        TYPE(log, struct CsrOpenLog)->pKeyInfo->nField*sizeof(u8));

                if(TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0] != 0x0){
                    memcpy(tmp+=TYPE(log, struct CsrOpenLog)->pKeyInfo->nField*sizeof(u8), TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0]->zName,
                            sqlite3Strlen30(TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0]->zName));
                }
            }
            return data;
        case csr_icsr:
            *size = sizeof(struct CsrIcsrLog);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct CsrIcsrLog), sizeof(struct CsrIcsrLog));
            return data;
        case csr_insert:
            *size = sizeof(struct CsrInsertLog);
            if(TYPE(log, struct CsrInsertLog)->pX != 0x0){
                *size+=sizeof(BtreePayload);
                if(TYPE(log, struct CsrInsertLog)->pX->pKey == 0x0){
                    *size +=  TYPE(log, struct CsrInsertLog)->pX->nData;
                }else{
                    *size += TYPE(log, struct CsrInsertLog)->pX->nKey;
                    *size += TYPE(log, struct CsrInsertLog)->pX->nMem * sizeof(Mem);
                    for(i=0; i < TYPE(log, struct CsrInsertLog)->pX->nMem; i++){
                        if(!(TYPE(log, struct CsrInsertLog)->pX->aMem[i].flags & (MEM_Str | MEM_Blob)))
                            continue;
                        *size += TYPE(log, struct CsrInsertLog)->pX->aMem[i].n;
                        *size += TYPE(log, struct CsrInsertLog)->pX->aMem[i].szMalloc;
                    }
                }
            }
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct CsrInsertLog),sizeof(struct CsrInsertLog));
            if(TYPE(log, struct CsrInsertLog)->pX != 0x0){
                memcpy(tmp+=sizeof(struct CsrInsertLog), TYPE(log, struct CsrInsertLog)->pX,sizeof(BtreePayload));

                if(TYPE(log, struct CsrInsertLog)->pX->pKey == 0x0){
                    memcpy(tmp+=sizeof(BtreePayload), TYPE(log, struct CsrInsertLog)->pX->pData,
                            TYPE(log, struct CsrInsertLog)->pX->nData);
                    tmp+=TYPE(log, struct CsrInsertLog)->pX->nData;
                }else{
                    memcpy(tmp+=sizeof(BtreePayload), TYPE(log, struct CsrInsertLog)->pX->pKey,
                            TYPE(log, struct CsrInsertLog)->pX->nKey);
                    tmp+=TYPE(log, struct CsrInsertLog)->pX->nKey;
                }

                if((TYPE(log, struct CsrInsertLog)->pX->pKey != 0x0) && TYPE(log, struct CsrInsertLog)->pX->nMem){
                    memcpy(tmp, TYPE(log, struct CsrInsertLog)->pX->aMem, sizeof(Mem)*TYPE(log, struct CsrInsertLog)->pX->nMem);
                    tmp+=sizeof(Mem)*TYPE(log, struct CsrInsertLog)->pX->nMem;
                    for(i=0; i < TYPE(log, struct CsrInsertLog)->pX->nMem; i++){
                        if(!(TYPE(log, struct CsrInsertLog)->pX->aMem[i].flags & (MEM_Str | MEM_Blob)))
                            continue;
                        if(TYPE(log, struct CsrInsertLog)->pX->aMem[i].n > 0){
                            memcpy(tmp, TYPE(log, struct CsrInsertLog)->pX->aMem[i].z, 
                                    TYPE(log, struct CsrInsertLog)->pX->aMem[i].n);
                            tmp+=TYPE(log, struct CsrInsertLog)->pX->aMem[i].n;
                        }
                        if(TYPE(log, struct CsrInsertLog)->pX->aMem[i].szMalloc > 0){
                            memcpy(tmp, TYPE(log, struct CsrInsertLog)->pX->aMem[i].zMalloc, 
                                    TYPE(log, struct CsrInsertLog)->pX->aMem[i].szMalloc);
                            tmp+=TYPE(log, struct CsrInsertLog)->pX->aMem[i].szMalloc;
                        }
                    }
                }
            }
            return data;
        case csr_flag:
            *size = sizeof(struct CsrFlagLog);
            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct CsrFlagLog), sizeof(struct CsrFlagLog));
            return data;
        case csr_unpacked:
            *size = sizeof(struct CsrUnpackedLog);
            if(TYPE(log, struct CsrUnpackedLog)->pIdxKey != 0x0){
                *size += sizeof(struct UnpackedRecord);
                if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo != 0){
                    *size += (sizeof(KeyInfo) + TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->nField*sizeof(u8)
                            + TYPE(log, struct CsrUnpackedLog)->pIdxKey->nField*sizeof(Mem)); 

                    if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0] != 0x0){
                        *size += sqlite3Strlen30(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0]->zName);
                        TYPE(log, struct CsrUnpackedLog)->enc2 = TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0]->enc2;
                    }
                }

                for(i=0; i < TYPE(log, struct CsrUnpackedLog)->pIdxKey->nField; i++){
                    if(!(TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].flags & (MEM_Str | MEM_Blob)))
                        continue;
                    *size += TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].n;
                    *size += TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].szMalloc;
                }
            }

            tmp = data  = sqlite3Malloc(*size);
            memcpy(tmp, TYPE(log, struct CsrUnpackedLog), sizeof(struct CsrUnpackedLog));
            tmp+=sizeof(struct CsrUnpackedLog);

            if(TYPE(log, struct CsrUnpackedLog)->pIdxKey != 0x0){
                memcpy(tmp, TYPE(log, struct CsrUnpackedLog)->pIdxKey, sizeof(struct UnpackedRecord));
                tmp+=sizeof(struct UnpackedRecord);
                if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo != 0){
                    memcpy(tmp, TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo, 
                            sizeof(struct KeyInfo));
                    tmp+=sizeof(struct KeyInfo);
                    memcpy(tmp, TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aSortOrder,
                            TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->nField*sizeof(u8));
                    tmp += TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->nField*sizeof(u8);
                    if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0] != 0x0){
                        memcpy(tmp, TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0]->zName,
                                sqlite3Strlen30(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0]->zName));
                        tmp += sqlite3Strlen30(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0]->zName);
                    }
                }

                if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->nField){
                    memcpy(tmp, TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem, sizeof(Mem)*TYPE(log, struct CsrUnpackedLog)->pIdxKey->nField);
                    tmp+=sizeof(Mem)*TYPE(log, struct CsrUnpackedLog)->pIdxKey->nField;
                    for(i=0; i < TYPE(log, struct CsrUnpackedLog)->pIdxKey->nField; i++){
                        if(!(TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].flags & (MEM_Str | MEM_Blob)))
                            continue;
                        if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].n > 0){
                            memcpy(tmp, TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].z, 
                                    TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].n);
                            tmp+=TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].n;
                        }
                        if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].szMalloc > 0){
                            memcpy(tmp, TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].zMalloc, 
                                    TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].szMalloc);
                            tmp+=TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].szMalloc;
                        }
                    }
                }
            }
            return data;
    }
    return data;
};

void deserialize(void* log, enum opcode op, Logger *pLogger){
    int i;
    void *tmp = log;
    void *tmp2;
    op = op >> 4;
    switch(op){
        case bt_open:
            tmp+= sizeof(struct BtOpenLog);
            TYPE(log, struct BtOpenLog)->zFilename = tmp;
            return;
        case csr_open:
            if(TYPE(log, struct CsrOpenLog)->pKeyInfo != 0x0){
                tmp+= sizeof(struct CsrOpenLog);
                TYPE(log, struct CsrOpenLog)->pKeyInfo = tmp;
                tmp += sizeof(KeyInfo);
                TYPE(log, struct CsrOpenLog)->pKeyInfo->aSortOrder = tmp;
                tmp += sizeof(u8)*TYPE(log, struct CsrOpenLog)->pKeyInfo->nField;
                if(TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0] != 0x0){
                    TYPE(log, struct CsrOpenLog)->zName = tmp;
                    TYPE(log, struct CsrOpenLog)->pKeyInfo->aColl[0] = 
                        FIND_ICOL(TYPE(log, struct CsrOpenLog), pLogger);
                }
            }
            return;
        case csr_insert:
            tmp += sizeof(struct CsrInsertLog);
            TYPE(log, struct CsrInsertLog)->pX = tmp;
            tmp += sizeof(BtreePayload);
            if(TYPE(log, struct CsrInsertLog)->pX->pKey == 0x0){
                WRITE(TYPE(log, struct CsrInsertLog)->pX->pData,tmp, tmp2);
                tmp +=TYPE(log, struct CsrInsertLog)->pX->nData;
            }else{
                WRITE(TYPE(log, struct CsrInsertLog)->pX->pKey, tmp, tmp2);
                tmp +=TYPE(log, struct CsrInsertLog)->pX->nKey;
            }
            WRITE(TYPE(log, struct CsrInsertLog)->pX->aMem, tmp, tmp2);
            tmp += sizeof(Mem)*TYPE(log, struct CsrInsertLog)->pX->nMem;
            for(i=0; i < TYPE(log, struct CsrInsertLog)->pX->nMem; i++){
                if(!(TYPE(log, struct CsrInsertLog)->pX->aMem[i].flags & (MEM_Str | MEM_Blob)))
                    continue;
                if(TYPE(log, struct CsrInsertLog)->pX->aMem[i].n > 0){
                    TYPE(log, struct CsrInsertLog)->pX->aMem[i].z = tmp;
                    tmp+=TYPE(log, struct CsrInsertLog)->pX->aMem[i].n;
                }
                if(TYPE(log, struct CsrInsertLog)->pX->aMem[i].szMalloc > 0){
                    TYPE(log, struct CsrInsertLog)->pX->aMem[i].z = tmp;
                    tmp+=TYPE(log, struct CsrInsertLog)->pX->aMem[i].szMalloc;
                }
            }
            return;
        case csr_unpacked:
            tmp+=sizeof(struct CsrUnpackedLog);
            TYPE(log, struct CsrUnpackedLog)->pIdxKey = tmp;
            tmp += sizeof(struct CsrUnpackedLog);
            if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo != 0x0){
                TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo = tmp;
                tmp+=sizeof(KeyInfo);
                TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aSortOrder = tmp;
                tmp += TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->nField * sizeof(u8);
                TYPE(log, struct CsrUnpackedLog)->zName = tmp;
                if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0] != 0x0){
                    TYPE(log, struct CsrUnpackedLog)->pIdxKey->pKeyInfo->aColl[0] = 
                        FIND_ICOL(TYPE(log, struct CsrUnpackedLog), pLogger);
                    tmp += sqlite3Strlen30(TYPE(log, struct CsrUnpackedLog)->zName);
                }
            }
            TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem = tmp;
            tmp+=sizeof(Mem)*TYPE(log, struct CsrUnpackedLog)->pIdxKey->nField;
            for(i=0; i < TYPE(log, struct CsrUnpackedLog)->pIdxKey->nField; i++){
                if(!(TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].flags & (MEM_Str | MEM_Blob)))
                    continue;
                if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].n > 0){
                    TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].z = tmp;
                    tmp+=TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].n;
                }
                if(TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].szMalloc > 0){
                    TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].z = tmp;
                    tmp+=TYPE(log, struct CsrUnpackedLog)->pIdxKey->aMem[i].szMalloc;
                }
            }
            return;
        defualt:
            return;
    }
};

/* BtCursor logging
 * */

void inline sqlite3LogCursorOpen(int wrFlag, BtCursor *pCur, Btree *pBtree){
    if(wrFlag == 0)
        return;
    if(pCur->pKeyInfo != 0x0){
        pCur->al.csr_open_log = (struct CsrOpenLog){.iCsr = pCur->idx_aries, .iDb = pCur->pBtree->idx_aries, 
            .wrFlag = wrFlag, .pKeyInfo = pCur->pKeyInfo, .iBt = pBtree->idx_aries, .iTable = pCur->pgnoRoot};
    }else{
        pCur->al.csr_open_log = (struct CsrOpenLog){.iCsr = pCur->idx_aries, .iDb = pCur->pBtree->idx_aries, .wrFlag = wrFlag, .iBt = pBtree->idx_aries, .iTable = pCur->pgnoRoot};
    }
    sqlite3Log(pCur->pBtree, &pCur->al.csr_open_log, CSR_OPEN); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorClose(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, CSR_CLOSE); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorNext(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, NEXT); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorPrev(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, PREV); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorEof(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, CSR_EOF); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorFirst(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, FIRST); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorLast(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, LAST); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorIntegerKey(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, INTEGERKEY); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorUnpacked(BtCursor *pCur, UnpackedRecord *pIdxKey, int intKey, int biasRight){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_unpacked_log = (struct CsrUnpackedLog){.iCsr = pCur->idx_aries, .pIdxKey = pIdxKey, 
        .intKey = intKey, .biasRight = biasRight};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_unpacked_log, MV_UNPACKED); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorClear(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, CSR_CLEAR); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorRestore(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, RESTORE); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorIncrBlob(BtCursor *pCur){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_icsr_log = (struct CsrIcsrLog){.iCsr = pCur->idx_aries};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_icsr_log, INCRBLOB); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorInsert(BtCursor *pCur, const BtreePayload *pX, int appendBias, int seekResult){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_insert_log = (struct CsrInsertLog){.iCsr = pCur->idx_aries, .pX = pX, .appendBias = appendBias, .seekResult = seekResult};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_insert_log, INSERT); 
    memset(&pCur->al,0,sizeof(pCur->al));
};

void inline sqlite3LogCursorDelete(BtCursor *pCur, u8 flags){
    if(pCur->curFlags != BTCF_WriteFlag)
        return;

    pCur->al.csr_flag_log = (struct CsrFlagLog){.iCsr = pCur->idx_aries, .flags = flags};
    sqlite3Log(pCur->pBtree, &pCur->al.csr_flag_log, DELETE); 
    memset(&pCur->al,0,sizeof(pCur->al));
};


/* Btree logging
 * */

void inline sqlite3LogBtreeOpen(Btree *pBtree, const char* zFilename, int flags, int vfsFlags){
    pBtree->al.bt_open_log = (struct BtOpenLog){.iBt = pBtree->idx_aries, .zFilename = zFilename, .flags = flags, .vfsFlags = vfsFlags};
    sqlite3Log(pBtree, &pBtree->al.bt_open_log, BTREE_OPEN); 
    memset(&pBtree->al,0,sizeof(pBtree->al));
};

void inline sqlite3LogBtreeClose(Btree *pBtree){
    pBtree->al.bt_iBt_log = (struct BtIbtLog){.iBt = pBtree->idx_aries};
    sqlite3Log(pBtree, &pBtree->al.bt_iBt_log, BTREE_CLOSE); 
    memset(&pBtree->al,0,sizeof(pBtree->al));
};

void inline sqlite3LogBtreeCreate(Btree *pBtree, int flags){
    pBtree->al.bt_flag_log = (struct BtFlagLog){.iBt = pBtree->idx_aries, .flags = flags};
    sqlite3Log(pBtree, &pBtree->al.bt_flag_log, CREATE); 
    memset(&pBtree->al,0,sizeof(pBtree->al));
};

void inline sqlite3LogBtreeDrop(Btree *pBtree, int iTable){
    pBtree->al.bt_flag_log = (struct BtFlagLog){.iBt = pBtree->idx_aries, .flags = iTable};
    sqlite3Log(pBtree, &pBtree->al.bt_flag_log, DROP); 
    memset(&pBtree->al,0,sizeof(pBtree->al));
};

void inline sqlite3LogBtreeClear(Btree *pBtree, int iTable){
    pBtree->al.bt_flag_log = (struct BtFlagLog){.iBt = pBtree->idx_aries, .flags = iTable};
    sqlite3Log(pBtree, &pBtree->al.bt_flag_log, BTREE_CLEAR); 
    memset(&pBtree->al,0,sizeof(pBtree->al));
};

void inline sqlite3LogBtreeBeginTrans(Btree *pBtree, int wrFlag){
    pBtree->al.bt_flag_log = (struct BtFlagLog){.iBt = pBtree->idx_aries, .flags = wrFlag};
    sqlite3Log(pBtree, &pBtree->al.bt_flag_log, BEGINTRANS); 
    memset(&pBtree->al,0,sizeof(pBtree->al));
};

void inline sqlite3LogBtreeSavepoint(Btree *pBtree, int op, int iSavepoint){
    pBtree->al.bt_save_log = (struct BtSaveLog){.iBt = pBtree->idx_aries, .op = op, .iSavepoint = iSavepoint};
    sqlite3Log(pBtree, &pBtree->al.bt_save_log, SAVEPOINT); 
    memset(&pBtree->al,0,sizeof(pBtree->al));
};

/* Commit and Rollback should be redesigned
 * */

void free_qLogCell(qLogCell* m_qLogCell){
    int rc;
    sqlite3_free((void*)m_qLogCell->m_logCell->data);
    sqlite3_free(m_qLogCell);
};


int sqlite3LogForceAtCommit(Btree *pBtree){
    Logger *pLogger = pBtree->pBt->pLogger;
    if(!pLogger || pLogger->state == OFF || pLogger->state == START)
        return 0;
    pBtree->al.bt_iBt_log = (struct BtIbtLog){.iBt = pBtree->idx_aries};
    sqlite3Log(pBtree,&pBtree->al.bt_iBt_log,COMMIT);
};

int sqlite3LogRollback(Logger *pLogger){
    if(!pLogger || pLogger->state == OFF || pLogger->state == START)
        return 0;

    struct list_head *tmp1, *tmp2;
    qLogCell *m_qLogCell;
    list_for_each_safe(tmp1,tmp2,&pLogger->q){
        m_qLogCell = list_entry(tmp1, qLogCell, q);
        del_from_logqueue(m_qLogCell);
        free_qLogCell(m_qLogCell);
    }
};

int sqlite3LogRollbackTop(Logger *pLogger){
    if(!pLogger || pLogger->state == OFF || pLogger->state == START)
        return 0;
    struct list_head *tmp1, *tmp2;
    qLogCell *m_qLogCell;
    m_qLogCell = list_top( &pLogger->q, qLogCell, q);
    del_from_logqueue(m_qLogCell);
    free_qLogCell(m_qLogCell);
};

/* apCol fetching 
 * */

void sqlite3Log(Btree * pBtree, void *log, enum opcode op){
    static int vers = 0;
    Logger *pLogger = pBtree->pBt->pLogger;
    void *pPtr;
    logCell *m_logCell;
    qLogCell *m_qLogCell;
    struct list_head *tmp1, *tmp2;

    if(!pLogger || pLogger->state == OFF || pLogger->state == START){
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
                pLogger->log_buffer = mremap(pLogger->log_buffer,LOG_SIZE*(pLogger->hdr.vers - 1),
                        LOG_SIZE*pLogger->hdr.vers, MREMAP_MAYMOVE);
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

void sqlite3LogFileInit(Logger *pLogger){
    close(pLogger->log_fd);
    remove(pLogger->zFile);
    pLogger->log_fd = 0;
    sqlite3LoggerOpenPhaseTwo(0,pLogger);
}

void sqlite3LogRecovery(Logger *pLogger, logCell *m_logCell){

    struct BtIbtLog *bt_iBt_log;
    struct BtOpenLog *bt_open_log;
    struct BtFlagLog *bt_flag_log;
    struct BtSaveLog *bt_save_log;

    struct CsrOpenLog *csr_open_log;
    struct CsrIcsrLog *csr_icsr_log;
    struct CsrUnpackedLog *csr_unpacked_log;
    struct CsrInsertLog *csr_insert_log;
    struct CsrFlagLog *csr_flag_log;

    int iBt,iCsr;
    int tmp;
    deserialize(m_logCell->data, m_logCell->op, pLogger);

    switch(m_logCell->op){
        /* Btree recovery
         * */
        case BTREE_OPEN:
            bt_open_log = m_logCell->data;
            /*
            sqlite3BtreeOpen(pLogger->pVfs, bt_open_log->zFilename, pLogger->db, &FIND_IBT(bt_open_log, pLogger),
                    bt_open_log->flags, bt_open_log->vfsFlags);
                    */
            break;
        case BTREE_CLOSE:
            bt_iBt_log = m_logCell->data;
            sqlite3BtreeClose(FIND_IBT(bt_iBt_log, pLogger));
            RELEASE_IBT(bt_iBt_log, pLogger);
            break;
        case CREATE:
            bt_flag_log = m_logCell->data;
            sqlite3BtreeCreateTable(FIND_IBT(bt_flag_log, pLogger), &tmp, bt_flag_log->flags);
            break;
        case DROP:
            bt_flag_log = m_logCell->data;
            sqlite3BtreeDropTable(FIND_IBT(bt_flag_log, pLogger), bt_flag_log->flags, &tmp);
            break;
        case BTREE_CLEAR:
            bt_flag_log = m_logCell->data;
            sqlite3BtreeClearTable(FIND_IBT(bt_flag_log, pLogger), bt_flag_log->flags, &tmp);
            break;
        case BEGINTRANS:
            bt_flag_log = m_logCell->data;
            sqlite3BtreeBeginTrans(FIND_IBT(bt_flag_log, pLogger), bt_flag_log->flags);
            break;
        case SAVEPOINT:
            bt_save_log = m_logCell->data;
            sqlite3BtreeSavepoint(FIND_IBT(bt_flag_log, pLogger), bt_save_log->op, bt_save_log->iSavepoint);
            break;
        case COMMIT:
            break;
        /* BtCursor recovery
         * */
        case CSR_OPEN:
            csr_open_log = m_logCell->data;
            FIND_ICSR(csr_open_log, pLogger) = (BtCursor*)sqlite3MallocZero(sizeof(BtCursor));
            sqlite3BtreeCursor(FIND_IBT(csr_open_log, pLogger), csr_open_log->iTable, csr_open_log->wrFlag, 
                    csr_open_log->pKeyInfo, FIND_ICSR(csr_open_log, pLogger));
            break;
        case CSR_CLOSE:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreeCloseCursor(FIND_ICSR(csr_icsr_log, pLogger));
            RELEASE_ICSR(csr_icsr_log, pLogger);
            break;
        case NEXT:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreeNext(FIND_ICSR(csr_icsr_log, pLogger), &tmp);
            break;
        case PREV:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreePrevious(FIND_ICSR(csr_icsr_log, pLogger), &tmp);
            break;
        case FIRST:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreeFirst(FIND_ICSR(csr_icsr_log, pLogger), &tmp);
            break;
        case LAST:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreeLast(FIND_ICSR(csr_icsr_log, pLogger), &tmp);
            break;
        case CSR_EOF:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreeEof(FIND_ICSR(csr_icsr_log, pLogger));
            break;
        case INTEGERKEY:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreeIntegerKey(FIND_ICSR(csr_icsr_log, pLogger));
            break;
        case MV_UNPACKED:
            csr_unpacked_log = m_logCell->data;
            sqlite3BtreeMovetoUnpacked(FIND_ICSR(csr_icsr_log, pLogger), csr_unpacked_log->pIdxKey, 
                    csr_unpacked_log->intKey, csr_unpacked_log->biasRight, &tmp);
            break;
        case CSR_CLEAR:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreeClearCursor(FIND_ICSR(csr_icsr_log, pLogger));
            break;
        case RESTORE:
            csr_icsr_log = m_logCell->data;
            sqlite3BtreeCursorRestore(FIND_ICSR(csr_icsr_log, pLogger), &tmp);
            break;
        case INCRBLOB:
            csr_icsr_log = m_logCell->data;
            #ifndef SQLITE_OMIT_INCRBLOB
            sqlite3BtreeIncrblobCursor(FIND_ICSR(csr_icsr_log, pLogger));
            #endif
            break;
        case INSERT:
            csr_insert_log = m_logCell->data;
            sqlite3BtreeInsert(FIND_ICSR(csr_insert_log, pLogger), csr_insert_log->pX, 
                    csr_insert_log->appendBias, csr_insert_log->seekResult);
            break;
        case DELETE:
            csr_flag_log = m_logCell->data;
            sqlite3BtreeDelete(FIND_ICSR(csr_flag_log, pLogger), csr_flag_log->flags);
            break;
    };
};

int sqlite3LogAnalysis(Logger *pLogger){
    logCell m_logCell;
    void *tmp;
    int offset =  pLogger->hdr.stLsn + sizeof(logHdr);    
    Btree* p = pLogger->pBtree;

    pLogger->state = OFF;
	pLogger->p_check = LOG_LIMIT;

    while(1){
        memcpy(&m_logCell,pLogger->log_buffer + offset,sizeof(logCell));
        if(m_logCell.op == EXIT){
            break;
        }
        offset+=sizeof(logCell);
        m_logCell.data = pLogger->log_buffer + offset;
        offset+=m_logCell.data_size;
        sqlite3LogRecovery(pLogger,&m_logCell);
    }

    sqlite3LogFileInit(pLogger);

	pLogger->p_check = 0;
    pLogger->sync = 1;
    pLogger->state = ON;

    return SQLITE_OK;
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
    pLogger->sync = 0;
};


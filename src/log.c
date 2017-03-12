#include "log.h"
#include <stdio.h>
#include "btree.h"
int sqlite3LoggerOpen(sqlite3_vfs *pVfs,const char* zFilename,Logger **ppLogger){
	Logger *pLogger;
	int fileLen, fullPathLen, logNameLen, m_log_fd;
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
	
	m_log_fd = open(zLogname, O_RDWR | O_CREAT, 0644);
	if(m_log_fd < 0){
		fprintf(stderr, "LOG FILE OPEN ERROR\n");
	}else{
		ftruncate(m_log_fd,1024*4096);
		pLogger->log_fd = m_log_fd;
	}

	m_log_buffer = (void*) mmap(NULL, 1024*4096, PROT_READ | PROT_WRITE, MAP_SHARED, m_log_fd,0);
	if(m_log_buffer == MAP_FAILED){
		fprintf(stderr, "LOG FILE MAPPING ERROR\n");
	}
	pLogger->log_buffer = m_log_buffer;
	pLogger->p_check = 0;
	pLogger->apLogCell = sqlite3MallocZero(sizeof(qLogCell));
	*ppLogger = pLogger;
	return SQLITE_OK;
}

void* serializeLog(int logType, void* logData, int* size);
void deserializeLog(int logType, void* data, void **dest);

void* serializeLog(int logType, void* logData, int* size){
	int m_size = 0, i;
	void* m_serialized;
	if(logType == 1){
		printf("logType %d\n",logType);
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
		//puts("test data");
	}else if(logType == 0){
		physicalLog* m_log = logData;
		*size = sizeof(int)*4 + sizeof(Pgno) + m_log->cellSize;
		m_serialized = sqlite3Malloc(*size);
		memcpy(m_serialized, &(m_log->idx), sizeof(int));
		memcpy(m_serialized + sizeof(int), &(m_log->cellSize), sizeof(int));
		memcpy(m_serialized + sizeof(int)*2, &(m_log->pgno), sizeof(Pgno));
		memcpy(m_serialized + sizeof(int)*2 + sizeof(Pgno), &(m_log->iTable), sizeof(int));
		memcpy(m_serialized + sizeof(int)*3, &(m_log->curFlags), sizeof(int));
		memcpy(m_serialized + sizeof(int)*4 + sizeof(Pgno), m_log->newCell, sizeof(char)*m_log->cellSize);
		//puts("test data");
	}
	//printf("serializeLog data_size %d logType  %d\n",*size, logType);
	return m_serialized;
}

void deserializeLog(int logType, void *data, void **dest){
	int i=0;
	printf("logType %d\n",logType);
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
		m_pLog->newCell = data + sizeof(int)*4 + sizeof(Pgno);
		*dest = m_pLog;
		for(i =0 ; i < m_pLog->cellSize; i++){
			//printf("%d ", m_pLog->newCell[i]);
		}
	}
	//puts("");
}

void sqlite3Log(Logger *pLogger ,logCell *pLogCell){
	int i;
	char *tmp1, *tmp2;
	void *m_log_buffer = pLogger->log_buffer + pLogger->lastLsn;
	char *m_logCell = sqlite3Malloc(sizeof(int) * 3 + pLogCell->data_size);
	pLogger->p_check += 1;
	pLogger->lastLsn += (sizeof(int) * 3 + pLogCell->data_size);
	//printf("lastLsn %d %lu\n",pLogger->lastLsn,sizeof(int) * 3 + pLogCell->data_size);
	pLogCell->lastLsn = pLogger->lastLsn;
	memcpy(m_logCell,&(pLogger->lastLsn),sizeof(int));
	memcpy(m_logCell+ sizeof(int),&(pLogCell->opcode), sizeof(int));
	memcpy(m_logCell+ sizeof(int)*2,&(pLogCell->data_size), sizeof(int));
	if(pLogCell->data_size > 0 && pLogCell->data != NULL)
		memcpy(m_logCell+ sizeof(int)*3,pLogCell->data, pLogCell->data_size);
	memcpy(m_log_buffer, m_logCell, sizeof(int) * 3 + pLogCell->data_size);
	sqlite3_free(m_logCell);
	/*
	if(pLogCell->opcode == 0){
		msync(pLogger->log_buffer + pLogger->syncedLsn, pLogger->lastLsn - pLogger->syncedLsn , MS_SYNC);
		pLogger->syncedLsn = pLogger->lastLsn;
	}
	*/
	//puts("");
	return;	
};

int parseLogBuffer(void* m_log_buffer, logCell **m_plogCell){
	*m_plogCell = sqlite3Malloc(sizeof(logCell));
	logCell* m_logCell = *m_plogCell;
	memcpy(&(m_logCell->lastLsn) ,m_log_buffer, sizeof(int));
	memcpy(&(m_logCell->opcode) ,m_log_buffer + sizeof(int), sizeof(int));
	memcpy(&(m_logCell->data_size) ,m_log_buffer + sizeof(int)*2, sizeof(int));
	m_logCell->data = sqlite3Malloc(m_logCell->data_size);
	memcpy(m_logCell->data ,m_log_buffer + sizeof(int)*3, m_logCell->data_size);
	//printf("m_logCell->lastLsn : %d\n",m_logCell->lastLsn);
	return m_logCell->lastLsn;
};

//implement queue pls:
int sqlite3LogAnalysis(Logger *pLogger){
	void* m_log_buffer = pLogger->log_buffer;
	logCell *m_logCell;
	qLogCell *tmpQueue,*nextTmp;
	int m_lastLsn, id = 0,start_id = 0 , i;
	while((m_lastLsn = parseLogBuffer(m_log_buffer, &m_logCell)) && (m_lastLsn != 0)){
		//printf("%d %d %d\n",m_logCell->lastLsn, m_logCell->opcode, m_logCell->data_size);
		if(m_logCell->opcode == -1){
			//flush queue
			tmpQueue = pLogger->apLogCell->next;
			while(tmpQueue != NULL &&tmpQueue != pLogger->apLogCell){
				nextTmp = tmpQueue->next;
				tmpQueue->next = tmpQueue->prev = NULL;
				sqlite3_free(tmpQueue);
				tmpQueue = nextTmp;
			}
			pLogger->apLogCell->next = pLogger->apLogCell->prev = NULL;
		}
		//insert into queue
		tmpQueue = sqlite3Malloc(sizeof(qLogCell));
		tmpQueue->m_logCell = m_logCell;
		nextTmp = pLogger->apLogCell->next;
		pLogger->apLogCell->next = tmpQueue;
		tmpQueue->prev = pLogger->apLogCell;
		if(nextTmp != NULL){
			tmpQueue->next = nextTmp;
			nextTmp->prev = tmpQueue;
		}else{
			tmpQueue->next = pLogger->apLogCell;
			pLogger->apLogCell->prev = tmpQueue;
		}
		m_log_buffer = pLogger->log_buffer + m_lastLsn;
	};
	return SQLITE_OK;
};

void sqlite3LogRecovery(Btree* pBtree, sqlite3* db){
	Logger *pLogger = pBtree->pBt->pLogger;
	logCell *mLogCell;
	BtCursor *btCsr = sqlite3Malloc(sizeof(BtCursor));
	logicalLog *m_lLog;
	physicalLog *m_pLog;
	qLogCell *tmpQueue,*prevTmp;
	CellInfo info;
	unsigned char* oldCell;
	char* newCell;
	MemPage* pPage;
	int rc,i;
	//while queue exists
	tmpQueue = pLogger->apLogCell->prev;
	while(tmpQueue != NULL && tmpQueue != pLogger->apLogCell){
		sqlite3BtreeCursorZero(btCsr);
		mLogCell = tmpQueue->m_logCell;
		printf("opcode : %d lastLsn :%d \n",mLogCell->opcode,mLogCell->lastLsn);
		switch(mLogCell->opcode){
			case 1:
				puts("insert & update logical recovery start");
				deserializeLog(1, mLogCell->data, (void**)&m_lLog);
				BtreePayload x;
				sqlite3BtreeCursor(pBtree, m_lLog->iTable, 4, 0x0, btCsr);
				while( pBtree->pBt->pPage1==0 && SQLITE_OK==(rc = lockBtree(pBtree->pBt)) );
				rc = sqlite3BtreeLast(btCsr,&rc);
				printf("result :%d\n",rc);
				x.nKey = m_lLog->nKey;
				x.nData = m_lLog->nData;
				x.nZero = m_lLog->nZero;
				x.nMem = m_lLog->nMem;
				x.aMem = NULL;
				x.pData = m_lLog->pData;
				x.pKey = NULL;
				btCsr->curFlags = m_lLog->curFlags;
				btCsr->curIntKey = 1;
				sqlite3BtreeBeginTrans(pBtree,4);
				sqlite3BtreeInsert(btCsr,&x,1,m_lLog->seekResult);
				sqlite3BtreeCloseCursor(btCsr);
				break;
			case 2:
				puts("insert with physical recovery start");
				deserializeLog(0, mLogCell->data, (void**)&m_pLog);
				rc = sqlite3BtreeCursor(pBtree, m_pLog->iTable, 4, 0x0, btCsr);
				//printf("btree cursor result %d\n",rc);
				while( pBtree->pBt->pPage1==0 && SQLITE_OK==(rc = lockBtree(pBtree->pBt)) );
				rc = sqlite3BtreeLast(btCsr,&rc);
				//printf("btree get last result %d\n",rc);
				newCell = pBtree->pBt->pTmpSpace;
				memcpy(newCell, m_pLog->newCell, m_pLog->cellSize);
				pPage = 0;
				rc = btreeGetPage(pBtree->pBt, m_pLog->pgno, &(pPage), 2);
				//printf("btree get page result %d\n",rc);

				insertCell(pPage, m_pLog->idx, newCell, m_pLog->cellSize, 0, 0, &rc);
				//printf("result of insertCell %d\n",rc);
				pPage->pDbPage->pPager->eState = PAGER_WRITER_FINISHED; 
				sqlite3PcacheMakeDirty(pPage->pDbPage);

				rc = sqlite3BtreeCloseCursor(btCsr);
				//printf("btree close cursor result %d\n",rc);
				break;
			case 3:
				/*
				puts("update with physical Log");
				deserializeLog(0, mLogCell->data, (void**)&m_pLog);
				rc = sqlite3BtreeCursor(pBtree, m_pLog->iTable, 4, 0x0, btCsr);
				btCsr->curIntKey = 3;
				while( pBtree->pBt->pPage1==0 && SQLITE_OK==(rc = lockBtree(pBtree->pBt)) );
				newCell = pBtree->pBt->pTmpSpace;
				memcpy(newCell, m_pLog->newCell, m_pLog->cellSize);
				MemPage* pPage = 0;
				rc = btreeGetPage(pBtree->pBt, m_pLog->pgno, &(pPage), 2);
				if(m_pLog->curFlags == 3){
					//findCell
					oldCell = findCell(pPage, m_pLog->idx);
					//clearCell, &info recovery
					rc = clearCell(pPage, oldCell, &info);
					//dropCell
					dropCell(pPage, m_pLog->idx, info.nSize, &rc);
				}
				insertCell(pPage, m_pLog->idx, newCell, m_pLog->cellSize, 0, 0, &rc);
				pPage->pDbPage->pPager->eState = PAGER_WRITER_FINISHED; 
				sqlite3PcacheMakeDirty(pPage->pDbPage);

				rc = sqlite3BtreeCloseCursor(btCsr);
				*/
				break;
		}
		tmpQueue = tmpQueue->prev;
	}
	tmpQueue = pLogger->apLogCell->prev;
	while(tmpQueue != NULL && tmpQueue != pLogger->apLogCell){
		prevTmp = tmpQueue->prev;
		tmpQueue->next = tmpQueue->next->prev = NULL;
		printf("tmpQueue : %p\n",tmpQueue);
		sqlite3_free(tmpQueue);
		tmpQueue = prevTmp;
	}
	char *zer;
	sqlite3_free(btCsr);
	pLogger->apLogCell->next = NULL;
	pLogger->p_check = LOG_LIMIT;
	pBtree->inTrans = TRANS_WRITE;
	pBtree->pBt->inTransaction = TRANS_WRITE;
	sqlite3BtreeCommit(pBtree);
	memset(pLogger->log_buffer, 0x00, 1024*4096);                                                                                                                                                   
	msync(pLogger->log_buffer, 1024*4096, MS_SYNC);   
};

int sqlite3LogForceAtCommit(Logger *pLogger){
	logCell m_logCell;
	m_logCell.opcode = 0;
	m_logCell.data_size = 0;
	m_logCell.data = NULL;
	if(pLogger->p_check != LOG_LIMIT)
		sqlite3Log(pLogger,&m_logCell);
	pLogger->p_check = 0;
};

int sqlite3LogCheckPoint(Logger *pLogger){
	logCell m_logCell;
	m_logCell.opcode = -1;
	m_logCell.data_size = 0;
	m_logCell.data = NULL;
	if(pLogger->p_check != LOG_LIMIT)
		sqlite3Log(pLogger,&m_logCell);
	pLogger->p_check = 0;
};

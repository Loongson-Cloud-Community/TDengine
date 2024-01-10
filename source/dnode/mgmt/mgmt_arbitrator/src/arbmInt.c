/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "arbmInt.h"
#include "libs/function/tudf.h"

static int32_t arbmRequire(const SMgmtInputOpt *pInput, bool *required) {
  *required = true;
  return 0;
}

SArbitratorObj *arbmAcquireArbitratorImpl(SArbitratorMgmt *pMgmt, int32_t arbId, bool strict) {
  SArbitratorObj *pArbObj = NULL;

  taosThreadRwlockRdlock(&pMgmt->lock);
  taosHashGetDup(pMgmt->hash, &arbId, sizeof(int32_t), (void *)&pArbObj);
  if (pArbObj == NULL || strict && (pArbObj->dropped || pArbObj->failed)) {
    terrno = TSDB_CODE_ARB_INVALID_ARB_ID;
    pArbObj = NULL;
  } else {
    int32_t refCount = atomic_add_fetch_32(&pArbObj->refCount, 1);
    // dTrace("arbId:%d, acquire arbitrator, ref:%d", pArbObj->arbId, refCount);
  }
  taosThreadRwlockUnlock(&pMgmt->lock);

  return pArbObj;
}

SArbitratorObj *arbmAcquireArbitrator(SArbitratorMgmt *pMgmt, int32_t arbId) {
  return arbmAcquireArbitratorImpl(pMgmt, arbId, true);
}

void arbmReleaseArbitrator(SArbitratorMgmt *pMgmt, SArbitratorObj *pArbObj) {
  if (pArbObj == NULL) return;

  taosThreadRwlockRdlock(&pMgmt->lock);
  int32_t refCount = atomic_sub_fetch_32(&pArbObj->refCount, 1);
  // dTrace("arbId:%d, release arbitrator, ref:%d", pArbObj->arbId, refCount);
  taosThreadRwlockUnlock(&pMgmt->lock);
}

static void arbmCleanup(SArbitratorMgmt *pMgmt) {
  arbmStopWorker(pMgmt);
  taosMemoryFree(pMgmt);
}

static void arbmFreeArbitratorObj(SArbitratorObj **ppArbObj) {
  if (!ppArbObj || !(*ppArbObj)) return;

  SArbitratorObj *pArbObj = *ppArbObj;
  taosMemoryFree(pArbObj->path);
  taosMemoryFree(pArbObj);
  ppArbObj[0] = NULL;
}

static void arbmGetDnodeEp(SDnodeData *pData, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort){

}

int32_t arbmOpenArbitrator(SArbitratorMgmt *pMgmt, SArbWrapperCfg *pCfg, SArbitrator *pImpl) {
  SArbitratorObj *pArbObj = taosMemoryCalloc(1, sizeof(SArbitratorObj));
  if (pArbObj == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pArbObj->arbId = pCfg->arbId;
  pArbObj->refCount = 0;
  pArbObj->dropped = 0;
  pArbObj->failed = 0;
  pArbObj->path = taosStrdup(pCfg->path);
  pArbObj->pImpl = pImpl;

  if (pArbObj->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pArbObj);
    return -1;
  }

  if (pImpl) {
    if (arbObjStartWorker(pArbObj) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(pArbObj->path);
      taosMemoryFree(pArbObj);
      return -1;
    }
  } else {
    pArbObj->failed = 1;
  }

  taosThreadRwlockWrlock(&pMgmt->lock);
  SArbitratorObj *pOld = NULL;
  taosHashGetDup(pMgmt->hash, &pArbObj->arbId, sizeof(int32_t), (void *)&pOld);
  if (pOld) {
    ASSERT(pOld->failed);
    arbmFreeArbitratorObj(&pOld);
  }
  int32_t code = taosHashPut(pMgmt->hash, &pArbObj->arbId, sizeof(int32_t), &pArbObj, sizeof(SArbitratorObj *));
  taosThreadRwlockUnlock(&pMgmt->lock);

  return code;
}

void arbmCloseArbitrator(SArbitratorMgmt *pMgmt, SArbitratorObj *pArbObj) {
  char path[TSDB_FILENAME_LEN] = {0};

  taosThreadRwlockWrlock(&pMgmt->lock);
  taosHashRemove(pMgmt->hash, &pArbObj->arbId, sizeof(int32_t));
  taosThreadRwlockUnlock(&pMgmt->lock);
  arbmReleaseArbitrator(pMgmt, pArbObj);

  if (pArbObj->failed) {
    ASSERT(pArbObj->pImpl == NULL);
    goto _closed;
  }

  dInfo("arbId:%d, wait for arbitrator ref become 0", pArbObj->arbId);
  while (pArbObj->refCount > 0) taosMsleep(10);

  dInfo("arbId:%d, wait for arbitrator write queue:%p is empty, thread:%08" PRId64, pArbObj->arbId,
        pArbObj->worker.queue, pArbObj->worker.queue->threadId);
  arbObjStopWorker(pArbObj);

  dInfo("arbId:%d, all arbitrator queues is empty", pArbObj->arbId);

  arbitratorClose(pArbObj->pImpl);
  pArbObj->pImpl = NULL;

_closed:
  dInfo("arbId:%d, arbitrator is closed", pArbObj->arbId);

  if (pArbObj->dropped) {
    dInfo("arbId:%d, arbitrator is destroyed, dropped:%d", pArbObj->arbId, pArbObj->dropped);
    snprintf(path, TSDB_FILENAME_LEN, "arbitrator%sarbitrator%d", TD_DIRSEP, pArbObj->arbId);
    arbitratorDestroy(path);
  }

  arbmFreeArbitratorObj(&pArbObj);
}

static void *arbmOpenArbitratorInThread(void *param) {
  SArbitratorThread *pThread = param;
  SArbitratorMgmt   *pMgmt = pThread->pMgmt;
  char               path[TSDB_FILENAME_LEN];

  dInfo("thread:%d, start to open arbitrator", pThread->threadIndex);
  setThreadName("open-arbitrators");

  SArbWrapperCfg *pCfg = pThread->pCfg;

  char stepDesc[TSDB_STEP_DESC_LEN] = {0};
  snprintf(stepDesc, TSDB_STEP_DESC_LEN, "arbId:%d, start to restore, %d of %d have been opened", pCfg->arbId,
           pMgmt->state.openArbitrators, pMgmt->state.totalArbitrators);
  tmsgReportStartup("arbit-open", stepDesc);

  snprintf(path, TSDB_FILENAME_LEN, "%s%sarbitrator%d", pMgmt->path, TD_DIRSEP, pCfg->arbId);

  SArbitrator *pImpl = arbitratorOpen(path, pMgmt->msgCb);
  if (pImpl == NULL) {
    dError("arbId:%d, failed to open arbitrator by thread:%d since %s", pCfg->arbId, pThread->threadIndex, terrstr());
    if (terrno != TSDB_CODE_NEED_RETRY) {
      pThread->failed++;
      goto _err_out;
    }
  }

  if (arbmOpenArbitrator(pMgmt, pCfg, pImpl) != 0) {
    dError("arbId:%d, failed to open arbitrator by thread:%d", pCfg->arbId, pThread->threadIndex);
    pThread->failed++;
    goto _err_out;
  }

  dInfo("arbId:%d, is opened by thread:%d", pCfg->arbId, pThread->threadIndex);
  pThread->opened++;
  atomic_add_fetch_32(&pMgmt->state.openArbitrators, 1);

_err_out:
  dInfo("thread:%d, opened:%d failed:%d", pThread->threadIndex, pThread->opened, pThread->failed);
  return NULL;
}

static int32_t arbmOpenArbitrators(SArbitratorMgmt *pMgmt) {
  pMgmt->hash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pMgmt->hash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    dError("failed to init arbitrator hash since %s", terrstr());
    return -1;
  }

  SArbWrapperCfg *pCfgs = NULL;
  int32_t         numOfArbitrators = 0;
  if (arbmGetArbitratorListFromFile(pMgmt, &pCfgs, &numOfArbitrators) != 0) {
    dInfo("failed to get arbitrator list from disk since %s", terrstr());
    return -1;
  }

  pMgmt->state.totalArbitrators = numOfArbitrators;

  SArbitratorThread *threads = taosMemoryCalloc(numOfArbitrators, sizeof(SArbitratorThread));
  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].pCfg = taosMemoryCalloc(1, sizeof(SArbWrapperCfg));
    memcpy(threads[t].pCfg, &pCfgs[t], sizeof(SArbWrapperCfg));
  }

  dInfo("open %d arbitrators", numOfArbitrators);

  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    SArbitratorThread *pThread = &threads[t];

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&pThread->thread, &thAttr, arbmOpenArbitratorInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to open arbitrator, reason:%s", pThread->threadIndex, strerror(errno));
    }

    taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    SArbitratorThread *pThread = &threads[t];
    if (taosCheckPthreadValid(pThread->thread)) {
      taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->pCfg);
  }
  taosMemoryFree(threads);
  taosMemoryFree(pCfgs);

  if (pMgmt->state.openArbitrators != pMgmt->state.totalArbitrators) {
    dError("there are total arbitrators:%d, opened:%d", pMgmt->state.totalArbitrators, pMgmt->state.openArbitrators);
    terrno = TSDB_CODE_ARB_INIT_FAILED;
    return -1;
  }

  if (arbmWriteArbitratorListToFile(pMgmt) != 0) {
    dError("failed to write arbitrator list since %s", terrstr());
    return -1;
  }

  dInfo("successfully opened %d arbitrators", pMgmt->state.totalArbitrators);
  return 0;
}

static void *arbmThreadFp(void *param) {
  SArbitratorMgmt *pMgmt = param;
  int64_t          lastTime = 0;
  setThreadName("arb-timer");

  while (1) {
    lastTime++;
    taosMsleep(100);
    if (pMgmt->stop) break;
    if (lastTime % 10 != 0) continue;

    int64_t sec = lastTime / 10;
    if (sec % (ARBITRATOR_TIMEOUT_SEC / 2) == 0) {
      arbmPullupArbHeartbeat(pMgmt);
    }
  }

  return NULL;
}

static int32_t arbmInitTimer(SArbitratorMgmt *pMgmt) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->thread, &thAttr, arbmThreadFp, pMgmt) != 0) {
    dError("failed to create arbitrator timer thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  return 0;
}

static void arbmCleanupTimer(SArbitratorMgmt *pMgmt) {
  pMgmt->stop = true;
  if (taosCheckPthreadValid(pMgmt->thread)) {
    taosThreadJoin(pMgmt->thread, NULL);
    taosThreadClear(&pMgmt->thread);
  }
}

static int32_t arbmInit(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t code = -1;

  SArbitratorMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SArbitratorMgmt));
  if (pMgmt == NULL) goto _OVER;

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)arbmPutRpcMsgToQueue;
  pMgmt->msgCb.qsizeFp = (GetQueueSizeFp)arbmGetQueueSize;
  pMgmt->msgCb.mgmt = pMgmt;

  if (arbmStartWorker(pMgmt) != 0) {
    dError("failed to start arb-mgmt worker since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("arb-mgmt", "initialized");

  if (arbmOpenArbitrators(pMgmt) != 0) {
    dError("failed to open all arbitrators since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("arb-arbitrators", "initialized");

  code = 0;

_OVER:
  pOutput->pMgmt = pMgmt;
  if (code == 0) {
    pOutput->pMgmt = pMgmt;
  } else {
    dError("failed to init arb-mgmt since %s", terrstr());
    arbmCleanup(pMgmt);
  }

  return code;
}

static int32_t arbmStart(SArbitratorMgmt *pMgmt) {
  arbmPullupGetArbitrators(pMgmt);

  return arbmInitTimer(pMgmt);
}

static void arbmStop(SArbitratorMgmt *pMgmt) { arbmCleanupTimer(pMgmt); }

SMgmtFunc arbmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = arbmInit;
  mgmtFunc.closeFp = (NodeCloseFp)arbmCleanup;
  mgmtFunc.startFp = (NodeStartFp)arbmStart;
  mgmtFunc.stopFp = (NodeStopFp)arbmStop;
  mgmtFunc.requiredFp = arbmRequire;
  mgmtFunc.getHandlesFp = arbmGetMsgHandles;

  return mgmtFunc;
}

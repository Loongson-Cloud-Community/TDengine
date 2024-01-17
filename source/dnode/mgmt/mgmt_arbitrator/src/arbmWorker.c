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

static inline void arbmSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void arbmProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SArbitratorMgmt *pMgmt = pInfo->ahandle;
  int32_t          code = -1;
  const STraceId  *trace = &pMsg->info.traceId;

  dGTrace("msg:%p, get from arb-mgmt queue", pMsg);
  switch (pMsg->msgType) {
    case TDMT_DND_CREATE_ARBITRATOR:
      code = arbmProcessCreateReq(pMgmt, pMsg);
      break;
    case TDMT_DND_DROP_ARBITRATOR:
      code = arbmProcessDropReq(pMgmt, pMsg);
      break;
    case TDMT_MND_GET_ARBITRATORS_RSP:
      code = arbmProcessGetAribtratorsRsp(pMgmt, pMsg);
      break;
    case TDMT_VND_ARB_HEARTBEAT_RSP:
      code = arbmProcessArbHeartBeatRsp(pMgmt, pMsg);
      break;
    case TDMT_VND_ARB_CHECK_SYNC_RSP:
      code = arbmProcessArbHeartBeatRsp(pMgmt, pMsg);
      break;
    case TDMT_ARB_REGISTER_GROUPS:
      code = arbmProcessRegisterGroupsRep(pMgmt, pMsg);
      break;
    case TDMT_ARB_UNREGISTER_GROUPS:
      code = arbmProcessUnregisterGroupsRep(pMgmt, pMsg);
      break;
    case TDMT_ARB_GET_ARBS_TIMER:
      code = arbmProcessGetArbitratorsTimer(pMgmt, pMsg);
      break;
    case TDMT_ARB_HEARTBEAT_TIMER:
      code = arbmProcessArbHeartBeatTimer(pMgmt, pMsg);
      break;
    case TDMT_ARB_CHECK_SYNC_TIMER:
      code = arbmProcessArbCheckSyncTimer(pMgmt, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      dGError("msg:%p, not processed in arb-mgmt queue", pMsg);
  }

  if (IsReq(pMsg)) {
    if (code != 0) {
      if (terrno != 0) code = terrno;
      dGError("msg:%p, failed to process since %s, type:%s", pMsg, tstrerror(code), TMSG_INFO(pMsg->msgType));
    }
    arbmSendRsp(pMsg, code);
  }

  dGTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static int32_t arbmPutNodeMsgToWorker(SSingleWorker *pWorker, SRpcMsg *pMsg) {
  dTrace("msg:%p, put into worker %s, type:%s", pMsg, pWorker->name, TMSG_INFO(pMsg->msgType));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t arbmPutNodeMsgToQueue(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  return arbmPutNodeMsgToWorker(&pMgmt->mgmtWorker, pMsg);
}

int32_t arbmPutRpcMsgToQueue(SArbitratorMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen);
  if (pMsg == NULL) return -1;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  dTrace("msg:%p, is created and will put into arb-mgmt queue, len:%d", pMsg, pRpc->contLen);
  taosWriteQitem(pMgmt->mgmtWorker.queue, pMsg);
  return 0;
}

int32_t arbmGetQueueSize(SArbitratorMgmt *pMgmt, int32_t vgId, EQueueType qtype) {
  return taosQueueItemSize(pMgmt->mgmtWorker.queue);
}

int32_t arbmPutNodeMsgToArbQueue(SArbitratorObj *pObj, SRpcMsg *pRpc) {
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), DEF_QITEM, pRpc->contLen);
  if (pMsg == NULL) return -1;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  dTrace("msg:%p, is created and will put into arb-worker:%d queue, len:%d", pMsg, pObj->arbId, pRpc->contLen);
  taosWriteQitem(pObj->worker.queue, pMsg);
  return 0;
}

int32_t arbObjStartWorker(SArbitratorObj *pArbObj) {
  SSingleWorkerCfg wcfg = {
      .min = 1, .max = 1, .name = "arb-worker", .fp = (FItem)arbitratorProcessQueue, .param = pArbObj->pImpl};
  (void)tSingleWorkerInit(&pArbObj->worker, &wcfg);

  if (pArbObj->worker.queue == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dInfo("arbId:%d, write-queue:%p is alloced, thread:%08" PRId64, pArbObj->arbId, pArbObj->worker.queue,
        pArbObj->worker.queue->threadId);
  return 0;
}

void arbObjStopWorker(SArbitratorObj *pArbObj) {
  tSingleWorkerCleanup(&pArbObj->worker);
  dDebug("arbId:%d, queue is freed", pArbObj->arbId);
}

int32_t arbmStartWorker(SArbitratorMgmt *pMgmt) {
  SSingleWorkerCfg workerCfg = {
      .min = 1,
      .max = 1,
      .name = "arb-mgmt",
      .fp = (FItem)arbmProcessQueue,
      .param = pMgmt,
  };

  if (tSingleWorkerInit(&pMgmt->mgmtWorker, &workerCfg) != 0) {
    dError("failed to start arb-mgmt worker since %s", terrstr());
    return -1;
  }

  dDebug("arb-mgmt worker is initialized");
  return 0;
}

void arbmStopWorker(SArbitratorMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  dDebug("arb-mgmt worker is closed");
}

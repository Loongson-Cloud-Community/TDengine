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

static void *arbmBuildTimerMsg(int32_t *pContLen) {
  SArbTimerReq timerReq = {0};

  int32_t contLen = tSerializeSArbTimerReq(NULL, 0, &timerReq);
  if (contLen <= 0) return NULL;
  void *pReq = rpcMallocCont(contLen);
  if (pReq == NULL) return NULL;

  tSerializeSArbTimerReq(pReq, contLen, &timerReq);
  *pContLen = contLen;
  return pReq;
}

/*--- arbm callbacks ---*/
int32_t arbmProcessCreateReq(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  SDCreateArbitratorReq createReq = {0};
  SArbWrapperCfg        wrapperCfg = {0};
  int32_t               code = -1;
  if (tDeserializeSDCreateArbitratorReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t arbId = createReq.arbId;

  char path[TSDB_FILENAME_LEN] = {0};
  snprintf(path, TSDB_FILENAME_LEN, "%s%sarbitrator%d", pMgmt->path, TD_DIRSEP, arbId);

  if (arbitratorCreate(path, arbId) != 0) {
    dError("arbId:%d, failed to create arbitrator since %s", arbId, terrstr());
    code = terrno;
    return code;
  }

  SArbitrator *pImpl = arbitratorOpen(path, pMgmt->msgCb);
  if (pImpl == NULL) {
    dError("arbId:%d, failed to open arbitrator since %s", arbId, terrstr());
    code = terrno;
    goto _OVER;
  }

  wrapperCfg.arbId = arbId;
  wrapperCfg.dropped = 0;
  snprintf(wrapperCfg.path, sizeof(wrapperCfg.path), "%s%sarbtrator%d", pMgmt->path, TD_DIRSEP, arbId);
  code = arbmOpenArbitrator(pMgmt, &wrapperCfg, pImpl);
  if (code != 0) {
    dError("arbId:%d, failed to open vnode since %s", arbId, terrstr());
    code = terrno;
    goto _OVER;
  }

  code = arbmWriteArbitratorListToFile(pMgmt);
  if (code != 0) {
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (code != 0) {
    arbitratorClose(pImpl);
    arbitratorDestroy(path);
  } else {
    dInfo("arbId:%d, arbitrator management handle msgType:%s, end to create arbitrator, arbitrator is created", arbId,
          TMSG_INFO(pMsg->msgType));
  }

  terrno = code;
  return code;
}

int32_t arbmProcessDropReq(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  SDDropArbitratorReq dropReq = {0};
  if (tDeserializeSDCreateArbitratorReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t arbId = dropReq.arbId;
  dInfo("arbId:%d, start to drop arbitrator", arbId);

  if (dropReq.dnodeId != pMgmt->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("arbId:%d, dnodeId:%d not matched with local dnode", dropReq.arbId, dropReq.dnodeId);
    return -1;
  }

  SArbitratorObj *pArbObj = arbmAcquireArbitratorImpl(pMgmt, arbId, false);
  if (pArbObj == NULL) {
    dInfo("arbId:%d, failed to drop since %s", arbId, terrstr());
    terrno = TSDB_CODE_ARB_NOT_EXIST;
    return -1;
  }

  pArbObj->dropped = 1;
  if (arbmWriteArbitratorListToFile(pMgmt) != 0) {
    pArbObj->dropped = 0;
    arbmReleaseArbitrator(pMgmt, pArbObj);
    return -1;
  }

  arbmCloseArbitrator(pMgmt, pArbObj);
  arbmWriteArbitratorListToFile(pMgmt);

  dInfo("arbId:%d, is dropped", arbId);
  return 0;
}

int32_t arbmProcessArbHeartBeatRsp(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  SVArbHeartBeatRsp arbHbRsp = {0};
  if (tDeserializeSVArbHeartBeatRsp(pMsg->pCont, pMsg->contLen, &arbHbRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t         arbId = arbHbRsp.arbId;
  SArbitratorObj *pArbObj = arbmAcquireArbitratorImpl(pMgmt, arbId, false);
  if (pArbObj == NULL) {
    dInfo("arbId:%d, failed to process arb-hb since %s", arbId, terrstr());
    terrno = TSDB_CODE_ARB_NOT_EXIST;
    return -1;
  }

  arbmPutNodeMsgToArbQueue(pArbObj, pMsg);
  arbmReleaseArbitrator(pMgmt, pArbObj);
  tFreeSVArbHeartBeatRsp(&arbHbRsp);
  return 0;
}

int32_t arbmProcessGetAribtratorsRsp(SArbitratorMgmt *pMgmt, SRpcMsg *pRsp) {
  int32_t             ret = -1;
  SMGetArbitratorsRsp getRsp = {0};
  if (tDeserializeSMGetArbitratorsRsp(pRsp->pCont, pRsp->contLen, &getRsp)) {
    dError("failed to deserialize get-arbitrators rsp since %s", terrstr());
    goto _OVER;
  }

  if (getRsp.dnodeId != pMgmt->pData->dnodeId) {
    dError("failed to process get-arbitrators rsp, dnodeId not match %d:%d", pMgmt->pData->dnodeId, getRsp.dnodeId);
    goto _OVER;
  }

  size_t arbGroupNum = taosArrayGetSize(getRsp.arbGroups);
  for (int32_t i = 0; i < arbGroupNum; i++) {
    SArbitratorGroups *pArbGroups = taosArrayGet(getRsp.arbGroups, i);
    SArbitratorObj   *pArbObj = arbmAcquireArbitrator(pMgmt, pArbGroups->arbId);
    if (pArbObj == NULL) {
      dInfo("failed to process get-arbitrators rsp, arbitrator:%d not exist", pArbGroups->arbId);
      goto _OVER;
    }

    int32_t contLen = tSerializeSArbitratorGroups(NULL, 0, pArbGroups);
    void   *pHead = rpcMallocCont(contLen);
    if (pRsp == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    tSerializeSArbitratorGroups(pHead, contLen, pArbGroups);

    SRpcMsg rpcMsg = {.pCont = pHead,
                      .contLen = contLen,
                      .msgType = TDMT_ARB_REGISTER_GROUPS,
                      .info.noResp = 1};

    arbmPutNodeMsgToArbQueue(pArbObj, &rpcMsg);
    arbmReleaseArbitrator(pMgmt, pArbObj);
  }

  ret = 0;

_OVER:
  tFreeSMGetArbitratorsRsp(&getRsp);
  return ret;
}

int32_t arbmProcessRegisterGroupsRep(SArbitratorMgmt *pMgmt, SRpcMsg *pReq) {
  int32_t              ret = -1;
  SArbRegisterGroupReq registerReq = {0};
  if (tDeserializeSArbitratorGroups(pReq->pCont, pReq->contLen, &registerReq)) {
    dError("failed to deserialize arb-register-groups req since %s", terrstr());
    goto _OVER;
  }

  SArbitratorObj *pArbObj = arbmAcquireArbitrator(pMgmt, registerReq.arbId);
  if (pArbObj == NULL) {
    dInfo("failed to process arb-register-groups req, arbitrator:%d not exist", registerReq.arbId);
    goto _OVER;
  }

  arbmPutNodeMsgToArbQueue(pArbObj, pReq);
  arbmReleaseArbitrator(pMgmt, pArbObj);

  pReq->info.noResp = 1;

  ret = 0;

_OVER:
  return ret;
}

int32_t arbmProcessUnregisterGroupsRep(SArbitratorMgmt *pMgmt, SRpcMsg *pReq) {
  int32_t              ret = -1;
  SArbUnregisterGroupReq unregisterReq = {0};
  if (tDeserializeSArbitratorGroups(pReq->pCont, pReq->contLen, &unregisterReq)) {
    dError("failed to deserialize arb-unregister-groups req since %s", terrstr());
    goto _OVER;
  }

  SArbitratorObj *pArbObj = arbmAcquireArbitrator(pMgmt, unregisterReq.arbId);
  if (pArbObj == NULL) {
    dInfo("failed to process arb-unregister-groups req, arbitrator:%d not exist", unregisterReq.arbId);
    goto _OVER;
  }

  arbmPutNodeMsgToArbQueue(pArbObj, pReq);
  arbmReleaseArbitrator(pMgmt, pArbObj);

  pReq->info.noResp = 1;

  ret = 0;

_OVER:
  return ret;
}

void arbmPullupGetArbitrators(SArbitratorMgmt *pMgmt) {
  int32_t contLen = 0;
  void   *pReq = arbmBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_ARB_GET_ARBS_TIMER, .pCont = pReq, .contLen = contLen, .info.noResp = 1};
    arbmPutRpcMsgToQueue(pMgmt, WRITE_QUEUE, &rpcMsg);
  }
}

/*--- pull up timers ---*/
void arbmPullupArbHeartbeat(SArbitratorMgmt *pMgmt) {
  int32_t contLen = 0;
  void   *pReq = arbmBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_ARB_HEARTBEAT_TIMER, .pCont = pReq, .contLen = contLen, .info.noResp = 1};
    arbmPutRpcMsgToQueue(pMgmt, WRITE_QUEUE, &rpcMsg);
  }
}

/*--- timer process funcs ---*/
int32_t arbmProcessGetArbitratorsTimer(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  SMGetArbitratorsReq req = {.dnodeId = pMgmt->pData->dnodeId};
  int32_t             contLen = tSerializeSMGetArbitratorsReq(NULL, 0, &req);
  void               *pHead = rpcMallocCont(contLen);
  tSerializeSMGetArbitratorsReq(pHead, contLen, &req);

  SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_MND_GET_ARBITRATORS};
  SEpSet  epset = {0};
  dmGetMnodeEpSet(pMgmt->pData, &epset);
  return tmsgSendReq(&epset, &rpcMsg);
}

int32_t arbmProcessArbHeartBeatTimer(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  SMGetArbitratorsReq req = {.dnodeId = pMgmt->pData->dnodeId};
  int32_t             contLen = tSerializeSMGetArbitratorsReq(NULL, 0, &req);
  void               *pHead = rpcMallocCont(contLen);
  tSerializeSMGetArbitratorsReq(pHead, contLen, &req);

  SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_ARB_HEARTBEAT_TIMER};
  void   *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter != NULL) {
    SArbitratorObj *pArbObj = *(SArbitratorObj **)pIter;
    arbmPutNodeMsgToArbQueue(pArbObj, &rpcMsg);
    pIter = taosHashIterate(pMgmt->hash, pIter);
  }
  return 0;
}

SArray *arbmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(16, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  // Requests handled by ARBITRATOR
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_ARBITRATOR, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_ARBITRATOR, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_ARB_REGISTER_GROUPS, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_ARB_UNREGISTER_GROUPS, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_ARBITRATORS_RSP, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_VND_ARB_HEARTBEAT_RSP, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  code = 0;

_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}

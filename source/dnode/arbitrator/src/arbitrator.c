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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "arbInt.h"
#include "tmisce.h"

static inline void arbSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static SArbGroupMember *arbitratorGetMember(SArbitrator *pArb, int32_t dnodeId, int32_t groupId);
static SArbGroup       *arbitratorGetGroup(SArbitrator *pArb, int32_t groupId);

static int32_t arbitratorProcessRegisterGroupsReq(SArbitrator *pArb, SRpcMsg *pMsg) {
  SArbRegisterGroupReq registerReq = {0};
  if (tDeserializeSArbitratorGroups(pMsg->pCont, pMsg->contLen, &registerReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (registerReq.arbId != pArb->arbId) {
    terrno = TSDB_CODE_INVALID_MSG;
    arbError("arbId not matched local:%d, msg:%d", pArb->arbId, registerReq.arbId);
    return -1;
  }

  size_t sz = taosArrayGetSize(registerReq.groups);
  for (size_t i = 0; i < sz; i++) {
    SArbitratorGroupInfo *pInfo = taosArrayGet(registerReq.groups, i);
    int32_t               groupId = pInfo->groupId;

    SArbGroup *pGroup = taosHashGet(pArb->arbGroupMap, &groupId, sizeof(int32_t));
    if (pGroup) {
      // TODO(LSG): handle group update
      continue;
    }

    SArbGroup group = {0};
    arbitratorInitSArbGroup(&group);
    for (int8_t j = 0; j < pInfo->replica; j++) {
      int32_t dnodeId = pInfo->dnodeIds[j];
      group.members[j].info.dnodeId = dnodeId;

      SArbDnode *pArbDnode = taosHashGet(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t));
      if (!pArbDnode) {
        SArbDnode arbDnode = {0};
        arbDnode.groupIds = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
        taosHashPut(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t), &arbDnode, sizeof(SArbDnode));
        pArbDnode = taosHashGet(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t));
      }
      taosHashPut(pArbDnode->groupIds, &groupId, sizeof(int32_t), NULL, 0);
      arbDebug("arbId:%d, add group:%d, map size:%d", pArb->arbId, groupId, taosHashGetSize(pArbDnode->groupIds));
    }
    taosHashPut(pArb->arbGroupMap, &groupId, sizeof(int32_t), &group, sizeof(SArbGroup));
  }

  arbInfo("arbId:%d, save config while process register groups", pArb->arbId);

  SArbitratorDiskDate diskData;
  diskData.arbId = pArb->arbId;
  diskData.arbGroupMap = pArb->arbGroupMap;  // not owned by diskData
  if (arbitratorUpdateDiskData(pArb->path, &diskData) < 0) {
    return -1;
  }

  return 0;
}

static int32_t arbitratorProcessUnregisterGroupsReq(SArbitrator *pArb, SRpcMsg *pMsg) {
  SArbUnregisterGroupReq unregisterReq = {0};
  if (tDeserializeSArbitratorGroups(pMsg->pCont, pMsg->contLen, &unregisterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (unregisterReq.arbId != pArb->arbId) {
    terrno = TSDB_CODE_INVALID_MSG;
    arbError("arbId not matched local:%d, msg:%d", pArb->arbId, unregisterReq.arbId);
    return -1;
  }

  size_t sz = taosArrayGetSize(unregisterReq.groups);
  for (size_t i = 0; i < sz; i++) {
    SArbitratorGroupInfo *pInfo = taosArrayGet(unregisterReq.groups, i);
    int32_t               groupId = pInfo->groupId;

    for (int8_t j = 0; j < pInfo->replica; j++) {
      int32_t    dnodeId = pInfo->dnodeIds[j];
      SArbDnode *pArbDnode = taosHashGet(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t));
      if (!pArbDnode) {
        SArbDnode arbDnode = {0};
        arbDnode.groupIds = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
        taosHashPut(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t), &arbDnode, sizeof(SArbDnode));
        pArbDnode = taosHashGet(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t));
      }
      taosHashRemove(pArbDnode->groupIds, &groupId, sizeof(int32_t));
    }
    taosHashRemove(pArb->arbGroupMap, &groupId, sizeof(int32_t));
  }

  arbInfo("arbId:%d, save config while process unregister groups", pArb->arbId);

  SArbitratorDiskDate diskData;
  diskData.arbId = pArb->arbId;
  diskData.arbGroupMap = pArb->arbGroupMap;  // not owned by diskData
  if (arbitratorUpdateDiskData(pArb->path, &diskData) < 0) {
    return -1;
  }

  return 0;
}

static int32_t arbitratorProcessArbHeartBeatTimer(SArbitrator *pArb, SRpcMsg *pMsg) {
  void *pDnodeIter = taosHashIterate(pArb->arbDnodeMap, NULL);
  while (pDnodeIter) {
    int32_t    dnodeId = *(int32_t *)taosHashGetKey(pDnodeIter, NULL);
    SArbDnode *pArbDnode = pDnodeIter;

    SVArbHeartBeatReq req = {0};
    req.arbId = pArb->arbId;
    memcpy(req.arbToken, pArb->arbToken, TD_ARB_TOKEN_SIZE);
    req.dnodeId = dnodeId;

    req.arbSeqArray = taosArrayInit(16, sizeof(SVArbHeartBeatSeq));

    void *piditer = taosHashIterate(pArbDnode->groupIds, NULL);
    while (piditer) {
      int32_t         *pGroupId = piditer;
      SArbGroupMember *pMember = arbitratorGetMember(pArb, dnodeId, *pGroupId);
      if (pMember != NULL) {
        SVArbHeartBeatSeq seq = {.groupId = *pGroupId, .seqNo = pMember->state.nextHbSeq++};
        taosArrayPush(req.arbSeqArray, &seq);
      }
      piditer = taosHashIterate(pArbDnode->groupIds, piditer);
    }

    int32_t contLen = tSerializeSVArbHeartBeatReq(NULL, 0, &req);
    void   *pHead = rpcMallocCont(contLen);
    tSerializeSVArbHeartBeatReq(pHead, contLen, &req);

    SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_VND_ARB_HEARTBEAT};

    SEpSet epset = {.inUse = 0, .numOfEps = 1};
    pArb->msgCb.getDnodeEpFp(pArb->msgCb.data, dnodeId, NULL, epset.eps[0].fqdn, &epset.eps[0].port);
    pArb->msgCb.sendReqFp(&epset, &rpcMsg);

    tFreeSVArbHeartBeatReq(&req);
    pDnodeIter = taosHashIterate(pArb->arbDnodeMap, pDnodeIter);
  }

  return 0;
}

static bool arbitratorUpdateMemberState(SArbitrator *pArb, SArbGroupMember *pLocalMember, SVArbHbMember *pHbMember,
                                        int64_t nowMs) {
  int32_t dnodeId = pLocalMember->info.dnodeId;
  bool    tokenUpdated = false;

  // update hb state
  pLocalMember->state.responsedHbSeq = pHbMember->seqNo;
  pLocalMember->state.lastHbMs = nowMs;
  if (strncmp(pLocalMember->state.token, pHbMember->memberToken, TD_ARB_TOKEN_SIZE) == 0) goto _OVER;

  // update token
  memcpy(pLocalMember->state.token, pHbMember->memberToken, TD_ARB_TOKEN_SIZE);
  tokenUpdated = true;
  arbInfo("arbId:%d, update dnodeId:%d groupId:%d token, local:%s msg:%s ", pArb->arbId, dnodeId, pHbMember->groupId,
          pLocalMember->state.token, pHbMember->memberToken);

_OVER:
  return tokenUpdated;
}

static void arbitratorResetAssignedLeader(SArbitrator *pArb, SArbGroup *pGroup, int32_t dnodeId) {
  pGroup->assignedLeader.dnodeId = 0;
  memset(pGroup->assignedLeader.token, 0, TD_ARB_TOKEN_SIZE);
}

static int32_t arbitratorProcessArbHeartBeatRsp(SArbitrator *pArb, SRpcMsg *pMsg) {
  SVArbHeartBeatRsp arbHbRsp = {0};
  if (tDeserializeSVArbHeartBeatRsp(pMsg->pCont, pMsg->contLen, &arbHbRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  if (arbHbRsp.arbId != pArb->arbId) {
    terrno = TSDB_CODE_INVALID_MSG;
    arbError("arbId not matched local:%d, msg:%d", pArb->arbId, arbHbRsp.arbId);
    goto _OVER;
  }

  if (strcmp(arbHbRsp.arbToken, pArb->arbToken) != 0) {
    terrno = TSDB_CODE_ARB_TOKEN_MISMATCH;
    arbInfo("arbId:%d, arbToken not matched local:%s, msg:%s", pArb->arbId, pArb->arbToken, arbHbRsp.arbToken);
    goto _OVER;
  }

  int64_t nowMs = taosGetTimestampMs();
  int32_t dnodeId = arbHbRsp.dnodeId;
  size_t  sz = taosArrayGetSize(arbHbRsp.hbMembers);
  for (size_t i = 0; i < sz; i++) {
    SVArbHbMember   *pHbMember = taosArrayGet(arbHbRsp.hbMembers, i);
    SArbGroupMember *pLocalMember = arbitratorGetMember(pArb, dnodeId, pHbMember->groupId);
    if (pLocalMember == NULL) {
      continue;
    }

    if (pLocalMember->state.responsedHbSeq >= pHbMember->seqNo) {
      arbInfo("arbId:%d, update dnodeId:%d groupId:%d token failed, seqNo expired, local:%d msg:%d", pArb->arbId,
              dnodeId, pHbMember->groupId, pLocalMember->state.responsedHbSeq, pHbMember->seqNo);
      continue;
    }

    bool tokenUpdated = arbitratorUpdateMemberState(pArb, pLocalMember, pHbMember, nowMs);
    SArbGroup *pGroup = arbitratorGetGroup(pArb, pHbMember->groupId);
    if (pGroup == NULL) {
      continue;
    }
    if (tokenUpdated && dnodeId == pGroup->assignedLeader.dnodeId) {
      arbitratorResetAssignedLeader(pArb, pGroup, dnodeId);
    }
  }

  terrno = TSDB_CODE_SUCCESS;

_OVER:
  tFreeSVArbHeartBeatRsp(&arbHbRsp);
  return terrno == TSDB_CODE_SUCCESS ? 0 : -1;
}

static int32_t arbitratorProcessArbCheckSyncTimer(SArbitrator *pArb, SRpcMsg *pMsg) {
  int64_t nowMs = taosGetTimestampMs();

  void *pIter = taosHashIterate(pArb->arbGroupMap, NULL);
  while (pIter) {
    int32_t    groupId = *(int32_t *)taosHashGetKey(pIter, NULL);
    SArbGroup *pArbGroup = pIter;
    SArbGroupMember *pArbMember = &pArbGroup->members[pArbGroup->epIndex];

    // has assigned_leader or isSync => skip
    if (pArbGroup->assignedLeader.dnodeId != 0 || pArbGroup->isSync) {
      pIter = taosHashIterate(pArb->arbDnodeMap, pIter);
      continue;
    }

    // hb timeout => skip
    if (pArbMember->state.lastHbMs < nowMs - ARBITRATOR_TIMEOUT_SEC * 1000) {
      pIter = taosHashIterate(pArb->arbDnodeMap, pIter);
      continue;
    }

    SVArbCheckSyncReq req = {0};
    req.arbId = pArb->arbId;
    memcpy(req.arbToken, pArb->arbToken, TD_ARB_TOKEN_SIZE);
    for (int i = 0; i < 2; i++) {
      memcpy(req.memberTokens[i], pArbGroup->members[i].state.token, TD_ARB_TOKEN_SIZE);
    }
    int32_t   reqLen = tSerializeSVArbCheckSyncReq(NULL, 0, &req);
    int32_t   contLen = reqLen + sizeof(SMsgHead);
    SMsgHead *pHead = rpcMallocCont(contLen);
    pHead->contLen = htonl(contLen);
    pHead->vgId = htonl(groupId);
    tSerializeSVArbCheckSyncReq((char *)pHead + sizeof(SMsgHead), contLen, &req);

    SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_VND_ARB_CHECK_SYNC};

    SEpSet epset = {.inUse = 0, .numOfEps = 1};
    pArb->msgCb.getDnodeEpFp(pArb->msgCb.data, pArbMember->info.dnodeId, NULL, epset.eps[0].fqdn, &epset.eps[0].port);
    pArb->msgCb.sendReqFp(&epset, &rpcMsg);

    pIter = taosHashIterate(pArb->arbDnodeMap, pIter);
  }

  return 0;
}

static int32_t arbitratorProcessArbCheckSyncRsp(SArbitrator *pArb, SRpcMsg *pMsg) {
  SVArbCheckSyncRsp checkSyncRsp = {0};
  if (tDeserializeSVArbCheckSyncRsp(pMsg->pCont, pMsg->contLen, &checkSyncRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  if (checkSyncRsp.arbId != pArb->arbId) {
    terrno = TSDB_CODE_INVALID_MSG;
    arbError("arbId not matched local:%d, msg:%d", pArb->arbId, checkSyncRsp.arbId);
    goto _OVER;
  }

  if (strcmp(checkSyncRsp.arbToken, pArb->arbToken) != 0) {
    terrno = TSDB_CODE_ARB_TOKEN_MISMATCH;
    arbInfo("arbId:%d, arbToken not matched local:%s, msg:%s", pArb->arbId, pArb->arbToken, checkSyncRsp.arbToken);
    goto _OVER;
  }

  SArbGroup *pGroup = arbitratorGetGroup(pArb, checkSyncRsp.groupId);
  if (pGroup == NULL) {
    terrno = TSDB_CODE_NOT_FOUND;
    arbError("arbId:%d, groupId:%d not found", pArb->arbId, checkSyncRsp.groupId);
    goto _OVER;
  }

  if (pGroup->assignedLeader.dnodeId != 0) {
    terrno = TSDB_CODE_SUCCESS;
    arbInfo("arbId:%d, recv arb check-sync-rsp from groupId:%d, but local has assigned leader:%d", pArb->arbId,
            checkSyncRsp.groupId, pGroup->assignedLeader.dnodeId);
    goto _OVER;
  }

  for (int i = 0; i < 2; i++) {
    if (strcmp(checkSyncRsp.memberTokens[i], pGroup->members[i].state.token) != 0) {
      terrno = TSDB_CODE_ARB_TOKEN_MISMATCH;
      arbInfo("arbId:%d, memberToken not matched local:%s, msg:%s", pArb->arbId, pArb->arbToken, checkSyncRsp.arbToken);
      goto _OVER;
    }
  }

  // handle errcode
  if (checkSyncRsp.errCode == TSDB_CODE_SYN_NOT_LEADER) {
    pGroup->epIndex = (pGroup->epIndex + 1) % 2;
  } else {
    pGroup->isSync = (checkSyncRsp.errCode == TSDB_CODE_SUCCESS);
  }

  terrno = TSDB_CODE_SUCCESS;

_OVER:
  return terrno == TSDB_CODE_SUCCESS ? 0 : -1;
}

void arbitratorProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SArbitrator *pArb = pInfo->ahandle;
  int32_t      code = -1;

  arbTrace("msg:%p, get from arb-mgmt queue", pMsg);
  switch (pMsg->msgType) {
    case TDMT_ARB_REGISTER_GROUPS:
      code = arbitratorProcessRegisterGroupsReq(pArb, pMsg);
      break;
    case TDMT_ARB_UNREGISTER_GROUPS:
      code = arbitratorProcessUnregisterGroupsReq(pArb, pMsg);
      break;
    case TDMT_ARB_HEARTBEAT_TIMER:
      code = arbitratorProcessArbHeartBeatTimer(pArb, pMsg);
      break;
    case TDMT_ARB_CHECK_SYNC_TIMER:
      code = arbitratorProcessArbCheckSyncTimer(pArb, pMsg);
      break;
    case TDMT_VND_ARB_HEARTBEAT_RSP:
      code = arbitratorProcessArbHeartBeatRsp(pArb, pMsg);
      break;
    case TDMT_VND_ARB_CHECK_SYNC_RSP:
      code = arbitratorProcessArbCheckSyncRsp(pArb, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      arbError("msg:%p, not processed in arb-mgmt queue", pMsg);
  }

  if (IsReq(pMsg)) {
    if (code != 0) {
      if (terrno != 0) code = terrno;
      arbError("msg:%p, failed to process since %s, type:%s", pMsg, tstrerror(code), TMSG_INFO(pMsg->msgType));
    }
    arbSendRsp(pMsg, code);
  }

  arbTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static SArbGroupMember *arbitratorGetMember(SArbitrator *pArb, int32_t dnodeId, int32_t groupId) {
  SArbGroup *pGroup = taosHashGet(pArb->arbGroupMap, &groupId, sizeof(int32_t));
  if (pGroup == NULL) {
    goto _OVER;
  }
  SArbGroupMember *pMember = NULL;
  for (int i = 0; i < 2; i++) {
    pMember = &pGroup->members[i];
    if (pMember->info.dnodeId == dnodeId) {
      break;
    }
  }

  if (pMember) return pMember;

_OVER:
  arbTrace("arbId:%d, get member groupId:%d dnodeId:%d failed, no member found", pArb->arbId, groupId, dnodeId);
  return NULL;
}

static SArbGroup *arbitratorGetGroup(SArbitrator *pArb, int32_t groupId) {
  SArbGroup *pGroup = taosHashGet(pArb->arbGroupMap, &groupId, sizeof(int32_t));
  if (pGroup == NULL) {
    goto _OVER;
  }

_OVER:
  arbTrace("arbId:%d, get group groupId:%d failed, no group found", pArb->arbId, groupId);
  return NULL;
}

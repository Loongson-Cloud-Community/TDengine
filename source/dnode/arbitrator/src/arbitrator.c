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
    for (int8_t j = 0; j < pInfo->replica; j++) {
      int32_t dnodeId = pInfo->dnodeIds[j];
      group.members[j].info.dnodeId = dnodeId;
      group.members[j].state.nextHbSeq = 0;
      group.members[j].state.responsedHbSeq = -1;

      SArbDnode *pArbDnode = taosHashGet(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t));
      if (!pArbDnode) {
        SArbDnode arbDnode = {0};
        arbDnode.groupIds = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
        taosHashPut(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t), &arbDnode, sizeof(SArbDnode));
        pArbDnode = taosHashGet(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t));
      }
      taosHashPut(pArbDnode->groupIds, &groupId, sizeof(int32_t), NULL, 0);
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
  void  *pIter = taosHashIterate(pArb->arbDnodeMap, NULL);
  while (pIter) {
    int32_t    dnodeId = *(int32_t *)taosHashGetKey(pIter, NULL);
    SArbDnode *pArbDnode = pIter;

    SVArbHeartBeatReq req = {0};
    req.arbId = pArb->arbId;
    memcpy(req.arbToken, pArb->arbToken, TD_ARB_TOKEN_SIZE);
    req.dnodeId = dnodeId;

    req.arbSeqArray = taosArrayInit(16, sizeof(SVArbHeartBeatSeq));

    void *iter = taosHashIterate(pArbDnode->groupIds, NULL);
    while (iter) {
      int32_t         *pGroupId = iter;
      SArbGroupMember *pMember = arbitratorGetMember(pArb, dnodeId, *pGroupId);
      if (pMember != NULL) {
        SVArbHeartBeatSeq seq = {.groupId = *pGroupId, .seqNo = pMember->state.nextHbSeq++};
        taosArrayPush(req.arbSeqArray, &seq);
      }
      iter = taosHashIterate(pArbDnode->groupIds, iter);
    }

    int32_t contLen = tSerializeSVArbHeartBeatReq(NULL, 0, &req);
    void   *pHead = rpcMallocCont(contLen);
    tSerializeSVArbHeartBeatReq(pHead, contLen, &req);

    SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_VND_ARB_HEARTBEAT};

    SEpSet epset = {.inUse = 0, .numOfEps = 1};
    pArb->msgCb.getDnodeEpFp(pArb->msgCb.data, dnodeId, NULL, epset.eps[0].fqdn, &epset.eps[0].port);
    pArb->msgCb.sendReqFp(&epset, &rpcMsg);

    tFreeSVArbHeartBeatReq(&req);
    pIter = taosHashIterate(pArb->arbDnodeMap, pIter);
  }

  return 0;
}

static void arbitratorUpdateArbGroupMemberState(SArbitrator *pArb, int32_t dnodeId, SArray *hbMembers) {
  size_t sz = taosArrayGetSize(hbMembers);
  for (size_t i = 0; i < sz; i++) {
    SVArbHbMember   *pHbMember = taosArrayGet(hbMembers, i);
    SArbGroupMember *pMember = arbitratorGetMember(pArb, dnodeId, pHbMember->groupId);
    if (pMember == NULL) {
      continue;
    }

    if (pMember->state.responsedHbSeq >= pHbMember->seqNo) {
      arbInfo("arbId:%d, update dnodeId:%d groupId:%d token failed, seqNo expired, msg:%d local:%d", pArb->arbId,
              dnodeId, pHbMember->groupId, pHbMember->seqNo, pMember->state.responsedHbSeq);
      continue;
    }

    // update local
    pMember->state.responsedHbSeq = pHbMember->seqNo;
    memcpy(pMember->state.token, pHbMember->memberToken, TD_ARB_TOKEN_SIZE);
  }
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

  arbitratorUpdateArbGroupMemberState(pArb, arbHbRsp.dnodeId, arbHbRsp.hbMembers);

_OVER:
  tFreeSVArbHeartBeatRsp(&arbHbRsp);
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
    case TDMT_VND_ARB_HEARTBEAT_RSP:
      code = arbitratorProcessArbHeartBeatRsp(pArb, pMsg);
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
    goto _OUT;
  }
  SArbGroupMember *pMember = NULL;
  for (int i = 0; i < 2; i++) {
    pMember = &pGroup->members[i];
    if (pMember->info.dnodeId != dnodeId) {
      continue;
    }
    return pMember;
  }

_OUT:
  arbError("arbId:%d, get hb seq groupId:%d dnodeId:%d failed, no member found", pArb->arbId, groupId, dnodeId);
  return NULL;
}

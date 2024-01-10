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

#define _DEFAULT_SOURCE
#include "mndArbitrator.h"
#include "audit.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"

#define ARBITRATOR_VER_NUMBER   1
#define ARBITRATOR_RESERVE_SIZE 64

static SSdbRaw *mndArbitratorActionEncode(SArbObj *pObj);
static SSdbRow *mndArbitratorActionDecode(SSdbRaw *pRaw);
static int32_t  mndArbitratorActionInsert(SSdb *pSdb, SArbObj *pObj);
static int32_t  mndArbitratorActionUpdate(SSdb *pSdb, SArbObj *pOld, SArbObj *pNew);
static int32_t  mndArbitratorActionDelete(SSdb *pSdb, SArbObj *pObj);
static int32_t  mndProcessCreateArbitratorReq(SRpcMsg *pReq);
static int32_t  mndProcessDropArbitratorReq(SRpcMsg *pReq);
static int32_t  mndProcessGetArbitratorsReq(SRpcMsg *pReq);
static int32_t  mndRetrieveArbitrators(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextArbitrator(SMnode *pMnode, void *pIter);

int32_t mndInitArbitrator(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_ARBITRATOR,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndArbitratorActionEncode,
      .decodeFp = (SdbDecodeFp)mndArbitratorActionDecode,
      .insertFp = (SdbInsertFp)mndArbitratorActionInsert,
      .updateFp = (SdbUpdateFp)mndArbitratorActionUpdate,
      .deleteFp = (SdbDeleteFp)mndArbitratorActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ARBITRATOR, mndProcessCreateArbitratorReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_ARBITRATOR, mndProcessDropArbitratorReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_ARBITRATORS, mndProcessGetArbitratorsReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_ARBITRATOR_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_ARBITRATOR_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_ARB_REGISTER_GROUPS_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_ARB_UNREGISTER_GROUPS_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ARBITRATOR, mndRetrieveArbitrators);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ARBITRATOR, mndCancelGetNextArbitrator);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupArbitrator(SMnode *pMnode) {}

SArbObj *mndAcquireArbitrator(SMnode *pMnode, int32_t arbId) {
  SArbObj *pObj = sdbAcquire(pMnode->pSdb, SDB_ARBITRATOR, &arbId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_ARBITRATOR_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseArbitrator(SMnode *pMnode, SArbObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndArbitratorActionEncode(SArbObj *pObj) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  size = sizeof(SArbObj) + pObj->numOfVgroups * sizeof(int32_t) + ARBITRATOR_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_ARBITRATOR, ARBITRATOR_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->numOfVgroups, _OVER)
  void *iter = taosHashIterate(pObj->groupIds, NULL);
  while (iter) {
    int32_t *vgId = iter;
    SDB_SET_INT32(pRaw, dataPos, *vgId, _OVER)
    iter = taosHashIterate(pObj->groupIds, iter);
  }
  SDB_SET_RESERVE(pRaw, dataPos, ARBITRATOR_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("arbitrator:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("arbitrator:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndArbitratorActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow *pRow = NULL;
  SArbObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != ARBITRATOR_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SArbObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->numOfVgroups, _OVER)
  pObj->groupIds = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (pObj->numOfVgroups > 0) {
    if (pObj->groupIds == NULL) goto _OVER;
    for (int i = 0; i < pObj->numOfVgroups; i++) {
      int32_t vgId = -1;
      SDB_GET_INT32(pRaw, dataPos, &vgId, _OVER)
      if (taosHashPut(pObj->groupIds, &vgId, sizeof(int32_t), NULL, 0) != 0) goto _OVER;
    }
  }
  SDB_GET_RESERVE(pRaw, dataPos, ARBITRATOR_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("arbitrator:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("arbitrator:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndArbitratorActionInsert(SSdb *pSdb, SArbObj *pObj) {
  mTrace("arbitrator:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("arbitrator:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndArbitratorActionDelete(SSdb *pSdb, SArbObj *pObj) {
  mTrace("arbitrator:%d, perform delete action, row:%p", pObj->id, pObj);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndArbitratorActionUpdate(SSdb *pSdb, SArbObj *pOld, SArbObj *pNew) {
  mTrace("arbitrator:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
  pOld->numOfVgroups = pNew->numOfVgroups;
  SHashObj *tmp = pOld->groupIds;
  pOld->groupIds = pNew->groupIds;
  pNew->groupIds = tmp;
  return 0;
}

static int32_t mndSetCreateArbitratorRedoLogs(STrans *pTrans, SArbObj *pObj) {
  SSdbRaw *pRedoRaw = mndArbitratorActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateArbitratorUndoLogs(STrans *pTrans, SArbObj *pObj) {
  SSdbRaw *pUndoRaw = mndArbitratorActionEncode(pObj);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

int32_t mndSetCreateArbitratorCommitLogs(STrans *pTrans, SArbObj *pObj) {
  SSdbRaw *pCommitRaw = mndArbitratorActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

int32_t mndSetCreateArbitratorRedoActions(STrans *pTrans, SDnodeObj *pDnode, SArbObj *pObj) {
  SDCreateArbitratorReq createReq = {0};
  createReq.dnodeId = pDnode->id;
  createReq.arbId = pObj->id;

  int32_t contLen = tSerializeSDCreateArbitratorReq(NULL, 0, &createReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSDCreateArbitratorReq(pReq, contLen, &createReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_ARBITRATOR;
  action.acceptableCode = TSDB_CODE_ARBITRATOR_ALREADY_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndSetCreateArbitratorUndoActions(STrans *pTrans, SDnodeObj *pDnode, SArbObj *pObj) {
  SDDropArbitratorReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;
  dropReq.arbId = pObj->id;

  int32_t contLen = tSerializeSDCreateArbitratorReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSDCreateArbitratorReq(pReq, contLen, &dropReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_ARBITRATOR;
  action.acceptableCode = TSDB_CODE_ARBITRATOR_NOT_DEPLOYED;

  if (mndTransAppendUndoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndCreateArbitrator(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateArbitratorReq *pCreate) {
  int32_t code = -1;

  SArbObj arbObj = {0};
  arbObj.id = pDnode->id;
  arbObj.createdTime = taosGetTimestampMs();
  arbObj.updateTime = arbObj.createdTime;
  arbObj.numOfVgroups = 0;
  arbObj.groupIds = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-arbitrator");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to create arbitrator:%d", pTrans->id, pCreate->dnodeId);
  if (mndSetCreateArbitratorRedoLogs(pTrans, &arbObj) != 0) goto _OVER;
  if (mndSetCreateArbitratorUndoLogs(pTrans, &arbObj) != 0) goto _OVER;
  if (mndSetCreateArbitratorCommitLogs(pTrans, &arbObj) != 0) goto _OVER;
  if (mndSetCreateArbitratorRedoActions(pTrans, pDnode, &arbObj) != 0) goto _OVER;
  if (mndSetCreateArbitratorUndoActions(pTrans, pDnode, &arbObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateArbitratorReq(SRpcMsg *pReq) {
  SMnode               *pMnode = pReq->info.node;
  int32_t               code = -1;
  SArbObj              *pObj = NULL;
  SDnodeObj            *pDnode = NULL;
  SMCreateArbitratorReq createReq = {0};

  if (tDeserializeSCreateDropMQSNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("arbitrator:%d, start to create", createReq.dnodeId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_ARBITRATOR) != 0) {
    goto _OVER;
  }

  pObj = mndAcquireArbitrator(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    terrno = TSDB_CODE_MND_ARBITRATOR_ALREADY_EXIST;
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_ARBITRATOR_NOT_EXIST) {
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  code = mndCreateArbitrator(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char obj[33] = {0};
  sprintf(obj, "%d", createReq.dnodeId);

  auditRecord(pReq, pMnode->clusterId, "createArbitrator", "", obj, createReq.sql, createReq.sqlLen);
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("arbitrator:%d, failed to create since %s", createReq.dnodeId, terrstr());
  }

  mndReleaseArbitrator(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  tFreeSMCreateQnodeReq(&createReq);
  return code;
}

static int32_t mndSetDropArbitratorRedoLogs(STrans *pTrans, SArbObj *pObj) {
  SSdbRaw *pRedoRaw = mndArbitratorActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;
  return 0;
}

static int32_t mndSetDropArbitratorCommitLogs(STrans *pTrans, SArbObj *pObj) {
  SSdbRaw *pCommitRaw = mndArbitratorActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetDropArbitratorRedoActions(STrans *pTrans, SDnodeObj *pDnode, SArbObj *pObj) {
  SDDropArbitratorReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;
  dropReq.arbId = pObj->id;

  int32_t contLen = tSerializeSDCreateArbitratorReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSDCreateArbitratorReq(pReq, contLen, &dropReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_ARBITRATOR;
  action.acceptableCode = TSDB_CODE_ARBITRATOR_NOT_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

int32_t mndSetDropArbitratorInfoToTrans(SMnode *pMnode, STrans *pTrans, SArbObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  if (mndSetDropArbitratorRedoLogs(pTrans, pObj) != 0) return -1;
  if (mndSetDropArbitratorCommitLogs(pTrans, pObj) != 0) return -1;
  if (!force) {
    if (mndSetDropArbitratorRedoActions(pTrans, pObj->pDnode, pObj) != 0) return -1;
  }
  return 0;
}

static int32_t mndDropArbitrator(SMnode *pMnode, SRpcMsg *pReq, SArbObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-arbitrator");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to drop arbitrator:%d", pTrans->id, pObj->id);
  if (mndSetDropArbitratorInfoToTrans(pMnode, pTrans, pObj, false) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropArbitratorReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SArbObj            *pObj = NULL;
  SMDropArbitratorReq dropReq = {0};

  if (tDeserializeSCreateDropMQSNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("arbitrator:%d, start to drop", dropReq.dnodeId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_ARBITRATOR) != 0) {
    goto _OVER;
  }

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireArbitrator(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    goto _OVER;
  }

  code = mndDropArbitrator(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char obj[33] = {0};
  sprintf(obj, "%d", dropReq.dnodeId);

  auditRecord(pReq, pMnode->clusterId, "dropArbitrator", "", obj, dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("arbitrator:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseArbitrator(pMnode, pObj);
  tFreeSDDropQnodeReq(&dropReq);
  return code;
}

static void *mndBuildRegisterVgToArbitratorReq(SMnode *pMnode, SDnodeObj *pDnode, int32_t arbId, SVgObj *pVgroup,
                                               int32_t *pContLen) {
  SArbitratorGroupInfo info = {0};
  info.groupId = pVgroup->vgId;
  info.replica = pVgroup->replica;
  for (int8_t i = 0; i < pVgroup->replica; i++) {
    info.dnodeIds[i] = pVgroup->vnodeGid[i].dnodeId;
  }

  SArbRegisterGroupReq registerReq = {0};
  registerReq.arbId = arbId;
  registerReq.groups = taosArrayInit(1, sizeof(SArbitratorGroupInfo));
  taosArrayPush(registerReq.groups, &info);

  int32_t contLen = tSerializeSArbitratorGroups(NULL, 0, &registerReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSArbitratorGroups(pReq, contLen, &registerReq);
  *pContLen = contLen;
  return pReq;
}

int32_t mndAddRegisterVgToArbitratorAction(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroup, bool registerVg,
                                           bool isRedoAction) {
  SArbObj *pArbObj = mndAcquireArbitrator(pMnode, pVgroup->arbId);
  if (!pArbObj) return -1;

  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pArbObj->pDnode->id);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildRegisterVgToArbitratorReq(pMnode, pDnode, pVgroup->arbId, pVgroup, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = registerVg ? TDMT_ARB_REGISTER_GROUPS : TDMT_ARB_UNREGISTER_GROUPS;
  action.acceptableCode = TSDB_CODE_ARB_GROUP_ALREADY_EXIST;

  if (isRedoAction) {
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  } else {
    if (mndTransAppendUndoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }

  return 0;
}

int32_t mndSetUpdateArbitratorCommitLogs(SMnode *pMnode, STrans *pTrans, SVgObj *pVgroups, int32_t numOfRegVgroups,
                                         bool registerVg) {
  SArbObj *pArbObj = mndAcquireArbitrator(pMnode, pVgroups[0].arbId);
  if (!pArbObj) return -1;

  SArbObj arbObj = {0};
  arbObj.id = pArbObj->id;
  arbObj.createdTime = pArbObj->createdTime;
  arbObj.updateTime = taosGetTimestampMs();
  arbObj.groupIds = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  void *iter = taosHashIterate(pArbObj->groupIds, NULL);
  while (iter) {
    int32_t *pGroupId = iter;
    taosHashPut(arbObj.groupIds, pGroupId, sizeof(int32_t), NULL, 0);
    iter = taosHashIterate(pArbObj->groupIds, iter);
  }

  for (int32_t i = 0; i < numOfRegVgroups; i++) {
    SVgObj *pVgroup = &pVgroups[i];
    if (registerVg) {
      taosHashPut(arbObj.groupIds, &pVgroup->vgId, sizeof(int32_t), NULL, 0);
    } else {
      taosHashRemove(arbObj.groupIds, &pVgroup->vgId, sizeof(int32_t));
    }
  }

  arbObj.numOfVgroups = taosHashGetSize(arbObj.groupIds);

  SSdbRaw *pCommitRaw = mndArbitratorActionEncode(&arbObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndProcessGetArbitratorsReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SMGetArbitratorsReq getReq = {0};
  SMGetArbitratorsRsp getRsp = {0};

  if (tDeserializeSMGetArbitratorsReq(pReq->pCont, pReq->contLen, &getReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("dnodeId:%d, start to get arbitrators", getReq.dnodeId);

  if (getReq.dnodeId <= 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  getRsp.dnodeId = getReq.dnodeId;
  getRsp.arbGroups = taosArrayInit(4, sizeof(SArbitratorGroups));

  void *pIter = NULL;
  while (1) {
    SArbObj *pArbObj = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_ARBITRATOR, pIter, (void **)&pArbObj);
    if (pIter == NULL) break;
    if (pArbObj->pDnode->id != getReq.dnodeId) {
      sdbRelease(pMnode->pSdb, pArbObj);
      continue;
    }

    SArbitratorGroups arbGroup = {0};
    arbGroup.arbId = pArbObj->id;
    arbGroup.groups = taosArrayInit(4, sizeof(SArbitratorGroupInfo));

    void *iter = taosHashIterate(pArbObj->groupIds, NULL);
    while (iter) {
      int32_t *pGroupId = iter;
      SVgObj  *pVgObj = mndAcquireVgroup(pMnode, *pGroupId);

      SArbitratorGroupInfo gInfo = {0};
      gInfo.groupId = *pGroupId;
      gInfo.replica = pVgObj->replica;
      for (int32_t j = 0; j < pVgObj->replica; j++) {
        SVnodeGid *pGroupId = &pVgObj->vnodeGid[j];
        gInfo.dnodeIds[j] = pGroupId->dnodeId;
      }
      taosArrayPush(arbGroup.groups, &gInfo);
      mndReleaseVgroup(pMnode, pVgObj);

      iter = taosHashIterate(pArbObj->groupIds, iter);
    }
    taosArrayPush(getRsp.arbGroups, &arbGroup);
  }

  int32_t contLen = tSerializeSMGetArbitratorsRsp(NULL, 0, &getRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _OVER;
  }

  tSerializeSMGetArbitratorsRsp(pRsp, contLen, &getRsp);

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

  code = 0;

_OVER:
  tFreeSMGetArbitratorsRsp(&getRsp);

  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("dnodeId:%d, failed to get since %s", getReq.dnodeId, terrstr());
  }

  return code;
}

static int32_t mndRetrieveArbitrators(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode  *pMnode = pReq->info.node;
  SSdb    *pSdb = pMnode->pSdb;
  int32_t  numOfRows = 0;
  int32_t  cols = 0;
  SArbObj *pObj = NULL;
  char    *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_ARBITRATOR, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);

    char ep[TSDB_EP_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(ep, pObj->pDnode->ep, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)ep, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createdTime, false);

    char vgroupIds[128 + VARSTR_HEADER_SIZE] = {0};
    int  size = 0;

    void *iter = taosHashIterate(pObj->groupIds, NULL);
    while (iter) {
      int32_t *vgId = iter;
      size += sprintf(vgroupIds + VARSTR_HEADER_SIZE + size, "%d,", *vgId);
      iter = taosHashIterate(pObj->groupIds, iter);
    }
    varDataSetLen(vgroupIds, size);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)vgroupIds, false);

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextArbitrator(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndGetArbitratorScore(SArbObj *pArbitrator) { return pArbitrator->numOfVgroups; }

static int32_t mndCompareDnodeArbitrators(SArbObj *pArbitrator1, SArbObj *pArbitrator2) {
  int32_t arb1Score = mndGetArbitratorScore(pArbitrator1);
  int32_t arb2Score = mndGetArbitratorScore(pArbitrator2);
  if (arb1Score == arb2Score) {
    return 0;
  }
  return arb1Score > arb2Score ? 1 : -1;
}

SArbObj *mndSortAvailableArbitrator(SArray *pArray) {
  if (taosArrayGetSize(pArray) == 0) {
    terrno = TSDB_CODE_ARB_NO_ENOUGH_NODE;
    return NULL;
  }

  mDebug("start to sort %d arbitrators", (int32_t)taosArrayGetSize(pArray));
  taosArraySort(pArray, (__compar_fn_t)mndCompareDnodeArbitrators);
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pArray); ++i) {
    SArbObj *pArbitrator = taosArrayGet(pArray, i);
    mDebug("arbitrator:%d, score:%d", pArbitrator->id, mndGetArbitratorScore(pArbitrator));
  }

  return taosArrayGet(pArray, 0);
}

int32_t mndGetArbitratorSize(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbGetSize(pSdb, SDB_ARBITRATOR);
}

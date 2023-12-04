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
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "audit.h"

#define ARBITRATOR_VER_NUMBER   1
#define ARBITRATOR_RESERVE_SIZE 64

static SSdbRaw *mndArbitratorActionEncode(SArbitratorObj *pObj);
static SSdbRow *mndArbitratorActionDecode(SSdbRaw *pRaw);
static int32_t  mndArbitratorActionInsert(SSdb *pSdb, SArbitratorObj *pObj);
static int32_t  mndArbitratorActionUpdate(SSdb *pSdb, SArbitratorObj *pOld, SArbitratorObj *pNew);
static int32_t  mndArbitratorActionDelete(SSdb *pSdb, SArbitratorObj *pObj);
static int32_t  mndProcessCreateArbitratorReq(SRpcMsg *pReq);
static int32_t  mndProcessDropArbitratorReq(SRpcMsg *pReq);
// static int32_t  mndProcessArbitratorListReq(SRpcMsg *pReq);
// static int32_t  mndRetrieveArbitrators(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
// static void     mndCancelGetNextArbitrator(SMnode *pMnode, void *pIter);

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
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_ARBITRATOR_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_ARBITRATOR_RSP, mndTransProcessRsp);
  //mndSetMsgHandle(pMnode, TDMT_MND_ARBITRATOR_LIST, mndProcessArbitratorListReq);

  // mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ARBITRATOR, mndRetrieveArbitrators);
  // mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ARBITRATOR, mndCancelGetNextArbitrator);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupArbitrator(SMnode *pMnode) {}

SArbitratorObj *mndAcquireArbitrator(SMnode *pMnode, int32_t arbitratorId) {
  SArbitratorObj *pObj = sdbAcquire(pMnode->pSdb, SDB_ARBITRATOR, &arbitratorId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_ARBITRATOR_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseArbitrator(SMnode *pMnode, SArbitratorObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndArbitratorActionEncode(SArbitratorObj *pObj) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_ARBITRATOR, ARBITRATOR_VER_NUMBER, sizeof(SArbitratorObj) + ARBITRATOR_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
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
  SSdbRow   *pRow = NULL;
  SArbitratorObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != ARBITRATOR_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SArbitratorObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
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

static int32_t mndArbitratorActionInsert(SSdb *pSdb, SArbitratorObj *pObj) {
  mTrace("arbitrator:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("arbitrator:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndArbitratorActionDelete(SSdb *pSdb, SArbitratorObj *pObj) {
  mTrace("arbitrator:%d, perform delete action, row:%p", pObj->id, pObj);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndArbitratorActionUpdate(SSdb *pSdb, SArbitratorObj *pOld, SArbitratorObj *pNew) {
  mTrace("arbitrator:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
  return 0;
}

static int32_t mndSetCreateArbitratorRedoLogs(STrans *pTrans, SArbitratorObj *pObj) {
  SSdbRaw *pRedoRaw = mndArbitratorActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateArbitratorUndoLogs(STrans *pTrans, SArbitratorObj *pObj) {
  SSdbRaw *pUndoRaw = mndArbitratorActionEncode(pObj);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

int32_t mndSetCreateArbitratorCommitLogs(STrans *pTrans, SArbitratorObj *pObj) {
  SSdbRaw *pCommitRaw = mndArbitratorActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

bool mndArbitratorInDnode(SArbitratorObj *pArbitrator, int32_t dnodeId) {
  return pArbitrator->pDnode->id == dnodeId;
}

int32_t mndSetCreateArbitratorRedoActions(STrans *pTrans, SDnodeObj *pDnode, SArbitratorObj *pObj) {
  SDCreateArbitratorReq createReq = {0};
  createReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

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

static int32_t mndSetCreateArbitratorUndoActions(STrans *pTrans, SDnodeObj *pDnode, SArbitratorObj *pObj) {
  SDDropArbitratorReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

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

  SArbitratorObj arbitratorObj = {0};
  arbitratorObj.id = pDnode->id;
  arbitratorObj.createdTime = taosGetTimestampMs();
  arbitratorObj.updateTime = arbitratorObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-arbitrator");
  if (pTrans == NULL) goto _OVER;

  mInfo("trans:%d, used to create arbitrator:%d", pTrans->id, pCreate->dnodeId);
  if (mndSetCreateArbitratorRedoLogs(pTrans, &arbitratorObj) != 0) goto _OVER;
  if (mndSetCreateArbitratorUndoLogs(pTrans, &arbitratorObj) != 0) goto _OVER;
  if (mndSetCreateArbitratorCommitLogs(pTrans, &arbitratorObj) != 0) goto _OVER;
  if (mndSetCreateArbitratorRedoActions(pTrans, pDnode, &arbitratorObj) != 0) goto _OVER;
  if (mndSetCreateArbitratorUndoActions(pTrans, pDnode, &arbitratorObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateArbitratorReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SArbitratorObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
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

static int32_t mndSetDropArbitratorRedoLogs(STrans *pTrans, SArbitratorObj *pObj) {
  SSdbRaw *pRedoRaw = mndArbitratorActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;
  return 0;
}

static int32_t mndSetDropArbitratorCommitLogs(STrans *pTrans, SArbitratorObj *pObj) {
  SSdbRaw *pCommitRaw = mndArbitratorActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetDropArbitratorRedoActions(STrans *pTrans, SDnodeObj *pDnode, SArbitratorObj *pObj) {
  SDDropArbitratorReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

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

int32_t mndSetDropArbitratorInfoToTrans(SMnode *pMnode, STrans *pTrans, SArbitratorObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  if (mndSetDropArbitratorRedoLogs(pTrans, pObj) != 0) return -1;
  if (mndSetDropArbitratorCommitLogs(pTrans, pObj) != 0) return -1;
  if (!force) {
    if (mndSetDropArbitratorRedoActions(pTrans, pObj->pDnode, pObj) != 0) return -1;
  }
  return 0;
}

static int32_t mndDropArbitrator(SMnode *pMnode, SRpcMsg *pReq, SArbitratorObj *pObj) {
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
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SArbitratorObj     *pObj = NULL;
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

// int32_t mndCreateArbitratorList(SMnode *pMnode, SArray **pList, int32_t limit) {
//   SSdb      *pSdb = pMnode->pSdb;
//   void      *pIter = NULL;
//   SArbitratorObj *pObj = NULL;
//   int32_t    numOfRows = 0;

//   SArray *arbitratorList = taosArrayInit(5, sizeof(SQueryNodeLoad));
//   if (NULL == arbitratorList) {
//     mError("failed to alloc epSet while process arbitrator list req");
//     terrno = TSDB_CODE_OUT_OF_MEMORY;
//     return terrno;
//   }

//   while (1) {
//     pIter = sdbFetch(pSdb, SDB_ARBITRATOR, pIter, (void **)&pObj);
//     if (pIter == NULL) break;

//     SQueryNodeLoad nodeLoad = {0};
//     nodeLoad.addr.nodeId = ARBITRATOR_HANDLE;
//     nodeLoad.addr.epSet.numOfEps = 1;
//     tstrncpy(nodeLoad.addr.epSet.eps[0].fqdn, pObj->pDnode->fqdn, TSDB_FQDN_LEN);
//     nodeLoad.addr.epSet.eps[0].port = pObj->pDnode->port;
//     nodeLoad.load = ARBITRATOR_LOAD_VALUE(pObj);

//     (void)taosArrayPush(arbitratorList, &nodeLoad);

//     numOfRows++;
//     sdbRelease(pSdb, pObj);

//     if (limit > 0 && numOfRows >= limit) {
//       sdbCancelFetch(pSdb, pIter);
//       break;
//     }
//   }

//   *pList = arbitratorList;

//   return TSDB_CODE_SUCCESS;
// }

// static int32_t mndProcessArbitratorListReq(SRpcMsg *pReq) {
//   int32_t       code = -1;
//   SMnode       *pMnode = pReq->info.node;
//   SArbitratorListReq qlistReq = {0};
//   SArbitratorListRsp qlistRsp = {0};

//   if (tDeserializeSArbitratorListReq(pReq->pCont, pReq->contLen, &qlistReq) != 0) {
//     mError("failed to parse arbitrator list req");
//     terrno = TSDB_CODE_INVALID_MSG;
//     goto _OVER;
//   }

//   if (mndCreateArbitratorList(pMnode, &qlistRsp.arbitratorList, qlistReq.rowNum) != 0) {
//     goto _OVER;
//   }

//   int32_t rspLen = tSerializeSArbitratorListRsp(NULL, 0, &qlistRsp);
//   void   *pRsp = rpcMallocCont(rspLen);
//   if (pRsp == NULL) {
//     terrno = TSDB_CODE_OUT_OF_MEMORY;
//     goto _OVER;
//   }

//   tSerializeSArbitratorListRsp(pRsp, rspLen, &qlistRsp);

//   pReq->info.rspLen = rspLen;
//   pReq->info.rsp = pRsp;
//   code = 0;

// _OVER:
//   tFreeSArbitratorListRsp(&qlistRsp);
//   return code;
// }

// static int32_t mndRetrieveArbitrators(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
//   SMnode    *pMnode = pReq->info.node;
//   SSdb      *pSdb = pMnode->pSdb;
//   int32_t    numOfRows = 0;
//   int32_t    cols = 0;
//   SArbitratorObj *pObj = NULL;
//   char      *pWrite;

//   while (numOfRows < rows) {
//     pShow->pIter = sdbFetch(pSdb, SDB_ARBITRATOR, pShow->pIter, (void **)&pObj);
//     if (pShow->pIter == NULL) break;

//     cols = 0;
//     SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//     colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);

//     char ep[TSDB_EP_LEN + VARSTR_HEADER_SIZE] = {0};
//     STR_WITH_MAXSIZE_TO_VARSTR(ep, pObj->pDnode->ep, pShow->pMeta->pSchemas[cols].bytes);
//     pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//     colDataSetVal(pColInfo, numOfRows, (const char *)ep, false);

//     pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
//     colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createdTime, false);

//     numOfRows++;
//     sdbRelease(pSdb, pObj);
//   }

//   pShow->numOfRows += numOfRows;

//   return numOfRows;
// }

// static void mndCancelGetNextArbitrator(SMnode *pMnode, void *pIter) {
//   SSdb *pSdb = pMnode->pSdb;
//   sdbCancelFetch(pSdb, pIter);
// }

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

#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "operator.h"
#include "os.h"
#include "querynodes.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttypes.h"
#include "hashjoin.h"

int32_t initJoinKeyBufInfo(SColBufInfo** ppInfo, int32_t* colNum, SNodeList* pList, char** ppBuf) {
  *colNum = LIST_LENGTH(pList);
  
  (*ppInfo) = taosMemoryMalloc((*colNum) * sizeof(SColBufInfo));
  if (NULL == (*ppInfo)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int64_t bufSize = 0;
  for (int32_t i = 0; i < *colNum; ++i) {
    SColumnNode* pColNode = (SColumnNode*)nodesListGetNode(pList, i);

    (*ppInfo)->slotId = pColNode->slotId;
    (*ppInfo)->vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
    (*ppInfo)->bytes = pColNode->node.resType.bytes;
    bufSize += pColNode->node.resType.bytes;
  }

  *ppBuf = taosMemoryMalloc(bufSize);
  if (NULL == *ppBuf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t initJoinTableInfo(SHJoinOperatorInfo* pJoin, SNodeList* pKeyList, int32_t idx) {
  SJoinTableInfo* pTable = &pJoin->tbs[idx];
  
  int32_t code = initJoinKeyBufInfo(&pTable->keyCols, &pTable->keyNum, pKeyList, &pTable->keyBuf);
  if (code) {
    return code;
  }
}

SOperatorInfo* createHashJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
  SHJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SHJoinOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  int32_t code = TSDB_CODE_SUCCESS;
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  int32_t      numOfCols = 0;
  pInfo->pRes = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);

  SExprInfo*   pExprInfo = createExprInfo(pJoinNode->pTargets, NULL, &numOfCols);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  setOperatorInfo(pOperator, "HashJoinOperator", QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;

  initJoinTableInfo(pJoinNode, pJoinNode->pOnLeft, 0);
  initJoinTableInfo(pJoinNode, pJoinNode->pOnRight, 1);

  code = initJoinColBufInfo(&pInfo->pRightKeyInfo, &pInfo->pRightKeyNum, pJoinNode->pOnRight, &pInfo->pRightKeyBuf);
  if (code) {
    goto _error;
  }

  memcpy(pInfo->inputStat, pJoinNode->inputStat, sizeof(pJoinNode->inputStat));

  size_t hashCap = pInfo->inputStat[1].inputRowNum > 0 ? (pInfo->inputStat[1].inputRowNum * 1.5) : 1024;
  pInfo->pKeyHash = tSimpleHashInit(hashCap, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (pInfo->pKeyHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  if (pJoinNode->pFilterConditions != NULL && pJoinNode->node.pConditions != NULL) {
    pInfo->pCondAfterJoin = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    if (pInfo->pCondAfterJoin == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }

    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)(pInfo->pCondAfterJoin);
    pLogicCond->pParameterList = nodesMakeList();
    if (pLogicCond->pParameterList == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }

    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->pFilterConditions));
    nodesListMakeAppend(&pLogicCond->pParameterList, nodesCloneNode(pJoinNode->node.pConditions));
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
  } else if (pJoinNode->pFilterConditions != NULL) {
    pInfo->pCondAfterJoin = nodesCloneNode(pJoinNode->pFilterConditions);
  } else if (pJoinNode->node.pConditions != NULL) {
    pInfo->pCondAfterJoin = nodesCloneNode(pJoinNode->node.pConditions);
  } else {
    pInfo->pCondAfterJoin = NULL;
  }

  code = filterInitFromNode(pInfo->pCondAfterJoin, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doHashJoin, NULL, destroHashJoinOperator, optrDefaultBufFn, NULL);
  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyMergeJoinOperator(pInfo);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void destroHashJoinOperator(void* param) {
  SHJoinOperatorInfo* pJoinOperator = (SHJoinOperatorInfo*)param;
  if (pJoinOperator->pColEqualOnConditions != NULL) {
    mergeJoinDestoryBuildTable(pJoinOperator->rightBuildTable);
    taosMemoryFreeClear(pJoinOperator->rightEqOnCondKeyBuf);
    taosArrayDestroy(pJoinOperator->rightEqOnCondCols);

    taosMemoryFreeClear(pJoinOperator->leftEqOnCondKeyBuf);
    taosArrayDestroy(pJoinOperator->leftEqOnCondCols);
  }
  nodesDestroyNode(pJoinOperator->pCondAfterMerge);

  taosArrayDestroy(pJoinOperator->rowCtx.leftCreatedBlocks);
  taosArrayDestroy(pJoinOperator->rowCtx.rightCreatedBlocks);
  taosArrayDestroy(pJoinOperator->rowCtx.leftRowLocations);
  taosArrayDestroy(pJoinOperator->rowCtx.rightRowLocations);

  pJoinOperator->pRes = blockDataDestroy(pJoinOperator->pRes);
  taosMemoryFreeClear(param);
}

static void doHashJoinImpl(struct SOperatorInfo* pOperator, SSDataBlock* pRes) {
  SHJoinOperatorInfo* pJoinInfo = pOperator->info;

  int32_t nrows = pRes->info.rows;

  bool asc = (pJoinInfo->inputOrder == TSDB_ORDER_ASC) ? true : false;

  while (1) {
    int64_t leftTs = 0;
    int64_t rightTs = 0;
    if (pJoinInfo->rowCtx.rowRemains) {
      leftTs = pJoinInfo->rowCtx.ts;
      rightTs = pJoinInfo->rowCtx.ts;
    } else {
      bool    hasNextTs = mergeJoinGetNextTimestamp(pOperator, &leftTs, &rightTs);
      if (!hasNextTs) {
        break;
      }
    }

    if (leftTs == rightTs) {
      mergeJoinJoinDownstreamTsRanges(pOperator, leftTs, pRes, &nrows);
    } else if ((asc && leftTs < rightTs) || (!asc && leftTs > rightTs)) {
      pJoinInfo->leftPos += 1;

      if (pJoinInfo->leftPos >= pJoinInfo->pLeft->info.rows && pRes->info.rows < pOperator->resultInfo.threshold) {
        continue;
      }
    } else if ((asc && leftTs > rightTs) || (!asc && leftTs < rightTs)) {
      pJoinInfo->rightPos += 1;
      if (pJoinInfo->rightPos >= pJoinInfo->pRight->info.rows && pRes->info.rows < pOperator->resultInfo.threshold) {
        continue;
      }
    }

    // the pDataBlock are always the same one, no need to call this again
    pRes->info.rows = nrows;
    pRes->info.dataLoad = 1;
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }
}

int32_t setColBufInfo(SSDataBlock* pBlock, int32_t colNum, SColBufInfo* pColList) {
  for (int32_t i = 0; i < colNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pColList[i].slotId);
    if (pColList[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pColList[i].slotId, pCol->info.type, pColList[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pColList[i].bytes != IS_VAR_DATA_TYPE(pCol->info.bytes))  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pColList[i].slotId, pCol->info.bytes, pColList[i].bytes);
      return TSDB_CODE_INVALID_PARA;
    }
    pColList[i].data = pCol->pData;
    if (pColList[i].vardata) {
      pColList[i].offset = pCol->varmeta.offset;
    }
  }

  return TSDB_CODE_SUCCESS;
}

FORCE_INLINE void copyColDataToBuf(int32_t colNum, int32_t rowIdx, SColBufInfo* pColList, char* pBuf, size_t *bufLen) {
  char *pData = NULL;

  *bufLen = 0;
  for (int32_t i = 0; i < colNum; ++i) {
    if (pColList[i].vardata) {
      pData = pColList[i].data + pColList[i].offset[rowIdx];
      memcpy(pBuf + *bufLen, pData, varDataTLen(pData));
      *bufLen += varDataTLen(pData);
    }
  }
}


int32_t addBlockRowsKeyToHash(SSDataBlock* pBlock, SHJoinOperatorInfo* pJoin) {
  int32_t code = setColBufInfo(pBlock, pJoin->pRightKeyNum, pJoin->pRightKeyInfo);
  if (code) {
    return code;
  }

  size_t bufLen = 0;
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    copyColDataToBuf(pJoin->pRightKeyNum, i, pJoin->pRightKeyInfo, pJoin->pRightKeyBuf, &bufLen);
  }

  tSimpleHashPut(pJoin->pKeyHash, pJoin->pRightKeyBuf, bufLen, NULL, 0);
}

int32_t buildJoinKeyHash(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SSDataBlock* pBlock = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  
  while (true) {
    pBlock = pOperator->pDownstream[1]->fpSet.getNextFn(pOperator->pDownstream[1]);
    if (NULL == pBlock) {
      break;
    }

    code = addBlockRowsKeyToHash(pBlock, pJoin);
    if (code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doHashJoin(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoinInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  int32_t code = TSDB_CODE_SUCCESS;
  SSDataBlock* pRes = pJoinInfo->pRes;
  blockDataCleanup(pRes);

  if (NULL == pJoinInfo->pKeyHash) {
    code = buildJoinKeyHash(pJoinInfo);
    if (code) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (tSimpleHashGetSize(pJoinInfo->pKeyHash) <= 0) {
      setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
      return NULL;
    }
  }

  while (true) {
    int32_t numOfRowsBefore = pRes->info.rows;
    doHashJoinImpl(pOperator, pRes);
    int32_t numOfNewRows = pRes->info.rows - numOfRowsBefore;
    if (numOfNewRows == 0) {
      break;
    }
    if (pOperator->exprSupp.pFilterInfo != NULL) {
      doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
    }
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      break;
    }
  }
  return (pRes->info.rows > 0) ? pRes : NULL;
}

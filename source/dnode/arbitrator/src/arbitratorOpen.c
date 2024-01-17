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
#include "arbitrator.h"

static void arbitratorGenerateArbToken(int32_t arbId, char *buf);

static int arbitratorEncodeDiskData(const SArbitratorDiskDate *pDiskData, char **ppData) {
  SJson *pJson;
  char  *pData;
  *ppData = NULL;

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    return -1;
  }

  if (tjsonAddIntegerToObject(pJson, "arbId", pDiskData->arbId) < 0) goto _err;

  SJson *jGroups = tjsonCreateArray();
  if (jGroups == NULL) goto _err;
  if (tjsonAddItemToObject(pJson, "groups", jGroups) < 0) goto _err;

  void *iter = taosHashIterate(pDiskData->arbGroupMap, NULL);
  while (iter != NULL) {
    SJson *jGroup = tjsonCreateObject();
    if (jGroup == NULL) goto _err;
    if (tjsonAddItemToArray(jGroups, jGroup) < 0) goto _err;
    SArbGroup *pGroup = iter;
    int32_t   *pGroupId = taosHashGetKey(iter, NULL);
    if (tjsonAddIntegerToObject(jGroup, "groupId", *pGroupId) < 0) goto _err;
    SJson *jMembers = tjsonCreateArray();
    if (jMembers == NULL) goto _err;
    if (tjsonAddItemToObject(jGroup, "members", jMembers) < 0) goto _err;
    for (int j = 0; j < 2; j++) {
      SJson *jMember = tjsonCreateObject();
      if (jMember == NULL) goto _err;
      if (tjsonAddItemToArray(jMembers, jMember) < 0) goto _err;
      SArbGroupMember *pMember = &pGroup->members[j];
      if (tjsonAddIntegerToObject(jMember, "dnodeId", pMember->info.dnodeId) < 0) goto _err;
    }
    SJson *jAssignedLeader = tjsonCreateObject();
    if (jAssignedLeader == NULL) goto _err;
    if (tjsonAddItemToObject(jGroup, "assignedLeader", jAssignedLeader) < 0) goto _err;
    if (tjsonAddIntegerToObject(jAssignedLeader, "dnodeId", pGroup->assignedLeader.dnodeId) < 0) goto _err;
    if (tjsonAddStringToObject(jAssignedLeader, "token", pGroup->assignedLeader.token) < 0) goto _err;
    iter = taosHashIterate(pDiskData->arbGroupMap, iter);
  }

  pData = tjsonToString(pJson);
  if (pData == NULL) {
    goto _err;
  }

  tjsonDelete(pJson);

  *ppData = pData;
  return 0;

_err:
  taosHashCancelIterate(pDiskData->arbGroupMap, iter);
  tjsonDelete(pJson);
  return -1;
}

static int arbitratorDecodeDiskData(uint8_t *pData, SArbitratorDiskDate *pDate) {
  SJson *pJson = NULL;

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    return -1;
  }

  if (tjsonGetIntValue(pJson, "arbId", &pDate->arbId) < 0) goto _err;

  SJson *groups = tjsonGetObjectItem(pJson, "groups");
  int    groupNum = tjsonGetArraySize(groups);
  pDate->arbGroupMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  for (int i = 0; i < groupNum; i++) {
    SJson *jGroup = tjsonGetArrayItem(groups, i);
    if (jGroup == NULL) goto _err;
    int32_t groupId = 0;
    if (tjsonGetIntValue(jGroup, "groupId", &groupId) < 0) goto _err;
    SArbGroup arbGroup = {0};
    arbitratorInitSArbGroup(&arbGroup);
    SJson *jMembers = tjsonGetObjectItem(jGroup, "members");
    for (int j = 0; j < 2; j++) {
      SJson *jMember = tjsonGetArrayItem(jMembers, j);
      if (jMember == NULL) goto _err;
      SArbGroupMember *pMember = &arbGroup.members[j];
      if (tjsonGetIntValue(jMember, "dnodeId", &pMember->info.dnodeId) < 0) goto _err;
    }
    SJson *jAssignedLeader = tjsonGetObjectItem(jGroup, "assignedLeader");
    if (tjsonGetIntValue(jAssignedLeader, "dnodeId", &arbGroup.assignedLeader.dnodeId) < 0) goto _err;
    if (tjsonGetStringValue(jAssignedLeader, "token", arbGroup.assignedLeader.token) < 0) goto _err;

    taosHashPut(pDate->arbGroupMap, &groupId, sizeof(int32_t), &arbGroup, sizeof(SArbGroup));
  }

  tjsonDelete(pJson);

  return 0;

_err:
  tjsonDelete(pJson);
  return -1;
}

static int32_t arbitratorLoadDiskData(const char *dir, SArbitratorDiskDate *pDate) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile = NULL;
  char     *pData = NULL;
  int64_t   size;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, ARB_INFO_FNAME);

  // read diskData
  pFile = taosOpenFile(fname, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  if (taosReadFile(pFile, pData, size) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pData[size] = '\0';

  taosCloseFile(&pFile);

  // decode diskData
  if (arbitratorDecodeDiskData(pData, pDate) < 0) {
    taosMemoryFree(pData);
    return -1;
  }

  taosMemoryFree(pData);

  return 0;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(pData);
  return -1;
}

static int32_t arbitratorSaveDiskData(const char *dir, SArbitratorDiskDate *pData) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile;
  char     *data;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, ARB_INFO_FNAME_TMP);

  // encode disk data
  data = NULL;

  if (arbitratorEncodeDiskData(pData, &data) < 0) {
    arbError("failed to encode json disk data.");
    return -1;
  }

  // save disk data to a arbitrator_tmp.json
  pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    arbError("failed to open disk data file:%s for write:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, data, strlen(data)) < 0) {
    arbError("failed to write disk data file:%s error:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosFsyncFile(pFile) < 0) {
    arbError("failed to fsync disk data file:%s error:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  taosCloseFile(&pFile);

  // free disk data binary
  taosMemoryFree(data);

  arbInfo("arbId:%d, arbitrator disk data is saved, fname:%s", pData->arbId, fname);

  return 0;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(data);
  return -1;
}

static int32_t arbitratorCommitDiskData(const char *dir) {
  char fname[TSDB_FILENAME_LEN];
  char tfname[TSDB_FILENAME_LEN];

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, ARB_INFO_FNAME);
  snprintf(tfname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, ARB_INFO_FNAME_TMP);

  if (taosRenameFile(tfname, fname) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  arbInfo("arbitrator disk data is committed, dir:%s", dir);
  return 0;
}

int32_t arbitratorUpdateDiskData(const char *dir, SArbitratorDiskDate *pData) {
  if (arbitratorSaveDiskData(dir, pData) < 0 || arbitratorCommitDiskData(dir) < 0) {
    arbError("arbId:%d, failed to save arbitrator config since %s", pData->arbId, tstrerror(terrno));
    return -1;
  }
  return 0;
}

void arbitratorInitSArbGroup(SArbGroup *pGroup) {
  pGroup->epIndex = 0;
  for (int i = 0; i < 2; i++) {
    pGroup->members[i].info.dnodeId = 0;
    pGroup->members[i].state.nextHbSeq = 1;
    pGroup->members[i].state.responsedHbSeq = 0;
    pGroup->members[i].state.lastHbMs = 0;
  }
  pGroup->epIndex = 0;
  pGroup->isSync = false;
  pGroup->assignedLeader.dnodeId = 0;
}

int32_t arbitratorCreate(const char *path, int32_t arbId) {
  SArbitratorDiskDate diskData = {0};
  char                dir[TSDB_FILENAME_LEN] = {0};

  // create arbitrator env
  if (taosMkDir(path)) {
    arbError("arbId:%d, failed to prepare arbitrator dir since %s, path: %s", arbId, strerror(errno), path);
    return TAOS_SYSTEM_ERROR(errno);
  }

  diskData.arbId = arbId;

  SArbitratorDiskDate oldDiskData = {0};
  oldDiskData.arbId = -1;
  if (arbitratorLoadDiskData(path, &oldDiskData) == 0) {
    arbWarn("vgId:%d, arbitrator disk data already exists at %s.", oldDiskData.arbId, path);
    return (oldDiskData.arbId == diskData.arbId) ? 0 : -1;
  }

  arbInfo("arbId:%d, save config while create", diskData.arbId);
  if (arbitratorUpdateDiskData(path, &diskData) < 0) {
    return -1;
  }

  arbInfo("arbId:%d, arbitrator is created", diskData.arbId);
  return 0;
}

void arbitratorDestroy(const char *path) {
  arbInfo("path:%s is removed while destroy arbitrator", path);
  taosRemoveDir(path);
}

static SHashObj *arbitratorInitDnodeMap(SHashObj *arbGroupMap) {
  SHashObj *arbDnodeMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  void *iter = taosHashIterate(arbGroupMap, NULL);
  while (iter) {
    SArbGroup *pArbGroup = iter;
    int32_t   *groupId = taosHashGetKey(iter, NULL);
    for (int32_t i = 0; i < 2; i++) {
      int32_t    dnodeId = pArbGroup->members[i].info.dnodeId;
      SArbDnode *pArbDnode = taosHashGet(arbDnodeMap, &dnodeId, sizeof(int32_t));
      if (!pArbDnode) {
        SArbDnode arbDnode = {0};
        arbDnode.groupIds = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
        taosHashPut(arbDnodeMap, &dnodeId, sizeof(int32_t), &arbDnode, sizeof(SArbDnode));
        pArbDnode = taosHashGet(arbDnodeMap, &dnodeId, sizeof(int32_t));
      }
      taosHashPut(pArbDnode->groupIds, &groupId, sizeof(int32_t), NULL, 0);
    }
    iter = taosHashIterate(arbGroupMap, iter);
  }

  return arbDnodeMap;
}

SArbitrator *arbitratorOpen(const char *path, SMsgCb msgCb) {
  SArbitrator        *pArbitrator = NULL;
  SArbitratorDiskDate diskDate = {0};
  char                dir[TSDB_FILENAME_LEN] = {0};
  int32_t             ret = 0;
  terrno = TSDB_CODE_SUCCESS;

  diskDate.arbId = -1;

  // load arbitrator disk data
  ret = arbitratorLoadDiskData(path, &diskDate);
  if (ret < 0) {
    arbError("failed to open arbitrator from %s since %s", path, tstrerror(terrno));
    terrno = TSDB_CODE_NEED_RETRY;
    return NULL;
  }

  // create handle
  pArbitrator = taosMemoryCalloc(1, sizeof(*pArbitrator) + strlen(path) + 1);
  if (pArbitrator == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    arbError("arbId:%d, failed to open arbitrator since %s", diskDate.arbId, tstrerror(terrno));
    return NULL;
  }

  pArbitrator->arbId = diskDate.arbId;
  pArbitrator->arbGroupMap = diskDate.arbGroupMap;
  pArbitrator->arbDnodeMap = arbitratorInitDnodeMap(pArbitrator->arbGroupMap);
  arbitratorGenerateArbToken(pArbitrator->arbId, pArbitrator->arbToken);
  pArbitrator->msgCb = msgCb;
  strcpy(pArbitrator->path, path);

  return pArbitrator;

_err:
  taosMemoryFree(pArbitrator);
  return NULL;
}

void arbitratorClose(SArbitrator *pArbitrator) {
  if (pArbitrator) {
    taosHashCleanup(pArbitrator->arbGroupMap);
    taosMemoryFree(pArbitrator);
  }
}

static void arbitratorGenerateArbToken(int32_t arbId, char *buf) {
  int32_t randVal = taosSafeRand() % 1000;
  int64_t currentMs = taosGetTimestampMs();
  sprintf(buf, "a%d#%" PRId64 "#%d", arbId, currentMs, randVal);
}

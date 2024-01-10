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

#ifndef _TD_ARBITRATOR_INT_H_
#define _TD_ARBITRATOR_INT_H_

#include "os.h"

#include "tjson.h"
#include "tlog.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "trpc.h"

#include "arbitrator.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ARB_INFO_FNAME     "arbitrator.json"
#define ARB_INFO_FNAME_TMP "arbitrator_tmp.json"

/* --------------------- SArbitratorInfo --------------------- */
typedef struct {
  int32_t   arbId;
  SHashObj *arbGroupMap;  // key: groupId, value: SArbGroup
} SArbitratorDiskDate;

/* --------------------- SArbGroup --------------------- */
typedef struct {
  int32_t dnodeId;
  char    token[TD_ARB_TOKEN_SIZE];
} SArbAssignedLeader;

typedef struct {
  int32_t dnodeId;
} SArbMemberInfo;

typedef struct {
  int32_t nextHbSeq;
  int32_t responsedHbSeq;
  char    token[TD_ARB_TOKEN_SIZE];
} SArbMemberState;

typedef struct {
  SArbMemberInfo  info;
  SArbMemberState state;
} SArbGroupMember;

typedef struct {
  SArbGroupMember    members[2];
  bool               isSync;
  SArbAssignedLeader assignedLeader;
} SArbGroup;

/* --------------------- SArbDnode --------------------- */
typedef struct {
  // int32_t   port;
  // char      fqdn[TSDB_FQDN_LEN];
  SHashObj *groupIds;  // groupId;
} SArbDnode;

/* --------------------- SArbitrator --------------------- */
struct SArbitrator {
  int32_t         arbId;
  SHashObj       *arbGroupMap;  // key: groupId, value: SArbGroup
  SHashObj       *arbDnodeMap;  // key: dnodeId, value: SArbDnode
  char            arbToken[TD_ARB_TOKEN_SIZE];
  SMsgCb          msgCb;
  char            path[];
};

int32_t arbitratorUpdateDiskData(const char *dir, SArbitratorDiskDate *pData);

#ifdef __cplusplus
}
#endif

#endif /*_TD_ARBITRATOR_INT_H_*/

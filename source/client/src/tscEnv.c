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

#include "clientInt.h"
#include "clientLog.h"
#include "os.h"
#include "taosmsg.h"
#include "tcache.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tnote.h"
#include "tref.h"
#include "trpc.h"
#include "tsched.h"
#include "ttime.h"
#include "ttimezone.h"

#define TSC_VAR_NOT_RELEASE 1
#define TSC_VAR_RELEASED    0

SAppInfo   appInfo;
int32_t    tscReqRef  = -1;
int32_t    tscConnRef = -1;
void      *tscQhandle = NULL;

static pthread_once_t tscinit = PTHREAD_ONCE_INIT;
int32_t tsNumOfThreads = 1;
volatile int32_t tscInitRes = 0;

static void registerRequest(SRequestObj* pRequest) {
  STscObj *pTscObj = (STscObj *)taosAcquireRef(tscConnRef, pRequest->pTscObj->id);
  assert(pTscObj != NULL);

  // connection has been released already, abort creating request.
  pRequest->self = taosAddRef(tscReqRef, pRequest);

  int32_t num = atomic_add_fetch_32(&pTscObj->numOfReqs, 1);

  if (pTscObj->pAppInfo) {
    SInstanceActivity *pActivity = &pTscObj->pAppInfo->summary;

    int32_t total = atomic_add_fetch_32(&pActivity->totalRequests, 1);
    int32_t currentInst = atomic_add_fetch_32(&pActivity->currentRequests, 1);
    tscDebug("0x%" PRIx64 " new Request from connObj:0x%" PRIx64 ", current:%d, app current:%d, total:%d", pRequest->self,
             pRequest->pTscObj->id, num, currentInst, total);
  }
}

static void deregisterRequest(SRequestObj* pRequest) {
  assert(pRequest != NULL);

  STscObj* pTscObj = pRequest->pTscObj;
  SInstanceActivity* pActivity = &pTscObj->pAppInfo->summary;

  int32_t currentInst = atomic_sub_fetch_32(&pActivity->currentRequests, 1);
  int32_t num = atomic_sub_fetch_32(&pTscObj->numOfReqs, 1);

  tscDebug("0x%"PRIx64" free Request from connObj: 0x%"PRIx64", current:%d, app current:%d", pRequest->self, pTscObj->id, num, currentInst);
  taosReleaseRef(tscConnRef, pTscObj->id);
}

static void tscInitLogFile() {
  taosReadGlobalLogCfg();
  if (mkdir(tsLogDir, 0755) != 0 && errno != EEXIST) {
    printf("failed to create log dir:%s\n", tsLogDir);
  }

  const char    *defaultLogFileNamePrefix = "taoslog";
  const int32_t  maxLogFileNum = 10;

  char temp[128] = {0};
  sprintf(temp, "%s/%s", tsLogDir, defaultLogFileNamePrefix);
  if (taosInitLog(temp, tsNumOfLogLines, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

void closeTransporter(STscObj* pTscObj)  {
  if (pTscObj == NULL || pTscObj->pTransporter == NULL) {
    return;
  }

  tscDebug("free transporter:%p in connObj: 0x%"PRIx64, pTscObj->pTransporter, pTscObj->id);
  rpcClose(pTscObj->pTransporter);
  pTscObj->pTransporter = NULL;
}

// TODO refactor
void* openTransporter(const char *user, const char *auth) {
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 0;
  rpcInit.label = "TSC";
  rpcInit.numOfThreads = tsNumOfThreads;
  rpcInit.cfp = processMsgFromServer;
  rpcInit.sessions = tsMaxConnections;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = (char *)user;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.ckey = "key";
//  rpcInit.spi = 1;
  rpcInit.secret = (char *)auth;

  void* pDnodeConn = rpcOpen(&rpcInit);
  if (pDnodeConn == NULL) {
    tscError("failed to init connection to server");
    return NULL;
  }

  return pDnodeConn;
}

void destroyTscObj(void *pObj) {
  STscObj *pTscObj = pObj;

  atomic_sub_fetch_64(&pTscObj->pAppInfo->numOfConns, 1);
  tscDebug("connObj 0x%"PRIx64" destroyed, totalConn:%"PRId64, pTscObj->id, pTscObj->pAppInfo->numOfConns);
  pthread_mutex_destroy(&pTscObj->mutex);
  tfree(pTscObj);
}

void* createTscObj(const char* user, const char* auth, const char *ip, uint32_t port, SAppInstInfo* pAppInfo) {
  STscObj *pObj = (STscObj *)calloc(1, sizeof(STscObj));
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pObj->pAppInfo = pAppInfo;
  if (pAppInfo != NULL) {
    pObj->pTransporter = pAppInfo->pTransporter;
  }

  tstrncpy(pObj->user, user, sizeof(pObj->user));
  memcpy(pObj->pass, auth, TSDB_PASSWORD_LEN);

  pthread_mutex_init(&pObj->mutex, NULL);
  pObj->id = taosAddRef(tscConnRef, pObj);

  tscDebug("connObj created, 0x%"PRIx64, pObj->id);
  return pObj;
}

void* createRequest(STscObj* pObj, __taos_async_fn_t fp, void* param, int32_t type) {
  assert(pObj != NULL);

  SRequestObj *pRequest = (SRequestObj *)calloc(1, sizeof(SRequestObj));
  if (NULL == pRequest) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  // TODO generated request uuid
  pRequest->requestId  = 0;

  pRequest->metric.start = taosGetTimestampMs();

  pRequest->type       = type;
  pRequest->pTscObj    = pObj;
  pRequest->body.fp    = fp;
  pRequest->body.param = param;
  pRequest->msgBuf     = calloc(1, ERROR_MSG_BUF_DEFAULT_SIZE);
  tsem_init(&pRequest->body.rspSem, 0, 0);

  registerRequest(pRequest);
  return pRequest;
}

static void doDestroyRequest(void* p) {
  assert(p != NULL);
  SRequestObj* pRequest = (SRequestObj*)p;

  assert(RID_VALID(pRequest->self));

  tfree(pRequest->msgBuf);
  tfree(pRequest->sqlstr);
  tfree(pRequest->pInfo);

  if (pRequest->body.pResInfo != NULL) {
    tfree(pRequest->body.pResInfo->pMsg);
    tfree(pRequest->body.pResInfo);
  }

  deregisterRequest(pRequest);
  tfree(pRequest);
}

void destroyRequest(SRequestObj* pRequest) {
  if (pRequest == NULL) {
    return;
  }

  taosReleaseRef(tscReqRef, pRequest->self);
}

void taos_init_imp(void) {
  // In the APIs of other program language, taos_cleanup is not available yet.
  // So, to make sure taos_cleanup will be invoked to clean up the allocated resource to suppress the valgrind warning.
  atexit(taos_cleanup);

  errno = TSDB_CODE_SUCCESS;
  srand(taosGetTimestampSec());

  deltaToUtcInitOnce();
  taosInitGlobalCfg();
  taosReadCfgFromFile();

  tscInitLogFile();
  if (taosCheckAndPrintCfg()) {
    tscInitRes = -1;
    return;
  }

  taosInitNotes();
  initMsgHandleFp();

  rpcInit();

  tscDebug("starting to initialize TAOS driver, local ep: %s", tsLocalEp);

  taosSetCoreDump(true);

  double factor = 4.0;
  int32_t numOfThreads = MAX((int)(tsNumOfCores * tsNumOfThreadsPerCore / factor), 2);

  int32_t queueSize = tsMaxConnections * 2;
  tscQhandle = taosInitScheduler(queueSize, numOfThreads, "tsc");
  if (NULL == tscQhandle) {
    tscError("failed to init task queue");
    tscInitRes = -1;
    return;
  }

  tscDebug("client task queue is initialized, numOfThreads: %d", numOfThreads);
  tscConnRef = taosOpenRef(200, destroyTscObj);
  tscReqRef  = taosOpenRef(40960, doDestroyRequest);

  taosGetAppName(appInfo.appName, NULL);
  appInfo.pid       = taosGetPId();
  appInfo.startTime = taosGetTimestampMs();
  appInfo.pInstMap  = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

  tscDebug("client is initialized successfully");
}

int taos_init() {
  pthread_once(&tscinit, taos_init_imp);
  return tscInitRes;
}

int taos_options_imp(TSDB_OPTION option, const char *str) {
  SGlobalCfg *cfg = NULL;

  switch (option) {
    case TSDB_OPTION_CONFIGDIR:
      cfg = taosGetConfigOption("configDir");
      assert(cfg != NULL);

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tstrncpy(configDir, str, TSDB_FILENAME_LEN);
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscInfo("set config file directory:%s", str);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, str, tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_SHELL_ACTIVITY_TIMER:
      cfg = taosGetConfigOption("shellActivityTimer");
      assert(cfg != NULL);

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tsShellActivityTimer = atoi(str);
        if (tsShellActivityTimer < 1) tsShellActivityTimer = 1;
        if (tsShellActivityTimer > 3600) tsShellActivityTimer = 3600;
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscInfo("set shellActivityTimer:%d", tsShellActivityTimer);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %d", cfg->option, str, tsCfgStatusStr[cfg->cfgStatus], *(int32_t *)cfg->ptr);
      }
      break;

    case TSDB_OPTION_LOCALE: {  // set locale
      cfg = taosGetConfigOption("locale");
      assert(cfg != NULL);

      size_t len = strlen(str);
      if (len == 0 || len > TSDB_LOCALE_LEN) {
        tscInfo("Invalid locale:%s, use default", str);
        return -1;
      }

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        char sep = '.';

        if (strlen(tsLocale) == 0) { // locale does not set yet
          char* defaultLocale = setlocale(LC_CTYPE, "");

          // The locale of the current OS does not be set correctly, so the default locale cannot be acquired.
          // The launch of current system will abort soon.
          if (defaultLocale == NULL) {
            tscError("failed to get default locale, please set the correct locale in current OS");
            return -1;
          }

          tstrncpy(tsLocale, defaultLocale, TSDB_LOCALE_LEN);
        }

        // set the user specified locale
        char *locale = setlocale(LC_CTYPE, str);

        if (locale != NULL) { // failed to set the user specified locale
          tscInfo("locale set, prev locale:%s, new locale:%s", tsLocale, locale);
          cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        } else { // set the user specified locale failed, use default LC_CTYPE as current locale
          locale = setlocale(LC_CTYPE, tsLocale);
          tscInfo("failed to set locale:%s, current locale:%s", str, tsLocale);
        }

        tstrncpy(tsLocale, locale, TSDB_LOCALE_LEN);

        char *charset = strrchr(tsLocale, sep);
        if (charset != NULL) {
          charset += 1;

          charset = taosCharsetReplace(charset);

          if (taosValidateEncodec(charset)) {
            if (strlen(tsCharset) == 0) {
              tscInfo("charset set:%s", charset);
            } else {
              tscInfo("charset changed from %s to %s", tsCharset, charset);
            }

            tstrncpy(tsCharset, charset, TSDB_LOCALE_LEN);
            cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;

          } else {
            tscInfo("charset:%s is not valid in locale, charset remains:%s", charset, tsCharset);
          }

          free(charset);
        } else { // it may be windows system
          tscInfo("charset remains:%s", tsCharset);
        }
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, str, tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;
    }

    case TSDB_OPTION_CHARSET: {
      /* set charset will override the value of charset, assigned during system locale changed */
      cfg = taosGetConfigOption("charset");
      assert(cfg != NULL);

      size_t len = strlen(str);
      if (len == 0 || len > TSDB_LOCALE_LEN) {
        tscInfo("failed to set charset:%s", str);
        return -1;
      }

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        if (taosValidateEncodec(str)) {
          if (strlen(tsCharset) == 0) {
            tscInfo("charset is set:%s", str);
          } else {
            tscInfo("charset changed from %s to %s", tsCharset, str);
          }

          tstrncpy(tsCharset, str, TSDB_LOCALE_LEN);
          cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        } else {
          tscInfo("charset:%s not valid", str);
        }
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, str, tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }

      break;
    }

    case TSDB_OPTION_TIMEZONE:
      cfg = taosGetConfigOption("timezone");
      assert(cfg != NULL);

      if (cfg->cfgStatus <= TAOS_CFG_CSTATUS_OPTION) {
        tstrncpy(tsTimezone, str, TSDB_TIMEZONE_LEN);
        tsSetTimeZone();
        cfg->cfgStatus = TAOS_CFG_CSTATUS_OPTION;
        tscDebug("timezone set:%s, input:%s by taos_options", tsTimezone, str);
      } else {
        tscWarn("config option:%s, input value:%s, is configured by %s, use %s", cfg->option, str, tsCfgStatusStr[cfg->cfgStatus], (char *)cfg->ptr);
      }
      break;

    default:
      // TODO return the correct error code to client in the format for taos_errstr()
      tscError("Invalid option %d", option);
      return -1;
  }

  return 0;
}

#if 0
#include "cJSON.h"
static setConfRet taos_set_config_imp(const char *config){
  setConfRet ret = {SET_CONF_RET_SUCC, {0}};
  static bool setConfFlag = false;
  if (setConfFlag) {
    ret.retCode = SET_CONF_RET_ERR_ONLY_ONCE;
    strcpy(ret.retMsg, "configuration can only set once");
    return ret;
  }
  taosInitGlobalCfg();
  cJSON *root = cJSON_Parse(config);
  if (root == NULL){
    ret.retCode = SET_CONF_RET_ERR_JSON_PARSE;
    strcpy(ret.retMsg, "parse json error");
    return ret;
  }

  int size = cJSON_GetArraySize(root);
  if(!cJSON_IsObject(root) || size == 0) {
    ret.retCode = SET_CONF_RET_ERR_JSON_INVALID;
    strcpy(ret.retMsg, "json content is invalid, must be not empty object");
    return ret;
  }

  if(size >= 1000) {
    ret.retCode = SET_CONF_RET_ERR_TOO_LONG;
    strcpy(ret.retMsg, "json object size is too long");
    return ret;
  }

  for(int i = 0; i < size; i++){
    cJSON *item = cJSON_GetArrayItem(root, i);
    if(!item) {
      ret.retCode = SET_CONF_RET_ERR_INNER;
      strcpy(ret.retMsg, "inner error");
      return ret;
    }
    if(!taosReadConfigOption(item->string, item->valuestring, NULL, NULL, TAOS_CFG_CSTATUS_OPTION, TSDB_CFG_CTYPE_B_CLIENT)){
      ret.retCode = SET_CONF_RET_ERR_PART;
      if (strlen(ret.retMsg) == 0){
        snprintf(ret.retMsg, RET_MSG_LENGTH, "part error|%s", item->string);
      }else{
        int tmp = RET_MSG_LENGTH - 1 - (int)strlen(ret.retMsg);
        size_t leftSize = tmp >= 0 ? tmp : 0;
        strncat(ret.retMsg, "|",  leftSize);
        tmp = RET_MSG_LENGTH - 1 - (int)strlen(ret.retMsg);
        leftSize = tmp >= 0 ? tmp : 0;
        strncat(ret.retMsg, item->string, leftSize);
      }
    }
  }
  cJSON_Delete(root);
  setConfFlag = true;
  return ret;
}

setConfRet taos_set_config(const char *config){
  pthread_mutex_lock(&setConfMutex);
  setConfRet ret = taos_set_config_imp(config);
  pthread_mutex_unlock(&setConfMutex);
  return ret;
}
#endif
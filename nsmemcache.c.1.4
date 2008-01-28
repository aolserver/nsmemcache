/*
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) 2001-2006 Vlad Seryakov
 * All rights reserved.
 *
 * Alternatively, the contents of this file may be used under the terms
 * of the GNU General Public License (the "GPL"), in which case the
 * provisions of GPL are applicable instead of those above.  If you wish
 * to allow use of your version of this file only under the terms of the
 * GPL and not to allow others to use your version of this file under the
 * License, indicate your decision by deleting the provisions above and
 * replace them with the notice and other provisions required by the GPL.
 * If you do not delete the provisions above, a recipient may use your
 * version of this file under either the License or the GPL.
 */

/*
 * nsmemcache.c -- memcached client
 *
 *
 * Authors
 *
 *     Vlad Seryakov vlad@crystalballinc.com
 */

#include "ns.h"

#define BUFSIZE               2048       /* Internal buffer size for read/write */
#define TIMEOUT               2          /* For how long to wait for responses */
#define DEADTIME              5          /* Period to ping dead servers */

#define PORT                  11211

#define MC_SERVER_LIVE        0          /* Server is alive and responding to requests */
#define MC_SERVER_DEAD        1          /* Server is not responding to requests */
#define MC_SERVER_DELETED     2          /* Server is not used anymore */

typedef struct mc_conn_t
{
    struct mc_conn_t *next;
    int sock;
    Ns_DString ds;
    struct mc_server_t *ms;
} mc_conn_t;

typedef struct mc_server_t
{
    char *host;                          /*  Hostname of this server */
    int port;                            /*  Port of this server */
    int status;                          /*  Server status */
    int timeout;
    mc_conn_t *conns;                    /*  List of actual client connections */
    Ns_Mutex lock;
    time_t deadtime;
} mc_server_t;

typedef struct mc_t
{
    uint16_t nalloc;                      /* Number of servers allocated */
    uint16_t ntotal;                      /* Number of servers added */
    mc_server_t **servers;                /* Array of servers */
    Ns_RWLock lock;
} mc_t;

static uint32_t mc_hash(const char* data, uint32_t data_len);
static mc_t *mc_create(uint16_t max_servers, uint32_t flags);
static mc_server_t *mc_server_create(char *host, int port, uint32_t timeout);
static mc_server_t * mc_server_find(mc_t *mc, char* host, int port);
static int mc_server_add(mc_t *mc, mc_server_t *ms);
static void mc_server_delete(mc_t *mc, mc_server_t *ms);
static int mc_get(mc_t *mc, char* key, char **data, size_t *length, uint16_t *flags);
static int mc_set(mc_t *mc, char* cmd, char* key, char *data, uint32_t data_size, uint32_t timeout, uint16_t flags);
static int mc_areplace(mc_t *mc, char* key, char *data, uint32_t data_size, uint32_t timeout, uint16_t flags,
                       char **data2, size_t *length2, uint16_t *flags2);
static int mc_delete(mc_t *mc, char* key, uint32_t timeout);
static int mc_incr(mc_t *mc, char *cmd, char* key, int32_t n, uint32_t *new_value);
static int mc_version(mc_t *mc, mc_server_t *ms,  char **data);
static int MCInterpInit(Tcl_Interp * interp, void *arg);
static int MCCmd(ClientData arg, Tcl_Interp * interp, int objc, Tcl_Obj * CONST objv[]);

/* The crc32 functions and data was originally written by Spencer
 * Garrett <srg@quick.com> and was cleaned from the PostgreSQL source
 * tree via the files contrib/ltree/crc32.[ch].  No license was
 * included, therefore it is assumed that this code is public
 * domain. */

static const uint32_t crc32tab[256] = {
  0x00000000, 0x77073096, 0xee0e612c, 0x990951ba,
  0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
  0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
  0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
  0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de,
  0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
  0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec,
  0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
  0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
  0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
  0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940,
  0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
  0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116,
  0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
  0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
  0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
  0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a,
  0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
  0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818,
  0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
  0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
  0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
  0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c,
  0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
  0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2,
  0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
  0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
  0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
  0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086,
  0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
  0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4,
  0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
  0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
  0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
  0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8,
  0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
  0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe,
  0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
  0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
  0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
  0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252,
  0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
  0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60,
  0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
  0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
  0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
  0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04,
  0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
  0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a,
  0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
  0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
  0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
  0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e,
  0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
  0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c,
  0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
  0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
  0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
  0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0,
  0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
  0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6,
  0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
  0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
  0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d,
};

NS_EXPORT int Ns_ModuleInit(char *server, char *module)
{
    int i;
    char *key, *path;
    mc_t *mc;
    Ns_Set *set;

    mc = mc_create(16, 0);

    path = Ns_ConfigGetPath(server, module, NULL);
    set = Ns_ConfigGetSection(path);
    for (i = 0; set != NULL && i < Ns_SetSize(set); ++i) {
        key = Ns_SetKey(set, i);
        if (!strcasecmp(key, "server")) {
            key = Ns_SetValue(set, i);
            mc_server_add(mc, mc_server_create(key, 0, 0));
            Ns_Log(Notice, "nsmemcache: added %s", key);
        }
    }
    Ns_TclRegisterTrace(server, MCInterpInit, mc, NS_TCL_TRACE_CREATE);
    return NS_OK;
}

static int MCInterpInit(Tcl_Interp * interp, void *arg)
{
    Tcl_CreateObjCommand(interp, "ns_memcache", MCCmd, arg, NULL);
    return NS_OK;
}

static int MCCmd(ClientData arg, Tcl_Interp * interp, int objc, Tcl_Obj * CONST objv[])
{
    mc_t *mc = arg;
    mc_server_t *ms;
    char *key, *data;
    uint16_t flags = 0;
    uint32_t size, expires = 0;
    int cmd;

    enum {
        cmdGet, cmdAdd, cmdAppend, cmdSet, cmdReplace, cmdAReplace, cmdDelete, cmdIncr, cmdDecr, cmdVersion,
        cmdServer
    };

    static CONST char *sCmd[] = {
        "get", "add", "append", "set", "replace", "areplace", "delete", "incr", "decr", "version",
        "server",
        0
    };

    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "cmd args");
        return TCL_ERROR;
    }
    if (Tcl_GetIndexFromObj(interp, objv[1], sCmd, "command", TCL_EXACT, (int *) &cmd) != TCL_OK) {
        return TCL_ERROR;
    }

    switch (cmd) {
    case cmdServer:
       if (objc < 4) {
           Tcl_WrongNumArgs(interp, 2, objv, "cmd server");
           return TCL_ERROR;
       }
       if (!strcmp(Tcl_GetString(objv[2]), "add")) {
           mc_server_add(mc, mc_server_create(Tcl_GetString(objv[3]), 0, 0));
       } else
       if (!strcmp(Tcl_GetString(objv[2]), "delete")) {
           mc_server_delete(mc, mc_server_find(mc, Tcl_GetString(objv[3]), 0));
       }
       break;

    case cmdGet:
       if (objc < 4) {
           Tcl_WrongNumArgs(interp, 2, objv, "key dataVar ?lengthVar? ?flagsVar?");
           return TCL_ERROR;
       }
       key = Tcl_GetString(objv[2]);
       cmd = mc_get(mc, key, &data, &size, &flags);
       if (cmd == 1) {
           Tcl_SetVar2Ex(interp, Tcl_GetString(objv[3]), NULL, Tcl_NewByteArrayObj((uint8_t*)data, size), 0);
           if (objc > 4) {
             Tcl_SetVar2Ex(interp, Tcl_GetString(objv[4]), NULL, Tcl_NewLongObj(size), 0);
           }
           if (objc > 5) {
             Tcl_SetVar2Ex(interp, Tcl_GetString(objv[5]), NULL, Tcl_NewIntObj(flags), 0);
           }
       }
       Tcl_SetObjResult(interp, Tcl_NewIntObj(cmd));
       break;

    case cmdAdd:
    case cmdSet:
    case cmdAppend:
    case cmdReplace:
       if (objc < 4) {
           Tcl_WrongNumArgs(interp, 2, objv, "key value ?expires? ?flags?");
           return TCL_ERROR;
       }
       key = Tcl_GetString(objv[2]);
       data = (char*)Tcl_GetByteArrayFromObj(objv[3], (int*)&size);
       if (objc > 4) {
           expires = atoi(Tcl_GetString(objv[4]));
       }
       if (objc > 5) {
           flags = atoi(Tcl_GetString(objv[5]));
       }
       switch (cmd) {
       case cmdAdd:
           cmd = mc_set(mc, "add", key, data, size, expires, flags);
           break;
       case cmdAppend:
           cmd = mc_set(mc, "append", key, data, size, expires, flags);
           break;
       case cmdSet:
           cmd = mc_set(mc, "set", key, data, size, expires, flags);
           break;
       case cmdReplace:
           cmd = mc_set(mc, "replace", key, data, size, expires, flags);
           break;
       }
       Tcl_SetObjResult(interp, Tcl_NewIntObj(cmd));
       break;

    case cmdAReplace: {
       char *data2, *outVar = NULL, *outSize = NULL, *outFlags = NULL;
       u_int32_t size2 = 0;
       u_int16_t flags2 = 0;

       Ns_ObjvSpec opts[] = {
          {"-expires",  Ns_ObjvInt,    &expires, NULL},
          {"-flags",    Ns_ObjvInt,    &flags,   NULL},
          {"-outsize",  Ns_ObjvString, &outSize,  NULL},
          {"-outflags", Ns_ObjvString, &outFlags, NULL},
          {"-outvar",   Ns_ObjvString, &outVar,   NULL},
          {"--",        Ns_ObjvBreak,  NULL,     NULL},
          {NULL, NULL, NULL, NULL}
       };
       Ns_ObjvSpec args[] = {
          {"key",     Ns_ObjvString,  &key,     NULL},
          {"value",   Ns_ObjvString,  &data,    &size},
          {NULL, NULL, NULL, NULL}
       };

       if (Ns_ParseObjv(opts, args, interp, 2, objc, objv) != NS_OK) {
           return TCL_ERROR;
       }
       cmd = mc_areplace(mc, key, data, size, expires, flags, &data2, &size2, &flags2);
       if (cmd == 1) {
           if (outVar != NULL) {
               Tcl_SetVar2Ex(interp, outVar, NULL, Tcl_NewByteArrayObj((uint8_t*)data2, size2), 0);
           }
           if (outSize != NULL) {
             Tcl_SetVar2Ex(interp, outSize, NULL, Tcl_NewLongObj(size2), 0);
           }
           if (outFlags != NULL) {
             Tcl_SetVar2Ex(interp, outFlags, NULL, Tcl_NewIntObj(flags2), 0);
           }
       }
       Tcl_SetObjResult(interp, Tcl_NewIntObj(cmd));
       break;
    }

    case cmdDelete:
       if (objc < 3) {
           Tcl_WrongNumArgs(interp, 2, objv, "key ?expires?");
           return TCL_ERROR;
       }
       key = Tcl_GetString(objv[2]);
       if (objc > 3) {
           expires = atoi(Tcl_GetString(objv[3]));
       }
       cmd = mc_delete(mc, key, expires);
       Tcl_SetObjResult(interp, Tcl_NewIntObj(cmd));
       break;

    case cmdIncr:
    case cmdDecr:
       if (objc < 4) {
           Tcl_WrongNumArgs(interp, 2, objv, "key value ?varname?");
           return TCL_ERROR;
       }
       key = Tcl_GetString(objv[2]);
       size = atoi(Tcl_GetString(objv[3]));
       switch (cmd) {
       case cmdIncr:
           cmd = mc_incr(mc, "incr", key, size, &size);
           break;
       case cmdDecr:
           cmd = mc_incr(mc, "decr", key, size, &size);
           break;
       }
       if (cmd == 1 && objc > 4) {
           Tcl_SetVar2Ex(interp, Tcl_GetString(objv[4]), NULL, Tcl_NewLongObj(size), 0);
       }
       Tcl_SetObjResult(interp, Tcl_NewIntObj(cmd));
       break;

    case cmdVersion:
       if (mc->ntotal == 0) {
           break;
       }
       ms = mc->servers[0];
       if (objc > 2) {
           ms = mc_server_find(mc, Tcl_GetString(objv[2]), PORT);
       }
       if (ms != NULL) {
           data = NULL;
           mc_version(mc, ms, &data);
           if (data != NULL) {
               Tcl_SetObjResult(interp, Tcl_NewStringObj(data, -1));
           }
       }
       break;

    }
    return TCL_OK;
}

static uint32_t mc_hash(const char* data, const uint32_t data_len)
{
    uint32_t i;
    uint32_t crc;
    crc = ~0;

    for (i = 0; i < data_len; i++)
        crc = (crc >> 8) ^ crc32tab[(crc ^ (data[i])) & 0xff];

    return ((~crc >> 16) & 0x7fff);
}

static mc_conn_t *mc_conn_create(mc_server_t *ms)
{
    int sock;
    mc_conn_t *conn;
    Ns_Time timeout = { TIMEOUT, 0 };

    sock = Ns_SockTimedConnect((char*)ms->host, ms->port, &timeout);
    if (sock == -1) {
        Ns_Log(Error, "nsmemcache: unable to connect to %s:%d", ms->host, ms->port);
        return NULL;
    }
    Ns_SockSetNonBlocking(sock);
    conn = ns_malloc(sizeof(mc_conn_t));
    Ns_DStringInit(&conn->ds);
    conn->sock = sock;
    conn->ms = ms;
    return conn;
}

static void mc_conn_free(mc_conn_t *conn)
{
    if (conn != NULL) {
        close(conn->sock);
        Ns_DStringFree(&conn->ds);
        ns_free(conn);
    }
}

static mc_conn_t *mc_conn_get(mc_server_t * ms)
{
    mc_conn_t *conn;

    Ns_MutexLock(&ms->lock);
    conn = ms->conns;
    if (ms->conns) {
        ms->conns = ms->conns->next;
    }
    Ns_MutexUnlock(&ms->lock);
    if (conn == NULL) {
        return mc_conn_create(ms);
    }
    Ns_DStringSetLength(&conn->ds, 0);
    return conn;
}

static void mc_conn_put(mc_conn_t * conn)
{
    mc_server_t *ms = conn->ms;

    Ns_MutexLock(&ms->lock);
    conn->next = ms->conns;
    ms->conns = conn;
    Ns_MutexUnlock(&ms->lock);
}

static int mc_conn_read(mc_conn_t * conn, int len, int reset, char **line)
{
    int nread, n, i;
    Ns_Time timeout = { conn->ms->timeout, 0 };

    if (reset) {
        Ns_DStringSetLength(&conn->ds, 0);
    }
    nread = conn->ds.length;
    Ns_DStringSetLength(&conn->ds, nread + len);
    while (len > 0) {
        n = Ns_SockRecv(conn->sock, conn->ds.string + nread, len, &timeout);
        if (n <= 0) {
            break;
        }
        // Store pointer of the second line which is data in case of get command
        if (line) {
            for (i = nread; i < n; i++) {
                if (conn->ds.string[nread + i] == '\n') {
                    *line = &conn->ds.string[nread + i + 1];
                    len = 0;
                    break;
                }
            }
        }
        nread += n;
        len -= n;
    }
    Ns_DStringSetLength(&conn->ds, nread);
    return nread;
}

static mc_server_t *mc_server_create(char* host, int port, uint32_t timeout)
{
    mc_server_t *server = ns_calloc(1, sizeof(mc_server_t));
    server->host = ns_strdup(host);
    server->port = port ? port : PORT;
    server->timeout = timeout ? timeout : TIMEOUT;
    server->status = MC_SERVER_LIVE;
    return server;
}

static void mc_server_dead(mc_t * mc, mc_server_t * ms)
{
    Ns_MutexLock(&ms->lock);
    ms->status = MC_SERVER_DEAD;
    ms->deadtime = time(0);
    Ns_MutexUnlock(&ms->lock);
    Ns_Log(Notice, "nsmemcache: server %s:%d is dead", ms->host, ms->port);
}

static int mc_server_add(mc_t *mc, mc_server_t *ms)
{
    if (ms == NULL) {
        return -1;
    }
    Ns_RWLockWrLock(&mc->lock);
    if (mc->ntotal >= mc->nalloc) {
        mc->nalloc += 8;
        mc->servers = ns_realloc(mc->servers, mc->nalloc);
    }
    mc->servers[mc->ntotal++] = ms;
    Ns_RWLockUnlock(&mc->lock);
    return 0;
}

static void mc_server_delete(mc_t *mc, mc_server_t *ms)
{
    int i;
    mc_conn_t *next;

    if (ms == NULL) {
        return;
    }
    Ns_RWLockWrLock(&mc->lock);
    for (i = 0; i < mc->ntotal; i++) {
        if (mc->servers[i] == ms) {
            Ns_MutexLock(&ms->lock);
            ms->status = MC_SERVER_DELETED;
            while (ms->conns) {
                next = ms->conns->next;
                mc_conn_free(ms->conns);
                ms->conns = next;
            }
            Ns_MutexUnlock(&ms->lock);
            break;
        }
    }
    Ns_RWLockUnlock(&mc->lock);
}

static mc_server_t * mc_server_find_hash(mc_t *mc, const uint32_t hash)
{
    mc_server_t *ms, *srv = NULL;
    uint32_t h = hash, i = 0;
    time_t now = time(0);
    int rc;

    Ns_RWLockRdLock(&mc->lock);
    while (srv == NULL && i < mc->ntotal) {
        ms = mc->servers[h % mc->ntotal];
        Ns_MutexLock(&ms->lock);
        switch (ms->status) {
        case MC_SERVER_LIVE:
           srv = ms;
           break;

        case MC_SERVER_DEAD:
           /* Try the the dead server */
           if (now - ms->deadtime > DEADTIME) {
               Ns_MutexUnlock(&ms->lock);
               rc = mc_version(mc, ms, NULL);
               Ns_MutexLock(&ms->lock);
               if (rc == 1) {
                   ms->status = MC_SERVER_LIVE;
                   srv = ms;
                   break;
               }
           }
           break;
        }
        Ns_MutexUnlock(&ms->lock);
        h++;
        i++;
    }
    Ns_RWLockUnlock(&mc->lock);
    if (srv == NULL) {
        Ns_Log(Error, "nsmemcache: no servers found");
    }
    return srv;
}

static mc_server_t * mc_server_find(mc_t *mc, char* host, int port)
{
    int i;
    mc_server_t *ms = NULL;

    Ns_RWLockRdLock(&mc->lock);
    for (i = 0; i < mc->ntotal; i++) {
        if (strcmp(mc->servers[i]->host, host) == 0
            && mc->servers[i]->port == port) {
            ms = mc->servers[i];
            break;
        }
    }
    Ns_RWLockUnlock(&mc->lock);
    return ms;
}

static mc_t *mc_create(uint16_t max_servers, uint32_t flags)
{
    mc_t *mc = ns_calloc(1, sizeof(mc_t));
    mc->nalloc = max_servers;
    mc->ntotal = 0;
    mc->servers = ns_calloc(mc->nalloc, sizeof(struct mc_server_t *));
    return mc;
}

static int mc_set(mc_t *mc, char* cmd, char* key, char *data, uint32_t data_size, uint32_t timeout, uint16_t flags)
{
    int rc;
    char *line;
    uint32_t hash;
    mc_server_t* ms;
    mc_conn_t* conn;
    struct iovec vec[3];
    Ns_Time wait = { 0, 0 };

    hash = mc_hash(key, strlen(key));
    ms = mc_server_find_hash(mc, hash);
    if (ms == NULL) {
        return -1;
    }
    conn = mc_conn_get(ms);
    if (conn == NULL) {
        mc_server_dead(mc, ms);
        return -1;
    }

    /* <command name> <key> <flags> <exptime> <bytes>\r\n<data>\r\n */

    Ns_DStringPrintf(&conn->ds, "%s %s %u %u %u\r\n", cmd, key, flags, timeout, data_size);

    vec[0].iov_base = conn->ds.string;
    vec[0].iov_len  = conn->ds.length;

    vec[1].iov_base = data;
    vec[1].iov_len  = data_size;

    vec[2].iov_base = "\r\n";
    vec[2].iov_len  = 2;

    wait.sec = ms->timeout;
    rc = Ns_SockWriteV(conn->sock, vec, 3, &wait);
    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }

    rc = mc_conn_read(conn, BUFSIZE, 1, &line);
    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }
    mc_conn_put(conn);

    if (strcmp(conn->ds.string, "STORED\r\n") == 0) {
        return 1;
    } else
    if (strcmp(conn->ds.string, "NOT_STORED\r\n") == 0) {
        return 0;
    } else {
        Ns_Log(Error, "nsmemcache: unknown response from %s:%d: %s", ms->host, ms->port, conn->ds.string);
        return -1;
    }
}

static int mc_get(mc_t *mc, char* key, char **data, size_t *length, uint16_t *flags)
{
    int rc;
    uint32_t hash;
    mc_server_t* ms;
    mc_conn_t* conn;
    char *line = NULL;
    const char *ptr;
    size_t total, len = 0;
    Ns_Time wait = { 0, 0 };

    hash = mc_hash(key, strlen(key));
    ms = mc_server_find_hash(mc, hash);
    if (ms == NULL) {
        return -1;
    }
    conn = mc_conn_get(ms);
    if (conn == NULL) {
        mc_server_dead(mc, ms);
        return -1;
    }

    /* get <key>[ <key>[...]]\r\n */

    Ns_DStringPrintf(&conn->ds, "get %s\r\n", key);

    wait.sec = ms->timeout;
    rc = Ns_SockWrite(conn->sock, conn->ds.string, conn->ds.length, &wait);

    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }

    /* VALUE <key> <flags> <bytes> [cas unique]\r\n<data block>\r\nEND\r\n */

    rc = mc_conn_read(conn, BUFSIZE, 1, &line);
    if (rc <= 0 || line == NULL) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }

    if (strncmp(conn->ds.string, "VALUE ", 6) == 0) {

        // Seek to key
        ptr = Ns_NextWord(conn->ds.string);
        // Seek to flags
        ptr = Ns_NextWord(ptr);
        if (flags) {
            *flags = atoi(ptr);
        }

        // Seek to bytes
        ptr = Ns_NextWord(ptr);
        len = atoi(ptr);
        if (length) {
            *length = len;
        }

        if (len < 0)  {
            mc_conn_put(conn);
            return -1;
        }

        // Calculate total response buffer size: header + data + footer
        total = (line - conn->ds.string) + len + 7;
        if (rc < total) {
            rc = mc_conn_read(conn, total - rc, 0, 0);
        }
        if (rc < total) {
            mc_conn_free(conn);
            return -1;
        }
        *data = ns_malloc(len + 1);
        strncpy(*data, line, len);

        mc_conn_put(conn);
        return 1;
    }
    mc_conn_put(conn);

    if (strncmp(conn->ds.string, "END\r\n", 5) == 0) {
        return 0;
    } else {
        Ns_Log(Error, "nsmemcache: unknown response from %s:%d: %s", ms->host, ms->port, conn->ds.string);
        return -1;
    }
}

static int mc_delete(mc_t *mc, char* key, uint32_t timeout)
{
    int rc;
    char *line;
    mc_server_t* ms;
    mc_conn_t* conn;
    uint32_t hash;
    Ns_Time wait = { 0, 0 };

    hash = mc_hash(key, strlen(key));
    ms = mc_server_find_hash(mc, hash);
    if (ms == NULL) {
        return -1;
    }
    conn = mc_conn_get(ms);
    if (conn == NULL) {
        mc_server_dead(mc, ms);
        return -1;
    }

    /* delete <key> <time>\r\n */

    Ns_DStringPrintf(&conn->ds, "delete %s %u\r\n", key, timeout);

    wait.sec = ms->timeout;
    rc = Ns_SockWrite(conn->sock, conn->ds.string, conn->ds.length, &wait);

    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }

    rc = mc_conn_read(conn, BUFSIZE, 1, &line);
    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }
    mc_conn_put(conn);

    if (strcmp(conn->ds.string, "DELETED\r\n") == 0) {
        return 1;
    } else
    if (strcmp(conn->ds.string, "NOT_FOUND\r\n") == 0) {
        return 0;
    } else {
        Ns_Log(Error, "nsmemcache: unknown response from %s:%d: %s", ms->host, ms->port, conn->ds.string);
        return -1;
    }
}

static int mc_incr(mc_t *mc, char* cmd, char* key, const int32_t inc, uint32_t *new_value)
{
    int rc;
    char *line;
    mc_server_t* ms;
    mc_conn_t* conn;
    uint32_t hash;
    Ns_Time wait = { 0, 0 };

    hash = mc_hash(key, strlen(key));
    ms = mc_server_find_hash(mc, hash);
    if (ms == NULL) {
        return -1;
    }
    conn = mc_conn_get(ms);
    if (conn == NULL) {
        mc_server_dead(mc, ms);
        return -1;
    }

    /* <cmd> <key> <value>\r\n */

    Ns_DStringPrintf(&conn->ds, "%s %s %u\r\n", cmd, key, inc);

    wait.sec = ms->timeout;
    rc = Ns_SockWrite(conn->sock, conn->ds.string, conn->ds.length, &wait);

    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }

    /*  <value>\r\n */

    rc = mc_conn_read(conn, BUFSIZE, 1, &line);
    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }
    mc_conn_put(conn);

    if (strcmp(conn->ds.string, "ERROR\r\n") == 0) {
        return -1;
    } else
    if (strcmp(conn->ds.string, "NOT_FOUND\r\n") == 0) {
        return 0;
    } else {
        if (new_value) {
            *new_value = atoi(conn->ds.string);
        }
        return 1;
    }
}

static int mc_version(mc_t *mc, mc_server_t *ms, char **data)
{
    int rc;
    char *line;
    mc_conn_t* conn;
    Ns_Time wait = { 0, 0 };

    conn = mc_conn_get(ms);
    if (conn == NULL) {
        mc_server_dead(mc, ms);
        return -1;
    }

    /* version\r\n */

    Ns_DStringPrintf(&conn->ds, "version\r\n");

    wait.sec = ms->timeout;
    rc = Ns_SockWrite(conn->sock, conn->ds.string, conn->ds.length, &wait);

    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }

    /* VERSION <version>\r\n */

    rc = mc_conn_read(conn, BUFSIZE, 1, &line);
    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }
    mc_conn_put(conn);

    if (strncmp(conn->ds.string, "VERSION ", 8) == 0) {
        if (data) {
            *data = ns_strdup(Ns_StrTrimRight(conn->ds.string+8));
        }
        return 1;
    } else {
        return 0;
    }
}

static int mc_areplace(mc_t *mc, char* key, char *data, uint32_t data_size, uint32_t timeout, uint16_t flags, char **data2, size_t *length2, uint16_t *flags2)
{
    int rc, len;
    uint32_t hash;
    char *line, *ptr;
    mc_server_t* ms;
    mc_conn_t* conn;
    struct iovec vec[3];
    Ns_Time wait = { 0, 0 };

    hash = mc_hash(key, strlen(key));
    ms = mc_server_find_hash(mc, hash);
    if (ms == NULL) {
        return -1;
    }
    conn = mc_conn_get(ms);
    if (conn == NULL) {
        mc_server_dead(mc, ms);
        return -1;
    }

    /* areplace <key> <flags> <exptime> <bytes>\r\n<data>\r\n */

    Ns_DStringPrintf(&conn->ds, "areplace %s %u %u %u\r\n", key, flags, timeout, data_size);

    vec[0].iov_base = conn->ds.string;
    vec[0].iov_len  = conn->ds.length;

    vec[1].iov_base = data;
    vec[1].iov_len  = data_size;

    vec[2].iov_base = "\r\n";
    vec[2].iov_len  = 2;

    wait.sec = ms->timeout;
    rc = Ns_SockWriteV(conn->sock, vec, 3, &wait);
    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }

    rc = mc_conn_read(conn, BUFSIZE, 1, &line);
    if (rc <= 0) {
        mc_conn_free(conn);
        mc_server_dead(mc, ms);
        return -1;
    }
    mc_conn_put(conn);

    if (strcmp(conn->ds.string, "NOT_STORED\r\n") == 0) {
        return 0;
    } else

    if (strncmp(conn->ds.string, "VALUE ", 6) == 0) {
        ptr = ns_strtok(conn->ds.string," ");
        ptr = ns_strtok(NULL," ");
        ptr = ns_strtok(NULL," ");

        if (flags2) {
            *flags2 = atoi(ptr);
        }
        ptr = ns_strtok(NULL," ");
        if (ptr) {
            len = atoi(ptr);
        }
        if (len < 0)  {
            mc_conn_put(conn);
            return -1;
        }
        if (length2) {
            *length2 = len;
        }
        if (*line) {
            memmove(conn->ds.string, line, rc - (line - conn->ds.string));
            Ns_DStringSetLength(&conn->ds, rc - (line - conn->ds.string));
        } else {
            Ns_DStringSetLength(&conn->ds, 0);
        }
        rc = mc_conn_read(conn, len - conn->ds.length + 7, 0, 0);
        if (rc < len) {
            mc_conn_free(conn);
            return -1;
        }
        *data2 = Ns_DStringExport(&conn->ds);

        /*
         * Cut the data buffer to correct size and move the rest back to conn
         */

        ptr = (*data2) + len;
        Ns_DStringAppend(&conn->ds, ptr);
        (*data2)[len] = 0;

        // Read trailing \r\n and END\r\n
        if (strncmp(conn->ds.string, "\r\n", 2)) {
            rc = mc_conn_read(conn, BUFSIZE, 0, &line);
        }
        if (strstr(conn->ds.string, "END\r\n") == NULL) {
          rc = mc_conn_read(conn, BUFSIZE, 0, &line);
        }
        mc_conn_put(conn);
        return 1;
    }

    if (strncmp(conn->ds.string, "END\r\n", 5) == 0) {
        return 0;
    } else {
        Ns_Log(Error, "nsmemcache: unknown response from %s:%d: %s", ms->host, ms->port, conn->ds.string);
        return -1;
    }
}

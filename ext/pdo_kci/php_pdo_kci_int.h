/*
  +----------------------------------------------------------------------+
  | Copyright (c) The PHP Group                                          |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Wez Furlong <wez@php.net>                                    |
  +----------------------------------------------------------------------+
*/

#include <dci.h>

typedef struct {
	const char *file;
	int line;
	sb4 errcode;
	char *errmsg;
} pdo_kci_error_info;

/* stuff we use in an DCI database handle */
typedef struct {
	DCIServer	*server;
	DCISession	*session;
	DCIEnv		*env;
	DCIError	*err;
	DCISvcCtx	*svc;
	/* DCI9; 0 == use NLS_LANG */
	ub4			prefetch;
	ub2			charset;
	sword		last_err;
	sb4			max_char_width;

	unsigned	attached:1;
	unsigned	_reserved:31;

	pdo_dci_error_info einfo;
} pdo_dci_db_handle;

typedef struct {
	DCIDefine	*def;
	ub2			fetched_len;
	ub2			retcode;
	sb2			indicator;

	char *data;
	ub4 datalen;

	ub2 dtype;

} pdo_dci_column;

typedef struct {
	pdo_dci_db_handle *H;
	DCIStmt		*stmt;
	DCIError	*err;
	sword		last_err;
	ub2			stmt_type;
	ub4			exec_type;
	pdo_dci_column *cols;
	pdo_dci_error_info einfo;
	unsigned int have_blobs:1;
} pdo_dci_stmt;

typedef struct {
	DCIBind		*bind;	/* allocated by DCI */
	sb2			dci_type;
	sb2			indicator;
	ub2			retcode;

	ub4			actual_len;

	dvoid		*thing;	/* for LOBS, REFCURSORS etc. */

	unsigned used_for_output;
} pdo_dci_bound_param;

extern const ub4 PDO_DCI_INIT_MODE;
extern const pdo_driver_t pdo_dci_driver;
extern DCIEnv *pdo_dci_Env;

ub4 _dci_error(DCIError *err, pdo_dbh_t *dbh, pdo_stmt_t *stmt, char *what, sword status, int isinit, const char *file, int line);
#define dci_init_error(w)	_dci_error(H->err, dbh, NULL, w, H->last_err, TRUE, __FILE__, __LINE__)
#define dci_drv_error(w)	_dci_error(H->err, dbh, NULL, w, H->last_err, FALSE, __FILE__, __LINE__)
#define dci_stmt_error(w)	_dci_error(S->err, stmt->dbh, stmt, w, S->last_err, FALSE, __FILE__, __LINE__)

extern const struct pdo_stmt_methods dci_stmt_methods;

/* Default prefetch size in number of rows */
#define PDO_DCI_PREFETCH_DEFAULT 100

/* Arbitrary assumed row length for prefetch memory limit calcuation */
#define PDO_DCI_PREFETCH_ROWSIZE 1024


enum {
	PDO_DCI_ATTR_ACTION = PDO_ATTR_DRIVER_SPECIFIC,
	PDO_DCI_ATTR_CLIENT_INFO,
	PDO_DCI_ATTR_CLIENT_IDENTIFIER,
	PDO_DCI_ATTR_MODULE,
	PDO_DCI_ATTR_CALL_TIMEOUT
};

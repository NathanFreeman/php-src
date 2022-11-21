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
  | Author: NathanFreeman <mariasocute@163.com>                          |
  +----------------------------------------------------------------------+
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "pdo/php_pdo.h"
#include "pdo/php_pdo_driver.h"
#include "php_pdo_kci.h"
#include "php_pdo_kci_int.h"
#include "Zend/zend_exceptions.h"

static inline ub4 pdo_kci_sanitize_prefetch(long prefetch);

static int pdo_kci_fetch_error_func(pdo_dbh_t *dbh, pdo_stmt_t *stmt, zval *info) /* {{{ */
{
	pdo_kci_db_handle *H = (pdo_kci_db_handle *)dbh->driver_data;
	pdo_kci_error_info *einfo;

	einfo = &H->einfo;

	if (stmt) {
		pdo_kci_stmt *S = (pdo_kci_stmt*)stmt->driver_data;

		if (S->einfo.errmsg) {
			einfo = &S->einfo;
		}
	}

	if (einfo->errcode) {
		add_next_index_long(info, einfo->errcode);
		add_next_index_string(info, einfo->errmsg);
	}

	return 1;
}
/* }}} */

ub4 _kci_error(DCIError *err, pdo_dbh_t *dbh, pdo_stmt_t *stmt, char *what, sword status, int isinit, const char *file, int line) /* {{{ */
{
	text errbuf[1024] = "<<Unknown>>";
	char tmp_buf[2048];
	pdo_kci_db_handle *H = (pdo_kci_db_handle *)dbh->driver_data;
	pdo_kci_error_info *einfo;
	pdo_kci_stmt *S = NULL;
	pdo_error_type *pdo_err = &dbh->error_code;

	if (stmt) {
		S = (pdo_kci_stmt*)stmt->driver_data;
		einfo = &S->einfo;
		pdo_err = &stmt->error_code;
	}
	else {
		einfo = &H->einfo;
	}

	if (einfo->errmsg) {
		pefree(einfo->errmsg, dbh->is_persistent);
	}

	einfo->errmsg = NULL;
	einfo->errcode = 0;
	einfo->file = file;
	einfo->line = line;

	if (isinit) { /* Initialization error */
		strcpy(*pdo_err, "HY000");
		slprintf(tmp_buf, sizeof(tmp_buf), "%s (%s:%d)", what, file, line);
		einfo->errmsg = pestrdup(tmp_buf, dbh->is_persistent);
	}
	else {
		switch (status) {
			case DCI_SUCCESS:
				strcpy(*pdo_err, "00000");
				break;
			case DCI_ERROR:
				DCIErrorGet(err, (ub4)1, NULL, &einfo->errcode, errbuf, (ub4)sizeof(errbuf), DCI_HTYPE_ERROR);
				slprintf(tmp_buf, sizeof(tmp_buf), "%s: %s (%s:%d)", what, errbuf, file, line);
				einfo->errmsg = pestrdup(tmp_buf, dbh->is_persistent);
				break;
			case DCI_SUCCESS_WITH_INFO:
				DCIErrorGet(err, (ub4)1, NULL, &einfo->errcode, errbuf, (ub4)sizeof(errbuf), DCI_HTYPE_ERROR);
				slprintf(tmp_buf, sizeof(tmp_buf), "%s: DCI_SUCCESS_WITH_INFO: %s (%s:%d)", what, errbuf, file, line);
				einfo->errmsg = pestrdup(tmp_buf, dbh->is_persistent);
				break;
			case DCI_NEED_DATA:
				slprintf(tmp_buf, sizeof(tmp_buf), "%s: DCI_NEED_DATA (%s:%d)", what, file, line);
				einfo->errmsg = pestrdup(tmp_buf, dbh->is_persistent);
				break;
			case DCI_NO_DATA:
				slprintf(tmp_buf, sizeof(tmp_buf), "%s: DCI_NO_DATA (%s:%d)", what, file, line);
				einfo->errmsg = pestrdup(tmp_buf, dbh->is_persistent);
				break;
			case DCI_INVALID_HANDLE:
				slprintf(tmp_buf, sizeof(tmp_buf), "%s: DCI_INVALID_HANDLE (%s:%d)", what, file, line);
				einfo->errmsg = pestrdup(tmp_buf, dbh->is_persistent);
				break;
			case DCI_STILL_EXECUTING:
				slprintf(tmp_buf, sizeof(tmp_buf), "%s: DCI_STILL_EXECUTING (%s:%d)", what, file, line);
				einfo->errmsg = pestrdup(tmp_buf, dbh->is_persistent);
				break;
			case DCI_CONTINUE:
				slprintf(tmp_buf, sizeof(tmp_buf), "%s: DCI_CONTINUE (%s:%d)", what, file, line);
				einfo->errmsg = pestrdup(tmp_buf, dbh->is_persistent);
				break;
		}

		if (einfo->errcode) {
			switch (einfo->errcode) {
				case 1013:	/* user requested cancel of current operation */
					zend_bailout();
					break;

				case 12154:	/* ORA-12154: TNS:could not resolve service name */
					strcpy(*pdo_err, "42S02");
					break;

				case	22:	/* ORA-00022: invalid session id */
				case   378:
				case   602:
				case   603:
				case   604:
				case   609:
				case  1012:	/* ORA-01012: */
				case  1033:
				case  1041:
				case  1043:
				case  1089:
				case  1090:
				case  1092:
				case  3113:	/* ORA-03133: end of file on communication channel */
				case  3114:
				case  3122:
				case  3135:
				case 12153:
				case 27146:
				case 28511:
					/* consider the connection closed */
					dbh->is_closed = 1;
					H->attached = 0;
					strcpy(*pdo_err, "01002"); /* FIXME */
					break;

				default:
					strcpy(*pdo_err, "HY000");
			}
		}

		if (stmt) {
			/* always propagate the error code back up to the dbh,
			 * so that we can catch the error information when execute
			 * is called via query.  See Bug #33707 */
			if (H->einfo.errmsg) {
				pefree(H->einfo.errmsg, dbh->is_persistent);
			}
			H->einfo = *einfo;
			H->einfo.errmsg = einfo->errmsg ? pestrdup(einfo->errmsg, dbh->is_persistent) : NULL;
			strcpy(dbh->error_code, stmt->error_code);
		}
	}

	/* little mini hack so that we can use this code from the dbh ctor */
	if (!dbh->methods) {
		zend_throw_exception_ex(php_pdo_get_exception(), einfo->errcode, "SQLSTATE[%s]: %s", *pdo_err, einfo->errmsg);
	}

	return einfo->errcode;
}
/* }}} */

static int dci_handle_closer(pdo_dbh_t *dbh) /* {{{ */
{
	pdo_dci_db_handle *H = (pdo_dci_db_handle *)dbh->driver_data;

	if (H->svc) {
		/* rollback any outstanding work */
		DCITransRollback(H->svc, H->err, 0);
	}

	if (H->session) {
		DCIHandleFree(H->session, DCI_HTYPE_SESSION);
		H->session = NULL;
	}

	if (H->svc) {
		DCIHandleFree(H->svc, DCI_HTYPE_SVCCTX);
		H->svc = NULL;
	}

	if (H->server && H->attached) {
		H->last_err = DCIServerDetach(H->server, H->err, DCI_DEFAULT);
		if (H->last_err) {
			dci_drv_error("DCIServerDetach");
		}
		H->attached = 0;
	}

	if (H->server) {
		DCIHandleFree(H->server, DCI_HTYPE_SERVER);
		H->server = NULL;
	}

	if (H->err) {
		DCIHandleFree(H->err, DCI_HTYPE_ERROR);
		H->err = NULL;
	}

	if (H->charset && H->env) {
		DCIHandleFree(H->env, DCI_HTYPE_ENV);
		H->env = NULL;
	}

	if (H->einfo.errmsg) {
		pefree(H->einfo.errmsg, dbh->is_persistent);
		H->einfo.errmsg = NULL;
	}

	pefree(H, dbh->is_persistent);

	return 0;
}
/* }}} */

static int dci_handle_preparer(pdo_dbh_t *dbh, const char *sql, size_t sql_len, pdo_stmt_t *stmt, zval *driver_options) /* {{{ */
{
	pdo_dci_db_handle *H = (pdo_dci_db_handle *)dbh->driver_data;
	pdo_dci_stmt *S = ecalloc(1, sizeof(*S));
	ub4 prefetch;
	char *nsql = NULL;
	size_t nsql_len = 0;
	int ret;

#ifdef HAVE_DCISTMTFETCH2
	S->exec_type = pdo_attr_lval(driver_options, PDO_ATTR_CURSOR,
		PDO_CURSOR_FWDONLY) == PDO_CURSOR_SCROLL ?
		DCI_STMT_SCROLLABLE_READONLY : DCI_DEFAULT;
#else
	S->exec_type = DCI_DEFAULT;
#endif

	S->H = H;
	stmt->supports_placeholders = PDO_PLACEHOLDER_NAMED;
	ret = pdo_parse_params(stmt, (char*)sql, sql_len, &nsql, &nsql_len);

	if (ret == 1) {
		/* query was re-written */
		sql = nsql;
		sql_len = nsql_len;
	} else if (ret == -1) {
		/* couldn't grok it */
		strcpy(dbh->error_code, stmt->error_code);
		efree(S);
		return 0;
	}

	/* create an DCI statement handle */
	DCIHandleAlloc(H->env, (dvoid*)&S->stmt, DCI_HTYPE_STMT, 0, NULL);

	/* and our own private error handle */
	DCIHandleAlloc(H->env, (dvoid*)&S->err, DCI_HTYPE_ERROR, 0, NULL);

	if (sql_len) {
		H->last_err = DCIStmtPrepare(S->stmt, H->err, (text*)sql, (ub4) sql_len, DCI_NTV_SYNTAX, DCI_DEFAULT);
		if (nsql) {
			efree(nsql);
			nsql = NULL;
		}
		if (H->last_err) {
			H->last_err = dci_drv_error("DCIStmtPrepare");
			DCIHandleFree(S->stmt, DCI_HTYPE_STMT);
			DCIHandleFree(S->err, DCI_HTYPE_ERROR);
			efree(S);
			return 0;
		}

	}

	prefetch = H->prefetch;  /* Note 0 is allowed so in future REF CURSORs can be used & then passed with no row loss*/
	H->last_err = DCIAttrSet(S->stmt, DCI_HTYPE_STMT, &prefetch, 0,
							 DCI_ATTR_PREFETCH_ROWS, H->err);
	if (!H->last_err) {
		prefetch *= PDO_DCI_PREFETCH_ROWSIZE;
		H->last_err = DCIAttrSet(S->stmt, DCI_HTYPE_STMT, &prefetch, 0,
								 DCI_ATTR_PREFETCH_MEMORY, H->err);
	}

	stmt->driver_data = S;
	stmt->methods = &dci_stmt_methods;
	if (nsql) {
		efree(nsql);
		nsql = NULL;
	}

	return 1;
}
/* }}} */

static zend_long dci_handle_doer(pdo_dbh_t *dbh, const char *sql, size_t sql_len) /* {{{ */
{
	pdo_dci_db_handle *H = (pdo_dci_db_handle *)dbh->driver_data;
	DCIStmt		*stmt;
	ub2 stmt_type;
	ub4 rowcount;
	int ret = -1;

	DCIHandleAlloc(H->env, (dvoid*)&stmt, DCI_HTYPE_STMT, 0, NULL);

	H->last_err = DCIStmtPrepare(stmt, H->err, (text*)sql, (ub4) sql_len, DCI_NTV_SYNTAX, DCI_DEFAULT);
	if (H->last_err) {
		H->last_err = dci_drv_error("DCIStmtPrepare");
		DCIHandleFree(stmt, DCI_HTYPE_STMT);
		return -1;
	}

	H->last_err = DCIAttrGet(stmt, DCI_HTYPE_STMT, &stmt_type, 0, DCI_ATTR_STMT_TYPE, H->err);

	if (stmt_type == DCI_STMT_SELECT) {
		/* invalid usage; cancel it */
		DCIHandleFree(stmt, DCI_HTYPE_STMT);
		php_error_docref(NULL, E_WARNING, "issuing a SELECT query here is invalid");
		return -1;
	}

	/* now we are good to go */
	H->last_err = DCIStmtExecute(H->svc, stmt, H->err, 1, 0, NULL, NULL,
			(dbh->auto_commit && !dbh->in_txn) ? DCI_COMMIT_ON_SUCCESS : DCI_DEFAULT);

	if (H->last_err) {
		H->last_err = dci_drv_error("DCIStmtExecute");
	} else {
		/* return the number of affected rows */
		H->last_err = DCIAttrGet(stmt, DCI_HTYPE_STMT, &rowcount, 0, DCI_ATTR_ROW_COUNT, H->err);
		ret = rowcount;
	}

	DCIHandleFree(stmt, DCI_HTYPE_STMT);

	return ret;
}
/* }}} */

static int dci_handle_quoter(pdo_dbh_t *dbh, const char *unquoted, size_t unquotedlen, char **quoted, size_t *quotedlen, enum pdo_param_type paramtype ) /* {{{ */
{
	int qcount = 0;
	char const *cu, *l, *r;
	char *c;

	if (!unquotedlen) {
		*quotedlen = 2;
		*quoted = emalloc(*quotedlen+1);
		strcpy(*quoted, "''");
		return 1;
	}

	/* count single quotes */
	for (cu = unquoted; (cu = strchr(cu,'\'')); qcount++, cu++)
		; /* empty loop */

	*quotedlen = unquotedlen + qcount + 2;
	*quoted = c = emalloc(*quotedlen+1);
	*c++ = '\'';

	/* foreach (chunk that ends in a quote) */
	for (l = unquoted; (r = strchr(l,'\'')); l = r+1) {
		strncpy(c, l, r-l+1);
		c += (r-l+1);
		*c++ = '\'';			/* add second quote */
	}

    /* Copy remainder and add enclosing quote */
	strncpy(c, l, *quotedlen-(c-*quoted)-1);
	(*quoted)[*quotedlen-1] = '\'';
	(*quoted)[*quotedlen]   = '\0';

	return 1;
}
/* }}} */

static int dci_handle_begin(pdo_dbh_t *dbh) /* {{{ */
{
	/* with Oracle, there is nothing special to be done */
	return 1;
}
/* }}} */

static int dci_handle_commit(pdo_dbh_t *dbh) /* {{{ */
{
	pdo_dci_db_handle *H = (pdo_dci_db_handle *)dbh->driver_data;

	H->last_err = DCITransCommit(H->svc, H->err, 0);

	if (H->last_err) {
		H->last_err = dci_drv_error("DCITransCommit");
		return 0;
	}
	return 1;
}
/* }}} */

static int dci_handle_rollback(pdo_dbh_t *dbh) /* {{{ */
{
	pdo_dci_db_handle *H = (pdo_dci_db_handle *)dbh->driver_data;

	H->last_err = DCITransRollback(H->svc, H->err, 0);

	if (H->last_err) {
		H->last_err = dci_drv_error("DCITransRollback");
		return 0;
	}
	return 1;
}
/* }}} */

static int dci_handle_set_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val) /* {{{ */
{
	zend_long lval = zval_get_long(val);
	pdo_dci_db_handle *H = (pdo_dci_db_handle *)dbh->driver_data;

	switch (attr) {
		case PDO_ATTR_AUTOCOMMIT:
		{
			if (dbh->in_txn) {
				/* Assume they want to commit whatever is outstanding */
				H->last_err = DCITransCommit(H->svc, H->err, 0);

				if (H->last_err) {
					H->last_err = dci_drv_error("DCITransCommit");
					return 0;
				}
				dbh->in_txn = 0;
			}

			dbh->auto_commit = (unsigned int)lval? 1 : 0;
			return 1;
		}
		case PDO_ATTR_PREFETCH:
		{
			H->prefetch = pdo_dci_sanitize_prefetch(lval);
			return 1;
		}
		case PDO_DCI_ATTR_ACTION:
		{
#if (DCI_MAJOR_VERSION >= 10)
			zend_string *action = zval_try_get_string(val);
			if (UNEXPECTED(!action)) {
				return 0;
			}

			H->last_err = DCIAttrSet(H->session, DCI_HTYPE_SESSION,
				(dvoid *) ZSTR_VAL(action), (ub4) ZSTR_LEN(action),
				DCI_ATTR_ACTION, H->err);
			if (H->last_err) {
				dci_drv_error("DCIAttrSet: DCI_ATTR_ACTION");
				return 0;
			}
			return 1;
#else
			dci_drv_error("Unsupported attribute type");
			return 0;
#endif
		}
		case PDO_DCI_ATTR_CLIENT_INFO:
		{
#if (DCI_MAJOR_VERSION >= 10)
			zend_string *client_info = zval_try_get_string(val);
			if (UNEXPECTED(!client_info)) {
				return 0;
			}

			H->last_err = DCIAttrSet(H->session, DCI_HTYPE_SESSION,
				(dvoid *) ZSTR_VAL(client_info), (ub4) ZSTR_LEN(client_info),
				DCI_ATTR_CLIENT_INFO, H->err);
			if (H->last_err) {
				dci_drv_error("DCIAttrSet: DCI_ATTR_CLIENT_INFO");
				return 0;
			}
			return 1;
#else
			dci_drv_error("Unsupported attribute type");
			return 0;
#endif
		}
		case PDO_DCI_ATTR_CLIENT_IDENTIFIER:
		{
#if (DCI_MAJOR_VERSION >= 10)
			zend_string *identifier = zval_try_get_string(val);
			if (UNEXPECTED(!identifier)) {
				return 0;
			}

			H->last_err = DCIAttrSet(H->session, DCI_HTYPE_SESSION,
				(dvoid *) ZSTR_VAL(identifier), (ub4) ZSTR_LEN(identifier),
				DCI_ATTR_CLIENT_IDENTIFIER, H->err);
			if (H->last_err) {
				dci_drv_error("DCIAttrSet: DCI_ATTR_CLIENT_IDENTIFIER");
				return 0;
			}
			return 1;
#else
			dci_drv_error("Unsupported attribute type");
			return 0;
#endif
		}
		case PDO_DCI_ATTR_MODULE:
		{
#if (DCI_MAJOR_VERSION >= 10)
			zend_string *module = zval_try_get_string(val);
			if (UNEXPECTED(!module)) {
				return 0;
			}

			H->last_err = DCIAttrSet(H->session, DCI_HTYPE_SESSION,
				(dvoid *) ZSTR_VAL(module), (ub4) ZSTR_LEN(module),
				DCI_ATTR_MODULE, H->err);
			if (H->last_err) {
				dci_drv_error("DCIAttrSet: DCI_ATTR_MODULE");
				return 0;
			}
			return 1;
#else
			dci_drv_error("Unsupported attribute type");
			return 0;
#endif
		}
		case PDO_DCI_ATTR_CALL_TIMEOUT:
		{
#if (DCI_MAJOR_VERSION >= 18)
			ub4 timeout = (ub4) lval;

			H->last_err = DCIAttrSet(H->svc, DCI_HTYPE_SVCCTX,
				(dvoid *) &timeout, (ub4) 0,
				DCI_ATTR_CALL_TIMEOUT, H->err);
			if (H->last_err) {
				dci_drv_error("DCIAttrSet: DCI_ATTR_CALL_TIMEOUT");
				return 0;
			}
			return 1;
#else
			dci_drv_error("Unsupported attribute type");
			return 0;
#endif
		}
		default:
			return 0;
	}

}
/* }}} */

static int dci_handle_get_attribute(pdo_dbh_t *dbh, zend_long attr, zval *return_value)  /* {{{ */
{
	pdo_dci_db_handle *H = (pdo_dci_db_handle *)dbh->driver_data;

	switch (attr) {
		case PDO_ATTR_SERVER_VERSION:
		case PDO_ATTR_SERVER_INFO:
		{
			text infostr[512];
			char verstr[15];
			ub4  vernum;

			if (DCIServerRelease(H->svc, H->err, infostr, (ub4)sizeof(infostr), (ub1)DCI_HTYPE_SVCCTX, &vernum))
			{
				ZVAL_STRING(return_value, "<<Unknown>>");
			} else {
				if (attr == PDO_ATTR_SERVER_INFO) {
					ZVAL_STRING(return_value, (char *)infostr);
				} else {
					slprintf(verstr, sizeof(verstr), "%d.%d.%d.%d.%d",
							 (int)((vernum>>24) & 0xFF),  /* version number */
							 (int)((vernum>>20) & 0x0F),  /* release number*/
							 (int)((vernum>>12) & 0xFF),  /* update number */
							 (int)((vernum>>8)  & 0x0F),  /* port release number */
							 (int)((vernum>>0)  & 0xFF)); /* port update number */

					ZVAL_STRING(return_value, verstr);
				}
			}
			return TRUE;
		}

		case PDO_ATTR_CLIENT_VERSION:
		{
#if DCI_MAJOR_VERSION > 10 || (DCI_MAJOR_VERSION == 10 && DCI_MINOR_VERSION >= 2)
			/* Run time client version */
			sword major, minor, update, patch, port_update;
			char verstr[15];

			DCIClientVersion(&major, &minor, &update, &patch, &port_update);
			slprintf(verstr, sizeof(verstr), "%d.%d.%d.%d.%d", major, minor, update, patch, port_update);
			ZVAL_STRING(return_value, verstr);
#elif defined(PHP_PDO_DCI_CLIENT_VERSION)
			/* Compile time client version */
			ZVAL_STRING(return_value, PHP_PDO_DCI_CLIENT_VERSION);
#else
			return FALSE;

#endif /* Check for DCIClientVersion() support */

			return TRUE;
		}

		case PDO_ATTR_AUTOCOMMIT:
			ZVAL_BOOL(return_value, dbh->auto_commit);
			return TRUE;

		case PDO_ATTR_PREFETCH:
			ZVAL_LONG(return_value, H->prefetch);
			return TRUE;
		case PDO_DCI_ATTR_CALL_TIMEOUT:
		{
#if (DCI_MAJOR_VERSION >= 18)
			ub4 timeout;

			H->last_err = DCIAttrGet(H->svc, DCI_HTYPE_SVCCTX,
				(dvoid *) &timeout, NULL,
				DCI_ATTR_CALL_TIMEOUT, H->err);
			if (H->last_err) {
				dci_drv_error("DCIAttrGet: DCI_ATTR_CALL_TIMEOUT");
				return FALSE;
			}

			ZVAL_LONG(return_value, (zend_long) timeout);
			return TRUE;
#else
			dci_drv_error("Unsupported attribute type");
			return FALSE;
#endif
		}
		default:
			return FALSE;

	}
	return FALSE;

}
/* }}} */

static int pdo_dci_check_liveness(pdo_dbh_t *dbh) /* {{{ */
{
	pdo_dci_db_handle *H = (pdo_dci_db_handle *)dbh->driver_data;
	sb4 error_code = 0;
#if (!((DCI_MAJOR_VERSION > 10) || ((DCI_MAJOR_VERSION == 10) && (DCI_MINOR_VERSION >= 2))))
	char version[256];
#endif

	/* TODO move attached check to PDO level */
	if (H->attached == 0) {
		return FAILURE;
	}
	/* TODO add persistent_timeout check at PDO level */


	/* Use DCIPing instead of DCIServerVersion. If DCIPing returns ORA-1010 (invalid DCI operation)
	 * such as from Pre-10.1 servers, the error is still from the server and we would have
	 * successfully performed a roundtrip and validated the connection. Use DCIServerVersion for
	 * Pre-10.2 clients
	 */
#if ((DCI_MAJOR_VERSION > 10) || ((DCI_MAJOR_VERSION == 10) && (DCI_MINOR_VERSION >= 2)))	/* DCIPing available 10.2 onwards */
	H->last_err = DCIPing (H->svc, H->err, DCI_DEFAULT);
#else
	/* use good old DCIServerVersion() */
	H->last_err = DCIServerVersion (H->svc, H->err, (text *)version, sizeof(version), DCI_HTYPE_SVCCTX);
#endif
	if (H->last_err == DCI_SUCCESS) {
		return SUCCESS;
	}

	DCIErrorGet (H->err, (ub4)1, NULL, &error_code, NULL, 0, DCI_HTYPE_ERROR);

	if (error_code == 1010) {
		return SUCCESS;
	}
	return FAILURE;
}
/* }}} */

static const struct pdo_dbh_methods dci_methods = {
	dci_handle_closer,
	dci_handle_preparer,
	dci_handle_doer,
	dci_handle_quoter,
	dci_handle_begin,
	dci_handle_commit,
	dci_handle_rollback,
	dci_handle_set_attribute,
	NULL,
	pdo_dci_fetch_error_func,
	dci_handle_get_attribute,
	pdo_dci_check_liveness,	/* check_liveness */
	NULL,	/* get_driver_methods */
	NULL,
	NULL
};

static int pdo_dci_handle_factory(pdo_dbh_t *dbh, zval *driver_options) /* {{{ */
{
	pdo_dci_db_handle *H;
	int i, ret = 0;
	struct pdo_data_src_parser vars[] = {
		{ "charset",  NULL,	0 },
		{ "dbname",   "",	0 },
		{ "user",     NULL, 0 },
		{ "password", NULL, 0 }
	};

	php_pdo_parse_data_source(dbh->data_source, dbh->data_source_len, vars, 4);

	H = pecalloc(1, sizeof(*H), dbh->is_persistent);
	dbh->driver_data = H;

	dbh->skip_param_evt =
		1 << PDO_PARAM_EVT_FETCH_PRE |
		1 << PDO_PARAM_EVT_FETCH_POST |
		1 << PDO_PARAM_EVT_NORMALIZE;

	H->prefetch = PDO_DCI_PREFETCH_DEFAULT;

	/* allocate an environment */
#ifdef HAVE_DCIENVNLSCREATE
	if (vars[0].optval) {
		H->charset = DCINlsCharSetNameToId(pdo_dci_Env, (const oratext *)vars[0].optval);
		if (!H->charset) {
			dci_init_error("DCINlsCharSetNameToId: unknown character set name");
			goto cleanup;
		} else {
			if (DCIEnvNlsCreate(&H->env, PDO_DCI_INIT_MODE, 0, NULL, NULL, NULL, 0, NULL, H->charset, H->charset) != DCI_SUCCESS) {
				dci_init_error("DCIEnvNlsCreate: Check the character set is valid and that PHP has access to Oracle libraries and NLS data");
				goto cleanup;
			}
		}
	}
#endif
	if (H->env == NULL) {
		/* use the global environment */
		H->env = pdo_dci_Env;
	}

	/* something to hold errors */
	DCIHandleAlloc(H->env, (dvoid **)&H->err, DCI_HTYPE_ERROR, 0, NULL);

	/* handle for the server */
	DCIHandleAlloc(H->env, (dvoid **)&H->server, DCI_HTYPE_SERVER, 0, NULL);

	H->last_err = DCIServerAttach(H->server, H->err, (text*)vars[1].optval,
		   	(sb4) strlen(vars[1].optval), DCI_DEFAULT);

	if (H->last_err) {
		dci_drv_error("pdo_dci_handle_factory");
		goto cleanup;
	}

	H->attached = 1;

	/* create a service context */
	H->last_err = DCIHandleAlloc(H->env, (dvoid**)&H->svc, DCI_HTYPE_SVCCTX, 0, NULL);
	if (H->last_err) {
		dci_drv_error("DCIHandleAlloc: DCI_HTYPE_SVCCTX");
		goto cleanup;
	}

	H->last_err = DCIHandleAlloc(H->env, (dvoid**)&H->session, DCI_HTYPE_SESSION, 0, NULL);
	if (H->last_err) {
		dci_drv_error("DCIHandleAlloc: DCI_HTYPE_SESSION");
		goto cleanup;
	}

	/* set server handle into service handle */
	H->last_err = DCIAttrSet(H->svc, DCI_HTYPE_SVCCTX, H->server, 0, DCI_ATTR_SERVER, H->err);
	if (H->last_err) {
		dci_drv_error("DCIAttrSet: DCI_ATTR_SERVER");
		goto cleanup;
	}

	/* username */
	if (!dbh->username && vars[2].optval) {
		dbh->username = pestrdup(vars[2].optval, dbh->is_persistent);
	}

	if (dbh->username) {
		H->last_err = DCIAttrSet(H->session, DCI_HTYPE_SESSION,
			   	dbh->username, (ub4) strlen(dbh->username),
				DCI_ATTR_USERNAME, H->err);
		if (H->last_err) {
			dci_drv_error("DCIAttrSet: DCI_ATTR_USERNAME");
			goto cleanup;
		}
	}

	/* password */
	if (!dbh->password && vars[3].optval) {
		dbh->password = pestrdup(vars[3].optval, dbh->is_persistent);
	}

	if (dbh->password) {
		H->last_err = DCIAttrSet(H->session, DCI_HTYPE_SESSION,
			   	dbh->password, (ub4) strlen(dbh->password),
				DCI_ATTR_PASSWORD, H->err);
		if (H->last_err) {
			dci_drv_error("DCIAttrSet: DCI_ATTR_PASSWORD");
			goto cleanup;
		}
	}

	/* Now fire up the session */
	H->last_err = DCISessionBegin(H->svc, H->err, H->session, DCI_CRED_RDBMS, DCI_DEFAULT);
	if (H->last_err) {
		dci_drv_error("DCISessionBegin");
		goto cleanup;
	}

	/* set the server handle into service handle */
	H->last_err = DCIAttrSet(H->svc, DCI_HTYPE_SVCCTX, H->session, 0, DCI_ATTR_SESSION, H->err);
	if (H->last_err) {
		dci_drv_error("DCIAttrSet: DCI_ATTR_SESSION");
		goto cleanup;
	}

	/* Get max character width */
 	H->last_err = DCINlsNumericInfoGet(H->env, H->err, &H->max_char_width, DCI_NLS_CHARSET_MAXBYTESZ);
 	if (H->last_err) {
 		dci_drv_error("DCINlsNumericInfoGet: DCI_NLS_CHARSET_MAXBYTESZ");
 		goto cleanup;
 	}

	dbh->methods = &dci_methods;
	dbh->alloc_own_columns = 1;
	dbh->native_case = PDO_CASE_UPPER;

	ret = 1;

cleanup:
	for (i = 0; i < sizeof(vars)/sizeof(vars[0]); i++) {
		if (vars[i].freeme) {
			efree(vars[i].optval);
		}
	}

	if (!ret) {
		dci_handle_closer(dbh);
	}

	return ret;
}
/* }}} */

const pdo_driver_t pdo_dci_driver = {
	PDO_DRIVER_HEADER(dci),
	pdo_dci_handle_factory
};

static inline ub4 pdo_dci_sanitize_prefetch(long prefetch) /* {{{ */
{
	if (prefetch < 0) {
		prefetch = 0;
	} else if (prefetch > UB4MAXVAL / PDO_DCI_PREFETCH_ROWSIZE) {
		prefetch = PDO_DCI_PREFETCH_DEFAULT;
	}
	return ((ub4)prefetch);
}
/* }}} */

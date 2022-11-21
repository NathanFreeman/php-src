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
#include "Zend/zend_extensions.h"

#define PDO_DCI_LOBMAXSIZE (4294967295UL) /* DCI_LOBMAXSIZE */

#define STMT_CALL(name, params)											\
	do {																\
		S->last_err = name params;										\
		S->last_err = _dci_error(S->err, stmt->dbh, stmt, #name, S->last_err, FALSE, __FILE__, __LINE__); \
		if (S->last_err) {												\
			return 0;													\
		}																\
	} while(0)

#define STMT_CALL_MSG(name, msg, params)								\
	do { 																\
		S->last_err = name params;										\
		S->last_err = _dci_error(S->err, stmt->dbh, stmt, #name ": " #msg, S->last_err, FALSE, __FILE__, __LINE__); \
		if (S->last_err) {												\
			return 0;													\
		}																\
	} while(0)

static php_stream *dci_create_lob_stream(zval *dbh, pdo_stmt_t *stmt, DCILobLocator *lob);

#define DCI_TEMPLOB_CLOSE(envhp, svchp, errhp, lob)				\
	do															\
	{															\
		boolean isTempLOB;										\
		DCILobIsTemporary(envhp, errhp, lob, &isTempLOB);		\
		if (isTempLOB)											\
			DCILobFreeTemporary(svchp, errhp, lob);				\
	} while(0)

static int dci_stmt_dtor(pdo_stmt_t *stmt) /* {{{ */
{
	pdo_dci_stmt *S = (pdo_dci_stmt*)stmt->driver_data;
	HashTable *BC = stmt->bound_columns;
	HashTable *BP = stmt->bound_params;

	int i;

	if (S->stmt) {
		/* cancel server side resources for the statement if we didn't
		 * fetch it all */
		DCIStmtFetch(S->stmt, S->err, 0, DCI_FETCH_NEXT, DCI_DEFAULT);

		/* free the handle */
		DCIHandleFree(S->stmt, DCI_HTYPE_STMT);
		S->stmt = NULL;
	}
	if (S->err) {
		DCIHandleFree(S->err, DCI_HTYPE_ERROR);
		S->err = NULL;
	}

	/* need to ensure these go away now */
	if (BC) {
		zend_hash_destroy(BC);
		FREE_HASHTABLE(stmt->bound_columns);
		stmt->bound_columns = NULL;
	}

	if (BP) {
		zend_hash_destroy(BP);
		FREE_HASHTABLE(stmt->bound_params);
		stmt->bound_params = NULL;
	}

	if (S->einfo.errmsg) {
		pefree(S->einfo.errmsg, stmt->dbh->is_persistent);
		S->einfo.errmsg = NULL;
	}

	if (S->cols) {
		for (i = 0; i < stmt->column_count; i++) {
			if (S->cols[i].data) {
				switch (S->cols[i].dtype) {
					case SQLT_BLOB:
					case SQLT_CLOB:
						DCI_TEMPLOB_CLOSE(S->H->env, S->H->svc, S->H->err,
							(DCILobLocator *) S->cols[i].data);
						DCIDescriptorFree(S->cols[i].data, DCI_DTYPE_LOB);
						break;
					default:
						efree(S->cols[i].data);
				}
			}
		}
		efree(S->cols);
		S->cols = NULL;
	}
	efree(S);

	stmt->driver_data = NULL;

	return 1;
} /* }}} */

static int dci_stmt_execute(pdo_stmt_t *stmt) /* {{{ */
{
	pdo_dci_stmt *S = (pdo_dci_stmt*)stmt->driver_data;
	ub4 rowcount;
	b4 mode;

	if (!S->stmt_type) {
		STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_STMT_TYPE",
				(S->stmt, DCI_HTYPE_STMT, &S->stmt_type, 0, DCI_ATTR_STMT_TYPE, S->err));
	}

	if (stmt->executed) {
		/* ensure that we cancel the cursor from a previous fetch */
		DCIStmtFetch(S->stmt, S->err, 0, DCI_FETCH_NEXT, DCI_DEFAULT);
	}

#ifdef DCI_STMT_SCROLLABLE_READONLY /* needed for dci8 ? */
	if (S->exec_type == DCI_STMT_SCROLLABLE_READONLY) {
		mode = DCI_STMT_SCROLLABLE_READONLY;
	} else
#endif
	if (stmt->dbh->auto_commit && !stmt->dbh->in_txn) {
		mode = DCI_COMMIT_ON_SUCCESS;
	} else {
		mode = DCI_DEFAULT;
	}

	STMT_CALL(DCIStmtExecute, (S->H->svc, S->stmt, S->err,
				(S->stmt_type == DCI_STMT_SELECT && !S->have_blobs) ? 0 : 1, 0, NULL, NULL,
				mode));

	if (!stmt->executed) {
		ub4 colcount;
		/* do first-time-only definition of bind/mapping stuff */

		/* how many columns do we have ? */
		STMT_CALL_MSG(DCIAttrGet, "ATTR_PARAM_COUNT",
				(S->stmt, DCI_HTYPE_STMT, &colcount, 0, DCI_ATTR_PARAM_COUNT, S->err));

		stmt->column_count = (int)colcount;

		if (S->cols) {
			int i;
			for (i = 0; i < stmt->column_count; i++) {
				if (S->cols[i].data) {
					switch (S->cols[i].dtype) {
						case SQLT_BLOB:
						case SQLT_CLOB:
							/* do nothing */
							break;
						default:
							efree(S->cols[i].data);
					}
				}
			}
			efree(S->cols);
		}

		S->cols = ecalloc(colcount, sizeof(pdo_dci_column));
	}

	STMT_CALL_MSG(DCIAttrGet, "ATTR_ROW_COUNT",
			(S->stmt, DCI_HTYPE_STMT, &rowcount, 0, DCI_ATTR_ROW_COUNT, S->err));
	stmt->row_count = (long)rowcount;

	return 1;
} /* }}} */

static sb4 dci_bind_input_cb(dvoid *ctx, DCIBind *bindp, ub4 iter, ub4 index, dvoid **bufpp, ub4 *alenp, ub1 *piecep, dvoid **indpp) /* {{{ */
{
	struct pdo_bound_param_data *param = (struct pdo_bound_param_data*)ctx;
	pdo_dci_bound_param *P = (pdo_dci_bound_param*)param->driver_data;
	zval *parameter;

	ZEND_ASSERT(param);

	*indpp = &P->indicator;

    if (Z_ISREF(param->parameter))
		parameter = Z_REFVAL(param->parameter);
	else
		parameter = &param->parameter;

	if (P->thing) {
		*bufpp = P->thing;
		*alenp = sizeof(void*);
	} else if (ZVAL_IS_NULL(parameter)) {
		/* insert a NULL value into the column */
		P->indicator = -1; /* NULL */
		*bufpp = 0;
		*alenp = -1;
	} else if (!P->thing) {
		/* regular string bind */
		if (!try_convert_to_string(parameter)) {
			return DCI_ERROR;
		}
		*bufpp = Z_STRVAL_P(parameter);
		*alenp = (ub4) Z_STRLEN_P(parameter);
	}

	*piecep = DCI_ONE_PIECE;
	return DCI_CONTINUE;
} /* }}} */

static sb4 dci_bind_output_cb(dvoid *ctx, DCIBind *bindp, ub4 iter, ub4 index, dvoid **bufpp, ub4 **alenpp, ub1 *piecep, dvoid **indpp, ub2 **rcodepp) /* {{{ */
{
	struct pdo_bound_param_data *param = (struct pdo_bound_param_data*)ctx;
	pdo_dci_bound_param *P = (pdo_dci_bound_param*)param->driver_data;
	zval *parameter;

	ZEND_ASSERT(param);

	if (Z_ISREF(param->parameter))
		parameter = Z_REFVAL(param->parameter);
	else
		parameter = &param->parameter;

	if (PDO_PARAM_TYPE(param->param_type) == PDO_PARAM_LOB) {
		P->actual_len = sizeof(DCILobLocator*);
		*bufpp = P->thing;
		*alenpp = &P->actual_len;
		*piecep = DCI_ONE_PIECE;
		*rcodepp = &P->retcode;
		*indpp = &P->indicator;
		return DCI_CONTINUE;
	}

	if (Z_TYPE_P(parameter) == IS_OBJECT || Z_TYPE_P(parameter) == IS_RESOURCE) {
		return DCI_CONTINUE;
	}

	zval_ptr_dtor(parameter);

	Z_STR_P(parameter) = zend_string_alloc(param->max_value_len, 1);
	P->used_for_output = 1;

	P->actual_len = (ub4) Z_STRLEN_P(parameter);
	*alenpp = &P->actual_len;
	*bufpp = (Z_STR_P(parameter))->val;
	*piecep = DCI_ONE_PIECE;
	*rcodepp = &P->retcode;
	*indpp = &P->indicator;

	return DCI_CONTINUE;
} /* }}} */

static int dci_stmt_param_hook(pdo_stmt_t *stmt, struct pdo_bound_param_data *param, enum pdo_param_event event_type) /* {{{ */
{
	pdo_dci_stmt *S = (pdo_dci_stmt*)stmt->driver_data;

	/* we're only interested in parameters for prepared SQL right now */
	if (param->is_param) {
		pdo_dci_bound_param *P;
		sb4 value_sz = -1;
		zval *parameter;

		if (Z_ISREF(param->parameter))
			parameter = Z_REFVAL(param->parameter);
		else
			parameter = &param->parameter;

		P = (pdo_dci_bound_param*)param->driver_data;

		switch (event_type) {
			case PDO_PARAM_EVT_FETCH_PRE:
			case PDO_PARAM_EVT_FETCH_POST:
			case PDO_PARAM_EVT_NORMALIZE:
				/* Do nothing */
				break;

			case PDO_PARAM_EVT_FREE:
				P = param->driver_data;
				if (P && P->thing) {
					DCI_TEMPLOB_CLOSE(S->H->env, S->H->svc, S->H->err, P->thing);
					DCIDescriptorFree(P->thing, DCI_DTYPE_LOB);
					P->thing = NULL;
					efree(P);
				}
				else if (P) {
					efree(P);
				}
				break;

			case PDO_PARAM_EVT_ALLOC:
				P = (pdo_dci_bound_param*)ecalloc(1, sizeof(pdo_dci_bound_param));
				param->driver_data = P;

				/* figure out what we're doing */
				switch (PDO_PARAM_TYPE(param->param_type)) {
					case PDO_PARAM_STMT:
						return 0;

					case PDO_PARAM_LOB:
						/* P->thing is now an DCILobLocator * */
						P->dci_type = SQLT_BLOB;
						value_sz = (sb4) sizeof(DCILobLocator*);
						break;

					case PDO_PARAM_STR:
					default:
						P->dci_type = SQLT_CHR;
						value_sz = (sb4) param->max_value_len;
						if (param->max_value_len == 0) {
							value_sz = (sb4) 1332; /* maximum size before value is interpreted as a LONG value */
						}

				}

				if (param->name) {
					STMT_CALL(DCIBindByName, (S->stmt,
							&P->bind, S->err, (text*)param->name->val,
							(sb4) param->name->len, 0, value_sz, P->dci_type,
							&P->indicator, 0, &P->retcode, 0, 0,
							DCI_DATA_AT_EXEC));
				} else {
					STMT_CALL(DCIBindByPos, (S->stmt,
							&P->bind, S->err, ((ub4)param->paramno)+1,
							0, value_sz, P->dci_type,
							&P->indicator, 0, &P->retcode, 0, 0,
							DCI_DATA_AT_EXEC));
				}

				STMT_CALL(DCIBindDynamic, (P->bind,
							S->err,
							param, dci_bind_input_cb,
							param, dci_bind_output_cb));

				return 1;

			case PDO_PARAM_EVT_EXEC_PRE:
				P->indicator = 0;
				P->used_for_output = 0;
				if (PDO_PARAM_TYPE(param->param_type) == PDO_PARAM_LOB) {
					ub4 empty = 0;
					STMT_CALL(DCIDescriptorAlloc, (S->H->env, &P->thing, DCI_DTYPE_LOB, 0, NULL));
					STMT_CALL(DCIAttrSet, (P->thing, DCI_DTYPE_LOB, &empty, 0, DCI_ATTR_LOBEMPTY, S->err));
					S->have_blobs = 1;
				}
				return 1;

			case PDO_PARAM_EVT_EXEC_POST:
				/* fixup stuff set in motion in dci_bind_output_cb */
				if (P->used_for_output) {
					if (P->indicator == -1) {
						/* set up a NULL value */
						if (Z_TYPE_P(parameter) == IS_STRING) {
							/* DCI likes to stick non-terminated strings in things */
							*Z_STRVAL_P(parameter) = '\0';
						}
						zval_ptr_dtor_str(parameter);
						ZVAL_UNDEF(parameter);
					} else if (Z_TYPE_P(parameter) == IS_STRING) {
						Z_STR_P(parameter) = zend_string_init(Z_STRVAL_P(parameter), P->actual_len, 1);
					}
				} else if (PDO_PARAM_TYPE(param->param_type) == PDO_PARAM_LOB && P->thing) {
					php_stream *stm;

					if (Z_TYPE_P(parameter) == IS_NULL) {
						/* if the param is NULL, then we assume that they
						 * wanted to bind a lob locator into it from the query
						 * */

						stm = dci_create_lob_stream(&stmt->database_object_handle, stmt, (DCILobLocator*)P->thing);
						if (stm) {
							DCILobOpen(S->H->svc, S->err, (DCILobLocator*)P->thing, DCI_LOB_READWRITE);
							php_stream_to_zval(stm, parameter);
						}
					} else {
						/* we're a LOB being used for insert; transfer the data now */
						size_t n;
						ub4 amt, offset = 1;
						char *consume;

						php_stream_from_zval_no_verify(stm, parameter);
						if (stm) {
							DCILobOpen(S->H->svc, S->err, (DCILobLocator*)P->thing, DCI_LOB_READWRITE);
							do {
								char buf[8192];
								n = php_stream_read(stm, buf, sizeof(buf));
								if ((int)n <= 0) {
									break;
								}
								consume = buf;
								do {
									amt = (ub4) n;
									DCILobWrite(S->H->svc, S->err, (DCILobLocator*)P->thing,
											&amt, offset, consume, (ub4) n,
											DCI_ONE_PIECE,
											NULL, NULL, 0, SQLCS_IMPLICIT);
									offset += amt;
									n -= amt;
									consume += amt;
								} while (n);
							} while (1);
							DCILobClose(S->H->svc, S->err, (DCILobLocator*)P->thing);
							DCILobFlushBuffer(S->H->svc, S->err, (DCILobLocator*)P->thing, 0);
						} else if (Z_TYPE_P(parameter) == IS_STRING) {
							/* stick the string into the LOB */
							consume = Z_STRVAL_P(parameter);
							n = Z_STRLEN_P(parameter);
							if (n) {
								DCILobOpen(S->H->svc, S->err, (DCILobLocator*)P->thing, DCI_LOB_READWRITE);
								while (n) {
									amt = (ub4) n;
									DCILobWrite(S->H->svc, S->err, (DCILobLocator*)P->thing,
											&amt, offset, consume, (ub4) n,
											DCI_ONE_PIECE,
											NULL, NULL, 0, SQLCS_IMPLICIT);
									consume += amt;
									n -= amt;
								}
								DCILobClose(S->H->svc, S->err, (DCILobLocator*)P->thing);
							}
						}
						DCI_TEMPLOB_CLOSE(S->H->env, S->H->svc, S->H->err, P->thing);
						DCIDescriptorFree(P->thing, DCI_DTYPE_LOB);
						P->thing = NULL;
					}
				}

				return 1;
		}
	}

	return 1;
} /* }}} */

static int dci_stmt_fetch(pdo_stmt_t *stmt, enum pdo_fetch_orientation ori,	zend_long offset) /* {{{ */
{
#ifdef HAVE_DCISTMTFETCH2
	ub4 dciori = DCI_FETCH_NEXT;
#endif
	pdo_dci_stmt *S = (pdo_dci_stmt*)stmt->driver_data;

#ifdef HAVE_DCISTMTFETCH2
	switch (ori) {
		case PDO_FETCH_ORI_NEXT:	dciori = DCI_FETCH_NEXT; break;
		case PDO_FETCH_ORI_PRIOR:	dciori = DCI_FETCH_PRIOR; break;
		case PDO_FETCH_ORI_FIRST:	dciori = DCI_FETCH_FIRST; break;
		case PDO_FETCH_ORI_LAST:	dciori = DCI_FETCH_LAST; break;
		case PDO_FETCH_ORI_ABS:		dciori = DCI_FETCH_ABSOLUTE; break;
		case PDO_FETCH_ORI_REL:		dciori = DCI_FETCH_RELATIVE; break;
	}
	S->last_err = DCIStmtFetch2(S->stmt, S->err, 1, dciori, (sb4) offset, DCI_DEFAULT);
#else
	S->last_err = DCIStmtFetch(S->stmt, S->err, 1, DCI_FETCH_NEXT, DCI_DEFAULT);
#endif

	if (S->last_err == DCI_NO_DATA) {
		/* no (more) data */
		return 0;
	}

	if (S->last_err == DCI_NEED_DATA) {
		dci_stmt_error("DCI_NEED_DATA");
		return 0;
	}

	if (S->last_err == DCI_SUCCESS_WITH_INFO || S->last_err == DCI_SUCCESS) {
		return 1;
	}

	dci_stmt_error("DCIStmtFetch");

	return 0;
} /* }}} */

static sb4 dci_define_callback(dvoid *octxp, DCIDefine *define, ub4 iter, dvoid **bufpp,
		ub4 **alenpp, ub1 *piecep, dvoid **indpp, ub2 **rcodepp)
{
	pdo_dci_column *col = (pdo_dci_column*)octxp;

	switch (col->dtype) {
		case SQLT_BLOB:
		case SQLT_CLOB:
			*piecep = DCI_ONE_PIECE;
			*bufpp = col->data;
			*alenpp = &col->datalen;
			*indpp = (dvoid *)&col->indicator;
			break;
		EMPTY_SWITCH_DEFAULT_CASE();
	}

	return DCI_CONTINUE;
}

static int dci_stmt_describe(pdo_stmt_t *stmt, int colno) /* {{{ */
{
	pdo_dci_stmt *S = (pdo_dci_stmt*)stmt->driver_data;
	DCIParam *param = NULL;
	text *colname;
	ub2 dtype, data_size, precis;
	ub4 namelen;
	struct pdo_column_data *col = &stmt->columns[colno];
	zend_bool dyn = FALSE;

	/* describe the column */
	STMT_CALL(DCIParamGet, (S->stmt, DCI_HTYPE_STMT, S->err, (dvoid*)&param, colno+1));

	/* what type ? */
	STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_DATA_TYPE",
			(param, DCI_DTYPE_PARAM, &dtype, 0, DCI_ATTR_DATA_TYPE, S->err));

	/* how big ? */
	STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_DATA_SIZE",
			(param, DCI_DTYPE_PARAM, &data_size, 0, DCI_ATTR_DATA_SIZE, S->err));

	/* precision ? */
	STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_PRECISION",
			(param, DCI_DTYPE_PARAM, &precis, 0, DCI_ATTR_PRECISION, S->err));

	/* name ? */
	STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_NAME",
			(param, DCI_DTYPE_PARAM, &colname, &namelen, DCI_ATTR_NAME, S->err));

	col->precision = precis;
	col->maxlen = data_size;
	col->name = zend_string_init((char *)colname, namelen, 0);

	S->cols[colno].dtype = dtype;

	/* how much room do we need to store the field */
	switch (dtype) {
		case SQLT_LBI:
		case SQLT_LNG:
			if (dtype == SQLT_LBI) {
				dtype = SQLT_BIN;
			} else {
				dtype = SQLT_CHR;
			}
			S->cols[colno].datalen = 512; /* XXX should be INT_MAX and fetched by pieces */
			S->cols[colno].data = emalloc(S->cols[colno].datalen + 1);
			col->param_type = PDO_PARAM_STR;
			break;

		case SQLT_BLOB:
		case SQLT_CLOB:
			col->param_type = PDO_PARAM_LOB;
			STMT_CALL(DCIDescriptorAlloc, (S->H->env, (dvoid**)&S->cols[colno].data, DCI_DTYPE_LOB, 0, NULL));
			S->cols[colno].datalen = sizeof(DCILobLocator*);
			dyn = TRUE;
			break;

		case SQLT_BIN:
		default:
			if (dtype == SQLT_DAT || dtype == SQLT_NUM || dtype == SQLT_RDD
#ifdef SQLT_TIMESTAMP
					|| dtype == SQLT_TIMESTAMP
#endif
#ifdef SQLT_TIMESTAMP_TZ
					|| dtype == SQLT_TIMESTAMP_TZ
#endif
					) {
				/* should be big enough for most date formats and numbers */
				S->cols[colno].datalen = 512;
#if defined(SQLT_IBFLOAT) && defined(SQLT_IBDOUBLE)
			} else if (dtype == SQLT_IBFLOAT || dtype == SQLT_IBDOUBLE) {
				S->cols[colno].datalen = 1024;
#endif
			} else if (dtype == SQLT_BIN) {
				S->cols[colno].datalen = (ub4) col->maxlen * 2; /* raw characters to hex digits */
			} else {
				S->cols[colno].datalen = (ub4) (col->maxlen * S->H->max_char_width);
			}

			S->cols[colno].data = emalloc(S->cols[colno].datalen + 1);
			dtype = SQLT_CHR;

			/* returning data as a string */
			col->param_type = PDO_PARAM_STR;
	}

	STMT_CALL(DCIDefineByPos, (S->stmt, &S->cols[colno].def, S->err, colno+1,
				S->cols[colno].data, S->cols[colno].datalen, dtype, &S->cols[colno].indicator,
				&S->cols[colno].fetched_len, &S->cols[colno].retcode, dyn ? DCI_DYNAMIC_FETCH : DCI_DEFAULT));

	if (dyn) {
		STMT_CALL(DCIDefineDynamic, (S->cols[colno].def, S->err, &S->cols[colno],
				dci_define_callback));
	}

	return 1;
} /* }}} */

struct _dci_lob_env {
	DCISvcCtx *svc;
	DCIError  *err;
};
typedef struct _dci_lob_env dci_lob_env;

struct dci_lob_self {
	zval dbh;
	pdo_stmt_t *stmt;
	pdo_dci_stmt *S;
	DCILobLocator *lob;
	dci_lob_env   *E;
	ub4 offset;
};

static ssize_t dci_blob_write(php_stream *stream, const char *buf, size_t count)
{
	struct dci_lob_self *self = (struct dci_lob_self*)stream->abstract;
	ub4 amt;
	sword r;

	amt = (ub4) count;
	r = DCILobWrite(self->E->svc, self->E->err, self->lob,
		&amt, self->offset, (char*)buf, (ub4) count,
		DCI_ONE_PIECE,
		NULL, NULL, 0, SQLCS_IMPLICIT);

	if (r != DCI_SUCCESS) {
		return (ssize_t)-1;
	}

	self->offset += amt;
	return amt;
}

static ssize_t dci_blob_read(php_stream *stream, char *buf, size_t count)
{
	struct dci_lob_self *self = (struct dci_lob_self*)stream->abstract;
	ub4 amt;
	sword r;

	amt = (ub4) count;
	r = DCILobRead(self->E->svc, self->E->err, self->lob,
		&amt, self->offset, buf, (ub4) count,
		NULL, NULL, 0, SQLCS_IMPLICIT);

	if (r != DCI_SUCCESS && r != DCI_NEED_DATA) {
		return (size_t)-1;
	}

	self->offset += amt;
	if (amt < count) {
		stream->eof = 1;
	}
	return amt;
}

static int dci_blob_close(php_stream *stream, int close_handle)
{
	struct dci_lob_self *self = (struct dci_lob_self *)stream->abstract;
	pdo_stmt_t *stmt = self->stmt;

	if (close_handle) {
		zend_object *obj = &stmt->std;

		DCILobClose(self->E->svc, self->E->err, self->lob);
		zval_ptr_dtor(&self->dbh);
		GC_DELREF(obj);
		efree(self->E);
		efree(self);
	}

	/* php_pdo_free_statement(stmt); */
	return 0;
}

static int dci_blob_flush(php_stream *stream)
{
	struct dci_lob_self *self = (struct dci_lob_self*)stream->abstract;
	DCILobFlushBuffer(self->E->svc, self->E->err, self->lob, 0);
	return 0;
}

static int dci_blob_seek(php_stream *stream, zend_off_t offset, int whence, zend_off_t *newoffset)
{
	struct dci_lob_self *self = (struct dci_lob_self*)stream->abstract;

	if (offset >= PDO_DCI_LOBMAXSIZE) {
		return -1;
	} else {
		self->offset = (ub4) offset + 1;  /* Oracle LOBS are 1-based, but PHP is 0-based */
		return 0;
	}
}

static const php_stream_ops dci_blob_stream_ops = {
	dci_blob_write,
	dci_blob_read,
	dci_blob_close,
	dci_blob_flush,
	"pdo_dci blob stream",
	dci_blob_seek,
	NULL,
	NULL,
	NULL
};

static php_stream *dci_create_lob_stream(zval *dbh, pdo_stmt_t *stmt, DCILobLocator *lob)
{
	php_stream *stm;
	struct dci_lob_self *self = ecalloc(1, sizeof(*self));

	ZVAL_COPY_VALUE(&self->dbh, dbh);
	self->lob = lob;
	self->offset = 1; /* 1-based */
	self->stmt = stmt;
	self->S = (pdo_dci_stmt*)stmt->driver_data;
	self->E = ecalloc(1, sizeof(dci_lob_env));
	self->E->svc = self->S->H->svc;
	self->E->err = self->S->err;

	stm = php_stream_alloc(&dci_blob_stream_ops, self, 0, "r+b");

	if (stm) {
		zend_object *obj;
		obj = &stmt->std;
		Z_ADDREF(self->dbh);
		GC_ADDREF(obj);
		return stm;
	}

	efree(self);
	return NULL;
}

static int dci_stmt_get_col(pdo_stmt_t *stmt, int colno, char **ptr, size_t *len, int *caller_frees) /* {{{ */
{
	pdo_dci_stmt *S = (pdo_dci_stmt*)stmt->driver_data;
	pdo_dci_column *C = &S->cols[colno];

	/* check the indicator to ensure that the data is intact */
	if (C->indicator == -1) {
		/* A NULL value */
		*ptr = NULL;
		*len = 0;
		return 1;
	} else if (C->indicator == 0) {
		/* it was stored perfectly */

		if (C->dtype == SQLT_BLOB || C->dtype == SQLT_CLOB) {
			if (C->data) {
				*ptr = (char*)dci_create_lob_stream(&stmt->database_object_handle, stmt, (DCILobLocator*)C->data);
				DCILobOpen(S->H->svc, S->err, (DCILobLocator*)C->data, DCI_LOB_READONLY);
			}
			*len = (size_t) 0;
			return *ptr ? 1 : 0;
		}

		*ptr = C->data;
		*len = (size_t) C->fetched_len;
		return 1;
	} else {
		/* it was truncated */
		php_error_docref(NULL, E_WARNING, "Column %d data was too large for buffer and was truncated to fit it", colno);

		*ptr = C->data;
		*len = (size_t) C->fetched_len;
		return 1;
	}
} /* }}} */


static int dci_stmt_col_meta(pdo_stmt_t *stmt, zend_long colno, zval *return_value) /* {{{ */
{
	pdo_dci_stmt *S = (pdo_dci_stmt*)stmt->driver_data;
	DCIParam *param = NULL;
	ub2 dtype, precis;
	sb1 scale;
	zval flags;
	ub1 isnull, charset_form;
	if (!S->stmt) {
		return FAILURE;
	}
	if (colno >= stmt->column_count) {
		/* error invalid column */
		return FAILURE;
	}

	array_init(return_value);
	array_init(&flags);

	/* describe the column */
	STMT_CALL(DCIParamGet, (S->stmt, DCI_HTYPE_STMT, S->err, (dvoid*)&param, colno+1));

	/* column data type */
	STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_DATA_TYPE",
			(param, DCI_DTYPE_PARAM, &dtype, 0, DCI_ATTR_DATA_TYPE, S->err));

	/* column precision */
	STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_PRECISION",
			(param, DCI_DTYPE_PARAM, &precis, 0, DCI_ATTR_PRECISION, S->err));

	/* column scale */
	STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_SCALE",
			(param, DCI_DTYPE_PARAM, &scale, 0, DCI_ATTR_SCALE, S->err));

	/* string column charset form */
	if (dtype == SQLT_CHR || dtype == SQLT_VCS || dtype == SQLT_AFC || dtype == SQLT_CLOB) {
		STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_CHARSET_FORM",
			(param, DCI_DTYPE_PARAM, &charset_form, 0, DCI_ATTR_CHARSET_FORM, S->err));
	}


	if (dtype) {
	/* if there is a declared type */
		switch (dtype) {
#ifdef SQLT_TIMESTAMP
		case SQLT_TIMESTAMP:
			add_assoc_string(return_value, "dci:decl_type", "TIMESTAMP");
			add_assoc_string(return_value, "native_type", "TIMESTAMP");
			break;
#endif
#ifdef SQLT_TIMESTAMP_TZ
		case SQLT_TIMESTAMP_TZ:
			add_assoc_string(return_value, "dci:decl_type", "TIMESTAMP WITH TIMEZONE");
			add_assoc_string(return_value, "native_type", "TIMESTAMP WITH TIMEZONE");
			break;
#endif
#ifdef SQLT_TIMESTAMP_LTZ
		case SQLT_TIMESTAMP_LTZ:
			add_assoc_string(return_value, "dci:decl_type", "TIMESTAMP WITH LOCAL TIMEZONE");
			add_assoc_string(return_value, "native_type", "TIMESTAMP WITH LOCAL TIMEZONE");
			break;
#endif
#ifdef SQLT_INTERVAL_YM
		case SQLT_INTERVAL_YM:
			add_assoc_string(return_value, "dci:decl_type", "INTERVAL YEAR TO MONTH");
			add_assoc_string(return_value, "native_type", "INTERVAL YEAR TO MONTH");
			break;
#endif
#ifdef SQLT_INTERVAL_DS
		case SQLT_INTERVAL_DS:
			add_assoc_string(return_value, "dci:decl_type", "INTERVAL DAY TO SECOND");
			add_assoc_string(return_value, "native_type", "INTERVAL DAY TO SECOND");
			break;
#endif
		case SQLT_DAT:
			add_assoc_string(return_value, "dci:decl_type", "DATE");
			add_assoc_string(return_value, "native_type", "DATE");
			break;
		case SQLT_FLT :
		case SQLT_NUM:
			/* if the precision is nonzero and scale is -127 then it is a FLOAT */
			if (scale == -127 && precis != 0) {
				add_assoc_string(return_value, "dci:decl_type", "FLOAT");
				add_assoc_string(return_value, "native_type", "FLOAT");
			} else {
				add_assoc_string(return_value, "dci:decl_type", "NUMBER");
				add_assoc_string(return_value, "native_type", "NUMBER");
			}
			break;
		case SQLT_LNG:
			add_assoc_string(return_value, "dci:decl_type", "LONG");
			add_assoc_string(return_value, "native_type", "LONG");
			break;
		case SQLT_BIN:
			add_assoc_string(return_value, "dci:decl_type", "RAW");
			add_assoc_string(return_value, "native_type", "RAW");
			break;
		case SQLT_LBI:
			add_assoc_string(return_value, "dci:decl_type", "LONG RAW");
			add_assoc_string(return_value, "native_type", "LONG RAW");
			break;
		case SQLT_CHR:
		case SQLT_VCS:
			if (charset_form == SQLCS_NCHAR) {
				add_assoc_string(return_value, "dci:decl_type", "NVARCHAR2");
				add_assoc_string(return_value, "native_type", "NVARCHAR2");
			} else {
				add_assoc_string(return_value, "dci:decl_type", "VARCHAR2");
				add_assoc_string(return_value, "native_type", "VARCHAR2");
			}
			break;
		case SQLT_AFC:
			if (charset_form == SQLCS_NCHAR) {
				add_assoc_string(return_value, "dci:decl_type", "NCHAR");
				add_assoc_string(return_value, "native_type", "NCHAR");
			} else {
				add_assoc_string(return_value, "dci:decl_type", "CHAR");
				add_assoc_string(return_value, "native_type", "CHAR");
			}
			break;
		case SQLT_BLOB:
			add_assoc_string(return_value, "dci:decl_type", "BLOB");
			add_next_index_string(&flags, "blob");
			add_assoc_string(return_value, "native_type", "BLOB");
			break;
		case SQLT_CLOB:
			if (charset_form == SQLCS_NCHAR) {
				add_assoc_string(return_value, "dci:decl_type", "NCLOB");
				add_assoc_string(return_value, "native_type", "NCLOB");
			} else {
				add_assoc_string(return_value, "dci:decl_type", "CLOB");
				add_assoc_string(return_value, "native_type", "CLOB");
			}
			add_next_index_string(&flags, "blob");
			break;
		case SQLT_BFILE:
			add_assoc_string(return_value, "dci:decl_type", "BFILE");
			add_next_index_string(&flags, "blob");
			add_assoc_string(return_value, "native_type", "BFILE");
			break;
		case SQLT_RDD:
			add_assoc_string(return_value, "dci:decl_type", "ROWID");
			add_assoc_string(return_value, "native_type", "ROWID");
			break;
		case SQLT_BFLOAT:
		case SQLT_IBFLOAT:
			add_assoc_string(return_value, "dci:decl_type", "BINARY_FLOAT");
			add_assoc_string(return_value, "native_type", "BINARY_FLOAT");
			break;
		case SQLT_BDOUBLE:
		case SQLT_IBDOUBLE:
			add_assoc_string(return_value, "dci:decl_type", "BINARY_DOUBLE");
			add_assoc_string(return_value, "native_type", "BINARY_DOUBLE");
			break;
		default:
			add_assoc_long(return_value, "dci:decl_type", dtype);
			add_assoc_string(return_value, "native_type", "UNKNOWN");
		}
	} else {
		/* if the column is NULL */
		add_assoc_long(return_value, "dci:decl_type", 0);
		add_assoc_string(return_value, "native_type", "NULL");
	}

	/* column can be null */
	STMT_CALL_MSG(DCIAttrGet, "DCI_ATTR_IS_NULL",
			(param, DCI_DTYPE_PARAM, &isnull, 0, DCI_ATTR_IS_NULL, S->err));

	if (isnull) {
		add_next_index_string(&flags, "nullable");
	} else {
		add_next_index_string(&flags, "not_null");
	}

	/* PDO type */
	switch (dtype) {
		case SQLT_BFILE:
		case SQLT_BLOB:
		case SQLT_CLOB:
			add_assoc_long(return_value, "pdo_type", PDO_PARAM_LOB);
			break;
		default:
			add_assoc_long(return_value, "pdo_type", PDO_PARAM_STR);
	}

	add_assoc_long(return_value, "scale", scale);
	add_assoc_zval(return_value, "flags", &flags);

	DCIDescriptorFree(param, DCI_DTYPE_PARAM);
	return SUCCESS;
} /* }}} */

const struct pdo_stmt_methods dci_stmt_methods = {
	dci_stmt_dtor,
	dci_stmt_execute,
	dci_stmt_fetch,
	dci_stmt_describe,
	dci_stmt_get_col,
	dci_stmt_param_hook,
	NULL, /* set_attr */
	NULL, /* get_attr */
	dci_stmt_col_meta,
	NULL,
	NULL
};

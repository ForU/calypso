#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Fri May  8 16:29:21 2015
# ht <515563130@qq.com, weixin:jacoolee>

"""
getting data from db by sql
"""

import MySQLdb
import MySQLdb.cursors

from co_sql import ModelIface
from co_error import COExcInternalError, COSqlExecuteError, CODuplicatedDBRecord
from co_constants import COConstants
from co_utils import g_utils, Magic


class InsertResult(Magic):
    def __init__(self, res=None, why=''):
        kwargs = {'res':res, 'why':why}
        super(InsertResult, self).__init__(**kwargs)
        self._is_duplicated = False
        # set attribute value
        self._set_attribute_value(res)

    def _set_attribute_value(self, last_id):
        if last_id == -1062:
            self._is_duplicated = True

    def isDuplicated(self):
        return self._is_duplicated


class UpdateResult(Magic):
    def __init__(self, **kwargs):
        super(UpdateResult, self).__init__(**kwargs)


class DeleteResult(Magic):
    def __init__(self, **kwargs):
        super(DeleteResult, self).__init__(**kwargs)


class SelectResult(Magic):
    def __init__(self, **kwargs):
        super(SelectResult, self).__init__(**kwargs)


class SqlExcecutor(object):
    DEFAULT_MYSQL_HOST = 'localhost'
    DEFAULT_MYSQL_PORT = 3306

    def __init__(self, mysql_host=DEFAULT_MYSQL_HOST, mysql_port=DEFAULT_MYSQL_PORT, mysql_user='', mysql_passwd='', mysql_db=''):
        self._mysql = Magic( host=mysql_host,
                             port=mysql_port,
                             user=mysql_user,
                             passwd=mysql_passwd,
                             db=mysql_db,
                         )
        self._auto_commit = True
        self._conn = None
        self._connect_to_db()

    def _connect_to_db(self):
        if self._mysql.host == None:
            print "[WARNING] failed to connect to database:%s@%s:%s" % \
                (self._mysql.user, self._mysql.host, self._mysql.port)
            return
        try:
            # TODO Thu Jun 11 17:28:38 2015 [load from connection pool other create a connection here]
            self._conn = MySQLdb.connect( host=self._mysql.host,
                                          port=self._mysql.port,
                                          user=self._mysql.user,
                                          passwd=self._mysql.passwd,
                                          db=self._mysql.db
                                      )
        except MySQLdb.OperationalError as e:
            raise COExcInternalError(e)
        except Exception as e:
            raise COExcInternalError(e)

    def _execute_sql(self, sql):
        """return None or action-specified value
        """
        if not self._conn:
            raise COExcInternalError("not connect to db yet")

        c = MySQLdb.cursors.DictCursor( self._conn )
        try:
            print "[INFO] executing SQL:\"%s\" @'%s'" % (sql, g_utils.now())
            c.execute( sql )
            if self._auto_commit:
                self.commit()
                print "[DIAGNOSE] executed: \"%s\" @'%s', info:'%s'" % (c._last_executed, g_utils.now(), c.__dict__)
            return c
        except MySQLdb.Error as e:
            raise COSqlExecuteError(*(list(e.args) + [sql]))
        except Exception as e:
            raise COExcInternalError(*(list(e.args) + [sql]))

    def startTransaction(self):
        self._auto_commit = False
        self._execute_sql('start transaction;')

    def commit(self):
        # print "[INFO] do commit here"
        self._conn.commit()

    def rollback(self):
        self._auto_commit = True
        self._conn.rollback()

    # has no need this function if connection is a pool
    def reInit(self, mysql_host=DEFAULT_MYSQL_HOST, mysql_port=DEFAULT_MYSQL_PORT, mysql_user='', mysql_passwd='', mysql_db=''):
        self._mysql = Magic( host=mysql_host,
                             port=mysql_port,
                             user=mysql_user,
                             passwd=mysql_passwd,
                             db=mysql_db
                         )
        print "[INFO] connecting %s ..." % self._mysql
        self._conn = MySQLdb.connect( host=self._mysql.host,
                                      port=self._mysql.port,
                                      user=self._mysql.user,
                                      passwd=self._mysql.passwd,
                                      db=self._mysql.db )
        print "[INFO] db connected."

    def execute(self, sql, sql_action, model_class=None, extra_model_fields=None):
        """
        1. get db datas by running sql
        2. new model and assign datas to models
        @model_class: the model to which the data will be assigned
        @extra_model_fields: is used only when model_class is subclass of ModelIface

        @Return: (result, why)
        @Exception: COExcInternalError
        """

        if not isinstance(sql, str):
            raise COExcInternalError("sql need to be string")
        if sql_action not in COConstants.VALID_SQL_ACTIONS:
            raise COExcInternalError("invalid sql_action:'%s', candidates:'%s'" % \
                                     (sql_action, COConstants.VALID_SQL_ACTIONS))
        if model_class and not issubclass(model_class, ModelIface):
            raise COExcInternalError("model_class need to be a Model")

        # refine result, avoid exception for better code programing experiences
        try:
            result, why = self._execute_sql(sql), 'OK'
            print "[DEBUG] Executed result of SQL:\"%s\" is:'%s'" % (sql, result)
        except Exception as e:
            if not self._auto_commit:
                # CRITICAL: if _auto_commit, a exception must be raise
                # to support transaction.
                # handle db duplicated case:
                if len(e.args) >= 2:
                    if isinstance(e.args[0], (int, long)):
                        if e.args[0] == 1062:
                            raise CODuplicatedDBRecord(e)
                # CRITICAL: more here for explict error
                # always raise a exception
                raise e

            # else auto commit mode
            result, why = None, str(e)
            # check if necessary to convert mysql error code
            if len(e.args) >= 2:
                if isinstance(e.args[0], (int, long)):
                    result = -e.args[0]

        if sql_action == COConstants.SQL_ACTION_INSERT:
            if isinstance(result, MySQLdb.cursors.DictCursor):
                result = result.lastrowid
            return InsertResult(res=result, why=why)

        if sql_action == COConstants.SQL_ACTION_UPDATE:
            result = True if isinstance(result, MySQLdb.cursors.DictCursor) else False
            return UpdateResult(res=result, why=why)

        if sql_action == COConstants.SQL_ACTION_DELETE:
            result = True if isinstance(result, MySQLdb.cursors.DictCursor) else False
            return DeleteResult(res=result, why=why)

        if sql_action == COConstants.SQL_ACTION_SELECT:
            # CRITICAL:
            # 'None' is return if no records found, # else a list
            if isinstance(result, MySQLdb.cursors.DictCursor):
                if model_class:
                    m = model_class()
                    if extra_model_fields:
                        m.registerExtraFields(extra_model_fields)
                    result = [ m.setDBData(i) for i in result.fetchall() ] or None
                else:
                    result = result.fetchall() or None
            return SelectResult(res=result, why=why)

        print "[ERROR] UNSUPPORTED sql action: '%s', should never happened" % sql_action

# global
g_co_executor = SqlExcecutor()


class Transaction(object):
    def __init__(self, executor=g_co_executor ):
        self._executor = executor

    def __enter__(self):
        self._executor.startTransaction()
        print "[INFO] starting transaction..."

    def __exit__(self, exc_type, exc_value, exc_tb):
        if not exc_tb:
            self._executor.commit()
            print "[INFO] commit and transaction success"
            return True
        # failed, do rollback
        print "[ERROR] transaction failed, rollback, caz: '%s', '%s'" % (exc_type, exc_value)
        self._executor.rollback()
        return False


def do_transaction(func):
    def _f_wrapper(*args, **kwargs):
        r = None
        with Transaction():
            r = func(*args, **kwargs)
        return r
    return _f_wrapper

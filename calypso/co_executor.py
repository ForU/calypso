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

from co_logger import l

EXEC_SQL_MAX_RETRY_TIME = 10
class InsertResult(Magic):
    def __init__(self, res=None, why=''):
        kwargs = {'res':res, 'why':why}
        super(InsertResult, self).__init__(**kwargs)
        self._is_duplicated = False
        # set attribute value
        self._set_attribute_value(res)

    def _set_attribute_value(self, last_id):
        if last_id == 1062:
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

class CommonResult(Magic):
    def __init__(self, **kwargs):
        super(CommonResult, self).__init__(**kwargs)


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
        # inner self used attribute
        self._auto_commit = True
        self._conn = None
        # self._connect_to_db()

    def _connect_to_db(self):
        if self._mysql.host == None:
            l.warn("failed to connect to database:%s@%s:%s" % \
                   (self._mysql.user, self._mysql.host, self._mysql.port))
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

    def _execute_sql(self, sql, max_try=EXEC_SQL_MAX_RETRY_TIME, allow_commit=True):
        if not self._conn:
            raise COExcInternalError("not connect to db yet, please reInit the g_co_executor please in your application initialization process")
        # l.debug("retrying to execute sql:[%s] @time:%s" % (sql, max_try))
        if max_try == 0:
            raise COExcInternalError("still failed to execute sql after retry:%s times" % (EXEC_SQL_MAX_RETRY_TIME))

        try:
            return self._bb_execute_sql(sql, allow_commit)
        except MySQLdb.Error as e:
            if isinstance(e.args[0], (int, long)) and e.args[0] == 2006:
                l.warn('MySQLdb.Error 2006 encountered, try to re-connect to db')
                self._connect_to_db()
                return self._execute_sql(sql, max_try=max_try-1)
            else:
                raise COSqlExecuteError(*(list(e.args) + [sql]))
        except Exception as e:
            raise COExcInternalError(*(list(e.args) + [sql]))

    def _bb_execute_sql(self, sql, allow_commit):
        """return None or action-specified value, no exception caught.
        """
        c = MySQLdb.cursors.DictCursor( self._conn )
        l.info("executing SQL:\"%s\" @'%s', allow_commit = %s" % (sql, g_utils.now(), allow_commit))
        c.execute( sql )
        if allow_commit:
            if self._auto_commit:
                self.commit()
                # l.dia("executed: \"%s\" @'%s', info:'%s'" % (c._last_executed, g_utils.now(), c.__dict__))
        return c

    def onStartTransaction(self):
        self._auto_commit = False
        self._execute_sql('start transaction;')

    def onEndTransaction(self):
        self._auto_commit = True

    def commit(self):
        # l.debug("do real commit, data saved to db")
        self._conn.commit()

    def rollback(self):
        self._auto_commit = True # force auto_commit
        self._conn.rollback()

    # has no need this function if connection is a pool
    def reInit(self, mysql_host=DEFAULT_MYSQL_HOST, mysql_port=DEFAULT_MYSQL_PORT, mysql_user='', mysql_passwd='', mysql_db=''):
        self._mysql = Magic( host=mysql_host,
                             port=mysql_port,
                             user=mysql_user,
                             passwd=mysql_passwd,
                             db=mysql_db
        )
        l.info("connecting %s ..." % self._mysql)
        self._conn = MySQLdb.connect( host=self._mysql.host,
                                      port=self._mysql.port,
                                      user=self._mysql.user,
                                      passwd=self._mysql.passwd,
                                      db=self._mysql.db )
        l.info("db connected.")

    def execute(self, sql, sql_action=COConstants.SQL_ACTION_COMMON, model_class=None, extra_model_fields=None, only_one=False, allow_commit=True):
        """
        1. get db datas by running sql
        2. new model and assign datas to models
        @model_class: the model to which the data will be assigned
        @extra_model_fields: is used only when model_class is subclass of ModelIface

        @Return: (result, why)
        @Exception: COExcInternalError
        """
        if not isinstance(sql, (str, unicode)):
            raise COExcInternalError("sql need to be string")
        if sql_action not in COConstants.VALID_SQL_ACTIONS:
            raise COExcInternalError("invalid sql_action:'%s', candidates:'%s'" % \
                                     (sql_action, COConstants.VALID_SQL_ACTIONS))
        if model_class and not issubclass(model_class, ModelIface):
            raise COExcInternalError("model_class need to be a Model")

        # refine result, avoid exception for better code programing experiences
        try:
            result, why = self._execute_sql(sql, allow_commit=allow_commit), 'OK'
            #l.debug("Executed result of SQL:\"%s\" is:'%s'" % (sql, result))
        except Exception as e:
            # handle db duplicated case specifically:
            if len(e.args) >= 2:
                if isinstance(e.args[0], (int, long)):
                    if e.args[0] == 1062:
                        raise CODuplicatedDBRecord(e)
                    # always raise a exception
            raise e

        if sql_action == COConstants.SQL_ACTION_COMMON:
            return CommonResult(res=result, why=why)

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
            """
            NOTICE:
            return 'None' if no records found, else a list, regardless of '@only_one'.
            """
            if isinstance(result, MySQLdb.cursors.DictCursor):
                # get raw results first
                if only_one:
                    v = result.fetchone()
                    raw_results = [v] if v else []
                else:
                    raw_results = result.fetchall() # list

                # refine it
                if model_class:
                    refined_results = []
                    for i in raw_results:
                        m = model_class()
                        if extra_model_fields:
                            m.registerExtraFields(extra_model_fields)
                        m.setDBData( i )
                        refined_results.append(m)
                else:
                    refined_results = raw_results

                # OK, do return stuff
                if refined_results == []:
                    result = None
                elif only_one:
                    result = refined_results[0]
                else:
                    result = refined_results
            return SelectResult(res=result, why=why)

        why = "UNSUPPORTED sql action: '%s', should never happened" % sql_action
        raise COSqlExecuteError(why)

# global
g_co_executor = SqlExcecutor()


class Transaction(object):
    def __init__(self, executor=g_co_executor ):
        self._executor = executor

    def __enter__(self):
        self._executor.onStartTransaction()
        l.info("starting transaction...")

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._executor.onEndTransaction()
        if not exc_tb:
            self._executor.commit()
            l.info("commit and transaction success")
            return True
        # failed, do rollback
        l.error("transaction failed, rollback, caz: '%s', '%s'" % (exc_type, exc_value))
        self._executor.rollback()
        return False


def do_transaction(func):
    def _f_wrapper(*args, **kwargs):
        r = None
        with Transaction():
            r = func(*args, **kwargs)
        return r
    return _f_wrapper

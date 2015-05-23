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
from co_error import COExcInternalError, COSqlExecuteError
from co_constants import COConstants
from co_utils import g_utils, Magic


class InsertResult(Magic):
    def __init__(self, **kwargs):
        super(InsertResult, self).__init__(**kwargs)


class UpdateResult(Magic):
    def __init__(self, **kwargs):
        super(InsertResult, self).__init__(**kwargs)


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
        self.conn = None
        self._connect_to_db()

    def _connect_to_db(self):
        if self._mysql.host == None:
            print "WARNING: failed to connect to database:%s@%s:%s" % \
                (self._mysql.user, self._mysql.host, self._mysql.port)
            return
        try:
            self.conn = MySQLdb.connect( host=self._mysql.host,
                                         port=self._mysql.port,
                                         user=self._mysql.user,
                                         passwd=self._mysql.passwd,
                                         db=self._mysql.db )
        except MySQLdb.OperationalError as e:
            raise COExcInternalError(e)
        except Exception as e:
            raise COExcInternalError(e)

    def _execute_sql(self, sql, sql_action=None):
        """return None or action-specified value
        """
        if not self.conn:
            raise COExcInternalError("not connect to db yet")

        c = MySQLdb.cursors.DictCursor( self.conn )
        try:
            print "executing SQL:\"%s\" @'%s'" % (sql, g_utils.now())
            c.execute( sql )
            self.conn.commit()
            print "[DIAGNOSE] executed: \"%s\" @'%s', info:'%s'" % (c._last_executed, g_utils.now(), c.__dict__)
            return c
        except MySQLdb.IntegrityError as e:
            raise COSqlExecuteError(*e.args)
        except MySQLdb.ProgrammingError as e:
            raise COSqlExecuteError(*e.args)
        except Exception as e:
            raise COExcInternalError(*e.args)

    def reInit(self, mysql_host=DEFAULT_MYSQL_HOST, mysql_port=DEFAULT_MYSQL_PORT, mysql_user='', mysql_passwd='', mysql_db=''):
        self._mysql = Magic( host=mysql_host,
                             port=mysql_port,
                             user=mysql_user,
                             passwd=mysql_passwd,
                             db=mysql_db
                         )
        self.conn = MySQLdb.connect( host=self._mysql.host,
                                     port=self._mysql.port,
                                     user=self._mysql.user,
                                     passwd=self._mysql.passwd,
                                     db=self._mysql.db )

    def execute(self, sql, sql_action, model_class=None, model_class_fields=None):
        """
        1. get db datas by running sql
        2. new model and assign datas to models
        @model_class: the model to which the data will be assigned
        @model_class_fields: is used only when model_class is subclass of ModelIface

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
            result, why = self._execute_sql(sql, sql_action), 'OK'
            print "DEBUG: Executed result of SQL:\"%s\" is:'%s'" % (sql, result)
        except Exception as e:
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
            return DeleteResult(res=result, why=why)

        if sql_action == COConstants.SQL_ACTION_DELETE:
            result = True if isinstance(result, MySQLdb.cursors.DictCursor) else False
            return DeleteResult(res=del_res, why=why)

        if sql_action == COConstants.SQL_ACTION_SELECT:
            if isinstance(result, MySQLdb.cursors.DictCursor):
                if model_class:
                    result = [ model_class(model_class_fields).setDBData(i) for i in result.fetchall() ]
                else:
                    result = result.fetchall()
            return SelectResult(res=result, why=why)

        print "UNSUPPORTED sql action: '%s', should never happened" % sql_action

# global
g_co_executor = SqlExcecutor()

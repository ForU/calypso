#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Sat May  9 13:00:29 2015
# ht <515563130@qq.com, weixin:jacoolee>


from co_logger import l

class CoBaseException(Exception):
    def __init__(self, *args, **kwargs):
        l.error('exception:%s why:%s %s' % (self.__class__, args, kwargs))
        super(Exception, self).__init__(*args, **kwargs)


class COExcInvalidSql(CoBaseException):
    def __init__(self, *args, **kwargs):
        super(COExcInvalidSql, self).__init__(*args, **kwargs)


class COExcInternalError(CoBaseException):
    def __init__(self, *args, **kwargs):
        super(COExcInternalError, self).__init__(*args, **kwargs)


class COSqlExecuteError(CoBaseException):
    def __init__(self, *args, **kwargs):
        super(COSqlExecuteError, self).__init__(*args, **kwargs)


class CODuplicatedDBRecord(CoBaseException):
    """app-level exception other than calypso exception
    """
    def __init__(self, *args, **kwargs):
        super(CODuplicatedDBRecord, self).__init__(*args, **kwargs)

#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Sat May  9 13:00:29 2015
# ht <515563130@qq.com, weixin:jacoolee>


class COExcInvalidSql(Exception):
    def __init__(self, *args, **kwargs):
        super(Exception, self).__init__(*args, **kwargs)


class COExcInternalError(Exception):
    def __init__(self, *args, **kwargs):
        super(Exception, self).__init__(*args, **kwargs)


class COSqlExecuteError(Exception):
    def __init__(self, *args, **kwargs):
        super(Exception, self).__init__(*args, **kwargs)


class CODuplicatedDBRecord(Exception):
    """app-level exception other than calypso exception
    """
    def __init__(self, *args, **kwargs):
        super(Exception, self).__init__(*args, **kwargs)

#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Sun May 10 00:20:04 2015
# ht <515563130@qq.com, weixin:jacoolee>

import MySQLdb


def sep(msg):
    print '_'*100
    print "# %s" % msg

conn = MySQLdb.connect(user='root', passwd='', db='go')

c = MySQLdb.cursors.DictCursor(conn) # dict cursor

sep("execute sql")
sql = "select * from person limit 1"
print sql
c.execute(sql)

sep("results")
print c.fetchall()

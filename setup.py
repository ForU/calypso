#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Sun May 10 01:31:47 2015
# ht <515563130@qq.com, weixin:jacoolee>
import os

try:
    from setuptools import setup
except ImportError:
    pass

from calypso import co_version

setup (
    name = 'calypso',
    version = co_version.version,
    keywords = ('The God Of The Sea'),
    description = 'A Simple Mysql Db Access Layer',
    author = 'jacoolee (515563130@qq.com)',
    packages = [ 'calypso' ],
)

useless_files = "build dist calypso.egg-info"
print "removing non-sense files: %s [%s]" % ( useless_files, 'OK' if 0 == os.system( 'rm -rf %s' % useless_files ) else 'FAILED' )
print "calypso successfully installed, well done"

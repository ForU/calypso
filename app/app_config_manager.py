#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Fri May  8 16:29:32 2015
# ht <515563130@qq.com, weixin:jacoolee>

import sys
import imp

from app_utils import DeepMagic

class ConfigureManager(object):
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        self.CNF = self.getConfigureData(self.config_file_path)

    def getConfigureData(self, config_file_path=None):
        self.config_file_path = config_file_path if config_file_path else self.config_file_path
        print "configure file: '%s'" % self.config_file_path
        try:
            mdl_key = self.config_file_path.replace('/', '.') # self.config_file_path unchanged
            cnf_mdl = imp.load_source(mdl_key, self.config_file_path)
            return DeepMagic( ** cnf_mdl.CNF )
        except Exception as e:
            print "failed to load:'%s' or no configure data, please check, why:'%s'" % \
                (self.config_file_path, e)
            sys.exit(-1)

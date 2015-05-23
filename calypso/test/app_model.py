#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Fri May  8 15:09:42 2015
# ht <515563130@qq.com, weixin:jacoolee>

"""
a example of generated app model file
"""

from test import Field, ModelIface, Table

class Person_Model(ModelIface):
    TABLE_NAME = 'person'      # must have
    def __init__(self):
        super(Person_Model, self).__init__()
        self.id = Field(name='id', type=int, default=0)
        self.name = Field(name='name', type=str, default='')
        self.sex = Field(name='sex', type=str, default='') # ENUM
        self.age = Field(name='age', type=str, default='')


class Person(Table):
    def __init__(self):
        super(Person, self).__init__(Person_Model)

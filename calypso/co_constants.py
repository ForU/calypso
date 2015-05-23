#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Sun May 10 00:48:19 2015
# ht <515563130@qq.com, weixin:jacoolee>

class COConstants(object):
    # sql action
    SQL_ACTION_INSERT = 0
    SQL_ACTION_DELETE = 1
    SQL_ACTION_SELECT = 2
    SQL_ACTION_UPDATE = 3

    VALID_SQL_ACTIONS = (
        SQL_ACTION_INSERT,
        SQL_ACTION_DELETE,
        SQL_ACTION_SELECT,
        SQL_ACTION_UPDATE
    )


    # join mode
    JOIN_MODE_INNER = 'INNER JOIN'
    JOIN_MODE_CROSS = 'CROSS JOIN'
    JOIN_MODE_STRAIGHT = 'STRAIGHT_JOIN'
    JOIN_MODE_LEFT = 'LEFT JOIN'
    JOIN_MODE_RIGHT = 'RIGHT JOIN'

    # micro for insert err, notation:
    # var name as err information and value as the opposite
    # of the real mysql error for easy bug-tracking
    RECORD_DUPLICATED = -1062
    INVALID_SQL = -1064

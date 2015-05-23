#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Sat May  9 10:07:32 2015
# ht <515563130@qq.com, weixin:jacoolee>

import time


class Magic(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self, ):
        return str(self.__dict__)


class DeepMagic(object):
    def __init__(self, *args, **kwargs):
        self.__raw__ = kwargs
        setattr(self, 'args', args)
        def __inner_do_magic(obj, d):
            for k,v in d.items():
                if not isinstance(v, dict):
                    setattr(obj, k, v)
                    continue
                else:
                    setattr(obj, k, BareBone())
                    bbone = getattr(obj, k)
                    __inner_do_magic( bbone, v )
        # main
        __inner_do_magic(self, kwargs)


class Utils(object):
    _STANDARD_TIME_FORMAT_IN_REGEX = '\d{4}-\d{2}-\d{2} +\d{2}:\d{2}:\d{2}'
    _STANDARD_TIME_FORMAT_IN_TIME = '%Y-%m-%d %H:%M:%S'
    def __init__(self, ):
        self.why = ''

    @classmethod
    def trace_exc(cls):
        exception_infos = traceback.format_exc().split('\n')
        for i in exception_infos:
            print ( "trace:'%s'" % i.lstrip() )

    def check_var(self, var, var_type, var_default=None, allow_none=True):
        # reset why
        self.why = 'OK'
        # 1. default check must comes first,
        # 2. we allow default varue can be different from var_type,
        #    so check var_default and type together.
        # 3. check wether the var allow to be None.
        is_type_valid = type(var) in var_type if type(var_type) in (tuple,list) else isinstance(var, var_type)
        cond = (var == var_default or is_type_valid) and (allow_none or var is not None)
        if not cond:
            self.why = "var:'%s', def:'%s', expected type:'%s', given type:'%s', allow_none:'%s'" % (var, var_default, var_type, type(var), allow_none)
        return cond

    def toString(self, var):
        default_return_string = ''
        if isinstance(var, str):
            return var
        if isinstance(var, unicode):
            try:
                return var.encode('utf-8')
            except Exception as e:
                self.why = "exception caught: '%s'" % e
                self.trace_exc()
                return default_return_string
        # others
        try:
            return str(var)
        except Exception as e:
            self.why = "exception caught: '%s'" % e
            self.trace_exc()
            return default_return_string

    def secs2datetime(self, seconds_since_ep, tm_fmt="%Y-%m-%d %H:%M:%S"):
        """seconds_since_ep: 1970-1-1 00:00:00
        """
        # 'int' is enough to hold the
        # BUG: 28800
        seconds_since_ep += 28800   # bugs
        try:
            return time.strftime(tm_fmt, time.gmtime(int(seconds_since_ep)))
        except Exception as e:
            self.trace_exc()
            return secs2datetime(0,tm_fmt)

    def datetime2secs(self, date_time_str, tm_fmt='%Y-%m-%d %H:%M:%S'):
        try:
            tm_tuple = time.strptime(date_time_str, tm_fmt)
            secs = int(time.strftime("%s", tm_tuple))
        except Exception as e:
            secs = 0
            self.trace_exc()
        finally:
            return secs

    def now(self, return_type='std'):
        _now = time.time()
        if return_type == 'int':
            return int(_now)
        if return_type == 'std':
            return self.secs2datetime(_now)
        # finally, 'raw'
        return _now

    def fitsStandardTimeFormat(self, raw_time_str):
        return None != re.match(self._STANDARD_TIME_FORMAT_IN_REGEX , raw_time_str)

    def md5(self, raw, return_type='string'):
        obj = hashlib.md5(self.toString(raw))
        return obj.hexdigest() if return_type == 'string' else obj

g_utils = Utils()

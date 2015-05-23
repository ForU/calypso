#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Wed May  6 17:45:59 2015
# ht <515563130@qq.com, weixin:jacoolee>

CONDITION_ITEM_TYPE_CONJ = 'conj'
CONDITION_ITEM_TYPE_ATOM = 'atom'

class ConditionItemBase(object):
    def __init__(self, type=CONDITION_ITEM_TYPE_ATOM, field=None, value=None):
        self.type = type
        self.field = field
        self.value = value

    @property
    def _class(self):
        return str(self.__class__)[8:-2]

    def sql(self):
        raise NotImplementedError('%s.sql() not implememted' % self._class)

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)


class Operator(ConditionItemBase):
    def __init__(self, operator=None, field=None, value=None):
        super(Operator, self).__init__(field=field, value=value)
        self.operator = str(operator)

    def sql(self):
        return "(%s %s %s)" % (self.field.sql(), self.operator, self.value)


class And(ConditionItemBase):
    def __init__(self, cond_item_l, cond_item_r):
        super(And, self).__init__(type=CONDITION_ITEM_TYPE_CONJ)
        self._cond_item_l = cond_item_l
        self._cond_item_r = cond_item_r

    def sql(self):
        return "(%s AND %s)" % (self._cond_item_l.sql(), self._cond_item_r.sql())


class Or(ConditionItemBase):
    def __init__(self, cond_item_l, cond_item_r):
        super(Or, self).__init__(type=CONDITION_ITEM_TYPE_CONJ)
        self._cond_item_l = cond_item_l
        self._cond_item_r = cond_item_r

    def sql(self):
        return "(%s OR %s)" % (self._cond_item_l.sql(), self._cond_item_r.sql())


class Not(ConditionItemBase):
    def __init__(self, cond_item):
        super(Not, self).__init__(type=CONDITION_ITEM_TYPE_CONJ)
        self._cond_item = cond_item

    def sql(self):
        return "NOT %s" % self._cond_item.sql()


class In(ConditionItemBase):
    def __init__(self, field, values):
        super(In, self).__init__(type=CONDITION_ITEM_TYPE_CONJ, field=field)
        self.values = values

    def sql(self):
        return "%s IN %s" % (self.field.sql(), self.values)

class Like(ConditionItemBase):
    def __init__(self, field, like_expr):
        super(Like, self).__init__(type=CONDITION_ITEM_TYPE_CONJ, field=field)
        self.like_expr = like_expr

    def sql(self):
        return "%s LIKE \"%s\"" % (self.field.sql(), self.like_expr)


class FieldTypeEnum(object):
    def __init__(self, ):
        # TODO Fri May  8 15:12:57 2015 []
        pass


class Field(object):
    def __init__(self, name=None, type=None, default=None, comment=None):
        self.name = name
        self.type = type
        self.default = default
        self.comment = comment

    def _f_built_in(self, operator_str, other):
        if not isinstance(other, (self.type, Field)):
            print "Error: right value is not type:'%s' or %s" % (self.type, 'Field')
            return
        return Operator(operator_str, self, other)

    def __eq__(self, other): return self._f_built_in('==', other)

    def __ne__(self, other): return self._f_built_in('!=', other)

    def __ge__(self, other): return self._f_built_in('>=', other)

    def __gt__(self, other): return self._f_built_in('>', other)

    def __le__(self, other): return self._f_built_in('<=', other)

    def __lt__(self, other): return self._f_built_in('<', other)

    def sql(self):
        return self.name

# TODO Thu May  7 23:51:22 2015 [Function from Field]
# TODO Thu May  7 23:51:40 2015 [model]

class SqlExcecutor(object):
    # TODO Fri May  8 14:23:51 2015 []
    def __init__(self):
        pass

    def execute(self, sql, model_class=None):
        # TODO Fri May  8 01:03:36 2015 []
        pass


class Select(object):
    def __init__(self, table=None, leak_fields=None):
        self._table = table
        self._leak_fields = leak_fields if isinstance(leak_fields, (list, tuple)) else [leak_fields]
        self._where = None
        self._order_by = None
        self._order_by_order = None
        self._group_by = None
        self._group_by_order = None
        self._limit_row_count = None
        self._limit_offset = None

    def where(self, cond):
        self._where = cond
        return self

    def orderBy(self, field, order='ASC'):
        self._order_by = field
        self._order_by_order = order
        return self

    def groupBy(self, field, order='ASC'):
        self._group_by = field
        self._group_by_order = order
        return self

    def limit(self, row_count, offset):
        self._limit_row_count = str(row_count)
        self._limit_offset = str(offset)
        return self

    def sql(                             self):
        # Make sure the order of component items are consistent
        # with `SELECT` sql syntax.
        components = [
            'SELECT'
            , ','.join([i.sql() for i in self._leak_fields])
            , 'FROM'
            ,               self._table.sql()
            , 'WHERE '    + self._where.sql() if self._where else ''
            , 'ORDER BY ' + self._order_by.sql() if self._order_by else ''
            ,               self._order_by_order if self._order_by_order else ''
            , 'GROUP BY ' + self._group_by.sql() if self._group_by else ''
            ,               self._group_by_order if self._group_by_order else ''
            , 'LIMIT '    + str( self._limit_row_count) if self._limit_row_count else ''
            , 'OFFSET '   + str( self._limit_offset) if self._limit_offset else ''
        ]
        # exclude empty items for generating pretty sql.
        components = [i for i in components if i]
        return (' '.join(components)).strip()

    def execute(self):
        # TODO Fri May  8 14:22:11 2015 []
        pass

class ModelIface(object):
    TABLE_NAME = None           # must exist in generated code.
    def __init__(self):
        pass


class TableIface(object):
    """a interface definition of table
    TODO: sucks, caz at least one model_class object
    need to be created.
    """
    def __init__(self, model_class):
        self._model_class = model_class     # definition
        self._name = self._model_class.TABLE_NAME

        self._register_model_fields()

    def _register_model_fields(self):
        ins_model = self._model_class()
        model_field_map = { k:v for k,v in ins_model.__dict__.items() if isinstance(v, Field) }
        self.__dict__.update( model_field_map )

    @property
    def _class(self):
        return str(self.__class__)[8:-2]

    def insert(self, dict_data_or_datas, mode=None):
        raise NotImplementedError('%s.insert() not implememted' % self._class)

    def update(self, cond, dict_data):
        raise NotImplementedError('%s.update() not implememted' % self._class)

    def delete(self, cond):
        raise NotImplementedError('%s.delete() not implememted' % self._class)

    def select(self, leak_fields=None):
        raise NotImplementedError('%s.select() not implememted' % self._class)

    def sql(self):
        return self._name


class Table(TableIface):
    def __init__(self, model_class):
        super(Table, self).__init__(model_class=model_class)

    def insert(self, dict_data_or_datas, mode=None):
        datas = dict_data_or_datas if isinstance(dict_data_or_datas, (list, tuple)) else list(dict_data_or_datas)
        # TODO Fri May  8 14:23:00 2015 []
        # gen sql
        # do insert
        # return stuff

    def select(self, leak_fields=None):
        """return a Select object. notice table is set to be self.
        """
        return Select(table=self, leak_fields=leak_fields)

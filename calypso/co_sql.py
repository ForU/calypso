#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Wed May  6 17:45:59 2015
# ht <515563130@qq.com, weixin:jacoolee>

CONDITION_ITEM_TYPE_CONJ = 'conj'
CONDITION_ITEM_TYPE_ATOM = 'atom'

import copy
import MySQLdb

from co_utils import Magic
from co_error import COExcInvalidSql, COExcInternalError
from co_constants import COConstants
from co_logger import l

FIELD_KEY_PREFIX = '__f_'
FIELD_KEY_PREFIX_LEN = len(FIELD_KEY_PREFIX)

def _Pack_Field_Key(raw_field_name):
    return FIELD_KEY_PREFIX + raw_field_name

def _Unpack_Field_Key(packed_field_name):
    return packed_field_name[FIELD_KEY_PREFIX_LEN:]

def escape(raw_string):
    return MySQLdb.escape_string(raw_string)

class ConditionItemBase(object):
    def __init__(self, type=CONDITION_ITEM_TYPE_ATOM, field=None, value=None):
        self.type = type
        self.field = field
        self.value = value

    @property
    def _class(self):
        return str(self.__class__)[8:-2]

    def sql(self, *args, **kwargs):
        raise NotImplementedError('%s.sql() not implememted' % self._class)

    def __and__(self, other):
        return And(self, other)

    def isEmpty(self):
        return False

    def __or__(self, other):
        return Or(self, other)


class Condition(ConditionItemBase):
    """a init condition
    """
    def __init__(self):
        super(ConditionItemBase, self).__init__()

    def sql(self,):
        return None

    def isEmpty(self):
        return True

    def __and__(self, cond):
        return cond

    def __or__(self, cond):
        return cond


class Operator(ConditionItemBase):
    def __init__(self, operator=None, field=None, value=None):
        super(Operator, self).__init__(field=field, value=value)
        self.operator = str(operator)

    def sql(self):
        # caz Field the root class, so subclass still ok
        if isinstance(self.value, Field):
            right_value = self.value.sql()
        elif isinstance(self.value, Select):
            right_value = "(%s)" % str(self.value.sql())
        elif isinstance(self.value, FieldTypeEnum):
            right_value = '"%s"' % str(self.value)
        elif isinstance(self.value, str):
            right_value = '"%s"' % str(self.value)
        else:
            right_value = str(self.value)
        return "(%s %s %s)" % (self.field.sql(), self.operator, right_value)


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

        # check validation of incomming stuff
        if not isinstance(values, ConditionItemBase) and not isinstance(values, str) and (not (isinstance(values, (list, tuple)) and len(values) > 0)):
            raise COExcInternalError("invalid args:@values:'%s', type:'%s'" % \
                                     (values, type(values)))

    def sql(self):
        if isinstance(self.values, ConditionItemBase):
            return "%s IN (%s)" % (self.field.sql(), self.values.sql())

        if isinstance(self.values,str):
            return "%s IN (%s)" % (self.field.sql(), self.values)

        # else (list,tuple)
        items = ['"%s"'%(i) for i in self.values]
        if len(items) > 1:
            return "%s IN (%s)" % (self.field.sql(), ','.join(items))
        if len(items) == 1:
            return "%s = %s" % (self.field.sql(), items[0])


LIKE_NONE = 0
LIKE_LEFT = 1
LIKE_RIGHT = 2
LIKE_BOTH = 3
class Like(ConditionItemBase):
    def __init__(self, field, like_expr, fuzzy=LIKE_NONE, caseSensitive=False):
        super(Like, self).__init__(type=CONDITION_ITEM_TYPE_CONJ, field=field)
        self.like_expr = like_expr
        self.fuzzy = fuzzy
        self.caseSensitive = caseSensitive

    def sql(self):
        if self.fuzzy == LIKE_NONE:
            return '%s LIKE %s "%s"' % (self.field.sql(), 'BINARY' if self.caseSensitive else '', self.like_expr)
        if self.fuzzy == LIKE_LEFT:
            return '%s LIKE %s "%%%s"' % (self.field.sql(), 'BINARY' if self.caseSensitive else '', self.like_expr)
        if self.fuzzy == LIKE_RIGHT:
            return '%s LIKE %s "%s%%"' % (self.field.sql(), 'BINARY' if self.caseSensitive else '', self.like_expr)
        if self.fuzzy == LIKE_BOTH:
            return '%s LIKE %s "%%%s%%"' % (self.field.sql(), 'BINARY' if self.caseSensitive else '', self.like_expr)
        raise COExcInvalidSql('unsupported like fuzzy type:'+str(self.fuzzy))

class FieldTypeEnum(object):
    def __init__(self, str_enum_value=None):
        self.v = str_enum_value # string

    def __str__(self):
        return self.v

    def __repr__(self):
        return "<FieldTypeEnum:%s>" % self.v

    def __eq__(self, rv):
        return self.v == rv.v


class Field(object):
    def __init__(self, name=None, type=None, default=None, comment=None, value=None, restriction=None, definition=None, accepted_right_value_types=None):
        self.name = name
        self.type = type
        self.default = default
        self.comment = comment
        self.value = value
        self.table = None
        self.f_as = None
        self.restriction = restriction
        self.accepted_right_value_types = accepted_right_value_types
        # about restriction
        # 1. enum, range, time, and so on.
        if definition: self.setByDefinition( definition )

    def _f_built_in(self, operator_str, other, as_operator=True):
        cand_types = [self.type, Field, Function, Select]

        # refine cand_types.
        if self.accepted_right_value_types:
            cand_types.extend(self.accepted_right_value_types)

        if self.type in [int, long, FieldTypeEnum]:
            cand_types.append( str )
            cand_types.append( long )

        if not isinstance( other, tuple(cand_types) ):
            l.warn("operator_str:'%s', right value:'%s' is not type:'%s' or %s, candidate types:%s, as_operator:'%s'" % (operator_str, other, self.type, 'Field', cand_types, as_operator))
            return

        if as_operator:
            return Operator(operator_str, self, other)
        else:
            other = self._right2Field(other)
            _name_sql = ' '.join([self.sql(), operator_str, other.sql()])
            return Field(name=_name_sql, type=other.type)

    def __str__(self, ):
        return str(self.value or '')

    def _right2Field(self, right):
        if isinstance(right, Field):
            return right

        if type(right) in (int,long,str,unicode):
            return RAW(right)

        return right

    def __eq__(self, other): return self._f_built_in('=', other)

    def __ne__(self, other): return self._f_built_in('!=', other)

    def __ge__(self, other): return self._f_built_in('>=', other)

    def __gt__(self, other): return self._f_built_in('>', other)

    def __le__(self, other): return self._f_built_in('<=', other)

    def __lt__(self, other): return self._f_built_in('<', other)

    def __sub__(self, other): return self._f_built_in('-', other, as_operator=False)

    def __add__(self, other): return self._f_built_in('+', other, as_operator=False)

    def reset(self, other, keep_old_name=True):
        """reset current field with new definition of other.
        @other: other field
        NOTICE: if you wanna change the return name, use field.AS please.
        """
        if keep_old_name:
            self.AS(self.name)

        self.name = other.sql()
        self.type = other.type
        self.default = other.default
        self.comment = other.comment
        self.value = other.value
        self.restriction = other.restriction
        self.definition = other.definition

        return self

    def toInsertStyleStr(self, raw_value):
        """CRITICAL: every type of the field should support '__str__'
        """
        if isinstance(raw_value, (int, long)):
            return "%s" % raw_value
        if isinstance(raw_value, (str, FieldTypeEnum)):
            return '"%s"' % escape(raw_value)
        if isinstance(raw_value, (Field, Function)):
            return "%s" % raw_value.sql()
        if isinstance(raw_value, unicode):
            return '"%s"' % raw_value.encode('utf8')

        # default.
        return '"%s"' % raw_value

    def isEnum(self):
        return self.type == FieldTypeEnum

    def definition(self):
        return { 'name':self.name, 'type':self.type, \
                 'default':self.default, 'comment':self.comment}

    def setByDefinition(self, definition):
        self.name=definition['name']
        self.type=definition['type']
        self.default=definition.get('default')
        self.comment=definition.get('comment')
        return self

    def AS(self, alias):
        self.f_as = alias
        return self

    def _gen_sql_prefix(self):
        """@enable_as: just for derived interface.
        """
        tbl_prefix = self.table.sql(as_prefered=True) + '.' if self.table else ''
        return tbl_prefix + self.name

    def _gen_sql_as(self, enable_as=False):
        if enable_as and self.f_as:
            return " AS %s" % self.f_as
        return ''

    def sql(self, enable_as=False):
        prefix = self._gen_sql_prefix()
        sql_as = self._gen_sql_as(enable_as=enable_as)
        return prefix + sql_as


STAR_FIELD = Field(name='*', type=str, default='*', comment='star')


FieldTypeFunction = str         # default type for function-type field

class RAW(Field):
    def __init__(self, value):
        super(RAW, self).__init__( value=value )

    def sql(self):
        return self.toInsertStyleStr(self.value)

class ORDER_BY_FIELD(object):
    def __init__(self, field, fieldValueCondidates, doWrapFieldValueCondidatesWithParenthsis=False):
        self._f = field
        self._fvc = fieldValueCondidates
        self._doWrapFieldValueCondidatesWithParenthsis = doWrapFieldValueCondidatesWithParenthsis

    def sql(self):
        if self._doWrapFieldValueCondidatesWithParenthsis:
            return 'FIELD(' + self._f.sql() + ', (' + self._fvc + '))'
        else:
            return 'FIELD(' + self._f.sql() + ', ' + self._fvc + ')'


class GROUP_CONCAT(Field):
    def __init__(self, expr, orderBy=None, asc=True, separator=', '):
        super(GROUP_CONCAT, self).__init__(name='GROUP_CONCAT', type=str, default='', comment='')
        self._expr = expr
        self._order_by = orderBy
        self._asc = asc
        self._separator = separator

    def _gen_sql_prefix(self):
        """
        GROUP_CONCAT([DISTINCT] expr [,expr ...]
             [ORDER BY {unsigned_integer | col_name | expr}
                 [ASC | DESC] [,col_name ...]]
             [SEPARATOR str_val])
        """
        return ''.join([self.name, '('
                        , self._expr.sql()
                        , ' ORDER BY ' + self._order_by.sql() if self._order_by else '' # ORDER
                        , ' ASC' if self._asc else ' DESC'
                        , ' SEPARATOR \'', self._separator,'\')' # SEPARATOR
                    ])


class DISTINCT(Field):
    def __init__(self, *fields):
        super(DISTINCT, self).__init__(name='DISTINCT', type=str, default='DISTINCT', comment='')
        self._fields = fields

    def _gen_sql_prefix(self):
        self.name = self.name + ' ' + ','.join([i.sql(enable_as=False) for i in self._fields])
        return super(DISTINCT, self)._gen_sql_prefix()


class Function(Field):
    """
    override #function:_gen_sql_prefix(...) to implement your function if necessary,
    if that can not fit your needs, try to override #function:sql directly.

    NOTICE:
    current function only support one-argument. if more arguments need to
    be supported, write your own version. GOOD LUCK :)
    """
    def __init__(self, name,
                 field=None,
                 type=FieldTypeFunction,
                 accepted_right_value_types=None ):
        self.field = field
        super(Function, self).__init__(name = name,
                                       type = type,
                                       accepted_right_value_types = accepted_right_value_types)
        self._gen_f_as_backup()

    def _gen_f_as_backup(self):
        try:
            self.f_as = str(self.__class__)[:-2].split('.')[-1]
        except Exception as e:
            l.error("failed to generate f as backup for function:'%s', do nothing, let app-level known this error" % self.__class__)

    def _gen_sql_prefix(self):
        field_prefix = self.field.sql() if self.field else ''
        return "%s(%s)" % (self.name, field_prefix)


# CRITICAL: for function class name, we use UPPERCASE to make the name
# would not conficts with db fields FOR MOST CASES.
# COUNT, MAX, ...

class COUNT(Function):
    def __init__(self, field=STAR_FIELD, type=long):
        super(COUNT, self).__init__(name='COUNT', field=field, type=type)


class MAX(Function):
    def __init__(self, field):
        super(MAX, self).__init__(name='MAX', field=field)

class SUM(Function):
    def __init__(self, field):
        super(SUM, self).__init__(name='SUM', field=field, type=float)

class TO_SECONDS(Function):
    def __init__(self, field):
        super(TO_SECONDS, self).__init__(name='TO_SECONDS', field=field)

class UNIX_TIMESTAMP(Function):
    def __init__(self, field):
        super(UNIX_TIMESTAMP, self).__init__(name='UNIX_TIMESTAMP', field=field, accepted_right_value_types=[int, long, str])


class NOW(Function):
    def __init__(self):
        super(NOW, self).__init__(name='NOW')


# ... other functions here


class Select(ConditionItemBase):
    def __init__(self, table=None, leak_fields=None, extra_field_definitions={}):
        self._table = table
        self._leak_fields = None
        self._where = None
        self._order_by = None
        self._order_by_order = None
        self._group_by = None
        self._group_by_order = None
        self._limit_row_count = None
        self._limit_offset = None
        self._extra_field_definitions = extra_field_definitions

        if leak_fields:
            if isinstance(leak_fields, (list, tuple)):
                self._leak_fields = leak_fields
            else:
                self._leak_fields = [leak_fields]

    def where(self, cond=None):
        if cond:
            self._where = cond
        return self

    def orderBy(self, field, asc=True):
        self._order_by = field
        self._order_by_order = 'ASC' if asc else 'DESC'
        return self

    def groupBy(self, field, asc=True):
        self._group_by = field
        self._group_by_order = 'ASC' if asc else 'DESC'
        return self

    def limit(self, row_count=None, offset=0):
        self._limit_row_count = row_count
        self._limit_offset = offset
        return self

    def sql(self):
        # Make sure the order of sql component items are consistent
        # with `SELECT` sql syntax.
        lk_flds = '*'               # default
        if self._leak_fields:
            lk_flds = ','.join([i.sql(enable_as=True) for i in self._leak_fields])

        where_value = (self._where.sql() or '') if self._where else ''
        sql_components = [
            'SELECT'
            , lk_flds
            , 'FROM'
            ,               self._table.sql()
            , 'WHERE '    + where_value if where_value else ''
            , 'ORDER BY ' + self._order_by.sql() if self._order_by else ''
            ,               escape(self._order_by_order) if self._order_by_order else ''
            , 'GROUP BY ' + escape(self._group_by.sql()) if self._group_by else ''
            ,               escape(self._group_by_order) if self._group_by_order else ''
            , 'LIMIT '    + escape(str( self._limit_row_count)) if self._limit_row_count else ''
            , 'OFFSET '   + escape(str( self._limit_offset)) if self._limit_offset else ''
        ]
        # exclude empty items for pretty SQL.
        sql_components = [i for i in sql_components if i]
        return (' '.join(sql_components))

    def one(self):
        return self._execute(only_one=True)

    def whole(self):
        return self._execute(only_one=False)

    def _execute(self, only_one=False):
        efds = self._extra_field_definitions or {} # for join, maybe empty: {}
        if self._leak_fields:
            efds.update( { _Pack_Field_Key(i.f_as):i.definition() for i in self._leak_fields if i.f_as } ) # leak field as
        return self._table._t_executor.execute (
            self.sql(),
            sql_action = COConstants.SQL_ACTION_SELECT,
            model_class = self._table._t_model_class,
            extra_model_fields = efds,
            only_one = only_one )


class ModelIface(object):
    DB_NAME = ''
    TABLE_NAME = ''             # must exist in generated code.
    def __init__(self, db_name=None, table_name=None):
        if db_name: self.DB_NAME = db_name
        if table_name: self.TABLE_NAME = table_name

    def __repr__(self, ):
        return str(self.__class__).split(' ')[1][1:-2]+'@'+self.TABLE_NAME

    def __str__(self, ):
        return self.dumpAsStr()

    def _is_field_registed(self, field_name):
        """
        """
        f_key = _Pack_Field_Key(field_name)
        f = self.__dict__.get(f_key)
        if not f:
            return False, None
        return isinstance(f, Field), f

    def addExtraAttributes(self, d_extra_attributes):
        #l.debug("already_keys:%s, new commers:%s" %  (self.__dict__.keys(), d_extra_attributes.keys()))
        self.__dict__.update(d_extra_attributes)

    def registerExtraFields(self, d_extra_field_definitions):
        for fnam, fdefi in d_extra_field_definitions.items():
            #l.debug("registering field:%s, fdefi:%s for '%s'" % (fnam, fdefi, self))
            self.__dict__[fnam] = Field(definition=fdefi)

    def setDBData(self, db_data={}):
        """set db_data as model attributes
        @db_data type: dict
        """
        for k,v in db_data.items():
            is_fld, f = self._is_field_registed(k)
            if not is_fld:
                l.warn("Field:'%s' not registed in Model:'%s', ignore it" % (k, self))
                continue
            try:
                # CRITICAL: set value when value is not None, default is None (set None explictly here)
                f.value = f.type(v) if v is not None else None
            except Exception as e:
                raise COExcInvalidSql(e)
        # CRITICAL: always return self
        return self

    def __setattr__(self, k, v):
        if isinstance(v, Field):
            k = _Pack_Field_Key(k)
        # CRITICAL:
        # always store it no matter whether @v is type of 'Field' or Not
        self.__dict__[k] = v

    def __getattr__(self, k):
        """ due to __getattr__'s characteristic, no need to check
        where @k is part to self.__dict__.
        """
        f_key = _Pack_Field_Key(k)
        v = self.__dict__.get(f_key, None)
        if v is None:
            raise AttributeError("no such key:'%s'" % k)
        # CRITICAL: handle Field and FieldTypeEnum
        if isinstance(v, Field):
            v = v.value
            return  v.v if isinstance(v, FieldTypeEnum) else v
        return v

    def fields(self):
        """get all fields of current model object
        """
        return [v for k,v in self.__dict__.items() if isinstance(v, Field)]

    def field(self, field_name):
        """get field, return Field named @field_name
        """
        f_key = _Pack_Field_Key(field_name)
        v = self.__dict__.get(f_key, None)
        if v is None:
            raise AttributeError("no such key:'%s'" % field_name)
        # CRITICAL: handle Field and FieldTypeEnum
        return v if isinstance(v, Field) else None

    def dumpAsStr(self,):
        return str({ _Unpack_Field_Key(k):str(v.value) \
                     for k,v in self.__dict__.items() if isinstance(v, Field) })

    def dumpAsDict(self,):
        return { _Unpack_Field_Key(k) : v.value.v if isinstance(v.value, FieldTypeEnum) else v.value \
                 for k,v in self.__dict__.items() if isinstance(v, Field) }

    def dumpAsDBData(self,):
        # CRITICAL: for safe insertion, we explicately set the default
        #           value for a field whose in SQL.
        #           And, the default value is gotten from ModelX which
        #           is autogenerated by model geneerator based on the
        #           database definition, so it's OK
        #
        # a little different from 'AsStr' is that, a DEFAULT VALUE is backup for more crucial situation.
        return { _Unpack_Field_Key(k):str(escape(v.value) if v.value else v.default) # v.value or v.default
                 for k,v in self.__dict__.items()
                 if isinstance(v, Field) and (v.value or v.isEnum()) }


class _DynamicModel(ModelIface):
    def __init__(self, dynamic_fields={}, db_name=None, table_name=None):
        """dynamically make items of dynamic_fields as instance attributes
        """
        for k,defin in dynamic_fields.items():
            self.__dict__[k] = Field(definition=defin)
        super(_DynamicModel, self).__init__(db_name=db_name, table_name=table_name)


class CompoundModel(_DynamicModel):
    def __init__(self, dynamic_fields={}, db_name=None, table_name=None):
        super(CompoundModel, self).__init__( dynamic_fields=dynamic_fields or {},
                                             db_name=db_name,
                                             table_name=table_name )


class JoinModel(_DynamicModel):
    def __init__(self, dynamic_fields={}, db_name=None, table_name=None):
        super(JoinModel, self).__init__( dynamic_fields=dynamic_fields or {},
                                         db_name=db_name,
                                         table_name=table_name )


class Join(object):
    pass


class TableIface(object):
    """
    A interface definition of table

    NOTICE: MAKE SURE ANY INSTANCES OF THIS CLASS WILL NOT BE INJECTED BY ANY OPERATIONS.

    Q. why do not inherit from ModelIface?
    A. 1. To keep interface clean, and in real, they have no inherit-relationship.
       2. seperate data and operation to make logic more clear
       3. more flexiable to extend.
    """
    def __init__(self, model_class, executor=None, db_name=None, table_name=None):
        """use heading '_' to make attribute wont be conflict with model fields
        """
        self._t_model_class = model_class     # model class definition
        self._t_executor = executor
        self._t_db_name = db_name if db_name else self._t_model_class.DB_NAME
        self._t_name = table_name if table_name else self._t_model_class.TABLE_NAME
        self._t_as = None
        self.ALL = STAR_FIELD

        self._register_model_fields()

    def __getattr__(self, k):
        """table.field => field_name
        """
        f_key = _Pack_Field_Key(k)
        v = self.__dict__.get(f_key, None)
        if v is None:
            raise AttributeError("no such key:'%s'" % k)
        return v                # CRITICAL: return Field for Condition

    def _get_fields(self, *model_classes):
        """@models: a list of model classes
        """
        model_field_map = {}
        for mc in model_classes:
            inst_mc, _map = mc(), {}
            for k,v in inst_mc.__dict__.items():
                if isinstance(v, Field):
                    v.table = self # reset table as self for table.as
                    _map[k] = v
            model_field_map.update( _map ) # CRITICAL: maybe overwrite
        return model_field_map

    def _register_fields(self, dict_fields={}):
        self.__dict__.update( dict_fields )

    def _register_model_fields(self):
        """register field of initialized TABLE MODE into current table instance
        """
        self._register_fields( self._get_fields(self._t_model_class) )

    @property
    def _class(self):
        return str(self.__class__)[8:-2]

    def AS(self, alias):
        # Use copy other than deepcopy for performance and memory saving.
        alias_obj = copy.deepcopy(self)
        alias_obj._t_as = alias
        # CRITICAL: An 1 billion dollar BUG, fuck it.
        alias_obj._t_executor = self._t_executor
        return alias_obj

    def setDbName(self, db_name):
        self._t_db_name = db_name

    def setTableName(self, table_name):
        self._t_name = table_name

    def insert(self, datas, *args, **kwargs):
        raise NotImplementedError('%s.insert() not implememted' % self._class)

    def update(self, dict_data, cond=None, *args, **kwargs):
        raise NotImplementedError('%s.update() not implememted' % self._class)

    def delete(self, cond, *args, **kwargs):
        raise NotImplementedError('%s.delete() not implememted' % self._class)

    def select(self, *leak_fields, **kwargs):
        # first of all exclude all None in leak_fields
        leak_fields = [getattr(self, i) if isinstance(i,str) else i for i in leak_fields if i]

        if isinstance(self, Join):
            efds = self._jn_dynamic_fields
        else:
            efds = {}
        return Select(table=self, leak_fields=leak_fields, extra_field_definitions=efds)

    def join(self, table_r, mode=COConstants.JOIN_MODE_INNER):
        return Join(self, table_r, mode=mode)

    def sql(self, as_prefered=False):
        """
        @as_prefered: means only give out table alias part, of-cause only when it exists
        """
        if as_prefered:
            if self._t_as:
                return self._t_as
        # use normal
        db_prefix = self._t_db_name +'.' if self._t_db_name else ''
        sql_basic = db_prefix + self._t_name
        if self._t_as:
            return sql_basic + " AS %s" % self._t_as
        else:
            return sql_basic


class Table(TableIface):
    def __init__(self, model_class, executor=None):
        super(Table, self).__init__(model_class=model_class, executor=executor)


    def _is_field_registed(self, field_name):
        """
        """
        f_key = _Pack_Field_Key(field_name)
        f = self.__dict__.get(f_key)
        if not f:
            return False, None
        return isinstance(f, Field), f

    def getField(self, field_name):
        return self.__dict__.get(field_name, Field(name='__null__'))

    def insert(self, datas, enable_ignore_mode=False):
        """
        @datas: can be following type:
        1. Coresponding Model
        2. single dict_data as:
        . format1: { table.field:v, table.field:v, table.field:v }
        . format2: {'field_name':v, 'field_name':v, 'field_name':v } # final type
        3. list of dict_datas
        """
        # refine datas first
        if not isinstance(datas, (list, tuple)):
            datas = [datas]
        refined_datas = []
        for i in datas:
            if isinstance(i, ModelIface):
                db_style_data = i.dumpAsDBData()
            else:               # dict
                db_style_data = {
                    # refine k,v
                    (k.sql() if isinstance(k, Field) else k):(v if isinstance(v, (Field,Function)) else v)
                    for k,v in i.items() if v is not None
                }
            refined_datas.append( db_style_data )

        # compose target columns
        target_columns = set()
        for i in refined_datas:
            target_columns.update(i.keys())
        target_columns = list(target_columns)

        # compose target columns' data
        column_datas = []
        for d in refined_datas:

            # convert raw value to the field's insert style string
            refined_d = []
            for c in target_columns:
                fld = self.getField(c)
                # 1. no key named '@c' in d
                col_v = d.get(c, 'NULL' if fld.default is None else fld.default )
                # 2. has key named '@c' in d, but value is 'None'
                #    we set the value directly other than save the backup as a var
                #    for performance
                if col_v is None:
                    col_v = 'NULL' if fld.default is None else fld.default

                refined_d.append( fld.toInsertStyleStr(col_v) )

            v = '(' + ','.join ( refined_d ) + ')'
            column_datas.append( v )

        # compose SQL
        sql_components = [ "INSERT%s" % (" IGNORE" if enable_ignore_mode else ""),
                           "INTO %s" % self.sql()
                           , "(%s)" % ','.join(target_columns)
                           , "VALUES"
                           , ','.join(column_datas)
                       ]
        sql = ' '.join(sql_components)
        return self._t_executor.execute(sql, sql_action=COConstants.SQL_ACTION_INSERT)

    def update(self, dict_data, cond=None, low_priority=False, ignore=False):
        update_set_items = []
        for k,v in dict_data.items():
            if k is None:
                continue

            if isinstance(k, Field):
                k = k.name
            if not self._is_field_registed(k)[0]:
                l.warn("no such field:'%s' in table:'%s'" % (k, self._t_name))
                continue

            # refine v
            if type(v) == str:
                v = '"%s"' % escape(v)
            elif isinstance(v, (Field, Function)):
                v = v.sql()

            if isinstance(v, unicode):
                v = '"%s"' % v.encode('utf8')

            us_item = "%s=%s" % (k, v)
            update_set_items.append(us_item)

        sql_components = [ 'UPDATE'
                           , 'LOW_PRIORITY' if low_priority else ''
                           , 'IGNORE' if ignore else ''
                           , self._t_name
                           , 'SET'
                           , ','.join( update_set_items )
                           , 'WHERE %s' % cond.sql() if cond else ''
                       ]
        # exclude empty items for pretty SQL.
        sql_components = [i for i in sql_components if i]
        sql = ' '.join(sql_components)
        return self._t_executor.execute(sql, sql_action=COConstants.SQL_ACTION_UPDATE)

    def delete(self, cond=None, low_priority=False, ignore=False, allow_none_cond=False):
        # CRITICAL: for safety, we do not allow cond to be None
        # unless allow_none_cond is TRUE which default is FALSE
        if not cond and not allow_none_cond:
            raise COExcInvalidSql("cond:'%s' is not allowed" % cond)
        sql_components = [ 'DELETE'
                           , 'LOW_PRIORITY' if low_priority else ''
                           , 'IGNORE' if ignore else ''
                           , 'FROM'
                           , self._t_name
                           , 'WHERE %s' % cond.sql() if cond else ''
                       ]
        # exclude empty items for pretty SQL.
        sql_components = [i for i in sql_components if i]
        sql = ' '.join(sql_components)
        return self._t_executor.execute(sql, sql_action=COConstants.SQL_ACTION_DELETE)


class Join(TableIface):
    """
    protocol #1: Join(table_left, table_right, mode=X).on( cond )
    protocol #2: Join(table_left, table_right, mode=X).using( column_list )

    # get following stuff properly set:
    _t_model_class

    # how to refine table name
    sql() # overwrite this function to refine table name

    """
    INNER    = COConstants.JOIN_MODE_INNER
    CROSS    = COConstants.JOIN_MODE_CROSS
    STRAIGHT = COConstants.JOIN_MODE_STRAIGHT
    LEFT     = COConstants.JOIN_MODE_LEFT
    RIGHT    = COConstants.JOIN_MODE_RIGHT
    # NATURAL = None              # ignore this case

    # join condition
    _JC = {
        INNER: Magic( required=True, allow_on=True, allow_using=True )
        , CROSS: Magic( required=True, allow_on=True, allow_using=True )
        , STRAIGHT: Magic( required=False, allow_on=True, allow_using=False )
        , LEFT: Magic( required=True, allow_on=True, allow_using=True )
        , RIGHT: Magic( required=True, allow_on=True, allow_using=True )
    }

    def __init__(self, table_left, table_right, mode=INNER, on=None, using=None):
        self._jn_dynamic_fields = {}
        self._jn_tbl_l = table_left
        self._jn_tbl_r = table_right
        self._jn_mode = mode
        self._jn_on = on
        self._jn_using = None if not using else using if isinstance(using, (list, tuple)) else [using]

        self._check_vaidation()

        # do some init
        j_executor = table_left._t_executor
        self._set_dynamic_fields( self._jn_tbl_l._t_model_class,
                                  self._jn_tbl_r._t_model_class )
        super(Join, self).__init__(model_class=JoinModel, executor=j_executor)

    def _check_vaidation(self):
        if not (isinstance(self._jn_tbl_l, TableIface) and self._jn_tbl_l.__class__ != TableIface):
            raise COExcInvalidSql( "invalid @table_left:'%s', need be instance of subclass of TableIface" % (self._jn_tbl_l) )

        if not (isinstance(self._jn_tbl_r, TableIface) and self._jn_tbl_r.__class__ != TableIface):
            raise COExcInvalidSql( "invalid @table_right:'%s', need be instance of subclass of TableIface" % (self._jn_tbl_r) )

        if self._jn_mode not in self._JC.keys():
            raise COExcInvalidSql( "invalid @mode:'%s', need be one of %s" % (self._jn_mode, self._JC.keys()) )

        if self._jn_on and not isinstance(self._jn_on, ConditionItemBase):
            raise COExcInvalidSql( "invalid @on:'%s', need to instance of subclass of ConditionItemBase" % (self._jn_on) )

        if self._jn_using and False in [isinstance(i, Field) for i in self._jn_using]:
            raise COExcInvalidSql( "invalid @using:'%s', all using should be Field" % (self._jn_on) )

    def _set_dynamic_fields(self, mc_l, mc_r):
        """use barebone JoinModel
        get it share attribute property set:
        2. TABLE_NAME
        """
        dict_fields = self._get_fields(mc_l, mc_r)
        self._jn_dynamic_fields = { _Pack_Field_Key(v.name):v.definition() for k, v in dict_fields.items() }
        JoinModel.TABLE_NAME = "<JoinTable(%s,%s,%s)>" % ( self._jn_tbl_l.sql(),
                                                           self._jn_mode,
                                                           self._jn_tbl_r.sql() )

    def on(self, condition):
        if condition and not isinstance(condition, ConditionItemBase):
            raise COExcInvalidSql( "invalid @on:'%s', need to instance of subclass of ConditionItemBase" % (condition) )
        self._jn_on = condition
        return self

    def using(self, *columns):
        if columns and False in [isinstance(i, Field) for i in columns]:
            raise COExcInvalidSql( "invalid @using:'%s', all using should be Field" % (columns) )
        self._jn_using = columns
        return self

    def sql(self):
        join_part = "%s %s %s" % (self._jn_tbl_l.sql(), self._jn_mode, self._jn_tbl_r.sql())
        jc = self._JC[self._jn_mode]
        if not jc.required:
            return join_part

        # else, required
        # 1. try on
        postfix = ''
        if jc.allow_on:
            if self._jn_on:
                postfix = ' ON ' + self._jn_on.sql()

        # 2. try using if necessary
        if not postfix:
            if jc.allow_using:
                if self._jn_using is None:
                    raise COExcInvalidSql("using columns needed for mode:'%s' of join:%s" % \
                                        (self._jn_mode, self._t_name))
                #else
                postfix = ' USING (' + ', '.join([i.sql() for i in self._jn_using]) + ')'
            else:
                raise COExcInvalidSql("this should never happen, both 'ON' and 'USING' "
                                    "not allowed for mode:'%s' of join:%s, "
                                    "current restriction:'%s'." % \
                                    (self._jn_mode, self._t_name, jc))
        return '(' + join_part + postfix + ')'

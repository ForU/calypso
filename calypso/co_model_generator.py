#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Wed May  6 16:48:21 2015
# ht <515563130@qq.com, weixin:jacoolee>

"""
input: a schema of a db
output: APP_tables/co_table.py, APP_models/co_db_constants.py

the APP is the app user of current lib.
"""

"""
simple but works !
"""

# TODO Wed May 13 15:11:26 2015 [set enum consts in table class]
# all valid enums


import os

def IND(level=0):
    return '    '*level

def CAP(astr):
    items = astr.split('_')
    items = [i.capitalize() for i in items]
    return ''.join(items)

def UPC(astr):
    return astr.upper()

def LOWC(astr):
    return astr.lower()


class Magic(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self, ):
        return str(self.__dict__)


class ModelGenerator(object):
    VERSION = 0.2
    def __init__(self, host=None, port=None, user=None, passwd=None, db=None):
        self.model_classes = []
        self.model_classes_decl = {} # key: model_class_name, value:class declaration
        self.table_classes_decl = {} # key: table_class_name, value:class declaration
        self._cmysql = Magic(host=host or '',
                             port=port or '',
                             user=user or '',
                             passwd=passwd or '',
                             db=db or '')

    # get tables of db
    def getDBData(self, sql):
        cmd_componets = [
            'mysql'
            , '-h%s' % self._cmysql.host if self._cmysql.host else ''
            , '-P%s' % self._cmysql.port if self._cmysql.port else ''
            , '-u%s' % self._cmysql.user if self._cmysql.user else ''
            , '-p%s' % self._cmysql.passwd if self._cmysql.passwd else ''
            , self._cmysql.db if self._cmysql.db else ''
            , "-e '%s'" % sql
         ]
        cmd_componets = [ i for i in cmd_componets if i ]
        cmd = ' '.join( cmd_componets )
        # print "# %s" % cmd
        return os.popen( cmd ).read()

    def getDBTables(self, db_name ):
        raw_tables = self.getDBData("use '%s'; show tables;" % db_name )
        tables = raw_tables.split('\n')[1:-1]
        return [ self.getDBTable(tname, db_name) for tname in tables ]

    def getTableDefinition(self, db_name, table_name):
        raw_table = self.getDBData("use '%s'; show create table %s;" % (db_name, table_name))
        table_items = raw_table.split('\n')[-2].split('\\n')
        return [i.strip().replace('`', "'") for i in table_items]

    def getTableUniqKeys(self, db_name, table_name):
        """
        return map, key: uniq_key, value: uk_components
        """
        table_defi_items = self.getTableDefinition(db_name, table_name)
        uniq_key_map = {}
        for i in table_defi_items:
            if i[:6].lower() != 'unique':
                continue
            uk = i.split(' ')
            name = eval(uk[-2])
            components = eval(uk[-1])
            # refine components if necessary to make sure it's a tuple or list
            if type(components) not in (list, tuple):
                components = [components]
            uniq_key_map[name] = Magic( ** { 'table':table_name, 'name': name, 'components': components } )
        return uniq_key_map

    def getDBTableNames(self, db_name ):
        raw_tables = self.getDBData("use '%s'; show tables;" % db_name )
        return raw_tables.split('\n')[1:-1]

    def getDBTable(self, table_name, db_name=None ):
        db_prefix = db_name+'.' if db_name else ''
        tname = db_prefix + table_name
        raw_table = self.getDBData("desc '%s'" % tname)
        table_columns = raw_table.split('\n')[1:-1]
        columns = []
        for i in table_columns:
            fname, ftype, fnull, fkey, fdefault, fextra = i.split('\t')
            db_column = Magic( fname=fname, ftype=ftype, fnull=fnull,
                               fkey=fkey, fdefault=fdefault,
                               fextra=fextra )
            columns.append( db_column )

        # results
        db_table = Magic( db_name=db_name,
                          table_name=table_name,
                          columns=columns,
                          uk_map = self.getTableUniqKeys(db_name, table_name) )
        return db_table

    def _ftype2field_type(self, ftype):
        if ftype.startswith('int'): return 'int'
        if ftype.startswith('tinyint'): return 'int'
        if ftype.startswith('varchar'): return 'str'
        if ftype.startswith('datetime'): return 'str'  # TODO
        if ftype.startswith('timestamp'): return 'str' # TODO
        if ftype.startswith('enum'): return 'FieldTypeEnum'
        return 'str'                                   # default to be 'str'

    def _fe(self, fname, enum_val):
        return "%s_%s" % (UPC(fname), UPC(enum_val))

    def _compose_class_model(self, db_table):
        """
        Model: [part 1]
        * class declaration
        * table_name, db_name
        * enum constants, all_valid
        * default
        * __init__
        * fields
        """
        class_name = "%sModel" % CAP(db_table.table_name)
        class_stm = 'class %s(ModelIface):' % class_name
        db_name_stm = "# DB_NAME = '%s'" % db_table.db_name
        table_name_stm = "TABLE_NAME = '%s'" % db_table.table_name

        init_stm = ''
        init_stm_prefix = 'def __init__(self, '
        init_stm_kwargs = []
        init_stm_postfix= '):'

        super_stm = 'super(%s, self).__init__()' % class_name
        all_valid_enum_constants = []
        enum_constants_stm = []
        enum_constants_wt_stm = [] # with table name
        fields_stm = []


        for cl in db_table.columns:
            if cl.ftype.startswith('enum'):
                enums = eval(cl.ftype[4:])
                sep = "\n%s"% IND(2)
                all_cands_name = "ALL_%s_CANDIDATES" % UPC(cl.fname)
                all_valid_enum_constants.append( "%s = (%s%s )" % \
                                                 (all_cands_name, sep, (sep+',').join([self._fe(cl.fname, i) for i in enums]))
                                             )
                enum_constants_wt_stm += [ "%s_%s = %s.%s" % \
                                           (UPC(db_table.table_name), self._fe(cl.fname,i), class_name, self._fe(cl.fname, i)) \
                                           for i in enums ]
                enum_constants_stm += ["%s = '%s'" % (self._fe(cl.fname, i), i) for i in enums]

            # fields declaration
            # TODO Wed May 13 13:50:53 2015 [ftype, fdefault, fkey, fnull, fextra]
            refined_f_type = self._ftype2field_type( cl.ftype )
            restriction = None
            kw_def_v = None
            refined_f_default = None

            # NOTICE: 'cl.fdefault is None' other than not not 'cl.fdefault'
            if cl.fdefault == 'NULL' or cl.fdefault is None:
                refined_f_default = None
            elif refined_f_type == 'FieldTypeEnum':
                kw_def_v = '%s' % self._fe(cl.fname, cl.fdefault)
                restriction = 'self.%s' % all_cands_name
                refined_f_default = 'self.%s' % self._fe(cl.fname, cl.fdefault)
            elif refined_f_type == 'str':
                if cl.fdefault == 'CURRENT_TIMESTAMP':
                    refined_f_default = "'NOW()'"
                else:
                    refined_f_default = "'%s'" % cl.fdefault
            else:
                refined_f_default = cl.fdefault

            init_stm_kwargs.append("%s=%s" % (cl.fname, kw_def_v))

            f_stm = "self.%s = Field(name='%s', type=%s, value=%s, default=%s, restriction=%s)" % \
                    ( cl.fname,
                      cl.fname,
                      refined_f_type,
                      cl.fname,
                      refined_f_default,
                      restriction)
            fields_stm.append(f_stm)

        # reset init_stm
        init_stm = init_stm_prefix + ', '.join(init_stm_kwargs) + init_stm_postfix
        components = [ class_stm , IND(1)+db_name_stm, IND(1)+table_name_stm ]
        components += [ IND(1)+i for i in enum_constants_stm ]
        components += [ IND(1)+i for i in all_valid_enum_constants ] # after enum_constants_stm
        components += [ IND(1)+init_stm , IND(2)+super_stm ]
        components += [ IND(2)+i for i in fields_stm ]

        return components, enum_constants_wt_stm, class_name

    def _compose_class_table(self, db_table):
        """
        class Person(Table):
          __UK_MAP = { 'uk_name': UkClass }
          class UkClass(object):
              def __init(self, x, y):
                  pass # ...
          def __init__(self):
            super(Person, self).__init__(PersonModel, executor=g_co_executor)
        """
        class_name_model = "%sModel" % CAP(db_table.table_name)
        tbl_class_name = "%s" % CAP(db_table.table_name)
        class_stm = 'class %s(Table):' % tbl_class_name
        # uk part

        table_uk_map = '__UK_MAP = %s' % '{' + ','.join(["'%s':%s" % (k, CAP(v.name)) for k,v in db_table.uk_map.items()]) + '}'
        def __inner_compose_uk_classes(ind_level):
            def __most_inner_compose_uk(uk_mgc, ind_level):
                uk_class_name = 'class %s(object):' % CAP(uk_mgc.name)
                init_stm = 'def __init__(self, %s):' % ', '.join(uk_mgc.components)
                attrs = ['self.%s = %s' % (i,i) for i in uk_mgc.components]
                cond_func_name = 'def cond(self):'
#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Wed May  6 16:48:21 2015
# ht <515563130@qq.com, weixin:jacoolee>

"""
input: a schema of a db
output: APP_tables/co_table.py, APP_models/co_db_constants.py

the APP is the app user of current lib.
"""

"""
simple but works !
"""

# TODO Wed May 13 15:11:26 2015 [set enum consts in table class]
# all valid enums


import os

def IND(level=0):
    return '    '*level

def CAP(astr):
    items = astr.split('_')
    items = [i.capitalize() for i in items]
    return ''.join(items)

def UPC(astr):
    return astr.upper()

def LOWC(astr):
    return astr.lower()


class Magic(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self, ):
        return str(self.__dict__)


class ModelGenerator(object):
    VERSION = 0.2
    def __init__(self, host=None, port=None, user=None, passwd=None, db=None):
        self.model_classes = []
        self.model_classes_decl = {} # key: model_class_name, value:class declaration
        self.table_classes_decl = {} # key: table_class_name, value:class declaration
        self._cmysql = Magic(host=host or '',
                             port=port or '',
                             user=user or '',
                             passwd=passwd or '',
                             db=db or '')

    # get tables of db
    def getDBData(self, sql):
        cmd_componets = [
            'mysql'
            , '-h%s' % self._cmysql.host if self._cmysql.host else ''
            , '-P%s' % self._cmysql.port if self._cmysql.port else ''
            , '-u%s' % self._cmysql.user if self._cmysql.user else ''
            , '-p%s' % self._cmysql.passwd if self._cmysql.passwd else ''
            , self._cmysql.db if self._cmysql.db else ''
            , "-e '%s'" % sql
         ]
        cmd_componets = [ i for i in cmd_componets if i ]
        cmd = ' '.join( cmd_componets )
        # print "# %s" % cmd
        return os.popen( cmd ).read()

    def getDBTables(self, db_name ):
        raw_tables = self.getDBData("use '%s'; show tables;" % db_name )
        tables = raw_tables.split('\n')[1:-1]
        return [ self.getDBTable(tname, db_name) for tname in tables ]

    def getTableDefinition(self, db_name, table_name):
        raw_table = self.getDBData("use '%s'; show create table %s;" % (db_name, table_name))
        table_items = raw_table.split('\n')[-2].split('\\n')
        return [i.strip().replace('`', "'") for i in table_items]

    def getTableUniqKeys(self, db_name, table_name):
        """
        return map, key: uniq_key, value: uk_components
        """
        table_defi_items = self.getTableDefinition(db_name, table_name)
        uniq_key_map = {}
        for i in table_defi_items:
            if i[:6].lower() != 'unique':
                continue
            uk = i.split(' ')
            name = eval(uk[-2])
            components = eval(uk[-1])
            # refine components if necessary to make sure it's a tuple or list
            if type(components) not in (list, tuple):
                components = [components]
            uniq_key_map[name] = Magic( ** { 'table':table_name, 'name': name, 'components': components } )
        return uniq_key_map

    def getDBTableNames(self, db_name ):
        raw_tables = self.getDBData("use '%s'; show tables;" % db_name )
        return raw_tables.split('\n')[1:-1]

    def getDBTable(self, table_name, db_name=None ):
        db_prefix = db_name+'.' if db_name else ''
        tname = db_prefix + table_name
        raw_table = self.getDBData("desc '%s'" % tname)
        table_columns = raw_table.split('\n')[1:-1]
        columns = []
        for i in table_columns:
            fname, ftype, fnull, fkey, fdefault, fextra = i.split('\t')
            db_column = Magic( fname=fname, ftype=ftype, fnull=fnull,
                               fkey=fkey, fdefault=fdefault,
                               fextra=fextra )
            columns.append( db_column )

        # results
        db_table = Magic( db_name=db_name,
                          table_name=table_name,
                          columns=columns,
                          uk_map = self.getTableUniqKeys(db_name, table_name) )
        return db_table

    def _ftype2field_type(self, ftype):
        if ftype.startswith('int'): return 'int'
        if ftype.startswith('tinyint'): return 'int'
        if ftype.startswith('varchar'): return 'str'
        if ftype.startswith('datetime'): return 'str'  # TODO
        if ftype.startswith('timestamp'): return 'str' # TODO
        if ftype.startswith('enum'): return 'FieldTypeEnum'
        return 'str'                                   # default to be 'str'

    def _fe(self, fname, enum_val):
        return "%s_%s" % (UPC(fname), UPC(enum_val))

    def _compose_class_model(self, db_table):
        """
        Model: [part 1]
        * class declaration
        * table_name, db_name
        * enum constants, all_valid
        * default
        * __init__
        * fields
        """
        class_name = "%sModel" % CAP(db_table.table_name)
        class_stm = 'class %s(ModelIface):' % class_name
        db_name_stm = "# DB_NAME = '%s'" % db_table.db_name
        table_name_stm = "TABLE_NAME = '%s'" % db_table.table_name

        init_stm = ''
        init_stm_prefix = 'def __init__(self, '
        init_stm_kwargs = []
        init_stm_postfix= '):'

        super_stm = 'super(%s, self).__init__()' % class_name
        all_valid_enum_constants = []
        enum_constants_stm = []
        enum_constants_wt_stm = [] # with table name
        fields_stm = []


        for cl in db_table.columns:
            if cl.ftype.startswith('enum'):
                enums = eval(cl.ftype[4:])
                sep = "\n%s"% IND(2)
                all_cands_name = "ALL_%s_CANDIDATES" % UPC(cl.fname)
                all_valid_enum_constants.append( "%s = (%s%s )" % \
                                                 (all_cands_name, sep, (sep+',').join([self._fe(cl.fname, i) for i in enums]))
                                             )
                enum_constants_wt_stm += [ "%s_%s = %s.%s" % \
                                           (UPC(db_table.table_name), self._fe(cl.fname,i), class_name, self._fe(cl.fname, i)) \
                                           for i in enums ]
                enum_constants_stm += ["%s = '%s'" % (self._fe(cl.fname, i), i) for i in enums]

            # fields declaration
            # TODO Wed May 13 13:50:53 2015 [ftype, fdefault, fkey, fnull, fextra]
            refined_f_type = self._ftype2field_type( cl.ftype )
            restriction = None
            kw_def_v = None
            refined_f_default = None

            # NOTICE: 'cl.fdefault is None' other than not not 'cl.fdefault'
            if cl.fdefault == 'NULL' or cl.fdefault is None:
                refined_f_default = None
            elif refined_f_type == 'FieldTypeEnum':
                kw_def_v = '%s' % self._fe(cl.fname, cl.fdefault)
                restriction = 'self.%s' % all_cands_name
                refined_f_default = 'self.%s' % self._fe(cl.fname, cl.fdefault)
            elif refined_f_type == 'str':
                if cl.fdefault == 'CURRENT_TIMESTAMP':
                    refined_f_default = "'NOW()'"
                else:
                    refined_f_default = "'%s'" % cl.fdefault
            else:
                refined_f_default = cl.fdefault

            init_stm_kwargs.append("%s=%s" % (cl.fname, kw_def_v))

            f_stm = "self.%s = Field(name='%s', type=%s, value=%s, default=%s, restriction=%s)" % \
                    ( cl.fname,
                      cl.fname,
                      refined_f_type,
                      cl.fname,
                      refined_f_default,
                      restriction)
            fields_stm.append(f_stm)

        # reset init_stm
        init_stm = init_stm_prefix + ', '.join(init_stm_kwargs) + init_stm_postfix
        components = [ class_stm , IND(1)+db_name_stm, IND(1)+table_name_stm ]
        components += [ IND(1)+i for i in enum_constants_stm ]
        components += [ IND(1)+i for i in all_valid_enum_constants ] # after enum_constants_stm
        components += [ IND(1)+init_stm , IND(2)+super_stm ]
        components += [ IND(2)+i for i in fields_stm ]

        return components, enum_constants_wt_stm, class_name

    def _compose_class_table(self, db_table):
        """
        class Person(Table):
          __UK_MAP = { 'uk_name': UkClass }
          class UkClass(object):
              def __init(self, x, y):
                  pass # ...
          def __init__(self):
            super(Person, self).__init__(PersonModel, executor=g_co_executor)
        """
        class_name_model = "%sModel" % CAP(db_table.table_name)
        tbl_class_name = "%s" % CAP(db_table.table_name)
        class_stm = 'class %s(Table):' % tbl_class_name
        # uk part

        table_uk_map = '__UK_MAP = %s' % '{' + ','.join(["'%s':%s" % (k, CAP(v.name)) for k,v in db_table.uk_map.items()]) + '}'
        def __inner_compose_uk_classes(ind_level):
            def __most_inner_compose_uk(uk_mgc, ind_level):
                uk_class_name = 'class %s(object):' % CAP(uk_mgc.name)
                init_stm = 'def __init__(self, %s):' % ', '.join(uk_mgc.components)
                attrs = ['self.%s = %s' % (i,i) for i in uk_mgc.components]
                cond_func_name = 'def cond(self):'
#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Wed May  6 16:48:21 2015
# ht <515563130@qq.com, weixin:jacoolee>

"""
input: a schema of a db
output: APP_tables/co_table.py, APP_models/co_db_constants.py

the APP is the app user of current lib.
"""

"""
simple but works !
"""

# TODO Wed May 13 15:11:26 2015 [set enum consts in table class]
# all valid enums


import os

def IND(level=0):
    return '    '*level

def CAP(astr):
    items = astr.split('_')
    items = [i.capitalize() for i in items]
    return ''.join(items)

def UPC(astr):
    return astr.upper()

def LOWC(astr):
    return astr.lower()


class Magic(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self, ):
        return str(self.__dict__)


class ModelGenerator(object):
    VERSION = 0.2
    def __init__(self, host=None, port=None, user=None, passwd=None, db=None):
        self.model_classes = []
        self.model_classes_decl = {} # key: model_class_name, value:class declaration
        self.table_classes_decl = {} # key: table_class_name, value:class declaration
        self._cmysql = Magic(host=host or '',
                             port=port or '',
                             user=user or '',
                             passwd=passwd or '',
                             db=db or '')

    # get tables of db
    def getDBData(self, sql):
        cmd_componets = [
            'mysql'
            , '-h%s' % self._cmysql.host if self._cmysql.host else ''
            , '-P%s' % self._cmysql.port if self._cmysql.port else ''
            , '-u%s' % self._cmysql.user if self._cmysql.user else ''
            , '-p%s' % self._cmysql.passwd if self._cmysql.passwd else ''
            , self._cmysql.db if self._cmysql.db else ''
            , "-e '%s'" % sql
         ]
        cmd_componets = [ i for i in cmd_componets if i ]
        cmd = ' '.join( cmd_componets )
        # print "# %s" % cmd
        return os.popen( cmd ).read()

    def getDBTables(self, db_name ):
        raw_tables = self.getDBData("use '%s'; show tables;" % db_name )
        tables = raw_tables.split('\n')[1:-1]
        return [ self.getDBTable(tname, db_name) for tname in tables ]

    def getTableDefinition(self, db_name, table_name):
        raw_table = self.getDBData("use '%s'; show create table %s;" % (db_name, table_name))
        table_items = raw_table.split('\n')[-2].split('\\n')
        return [i.strip().replace('`', "'") for i in table_items]

    def getTableUniqKeys(self, db_name, table_name):
        """
        return map, key: uniq_key, value: uk_components
        """
        table_defi_items = self.getTableDefinition(db_name, table_name)
        uniq_key_map = {}
        for i in table_defi_items:
            if i[:6].lower() != 'unique':
                continue
            uk = i.split(' ')
            name = eval(uk[-2])
            components = eval(uk[-1])
            # refine components if necessary to make sure it's a tuple or list
            if type(components) not in (list, tuple):
                components = [components]
            uniq_key_map[name] = Magic( ** { 'table':table_name, 'name': name, 'components': components } )
        return uniq_key_map

    def getDBTableNames(self, db_name ):
        raw_tables = self.getDBData("use '%s'; show tables;" % db_name )
        return raw_tables.split('\n')[1:-1]

    def getDBTable(self, table_name, db_name=None ):
        db_prefix = db_name+'.' if db_name else ''
        tname = db_prefix + table_name
        raw_table = self.getDBData("desc '%s'" % tname)
        table_columns = raw_table.split('\n')[1:-1]
        columns = []
        for i in table_columns:
            fname, ftype, fnull, fkey, fdefault, fextra = i.split('\t')
            db_column = Magic( fname=fname, ftype=ftype, fnull=fnull,
                               fkey=fkey, fdefault=fdefault,
                               fextra=fextra )
            columns.append( db_column )

        # results
        db_table = Magic( db_name=db_name,
                          table_name=table_name,
                          columns=columns,
                          uk_map = self.getTableUniqKeys(db_name, table_name) )
        return db_table

    def _ftype2field_type(self, ftype):
        if ftype.startswith('int'): return 'int'
        if ftype.startswith('tinyint'): return 'int'
        if ftype.startswith('varchar'): return 'str'
        if ftype.startswith('datetime'): return 'str'  # TODO
        if ftype.startswith('timestamp'): return 'str' # TODO
        if ftype.startswith('enum'): return 'FieldTypeEnum'
        return 'str'                                   # default to be 'str'

    def _fe(self, fname, enum_val):
        return "%s_%s" % (UPC(fname), UPC(enum_val))

    def _compose_class_model(self, db_table):
        """
        Model: [part 1]
        * class declaration
        * table_name, db_name
        * enum constants, all_valid
        * default
        * __init__
        * fields
        """
        class_name = "%sModel" % CAP(db_table.table_name)
        class_stm = 'class %s(ModelIface):' % class_name
        db_name_stm = "# DB_NAME = '%s'" % db_table.db_name
        table_name_stm = "TABLE_NAME = '%s'" % db_table.table_name

        init_stm = ''
        init_stm_prefix = 'def __init__(self, '
        init_stm_kwargs = []
        init_stm_postfix= '):'

        super_stm = 'super(%s, self).__init__()' % class_name
        all_valid_enum_constants = []
        enum_constants_stm = []
        enum_constants_wt_stm = [] # with table name
        fields_stm = []


        for cl in db_table.columns:
            if cl.ftype.startswith('enum'):
                enums = eval(cl.ftype[4:])
                sep = "\n%s"% IND(2)
                all_cands_name = "ALL_%s_CANDIDATES" % UPC(cl.fname)
                all_valid_enum_constants.append( "%s = (%s%s )" % \
                                                 (all_cands_name, sep, (sep+',').join([self._fe(cl.fname, i) for i in enums]))
                                             )
                enum_constants_wt_stm += [ "%s_%s = %s.%s" % \
                                           (UPC(db_table.table_name), self._fe(cl.fname,i), class_name, self._fe(cl.fname, i)) \
                                           for i in enums ]
                enum_constants_stm += ["%s = '%s'" % (self._fe(cl.fname, i), i) for i in enums]

            # fields declaration
            # TODO Wed May 13 13:50:53 2015 [ftype, fdefault, fkey, fnull, fextra]
            refined_f_type = self._ftype2field_type( cl.ftype )
            restriction = None
            kw_def_v = None
            refined_f_default = None

            # NOTICE: 'cl.fdefault is None' other than not not 'cl.fdefault'
            if cl.fdefault == 'NULL' or cl.fdefault is None:
                refined_f_default = None
            elif refined_f_type == 'FieldTypeEnum':
                kw_def_v = '%s' % self._fe(cl.fname, cl.fdefault)
                restriction = 'self.%s' % all_cands_name
                refined_f_default = 'self.%s' % self._fe(cl.fname, cl.fdefault)
            elif refined_f_type == 'str':
                if cl.fdefault == 'CURRENT_TIMESTAMP':
                    refined_f_default = "'NOW()'"
                else:
                    refined_f_default = "'%s'" % cl.fdefault
            else:
                refined_f_default = cl.fdefault

            init_stm_kwargs.append("%s=%s" % (cl.fname, kw_def_v))

            f_stm = "self.%s = Field(name='%s', type=%s, value=%s, default=%s, restriction=%s)" % \
                    ( cl.fname,
                      cl.fname,
                      refined_f_type,
                      cl.fname,
                      refined_f_default,
                      restriction)
            fields_stm.append(f_stm)

        # reset init_stm
        init_stm = init_stm_prefix + ', '.join(init_stm_kwargs) + init_stm_postfix
        components = [ class_stm , IND(1)+db_name_stm, IND(1)+table_name_stm ]
        components += [ IND(1)+i for i in enum_constants_stm ]
        components += [ IND(1)+i for i in all_valid_enum_constants ] # after enum_constants_stm
        components += [ IND(1)+init_stm , IND(2)+super_stm ]
        components += [ IND(2)+i for i in fields_stm ]

        return components, enum_constants_wt_stm, class_name

    def _compose_class_table(self, db_table):
        """
        class Person(Table):
          __UK_MAP = { 'uk_name': UkClass }
          class UkClass(object):
              def __init(self, x, y):
                  pass # ...
          def __init__(self):
            super(Person, self).__init__(PersonModel, executor=g_co_executor)
        """
        class_name_model = "%sModel" % CAP(db_table.table_name)
        tbl_class_name = "%s" % CAP(db_table.table_name)
        class_stm = 'class %s(Table):' % tbl_class_name
        # uk part

        table_uk_map = '__UK_MAP = %s' % '{' + ','.join(["'%s':%s" % (k, CAP(v.name)) for k,v in db_table.uk_map.items()]) + '}'
        def __inner_compose_uk_classes(ind_level):
            def __most_inner_compose_uk(uk_mgc, ind_level):
                uk_class_name = 'class %s(object):' % CAP(uk_mgc.name)
                init_stm = 'def __init__(self, %s):' % ', '.join(uk_mgc.components)
                attrs = ['self.%s = %s' % (i,i) for i in uk_mgc.components]
                cond_func_name = 'def cond(self):'
                cond_func_imp_l1 = 't=%s()' % tbl_class_name
                cond_func_imp_l2 = 'return ' + ' & '.join( ['(t.%s == self.%s)' % (i, i) for i in uk_mgc.components] )
                ret = [ IND(ind_level) + uk_class_name,
                        IND(ind_level+1) + init_stm, ]
                ret += [ IND(ind_level+2)+i for i in attrs ]
                ret += [ IND(ind_level+1) + cond_func_name,
                         IND(ind_level+2) + cond_func_imp_l1,
                         IND(ind_level+2) + cond_func_imp_l2, ]
                return ret
            # inner main here
            ret = []
            for i in db_table.uk_map.values():
                ret += __most_inner_compose_uk(i, ind_level)
            return ret

        # table class init
        init_stm = 'def __init__(self):'
        super_stm = 'super(%s, self).__init__(%s, executor=g_co_executor)' % (tbl_class_name, class_name_model)
        all_valid_enum_constants = []
        enum_constants_stm = []
        enum_constants_wt_stm = [] # with table name

        # traverse columns to handle enum stuff
        for cl in db_table.columns:
            if cl.ftype.startswith('enum'):
                enums = eval(cl.ftype[4:])
                sep = "\n%s" % IND(2)
                all_valid_enum_constants.append( "ALL_%s_CANDIDATES = (%s%s )" % \
                                                 ( UPC(cl.fname), sep, (sep+',').join([self._fe(cl.fname, i) for i in enums]) ) )
                enum_constants_wt_stm += [ "%s = '%s'" % ( UPC(db_table.table_name)+'_'+self._fe(cl.fname,i), i ) for i in enums ]
                # put all_valid_enum_constants into enum_constants_wt_stm
                enum_constants_keys = [ UPC(db_table.table_name)+'_'+self._fe(cl.fname,i) for i in enums ]
                enum_constants_wt_stm.append( "%s_ALL_%s_CANDIDATES = [ %s ]" % \
                                              ( UPC(db_table.table_name), UPC(cl.fname), ', '.join(enum_constants_keys) ) )

                enum_constants_stm += ["%s = '%s'" % (self._fe(cl.fname, i), i) for i in enums]

        # compose getByUniqKey(...) if necessary
        gbuk_func_name = 'def getByUniqKey(self, uk, *leak_fields, **kwargs):'
        gbuk_func_imp_l1 = 'return self.select(*leak_fields, **kwargs).where(uk.cond()).one()'

        # components
        components = [ class_stm ]
        components += [ IND(1)+i for i in enum_constants_stm ]
        components += [ IND(1)+i for i in all_valid_enum_constants ] # after enum_constants_stm
        components += __inner_compose_uk_classes(1)
        components += [ IND(1)+table_uk_map ]
        components += [ IND(1)+init_stm , IND(2)+super_stm ]
        components += [ IND(1)+gbuk_func_name, IND(2)+gbuk_func_imp_l1]

        return components, enum_constants_wt_stm, tbl_class_name

    def dumpTable(self, table_name, db_name=None, fp_model_file=None, fp_model_constant_file=None):
        """
        Model: [part 1]
        Table: [part 2]
        """
        db_table = self.getDBTable(table_name, db_name)
        model_components, model_ecwt, model_class_name = self._compose_class_model(db_table)
        self.model_classes.append( model_class_name )

        table_components, table_ecwt, table_class_name = self._compose_class_table(db_table)
        self.model_classes_decl[model_class_name] = '\n'.join(model_components)
        self.table_classes_decl[table_class_name] = '\n'.join(table_components)

        # write into model file
        print "generating model for '%s' ..." % table_class_name
        fp_model_file.write('\n'.join(model_components))
        fp_model_file.write('\n'*2)
        fp_model_file.write('\n'.join(table_components))
        fp_model_file.write('\n'*2)
        fp_model_file.flush()

        print "generating const for '%s' ..." % table_class_name
        # write constants into constants file
        table_ecwt.insert(0, '') # trick
        sep = '\n'+IND(1)
        data = sep.join(table_ecwt)
        fp_model_constant_file.write(data)


    def _gen_model_file_headers(self, fp_m, app_name=''):
        headers = [
            "#! /usr/bin/env python"
            , "# -*- coding: utf-8 -*-"
            , "#"
            , "# Autogenerated by Model Generator (%s)" % self.VERSION
            , "# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING"
            , ""
            , "from calypso.co_sql import Field, ModelIface, Table, FieldTypeEnum"
            , "from calypso.co_executor import g_co_executor"
            , ""
            , ""
        ]
        fp_m.write('\n'.join(headers))

    def _gen_table_file_headers(self, fp_mc, model_file_name='db_model', app_name=''):
        refined_model_file_name = (LOWC(app_name)+'_' if app_name else '') + model_file_name
        headers = [
            "#! /usr/bin/env python"
            , "# -*- coding: utf-8 -*-"
            , "#"
            , "# Autogenerated by Model Generator (%s)" % self.VERSION
            , "# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING"
            , ""
            # , "from %s import *" % refined_model_file_name
            , ""
            , ""
            , "class %sDBConstants(object):" % CAP(app_name)
            , ""
        ]
        fp_mc.write('\n'.join(headers))

    def dumpDBSchema(self, db_name, model_des_dir_path='/tmp/', app_name=''):
        refined_app_name = LOWC(app_name) +'_' if app_name else ''
        path_d_m = model_des_dir_path + '/%smodel' % refined_app_name
        path_f_m = path_d_m + '/' + '%sdb_model.py' % refined_app_name
        path_f_mc = path_d_m + '/' + '%sdb_constants.py' % refined_app_name

        os.popen("mkdir -p '%s'" % path_d_m)
        os.popen("touch '%s/__init__.py'" % path_d_m)

        fp_m = open(path_f_m, 'w')
        fp_mc = open(path_f_mc, 'w')


        # write header into model file
        self._gen_model_file_headers(fp_m, app_name=app_name)
        self._gen_table_file_headers(fp_mc, app_name=app_name)

        # handle tables
        table_names = self.getDBTableNames(db_name)
        for tname in table_names:
            self.dumpTable( tname, db_name=db_name,
                            fp_model_file=fp_m,
                            fp_model_constant_file= fp_mc)

        print "generated files are stored under path: '%s'" % path_d_m.replace('//', '/')

if __name__ == '__main__':
    g=ModelGenerator(host='localhost', user='root')

    # # LOCAL HERE
    # g.dumpDBSchema('honeycomb', model_des_dir_path='/tmp/model', app_name='honeycomb')
    # g.dumpDBSchema('pollens', model_des_dir_path='/tmp/model', app_name='pollens')
    # g.dumpDBSchema('world', model_des_dir_path='/tmp/model', app_name='world')
    g.dumpDBSchema('warehouse', model_des_dir_path='/tmp/model', app_name='warehouse')


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
                          columns=columns)
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
        table_name_stm = "TABLE_NAME = '%s'" % db_table.table_name
        db_name_stm = "# DB_NAME = '%s'" % db_table.db_name

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
        components = [ class_stm , IND(1)+table_name_stm , IND(1)+db_name_stm
                   ]
        components += [ IND(1)+i for i in enum_constants_stm ]
        components += [ IND(1)+i for i in all_valid_enum_constants ] # after enum_constants_stm
        components += [ IND(1)+init_stm , IND(2)+super_stm ]
        components += [ IND(2)+i for i in fields_stm ]

        return components, enum_constants_wt_stm, class_name

    def _compose_class_table(self, db_table):
        """
        class Person(Table):
          def __init__(self):
            super(Person, self).__init__(PersonModel, executor=g_co_executor)
        """
        class_name_model = "%sModel" % CAP(db_table.table_name)
        class_name = "%s" % CAP(db_table.table_name)
        class_stm = 'class %s(Table):' % class_name
        init_stm = 'def __init__(self):'
        super_stm = 'super(%s, self).__init__(%s, executor=g_co_executor)' % (class_name, class_name_model)
        all_valid_enum_constants = []
        enum_constants_stm = []
        enum_constants_wt_stm = [] # with table name

        # traverse columns to handle enum stuff
        for cl in db_table.columns:
            if cl.ftype.startswith('enum'):
                enums = eval(cl.ftype[4:])
                sep = "\n%s"% IND(2)
                all_valid_enum_constants.append( "ALL_%s_CANDIDATES = (%s%s )" % \
                                                 ( UPC(cl.fname), sep, (sep+',').join([self._fe(cl.fname, i) for i in enums]) )
                                             )
                enum_constants_wt_stm += [ "%s = %s.%s" % \
                                           ( UPC(db_table.table_name)+'_'+self._fe(cl.fname,i), class_name, self._fe(cl.fname, i) ) \
                                           for i in enums ]
                # put all_valid_enum_constants into enum_constants_wt_stm
                enum_constants_wt_stm.append( "%s_ALL_%s_CANDIDATES = %s.ALL_%s_CANDIDATES" % \
                                              ( UPC(db_table.table_name), UPC(cl.fname), class_name, UPC(cl.fname))
                                          )

                enum_constants_stm += ["%s = '%s'" % (self._fe(cl.fname, i), i) for i in enums]

        # components
        components = [ class_stm ]
        components += [ IND(1)+i for i in enum_constants_stm ]
        components += [ IND(1)+i for i in all_valid_enum_constants ] # after enum_constants_stm
        components += [ IND(1)+init_stm , IND(2)+super_stm ]

        return components, enum_constants_wt_stm, class_name

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
        refiend_model_file_name = (LOWC(app_name)+'_' if app_name else '') + model_file_name
        headers = [
            "#! /usr/bin/env python"
            , "# -*- coding: utf-8 -*-"
            , "#"
            , "# Autogenerated by Model Generator (%s)" % self.VERSION
            , "# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING"
            , ""
            , "from %s import *" % refiend_model_file_name
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
    g.dumpDBSchema('ilovect_users', model_des_dir_path='/Users/zhang/ilovect/users', app_name='users')
    g.dumpDBSchema('ilovect_collections', model_des_dir_path='/Users/zhang/ilovect/collections', app_name='collections')
    g.dumpDBSchema('ilovect_address', model_des_dir_path='/Users/zhang/ilovect/address', app_name='address')

#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Sun May 10 01:29:21 2015
# ht <515563130@qq.com, weixin:jacoolee>

import random

from app_model.app_db_model import *
from app_model.calypso.co_sql import *
from app_model.calypso.co_executor import *
from app_model.calypso.co_constants import COConstants

from app_config_manager import ConfigureManager


# re-complete g_co_executor with configure manager
cm = ConfigureManager('./config/app_db_config.py')
g_co_executor.reInit( mysql_host=cm.CNF.host,
                      mysql_port=cm.CNF.port,
                      mysql_user=cm.CNF.user,
                      mysql_passwd=cm.CNF.passwd,
                      mysql_db=cm.CNF.db
                  )

################################################################
# logic
p=Person()
o=Orders()

def sep(f):
    def _fargs(enable=True, *args, **kwargs):
        if not enable:
            return
        print "_"*100 + '\n# ' + f.__name__.replace('_', ' ')
        rc = f(enable, *args, **kwargs)
        print
        return rc
    return _fargs

@sep
def test_join_tables(enable=True):
    j=Join(p, o).on(p.id == o.person_id)
    print j.sql()

@sep
def test_table_join_table(enable=True):
    j=p.join(o).on(Like(p.name, o.serial.name + '%'))
    print j.sql()

@sep
def test_table_join_table_with_table_as(enable=True):
    P = p.AS('P')
    O = o.AS('O')
    j=P.join(O).on(P.id == O.person_id)
    print j.sql()

@sep
def test_select(enable=True):
    p=Person()
    res=p.select(p.id.AS('pid')).where(p.id == 1).execute()
    print "$>", res
    for i in res.res or []:
        print i.dumpAsStr()

@sep
def test_join_with_leak_field_as(enable=True):
    # has one
    j=p.join(o).on(p.id == o.person_id)
    cond=In(p.id, (1,2,3)) & Not(Like(p.name, '%100%'))
    res = j.select(p.id.AS('pid'), o.id.AS('order_id')).where(cond).execute()
    print "$>", res
    for i in res.res or []:
        print i.dumpAsStr()

@sep
def test_join_without_leak_field_as(enable=True):
    # has one
    j=p.join(o).on(p.id == o.person_id)
    cond=In(p.id, (1,2,3)) & Not(Like(p.name, '%100%'))
    res = j.select().where(cond).execute()
    print "$>", res
    for i in res.res or []:
        print i.dumpAsStr()


@sep
def test_join_no_result(enable=True):
    # none
    j=p.join(o).on(p.id == o.person_id)
    cond=In(p.id, (3000000,400000)) & Not(Like(p.name, '%100%'))
    res = j.select(p.id.AS('pid'), o.id.AS('order_id')).where(cond).execute()
    print "$>", res
    for i in res.res or []:
        print i.dumpAsStr()

@sep
def test_table_as(enable=True):
    p_as = p.AS('devil')
    o_as = o.AS('angel')
    j = Join(p_as, o_as).on(p_as.id == o_as.person_id)
    cond=In(p_as.id, (1,2,3)) & Not(Like(p_as.name, '%100%'))
    res = j.select(p_as.id.AS('why_so_serious'), o_as.person_id).where(cond).execute()
    print "$>", res
    for i in res.res or []:
        print i.dumpAsStr()

@sep
def test_select_all(enable=True):
    try:
        j=p.join(o).on(p.id == o.person_id)
        res = j.select().execute()
        print "$>", res
        for i in res.res or []:
            print i.dumpAsStr()
    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)

@sep
def test_empty_where_condition(enable=True):
    cond=None
    try:
        j=p.join(o).on(p.id == o.person_id)
        res = j.select().where(cond).execute()
        print "$>", res
        for i in res.res or []:
            print i.dumpAsStr()
    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)



@sep
def test_insert_with_none_field(enable=True):
    datas = [
        {'name':'haha', 'sex':None},
        {'name':'jaci', 'sex':'FEMALE', 'age':12}
    ]
    print datas

    try:
        r = p.insert( datas )
        print "$>", r

        # normal process logic
        if r.res > 0:
            print "insert success, the newly added id:" % r.res
        elif r.res == COConstants.RECORD_DUPLICATED:
            print "duplicated occurs, caz:'%s'" % r.why
        elif r.res == COConstants.INVALID_SQL:
            print "invalid sql, caz:'%s'" % r.why
        else:
            print r

    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)


@sep
def test_insert(enable=True):
    datas = [
        {'name':'haha'},
        {'name':'jaci', 'sex':'FEMALE', 'age':12}
    ]
    print datas
    try:
        r = p.insert( datas )
        print "$>", r

        # normal process logic
        if r.res > 0:
            print "insert success, the newly added id:'%s'" % r.res
        elif r.res == COConstants.RECORD_DUPLICATED:
            print "duplicated occurs, caz:'%s'" % r.why
        elif r.res == COConstants.INVALID_SQL:
            print "invalid sql, caz:'%s'" % r.why
        else:
            print r

    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)


@sep
def test_insert_by_model(enable=True):
    m = PersonModel(name='haha', sex=Person.SEX_FEMALE)
    try:
        r = p.insert( m )
        print "$>", r

        # normal process logic
        if r.res > 0:
            print "insert success, the newly added id:'%s'" % r.res
        elif r.res == COConstants.RECORD_DUPLICATED:
            print "duplicated occurs, caz:'%s'" % r.why
        elif r.res == COConstants.INVALID_SQL:
            print "invalid sql, caz:'%s'" % r.why
        else:
            print r

    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)

@sep
def test_update(enable=True):
    data = { 'age':random.randint(1,100),
             'sex': 'MALE' if random.randint(0,1) else 'FEMALE'
    }
    cond = In(p.id, (1,2,3))
    try:
        r = p.update(data, cond=cond)
        print "$>", r
    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)

@sep
def test_delete(enable=True):
    cond = (p.id > 10)
    try:
        r = p.delete(cond)
        print "$>", r
    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)

@sep
def test_delete_with_cond_is_none(enable=True):
    return None
    try:
        r = p.delete(None)
        print "$>", r
    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)

@sep
def test_delete_with_cond_is_none_allowed(enable=True):
    # no need to test this, all your data will gone, fuck it
    return None
    try:
        r = p.delete(cond=None, allow_none_cond=True)
        print "$>", r
    except Exception as e:
        print "%s, caz:'%s'" % (e.__class__, e)


@sep
@do_transaction
def test_transaction(enable=True):
    def _inner_one():
        datas = [
            {'name':'haha', 'sex':None},
            {'name':'jaci', 'sex':'FEMALE', 'age':12}
        ]
        r = p.insert( datas )
        print "$>", r
    # real starts here
    _inner_one()
    _inner_one()



################################################################
if __name__ == '__main__':

    test_insert(0)
    test_select()

    # test_insert_with_none_field()
    # test_insert_by_model()

    # test_update()

    test_join_with_leak_field_as(0)
    test_join_without_leak_field_as()
    test_join_no_result()

    # test_table_as()

    # test_select_all()
    # test_empty_where_condition()

    # # join
    # test_join_tables()
    # test_table_join_table()
    # test_table_join_table_with_table_as()

    # transaction
    # test_transaction()

    # # delete
    # test_delete()
    # test_delete_with_cond_is_none()
    # test_delete_with_cond_is_none_allowed() # fuck, data are gone

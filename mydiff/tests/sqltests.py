import os

import pymysql

from mydiff import __compare, config, connect, Database

TESTDIR = 'mydiff/tests/sqltests/'
CONFIG = 'mydiff/tests/sqltests/config.json'


__conf = None
def conf():
    global __conf
    if __conf is None:
        __conf = config(CONFIG)
    return __conf


def load(fn):
    sql = {}

    with open(fn, 'r') as f:
        target = None
        for line in f.readlines():
            line = line.strip()
            if line.startswith('#') or line == '':
                continue
            if line in ['<both>','<old>','<new>','<expected>']:
                target = line
                continue
            if target is None:
                raise Exception('SQL code before a <directive>?')
            tar = target.replace('<','').replace('>','')
            if tar not in sql:
                sql[tar] = ""
            sql[tar] += line + '\n'
        for key in sql:
            sql[key] = sql[key].strip()

    return sql


def reset():    
    db = conf()['db1']
    conn = pymysql.connect(host=db['host'],user=db['user'],passwd=db['pass'])
    
    def recreate(dbname):
        cur = conn.cursor()
        try:
            cur.execute('drop database if exists '+dbname+';'+
                        'create database '+dbname+';')
        finally:
            cur.close()

    try:
        recreate('sqltests_1')
        recreate('sqltests_2')
    finally:
        conn.close()


def run(sqltest, db1, db2):
    name = sqltest.replace('.sqltest','')
    print('Running sqltest ['+name+'].. ',end="")

    test = load(TESTDIR+sqltest)

    if 'both' in test:
        for db in [db1,db2]:
            db.cmd(test['both'])

    if 'old' in test:
        db1.cmd(test['old'])

    if 'new' in test:
        db2.cmd(test['new'])

    def cleanup(p):
        return [l.strip() for l in p if l.strip() != '']

    expected = cleanup(test['expected'].split('\n'))
    actual = cleanup([line for line in __compare(conf())])

    for i in range(len(expected)):
        if expected[i] != actual[i]:
            def _out(o):
                for l in o:
                    print(o)
            print("\nTest failed.")
            print("Expected changes:")
            _out(expected)
            print("-------------------------")
            print("Actual changes:")
            _out(actual)
            return

    print("Done")


if __name__ == '__main__':
    sqltests = [fn for fn in os.listdir(TESTDIR) if fn.endswith('.sqltest')]

    for sqltest in sqltests:
        reset()

        with connect(conf()['db1']) as conn1:
            with connect(conf()['db2']) as conn2:
                run(sqltest, Database(conn1), Database(conn2))

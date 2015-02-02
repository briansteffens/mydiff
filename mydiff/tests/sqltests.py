"""
sqltests

Acceptance tests for mydiff

For each test, two empty databases will be created: old and new. Each test
should make new differ from old in some unique way and then verify that mydiff
generates the correct SQL to make old match new.

All files matching TESTDIR/*.sqltest will be loaded as tests. Each test file is
of the following form:

```
<both>
    create table T
    (
        id integer auto_increment
    ,   name varchar(32)

    ,   primary key (id)
    );
<new>
    insert into T (name) values ('abc');
```

In the test above, both databases start with an empty table T. A row is added
to T in the 'new' database but not 'old'. mydiff is executed against the
databases, and its output is compared against <new>: if mydiff generates the
same SQL code that was actually used to differentiate the databases, the test
passes.

The .sqltest format can be expanded to the following full form:

```
<both>
    # Code to be run on both old and new databases (executes first)
<old>
    # Code to be run only on the old database (executes second)
<new>
    # Code to be run only on the new database (executes third)
    # Also the expected SQL output of mydiff if <expected> is not present
<expected>
    # The expected SQL output of mydiff if it differs from <new> or <new> is
    # not present.
```

All <headers> are individually optional, however at least one of <new> or 
<expected> is required.

"""

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

    exp = test['new'] if 'expected' not in test else test['expected']
    expected = cleanup(exp.split('\n'))
    actual = cleanup([line for line in __compare(conf())])

    for i in range(len(expected)):
        if expected[i] != actual[i]:
            def _out(o):
                for l in o:
                    print(l)
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
                run(sqltest, Database(conn1, conf()['db1']['dbname']), 
			     Database(conn2, conf()['db2']['dbname']))

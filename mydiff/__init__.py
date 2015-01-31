#!/usr/bin/env python3

import json
import pymysql
from contextlib import contextmanager


def config(fn):
    with open(fn, 'r') as f:
        return json.loads(f.read())


def __always_close(obj):
    try:
        yield obj
    finally:
        obj.close()


@contextmanager
def connect(dbconf):
    return __always_close(pymysql.connect(
        host=dbconf['host'],
        user=dbconf['user'],
        passwd=dbconf['pass'],
        db=dbconf['dbname'],
        port=3306))


@contextmanager
def cursor(conn):
    return __always_close(conn.cursor())


def dicts(cur):
    md = cur.description
    
    basedict = {}
    colname_to_index = {}
    for i in range(len(md)):
        colname = md[i][0]
        basedict[colname] = None
        colname_to_index[colname] = i

    for row in cur:
        ret = basedict.copy()

        for colname in ret:
            ret[colname] = row[colname_to_index[colname]]

        yield ret


def results(db, sql):
    with cursor(db) as cur:
        #print("\nSQL: "+sql+"\n")
        cur.execute(sql)
        return dicts(cur)


class Column(object):
    def __init__(self, describe_row):
        self.name = describe_row['Field']
        self.key = describe_row['Key']
        self.pk = ('PRI' in self.key)
        self.unique = ('UNI' in self.key)
        self.auto_increment = ('auto_increment' in describe_row['Extra'])
        self.nullable = (describe_row['Null'] == 'YES')
        self.dbtype = describe_row['Type']


class Table(object):
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.columns = [Column(r) for r in results(db, 'describe '+name+';')]
        self.pks = [col for col in self.columns if col.pk]

    def column(self, name):
        for col in self.columns:
            if col.name == name:
                return col

        raise Exception("Column ["+name+"] not found in "+
                        "table ["+self.name+"].")

    def rows(self):
        pks = ['`'+pk.name+'`' for pk in self.pks]
        orderby = "" if len(pks) == 0 else "order by "+",".join(pks)
        for ret in results(self.db, 'select * from `'+self.name+'` '+
                                    orderby+';'):
            yield Row(self, ret)


class Database(object):
    def __init__(self, conn):
        self.conn = conn
        self.tablenames=[str(r[k]) for r in self.res('show tables') for k in r]

    def cmd(self, sql, commit=True):
        with cursor(self.conn) as cur:
            cur.execute(sql)
            if commit:
                self.conn.commit()
            return cur.rowcount

    def res(self, sql):
        return results(self.conn, sql)


class Row(object):
    def __init__(self, table, data):
        self.table = table
        self.data = data
    def val(self, colname):
        return self.data[colname]
    def md(self, colname):
        for col in self.table.columns:
            if col.name == colname:
                return col
        raise Exception("Column ["+colname+"] not found in "+
                        "table ["+self.table.name+"].")
    def dbtype(self, colname):
        return self.md(colname).dbtype


def sort_val(dbtype, val1, val2):
    """
    Compare val1 to val2, treating them both as dbtype

    Returns:
        1 if val2 comes after val1 in sorting order
        0 if val1 and val2 are equal in sorting order
        -1 if val1 comes after val2 in sorting order

    """
    
    if dbtype.startswith('int') or dbtype.startswith('varchar'):
        return 1 if val2 > val1 else 0 if val2 == val1 else -1

    raise Exception("Unable to compare dbtype ["+dbtype+"].")


def sort_pks(row1, row2):
    """
    Compare the primary key in row1 to row2 for sorting.

    Assumes the primary key definitions are identical. This will be run tons 
    of times during data comparison, so it doesn't do any checking. If the PK 
    format has changed between db1 and db2 this won't work.

    Returns:
        1 if row2 comes after row1 in sorting order
        0 if row1 and row2 match
        -1 if row1 comes after row2 in sorting order

    """
    
    for i in range(len(row1.table.pks)):
        col1 = row1.table.pks[i]
        val1 = row1.data[col1.name]

        col2 = row2.table.pks[i]
        val2 = row2.data[col2.name]

        order = sort_val(col1.dbtype, val1, val2)
        if order != 0:
            return order

    return 0


class Sql(object):

    @staticmethod
    def name(name):
        return '`'+name+'`'

    @staticmethod
    def val(v):
        if v is None:
            return "null"

        if isinstance(v, str):
            return "'"+v.replace("'","''")+"'"

        return str(v)

    @staticmethod
    def pk_row(row):
        terms = [Sql.name(pk.name)+'='+Sql.val(row.val(pk.name)) 
                 for pk in row.table.pks]
        return 'where '+' and '.join(terms)


def compare_rows(row1, row2):
    """
    Compare two rows from the same table. Assumes identical PKs.

    """
    
    updates = []

    for col in row2.data:
        if col not in row1.data:
            updates.append(col)
            continue

        sort = sort_val(row2.dbtype(col), row1.data[col], row2.data[col])

        if sort == 0:
            continue

        updates.append(col)

    if len(updates) == 0:
        return None

    sets = ','.join([Sql.name(c)+'='+Sql.val(row2.data[c]) for c in updates])

    return 'update '+Sql.name(row2.table.name)+' set '+sets+' '+\
           Sql.pk_row(row2)+';'


def compare_data(db1, db2, table):
    """
    Compare the data in two tables. Assumes identical PKs.

    """

    t1 = Table(db1, table)
    t2 = Table(db2, table)

    gen1 = t1.rows()
    gen2 = t2.rows()

    last_sort = 0

    row1 = None
    row2 = None

    def nextrow(gen):
        try:
            return next(gen)
        except StopIteration:
            return None

    ret = []

    def insert(r):
        cols = ','.join([Sql.name(col.name) for col in r.table.columns])
        vals = ','.join([Sql.val(r.data[col.name]) for col in r.table.columns])

        ret.append('insert into '+Sql.name(r.table.name)+' '+
                   '('+cols+') values ('+vals+');')

    def update(r1, r2):
        ret.append(compare_rows(r1, r2))

    def delete(r):
        ret.append('delete from '+Sql.name(r.table.name)+' '+Sql.pk_row(r)+';')

    for i in range(10):
        #print("sort:"+str(last_sort))
        if last_sort >= 0:
            row1 = nextrow(gen1)
        if last_sort <= 0:
            row2 = nextrow(gen2)

        def rowstr(r):
            return "null" if r is None else str(r.data)

        #print("\n\n"+rowstr(row1)+"\t|\t"+rowstr(row2))

        if row1 is None:
            if row2 is None:
                break
            else:
                insert(row2)
                last_sort = -1
                continue
        elif row2 is None:
            delete(row1)
            last_sort = 1
            continue

        last_sort = sort_pks(row1, row2)

        if last_sort == 0:
            update(row1, row2)
        elif last_sort > 0:
            delete(row1)
        elif last_sort < 0:
            insert(row2)

    return ret



def compare_databases(db1, db2):
    ts1 = db1.tablenames
    ts2 = db2.tablenames

    for t1 in db1.tablenames:   
        for t2 in db2.tablenames:
            if t1 == t2:
                yield 'ALTER TABLE '+t2
            else:
                yield 'drop table '+Sql.name(t1)+';'

    for t2 in db2.tablenames:
        for t1 in db1.tablenames:
            if t1 != t2:
                raise NotImplemented("create table")
                yield 'CREATE TABLE '+t2


def __compare(config):
    with connect(config['db1']) as conn1:
        with connect(config['db2']) as conn2:
            db1 = Database(conn1)
            db2 = Database(conn2)
            
            #for change in compare_databases(db1, db2):
            #    yield change

            for t1 in db1.tablenames:
                for t2 in db2.tablenames:
                    if t1 == t2:
                        for change in compare_data(conn1, conn2, t2):
                            yield change


#!/usr/bin/env python3

import itertools
import json
from contextlib import contextmanager
from collections import OrderedDict

import pymysql
from pymysql.converters import escape_item


class NotFoundException(Exception):
    """ Raised when an element of metadata cannot be found. """
    pass


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



class Column(object):
    def __init__(self, describe_row):
        self.name = describe_row['Field']
        self.key = describe_row['Key']
        self.pk = ('PRI' in self.key)
        self.unique = ('UNI' in self.key)
        self.auto_increment = ('auto_increment' in describe_row['Extra'])
        self.nullable = (describe_row['Null'] == 'YES')
        self.dbtype = describe_row['Type']



class KeyPart(object):
    def __init__(self, show_key):
        self.name = show_key['Key_name']
        self.seq = show_key['Seq_in_index']
        self.colname = show_key['Column_name']

    def set_key(self, key):
        self.key = key
        self.column = self.key.table.column(self.colname)

    def compare(self, other):
        return (self.name == other.name and 
                self.seq == other.seq and
                self.colname == other.colname)


class Key(object):
    def __init__(self, table, parts):
        self.table = table
        self.parts = parts 
        for part in self.parts:
            part.set_key(self)
        
        self.name = self.parts[0].name

        self.kind = {
            'PRI': 'primary',
            'UNI': 'unique',
            'MUL': 'multiple',
        }[self.parts[0].column.key]

        self.primary = self.kind == 'primary'

    def compare(self, other):
        if self.kind != other.kind or len(self.parts) != len(other.parts):
            return False

        for i in range(len(self.parts)):
            if not self.parts[i].compare(other.parts[i]):
                return False

        return True



class Table(object):
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.columns = [Column(r) for r in self.db.res('describe '+name+';')]

        parts = [KeyPart(r) for r in self.db.res('show keys from '+name+';')]
        
        keys = OrderedDict()
        for part in parts:
            if not part.name in keys:
                keys[part.name] = []
            keys[part.name].append(part)
        self.keys = [Key(self,keys[k]) for k in keys]
        
        self.pks = [col for col in self.columns if col.pk]

    def column(self, name):
        for col in self.columns:
            if col.name == name:
                return col

        raise NotFoundException("Column ["+name+"] not found in "+
                                "table ["+self.name+"].")

    def key(self, name):
        for k in self.keys:
            if k.name == name:
                return k

        raise NotFoundException('Key ['+name+'] not found in '+
                                'table ['+self.name+'].')

    def rows(self):
        pks = ['`'+pk.name+'`' for pk in self.pks]
        orderby = "" if len(pks) == 0 else "order by "+",".join(pks)
        for ret in self.db.res('select * from `'+self.name+'` '+orderby+';'):
            yield Row(self, ret)



class Database(object):
    def __init__(self, conn):
        self.conn = conn
        self.tablenames=[str(r[k]) for r in self.res('show tables;') for k in r]
        self.tables = [Table(self, tn) for tn in self.tablenames]

    def table(self, name):
        for t in self.tables:
            if t.name == name:
                return t
        raise NotFoundException("Table ["+name+"] not found.")

    def cmd(self, sql, commit=True):
        with cursor(self.conn) as cur:
            cur.execute(sql)
            if commit:
                self.conn.commit()
            return cur.rowcount

    def res(self, sql):
        with cursor(self.conn) as cur:
            cur.execute(sql)
            
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



class SqlRenderer(object):

    def name(self, name):
        if isinstance(name, str):
            return name
        elif hasattr(name, 'name'):
            return name.name

    def val(self, v, charset='latin1'):
        return escape_item(v, charset)

    def pk_row(self, row):
        terms = [self.name(pk)+'='+self.val(row.val(pk.name)) 
                 for pk in row.table.pks]
        return 'where '+' and '.join(terms)

    def update(self, row, updates):
        sets = ','.join([self.name(c)+'='+self.val(row.data[c]) 
                         for c in updates])

        return 'update '+self.name(row.table.name)+' set '+sets+' '+\
               self.pk_row(row)+';'

    def insert(self, row):
        cols = ','.join([self.name(col) for col in row.table.columns])
        vals = ','.join([self.val(row.data[col.name]) 
                         for col in row.table.columns])

        return 'insert into '+self.name(row.table)+' '+\
               '('+cols+') values ('+vals+');'

    def delete(self, row):
        return 'delete from '+self.name(row.table)+' '+\
               self.pk_row(row)+';'

    def drop_table(self, t):
        return 'drop table '+self.name(t)+';'

    def column(self, c):
        ret = self.name(c)+' '+c.dbtype

        if not c.nullable:
            ret += ' not null'

        if c.auto_increment:
            ret += ' auto_increment'

        return ret

    def keycols(self, k):
        return ','.join([self.name(p.column) for p in k.parts])

    def key(self, k):
        kind = k.kind+' ' if k.kind != 'multiple' else ''
        return kind+'key('+self.keycols(k)+')'

    def keymod(self, km):
        action = km[0]
        key = km[1]

        if action == 'drop':
            if key.primary:
                return 'drop primary key'
            else:
                return 'drop key '+self.name(key)
        elif action == 'add':
            if key.primary:
                return 'add '+self.key(key)
            elif key.kind == 'multiple':
                return 'add index '+self.name(key)+' ('+self.keycols(key)+')'
            else:
                return 'add constraint '+key.name+' '+self.key(key)
        else:
            raise NotImplemented('Unrecognized keymod action ['+action+']')

    def keys(self, t):
        return [self.key(k) for k in t.keys]

    def create_table(self, t):
        cols = [self.column(c) for c in t.columns]
        keys = self.keys(t)
        mods = ','.join([e for l in [cols,keys] for e in l])
        return 'create table '+self.name(t)+' ('+mods+');'

    def alter_table(self, t, additions, changes, deletions, keymods):
        a = ['add '+self.column(c) for c in additions]
        c = ['change '+self.name(c)+' '+self.column(c) for c in changes]
        d = ['drop '+self.name(c) for c in deletions]
        #keys = [km[0]+' '+self.key(km[1]) for km in keymods]
        #keys = ['drop primary key' if k.startswith('drop primary key') else k 
        #        for k in keys]
        keys = [self.keymod(km) for km in keymods]
        mods = ','.join([e for l in [d,c,a,keys] for e in l])
        return 'alter table '+self.name(t)+' '+mods+';'



def compare_rows(render, row1, row2):
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

    return render.update(row2, updates)



def compare_data(render, t1, t2):
    """
    Compare the data in two tables. Assumes identical PKs.

    """

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

    for i in range(10):
        if last_sort >= 0:
            row1 = nextrow(gen1)
        if last_sort <= 0:
            row2 = nextrow(gen2)

        if row1 is None:
            if row2 is None:
                break
            else:
                last_sort = -1
        elif row2 is None:
            last_sort = 1
        else:
            last_sort = sort_pks(row1, row2)

        if last_sort == 0:
            n = compare_rows(render,row1,row2)
        elif last_sort > 0:
            n = render.delete(row1)
        elif last_sort < 0:
            n = render.insert(row2)

        ret.append(n)

    return ret



def compare_columns(render, c1, c2):
    def changed(attr):
        return getattr(c1, attr) != getattr(c2, attr)

    if changed('auto_increment') or changed('nullable') or changed('dbtype'):
        return c2

    return None



def compare_table_keys(t1, t2):
    """
    Compare the keys between two tables and return a list of keymods.

    Returns:
        A list of keymods in tuple-form: (<drop|add>,<Key>,)

    """

    keymods = []

    def drop_key(key):
        keymods.append(('drop',key,))
    def add_key(key):
        keymods.append(('add',key,))

    for key1 in t1.keys:
        try:
            key2 = t2.key(key1.name)
            if not key1.compare(key2):
                drop_key(key1)
                add_key(key2)
        except NotFoundException:
            drop_key(key1)

    for key2 in t2.keys:
        try:
            key1 = t1.key(key2.name)
        except NotFoundException:
            add_key(key2)

    return keymods



def compare_tables(render, t1, t2):
    changes = []
    deletions = []
    for col1 in t1.columns:
        try:
            col2 = t2.column(col1.name)
            
            # same column name in both tables, compare them
            change = compare_columns(render, col1, col2)
            if change is not None:
                changes.append(change)

        except NotFoundException:
            deletions.append(col1)

    additions = []
    for col2 in t2.columns:
        try:
            col1 = t1.column(col2.name)
        except NotFoundException:
            additions.append(col2)

    keymods = compare_table_keys(t1, t2)

    no = lambda lst: len(lst) == 0

    if no(additions) and no(changes) and no(deletions) and no(keymods):
        return

    return render.alter_table(t2, additions, changes, deletions, keymods)



def compare_databases(render, db1, db2):
    for t1 in db1.tablenames:
        try:
            t2 = db2.table(t1)
            yield compare_tables(render,db1.table(t1),t2)
        except NotFoundException:
            yield render.drop_table(db1.table(t1))

    for t2 in db2.tablenames:
        try:
            t1 = db1.table(t2)
        except NotFoundException:
            yield render.create_table(db2.table(t2))



def __compare(config):
    with connect(config['db1']) as conn1:
        with connect(config['db2']) as conn2:
            db1 = Database(conn1)
            db2 = Database(conn2) 

            render = SqlRenderer()

            for change in compare_databases(render, db1, db2):
                if change is not None:
                    yield change

            for t1 in db1.tablenames:
                for t2 in db2.tablenames:
                    if t1 == t2:
                        tbl1 = db1.table(t1)
                        tbl2 = db2.table(t2)
                        for change in compare_data(render,tbl1,tbl2):
                            if change is not None:
                                yield change

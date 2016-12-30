# -*- coding:utf-8 -*-
import datetime
from pymongo import MongoClient
from gridfs import GridFS
from bson import ObjectId

try:
    DB_HOST = __conf__.DB_HOST
except Exception as e:
    DB_HOST = 'localhost'

try:
    DB_NAME = __conf__.DB_NAME
except Exception as e:
    DB_NAME = 'test'

try:
    import settings
except Exception as e:
    pass

def mongo_conv(d):
    if isinstance(d, (ObjectId, datetime.datetime)):
        return str(d)
    elif isinstance(d,(unicode,)):
        return str(d.encode('utf-8'))
    elif isinstance(d, list):
        return map(mongo_conv, d)
    elif isinstance(d, tuple):
        return tuple(map(mongo_conv, d))
    elif isinstance(d, dict):
        return dict([(mongo_conv(k), mongo_conv(v)) for k, v in d.items()])
    else:
        return d

class MongoIns(object):
    conn = {}

    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance_'):
            orig = super(MongoIns, cls)
            cls._instance_ = orig.__new__(cls, *args, **kw)

        return cls._instance_

    def get_conn(self, host = None, **kwargs):
        host = host or DB_HOST
        if not self.conn.get(host):
            self.conn[host] = MongoClient(host = host)

        return self.conn[host]

    def get_gfs(self, host = None, dbname = None):
        return GridFS(self.get_conn(host = host)[dbname or DB_NAME])

    def m_insert(self, table, **kwargs):
        """
            简单保存数据
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)

        return str(self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].insert(kwargs))


    def m_find_one(self, table, fields=None, **kwargs):
        """
            查询单条记录
            fields 指定需要输出的字段 like {'name':1}
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)

        return mongo_conv(self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].find_one(kwargs, fields)) or {}

    def m_list(self, table, fields=None, sorts = None, **kwargs):
        """
            列表查询
            fields 指定需要输出的字段 like {'name':1}
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        #if not sorts:
        #    sorts = [('_id', 1)]

        page_index = int(kwargs.pop('page_index', 1))
        page_size = int(kwargs.pop('page_size', 10))
        findall = kwargs.pop('findall', None)

        tb = self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table]
        count = tb.find(kwargs).count()
        if count and findall in [1, '1', True]:
            page_index = 1
            page_size = count

        page_num = (count + page_size - 1)/ page_size
        page = dict(page_index = page_index, page_size = page_size, page_num = page_num,allcount=count)

        if sorts:
            ret = mongo_conv(list(tb.find(kwargs, fields).sort(sorts).skip((page_index - 1) * page_size).limit(page_size)))
        else:
            ret = mongo_conv(list(tb.find(kwargs, fields).skip((page_index - 1) * page_size).limit(page_size)))


        return ret, page

    def m_cursor(self, table, fields=None, sorts = None, **kwargs):
        """
            结果指针查询, 不分页
            fields 指定需要输出的字段 like {'name':1}
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)

        tb = self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table]

        if sorts:
            ret = tb.find(kwargs, fields).sort(sorts)
        else:
            ret = tb.find(kwargs, fields)


        return ret

    def m_del(self, table, **kwargs):
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].remove(kwargs)
        return True

    def m_update(self, table, query, upsert = False, **kwargs):
        """
            简单更新逻辑
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].update(query, {'$set': kwargs}, upsert = upsert, multi = True)
        return True

    def m_update_original(self, table, query, doc, upsert = False, **kwargs):
        """
            复杂自定义更新逻辑
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].update(query, doc, upsert = upsert, multi = True)
        return True

    def m_unset(self, table, query, fields, **kwargs):
        """
            fields: ['col1', 'col2']
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        unset = {}
        for item in fields:
            unset[item] = 1

        self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].update(query, {'$unset': unset}, multi = True)

    def m_addToSet(self, table, query, upsert = False, **kwargs):
        """ 
            追加列表
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].update(query, {'$addToSet': kwargs}, upsert = upsert)

    def m_pull(self, table, query, **kwargs):
        """ 
            追加列表
            fields: {字段: 值}
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].update(query, {'$pull': kwargs})

    def m_count(self, table, **kwargs):
        """ 
            求数量
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        return self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].find(kwargs).count()

    def m_group(self, table, key, cond, initial, func,  **kwargs):
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)

        return self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].group(key, cond, initial, func, **kwargs)

    def m_distinct(self, table, key, query = {}, **kwargs):
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        return self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].find(query).distinct(key)

    def m_aggregate(self, table, pipeline, **kwargs):
        """
            aggregate
        """
        dbname = None
        if 'dbname' in kwargs:
            dbname=kwargs.pop('dbname')

        host = kwargs.pop('host', None)
        return self.get_conn(host = host, **kwargs)[dbname or DB_NAME][table].aggregate(pipeline)



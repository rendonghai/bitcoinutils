import os

from sqlalchemy import Column, ForeignKey, Integer, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine, Table
from sqlalchemy.sql.expression import select, update
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.reflection import Inspector

from bitcoinutils.utils import FlyweightMeta, with_metaclass

class Databas(object):


    def __init__(self):
        self.db_type = None

class SqliteDB(Databas, with_metaclass(FlyweightMeta)):

    def __init__(self, uri, alias, zone):
        super(SqliteDB, self).__init__()
        self.uri = uri
        self.alias = alias
        self.zone = zone
        self.db_type = 'sqlite'

class MysqlDB(Databas, with_metaclass(FlyweightMeta)):

    def __init__(self, name, alias, zone=None):
        super(MysqlDB, self).__init__()
        self.name = name
        self.alias = alias
        self.zone = zone
        self.db_type = 'mysql'

class DatabaseManager(object):

    __metaclass__ = FlyweightMeta

    def __init__(self):
        self.engine = None
        self.session = None

class SqliteDatabaseManager(DatabaseManager, with_metaclass(FlyweightMeta)):

    def __init__(self, *dbs):
        super(SqliteDatabaseManager, self).__init__()
        self.engine = create_engine('sqlite://', echo=False)

        self.attached_databases = dbs

        for db in dbs:
            self.engine.execute("attach database '{}' as {};".format(db.uri, db.alias))

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def __del__(self):
        self.session.close()

    def get_all_table_names(self):

        tb_names = []
        for db in self.attached_databases:
            res = self.session.execute("select name from {}.sqlite_master where type='table';".format(db.alias))
            if res:
                tb_names.extend(['{}.{}'.format(db.alias, r[0]) for r in res])

        return tb_names

    def get_table_names_from_db(self, db):

        tb_names = []
        res = self.session.execute("select name from {}.sqlite_master where type='table';".format(db.alias))

        if res:
            tb_names.extend(['{}.{}'.format(db.alias, r[0]) for r in res])

        return tb_names


class MysqlDatabaseManager(DatabaseManager, with_metaclass(FlyweightMeta)):

    def __init__(self, uri, *dbs):
        super(MysqlDatabaseManager, self).__init__()

        for db in dbs:
            if db.db_type != 'mysql':
                raise ValueError('Invalid DB Type')
        self.engine = create_engine(uri, echo=False)
        self.attached_dbs = dbs
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def __del__(self):
        self.session.close()

    def get_all_table_names(self):
        tb_names = []

        for db in self.attached_dbs:
            res = self.session.execute('show tables in {};'.format(db.name))
            if res:
                tb_names.extend(['{}.{}'.format(db.name, r[0]) for r in res])
        return tb_names

    def get_table_names_from_db(self, db):
        if db not in self.attached_dbs:
            raise ValueError('No such DB.')
        tb_names = []
        res = self.session.execute('show tables in {}'.format(db.alias))
        if res:
            tb_names.extend(['{}.{}'.format(db.alias, r[0]) for r in res])
        return tb_names

    def is_table_empty(self, db, table_name):
        res = self.session.execute('select 1 from {}.{} limit 1;'.format(db.alias, table_name))
        return  res is not None

    def is_table_existed(self, db, table_name):
        tb_names = self.get_table_names_from_db(db)
        return '{}.{}'.format(db.alias, table_name) in tb_names

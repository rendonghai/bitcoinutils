import sys, os
from bitcoinutils.dbmanager import MysqlDatabaseManager, MysqlDB
from datetime import datetime, timedelta


class KBarDurance(object):

    def __init__(self, durance):
        self.durance = int(durance)
        self.is_need_create_new_table = self.durance < 86400


class OneMinKBar(KBarDurance):

    def __init__(self):
        super(OneMinKBar, 1)

class KBarGenerator(object):

    def __init__(self, mysql_uri, target_db):

        self.db_mgr = MysqlDatabaseManager(mysql_uri, target_db)


    def create_kbar_data_table(self, db, instmt, coin, durance, timestamp=None):

        if timestamp:
            table_name = '{}.exch_{}_{}_{}_kbar_{}'.format(
                db.alias, instmt, coin, durance, timestamp)
        else:
            table_name = '{}.exch_{}_{}_{}_kar'.format(
                db.alias, instmt, coin, durance)

        stmt = '''CREATE TABLE IF NOT EXISTS  {}
        (time_start varchar(25),
        time_end varchar(25),
        exchange varchar(25),
        market varchar(25),
        open decimal(20, 8),
        high decimal(20, 8),
        low decimal(20, 8),
        close decimal(20, 8),
        volume decimal(20, 8),
        primary key (time_start , time_end)
        );
        '''

        print('Creating {} {} KBar with durance {}.'.format(instmt, coin, durance))
        self.db_mgr.session.execute(stmt.format(table_name))


    def create_last_kbar_data_table(self, db):

        table_name = '{}.last_updated_kbar'.format(db.alias)

        stmt = '''CREATE TABLE IF NOT EXISTS  {}
        (
        exchange varchar(25),
        market varchar(25),
        durance varchar(25),
        open decimal(20, 8),
        high decimal(20, 8),
        low decimal(20, 8),
        close decimal(20, 8),
        volume decimal(20, 8),
        primary key (exchange , market, durance)
        );
        '''

        print('Creating Last Updated KBar Table')
        self.db_mgr.session.execute(stmt.format(table_name))



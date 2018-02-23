import os
from bitcoinutils.dbmanager import MysqlDatabaseManager, MysqlDB
from datetime import datetime, timedelta
import re


class MergeUtil(object):

    def __init__(self, db_uri, from_db, to_db, zone_info, since_date=None):
        super(MergeUtil, self).__init__()
        self.db_uri = db_uri
        self.origin_db = from_db
        self.target_db = to_db
        self.zone_info = zone_info
        self.db_mgr = None
        if not since_date:
            self.since_date = (datetime.utcnow().date() - timedelta(2)).strftime('%Y%m%d')
        else:
            self.since_date = since_date

    def get_stored_instmt_and_coin(self):
        tb_names = self.db_mgr.get_table_names_from_db(self.origin_db)

        instmt_coin_map = {}
        for tb in tb_names:
            m = re.search(r'exch_(.*)_(.*)_snapshot_(\d+)', tb)
            if m:
                instmt = m.group(1)
                coin = m.group(2)
                if instmt not in instmt_coin_map:
                    instmt_coin_map[instmt] = [coin]
                else:
                    instmt_coin_map[instmt].append(coin)

        return instmt_coin_map

    def create_market_data_table(self, db, instmt, coin, timestamp):

        stmt = '''CREATE TABLE IF NOT EXISTS {}.exch_{}_{}_snapshot_{}
        (id int, trade_px decimal(20, 8), trade_volume decimal(20, 8),
        b1 decimal(20, 8), b2 decimal(20, 8), b3 decimal(20, 8),
        b4 decimal(20, 8), b5 decimal(20, 8), a1 decimal(20, 8),
        a2 decimal(20, 8), a3 decimal(20, 8), a4 decimal(20, 8),
        a5 decimal(20, 8), bq1 decimal(20, 8), bq2 decimal(20, 8),
        bq3 decimal(20, 8), bq4 decimal(20, 8), bq5 decimal(20, 8),
        aq1 decimal(20, 8), aq2 decimal(20, 8), aq3 decimal(20, 8),
        aq4 decimal(20, 8), aq5 decimal(20, 8), order_date_time varchar(25),
        trades_date_time varchar(25), update_type int,
        primary key (order_date_time, trades_date_time));'''

        print('Creating tables:{}.exch_{}_{}_snapshot'.format(db.alias,
                                                              instmt, coin, timestamp))
        self.db_mgr.session.execute(stmt.format(db.alias,instmt, coin, timestamp))

    def pickup_origin_instmt_table(self, instmt, coin):

        tb_names = self.db_mgr.get_table_names_from_db(self.origin_db)
        if tb_names:
            candidates = list(filter(lambda tn: '{}_{}'.format(instmt, coin) in tn , tb_names))
            #Get the newest table
            return sorted(candidates)[-1]
        else:
            return None

    # def get_origin_snapshot_time_range(self, instmt, coin):
    #     tb_name = self.pickup_origin_instmt_table(instmt, coin)
    #     if tb_name:
    #         stmt = 'select min(order_date_time) from {};'.format(tb_name)
    #         res = self.db_mgr.session.execute(stmt)
    #         begin = res.fetchone()[0] if res else None
    #         stmt = 'select max(trades_date_time) from {}'.format(tb_name)
    #         res = self.db_mgr.session.execute(stmt)
    #         end = res.fetchone()[0] if res else None
    #         return (begin, end)
    #     else:
    #         return (None, None)


    def merge(self):
        pass


class MysqlMergeUtil(MergeUtil):

    def __init__(self, db_uri, from_db, to_db, zone_info , since_date=None):
        super(MysqlMergeUtil, self).__init__(db_uri, from_db, to_db, zone_info, since_date)
        self.db_mgr = MysqlDatabaseManager(db_uri, from_db, to_db)

    def is_table_need_merge(self, instmt, coin, timestamp):

        tb_name_post_fix = 'exch_{}_{}_snapshot_{}'.format(instmt, coin, timestamp)

        origin_table_name = '{}.{}'.format(self.origin_db.alias, tb_name_post_fix)
        target_table_name = '{}.{}'.format(self.target_db.alias, tb_name_post_fix)

        stmt = '''select count(trades_date_time) from {}
        where {}.trades_date_time != '20000101 00:00:00.000000'
        and {}.order_date_time != '20000101 00:00:00.000000'
        and {}.trades_date_time not in
        (select trades_date_time from {});
        '''.format(origin_table_name,
                   origin_table_name,
                   origin_table_name,
                   origin_table_name,
                   target_table_name)

        res = self.db_mgr.session.execute(stmt)
        count = res.fetchone()[0]
        return count > 0

    def merge_unconfident_table(self, instmt, coin, timestamp):

        tb_name_post_fix = 'exch_{}_{}_snapshot_{}'.format(instmt, coin, timestamp)

        origin_table_name = '{}.{}'.format(self.origin_db.alias, tb_name_post_fix)
        target_table_name = '{}.{}'.format(self.target_db.alias, tb_name_post_fix)

        stmt = '''replace into {} (id, trade_px, trade_volume,
                                  b1, b2, b3, b4, b5, a1, a2, a3, a4, a5,
                                  bq1, bq2, bq3, bq4, bq5, aq1, aq2, aq3, aq4,
                                  aq5, order_date_time, trades_date_time, update_type)
        select distinct * from {}
        where {}.trades_date_time != '20000101 00:00:00.000000'
        and {}.order_date_time != '20000101 00:00:00.000000'
        and {}.trades_date_time not in
        (select distinct trades_date_time from {});
        '''.format(target_table_name, origin_table_name,
                   origin_table_name, origin_table_name,
                   origin_table_name, target_table_name)
        try:
            res = self.db_mgr.session.execute(stmt)
            self.db_mgr.session.commit()
        except Exception as e:
            print(e)
            self.db_mgr.session.rollback()

    def merge_confident_table(self, instmt, coin, timestamp):

        tb_name_post_fix = 'exch_{}_{}_snapshot_{}'.format(instmt, coin, timestamp)

        origin_table_name = '{}.{}'.format(self.origin_db.alias, tb_name_post_fix)
        target_table_name = '{}.{}'.format(self.target_db.alias, tb_name_post_fix)

        stmt = '''replace into {} (id, trade_px, trade_volume,
                                  b1,b2,b3,b4,b5,a1,a2,a3,a4,a5,
                                  bq1,bq2,bq3,bq4,bq5,aq1,aq2,aq3,aq4,
                                  aq5,order_date_time,trades_date_time,update_type)
        select distinct * from {}
        where {}.trades_date_time != '20000101 00:00:00.000000'
        and {}.order_date_time != '20000101 00:00:00.000000';
        '''.format(target_table_name, origin_table_name,
                   origin_table_name, origin_table_name)

        try:
            res = self.db_mgr.session.execute(stmt)
            self.db_mgr.session.commit()
        except Exception as e:
            print(e)
            self.db_mgr.session.rollback()

    def merge(self):

        origin_tb_names = self.db_mgr.get_table_names_from_db(self.origin_db)
        instmt_coin_table = {}
        for tn in origin_tb_names:
            r = re.search(r'.*.exch_([a-z]+)_(.*)_snapshot_(\d+)', tn)
            if r:
                if r.group(1) not in instmt_coin_table:
                    instmt_coin_table[r.group(1)] = {r.group(2): [r.group(3)]}
                else:
                    if r.group(2) not in instmt_coin_table[r.group(1)]:
                        instmt_coin_table[r.group(1)][r.group(2)] = [r.group(3)]
                    else:
                        instmt_coin_table[r.group(1)][r.group(2)].append(r.group(3))

        import json
        print(json.dumps(instmt_coin_table))

        for instmt in instmt_coin_table:
            for coin in instmt_coin_table[instmt]:
                for timestamp in instmt_coin_table[instmt][coin]:
                    if timestamp >= self.since_date:
                        print('Merging data table: exch_{}_{}_{}'.format(instmt, coin, timestamp))
                        if not self.db_mgr.is_table_existed(self.target_db,
                                                            'exch_{}_{}_{}'.format(instmt, coin, timestamp)):
                            self.create_market_data_table(self.target_db, instmt, coin, timestamp)
                        if self.zone_info.is_confident(self.origin_db.zone, instmt):
                            self.merge_confident_table(instmt, coin, timestamp)
                        else:
                            self.merge_unconfident_table(instmt, coin, timestamp)

import sys, os, re
from bitcoinutils.dbmanager import MysqlDatabaseManager, MysqlDB
from datetime import datetime, timedelta
from functools import reduce


class Trade(object):

    def __init__(self, instmt, coin,
                 open=0, high=0, low=0,
                 close=0, volume=0):
        self.instmt = instmt
        self.coin = coin
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

class KBarDurance(object):

    kbar_durance_map = {'1min': 1,
                        '3min': 3,
                        '5min': 5,
                        '15min': 15,
                        '30min': 30,
                        '1hour': 60,
                        '2hours': 120,
                        '6hours': 360,
                        '12hours': 720,
                        '1day': 1440
    }

    def __init__(self, durance):
        self._durance = durance
        self.is_need_create_new_table = KBarDurance.kbar_durance_map[self._durance] < 60

    @property
    def interval(self):
        secs = KBarDurance.kbar_durance_map[self._durance] * 60
        return timedelta(0, secs)

class KBarGenerator(object):

    def __init__(self, mysql_uri, target_db, since_date):

        self.db_mgr = MysqlDatabaseManager(mysql_uri, target_db)
        self.target_db = target_db
        if not since_date:
            self.since_date = (datetime.utcnow().date() - timedelta(2)).strftime('%Y%m%d')
        else:
            self.since_date = since_date

    def create_kbar_data_table_if_not_exists(self, db, instmt, coin, durance, timestamp=None):

        kb_durance = KBarDurance(durance)
        if kb_durance.is_need_create_new_table:
            table_name = '{}.exch_{}_{}_{}_kbar_{}'.format(
                db.alias, instmt, coin, durance, timestamp)
        else:
            table_name = '{}.exch_{}_{}_{}_kbar'.format(
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

        try:
            if not self.db_mgr.is_table_existed(db, table_name.split('.')[1]):
                print('Creating {} {} KBar with durance {}.'.format(instmt, coin, durance))
                self.db_mgr.session.execute(stmt.format(table_name))
                self.db_mgr.session.commit()
                return table_name
        except Exception as e:
            print(e)
            self.db_mgr.session.rollback()


    def create_last_kbar_data_table_if_not_exists(self, db):

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

        if not self.db_mgr.is_table_existed(db, 'last_updated_kbar'):
            print('Creating Last Updated KBar Table')
            self.db_mgr.session.execute(stmt.format(table_name))
            self.db_mgr.session.commit()

    def get_last_kbar_item(self, db, instmt, coin, durance):

        table_name = '{}.last_updated_kbar'.format(db.alias)

        stmt = '''select * from {} where exchange='{}'
        and market='{}' and durance='{}';
        '''

        res = self.db_mgr.session.execute(stmt.format(table_name, instmt,
                                                      coin, durance))
        if res:
            kbar = res.fetchone()
            return kbar
        else:
            return None

    def update_last_kbar_item(self, db, instmt, coin, durance,
                              open, high , low , close, volume):

        table_name = '{}.last_updated_kbar'.format(db.alias)

        stmt = '''replace into {} (exchange , market, durance , open, high , low , close, volume)
        values ('{}', '{}', '{}', {}, {}, {}, {}, {})
        '''.format(table_name, instmt, coin, durance, open, high, low, close, volume )

        try:
            self.db_mgr.session.execute(stmt)
            self.db_mgr.session.commit()
        except Exception as e:
            self.db_mgr.session.fallback()

    def kbar_generate_worker(self, db, instmt, coin, durance, timestamp):

        kbar_durance = KBarDurance(durance)

        data_table = '{}.exch_{}_{}_snapshot_{}'.format(db.alias, instmt, coin, timestamp)

        if kbar_durance.is_need_create_new_table:
            kbar_table = '{}.exch_{}_{}_{}_kbar_{}'.format(db.alias, instmt, coin, durance, timestamp)
        else:
            kbar_table = '{}.exch_{}_{}_{}_kbar'.format(db.alias, instmt, coin, durance)

        current_date = datetime.strptime(timestamp, '%Y%m%d').date()
        begin_time = datetime.strptime(timestamp, '%Y%m%d')

        last_kbar_item = self.get_last_kbar_item(db, instmt, coin, durance)

        last_trade = Trade(instmt, coin, last_kbar_item[3], last_kbar_item[4],
                           last_kbar_item[5], last_kbar_item[6],
                           last_kbar_item[7]) if last_kbar_item else Trade(instmt, coin)

        while True:
            if begin_time.date() > current_date:
                self.update_last_kbar_item(db, instmt, coin, durance,
                                           last_trade.open, last_trade.high,
                                           last_trade.low, last_trade.close,
                                           last_trade.volume)
                break

            end_time = begin_time + kbar_durance.interval

            stmt = '''
            select trade_px, trade_volume, trades_date_time from {}
            where update_type=2 and trades_date_time >= '{}' and trades_date_time < '{}'
            order by trades_date_time;
            '''.format(data_table, begin_time.strftime('%Y%m%d %H:%M:%S.%f'),
                       end_time.strftime('%Y%m%d %H:%M:%S.%f'))

            res = self.db_mgr.session.execute(stmt)
            items = [trade for trade in res]
            stmt = '''
            replace into {} (time_start, time_end, exchange, market, open, high , low, close, volume)
            values('{}', '{}', '{}', '{}', {}, {}, {}, {}, {})
            '''

            try:
                if not items:

                    self.db_mgr.session.execute(stmt.format(kbar_table,
                                                            begin_time.strftime('%Y%m%d %H:%M:%S.%f'),
                                                            end_time.strftime('%Y%m%d %H:%M:%S.%f'),
                                                            instmt, coin,
                                                            last_trade.open, last_trade.high, last_trade.low,
                                                            last_trade.close, last_trade.volume
                                                            ))
                    self.db_mgr.session.commit()
                else:
                    open = items[0][0]
                    high = reduce(lambda x,y:  x if x> y else y , [ item[0] for item in items])
                    low = reduce(lambda x,y:  x if x < y else y , [ item[0] for item in items])
                    close = items[-1][0]
                    volume = sum([item[1] for item in items])
                    #self.update_last_kbar_item(db, instmt, coin, durance, open, high, low, close, volume)
                    last_trade = Trade(instmt, coin, open, high, low, close, volume)
                    self.db_mgr.session.execute(stmt.format(kbar_table,
                                                            begin_time.strftime('%Y%m%d %H:%M:%S.%f'),
                                                            end_time.strftime('%Y%m%d %H:%M:%S.%f'),
                                                            instmt, coin,
                                                            open, high, low, close, volume))

                    self.db_mgr.session.commit()
            except Exception as e:
                print(e)
                self.db_mgr.session.rollback()

            begin_time = end_time

    def kbar_generate(self):
        tb_names = self.db_mgr.get_table_names_from_db(self.target_db)
        instmt_coin_table = {}
        self.create_last_kbar_data_table_if_not_exists(self.target_db)
        for tn in tb_names:
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
                        for durance in KBarDurance.kbar_durance_map.keys():
                            print('Generate KBar data for {} with {} by {} on {}'.format(instmt, coin, durance, timestamp))
                            self.create_kbar_data_table_if_not_exists(self.target_db, instmt, coin, durance, timestamp)
                            self.kbar_generate_worker(self.target_db,
                                                      instmt, coin, durance, timestamp)

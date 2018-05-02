#
import os
from bitcoinutils.dbmanager import MysqlDatabaseManager, MysqlDB
from datetime import datetime, timedelta
from bitcoinutils.utils import with_metaclass, FlyweightMeta
import re


class ExchangeRate(with_metaclass(FlyweightMeta)):


    supported_currency = ['CNY', 'JPY', 'HKD', 'EUR', 'KRW']

    def __init__(self):
        super(ExchangeRate, self).__init__()
        self.base = 'usd'
        self.usd = 1.0
        self.usdt = 1.0
        self.cny = None
        self.jpy = None
        self.hkd = None
        self.eur = None
        self.krw = None


class Exchange(with_metaclass(FlyweightMeta)):

    def __init__(self, name):
        self.exchange_name = name
        self.base_currency = 'usd'

    def get_base_currency(self):
        return self.base_currency

    def __str__(self):
        return self.exchange_name


class Okex(Exchange):

    def __init__(self, name):
        super(Okex, self).__init__(name)


class CoinOne(Exchange):

    def __init__(self, name):
        super(CoinOne, self).__init__(name)
        self.base_currency = 'krw'

class Bithumb(Exchange):

    def __init__(self, name):
        super(Bithumb, self).__init__(name)
        self.base_currency = 'krw'

class ExchangeFactory(with_metaclass(FlyweightMeta)):

    def get_exchange(self, **kw):
        
        instmtname = kw['exchange'].lower()

        if instmtname == 'Okex'.lower():
            exch = Okex('Okex')
        elif instmtname == 'CoinOne'.lower():
            exch = CoinOne('CoinOne')
        elif instmtname == 'Bithumb'.lower():
            exch = Bithumb('Bithumb')            
        else:
            exch = Exchange(name=kw['exchange'])
        return exch


class PriceDiffUtil(object):

    def __init__(self, db_uri, exchangerate_db, source_db, target_db, coin, first, second, date):

        super(PriceDiffUtil, self).__init__()
        self.exchangerate_db = exchangerate_db
        self.source_db = source_db
        self.target_db = target_db
        self.db_mgr = MysqlDatabaseManager(db_uri, exchangerate_db, source_db, target_db)

        self.coin = coin
        self.first_instmt = first.lower()
        self.second_instmt = second.lower()
        self.date = date

        self.exchange_rate = self.get_exchange_rate_of_day()
        self.price_diff_table = None

        self.create_price_diff_table()

    def get_exchange_rate_of_day(self):

        exchange_rate_table = 'exchange_rate'

        if not self.db_mgr.is_table_existed(self.exchangerate_db,
                                            exchange_rate_table) :
            raise ValueError('No exchange rate table found')

        stmt = '''select * from {}.{} where date_time = '{}';
        '''.format(self.exchangerate_db.alias, exchange_rate_table, self.date)

        res = self.db_mgr.session.execute(stmt)
        rates = [rate for rate in res]
        if not rates:
            raise ValueError('No record exchange rate for day {}'.format(self.date))
        exchange_rate = ExchangeRate()
        exchange_rate.cny = rates[0][1]
        exchange_rate.jpy = rates[0][2]
        exchange_rate.eur = rates[0][3]
        exchange_rate.krw = rates[0][4]

        return exchange_rate


    def create_price_diff_table(self):
        self.price_diff_table = 'price_diff_{}_{}_{}_{}'.format(self.coin.lower(),
                                                                self.first_instmt.lower(),
                                                                self.second_instmt.lower(),
                                                                self.date)

        stmt = '''create table if not exists {}.{}
        (timestamp varchar(25) primary key, price_diff decimal(20, 8), price_diff_percent decimal(20, 8));'''
        if not self.db_mgr.is_table_existed(self.target_db,
                                            self.price_diff_table):
            self.db_mgr.session.execute(stmt.format(self.target_db.alias, self.price_diff_table))

    def get_price_by_tick(self, instmt, coin, date, start_time, tick):
        table_name = None
        tb_names = self.db_mgr.get_table_names_from_db(self.source_db)

        for tb in tb_names:
            m = re.search(r'exch_(.*)_(.*)_snapshot_(\d+)', tb)
            if m and instmt == m.group(1) \
               and coin in m.group(2) and m.group(3) == date:
                table_name = tb
                break
        else:
            raise ValueError('No instmt and coin pair table found')

        s_time = start_time.strftime('%Y%m%d %H:%M:%S.%f')
        d_time = (start_time + timedelta(0, tick)).strftime('%Y%m%d %H:%M:%S.%f')
        stmt = '''select trade_px from {} where trades_date_time >= '{}'
        and trades_date_time < '{}' and update_type=2
        '''.format(table_name, s_time, d_time)

        res = self.db_mgr.session.execute(stmt)

        prices = [px for px in res]

        if prices:
            return float(prices[-1][0])
        else:
            return 0

    def consistent_currency_by_usd(self, instmt, coin, price, exchange_rate):
        er = exchange_rate
        base_currency = instmt.get_base_currency()
        coin_currency = re.split('_|-', coin)
        if len(coin_currency) > 1:
            nation = coin_currency[1]
            return price / float(getattr(er, nation.lower()))
        else:
            for item in er.supported_currency:
                if item.lower() in coin_currency:
                    return price / float(getattr(exchange_rate, item.lower()))
            else:
                return price / float(getattr(er, base_currency))

    def get_exact_coin_name(self, instmt, coin, date):

        tb_names = self.db_mgr.get_table_names_from_db(self.source_db)

        for tb in tb_names:
            m = re.search(r'exch_(.*)_(.*)_snapshot_(\d+)', tb)
            if m and instmt == m.group(1) \
               and coin in m.group(2) and m.group(3) == date:
                return m.group(2)
        else:
            raise ValueError('No instmt and coin pair table found')


    def generate_price_diff(self):
        exact_date = datetime.strptime(self.date, '%Y%m%d').date()
        begin_time = datetime.strptime(self.date, '%Y%m%d')

        exch_factory = ExchangeFactory()
        exch1 = exch_factory.get_exchange(exchange=self.first_instmt)
        exch2 = exch_factory.get_exchange(exchange=self.second_instmt)

        while True:
            if begin_time.date() > exact_date:
                break
            end_time = begin_time + timedelta(0, 60)

            coin1 = self.get_exact_coin_name(self.first_instmt, self.coin, self.date)
            coin2 = self.get_exact_coin_name(self.second_instmt, self.coin, self.date)

            price1 = self.get_price_by_tick(self.first_instmt,
                                            self.coin, self.date, begin_time, 60)
            price2 = self.get_price_by_tick(self.second_instmt,
                                            self.coin, self.date, begin_time, 60)

            if price1 == 0 or price2 == 0:
                begin_time = end_time
                continue

            consistent_currency1 = self.consistent_currency_by_usd(exch1, coin1, price1, self.exchange_rate)
            consistent_currency2 = self.consistent_currency_by_usd(exch2, coin2, price2, self.exchange_rate)

            price_diff = consistent_currency1 - consistent_currency2
            price_diff_percent = price_diff / min(consistent_currency1, consistent_currency2) * 100.0

            stmt = '''replace into {}.{} (timestamp, price_diff, price_diff_percent) values('{}',{},{})
            '''.format(self.target_db.alias, self.price_diff_table,
                       begin_time.strftime('%Y%m%d %H:%M:%S.%f'),
                       price_diff,
                       price_diff_percent)

            try:
                print(stmt)
                self.db_mgr.session.execute(stmt)
                self.db_mgr.session.commit()
            except Exception as e:
                print(e)
                self.db_mgr.session.rollback()

            begin_time = end_time

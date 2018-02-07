#
import requests
import time
import threading
import json
import zmq
import re
from bitcoinutils.utils import with_metaclass, FlyweightMeta
from bitcoinutils.notification import MailNotification
from bitcoinutils.dbmanager import MysqlDatabaseManager, MysqlDB
import threading

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


class ExchangeFactory(with_metaclass(FlyweightMeta)):

    def get_exchange(self, **kw):

        if kw['exchange'] == 'Okex':
            exch = Okex('Okex')
        elif kw['exchange'] == 'CoinOne':
            exch = CoinOne('CoinOne')
        else:
            exch = Exchange()
        return exch


class ExchangeRate(with_metaclass(FlyweightMeta)):

    fixer_uri = 'http://api.fixer.io/latest?base=USD&symbols=CNY,JPY,HKD,EUR,KRW'

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

        self.lock = threading.Lock()
        self.update_exchage_rate()

    def update_exchage_rate(self):

        with requests.Session() as sess:
            try:
                self.lock.acquire()
                res = sess.get(self.fixer_uri).json()

                for item in ExchangeRate.supported_currency:
                    setattr(self, item.lower(), res['rates'][item])
            except Exception as e:
                print(e)
            finally:
                self.lock.release()

    def __str__(self):
        return ' '.join(['{}:{}'.format(x, getattr(self, x.lower())) for x in self.supported_currency])


class BCEXSnapshot(object):

    def __init__(self, exchange, currency='usd', volume=0, price=0):
        exch_factory = ExchangeFactory()
        self.exchange = exch_factory.get_exchange(exchange=exchange)
        self.currency = currency
        self.volume = volume
        self.price = self.consistent_currency_by_usd(float(price))

    def consistent_currency_by_usd(self, price):
        er = ExchangeRate()

        base_currency = self.exchange.get_base_currency()
        coin_currency = re.split('_|-', self.currency)
        if len(coin_currency) > 1:
            nation = coin_currency[1]
            return price / float(getattr(er, nation.lower()))
        else:
            for item in er.supported_currency:
                if item.lower() in coin_currency:
                    return price / float(getattr(er, item.lower()))
            else:
                return price / float(getattr(er, base_currency))

    def update_snapshot(self, price, volume):
        print('update pinned snapshot')
        self.volume = volume
        self.price = self.consistent_currency_by_usd(price)

    def __str__(self):
        return 'exchange: {} currency: {} price: {} volume: {}'.format(self.exchange,
                                                                       self.currency,
                                                                       self.price,
                                                                       self.volume)


class MonitorConfig(with_metaclass(FlyweightMeta)):

    def __init__(self):

        super(MonitorConfig, self).__init__()
        self.config_file = None
        self.mysql_uri = None
        self.mysql_db = None
        self.rules = None
        self.mail_config = None
        self.rule_table_name = 'exchange_data_monitor_rules'
        self.lock = threading.Lock()

    def load_config(self, mysql_uri, mysql_db, config_file):
        self.mysql_uri = mysql_uri
        self.config_file = config_file
        with open(self.config_file, 'r') as f:
            content = json.loads(f.read())
            self.mail_config = content['mail']

        self.mysql_db = MysqlDB(mysql_db, mysql_db)

        self.db_mgr = MysqlDatabaseManager(self.mysql_uri, self.mysql_db)

        stmt = '''
        CREATE TABLE IF NOT EXISTS  {}.{}
        ( id int not null AUTO_INCREMENT, exch1 varchar(25), exch2 varchar(25),
        currency varchar(25), price_diff_threshold decimal(20, 8),
        succeeded_rule_id varchar(128),
        status varchar(16),
        primary key (id)
        );
        '''.format(self.mysql_db.alias, self.rule_table_name)
        if not self.db_mgr.is_table_existed(self.mysql_db, self.rule_table_name):
            self.db_mgr.session.execute(stmt)

        self.fetch_rules()

    def fetch_rules(self):

        stmt = '''
        select id, exch1, exch2, currency, price_diff_threshold
        from {}.{} where status = 'active';
        '''.format(self.mysql_db.alias, self.rule_table_name)

        try:
            self.lock.acquire()
            res = self.db_mgr.session.execute(stmt)
            if self.rules:
                del self.rules

            self.rules = {}
            for item in res:
                self.rules[item[0]] = item[1:]
        except Exception as e:
            print(e)
        finally:
            self.lock.release()

    def deactive_rule(self, id):

        stmt = '''
        update {}.{} set status = 'deactive' where id = {};
        '''.format(self.mysql_db.alias, self.rule_table_name, id)

        try:
            self.lock.acquire()
            self.db_mgr.session.execute(stmt)
            self.db_mgr.session.commit()
        except Exception as e:
            print(e)
        finally:
            self.lock.release()

    def active_rule(self, id):
        stmt = '''
        update {}.{} set status = 'active' where id = {};
        '''.format(self.mysql_db.alias, self.rule_table_name, id)

        try:
            self.lock.acquire()
            self.db_mgr.session.execute(stmt)
            self.db_mgr.session.commit()
        except Exception as e:
            print(e)
        finally:
            self.lock.release()

    def get_succeeded_rule_ids(self, key):
        stmt = '''
        select succeeded_rule_id from {}.{} where id = {};
        '''.format(self.mysql_db.alias, self.rule_table_name, key)

        try:
            self.lock.acquire()
            res = self.db_mgr.session.execute(stmt).fetchone()
            if res[0]:
                ids = list(map(lambda x:int(x.strip()), re.split(',|\ |;', res[0])))
                return ids
            else:
                return None
        except Exception as e:
            print(e)
        finally:
            self.lock.release()


class ExchangeDataMonitor(object):

    def __init__(self, feed_uri, mysql_uri, mysql_db, config_file):
        self.feed_uri = feed_uri
        self.config = MonitorConfig()
        self.config.load_config(mysql_uri, mysql_db, config_file)
        mail_config = self.config.mail_config

        self.mail_notifier =  MailNotification(mail_config['smtp_server'],
                                               mail_config['me'],
                                               mail_config['mail_pwd'],
                                               mail_config['receivers'])

        self.pinned_snapshots = []

        for rule in self.config.rules.values():
            item = {}
            exch1 = rule[0]
            exch2 = rule[1]
            currency = rule[2]
            price_diff_threshold = rule[3]
            snap1 = BCEXSnapshot(exchange=exch1, currency=currency)
            snap2 = BCEXSnapshot(exchange=exch2, currency=currency)
            item1 = {(exch1, currency): snap1}
            item2 = {(exch2, currency): snap2}
            self.pinned_snapshots.extend([item1, item2])
        context = zmq.Context()
        self.sock = context.socket(zmq.SUB)
        self.sock.connect(self.feed_uri)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, '')

    def update_snapshot(self, exchange, coin, price, volume):

        for snap in self.pinned_snapshots:
            for item in snap:
                if item[1] in coin and (exchange, coin) in snap:
                    snap[(exchange, coin)].update_snapshot(price, volume)

    def check_price_difference(self):

        over_threshold_data = []

        for key in self.config.rules:
            rule = self.config.rules[key]
            exch1 = rule[0]
            exch2 = rule[1]
            coin = rule[2]
            price_gap_threshold = rule[3]
            print(exch1, exch2, coin, price_gap_threshold)
            price1 = 0
            price2 = 0
            for item in self.pinned_snapshots:
                if (exch1, coin) in item.keys():
                    price1 = item[(exch1, coin)].price
                if (exch2, coin) in item.keys():
                    price2 = item[(exch2, coin)].price
            if price1 !=0 and price2 !=0:
                price_diff = abs(price1 - price2)
                print(price_diff)
                if price_diff > price_gap_threshold:
                    over_threshold_data.append((key, exch1, exch2, coin, price1, price2, price_diff))

        return over_threshold_data

    def print_pinned_snapshots(self):
        for item in self.pinned_snapshots:
            for sub in item:
                print (item[sub])

    def monitor(self):

        print('Start receiving exchange data...')
        self.print_pinned_snapshots()
        while True:
            ret = self.sock.recv()
            res = json.loads(ret.decode('UTF-8'))
            if 'update_type' in res and res['update_type'] == 2 and\
               'table' in res and res['table'] == 'exchanges_snapshot':
                exch = res['exchange']
                coin = res['instmt']
                volume = float(res['trade_volume'])
                price = float(res['trade_px'])
                print(exch, coin, price, volume)
                self.update_snapshot(exch, coin.lower(), price, volume)
                self.print_pinned_snapshots()
                over_threshold = self.check_price_difference()
                if over_threshold:
                    for item in over_threshold:
                        key = item[0]
                        print('X'*100)
                        print(key)
                        self.config.deactive_rule(key)
                        ids = self.config.get_succeeded_rule_ids(key)
                        if ids:
                            for sk in ids:
                                self.config.active_rule(sk)
                    subject = 'Price Threshold Triggered'
                    content = ''
                    for item in over_threshold:
                        content += str(item)
                        content += '\n'
                    #print(subject, content)
                    self.mail_notifier.send_notification(subject, content)
                else:
                    pass
                self.config.fetch_rules()

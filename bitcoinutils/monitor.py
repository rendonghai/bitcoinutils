#coding:utf-8
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
from datetime import datetime

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

        if kw['exchange'] == 'Okex':
            exch = Okex('Okex')
        elif kw['exchange'] == 'CoinOne':
            exch = CoinOne('CoinOne')
        elif kw['exchange'] == 'Bithumb':
            exch = Bithumb('Bithumb')            
        else:
            exch = Exchange(name=kw['exchange'])
        return exch


class ExchangeRate(with_metaclass(FlyweightMeta)):

    aliyun_uri = 'http://ali-waihui.showapi.com/waihui-transform?fromCode='

    supported_currency = ['CNY', 'JPY', 'HKD', 'EUR', 'KRW']

    def __init__(self):
        super(ExchangeRate, self).__init__()
        self.AppCode = '4a6b2d8ebc494dd6808d78919cfb5018'
        self.base = 'USD'
        self.usd = 1.0
        self.usdt = 1.0
        self.cny = 0.0
        self.jpy = 0.0
        self.hkd = 0.0
        self.eur = 0.0
        self.krw = 0.0

        self.config = None
        self.lock = threading.Lock()
        self.exchange_rate_table_name = 'exchange_rate'
        #self.update_exchage_rate()

    def set_config(self, config):
        self.config = config

    def initialize_exchange_rate_table(self):

        stmt = '''
        CREATE TABLE IF NOT EXISTS  {}.{}
        (date_time varchar(25), cny decimal(20, 8),
         jpy decimal(20, 8), eur decimal(20, 8),
         krw decimal(20, 8), primary key (date_time)
        );
        '''.format(self.config.mysql_db.alias, self.exchange_rate_table_name)

        if not self.config.db_mgr.is_table_existed(self.config.mysql_db,
                                                   self.exchange_rate_table_name):
            try:
                self.config.db_mgr.session.execute(stmt)
                self.config.db_mgr.session.commit()
            except Exception as e:
                print(e)
                self.config.db_mgr.session.rollback()

    def update_exchage_rate(self):

        with requests.Session() as sess:
            try:
                self.lock.acquire()
                headers = {'content-type': 'application/json', 'Authorization': 'APPCODE ' + self.AppCode}
                
                for item in ExchangeRate.supported_currency:
                    res = sess.get(self.aliyun_uri + self.base + '&money=1' + '&toCode=' + item, headers=headers).json()
                    
                    if not res and not res['showapi_res_code'] == '0':
                        raise ValueError
                    
                    setattr(self, item.lower(), res['showapi_res_body']['money'])

                if not self.config:
                    raise ValueError

                self.config.safe_exchage_rate(self.config.mysql_db.alias, self.exchange_rate_table_name, self.cny, self.jpy, self.eur, self.krw)
            except Exception as e:
                print(e)
            finally:
                self.lock.release()

    def __str__(self):
        return ' '.join(['{}:{}'.format(x, getattr(self, x.lower())) for x in self.supported_currency])

exchange_rate = ExchangeRate()

class BCEXSnapshot(object):

    def __init__(self, exchange, currency='usd', volume=0, price=0):
        exch_factory = ExchangeFactory()
        self.exchange = exch_factory.get_exchange(exchange=exchange)
        self.currency = currency
        self.volume = volume
        self.price = self.consistent_currency_by_usd(float(price))

    def consistent_currency_by_usd(self, price):
        er = exchange_rate
        base_currency = self.exchange.get_base_currency()
        coin_currency = re.split('_|-', self.currency)
        if len(coin_currency) > 1:
            nation = coin_currency[1]
            return price / float(getattr(er, nation.lower()))
        else:
            for item in er.supported_currency:
                if item.lower() in coin_currency:
                    return price / float(getattr(exchange_rate, item.lower()))
            else:
                return price / float(getattr(er, base_currency))

    def update_snapshot(self, price, volume):
        #print('update pinned snapshot')
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
        currency varchar(25), price_diff_threshold decimal(20, 8), direction varchar(25),
        succeeded_rule_id varchar(128),
        status varchar(16),
        primary key (id)
        );
        '''.format(self.mysql_db.alias, self.rule_table_name)
        if not self.db_mgr.is_table_existed(self.mysql_db, self.rule_table_name):
            try:
                self.db_mgr.session.execute(stmt)
                self.db_mgr.session.commit()
            except Exception as e:
                print(e)
                self.db_mgr.session.rollback()

        self.fetch_rules()

    def fetch_rules(self):

        stmt = '''
        select id, exch1, exch2, currency, price_diff_threshold, direction
        from {}.{} where status = 'active';
        '''.format(self.mysql_db.alias, self.rule_table_name)

        try:
            self.lock.acquire()
            print('Fetching Rules')
            self.db_mgr.session.execute('flush table {}.{}'.format(self.mysql_db.alias, self.rule_table_name))
            self.db_mgr.session.commit()
            res = self.db_mgr.session.execute(stmt)
            if self.rules:
                del self.rules

            self.rules = {}
            for item in res:
                self.rules[item[0]] = list(item[1:])
            print(self.rules)
        except Exception as e:
            print(e)
            self.db_mgr.session.rollback()
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
            
    def safe_exchage_rate(self, dbalias, tablename, cny, jpy, eur, krw):
        date = datetime.utcnow().date().strftime("%Y%m%d")
        stmt = '''replace into {}.{} (date_time, cny, jpy, eur, krw)
        values ('{}', {}, {}, {}, {});
        '''.format(dbalias,
                   tablename, date,
                   cny, jpy, eur, krw)
        
        try:
            self.lock.acquire()
            self.db_mgr.session.execute(stmt)
            self.db_mgr.session.commit()   
        except Exception as e:
            print(e)
        finally:
            self.lock.release()
    


class ExchangeDataMonitor(object):

    def __init__(self, feed_uri, mysql_uri, mysql_db, config_file, hit_times, sendmail_interval):
        self.feed_uri = feed_uri
        self.config = MonitorConfig()
        self.config.load_config(mysql_uri, mysql_db, config_file)
        self.hit_times = hit_times
        self.sendmail_interval = sendmail_interval
        self.rule_hit_map = {}
        self.rule_lastmail_sendtime = {}
        mail_config = self.config.mail_config

        self.mail_notifier =  MailNotification(mail_config['smtp_server'],
                                               mail_config['me'],
                                               mail_config['mail_pwd'],
                                               mail_config['receivers'])

        er = exchange_rate
        er.set_config(self.config)
        er.initialize_exchange_rate_table()
        er.update_exchage_rate()

        self.pinned_snapshots = []

        print ('init rule content')
        self.update_snapshots_item()
        context = zmq.Context()
        self.sock = context.socket(zmq.SUB)
        self.sock.connect(self.feed_uri)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, '')
        
    def find_key_in_pinned_snapshots(self, key):
        for item in self.pinned_snapshots:
            if key in item:
                return True
        return False
    
    def update_snapshots_item(self):
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
            if not self.find_key_in_pinned_snapshots((exch1, currency)):
                self.pinned_snapshots.append(item1)  
            if not self.find_key_in_pinned_snapshots((exch2, currency)):
                self.pinned_snapshots.append(item2)               

    def update_snapshot(self, exchange, coin, price, volume):

        for snap in self.pinned_snapshots:
            for item in snap:
                if item[1] in coin and (exchange, item[1]) in snap:
                    snap[(exchange, item[1])].update_snapshot(price, volume)

    def update_task(self):
        self.config.fetch_rules()
        self.update_snapshots_item()
        self.print_pinned_snapshots()
        
    def check_price_difference(self, exchange, coin_code):

        over_threshold_data = []

        for key in self.config.rules:
            rule = self.config.rules[key]
            exch1 = rule[0]
            exch2 = rule[1]
            coin = rule[2]
            price_gap_threshold = rule[3]
            direction = rule[4]
            #print(exch1, exch2, coin, price_gap_threshold)
            price1 = 0
            price2 = 0
            for item in self.pinned_snapshots:
                if (exch1, coin) in item.keys():
                    price1 = item[(exch1, coin)].price
                if (exch2, coin) in item.keys():
                    price2 = item[(exch2, coin)].price
            if price1 !=0 and price2 !=0 and \
               exchange.lower() in [exch1.lower(), exch2.lower()] and \
               coin.lower() in coin_code.lower():
                price_diff = price1 - price2
                price_diff_percent = price_diff / min(price1, price2) * 100.0
                #print('F'*100)
                #print('Hit rule {}'.format(key))
                #print(price_diff, price_diff_percent)
                if key not in self.rule_hit_map:
                    self.rule_hit_map[key] = 0
                if key not in self.rule_lastmail_sendtime:
                    self.rule_lastmail_sendtime[key] = datetime.now()                
                #print('Checking rule:{}'.format(key))
                #print(self.config.rules[key])
                #print('Hit count {}'.format(self.rule_hit_map[key]))
                if direction.lower() == 'up' and price_diff_percent >= price_gap_threshold or \
                   direction.lower() == 'down' and price_diff_percent < price_gap_threshold:
                    #self.config.rules[key][-1] += 1
                    self.rule_hit_map[key] += 1
                    if self.rule_hit_map[key] >= self.hit_times and \
                       (datetime.now() - self.rule_lastmail_sendtime[key]).total_seconds()/60 >= self.sendmail_interval:
                        over_threshold_data.append((key, exch1, exch2, coin, price1, price2, price_diff, price_diff_percent))
                        self.rule_hit_map[key] = 0
                        self.rule_lastmail_sendtime[key] = datetime.now()
                else:
                    #print('Unset hit count in rule {}'.format(key))
                    self.rule_hit_map[key] = 0

        return over_threshold_data

    def print_pinned_snapshots(self):
        for item in self.pinned_snapshots:
            for sub in item:
                print ("print_pinned_snapshots",item[sub])

    def monitor(self):

        print('Start receiving exchange data...')
        self.print_pinned_snapshots()
        #er = ExchangeRate()
        er = exchange_rate
        while True:
            ret = self.sock.recv()
            res = json.loads(ret.decode('UTF-8'))
            if 'update_type' in res and res['update_type'] == 2 and\
               'table' in res and res['table'] == 'exchanges_snapshot':
                exch = res['exchange']
                coin = res['instmt']
                volume = float(res['trade_volume'])
                price = float(res['trade_px'])
                self.update_snapshot(exch, coin.lower(), price, volume)
                #self.print_pinned_snapshots()
                over_threshold = self.check_price_difference(exch, coin)
                if over_threshold:
                    for item in over_threshold:
                        key = item[0]
#                        self.config.deactive_rule(key)
                        ids = self.config.get_succeeded_rule_ids(key)
                        if ids:
                            for sk in ids:
                                self.config.active_rule(sk)
                    subject = u'交易所差价提醒'
                    content = u''
                    for item in over_threshold:
                        #content += str(item)
                        #content += '\n'
                        content += u'''货币: {}\n交易所1: {}\t{}\n交易所2: {}\t{}\n交易差价: {}\n交易差价比例{}%\n'''.format(item[3],
                                   item[1], round(item[4],2),
                                   item[2], round(item[5],2),
                                   round(item[6],2), round(item[7],2)
                        )
                    print('Trigger Mail Notification')
                    print(subject, content)
                    self.mail_notifier.send_notification(subject, content)
                    self.update_task()
                else:
                    pass

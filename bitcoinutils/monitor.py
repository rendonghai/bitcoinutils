#
import requests
import time
import threading
import json
import zmq
import re
from bitcoinutils.utils import with_metaclass, FlyweightMeta
from bitcoinutils.notification import MailNotification

class Exchange(with_metaclass(FlyweightMeta)):

    def __init__(self, name):
        super(Exchange, self).___init__()
        self.exchange_name = name
        self.base_currency = 'usd'

    def get_base_currency(self):
        return self.base_currency


class Okex(Exchange, with_metaclass(FlyweightMeta)):

    def __init__(self, name):
        super(Okex, self).__init__(name)


class CoinOne(Exchange, with_metaclass(FlyweightMeta)):

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
        self.usd = 1
        self.usdt = 1
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
                print(self)
            except Exception as e:
                print(e)
            finally:
                self.lock.release()

    def __str__(self):
        return ' '.join(['{}:{}'.format(x, getattr(self, x.lower())) for x in self.supported_currency])


class BCEXSnapshot(object):

    def __init__(self, exchange, currency='usd', volume=0, price=0):
        exch_factory = ExchangeFactory()
        self.exchange = exch_factory.get_exchange(exchange)
        self.currency = currency
        self.volume = volume
        self.price = self.consistent_currency_by_usd(price)

    def consistent_currency_by_usd(self, price):

        er = ExchangeRate()

        base_currency = self.exchange.get_base_currency()

        coin_currency = re.split('_|-', self.currency)
        if len(coin_currency) > 1:
            nation = coin_currency[1]
            return price / float(getattr(er, nation.lower()))
        else:
            for item in er.supported_currency:
                if item in coin_currency.lower():
                    return price / float(getattr(er, item))
            else:
                return price / float(getattr(er, getattr(er, er.base)))

    def update_snapshot(self, volume, price):
        print('update pinned snapshot')
        self.volume = volume
        self.price = self.consistent_currency_by_usd(price)


class MonitorConfig(with_metaclass(FlyweightMeta)):

    def __init__(self):

        super(MonitorConfig, self).__init__()
        self.config_file = None
        self.rules = None
        self.mail_config = None

    def load_config(self, config_file):
        self.config_file = config_file
        with open(self.config_file, 'r') as f:
            content = json.loads(f.read())
            self.rules = content['rules']
            self.mail_config = content['mail']


class ExchangeDataMonitor(object):

    def __init__(self, feed_uri, config_file):
        self.feed_uri = feed_uri
        self.config = MonitorConfig()
        self.config.load_config(config_file)
        mail_config = self.config.mail_config

        self.mail_notifier =  MailNotification(mail_config['smtp_server'],
                                               mail_config['me'],
                                               mail_config['mail_pwd'],
                                               mail_config['receivers'])
        self.pinned_snapshots = []

        for rule in self.config.rules:
            item = {}
            for exch in rule['exchanges']:
                for currency in rule['instmt_code']:
                    snap = BCEXSnapshot(exch, currency)
                    if (exch, currency) in item:
                        item[(exch, currency)].append(snap)
                    else:
                        item[(exch, currency)] = [snap]
            self.pinned_snapshots.append(item)

        context = mq.Context()
        self.sock = context.socket(zmq.SUB)
        self.sock.connect(self.feed_uri)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, '')

    def update_snapshot(self, exchange, coin, volume, price):

        for item in self.pinned_snapshots:
            if (exchage, coin) in item:
                item[(exchange, coin)].update_snapshot(volume, price)

    def check_price_difference(self):

        over_threshold_data = []

        for rule in self.config.rules:
            exch1 = rule['exchanges'][0]
            exch2 = rule['exchanges'][1]
            price_gap_threshold = rule['price_gap_threshold']
            for coin in rule['price_gap_threshold']:
                for item in self.pinned_snapshots:
                    if (exch1, coin) in item and (exch2, coin) in item:
                        price1 = item[(exch1, coin)].price
                        price2 = item[(exch2, coin)].price

                        if price1 !=0 and price2 !=0:
                            price_diff = abs(price1 - price2)
                            if price_diff > price_gap_threshold:
                                over_threshold_data.append( (exch1, exch2, coin, price1, price2, price_diff) )

        return over_threshold_data


    def check_volume_difference(self):

        over_threshold_data = []
        for rule in self.config.rules:
            exch1 = rule['exchanges'][0]
            exch2 = rule['exchanges'][1]
            volume_threshold = rule['volume_threshold']
            for coin in rule['volume_threshold']:
                for item in self.pinned_snapshots:
                    if (exch1, coin) in item and (exch2, coin) in item:
                        v1 = item[(exch1, coin)].volume
                        v2 = item[(exch2, coin)].volume

                        if v1 !=0 and v2 != 0:
                            volume_diff = abs(v1 - v2)
                            if volume_diff > volume_threshold:
                                over_threshold_data.append( (exch1, exch2, coin, price1, price2, price_diff) )

        return over_threshold_data

    def monitor(self):

        while True:
            ret = self.sock.recv()
            res = json.loads(ret.decode('UTF-8'))

            if 'update_type' in res and res['update_type'] == 2 and\
               'table' in res and res['table'] == 'exchanges_snapshot':

                exch = res['exchange']
                coin = res['instmt']
                volume = res['trade_volume']
                price = res['trade_px']
                self.update_snapshot(exch, coin, volume, price)

                over_threshold = self.check_price_difference()
                if over_threshold:
                    subject = 'Price Threshold Triggered'
                    content = '\n'.join(over_threshold)
                    self.mail_notifier.send_notification(subject, content)

                over_threshold = self.check_volume_difference()
                if over_threshold:
                    subject = 'Volume Threshold Triggered'
                    content = '\n'.join()
                    self.mail_notifier.send_notification(subject, content)

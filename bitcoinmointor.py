#!/usr/bin/env python3

import argparse
from apscheduler.schedulers.blocking import BlockingScheduler
from bitcoinutils.monitor import ExchangeRate, ExchangeDataMonitor
import threading, time

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Bitcoin exchange market data monitor tool.')
    parser.add_argument('--zmqsrc', action='store',
                        help='BitCoin Exchange Handler ZMQ Source')
    parser.add_argument('--config', action='store',
                        help='Configuration File')
    parser.add_argument('--mysqluri', action='store',
                        help='Mysql URI')
    parser.add_argument('--mysqldb', action='store',
                        help='Mysql Database name')

    args = parser.parse_args()

    er = ExchangeRate()

    scheduler = BlockingScheduler()
    scheduler.add_job(er.update_exchage_rate, 'interval', hours=1)

    thread1 = threading.Thread(target=scheduler.start)
    monitor = ExchangeDataMonitor(args.zmqsrc, args.mysqluri, args.mysqldb, args.config)
    thread2 = threading.Thread(target=monitor.monitor)
    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()
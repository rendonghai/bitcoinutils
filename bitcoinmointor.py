#!/usr/bin/env python3

import argparse
from apscheduler.schedulers.blocking import BlockingScheduler
from bitcoinutils.monitor import ExchangeRate, ExchangeDataMonitor, exchange_rate
import threading, time
from datetime import datetime

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
    parser.add_argument('--hittimes', action='store',
                        help='Max rule hit times')

    args = parser.parse_args()

    monitor = ExchangeDataMonitor(args.zmqsrc, args.mysqluri, args.mysqldb, args.config, int(args.hittimes))
    scheduler = BlockingScheduler()
    #er = ExchangeRate()
    er = exchange_rate
    scheduler.add_job(er.update_exchage_rate, 'interval', hours=12)
    scheduler.add_job(monitor.update_task, 'interval', minutes=1)

    thread1 = threading.Thread(target=scheduler.start)
    thread2 = threading.Thread(target=monitor.monitor)
    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

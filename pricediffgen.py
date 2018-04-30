#!/usr/bin/env python3

import argparse
import sys, os
from bitcoinutils.dbmanager import MysqlDatabaseManager, MysqlDB

from bitcoinutils.pricediffutil import PriceDiffUtil
from pprint import pprint

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Price spread of bitcoin in different instrument.')
    parser.add_argument('--mysqluri', action='store',
                        help='Mysql Server URI.')
    parser.add_argument('--exchangeratedb', action='store',
                        help='Currency exchangerate Database')       
    parser.add_argument('--sourcedb', action='store',
                        help='Source data Database')        
    parser.add_argument('--targetdb', action='store',
                        help='Target Database to generator K Bar data')
    parser.add_argument('--coin', action='store',
                        help='The coin to be calculated')
    parser.add_argument('--first', action='store',
                        help='First instrument to calculate price difference')
    parser.add_argument('--second', action='store',
                        help='second instrument to calculate price difference')
    parser.add_argument('--date', action='store',
                        help='The date that price spread will be generated')
    args = parser.parse_args()

    exchangerate_db = MysqlDB(args.exchangeratedb, args.exchangeratedb)
    source_db = MysqlDB(args.sourcedb, args.sourcedb)
    target_db = MysqlDB(args.targetdb, args.targetdb)

    pdu = PriceDiffUtil(args.mysqluri, exchangerate_db, source_db, target_db, args.coin, args.first, args.second, args.date)

    pdu.generate_price_diff()

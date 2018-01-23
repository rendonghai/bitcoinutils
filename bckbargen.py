#!/bin/env python3

import argparse
import sys, os
from bitcoinutils.dbmanager import MysqlDatabaseManager, MysqlDB
from bitcoinutils.kbargenerator import KBarGenerator


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bitcoin exchange market data K-Bar generate tool.')
    parser.add_argument('--mysql', action='store_true',
                        help='Using MySql Database')
    parser.add_argument('--mysqluri', action='store',
                        help='Mysql Server URI.')
    parser.add_argument('--targetdb', action='store',
                        help='Target Database to generator K Bar data')
    parser.add_argument('--sincedate', action='store_true',
                        help='Since the date that K bar data will be generated')


    args = parser.parse_args()

    target_db = MysqlDB(args.targetdb, args.targetdb)
    kb_gen = KBarGenerator(args.mysqluri, target_db)

    kb_gen.create_last_kbar_data_table(target_db)
    #kb_gen.create_kbar_data_table(target_db, 'huobi', 'btcusdt', '1min', '20180122')

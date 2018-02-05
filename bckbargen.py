#!/usr/bin/env python3

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
    parser.add_argument('--sincedate', action='store',
                        help='Since the date that K bar data will be generated')

    args = parser.parse_args()

    since_date = args.sincedate if hasattr(args, 'sincedate') else None

    target_db = MysqlDB(args.targetdb, args.targetdb)

    kb_gen = KBarGenerator(args.mysqluri, target_db, since_date)
    kb_gen.kbar_generate()

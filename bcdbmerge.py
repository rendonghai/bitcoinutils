#!/usr/bin/env python3

import argparse
import sys, os
from bitcoinutils.dbmanager import SqliteDatabaseManager, SqliteDB, MysqlDatabaseManager, MysqlDB
from bitcoinutils.mergeutils import MysqlMergeUtil
from bitcoinutils.zone import ConfidentInstmtParser


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Bitcoin exchange market data database merge tool.')
    parser.add_argument('--mysql', action='store_true',
                        help='Using MySql Database')
    parser.add_argument('--mysqluri', action='store',
                        help='Mysql Server URI.')
    parser.add_argument('--primarydb', action='store',
                       help='Specify the primary database.')
    parser.add_argument('--primarydbzone', action='store',
                        help='Specify the primary database zone')
    parser.add_argument('--targetdb', action='store',
                       help='Specify the target database.')
    parser.add_argument('--zoneinfo', action='store',
                       help='InI file which stores confident instruments and coins for different zone.')
    parser.add_argument('--sincedate', action='store',
                        help='Since the date then data tables will be merged.')
    parser.add_argument('--enddate', action='store',
                        help='End the date then data tables will be merged.')    
    args = parser.parse_args()

    if not args.primarydb or\
       not args.targetdb:
        raise ValueError('Error: There should be two databases specified')

    pdb = MysqlDB(args.primarydb, args.primarydb, args.primarydbzone)
    tdb = MysqlDB(args.targetdb, args.targetdb)
    zp = ConfidentInstmtParser(args.zoneinfo)
    since_date = args.sincedate if hasattr(args, 'sincedate') else None
    end_date = args.enddate if hasattr(args, 'enddate') else None
    mutil = MysqlMergeUtil(args.mysqluri, pdb, tdb, zp, since_date, end_date)
    mutil.merge()

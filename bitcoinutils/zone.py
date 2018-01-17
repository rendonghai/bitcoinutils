import os
import configparser
from bitcoinutils.utils import FlyweightMeta, with_metaclass
import re

class ConfidentInstmtParser(with_metaclass(FlyweightMeta)):

    def __init__(self, zone_ini_file=None):
        self.zone_ini_file = zone_ini_file
        self.config = configparser.ConfigParser()
        if os.path.exists(zone_ini_file):
            self.config.read(zone_ini_file)
        else:
            raise Error('No Such Confident Zone Info File.')

    def is_confident(self, zone, instmt):
        try:
            if self.zone_ini_file is None:
                return False
            instmts = self.config.get(zone.upper(), 'instruments')
            if instmts:
                imts = list(map(lambda x: x.strip(), instmts.split(',')))
                return instmt in imts
            else:
                return False
        except Exception as e:
            return False

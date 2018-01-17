import logging
import datetime

from abc import ABCMeta
import weakref

class FlyweightMeta(type):

    def __new__(mcs, name, parents, dct):
        """
        Set up object pool
        :param name: class name
        :param parents: class parents
        :param dct: dict: includes class attributes, class methods,
        static methods, etc
        :return: new class
        """
        dct['pool'] = weakref.WeakValueDictionary()
        return super(FlyweightMeta, mcs).__new__(mcs, name, parents, dct)

    @staticmethod
    def _serialize_params(cls, *args, **kwargs):
        """
        Serialize input parameters to a key.
        Simple implementation is just to serialize it as a string
        """
        args_list = list(map(str, args))
        args_list.extend([str(kwargs), cls.__name__])
        key = ''.join(args_list)
        return key

    def __call__(cls, *args, **kwargs):
        key = FlyweightMeta._serialize_params(cls, *args, **kwargs)
        pool = getattr(cls, 'pool', {})

        instance = pool.get(key)
        if instance is None:
            instance = super(FlyweightMeta, cls).__call__(*args, **kwargs)
            pool[key] = instance
        return instance

def with_metaclass(meta, *bases):
    """ Provide python cross-version metaclass compatibility. """
    return meta("NewBase", bases, {})

def date_range(start_date, end_date):

    if (start_date > end_date):
        raise ValueError('invalid date range.')

    days = int((end_date - start_date).days)
    for d in range(days+1):
        yield start_date + timedelta(d)

def max_datetime_of_date(d):
    return datetime.combine(d, datetime.max.time())

def min_datetime_of_date(d):
    return datetime.combine(d, datetime.min.time())


if __name__ == '__main__':
    d1 = datetime.strptime('20180111 14:40:49.049512', '%Y%m%d %H:%M:%S.%f')
    d2 = datetime.strptime('20180113 14:49:49.049512', '%Y%m%d %H:%M:%S.%f')

    for day in date_range(d1.date(), d2.date()):
        print (day.strftime("%Y%m%d"))

    print (min_datetime_of_date(date.today()).strftime("%Y%m%d %H:%M:%S.%f"))
    print (max_datetime_of_date(date.today()).strftime("%Y%m%d %H:%M:%S.%f"))

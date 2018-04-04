import datetime
import pandas as pd
import config.settings
import config.currencies
from core import data_feed
from core.logger import get_logger

logger = get_logger('currency')


class Currency(object):
    """
    Object representing currency exchange rate
    """

    @classmethod
    def load_all(cls):
        """Load all currencies in the system into a dictionary"""
        return {v['code']: Currency(v['code']) for v in config.currencies.currencies_definitions}

    def __init__(self, code):
        """Initialise the currency with defaults, taking overrides from the config/currencies.py"""
        self.code = code
        self.ib_symbol = None
        self.quandl_symbol = None
        self.currency_data = ['ib', 'quandl']
        kwargs = config.currencies.currencies_all[code]
        for key, value in kwargs.items():
            setattr(self, key, value)

    def rate(self, nofx=False):
        """
        :param nofx: used to disable currency rates in the AccountCurve
        :return: currency exchange rate as pd.Series, or 1 in special cases
        """
        if nofx is True:
            return 1
        elif self.code == config.settings.base_currency * 2:
            return 1
        else:
            data = data_feed.get_currency(self)
            if data is None or data.empty:
                raise Exception("No price data for currency %s" % self.code)
            return data['rate']

    def __repr__(self):
        return self.code

    def age(self):
        """
        :return: age of the price data in days
        """
        if self.rate() is 1:
            return 0
        else:
            return (pd.to_datetime(datetime.date.today()) - self.rate().tail(1).index[0]).days

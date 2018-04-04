from core import data_feed
import config.spots


class Spot(object):
    """
    Object representing the underlying price for a future contract
    """
    @classmethod
    def load_all(cls):
        """Load all spots in the system into a dictionary"""
        return {v['name']: Spot(v['name']) for v in config.spots.spots_definitions}

    def __init__(self, name):
        """Initialise the spot with defaults, taking overrides from the config/currencies.py"""
        self.name = name
        self.ib_symbol = None
        self.quandl_symbol = None
        self.price_data = ['ib', 'quandl']
        self.multiplier = 1.0
        kwargs = config.spots.spots_all[name]
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return self.name + ' (spot)'

    def get(self):
        """
        :return: close price as pd.Series
        """
        data = data_feed.get_spot(self)
        if data is None or data.empty:
            raise Exception("No price data for symbol: %s" % self)
        return data['close']

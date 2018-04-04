import pandas as pd
import core.basestore as basestore
from enum import Enum

# This list is kept separately from the global config.settings.data_sources, because data may
# still be written for a data provider, while it is disabled and not used
providers_list = ['ib', 'quandl']


# Types of data that can be stored. Each type corresponds to a MySQL table or
# a separate directory if HDF storage is used
class QuotesType(Enum):
    futures = 'futures'
    currency = 'currency'
    others = 'others'


# Define column names mapping from data provider's API to a local storage schema
columns_mapping = {
    ('quandl', 'futures'): {'Date': 'date', 'Trade Date': 'date', 'Open': 'open', 'High': 'high',
                            'Low': 'low', 'Settle': 'close', 'Last Traded': 'close',
                            'Close': 'close', 'Volume': 'volume', 'Total Volume': 'volume'},
    ('quandl', 'currency'): {'Date': 'date', 'Rate': 'rate', 'High (est)': 'high',
                             'Low (est)': 'low'},
    ('quandl', 'others'): {'Date': 'date'},
    ('ib', 'futures'): {},
    ('ib', 'others'): {},
    ('ib', 'currency'): {'close': 'rate'},
}


class Store(object):
    """
    Class to write, read and delete data from local storage (either MySQL of HDF5)
    """
    def __init__(self, provider, quotes_type, symbol):
        assert isinstance(quotes_type, QuotesType)
        assert provider in providers_list
        self.provider = provider
        self.key = symbol
        self.quotes_type = quotes_type
        self.symbol = symbol

    def update(self, new_data):
        """Write a DataFrame to the local storage. Data will be updated on index collision"""
        assert type(new_data) is pd.DataFrame
        basestore.write_data(new_data, self.symbol, self.quotes_type.value, self.provider)

    def get(self):
        """Read a symbol from the local storage"""
        return basestore.read_symbol(self.symbol, self.quotes_type.value, self.provider)

    def delete(self):
        """Delete a symbol from the local storage"""
        basestore.drop_symbol(self.symbol, self.quotes_type.value, self.provider)

import pandas as pd
import core.basestore as basestore
from enum import Enum

providers_list = ['ib', 'quandl', 'bitmex']


# Types of data that can be stored. Each type corresponds to a MySQL table.
class QuotesType(Enum):
    futures = 'futures'
    currency = 'currency'
    others = 'others'


# Define column names mapping from provider's API to SQL schema
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
    # TODO: add mappings for bitmex
}


class Store(object):
    def __init__(self, provider, quotes_type, symbol):
        assert isinstance(quotes_type, QuotesType)
        assert provider in providers_list
        self.provider = provider
        self.key = symbol
        self.quotes_type = quotes_type
        self.symbol = symbol

    def update(self, new_data):
        assert type(new_data) is pd.DataFrame
        basestore.write_data(new_data, self.symbol, self.quotes_type.value, self.provider)

    def get(self):
        return basestore.read_symbol(self.symbol, self.quotes_type.value, self.provider)

    def delete(self):
        basestore.drop_symbol(self.symbol, self.quotes_type.value, self.provider)

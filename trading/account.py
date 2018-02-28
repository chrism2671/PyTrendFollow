import pandas as pd
import config.settings


class Account(object):
    """
    Represents an account in IB multi-account system
    """
    def __init__(self, name):
        self.name = name
        self.base_currency = None
        self.summary = None
        self.net = 0
        self.portfolio = pd.DataFrame()

    def is_valid(self):
        return (self.net > 0) and (self.base_currency == config.settings.base_currency)
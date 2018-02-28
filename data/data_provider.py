# -*- coding: utf-8 -*-


class DataProvider(object):

    def __init__(self):
        self.library = ''

    def connect(self):
        raise NotImplementedError

    def disconnect(self):
        raise NotImplementedError

    def download_instrument(self, instrument, **kwagrs):
        """
        Download and store historical data for all contracts for the given instrument
        :param instrument: core.instrument.Instrument object
        :param kwagrs: some providers may accept additional args, like start/end date, etc.
        :return:
        """
        raise NotImplementedError

    def download_contract(self, instrument, cont_name, **kwargs):
        """
        Download and store historical data for the given instrument and expiry
        :param instrument: core.instrument.Instrument object
        :param cont_name: contract label string (usually defined as expiry in format YYYYMM)
        :param kwagrs: some providers may accept additional args, like start/end date, etc.
        :return:
        """
        raise NotImplementedError

    def download_currency(self, currency, **kwargs):
        """
        Download and store historical data for the currencies exchange rates
        :param currency: core.currency.Currency object
        :param kwagrs: some providers may accept additional args, like start/end date, etc.
        :return:
        """
        raise NotImplementedError

    def download_table(self, **kwargs):
        """
        General method to download data from provider
        :param kwargs: database and symbol for Quandl, contract for IB, in general can be anything depending on prov
        :return:
        """
        raise NotImplementedError

    def download_spot(self, spot):
        """
        Download historical data for spot prices
        :param spot: core.spot.Spot object
        :return:
        """
        raise NotImplementedError

    def drop_symbol(self, **kwargs):
        """
        Delete a symbol from the storage
        """
        raise NotImplementedError

    def drop_instrument(self, instrument):
        """
        Delete the price data for the given instrument from the storage
        :param instrument: core.trading.Instrument object
        :return:
        """
        raise NotImplementedError

    def drop_currency(self, currency):
        """
        Delete the exchange rate data for the given currency from the storage
        :param instrument: core.currency.Currency object
        :return:
        """
        raise NotImplementedError
# -*- coding: utf-8 -*-
from json import JSONDecodeError
from time import sleep
from functools import partial

import pandas as pd
import quandl
import quandl.errors.quandl_error

import core.utility
from core.contract_store import Store, QuotesType, columns_mapping
from data.data_provider import DataProvider
from core.logger import get_logger

logger = get_logger('quandl_provider')


class QuandlProvider(DataProvider):

    def __init__(self):
        super().__init__()
        self.library = 'quandl'
        self.api_delay = 0
        self.quotes_formats = { QuotesType.futures: self._format_future,
                                QuotesType.currency: self._format_currency,
                                QuotesType.others: self._format_other}

    def connect(self):
        """
        Quandl doesn't require connection. Api key is specified in config.settings
        """
        pass

    def disconnect(self):
        pass

    def download_instrument(self, instrument, **kwagrs):
        recent = kwagrs.get('recent', False)
        if recent:
            c = instrument.roll_progression().tail(1).iloc[0] - 100 #-100 here rolls it back one year
        else:
            c = str(instrument.first_contract) # Download all contracts
        fail_count = 0
        # Since quandl doesn't seem to have an API function to list all available contracts,
        # we just loop and download them one by one until no data can be found for the next
        logger.info('Downloading contracts for instrument: %s' % instrument.name)
        while fail_count <= 12:
            # print(c)
            if self.download_contract(instrument, c):
                fail_count = 0
            else:
                fail_count += 1
                # Just try one more time in case of a network error
                self.download_contract(instrument, c)
            c = instrument.next_contract(c)
        logger.debug('More than 12 missing contracts in a row - ending the downloading'
                     ' for the instrument %s' % instrument.name)
        return True

    def download_contract(self, instrument, cont_name, **kwagrs):
        api_symbol = core.utility.contract_format(instrument.quandl_symbol, cont_name)
        return self.download_table(QuotesType.futures, instrument.quandl_database,
                                   symbol=api_symbol, db_symbol=instrument.quandl_symbol,
                                   instrument=instrument, contract=cont_name)

    def download_currency(self, currency, **kwargs):
        if currency.quandl_symbol[0:3] == currency.quandl_symbol[3:6]:
            return True
        return self.download_table(QuotesType.currency, currency.quandl_database,
                                   currency.quandl_symbol, currency=currency)

    def download_spot(self, spot):
        return self.download_table(QuotesType.others, spot.quandl_database,
                                   spot.quandl_symbol, col=spot.quandl_column)

    def download_table(self, q_type, database, symbol, db_symbol=None, **kwargs):
        # symbol name for the DB storage may be different from what we send to quandl API (e.g. for futures)
        if db_symbol is None:
            db_symbol = symbol

        # specify the format function for the table (depends on quotes type)
        formnat_fn = self.quotes_formats[q_type]
        # for some spot prices the data column is specified explicitly in instruments.py
        # in such cases we pass this column to a format function and save it to database as 'close'
        if 'col' in kwargs.keys():
            formnat_fn = partial(formnat_fn, column=kwargs.get('col'))
        # pass currency object to scale the rate values where needed
        if 'currency' in kwargs.keys():
            formnat_fn = partial(formnat_fn, currency=kwargs.get('currency'))
        # pass spot object to apply multiplier on data format
        if 'spot' in kwargs.keys():
            formnat_fn = partial(formnat_fn, spot=kwargs.get('spot'))
        # pass instrument and contract to format futures data
        if 'instrument' in kwargs.keys():
            formnat_fn = partial(formnat_fn, instrument=kwargs.get('instrument'),
                                 contract=kwargs.get('contract'))

        try:
            data = quandl.get(database + '/' + symbol)
            Store(self.library, q_type, database + '_' + db_symbol).update(formnat_fn(data=data))
            logger.debug('Wrote data for %s/%s' % (database, symbol))
            sleep(self.api_delay)
        except JSONDecodeError:
            logger.warning("JSONDecodeError")
            return False
        except quandl.errors.quandl_error.NotFoundError:
            logger.debug('Symbol %s not found on database %s' % (symbol, database))
            return False
        except quandl.errors.quandl_error.LimitExceededError:
            logger.warning('Quandl API limit exceeded!')
            return False
        except Exception as e:
            logger.warning('Unexpected error occured: %s' % e)
            return False
        return True

    def drop_symbol(self, q_type, database, symbol, **kwargs):
        Store(self.library, q_type, database + '_' + symbol).delete()

    def drop_instrument(self, instrument):
        self.drop_symbol(QuotesType.futures, instrument.quandl_database, instrument.quandl_symbol)

    def drop_currency(self, currency):
        self.drop_symbol(QuotesType.currency, currency.quandl_database, currency.quandl_symbol)

    def _format_future(self, data, instrument, contract):
        if data is None:
            return None
        if hasattr(instrument, 'quandl_rename_columns'):
            data.rename(columns=instrument.quandl_rename_columns, inplace=True)
        # Very important - Some futures, such as MXP or Cotton have a different factor on Quandl vs reality
        if instrument.quandl_data_factor != 1:
            data[['Settle']] = \
                data[['Settle']] / instrument.quandl_data_factor
        # Sometimes Quandl doesn't return 'Date' for the index, so let's make sure it's set
        data.index = data.index.rename('Date')

        data.reset_index(inplace=True)
        data.rename(columns=columns_mapping[('quandl', QuotesType.futures.value)], inplace=True)
        data['contract'] = pd.Series(int(contract), index=data.index)
        # .to_datetime() doesn't work here if source datatype is 'M8[ns]'
        # data_out['date'] = data_out['date'].astype('datetime64[ns]')
        return data[['date', 'contract', 'close', 'high', 'low', 'open', 'volume']].copy()

    def _format_currency(self, data, currency):
        if data is None:
            return None
        data.reset_index(inplace=True)
        data.rename(columns=columns_mapping[('quandl', QuotesType.currency.value)], inplace=True)
        data[['rate', 'high', 'low']] = currency.quandl_rate(data[['rate', 'high', 'low']])
        return data[['date', 'rate', 'high', 'low']].copy()

    def _format_other(self, column, data, spot=None):
        if data is None:
            return None
        data.reset_index(inplace=True)
        mapping = columns_mapping[('quandl', QuotesType.others.value)]
        mapping[column] = 'close'
        data.rename(columns=mapping, inplace=True)
        if spot is not None:
            data['close'] *= spot.multiplier
        return data[['date', 'close']].copy()

    def _format_btc(self, data_in):
        raise NotImplementedError

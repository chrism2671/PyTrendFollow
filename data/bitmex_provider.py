import pandas as pd
from bravado.client import SwaggerClient
import bravado.exception
from core.contract_store import Store
from data.data_provider import DataProvider
from core.utility import contract_format
from core.logger import get_logger
from core.bitmex_auth import get_request_client, api_config, SPEC_URI
import numpy as np
from time import sleep
from functools import partial
logger = get_logger('bitmex_provider')


class BitmexProvider(DataProvider):

    def __init__(self):
        super().__init__()
        self.library = 'bitmex'
        # it's not necessary to use authorized access for data downloading,
        #  but API limitations will be more restrictive if you don't
        self.auth = True
        self.client = None

    def download_instrument(self, inst, **kwargs):
        """
        :param kwargs: 'clean=True' will force delete and re-download all data,
                        otherwise db will be updated with the new data
        """
        clean = kwargs.get('clean', False)
        if not clean:  # if clean is False, but no existing data found - force it to be True
            try:
                tmp_data = Store(self.library, '', inst.bitmex_symbol).get()
            except KeyError:
                clean = True
        if clean:
            self.drop_symbol(inst.bitmex_symbol)
            last_timestamp = None
        else:  # get the last timestamp and contract from the sorted index
            last_index = tmp_data.index[-1]
            last_timestamp = last_index[1].to_pydatetime()
            last_contract = last_index[0]
        c = str(inst.first_contract)
        logger.info('Downloading contracts for instrument: %s' % inst.name)
        while c is not None:
            nrows = self.download_contract(inst, c, start_time=last_timestamp)
            logger.debug('Got %d rows for contract %s' % (nrows, c))
            # keep downloading contracts until nothing is returned by the server for the next one.
            # if not doing a clean re-download, most dataframes for historical contracts will be
            # empty, so add another condition: c < last_contract
            try_next = (nrows > 0) or ((not clean) and (int(c) < last_contract))
            c = inst.next_contract(c) if try_next else None
        return True

    def download_contract(self, instrument, cont_name, **kwargs):
        start_time = kwargs.get('start_time', None)
        symbol_format = contract_format(instrument.bitmex_symbol, cont_name, format='bitmex')
        n = self.download_table(symbol_format, start_time, partial(self._augment_price, contract=cont_name),
                                instrument.bitmex_symbol)
        return n

    def download_currency(self, currency, **kwargs):
        """
        :param currency:  core.trade.Currency object
        :param kwargs: 'clean=True' will force delete and re-download all data,
                        otherwise db will be updated with the new data
        """
        logger.info('Downloading currency data for %s' % currency.bitmex_symbol)
        clean = kwargs.get('clean', False)
        if clean:
            self.drop_symbol(currency.bitmex_symbol)
            last_timestamp = None
        else:  # get the last timestamp and contract from the sorted index
            last_timestamp = Store(self.library, '', currency.bitmex_symbol).get().index[-1].to_pydatetime()
        n = self.download_table(currency.bitmex_symbol, last_timestamp, self._augment_trades)
        return n > 0

    def download_table(self, symbol, start_time=None, price_func=None, store_symbol=None):
        # Get bravado client on first download call
        if self.client is None:
            if self.auth:
                self.client = get_request_client()
            else:
                self.client = SwaggerClient.from_url(SPEC_URI, config=api_config)
        # In general we may need separate symbol strings for storage and to pass to API
        # (as for futures), but they also may be equal (as for currencies)
        if store_symbol is None:
            store_symbol = symbol
        if price_func is None:
            price_func = self._augment_trades
        store = Store(self.library, '', store_symbol)
        df = pd.DataFrame()
        i = 0
        n = 0
        while True:
            try:
                res = pd.DataFrame(self.client.Trade.Trade_getBucketed(count=500, start=i*500,
                                     symbol=symbol, binSize='1d', startTime=start_time).result())
            except bravado.exception.HTTPTooManyRequests:
                logger.info('Requests limit exceeded, sleeping for 5 mins...')
                sleep(300)
                logger.info('Resuming download')
                continue
            if len(res) == 0:
                break
            # keep track of total records count for calling functions to know if we got any data
            n += len(res)
            df = df.append(res)
            # send data to db each 150K rows and empty local df so it won't get too big
            if len(df) > 150000:  # make this number a class property?
                store.write(price_func(df.fillna(value=np.nan)))
                df = pd.DataFrame()
            i += 1
        if len(df) > 0:
            store.write(price_func(df.fillna(value=np.nan)))
        return n

    def drop_symbol(self, symbol, database=''):
        store = Store(self.library, database, symbol)
        store.delete()

    def drop_instrument(self, instrument):
        self.drop_symbol(symbol=instrument.bitmex_symbol)

    def drop_currency(self, currency):
        self.drop_symbol(symbol=currency.bitmex_symbol)

    def _augment_trades(self, data_in):
        """
        Add datetime index to the price data for correct handling in storage
        """
        data_in['timestamp'] = data_in['timestamp'].astype('datetime64[ns]')
        return data_in.set_index('timestamp').sort_index()

    def _augment_price(self, data_in, contract):
        """
        Add datetime/contract index
        """
        data_in['timestamp'] = data_in['timestamp'].astype('datetime64[ns]')
        data_in = data_in.set_index('timestamp')
        data_in['contract'] = pd.Series(int(contract), index=data_in.index)
        return data_in.set_index('contract', append=True).swaplevel().sort_index()

    def _format_trades(self, data_in):
        """
        Format OHLC price data
        """
        if data_in is None or data_in.empty:
            return None
        data_in.index.rename('date', inplace=True)
        return data_in[['close', 'high', 'low', 'open', 'volume']]

    def _format_price(self, data_in):
        """
        Format OHLC price data for future contract
        """
        if data_in is None or data_in.empty:
            return None
        data_in.index.rename(['contract', 'date'], inplace=True)
        data_in['year'] = data_in.index.to_series().apply(lambda x: x[0] // 100)
        data_in['month'] = data_in.index.to_series().apply(lambda x: x[0] % 100)
        return data_in[['close', 'high', 'low', 'open', 'volume', 'year', 'month']]

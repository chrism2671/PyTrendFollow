# -*- coding: utf-8 -*-
import datetime
import os
import threading
from time import sleep
from core.utility import ConnectionException

import numpy as np
import pandas as pd
from ib.ext.Contract import Contract
from ib.opt import ibConnection

import config.settings
from core.contract_store import Store, QuotesType, columns_mapping
from data.data_provider import DataProvider
from core.ib_connection import get_next_id
from core.logger import get_logger

logger = get_logger('ib_provider')

ib_errors = {
    200: 'Contract not found',
    162: 'Historical market data Service error message',
    354: 'Not subscribed to requested market data',
    321: 'Error validating request',
}


class IBProvider(DataProvider):

    def __init__(self):
        super().__init__()
        self.library = 'ib'
        self.api_delay = 5
        self.api_timeout = 30
        self.clientId = get_next_id()
        self.port = getattr(config.settings, 'ib_port', 4001)
        if os.environ.get('D_HOST') is not None:
            self.host = os.environ.get('D_HOST')
        else:
            self.host = 'localhost'

        # an increasing id for handling async requests
        self.ticker_id = 1
        # temporary data structures to keep data returned by API
        self.historical_data_req_contract = {}
        self.historical_data = {}
        self.historical_data_result = {}
        self.contracts_data = {}
        # threading events
        self.historical_data_event = threading.Event()
        self.contract_details_event = threading.Event()
        self.connection = None

    def connect(self):
        self.connection = ibConnection(host=self.host, port=self.port, clientId=self.clientId)
        fail_count = 0
        fail_limit = 10
        # 'isConnected' attribute is added to self.connection only after the actual connection,
        #  so we are checking both conditions here
        while not ((hasattr(self.connection, 'isConnected') and self.connection.isConnected())
              or fail_count >= fail_limit):
            logger.info('Connecting with clientId %d on %s:%d' % (self.clientId, self.host, self.port))
            r = self.connection.connect()
            if r:
                logger.info('Connection successful!')
                self._register()
            else:
                logger.warning("Couldn't establish connection, retrying in %d s."
                               % (self.api_delay * 2))
                fail_count += 1
                sleep(self.api_delay*2)
        return fail_count < fail_limit

    def disconnect(self):
        if hasattr(self.connection, 'isConnected') and self.connection.isConnected():
            self.connection.close()

    def download_instrument(self, instrument, **kwagrs):
        """
        :param instrument: core.Instrument object
        :return: bool
        """
        ok = self.connect()
        if not ok:
            logger.warning("Download failed: couldn't connect to TWS")
            raise ConnectionException("Couldn't connect to TWS")
        contracts = self.get_contracts(instrument)
        logger.info('Downloading contracts for instrument: %s' % instrument.name)
        for c in contracts:
            # setting noconn=True to prevent reconnecting for each contract
            self.download_contract(instrument, c, noconn=True)
        self.disconnect()
        return True

    def get_contracts(self, instrument):
        contract = Contract()
        contract.m_symbol = instrument.ib_code
        contract.m_secType = 'FUT'
        contract.m_exchange = instrument.exchange
        contract.m_currency = instrument.denomination
        # define the multiplier if we know it
        if hasattr(instrument, 'ib_multiplier'):
            contract.m_multiplier = instrument.ib_multiplier

        self.connection.reqContractDetails(self.ticker_id, contract)
        self.contract_details_event.wait()
        self.contract_details_event.clear()
        sleep(self.api_delay)
        res = self.contracts_data[self.ticker_id]
        res.sort()
        self.ticker_id += 1
        return res

    def download_contract(self, instrument, cont_name, **kwargs):
        noconn = kwargs.get('noconn', False)
        contract = Contract()
        contract.m_symbol = instrument.ib_code
        contract.m_secType = 'FUT'
        # Contract.m_expiry is actually the contract label, not the expiration date
        contract.m_expiry = cont_name
        contract.m_exchange = instrument.exchange
        contract.m_currency = instrument.denomination
        # define the multiplier if we know it
        if hasattr(instrument, 'ib_multiplier'):
            contract.m_multiplier = instrument.ib_multiplier
        req_args = ["3 Y", "1 day", "TRADES", 1, 1]
        return self.download_table(contract, req_args, noconn=noconn)

    def download_currency(self, currency, **kwargs):
        """
        :param currency: core.Currency object
        :return: bool
        """
        if currency.ib_symbol == currency.ib_currency:
            return True
        contract = Contract()
        contract.m_symbol = currency.ib_symbol
        contract.m_currency = currency.ib_currency
        contract.m_secType = 'CASH'
        contract.m_exchange = currency.ib_exchange
        # save currency object to a custom attribute, as it is needed for data formatting
        setattr(contract, 'currency_object', currency)
        req_args = ["3 Y", "1 day", "BID_ASK", 0, 1]
        return self.download_table(contract, req_args)

    def download_spot(self, spot):
        """
        :param spot: core.Spot object
        :return: bool
        """
        contract = Contract()
        contract.m_secType = spot.sec_type
        contract.m_symbol = spot.ib_symbol
        contract.m_currency = spot.denomination
        contract.m_exchange = spot.ib_exchange
        # save spot object to a custom attribute, as it is needed for data formatting
        setattr(contract, 'spot_object', spot)
        req_args = ["3 Y", "1 day", "TRADES", 1, 1]
        return self.download_table(contract, req_args)

    def download_table(self, contract, req_args, **kwargs):
        """
        General method to download data from IB. Contract and req_args are filled according to the
        specific data.
        noconn should be set to True only if the client connection is handled somewhere
        outside this method.
        """
        if self.historical_data_event.is_set():
            return False
        noconn = kwargs.get('noconn', False)
        if not noconn:
            ok = self.connect()
            if not ok:
                logger.warning("Download failed: couldn't connect to TWS")
                raise ConnectionException("Couldn't connect to TWS")
        end_datetime = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
        self.historical_data_req_contract[self.ticker_id] = contract
        self.connection.reqHistoricalData(self.ticker_id, contract, end_datetime, *req_args)
        res = self.historical_data_event.wait(self.api_timeout)
        self.historical_data_event.clear()
        if res:
            if not noconn:
                self.disconnect()
            sleep(self.api_delay)
            res = self.historical_data_result[self.ticker_id]
            self._clear_requests_data()
        else:
            logger.warning('IB historical data request timed out')
        self.ticker_id += 1
        return res

    def drop_symbol(self, q_type, exchange, symbol, **kwargs):
        Store(self.library, q_type, exchange + '_' + symbol).delete()

    def drop_instrument(self, instrument):
        self.drop_symbol(QuotesType.futures, instrument.exchange, instrument.ib_code)

    def drop_currency(self, currency):
        self.drop_symbol(QuotesType.currency, currency.ib_exchange,
                         currency.ib_symbol + currency.ib_currency)

    def _register(self):
        self.connection.register(self._error_handler, 'Error')
        self.connection.register(self._historical_data_handler, 'HistoricalData')
        self.connection.register(self._contract_details_handler, 'ContractDetails')
        self.connection.register(self._contract_details_end_handler, 'ContractDetailsEnd')

    def _historical_data_handler(self, msg):
        if int(msg.reqId) not in self.historical_data.keys():
            self.historical_data[int(msg.reqId)] = []

        msg_dict = dict(zip(msg.keys(), msg.values()))
        if msg.close > 0:
            # We've got an incoming history line, save it to a buffer
            self.historical_data[int(msg.reqId)].append(msg_dict)
        else:
            # If msg.close = 0 then we've reached the end of the history. IB sends a zero line to signify the end.
            data = pd.DataFrame(self.historical_data[int(msg.reqId)])
            data['date'] = pd.to_datetime(data['date'], format="%Y%m%d")
            contract = self.historical_data_req_contract[int(msg.reqId)]

            dbg_message = 'Wrote data for symbol %s' % contract.m_symbol
            if contract.m_secType == 'FUT':  # futures
                data = self._format_future(data, contract.m_expiry)
                dbg_message += ', contract %s' % contract.m_expiry
                store_symbol = '_'.join([contract.m_exchange, contract.m_symbol])
                q_type = QuotesType.futures
            elif contract.m_secType == 'CASH':  # currency
                data = self._format_currency(data, contract.currency_object)
                dbg_message += contract.m_currency
                store_symbol = '_'.join([contract.m_exchange, contract.m_symbol + contract.m_currency])
                q_type = QuotesType.currency
            elif contract.m_secType == 'IND':  # indices
                data = self._format_other(data, contract.spot_object)
                dbg_message += ' (index)'
                q_type = QuotesType.others
                store_symbol = '_'.join([contract.m_exchange, contract.m_symbol])
            else:
                raise Exception('Attempt to download data of unsupported secType')

            Store(self.library, q_type, store_symbol).update(data)
            logger.debug(dbg_message)
            self.historical_data_result[int(msg.reqId)] = True
            self.historical_data_event.set()

    def _error_handler(self, msg):
        if (msg.id is None) or (msg.id < 1):  # if msg doesn't belong to any ticker - skip it
            return

        if msg.errorCode in [200, 162, 354, 321]:
            c = self.historical_data_req_contract.get(int(msg.id))
            if c is None:
                c_str = 'No contract data'
            else:
                c_str = '%s: %s/%s%s' % (c.m_secType, c.m_exchange, c.m_symbol, c.m_expiry)
            logger.warning('%d: %s (%s)' % (msg.errorCode, ib_errors[msg.errorCode], c_str))
        # elif ... (more error codes can be added and handled here if needed)
        else:
            logger.warning(str(msg))

        self.historical_data_result[int(msg.id)] = False
        self.historical_data_event.set()

    def _contract_details_handler(self, msg):
        if int(msg.reqId) not in self.contracts_data.keys():
            self.contracts_data[int(msg.reqId)] = []
        self.contracts_data[int(msg.reqId)].append(str(msg.contractDetails.m_contractMonth))

    def _contract_details_end_handler(self, msg):
        self.contract_details_event.set()

    def _format_currency(self, data, currency):
        if data is None:
            return None
        data.reset_index(inplace=True)
        data.rename(columns=columns_mapping[('ib', QuotesType.currency.value)], inplace=True)
        data[['rate', 'high', 'low']] = currency.ib_rate(data[['rate', 'high', 'low']])
        return data[['date', 'rate', 'high', 'low']].copy()

    def _format_future(self, data, contract):
        if data is None:
            return None
        data.reset_index(inplace=True)
        data.rename(columns=columns_mapping[('ib', QuotesType.futures.value)], inplace=True)
        data['contract'] = int(contract)
        return data[['date', 'contract', 'close', 'high', 'low', 'open', 'volume']].copy()

    def _format_other(self, data, spot=None):
        if data is None:
            return None
        data.reset_index(inplace=True)
        data.rename(columns=columns_mapping[('ib', QuotesType.others.value)], inplace=True)
        if spot is not None:
            data['close'] *= spot.multiplier
        return data[['date', 'close']].copy()

    def _clear_requests_data(self):
        # Clear temporary data returned by API to prevent dictionaries from getting too big
        remove_keys = np.arange(1, self.ticker_id - 100)  # store only 100 last responses
        for k in remove_keys:
            self.historical_data_req_contract.pop(k, None)
            self.historical_data.pop(k, None)
            self.historical_data_result.pop(k, None)
            self.contracts_data.pop(k, None)

    def _expiry_to_contract(self, expiry, inst):
        """Some contracts like CL and NG expire the month before the contract name.
        This transforms the expiries returned from IB to the correct contract if that's the case"""
        expiry = int(expiry)
        date = datetime.date(expiry // 100, expiry % 100, inst.expiry)
        date = date + datetime.timedelta(weeks = (-4 * inst.expiration_month))
        return int(str(date.year) + '%02d' % date.month)

    def _contract_to_expiry(self, cont_name, inst):
        cont_name = int(cont_name)
        date = datetime.date(cont_name // 100, cont_name % 100, inst.expiry)
        date = date + datetime.timedelta(weeks = (4 * inst.expiration_month))
        return str(date.year) + '%02d' % date.month
